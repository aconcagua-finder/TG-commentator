import json
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from telethon import TelegramClient
from tg_device import device_kwargs

from admin_web.helpers import (
    _active_project_id,
    _auto_pause_commentator,
    _channel_bare_id,
    _clean_username,
    _clear_account_failure,
    _db_connect,
    _extract_invite_hash,
    _filter_accounts_by_project,
    _filter_by_project,
    _find_discussion_target_by_id,
    _flash,
    _load_accounts,
    _load_join_status,
    _load_settings,
    _parse_bool,
    _parse_int_field,
    _project_id_for,
    _record_account_failure,
    _redirect,
    _save_settings,
    _update_join_status,
)
from admin_web.sort_helpers import apply_sort, resolve_key, template_options
from admin_web.telethon_utils import (
    _attempt_join_target,
    _derive_target_chat_info,
    _refresh_target_access_hashes,
    _resolve_account_credentials,
    _resolve_account_proxy,
    _resolve_account_session,
    _telethon_credentials,
)
from admin_web.templating import _template_context, templates

router = APIRouter()


@router.get("/discussions", response_class=HTMLResponse)
async def discussions_page(request: Request, sort: str = ""):
    settings, settings_err = _load_settings()
    project_id = _active_project_id(settings)
    targets = _filter_by_project(settings.get("discussion_targets", []) or [], project_id)
    sort_key = resolve_key(sort, "chat_target")
    targets_sorted = apply_sort(targets, sort_key, "chat_target")
    targets_view: List[Dict[str, Any]] = [dict(t) for t in targets_sorted]
    last_by_target: Dict[str, Dict[str, Any]] = {}
    try:
        target_ids = [str(t.get("id") or "").strip() for t in targets_view if str(t.get("id") or "").strip()]
        if target_ids:
            placeholders = ", ".join(["?"] * len(target_ids))
            with _db_connect() as conn:
                rows = conn.execute(
                    f"""
                    SELECT s.*
                    FROM discussion_sessions s
                    JOIN (
                      SELECT discussion_target_id, MAX(id) AS max_id
                      FROM discussion_sessions
                      WHERE project_id = ? AND discussion_target_id IN ({placeholders})
                      GROUP BY discussion_target_id
                    ) x
                    ON s.discussion_target_id = x.discussion_target_id AND s.id = x.max_id
                    """,
                    tuple([project_id, *target_ids]),
                ).fetchall()
            last_by_target = {str(r["discussion_target_id"]): dict(r) for r in rows if r and r["discussion_target_id"]}
    except Exception:
        last_by_target = {}
    for t in targets_view:
        tid = str(t.get("id") or "").strip()
        if tid and tid in last_by_target:
            t["last_session"] = last_by_target.get(tid)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse(
        "discussion_targets.html",
        _template_context(
            request,
            settings_err=settings_err,
            targets=targets_view,
            accounts=accounts,
            sort_options=template_options("chat_target"),
            current_sort=sort_key,
        ),
    )


@router.get("/discussions/new", response_class=HTMLResponse)
async def discussions_new_page(request: Request, chat_input: str = ""):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    chat_catalog: Dict[str, Dict[str, str]] = {}

    def _add_chat(chat_id: Any, chat_name: Any, chat_username: Any) -> None:
        cid = str(chat_id or "").strip()
        if not cid:
            return
        entry = chat_catalog.setdefault(cid, {"chat_id": cid, "chat_name": "", "chat_username": ""})
        name = str(chat_name or "").strip()
        if name and not entry.get("chat_name"):
            entry["chat_name"] = name
        username = _clean_username(chat_username)
        if username and not entry.get("chat_username"):
            entry["chat_username"] = username

    for t in _filter_by_project(settings.get("targets", []) or [], project_id):
        _add_chat(t.get("chat_id"), t.get("chat_name"), t.get("chat_username"))
        _add_chat(t.get("linked_chat_id"), t.get("linked_chat_name") or t.get("chat_name"), t.get("linked_chat_username"))
    for t in _filter_by_project(settings.get("discussion_targets", []) or [], project_id):
        _add_chat(t.get("chat_id"), t.get("chat_name"), t.get("chat_username"))
        _add_chat(t.get("linked_chat_id"), t.get("linked_chat_name"), t.get("linked_chat_username"))
    for t in _filter_by_project(settings.get("reaction_targets", []) or [], project_id):
        _add_chat(t.get("chat_id"), t.get("chat_name"), t.get("chat_username"))
        _add_chat(t.get("linked_chat_id"), t.get("linked_chat_name"), t.get("linked_chat_username"))
    for t in _filter_by_project(settings.get("monitor_targets", []) or [], project_id):
        _add_chat(t.get("chat_id"), t.get("chat_name"), t.get("chat_username"))

    chat_options: List[Dict[str, str]] = []
    for cid, entry in chat_catalog.items():
        username = _clean_username(entry.get("chat_username"))
        value = f"@{username}" if username else str(entry.get("chat_id") or cid)
        label_name = str(entry.get("chat_name") or "").strip() or (f"@{username}" if username else cid)
        parts: List[str] = []
        if username:
            parts.append(f"@{username}")
        parts.append(str(entry.get("chat_id") or cid))
        label = f"{label_name} ({' · '.join(parts)})"
        chat_options.append({"value": value, "label": label})
    chat_options = sorted(chat_options, key=lambda x: str(x.get("label") or "").lower())
    return templates.TemplateResponse(
        "discussion_target_new.html",
        _template_context(request, accounts=accounts, chat_input_prefill=chat_input, chat_options=chat_options),
    )


@router.post("/discussions/new")
async def discussions_new_submit(
    request: Request,
    chat_input: str = Form(...),
    title: str = Form(""),
    enabled: Optional[str] = Form(None),
    operator_session_name: str = Form(""),
    start_prefix: str = Form(">>"),
    start_on_operator_message: Optional[str] = Form(None),
    vector_prompt: str = Form(""),
    seed_text: str = Form(""),
    action: str = Form("create"),
    turns_min: str = Form("6"),
    turns_max: str = Form("10"),
    memory_turns: str = Form("20"),
    initial_delay_min: str = Form("10"),
    initial_delay_max: str = Form("40"),
    delay_between_min: str = Form("20"),
    delay_between_max: str = Form("80"),
    ai_provider: str = Form("default"),
    slow_join_interval_mins: str = Form("0"),
    select_all: Optional[str] = Form(None),
    assigned_accounts: Optional[List[str]] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)

    seed_text = (seed_text or "").strip()
    action = (action or "").strip().lower()

    chat_input = chat_input.strip()

    # Fast-path: if the chat is already known, reuse its resolved ids/access_hashes (helps when proxies are unstable).
    existing = _filter_by_project(settings.get("discussion_targets", []) or [], project_id)
    input_bare = _channel_bare_id(chat_input)
    input_user = _clean_username(chat_input)
    input_invite = _extract_invite_hash(chat_input)
    matched_target = None
    for t in existing:
        if input_bare is not None:
            existing_ids = {_channel_bare_id(t.get("chat_id")), _channel_bare_id(t.get("linked_chat_id"))}
            existing_ids = {i for i in existing_ids if i is not None}
            if input_bare in existing_ids:
                matched_target = t
                break
        if input_user:
            t_user = _clean_username(t.get("chat_username"))
            if t_user and t_user == input_user:
                matched_target = t
                break
        if input_invite:
            t_invite = str(t.get("invite_link") or "").strip()
            if t_invite and t_invite == input_invite:
                matched_target = t
                break

    if matched_target is not None:
        base = {
            "chat_id": matched_target.get("chat_id"),
            "chat_username": matched_target.get("chat_username"),
            "linked_chat_id": matched_target.get("linked_chat_id"),
            "linked_chat_name": matched_target.get("linked_chat_name"),
            "linked_chat_username": matched_target.get("linked_chat_username"),
            "chat_name": matched_target.get("chat_name"),
            "invite_link": matched_target.get("invite_link"),
            "chat_access_hash": matched_target.get("chat_access_hash"),
            "linked_chat_access_hash": matched_target.get("linked_chat_access_hash"),
        }
    else:
        async with _auto_pause_commentator(
            request, reason="Проверка/вступление в чат (обсуждения)"
        ):
            try:
                base = await _derive_target_chat_info(chat_input)
            except HTTPException as e:
                _flash(request, "danger", str(e.detail))
                return _redirect("/discussions/new")

    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    allowed_sessions = [a.get("session_name") for a in accounts if a.get("session_name")]
    allowed_set = set(allowed_sessions)

    operator_session_name = operator_session_name.strip()
    if not operator_session_name or operator_session_name not in allowed_set:
        _flash(request, "warning", "Нужно выбрать аккаунт оператора (из аккаунтов текущего проекта).")
        return _redirect("/discussions/new")

    new_target: Dict[str, Any] = {
        "id": uuid.uuid4().hex,
        "title": (title or "").strip(),
        **base,
        "enabled": bool(enabled),
        "operator_session_name": operator_session_name,
        "start_prefix": (start_prefix or "").strip(),
        "start_on_operator_message": _parse_bool(start_on_operator_message, default=False),
        "vector_prompt": (vector_prompt or "").strip(),
        "turns_min": _parse_int_field(request, turns_min, default=6, label="Мин. реплик", min_value=1, max_value=200),
        "turns_max": _parse_int_field(request, turns_max, default=10, label="Макс. реплик", min_value=1, max_value=200),
        "memory_turns": _parse_int_field(
            request,
            memory_turns,
            default=20,
            label="Память (реплик)",
            min_value=0,
            max_value=200,
        ),
        "initial_delay_min": _parse_int_field(
            request, initial_delay_min, default=10, label="Мин. задержка старта (сек)", min_value=0, max_value=86400
        ),
        "initial_delay_max": _parse_int_field(
            request, initial_delay_max, default=40, label="Макс. задержка старта (сек)", min_value=0, max_value=86400
        ),
        "delay_between_min": _parse_int_field(
            request, delay_between_min, default=20, label="Мин. пауза (сек)", min_value=0, max_value=86400
        ),
        "delay_between_max": _parse_int_field(
            request, delay_between_max, default=80, label="Макс. пауза (сек)", min_value=0, max_value=86400
        ),
        "ai_provider": (ai_provider or "default").strip() or "default",
        "slow_join_interval_mins": _parse_int_field(
            request, slow_join_interval_mins, default=0, label="Медленное вступление (мин)", min_value=0
        ),
        "date_added": datetime.now(timezone.utc).isoformat(),
        "assigned_accounts": [],
        "project_id": project_id,
    }

    if seed_text:
        new_target["start_on_operator_message"] = True
        new_target["scene1_operator_text"] = seed_text

    if int(new_target.get("turns_max", 0) or 0) < int(new_target.get("turns_min", 0) or 0):
        _flash(request, "warning", "Диапазон реплик: максимум меньше минимума, исправлено.")
        new_target["turns_max"] = int(new_target.get("turns_min", 1) or 1)
    if int(new_target.get("initial_delay_max", 0) or 0) < int(new_target.get("initial_delay_min", 0) or 0):
        _flash(request, "warning", "Диапазон задержки старта: максимум меньше минимума, исправлено.")
        new_target["initial_delay_max"] = int(new_target.get("initial_delay_min", 0) or 0)
    if int(new_target.get("delay_between_max", 0) or 0) < int(new_target.get("delay_between_min", 0) or 0):
        _flash(request, "warning", "Диапазон паузы: максимум меньше минимума, исправлено.")
        new_target["delay_between_max"] = int(new_target.get("delay_between_min", 0) or 0)

    if select_all is not None:
        new_target["assigned_accounts"] = allowed_sessions
    elif assigned_accounts is None:
        new_target["assigned_accounts"] = []
    else:
        new_target["assigned_accounts"] = [s for s in list(assigned_accounts) if s in allowed_set]

    # Optional: scenes 2+ can be created right away on the "new" page.
    try:
        form = await request.form()
    except Exception:
        form = None

    if form is not None:
        scene_id_list = list(form.getlist("scene_id"))
        scene_title_list = list(form.getlist("scene_title"))
        scene_operator_list = list(form.getlist("scene_operator_text"))
        scene_operator_session_list = list(form.getlist("scene_operator_session_name"))
        scene_vector_list = list(form.getlist("scene_vector_prompt"))
        scene_turns_min_list = list(form.getlist("scene_turns_min"))
        scene_turns_max_list = list(form.getlist("scene_turns_max"))
        scene_initial_delay_min_list = list(form.getlist("scene_initial_delay_min"))
        scene_initial_delay_max_list = list(form.getlist("scene_initial_delay_max"))
        scene_delay_between_min_list = list(form.getlist("scene_delay_between_min"))
        scene_delay_between_max_list = list(form.getlist("scene_delay_between_max"))

        scene_lists = [
            scene_id_list,
            scene_title_list,
            scene_operator_list,
            scene_operator_session_list,
            scene_vector_list,
            scene_turns_min_list,
            scene_turns_max_list,
            scene_initial_delay_min_list,
            scene_initial_delay_max_list,
            scene_delay_between_min_list,
            scene_delay_between_max_list,
        ]
        max_len = max([len(x) for x in scene_lists] or [0])
        scenes: List[Dict[str, Any]] = []
        base_assigned = [str(s).strip() for s in (new_target.get("assigned_accounts") or []) if str(s).strip()]

        for i in range(max_len):
            sid = (scene_id_list[i] if i < len(scene_id_list) else "") or ""
            stitle = (scene_title_list[i] if i < len(scene_title_list) else "") or ""
            sop = (scene_operator_list[i] if i < len(scene_operator_list) else "") or ""
            sop_session = (scene_operator_session_list[i] if i < len(scene_operator_session_list) else "") or ""
            svector = (scene_vector_list[i] if i < len(scene_vector_list) else "") or ""
            stmin = (scene_turns_min_list[i] if i < len(scene_turns_min_list) else "") or ""
            stmax = (scene_turns_max_list[i] if i < len(scene_turns_max_list) else "") or ""
            sdmin = (scene_initial_delay_min_list[i] if i < len(scene_initial_delay_min_list) else "") or ""
            sdmax = (scene_initial_delay_max_list[i] if i < len(scene_initial_delay_max_list) else "") or ""
            sbmin = (scene_delay_between_min_list[i] if i < len(scene_delay_between_min_list) else "") or ""
            sbmax = (scene_delay_between_max_list[i] if i < len(scene_delay_between_max_list) else "") or ""

            stitle = str(stitle or "").strip()
            sop = str(sop or "").strip()
            sop_session = str(sop_session or "").strip()
            svector = str(svector or "").strip()

            has_any = any(
                [
                    stitle,
                    sop,
                    svector,
                    str(stmin).strip(),
                    str(stmax).strip(),
                    str(sdmin).strip(),
                    str(sdmax).strip(),
                    str(sbmin).strip(),
                    str(sbmax).strip(),
                ]
            )
            if not has_any:
                continue

            scene_id_norm = str(sid or "").strip() or uuid.uuid4().hex
            scene_obj: Dict[str, Any] = {
                "id": scene_id_norm,
                "title": stitle,
                "operator_text": sop,
                "vector_prompt": svector,
            }
            if sop_session and sop_session in allowed_set:
                scene_obj["operator_session_name"] = sop_session

            if str(stmin).strip():
                scene_obj["turns_min"] = _parse_int_field(
                    request,
                    str(stmin),
                    default=int(new_target.get("turns_min", 6)),
                    label="Сцена: мин. реплик",
                    min_value=1,
                    max_value=200,
                )
            if str(stmax).strip():
                scene_obj["turns_max"] = _parse_int_field(
                    request,
                    str(stmax),
                    default=int(new_target.get("turns_max", 10)),
                    label="Сцена: макс. реплик",
                    min_value=1,
                    max_value=200,
                )

            if str(sdmin).strip():
                scene_obj["initial_delay_min"] = _parse_int_field(
                    request,
                    str(sdmin),
                    default=int(new_target.get("initial_delay_min", 10)),
                    label="Сцена: мин. задержка старта (сек)",
                    min_value=0,
                    max_value=86400,
                )
            if str(sdmax).strip():
                scene_obj["initial_delay_max"] = _parse_int_field(
                    request,
                    str(sdmax),
                    default=int(new_target.get("initial_delay_max", 40)),
                    label="Сцена: макс. задержка старта (сек)",
                    min_value=0,
                    max_value=86400,
                )

            if str(sbmin).strip():
                scene_obj["delay_between_min"] = _parse_int_field(
                    request,
                    str(sbmin),
                    default=int(new_target.get("delay_between_min", 20)),
                    label="Сцена: мин. пауза (сек)",
                    min_value=0,
                    max_value=86400,
                )
            if str(sbmax).strip():
                scene_obj["delay_between_max"] = _parse_int_field(
                    request,
                    str(sbmax),
                    default=int(new_target.get("delay_between_max", 80)),
                    label="Сцена: макс. пауза (сек)",
                    min_value=0,
                    max_value=86400,
                )

            selected_raw = [str(s or "").strip() for s in list(form.getlist(f"scene_assigned_accounts_{scene_id_norm}"))]
            selected_raw = [s for s in selected_raw if s]
            selected = [s for s in selected_raw if s in allowed_set]
            seen = set()
            uniq: List[str] = []
            for s in selected:
                if s in seen:
                    continue
                seen.add(s)
                uniq.append(s)
            if uniq and uniq != base_assigned:
                scene_obj["assigned_accounts"] = uniq

            scenes.append(scene_obj)

        if scenes:
            new_target["scenes"] = scenes

    settings.setdefault("discussion_targets", []).append(new_target)
    _save_settings(settings)
    _flash(request, "success", f"Цель обсуждений добавлена: {base.get('chat_name')}")
    if action == "create_and_start":
        if not seed_text:
            _flash(request, "warning", "Фраза для запуска пустая. Цель добавлена, но обсуждение не запущено.")
            return _redirect(f"/discussions/targets/{quote(str(new_target.get('id')))}")
        return await discussion_target_start(
            request,
            str(new_target.get("id")),
            seed_text=seed_text,
        )
    return _redirect(f"/discussions/targets/{quote(str(new_target.get('id')))}")


@router.get("/discussions/targets/{target_id}", response_class=HTMLResponse)
async def discussion_target_edit_page(request: Request, target_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_discussion_target_by_id(settings, target_id, project_id)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    join_status = _load_join_status([target.get("chat_id"), target.get("linked_chat_id")])

    sessions: List[Dict[str, Any]] = []
    try:
        chat_key = str(target.get("chat_id") or "").strip()
        same_chat_count = sum(
            1
            for t in _filter_by_project(settings.get("discussion_targets", []) or [], project_id)
            if str(t.get("chat_id") or "").strip() == chat_key
        )
        with _db_connect() as conn:
            if same_chat_count <= 1:
                rows = conn.execute(
                    """
                    SELECT
                      s.id, s.status,
                      s.created_at, s.started_at, s.finished_at, s.schedule_at,
                      s.operator_session_name, s.seed_msg_id, s.seed_text,
                      s.chat_id, s.error,
                      (SELECT COUNT(*) FROM discussion_messages m WHERE m.session_id = s.id) AS messages_count
                    FROM discussion_sessions s
                    WHERE s.project_id = ?
                      AND (
                        s.discussion_target_id = ?
                        OR (s.discussion_target_id IS NULL AND s.discussion_target_chat_id = ?)
                      )
                    ORDER BY s.id DESC
                    LIMIT 50
                    """,
                    (project_id, str(target.get("id") or ""), chat_key),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT
                      s.id, s.status,
                      s.created_at, s.started_at, s.finished_at, s.schedule_at,
                      s.operator_session_name, s.seed_msg_id, s.seed_text,
                      s.chat_id, s.error,
                      (SELECT COUNT(*) FROM discussion_messages m WHERE m.session_id = s.id) AS messages_count
                    FROM discussion_sessions s
                    WHERE s.project_id = ? AND s.discussion_target_id = ?
                    ORDER BY s.id DESC
                    LIMIT 50
                    """,
                    (project_id, str(target.get("id") or "")),
                ).fetchall()
        sessions = [dict(r) for r in rows]
    except Exception:
        sessions = []

    return templates.TemplateResponse(
        "discussion_target_edit.html",
        _template_context(request, target=target, accounts=accounts, join_status=join_status, sessions=sessions),
    )


@router.get("/discussions/{chat_id}", response_class=HTMLResponse)
async def discussion_targets_for_chat_page(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    chat_id = str(chat_id or "").strip()
    targets = [
        t
        for t in _filter_by_project(settings.get("discussion_targets", []) or [], project_id)
        if str(t.get("chat_id") or "").strip() == chat_id or str(t.get("linked_chat_id") or "").strip() == chat_id
    ]
    if not targets:
        raise HTTPException(status_code=404, detail="Цель обсуждений не найдена в текущем проекте")
    if len(targets) == 1:
        return _redirect(f"/discussions/targets/{quote(str(targets[0].get('id') or ''))}")
    return templates.TemplateResponse(
        "discussion_targets_pick.html",
        _template_context(request, chat_id=chat_id, targets=targets),
    )


@router.get("/discussions/sessions/{session_id}", response_class=HTMLResponse)
async def discussion_session_detail_page(request: Request, session_id: int):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    try:
        sid = int(session_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Сессия не найдена")

    with _db_connect() as conn:
        row = conn.execute(
            "SELECT * FROM discussion_sessions WHERE id = ? AND project_id = ?",
            (sid, project_id),
        ).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Сессия не найдена")
        session = dict(row)
        msg_rows = conn.execute(
            "SELECT * FROM discussion_messages WHERE session_id = ? ORDER BY id ASC",
            (sid,),
        ).fetchall()
    messages = [dict(r) for r in msg_rows]

    def _pretty_json(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (dict, list)):
            try:
                return json.dumps(value, ensure_ascii=False, indent=2)
            except Exception:
                return str(value)
        s = str(value).strip()
        if not s:
            return ""
        try:
            obj = json.loads(s)
            return json.dumps(obj, ensure_ascii=False, indent=2)
        except Exception:
            return s

    settings_pretty = _pretty_json(session.get("settings_json"))
    participants_pretty = _pretty_json(session.get("participants_json"))
    session_target_id = str(session.get("discussion_target_id") or "").strip()
    if session_target_id:
        back_url = f"/discussions/targets/{quote(session_target_id)}"
    else:
        back_url = f"/discussions/{quote(str(session.get('discussion_target_chat_id') or ''))}"
    return templates.TemplateResponse(
        "discussion_session_detail.html",
        _template_context(
            request,
            session=session,
            messages=messages,
            settings_pretty=settings_pretty,
            participants_pretty=participants_pretty,
            back_url=back_url,
        ),
    )


@router.post("/discussions/targets/{target_id}")
async def discussion_target_edit_save(
    request: Request,
    target_id: str,
    title: str = Form(""),
    enabled: Optional[str] = Form(None),
    operator_session_name: str = Form(""),
    start_prefix: str = Form(""),
    start_on_operator_message: Optional[str] = Form(None),
    vector_prompt: str = Form(""),
    turns_min: str = Form(""),
    turns_max: str = Form(""),
    memory_turns: str = Form(""),
    initial_delay_min: str = Form(""),
    initial_delay_max: str = Form(""),
    delay_between_min: str = Form(""),
    delay_between_max: str = Form(""),
    ai_provider: str = Form("default"),
    slow_join_interval_mins: str = Form(""),
    select_all: Optional[str] = Form(None),
    assigned_accounts: Optional[List[str]] = Form(None),
    scene_id: Optional[List[str]] = Form(None),
    scene_title: Optional[List[str]] = Form(None),
    scene_operator_text: Optional[List[str]] = Form(None),
    scene_operator_session_name: Optional[List[str]] = Form(None),
    scene_vector_prompt: Optional[List[str]] = Form(None),
    scene_turns_min: Optional[List[str]] = Form(None),
    scene_turns_max: Optional[List[str]] = Form(None),
    scene_initial_delay_min: Optional[List[str]] = Form(None),
    scene_initial_delay_max: Optional[List[str]] = Form(None),
    scene_delay_between_min: Optional[List[str]] = Form(None),
    scene_delay_between_max: Optional[List[str]] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, target = _find_discussion_target_by_id(settings, target_id, project_id)

    target["title"] = (title or "").strip()
    target["enabled"] = bool(enabled)
    target["ai_provider"] = (ai_provider or "default").strip() or "default"
    target["operator_session_name"] = (operator_session_name or "").strip()
    target["start_prefix"] = (start_prefix or "").strip()
    target["start_on_operator_message"] = _parse_bool(start_on_operator_message, default=False)
    target["vector_prompt"] = (vector_prompt or "").strip()

    if turns_min.strip():
        target["turns_min"] = _parse_int_field(
            request, turns_min, default=int(target.get("turns_min", 6)), label="Мин. реплик", min_value=1, max_value=200
        )
    if turns_max.strip():
        target["turns_max"] = _parse_int_field(
            request, turns_max, default=int(target.get("turns_max", 10)), label="Макс. реплик", min_value=1, max_value=200
        )
    if int(target.get("turns_max", 0) or 0) < int(target.get("turns_min", 0) or 0):
        _flash(request, "warning", "Диапазон реплик: максимум меньше минимума, исправлено.")
        target["turns_max"] = int(target.get("turns_min", 1) or 1)

    target["memory_turns"] = _parse_int_field(
        request,
        memory_turns,
        default=int(target.get("memory_turns", 20) or 20),
        label="Память (реплик)",
        min_value=0,
        max_value=200,
    )
    target["slow_join_interval_mins"] = _parse_int_field(
        request,
        slow_join_interval_mins,
        default=int(target.get("slow_join_interval_mins", 0) or 0),
        label="Медленное вступление (мин)",
        min_value=0,
    )

    if initial_delay_min.strip():
        target["initial_delay_min"] = _parse_int_field(
            request,
            initial_delay_min,
            default=int(target.get("initial_delay_min", 10)),
            label="Мин. задержка старта (сек)",
            min_value=0,
            max_value=86400,
        )
    if initial_delay_max.strip():
        target["initial_delay_max"] = _parse_int_field(
            request,
            initial_delay_max,
            default=int(target.get("initial_delay_max", 40)),
            label="Макс. задержка старта (сек)",
            min_value=0,
            max_value=86400,
        )
    if int(target.get("initial_delay_max", 0) or 0) < int(target.get("initial_delay_min", 0) or 0):
        _flash(request, "warning", "Диапазон задержки старта: максимум меньше минимума, исправлено.")
        target["initial_delay_max"] = int(target.get("initial_delay_min", 0) or 0)

    if delay_between_min.strip():
        target["delay_between_min"] = _parse_int_field(
            request,
            delay_between_min,
            default=int(target.get("delay_between_min", 20)),
            label="Мин. пауза (сек)",
            min_value=0,
            max_value=86400,
        )
    if delay_between_max.strip():
        target["delay_between_max"] = _parse_int_field(
            request,
            delay_between_max,
            default=int(target.get("delay_between_max", 80)),
            label="Макс. пауза (сек)",
            min_value=0,
            max_value=86400,
        )
    if int(target.get("delay_between_max", 0) or 0) < int(target.get("delay_between_min", 0) or 0):
        _flash(request, "warning", "Диапазон паузы: максимум меньше минимума, исправлено.")
        target["delay_between_max"] = int(target.get("delay_between_min", 0) or 0)

    accounts, _ = _load_accounts()
    allowed_sessions = [
        a.get("session_name")
        for a in _filter_accounts_by_project(accounts, project_id)
        if a.get("session_name")
    ]
    allowed_set = set(allowed_sessions)
    if select_all is not None:
        target["assigned_accounts"] = allowed_sessions
    elif assigned_accounts is None:
        target["assigned_accounts"] = []
    else:
        target["assigned_accounts"] = [s for s in list(assigned_accounts) if s in allowed_set]

    if (
        scene_id is not None
        or scene_title is not None
        or scene_operator_text is not None
        or scene_operator_session_name is not None
        or scene_vector_prompt is not None
        or scene_turns_min is not None
        or scene_turns_max is not None
        or scene_initial_delay_min is not None
        or scene_initial_delay_max is not None
        or scene_delay_between_min is not None
        or scene_delay_between_max is not None
    ):
        form = await request.form()
        scene_lists = [
            scene_id or [],
            scene_title or [],
            scene_operator_text or [],
            scene_operator_session_name or [],
            scene_vector_prompt or [],
            scene_turns_min or [],
            scene_turns_max or [],
            scene_initial_delay_min or [],
            scene_initial_delay_max or [],
            scene_delay_between_min or [],
            scene_delay_between_max or [],
        ]
        max_len = max([len(x) for x in scene_lists] or [0])
        scenes: List[Dict[str, Any]] = []
        for i in range(max_len):
            sid = (scene_id[i] if scene_id and i < len(scene_id) else "") or ""
            stitle = (scene_title[i] if scene_title and i < len(scene_title) else "") or ""
            sop = (scene_operator_text[i] if scene_operator_text and i < len(scene_operator_text) else "") or ""
            sop_session = (
                (scene_operator_session_name[i] if scene_operator_session_name and i < len(scene_operator_session_name) else "")
                or ""
            )
            svector = (scene_vector_prompt[i] if scene_vector_prompt and i < len(scene_vector_prompt) else "") or ""
            stmin = (scene_turns_min[i] if scene_turns_min and i < len(scene_turns_min) else "") or ""
            stmax = (scene_turns_max[i] if scene_turns_max and i < len(scene_turns_max) else "") or ""
            sdmin = (
                (scene_initial_delay_min[i] if scene_initial_delay_min and i < len(scene_initial_delay_min) else "")
                or ""
            )
            sdmax = (
                (scene_initial_delay_max[i] if scene_initial_delay_max and i < len(scene_initial_delay_max) else "")
                or ""
            )
            sbmin = (
                (scene_delay_between_min[i] if scene_delay_between_min and i < len(scene_delay_between_min) else "")
                or ""
            )
            sbmax = (
                (scene_delay_between_max[i] if scene_delay_between_max and i < len(scene_delay_between_max) else "")
                or ""
            )

            stitle = str(stitle or "").strip()
            sop = str(sop or "").strip()
            sop_session = str(sop_session or "").strip()
            svector = str(svector or "").strip()

            has_any = any(
                [
                    stitle,
                    sop,
                    svector,
                    str(stmin).strip(),
                    str(stmax).strip(),
                    str(sdmin).strip(),
                    str(sdmax).strip(),
                    str(sbmin).strip(),
                    str(sbmax).strip(),
                ]
            )
            if not has_any:
                continue

            scene_id_norm = str(sid or "").strip() or uuid.uuid4().hex
            scene_obj: Dict[str, Any] = {
                "id": scene_id_norm,
                "title": stitle,
                "operator_text": sop,
                "vector_prompt": svector,
            }
            if sop_session and sop_session in allowed_set:
                scene_obj["operator_session_name"] = sop_session

            if str(stmin).strip():
                scene_obj["turns_min"] = _parse_int_field(
                    request, str(stmin), default=int(target.get("turns_min", 6)), label="Сцена: мин. реплик", min_value=1, max_value=200
                )
            if str(stmax).strip():
                scene_obj["turns_max"] = _parse_int_field(
                    request, str(stmax), default=int(target.get("turns_max", 10)), label="Сцена: макс. реплик", min_value=1, max_value=200
                )

            if str(sdmin).strip():
                scene_obj["initial_delay_min"] = _parse_int_field(
                    request,
                    str(sdmin),
                    default=int(target.get("initial_delay_min", 10)),
                    label="Сцена: мин. задержка старта (сек)",
                    min_value=0,
                    max_value=86400,
                )
            if str(sdmax).strip():
                scene_obj["initial_delay_max"] = _parse_int_field(
                    request,
                    str(sdmax),
                    default=int(target.get("initial_delay_max", 40)),
                    label="Сцена: макс. задержка старта (сек)",
                    min_value=0,
                    max_value=86400,
                )

            if str(sbmin).strip():
                scene_obj["delay_between_min"] = _parse_int_field(
                    request,
                    str(sbmin),
                    default=int(target.get("delay_between_min", 20)),
                    label="Сцена: мин. пауза (сек)",
                    min_value=0,
                    max_value=86400,
                )
            if str(sbmax).strip():
                scene_obj["delay_between_max"] = _parse_int_field(
                    request,
                    str(sbmax),
                    default=int(target.get("delay_between_max", 80)),
                    label="Сцена: макс. пауза (сек)",
                    min_value=0,
                    max_value=86400,
                )

            try:
                selected_raw = list(form.getlist(f"scene_assigned_accounts_{scene_id_norm}"))
            except Exception:
                selected_raw = []
            selected_raw = [str(s or "").strip() for s in selected_raw if str(s or "").strip()]
            selected = [s for s in selected_raw if s in allowed_set]
            seen = set()
            uniq = []
            for s in selected:
                if s in seen:
                    continue
                seen.add(s)
                uniq.append(s)

            base_assigned = [str(s).strip() for s in (target.get("assigned_accounts") or []) if str(s).strip()]
            if uniq and uniq != base_assigned:
                scene_obj["assigned_accounts"] = uniq

            scenes.append(scene_obj)
        target["scenes"] = scenes

    settings["discussion_targets"][idx] = target
    _save_settings(settings)
    _flash(request, "success", "Настройки обсуждений обновлены.")
    return _redirect(f"/discussions/targets/{quote(str(target.get('id') or target_id))}")


@router.post("/discussions/targets/{target_id}/rename")
async def discussion_target_rename(
    request: Request,
    target_id: str,
    title: str = Form(""),
    next: str = Form(""),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, target = _find_discussion_target_by_id(settings, target_id, project_id)
    target["title"] = (title or "").strip()
    settings["discussion_targets"][idx] = target
    _save_settings(settings)
    _flash(request, "success", "Название цели обновлено.")

    next_url = str(next or "").strip()
    if next_url.startswith("/"):
        return _redirect(next_url)
    return _redirect(f"/discussions/targets/{quote(str(target.get('id') or target_id))}")


@router.post("/discussions/targets/{target_id}/refresh")
async def discussion_target_refresh_chat_info(
    request: Request,
    target_id: str,
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, target = _find_discussion_target_by_id(settings, target_id, project_id)

    chat_input = str(target.get("chat_username") or "").strip()
    if chat_input:
        chat_input = f"@{chat_input}"
    if not chat_input:
        chat_input = str(target.get("chat_id") or "").strip()
    if not chat_input:
        _flash(request, "danger", "Не удалось определить чат для обновления (chat_id/chat_username пустые).")
        return _redirect(f"/discussions/targets/{quote(str(target.get('id') or target_id))}")

    async with _auto_pause_commentator(
        request, reason="Обновление информации о чате (обсуждения)"
    ):
        try:
            base = await _derive_target_chat_info(chat_input)
        except HTTPException as e:
            _flash(request, "danger", str(e.detail))
            return _redirect(f"/discussions/targets/{quote(str(target.get('id') or target_id))}")

    for k in (
        "chat_id",
        "chat_username",
        "chat_name",
        "invite_link",
        "chat_access_hash",
        "linked_chat_id",
        "linked_chat_name",
        "linked_chat_username",
        "linked_chat_access_hash",
    ):
        if k in base and base.get(k) is not None:
            target[k] = base.get(k)

    settings["discussion_targets"][idx] = target
    _save_settings(settings)
    _flash(request, "success", "Информация о чате обновлена.")
    return _redirect(f"/discussions/targets/{quote(str(target.get('id') or target_id))}")


@router.post("/discussions/targets/{target_id}/join")
async def discussion_target_join_attempt(
    request: Request,
    target_id: str,
    session_name: str = Form(""),
    join_target_id: str = Form(""),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_discussion_target_by_id(settings, target_id, project_id)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)

    if session_name:
        accounts = [a for a in accounts if a.get("session_name") == session_name]
    else:
        assigned = target.get("assigned_accounts") or []
        if assigned:
            accounts = [a for a in accounts if a.get("session_name") in assigned]
        else:
            accounts = []

    if not accounts:
        _flash(request, "warning", "Нет выбранных аккаунтов для вступления.")
        return _redirect(f"/discussions/targets/{quote(str(target.get('id') or target_id))}")

    api_id_default, api_hash_default = _telethon_credentials()
    target_ids: List[str] = []
    if join_target_id:
        target_ids = [str(join_target_id)]
    else:
        if target.get("chat_id"):
            target_ids.append(str(target.get("chat_id")))
        if target.get("linked_chat_id"):
            target_ids.append(str(target.get("linked_chat_id")))

    total_joined = 0
    total_failed = 0
    had_lock = False

    async with _auto_pause_commentator(request, reason="Вступление в чат (обсуждения)"):
        await _refresh_target_access_hashes(target, settings)
        for acc in accounts:
            account_success = True
            session = _resolve_account_session(acc)
            if not session:
                continue
            api_id, api_hash = _resolve_account_credentials(acc, api_id_default, api_hash_default)
            proxy_tuple = _resolve_account_proxy(acc)
            client = TelegramClient(
                session,
                api_id,
                api_hash,
                proxy=proxy_tuple,
                **device_kwargs(acc),
            )
            try:
                try:
                    await client.connect()
                except Exception as exc:
                    if "locked" in str(exc).lower():
                        had_lock = True
                        for t_id in target_ids:
                            _update_join_status(
                                acc.get("session_name", ""),
                                t_id,
                                "failed",
                                last_error="session_db_locked",
                                last_method="connect",
                            )
                            total_failed += 1
                        continue
                    raise
                if not await client.is_user_authorized():
                    _update_join_status(
                        acc.get("session_name", ""),
                        str(target.get("chat_id")),
                        "failed",
                        last_error="unauthorized",
                        last_method="auth",
                    )
                    _record_account_failure(
                        acc.get("session_name", ""),
                        "join",
                        last_error="unauthorized",
                        last_target=str(target.get("chat_id")),
                    )
                    total_failed += 1
                    account_success = False
                    continue
                for t_id in target_ids:
                    joined, last_error, last_method = await _attempt_join_target(
                        client, acc.get("session_name", ""), target, t_id
                    )
                    if joined:
                        _update_join_status(acc.get("session_name", ""), t_id, "joined")
                        total_joined += 1
                    else:
                        _update_join_status(
                            acc.get("session_name", ""), t_id, "failed", last_error=last_error, last_method=last_method
                        )
                        _record_account_failure(
                            acc.get("session_name", ""),
                            "join",
                            last_error=str(last_error) if last_error else None,
                            last_target=str(t_id),
                        )
                        account_success = False
                        total_failed += 1
            except Exception as exc:
                if "locked" in str(exc).lower():
                    had_lock = True
                    for t_id in target_ids:
                        _update_join_status(
                            acc.get("session_name", ""),
                            t_id,
                            "failed",
                            last_error="session_db_locked",
                            last_method="connect",
                        )
                        _record_account_failure(
                            acc.get("session_name", ""),
                            "join",
                            last_error="session_db_locked",
                            last_target=str(t_id),
                        )
                        total_failed += 1
                        account_success = False
                else:
                    for t_id in target_ids:
                        _update_join_status(
                            acc.get("session_name", ""),
                            t_id,
                            "failed",
                            last_error=str(exc),
                            last_method="connect",
                        )
                        _record_account_failure(
                            acc.get("session_name", ""),
                            "join",
                            last_error=str(exc),
                            last_target=str(t_id),
                        )
                        total_failed += 1
                        account_success = False
            finally:
                try:
                    if client.is_connected():
                        await client.disconnect()
                except Exception:
                    had_lock = True
                except Exception:
                    pass
            if account_success:
                _clear_account_failure(acc.get("session_name", ""), "join")

    if had_lock:
        _flash(
            request,
            "warning",
            "Сессия занята другим процессом (database is locked). "
            "Остановите commentator и повторите попытку.",
        )
    else:
        _flash(request, "success", f"Вступление: OK={total_joined}, failed={total_failed}")
    return _redirect(f"/discussions/targets/{quote(str(target.get('id') or target_id))}")


@router.post("/discussions/targets/{target_id}/start")
async def discussion_target_start(
    request: Request,
    target_id: str,
    seed_text: str = Form(...),
):
    seed_text = (seed_text or "").strip()
    if not seed_text:
        _flash(request, "warning", "Фраза пустая.")
        return _redirect(f"/discussions/targets/{quote(target_id)}")

    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, target = _find_discussion_target_by_id(settings, target_id, project_id)
    target["scene1_operator_text"] = seed_text
    settings["discussion_targets"][idx] = target

    operator_session = str(target.get("operator_session_name") or "").strip()
    if not operator_session:
        _flash(request, "danger", "Не выбран аккаунт оператора. Сначала сохраните настройки цели.")
        return _redirect(f"/discussions/targets/{quote(target_id)}")

    send_chat_id = str(target.get("linked_chat_id") or target.get("chat_id") or "").strip()
    if not send_chat_id:
        _flash(request, "danger", "Не удалось определить чат для отправки (linked_chat_id/chat_id пустые).")
        return _redirect(f"/discussions/targets/{quote(target_id)}")

    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    acc = next((a for a in accounts if a.get("session_name") == operator_session), None)
    if not acc:
        _flash(request, "danger", "Аккаунт оператора не найден в текущем проекте. Проверьте аккаунты.")
        return _redirect(f"/discussions/targets/{quote(target_id)}")

    status = str(acc.get("status") or "").lower().strip()
    if status in {"banned", "frozen", "limited", "human_check", "unauthorized"}:
        _flash(request, "danger", f"Аккаунт оператора недоступен (status={status}). Проверьте аккаунты.")
        return _redirect(f"/discussions/targets/{quote(target_id)}")

    # Queue the start request for commentator.py to execute (so we don't block the UI and avoid session locks).
    if not isinstance(settings.get("discussion_start_queue"), list):
        settings["discussion_start_queue"] = []
    # Replace pending tasks for the same discussion target (user often clicks "start" multiple times if nothing happens).
    removed = []
    kept = []
    for t in (settings.get("discussion_start_queue") or []):
        task_target_id = str(t.get("discussion_target_id") or "").strip()
        if (
            _project_id_for(t) == project_id
            and (
                (task_target_id and task_target_id == str(target.get("id") or "").strip())
                or (
                    not task_target_id
                    and str(t.get("discussion_target_chat_id") or "").strip() == str(target.get("chat_id") or "").strip()
                )
            )
        ):
            removed.append(t)
        else:
            kept.append(t)
    settings["discussion_start_queue"] = kept

    removed_session_ids: list[int] = []
    for t in removed:
        try:
            sid = int(t.get("session_id") or 0)
        except Exception:
            sid = 0
        if sid:
            removed_session_ids.append(sid)
    if removed_session_ids:
        try:
            now = time.time()
            placeholders = ", ".join(["?"] * len(removed_session_ids))
            with _db_connect() as conn:
                conn.execute(
                    f"UPDATE discussion_sessions SET status='canceled', finished_at=?, error=? WHERE id IN ({placeholders})",
                    tuple([now, "replaced_by_new_start", *removed_session_ids]),
                )
        except Exception:
            pass

    session_id: int | None = None
    try:
        settings_snapshot = json.dumps({"target": target}, ensure_ascii=False)
        now = time.time()
        with _db_connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO discussion_sessions (
                    project_id, discussion_target_id, discussion_target_chat_id, chat_id,
                    status, created_at, started_at, finished_at, schedule_at,
                    operator_session_name, seed_msg_id, seed_text,
                    settings_json, participants_json, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                RETURNING id
                """,
                (
                    project_id,
                    str(target.get("id") or ""),
                    str(target.get("chat_id") or ""),
                    str(send_chat_id),
                    "planned",
                    float(now),
                    None,
                    None,
                    None,
                    operator_session,
                    None,
                    seed_text,
                    settings_snapshot,
                    None,
                    None,
                ),
            )
            row = cur.fetchone()
            session_id = int(row[0]) if row and row[0] is not None else None
    except Exception:
        session_id = None
    settings.setdefault("discussion_start_queue", []).append(
        {
            "project_id": project_id,
            "discussion_target_id": str(target.get("id") or ""),
            "discussion_target_chat_id": str(target.get("chat_id") or ""),
            "chat_id": str(send_chat_id),
            "seed_text": seed_text,
            "created_at": time.time(),
            "operator_session_name": operator_session,
            "force_restart": True,
            "tries": 0,
            "next_retry_at": 0.0,
            **({"session_id": session_id} if session_id else {}),
        }
    )
    _save_settings(settings)

    if str(settings.get("status") or "").strip() != "running":
        _flash(request, "warning", "Комментатор сейчас остановлен: запуск обсуждения выполнится после старта комментатора.")
    if session_id:
        _flash(
            request,
            "success",
            f"Задача создана (сессия #{session_id}): сообщение оператора будет отправлено и обсуждение запустится в течение ~10 секунд.",
        )
    else:
        _flash(request, "success", "Задача создана: сообщение оператора будет отправлено и обсуждение запустится в течение ~10 секунд.")
    return _redirect(f"/discussions/targets/{quote(str(target.get('id') or target_id))}")


@router.post("/discussions/targets/{target_id}/delete")
async def discussion_target_delete(request: Request, target_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, _ = _find_discussion_target_by_id(settings, target_id, project_id)
    settings["discussion_targets"].pop(idx)
    _save_settings(settings)
    _flash(request, "success", "Цель обсуждений удалена.")
    return _redirect("/discussions")

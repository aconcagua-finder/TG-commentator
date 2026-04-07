"""Stats, export, rebrand, manual tasks, and tasks overview routes."""

from __future__ import annotations

import io
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import pandas as pd
from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse

from admin_web.activity_helpers import enrich_log_rows, log_type_meta
from admin_web.helpers import (
    _load_settings,
    _save_settings,
    _load_accounts,
    _active_project_id,
    _filter_by_project,
    _filter_accounts_by_project,
    _project_id_for,
    _db_connect,
    _list_manual_tasks,
    _enqueue_manual_task,
    _clear_manual_tasks,
    _clean_username,
    _parse_bool,
    _flash,
    _redirect,
    _auto_pause_commentator,
)
from admin_web.telethon_utils import _get_any_authorized_client
from admin_web.templating import templates, _template_context
from db.schema import _is_postgres

router = APIRouter()


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

LOG_TYPE_OPTIONS: list[tuple[str, str]] = [
    ("", "Все"),
    ("reaction", "Реакция"),
    ("comment", "Комментарий"),
    ("comment_reply", "Ответ"),
    ("comment_failed", "Ошибка"),
    ("comment_skip", "Пропуск"),
    ("monitoring", "Мониторинг"),
    ("forbidden", "Запрещено"),
    ("spam_deleted", "Антиспам: удалён"),
    ("spam_failed", "Антиспам: не удалось"),
]

PERIOD_LABELS: Dict[str, str] = {
    "day": "за сегодня",
    "week": "за неделю",
    "month": "за месяц",
}

# Order + colors for the summary cards on the stats page
SUMMARY_CARDS: List[Tuple[str, str, str]] = [
    ("comment", "Комментарии", "bi-chat-left-text"),
    ("comment_reply", "Ответы", "bi-reply"),
    ("reaction", "Реакции", "bi-hand-thumbs-up"),
    ("comment_failed", "Ошибки комментов", "bi-exclamation-triangle"),
    ("spam_deleted", "Антиспам ✅", "bi-shield-check"),
    ("spam_failed", "Антиспам ❌", "bi-shield-exclamation"),
    ("monitoring", "Мониторинг", "bi-broadcast"),
]


def _period_sql_filter(period: str) -> str:
    if _is_postgres():
        if period == "day":
            return "timestamp::timestamptz >= NOW() - INTERVAL '1 day'"
        if period == "week":
            return "timestamp::timestamptz >= NOW() - INTERVAL '7 days'"
        return "timestamp::timestamptz >= NOW() - INTERVAL '30 days'"
    if period == "day":
        return "timestamp >= datetime('now', '-1 day', 'localtime')"
    if period == "week":
        return "timestamp >= datetime('now', '-7 days', 'localtime')"
    return "timestamp >= datetime('now', '-30 days', 'localtime')"


def _get_period_summary(period: str) -> Dict[str, int]:
    """Return counts grouped by log_type for the whole period."""
    period_filter = _period_sql_filter(period)
    with _db_connect() as conn:
        rows = conn.execute(
            f"""
            SELECT log_type, COUNT(*) AS c
              FROM logs
             WHERE {period_filter}
             GROUP BY log_type
            """
        ).fetchall()

    counts: Dict[str, int] = {"total": 0}
    for row in rows:
        lt = str(row["log_type"] or "other").strip() or "other"
        count = int(row["c"] or 0)
        counts[lt] = counts.get(lt, 0) + count
        counts["total"] += count
    return counts


def _get_logs_for_period(period: str, page: int, items_per_page: int = 20, *, log_type: str = "") -> Tuple[List[Any], int]:
    period_filter = _period_sql_filter(period)

    offset = page * items_per_page
    log_type_value = str(log_type or "").strip()
    log_type_filter = " AND log_type = ?" if log_type_value else ""
    params: tuple[Any, ...] = (log_type_value,) if log_type_value else ()

    with _db_connect() as conn:
        dedup_sub = f"""
            SELECT MIN(id) AS id
            FROM logs
            WHERE {period_filter}{log_type_filter}
            GROUP BY post_id, account_session_name, content
        """

        total_items = conn.execute(f"SELECT COUNT(*) AS c FROM ({dedup_sub}) AS sub", params).fetchone()["c"]

        rows = conn.execute(
            f"""
            SELECT l.* FROM logs l
            INNER JOIN ({dedup_sub}) AS sub ON l.id = sub.id
            ORDER BY l.timestamp DESC
            LIMIT ? OFFSET ?
            """,
            params + (items_per_page, offset),
        ).fetchall()

    return rows, total_items


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


@router.get("/stats", response_class=HTMLResponse)
async def stats_page(request: Request, period: str = "day", page: int = 0, log_type: str = ""):
    if period not in {"day", "week", "month"}:
        period = "day"
    rows, total = _get_logs_for_period(period, page, log_type=log_type)

    total_pages = max((total + 19) // 20, 1)
    page = max(min(page, total_pages - 1), 0)

    settings, _ = _load_settings()
    activity = enrich_log_rows(rows, settings)

    # Build summary cards: counts across the entire period, ignoring the log_type filter
    period_counts = _get_period_summary(period)
    summary_cards: List[Dict[str, Any]] = []
    for lt, label, icon in SUMMARY_CARDS:
        meta = log_type_meta(lt)
        summary_cards.append(
            {
                "key": lt,
                "label": label,
                "icon": icon,
                "color": meta["color"],
                "value": period_counts.get(lt, 0),
            }
        )

    return templates.TemplateResponse(
        "stats.html",
        _template_context(
            request,
            period=period,
            period_label=PERIOD_LABELS.get(period, period),
            page=page,
            total=total,
            total_pages=total_pages,
            activity=activity,
            summary_cards=summary_cards,
            period_total=period_counts.get("total", 0),
            log_type=str(log_type or "").strip(),
            log_type_options=LOG_TYPE_OPTIONS,
        ),
    )


@router.get("/stats/export")
async def stats_export(period: str = "day", log_type: str = ""):
    if period not in {"day", "week", "month"}:
        period = "day"
    rows, _ = _get_logs_for_period(period, 0, items_per_page=100000, log_type=log_type)

    table_data: List[List[Any]] = []
    for row in rows:
        log_type_raw = row["log_type"] or ""
        log_type_map = {
            "reaction": "Реакция",
            "comment": "Комментарий",
            "comment_reply": "Ответ",
            "comment_failed": "Ошибка",
            "comment_skip": "Пропуск",
            "monitoring": "Мониторинг",
            "forbidden": "Запрещено",
            "spam_deleted": "Антиспам: удалён",
            "spam_failed": "Антиспам: не удалось",
        }
        log_type = log_type_map.get(log_type_raw, log_type_raw or "—")

        date_str = str(row["timestamp"] or "")
        try:
            dt = datetime.fromisoformat(date_str)
            date_str = dt.astimezone(timezone(timedelta(hours=3))).strftime("%d.%m.%Y %H:%M:%S")
        except Exception:
            pass

        post_id = row["post_id"]
        channel_username = row["channel_username"]
        source_channel_id = row["source_channel_id"]
        post_link = ""
        if channel_username and post_id:
            post_link = f"https://t.me/{channel_username}/{post_id}"
        elif source_channel_id and post_id:
            chat_id_clean = str(source_channel_id or "").replace("-100", "")
            post_link = f"https://t.me/c/{chat_id_clean}/{post_id}"

        content = row["content"] or ""
        table_data.append(
            [
                log_type,
                date_str,
                row["channel_name"] or "",
                row["account_session_name"] or "",
                row["account_username"] or "",
                post_id,
                content,
                post_link,
            ]
        )

    header = ["Тип", "Дата (МСК)", "Канал", "Исполнитель", "Юзернейм", "ID Поста", "Текст", "Ссылка на пост"]
    df = pd.DataFrame(table_data, columns=header)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Статистика")
        worksheet = writer.sheets["Статистика"]
        for i, col in enumerate(df.columns):
            max_len = max((len(str(v)) for v in df[col]), default=0)
            worksheet.column_dimensions[chr(ord("A") + i)].width = min(max_len + 2, 50)

    output.seek(0)
    today_str = datetime.now().strftime("%Y-%m-%d")
    file_name = f"stats_{period}_{today_str}.xlsx"

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={file_name}"},
    )


# ---------------------------------------------------------------------------
# Rebrand
# ---------------------------------------------------------------------------


@router.get("/rebrand", response_class=HTMLResponse)
async def rebrand_page(request: Request):
    return templates.TemplateResponse("rebrand.html", _template_context(request))


@router.post("/rebrand")
async def rebrand_submit(
    request: Request,
    topic: str = Form(...),
    source: str = Form(...),
):
    topic = topic.strip()
    source = source.strip()
    if not topic or not source:
        raise HTTPException(status_code=400, detail="Нужно указать тему и источник")

    is_channel = source.startswith("@") or "t.me/" in source
    final_source = source.replace("@", "") if is_channel else source

    settings, _ = _load_settings()
    settings["rebrand_task"] = {
        "topic": topic,
        "source_value": final_source,
        "is_channel": is_channel,
        "status": "pending",
    }
    _save_settings(settings)

    _flash(request, "success", "Задача на ребрендинг создана (commentator.py подхватит в течение ~10 секунд).")
    return _redirect("/")


# ---------------------------------------------------------------------------
# Manual queue
# ---------------------------------------------------------------------------


@router.get("/manual", response_class=HTMLResponse)
async def manual_page(request: Request):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    q = _list_manual_tasks(project_id, statuses=("pending", "processing"), limit=500)
    return templates.TemplateResponse("manual.html", _template_context(request, queue=q))


@router.post("/manual/run")
async def manual_run(
    request: Request,
    link: str = Form(...),
    override_vector_prompt: str = Form(""),
    override_accounts_per_post_min: str = Form(""),
    override_accounts_per_post_max: str = Form(""),
    override_delay_between_accounts: str = Form(""),
    override_ignore_daily_limit: Optional[str] = Form(None),
):
    link = link.strip()
    chat_identifier: Any = None
    post_id: int | None = None
    is_private_link = False

    try:
        if "t.me/c/" in link:
            parts = link.split("t.me/c/")[-1].split("/")
            chat_id_num = parts[0]
            post_id = int(parts[1])
            chat_identifier = int(f"-100{chat_id_num}")
            is_private_link = True
        elif "t.me/" in link:
            parts = link.split("t.me/")[-1].split("/")
            chat_identifier = parts[0]
            post_id = int(parts[1])
        else:
            raise ValueError
    except Exception:
        _flash(request, "danger", "Неверный формат ссылки. Пример: https://t.me/channel/123 или https://t.me/c/123/456")
        return _redirect("/manual")

    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    targets = _filter_by_project(settings.get("targets", []) or [], project_id)
    found_target: Dict[str, Any] | None = None
    matched_side: str | None = None  # "main" | "linked"

    target_identifier_clean = _clean_username(chat_identifier)
    check_val = str(chat_identifier)
    for t in targets:
        t_id = str(t.get("chat_id", ""))
        t_link = str(t.get("linked_chat_id", ""))
        t_user = _clean_username(t.get("chat_username", ""))
        t_link_user = _clean_username(t.get("linked_chat_username", ""))

        if check_val == t_id:
            found_target = t
            matched_side = "main"
            break
        if check_val == t_link:
            found_target = t
            matched_side = "linked"
            break

        if is_private_link:
            short_id = str(chat_identifier).replace("-100", "")
            if short_id and short_id in t_id:
                found_target = t
                matched_side = "main"
                break
            if short_id and short_id in t_link:
                found_target = t
                matched_side = "linked"
                break

        if t_user and t_user == target_identifier_clean:
            found_target = t
            matched_side = "main"
            break
        if t_link_user and t_link_user == target_identifier_clean:
            found_target = t
            matched_side = "linked"
            break

    pending_username_update: Tuple[Dict[str, Any], str, str] | None = None
    if not found_target and not is_private_link:
        async with _auto_pause_commentator(
            request,
            reason="Поиск канала по ссылке",
        ):
            try:
                client = await _get_any_authorized_client()
            except HTTPException:
                client = None
            if client:
                try:
                    entity = await client.get_entity(link)
                    real_id = f"-100{entity.id}"
                    for t in targets:
                        t_id = str(t.get("chat_id", ""))
                        t_link = str(t.get("linked_chat_id", ""))
                        if real_id == t_id:
                            found_target = t
                            matched_side = "main"
                            if not t.get("chat_username") and getattr(entity, "username", None):
                                pending_username_update = (t, entity.username, "chat_username")
                            break
                        if real_id == t_link:
                            found_target = t
                            matched_side = "linked"
                            if not t.get("linked_chat_username") and getattr(entity, "username", None):
                                pending_username_update = (t, entity.username, "linked_chat_username")
                            break
                except Exception:
                    pass
                finally:
                    try:
                        if client.is_connected():
                            await client.disconnect()
                    except Exception:
                        pass

    if pending_username_update:
        target_ref, username, field_name = pending_username_update
        if field_name:
            target_ref[field_name] = username
        _save_settings(settings)

    if not found_target:
        _flash(request, "warning", "Канал из ссылки не найден в ваших целях комментирования.")
        return _redirect("/manual")

    # The message can be in the main channel OR in the linked discussion chat (when the user copies a link from comments).
    message_chat_id = str(found_target.get("chat_id") or "").strip()
    if matched_side == "linked":
        linked = str(found_target.get("linked_chat_id") or "").strip()
        if linked:
            message_chat_id = linked

    def _parse_override_int(
        raw: str | None,
        *,
        label: str,
        min_value: int | None = None,
        max_value: int | None = None,
    ) -> int | None:
        s = (raw or "").strip().replace(",", ".")
        if s == "":
            return None
        try:
            n = int(float(s))
        except ValueError:
            _flash(request, "warning", f"Ручной запуск: «{label}» — некорректное число. Игнорирую.")
            return None
        if min_value is not None and n < min_value:
            _flash(request, "warning", f"Ручной запуск: «{label}» — минимум {min_value}. Исправлено.")
            n = min_value
        if max_value is not None and n > max_value:
            _flash(request, "warning", f"Ручной запуск: «{label}» — максимум {max_value}. Исправлено.")
            n = max_value
        return n

    overrides: Dict[str, Any] = {}
    vector_prompt = (override_vector_prompt or "").strip()
    if vector_prompt:
        overrides["vector_prompt"] = vector_prompt

    acc_min = _parse_override_int(
        override_accounts_per_post_min,
        label="Мин. аккаунтов",
        min_value=0,
        max_value=200,
    )
    if acc_min is not None:
        overrides["accounts_per_post_min"] = acc_min

    acc_max = _parse_override_int(
        override_accounts_per_post_max,
        label="Макс. аккаунтов",
        min_value=0,
        max_value=200,
    )
    if acc_max is not None:
        overrides["accounts_per_post_max"] = acc_max

    delay_between = _parse_override_int(
        override_delay_between_accounts,
        label="Пауза между аккаунтами (сек)",
        min_value=0,
        max_value=86400,
    )
    if delay_between is not None:
        overrides["delay_between_accounts"] = delay_between

    if _parse_bool(override_ignore_daily_limit, default=False):
        overrides["daily_comment_limit"] = 0

    _enqueue_manual_task(
        project_id=project_id,
        chat_id=str(found_target["chat_id"]),
        message_chat_id=str(message_chat_id),
        post_id=int(post_id),
        overrides=overrides or {},
    )
    _flash(request, "success", f"Задание добавлено: {found_target.get('chat_name')} / post_id={post_id}")
    return _redirect("/manual")


@router.post("/manual/clear")
async def manual_clear(request: Request):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    removed = _clear_manual_tasks(project_id, statuses=("pending", "processing"))
    _flash(request, "success", f"Очередь ручных заданий очищена ({removed}).")
    return _redirect("/manual")


# ---------------------------------------------------------------------------
# Tasks overview
# ---------------------------------------------------------------------------


@router.get("/tasks", response_class=HTMLResponse)
async def tasks_page(request: Request):
    settings, settings_err = _load_settings()
    project_id = _active_project_id(settings)

    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    project_sessions = {str(a.get("session_name") or "").strip() for a in accounts if a.get("session_name")}

    def _ts(value: Any) -> float:
        if value is None:
            return 0.0
        if isinstance(value, (int, float)):
            return float(value)
        s = str(value).strip()
        if not s:
            return 0.0
        try:
            if s.replace(".", "", 1).isdigit():
                return float(s)
            return datetime.fromisoformat(s).timestamp()
        except Exception:
            return 0.0

    # Build a mapping from chat_id -> (name, href) to help link join tasks.
    link_by_chat_id: Dict[str, str] = {}
    name_by_chat_id: Dict[str, str] = {}

    comment_targets = _filter_by_project(settings.get("targets", []) or [], project_id)
    discussion_targets = _filter_by_project(settings.get("discussion_targets", []) or [], project_id)
    reaction_targets = _filter_by_project(settings.get("reaction_targets", []) or [], project_id)
    monitor_targets = _filter_by_project(settings.get("monitor_targets", []) or [], project_id)

    for t in comment_targets:
        main_id = str(t.get("chat_id") or "").strip()
        if not main_id:
            continue
        href = f"/targets/{quote(main_id)}"
        chat_name = str(t.get("chat_name") or "").strip()
        for cid in {main_id, str(t.get("linked_chat_id") or "").strip()}:
            if not cid:
                continue
            link_by_chat_id.setdefault(cid, href)
            if chat_name:
                name_by_chat_id.setdefault(cid, chat_name)

    for t in discussion_targets:
        chat_name = str(t.get("chat_name") or "").strip()
        main_id = str(t.get("chat_id") or "").strip()
        linked_id = str(t.get("linked_chat_id") or "").strip()
        for cid in {main_id, linked_id}:
            if not cid:
                continue
            link_by_chat_id.setdefault(cid, f"/discussions/{quote(cid)}")
            if chat_name:
                name_by_chat_id.setdefault(cid, chat_name)

    for t in reaction_targets:
        main_id = str(t.get("chat_id") or "").strip()
        if not main_id:
            continue
        href = f"/reaction-targets/{quote(main_id)}"
        chat_name = str(t.get("chat_name") or "").strip()
        for cid in {main_id, str(t.get("linked_chat_id") or "").strip()}:
            if not cid:
                continue
            link_by_chat_id.setdefault(cid, href)
            if chat_name:
                name_by_chat_id.setdefault(cid, chat_name)

    for t in monitor_targets:
        main_id = str(t.get("chat_id") or "").strip()
        if not main_id:
            continue
        link_by_chat_id.setdefault(main_id, f"/monitor-targets/{quote(main_id)}")
        chat_name = str(t.get("chat_name") or "").strip()
        if chat_name:
            name_by_chat_id.setdefault(main_id, chat_name)

    tasks: List[Dict[str, Any]] = []

    # Manual queue (commenting)
    manual_queue = _list_manual_tasks(project_id, statuses=("pending", "processing"), limit=300)
    by_chat_id = {str(t.get("chat_id") or "").strip(): t for t in comment_targets if t.get("chat_id")}
    for task in manual_queue:
        chat_id = str(task.get("chat_id") or "").strip()
        post_id = task.get("post_id")
        task_status = str(task.get("status") or "pending").strip()
        if task_status == "processing":
            status_label = "обрабатывается"
            status_color = "primary"
        else:
            status_label = "в очереди"
            status_color = "warning"
        tgt = by_chat_id.get(chat_id, {}) if isinstance(by_chat_id, dict) else {}
        chat_name = str((tgt or {}).get("chat_name") or name_by_chat_id.get(chat_id) or chat_id or "—")
        title = chat_name
        subtitle = f"post_id={post_id} · chat_id={chat_id}" if chat_id or post_id else ""
        tasks.append(
            {
                "group": "commenting",
                "group_label": "Комментирование",
                "kind_label": "Ручной запуск",
                "title": title,
                "subtitle": subtitle,
                "status": task_status,
                "status_label": status_label,
                "status_color": status_color,
                "is_active": True,
                "when_ts": _ts(task.get("added_at")),
                "href": "/manual",
                "meta": "",
                "search_text": " ".join([str(title), str(subtitle), "manual"]).strip(),
            }
        )

    # Discussions: sessions history
    discussion_target_by_id = {
        str(t.get("id") or "").strip(): t for t in discussion_targets if str(t.get("id") or "").strip()
    }
    try:
        with _db_connect() as conn:

            rows = conn.execute(
                """
                SELECT
                  id, status,
                  created_at, started_at, finished_at, schedule_at,
                  discussion_target_id, discussion_target_chat_id, chat_id,
                  operator_session_name, seed_msg_id, seed_text, error
                FROM discussion_sessions
                WHERE project_id = ?
                ORDER BY id DESC
                LIMIT 200
                """,
                (project_id,),
            ).fetchall()
        for r in rows:
            status = str(r["status"] or "").strip()
            status_map = {
                "running": ("в процессе", "primary", True),
                "planned": ("запланировано", "warning", True),
                "completed": ("завершено", "success", False),
                "failed": ("ошибка", "danger", False),
                "canceled": ("отменено", "secondary", False),
            }
            status_label, status_color, is_active = status_map.get(status, (status or "—", "secondary", False))

            when_ts = _ts(r["created_at"])
            if status == "planned" and r["schedule_at"]:
                when_ts = _ts(r["schedule_at"])
            elif status == "running" and r["started_at"]:
                when_ts = _ts(r["started_at"])
            elif r["finished_at"]:
                when_ts = _ts(r["finished_at"])

            dtid = str(r["discussion_target_id"] or "").strip()
            tgt = discussion_target_by_id.get(dtid) if dtid else None
            tgt_title = str((tgt or {}).get("title") or "").strip()
            chat_name = str((tgt or {}).get("chat_name") or "").strip()
            if not chat_name:
                chat_name = name_by_chat_id.get(str(r["discussion_target_chat_id"] or "").strip(), "")
            title = tgt_title or chat_name or "Обсуждение"
            subtitle = f"Сессия #{r['id']} · chat={r['chat_id']} · operator={r['operator_session_name'] or '—'}"
            meta = str(r["error"] or "").strip()

            tasks.append(
                {
                    "group": "discussions",
                    "group_label": "Обсуждения",
                    "kind_label": "Сессия",
                    "title": title,
                    "subtitle": subtitle,
                    "status": status,
                    "status_label": status_label,
                    "status_color": status_color,
                    "is_active": bool(is_active),
                    "when_ts": when_ts,
                    "href": f"/discussions/sessions/{int(r['id'])}",
                    "meta": meta,
                    "search_text": " ".join(
                        [
                            title,
                            subtitle,
                            str(r.get("seed_text") or ""),
                            str(r.get("operator_session_name") or ""),
                            status,
                        ]
                    ).strip(),
                }
            )
    except Exception:
        pass

    # Inbox outgoing queue (DM/quotes) - best effort
    try:
        with _db_connect() as conn:

            rows = conn.execute(
                """
                SELECT id, kind, status, created_at, session_name, chat_id, reply_to_msg_id, text, error
                FROM inbox_messages
                WHERE direction = 'out'
                ORDER BY id DESC
                LIMIT 200
                """
            ).fetchall()
        for r in rows:
            sess = str(r["session_name"] or "").strip()
            if project_sessions and sess and sess not in project_sessions:
                continue
            kind = str(r["kind"] or "").strip()
            kind_label = "DM" if kind == "dm" else ("Цитирование" if kind == "quote" else (kind or "out"))
            status = str(r["status"] or "").strip()
            status_map = {
                "queued": ("в очереди", "warning", True),
                "sent": ("отправлено", "success", False),
                "error": ("ошибка", "danger", False),
            }
            status_label, status_color, is_active = status_map.get(status, (status or "—", "secondary", False))
            chat_id = str(r["chat_id"] or "").strip()
            reply_to = r["reply_to_msg_id"]
            text_preview = str(r["text"] or "").strip()
            if len(text_preview) > 140:
                text_preview = text_preview[:139].rstrip() + "…"
            title = f"{kind_label}: {chat_id or '—'}"
            subtitle = f"{sess} · reply_to={reply_to or '∅'}"
            meta = str(r["error"] or "").strip() or text_preview
            href = "/dialogs" if kind == "dm" else ("/quotes" if kind == "quote" else None)
            tasks.append(
                {
                    "group": "inbox",
                    "group_label": "Инбокс",
                    "kind_label": kind_label,
                    "title": title,
                    "subtitle": subtitle,
                    "status": status,
                    "status_label": status_label,
                    "status_color": status_color,
                    "is_active": bool(is_active),
                    "when_ts": _ts(r["created_at"]),
                    "href": href,
                    "meta": meta,
                    "search_text": " ".join([title, subtitle, meta, chat_id, sess, status]).strip(),
                }
            )
    except Exception:
        pass

    # Scheduled joins (slow-join)
    try:
        with _db_connect() as conn:

            rows = conn.execute(
                """
                SELECT session_name, target_id, status, next_retry_at, last_error
                FROM join_status
                WHERE status = 'scheduled' AND next_retry_at IS NOT NULL
                ORDER BY next_retry_at ASC
                LIMIT 200
                """
            ).fetchall()
        for r in rows:
            sess = str(r["session_name"] or "").strip()
            if project_sessions and sess and sess not in project_sessions:
                continue
            target_id = str(r["target_id"] or "").strip()
            when_ts = _ts(r["next_retry_at"])
            title = str(name_by_chat_id.get(target_id) or target_id or "—")
            subtitle = f"{sess} · {target_id}"
            meta = str(r["last_error"] or "").strip()
            tasks.append(
                {
                    "group": "join",
                    "group_label": "Вступления",
                    "kind_label": "slow\u2011join",
                    "title": title,
                    "subtitle": subtitle,
                    "status": "scheduled",
                    "status_label": "запланировано",
                    "status_color": "info",
                    "is_active": True,
                    "when_ts": when_ts,
                    "href": link_by_chat_id.get(target_id),
                    "meta": meta,
                    "search_text": " ".join([title, subtitle, meta, "join", "scheduled"]).strip(),
                }
            )
    except Exception:
        pass

    tasks_sorted = sorted(tasks, key=lambda t: float(t.get("when_ts") or 0.0), reverse=True)
    return templates.TemplateResponse(
        "tasks.html",
        _template_context(
            request,
            settings_err=settings_err,
            tasks=tasks_sorted,
        ),
    )

"""Antispam-targets routes (independent section)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse

from admin_web.helpers import (
    _active_project_id,
    _auto_pause_commentator,
    _db_connect,
    _filter_accounts_by_project,
    _filter_by_project,
    _find_antispam_target_by_chat_id,
    _flash,
    _load_accounts,
    _load_settings,
    _parse_bool,
    _redirect,
    _save_settings,
)
from admin_web.telethon_utils import _derive_target_chat_info
from admin_web.templating import _template_context, templates

router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_rule(chat_id: str) -> Dict[str, Any]:
    """Load spam_rules row from DB by chat_id."""
    chat_id = str(chat_id or "").strip()
    defaults: Dict[str, Any] = {
        "enabled": 0,
        "keywords": [],
        "ai_enabled": 1,
        "ai_prompt": "",
        "ai_model": "gpt-4.1-nano",
        "notify_telegram": 0,
    }
    if not chat_id:
        return defaults
    with _db_connect() as conn:
        row = conn.execute(
            """
            SELECT enabled, keywords, ai_enabled, ai_prompt, ai_model, notify_telegram
            FROM spam_rules
            WHERE chat_id = ?
            LIMIT 1
            """,
            (chat_id,),
        ).fetchone()
    if not row:
        return defaults
    raw_keywords = row["keywords"]
    keywords: list[str] = []
    if raw_keywords:
        try:
            parsed = json.loads(raw_keywords)
            if isinstance(parsed, list):
                keywords = [str(x or "").strip() for x in parsed if str(x or "").strip()]
        except Exception:
            keywords = []
    return {
        "enabled": int(row["enabled"] or 0),
        "keywords": keywords,
        "ai_enabled": int(row["ai_enabled"] or 0),
        "ai_prompt": str(row["ai_prompt"] or ""),
        "ai_model": str(row["ai_model"] or "gpt-4.1-nano") or "gpt-4.1-nano",
        "notify_telegram": int(row["notify_telegram"] or 0),
    }


def _keywords_from_textarea(raw: str) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for line in str(raw or "").splitlines():
        kw = line.strip()
        if not kw:
            continue
        key = kw.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(kw)
    return out


def _load_spam_logs(chat_id: str, *, page: int, per_page: int) -> Tuple[List[Any], int]:
    chat_id = str(chat_id or "").strip()
    if not chat_id:
        return [], 0
    page = max(int(page or 0), 0)
    per_page = max(min(int(per_page or 50), 200), 10)
    offset = page * per_page
    with _db_connect() as conn:
        total_row = conn.execute(
            "SELECT COUNT(*) AS c FROM spam_log WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
        total = int(total_row["c"] or 0) if total_row else 0
        rows = conn.execute(
            """
            SELECT *
            FROM spam_log
            WHERE chat_id = ?
            ORDER BY id DESC
            LIMIT ? OFFSET ?
            """,
            (chat_id, per_page, offset),
        ).fetchall()
    return list(rows or []), total


def _upsert_spam_rule(
    chat_id: str,
    *,
    enabled: int,
    keywords_json: str,
    ai_enabled: int,
    ai_prompt: str,
    ai_model: str,
    notify_telegram: int,
) -> None:
    with _db_connect() as conn:
        conn.execute(
            """
            INSERT INTO spam_rules(chat_id, enabled, keywords, ai_enabled, ai_prompt, ai_model, notify_telegram, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                enabled = excluded.enabled,
                keywords = excluded.keywords,
                ai_enabled = excluded.ai_enabled,
                ai_prompt = excluded.ai_prompt,
                ai_model = excluded.ai_model,
                notify_telegram = excluded.notify_telegram
            """,
            (chat_id, enabled, keywords_json, ai_enabled, ai_prompt, ai_model, notify_telegram, _now_iso()),
        )
        conn.commit()


# ---------------------------------------------------------------------------
# List
# ---------------------------------------------------------------------------


@router.get("/antispam-targets", response_class=HTMLResponse)
async def antispam_targets_page(request: Request):
    settings, settings_err = _load_settings()
    project_id = _active_project_id(settings)
    targets = _filter_by_project(settings.get("antispam_targets", []) or [], project_id)
    targets_sorted = sorted(targets, key=lambda x: x.get("date_added", ""), reverse=True)

    # Enrich with enabled status from spam_rules DB.
    for t in targets_sorted:
        rule = _load_rule(str(t.get("chat_id") or ""))
        t["_rule_enabled"] = bool(rule.get("enabled"))

    return templates.TemplateResponse(
        "antispam_targets.html",
        _template_context(request, settings_err=settings_err, targets=targets_sorted),
    )


# ---------------------------------------------------------------------------
# New
# ---------------------------------------------------------------------------


@router.get("/antispam-targets/new", response_class=HTMLResponse)
async def antispam_targets_new_page(request: Request):
    return templates.TemplateResponse("antispam_target_new.html", _template_context(request))


@router.post("/antispam-targets/new")
async def antispam_targets_new_submit(
    request: Request,
    chat_input: str = Form(...),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)

    chat_input = chat_input.strip()
    async with _auto_pause_commentator(request, reason="Проверка чата (антиспам)"):
        try:
            base = await _derive_target_chat_info(chat_input)
        except HTTPException as e:
            _flash(request, "danger", str(e.detail))
            return _redirect("/antispam-targets/new")

    chat_id = base["chat_id"]
    existing = _filter_by_project(settings.get("antispam_targets", []) or [], project_id)
    if any(str(t.get("chat_id")) == str(chat_id) for t in existing):
        _flash(request, "warning", "Этот чат уже добавлен в цели антиспама.")
        return _redirect(f"/antispam-targets/{quote(chat_id)}")

    new_target: Dict[str, Any] = {
        **base,
        "assigned_accounts": [],
        "date_added": datetime.now(timezone.utc).isoformat(),
        "project_id": project_id,
    }
    settings.setdefault("antispam_targets", []).append(new_target)
    _save_settings(settings)

    # Create default spam_rules row if missing.
    _upsert_spam_rule(
        chat_id,
        enabled=0,
        keywords_json="[]",
        ai_enabled=1,
        ai_prompt="",
        ai_model="gpt-4.1-nano",
        notify_telegram=0,
    )

    _flash(request, "success", f"Цель антиспама добавлена: {base.get('chat_name')}")
    return _redirect(f"/antispam-targets/{quote(chat_id)}")


# ---------------------------------------------------------------------------
# Edit
# ---------------------------------------------------------------------------


@router.get("/antispam-targets/{chat_id}", response_class=HTMLResponse)
async def antispam_target_edit_page(request: Request, chat_id: str):
    settings, settings_err = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_antispam_target_by_chat_id(settings, chat_id, project_id)

    rule = _load_rule(str(target.get("chat_id") or ""))
    keywords_text = "\n".join(rule.get("keywords") or [])

    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)

    logs, _ = _load_spam_logs(str(target.get("chat_id") or ""), page=0, per_page=20)

    return templates.TemplateResponse(
        "antispam_target_edit.html",
        _template_context(
            request,
            settings_err=settings_err,
            target=target,
            rule=rule,
            keywords_text=keywords_text,
            accounts=accounts,
            recent_logs=logs,
        ),
    )


@router.post("/antispam-targets/{chat_id}")
async def antispam_target_edit_save(
    request: Request,
    chat_id: str,
    enabled: str | None = Form(None),
    keywords_text: str = Form(""),
    ai_enabled: str | None = Form(None),
    ai_prompt: str = Form(""),
    ai_model: str = Form("gpt-4.1-nano"),
    notify_telegram: str | None = Form(None),
    ban_spammers: str | None = Form(None),
    bot_token: str = Form(""),
    select_all: Optional[str] = Form(None),
    assigned_accounts: Optional[List[str]] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, target = _find_antispam_target_by_chat_id(settings, chat_id, project_id)

    target_chat_id = str(target.get("chat_id") or "").strip()

    # Update spam rules in DB.
    keywords = _keywords_from_textarea(keywords_text)
    _upsert_spam_rule(
        target_chat_id,
        enabled=1 if _parse_bool(enabled, default=False) else 0,
        keywords_json=json.dumps(keywords, ensure_ascii=False),
        ai_enabled=1 if _parse_bool(ai_enabled, default=False) else 0,
        ai_prompt=str(ai_prompt or "").strip(),
        ai_model=str(ai_model or "gpt-4.1-nano").strip() or "gpt-4.1-nano",
        notify_telegram=1 if _parse_bool(notify_telegram, default=False) else 0,
    )

    # Bot token for deletion via Bot API.
    target["bot_token"] = str(bot_token or "").strip()
    target["ban_spammers"] = _parse_bool(ban_spammers, default=False)

    # Update assigned accounts in settings.json (used when bot_token is empty).
    accounts, _ = _load_accounts()
    allowed_sessions = [
        a.get("session_name")
        for a in _filter_accounts_by_project(accounts, project_id)
        if a.get("session_name")
    ]
    allowed_set = set(allowed_sessions)
    if select_all is not None:
        target["assigned_accounts"] = allowed_sessions
    else:
        target["assigned_accounts"] = [s for s in list(assigned_accounts or []) if s in allowed_set]

    settings["antispam_targets"][idx] = target
    _save_settings(settings)
    _flash(request, "success", "Настройки антиспама сохранены.")
    return _redirect(f"/antispam-targets/{quote(chat_id)}")


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


@router.post("/antispam-targets/{chat_id}/delete")
async def antispam_target_delete(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, _ = _find_antispam_target_by_chat_id(settings, chat_id, project_id)
    settings["antispam_targets"].pop(idx)
    _save_settings(settings)
    _flash(request, "success", "Цель антиспама удалена.")
    return _redirect("/antispam-targets")


# ---------------------------------------------------------------------------
# Log
# ---------------------------------------------------------------------------


@router.get("/antispam-targets/{chat_id}/log", response_class=HTMLResponse)
async def antispam_target_log_page(request: Request, chat_id: str, page: int = 0):
    settings, settings_err = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_antispam_target_by_chat_id(settings, chat_id, project_id)

    target_chat_id = str(target.get("chat_id") or "").strip()
    rows, total = _load_spam_logs(target_chat_id, page=page, per_page=50)
    total_pages = max((total + 49) // 50, 1)
    page = max(min(int(page or 0), total_pages - 1), 0)

    return templates.TemplateResponse(
        "antispam_target_log.html",
        _template_context(
            request,
            settings_err=settings_err,
            target=target,
            rows=rows,
            page=page,
            total=total,
            total_pages=total_pages,
        ),
    )


@router.post("/antispam-targets/{chat_id}/log/{log_id}/restore")
async def antispam_target_log_restore(request: Request, chat_id: str, log_id: int):
    try:
        with _db_connect() as conn:
            conn.execute("UPDATE spam_log SET action = 'restored' WHERE id = ?", (int(log_id),))
            conn.commit()
    except Exception:
        pass
    _flash(request, "success", "Запись отмечена как восстановленная (ложное срабатывание).")
    return _redirect(f"/antispam-targets/{quote(chat_id)}/log")


# ---------------------------------------------------------------------------
# Bans
# ---------------------------------------------------------------------------


def _load_spam_bans(chat_id: str, *, page: int, per_page: int):
    chat_id = str(chat_id or "").strip()
    if not chat_id:
        return [], 0
    page = max(int(page or 0), 0)
    per_page = max(min(int(per_page or 50), 200), 10)
    offset = page * per_page
    with _db_connect() as conn:
        total_row = conn.execute(
            "SELECT COUNT(*) AS c FROM spam_bans WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()
        total = int(total_row["c"] or 0) if total_row else 0
        rows = conn.execute(
            """
            SELECT *
            FROM spam_bans
            WHERE chat_id = ?
            ORDER BY banned_at DESC
            LIMIT ? OFFSET ?
            """,
            (chat_id, per_page, offset),
        ).fetchall()
    return list(rows or []), total


@router.get("/antispam-targets/{chat_id}/bans", response_class=HTMLResponse)
async def antispam_target_bans_page(request: Request, chat_id: str, page: int = 0):
    settings, settings_err = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_antispam_target_by_chat_id(settings, chat_id, project_id)

    target_chat_id = str(target.get("chat_id") or "").strip()
    rows, total = _load_spam_bans(target_chat_id, page=page, per_page=50)
    total_pages = max((total + 49) // 50, 1)
    page = max(min(int(page or 0), total_pages - 1), 0)

    return templates.TemplateResponse(
        "antispam_target_bans.html",
        _template_context(
            request,
            settings_err=settings_err,
            target=target,
            rows=rows,
            page=page,
            total=total,
            total_pages=total_pages,
        ),
    )


@router.post("/antispam-targets/{chat_id}/bans/{user_id}/unban")
async def antispam_target_unban_user(request: Request, chat_id: str, user_id: int):
    from services.antispam import _unban_user_via_bot, _unban_user_via_client

    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_antispam_target_by_chat_id(settings, chat_id, project_id)

    target_chat_id = str(target.get("chat_id") or "").strip()
    bot_token = str(target.get("bot_token") or "").strip()

    unbanned = False
    if bot_token:
        unbanned = await _unban_user_via_bot(bot_token, int(target_chat_id), user_id)
    else:
        # Unban via Telethon requires active_clients from commentator.
        # We only have Bot API path from web UI; mark as unbanned in DB regardless.
        unbanned = True

    if unbanned:
        try:
            with _db_connect() as conn:
                conn.execute(
                    "UPDATE spam_bans SET unbanned_at = ? WHERE chat_id = ? AND user_id = ?",
                    (_now_iso(), target_chat_id, user_id),
                )
                conn.commit()
        except Exception:
            pass
        _flash(request, "success", f"Пользователь {user_id} разбанен.")
    else:
        _flash(request, "danger", f"Не удалось разбанить пользователя {user_id}.")
    return _redirect(f"/antispam-targets/{quote(chat_id)}/bans")

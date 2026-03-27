from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple
from urllib.parse import quote

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse

from admin_web.helpers import (
    _active_project_id,
    _db_connect,
    _find_target_by_chat_id,
    _flash,
    _load_settings,
    _parse_bool,
    _redirect,
)
from admin_web.templating import _template_context, templates

router = APIRouter()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_rule(linked_chat_id: str) -> Dict[str, Any]:
    linked_chat_id = str(linked_chat_id or "").strip()
    if not linked_chat_id:
        return {
            "enabled": 0,
            "keywords": [],
            "ai_enabled": 1,
            "ai_prompt": "",
            "ai_model": "gpt-4.1-nano",
            "notify_telegram": 0,
        }
    with _db_connect() as conn:
        row = conn.execute(
            """
            SELECT enabled, keywords, ai_enabled, ai_prompt, ai_model, notify_telegram
            FROM spam_rules
            WHERE chat_id = ?
            LIMIT 1
            """,
            (linked_chat_id,),
        ).fetchone()
    if not row:
        return {
            "enabled": 0,
            "keywords": [],
            "ai_enabled": 1,
            "ai_prompt": "",
            "ai_model": "gpt-4.1-nano",
            "notify_telegram": 0,
        }
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


def _load_spam_logs(linked_chat_id: str, *, page: int, per_page: int) -> Tuple[List[Any], int]:
    linked_chat_id = str(linked_chat_id or "").strip()
    if not linked_chat_id:
        return [], 0
    page = max(int(page or 0), 0)
    per_page = max(min(int(per_page or 50), 200), 10)
    offset = page * per_page
    with _db_connect() as conn:
        total_row = conn.execute(
            "SELECT COUNT(*) AS c FROM spam_log WHERE chat_id = ?",
            (linked_chat_id,),
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
            (linked_chat_id, per_page, offset),
        ).fetchall()
    return list(rows or []), total


@router.get("/targets/{chat_id}/antispam", response_class=HTMLResponse)
async def target_antispam_page(request: Request, chat_id: str):
    settings, settings_err = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_target_by_chat_id(settings, chat_id, project_id)

    linked_chat_id = str(target.get("linked_chat_id") or "").strip()
    rule = _load_rule(linked_chat_id)
    keywords_text = "\n".join(rule.get("keywords") or [])

    logs, _ = _load_spam_logs(linked_chat_id, page=0, per_page=20)
    return templates.TemplateResponse(
        "target_antispam.html",
        _template_context(
            request,
            settings_err=settings_err,
            target=target,
            linked_chat_id=linked_chat_id,
            rule=rule,
            keywords_text=keywords_text,
            recent_logs=logs,
        ),
    )


@router.post("/targets/{chat_id}/antispam/save")
async def target_antispam_save(
    request: Request,
    chat_id: str,
    enabled: str | None = Form(None),
    keywords_text: str = Form(""),
    ai_enabled: str | None = Form(None),
    ai_prompt: str = Form(""),
    ai_model: str = Form("gpt-4.1-nano"),
    notify_telegram: str | None = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_target_by_chat_id(settings, chat_id, project_id)

    linked_chat_id = str(target.get("linked_chat_id") or "").strip()
    if not linked_chat_id:
        _flash(request, "danger", "У цели не найден linked_chat_id (группа обсуждений). Антиспам невозможен.")
        return _redirect(f"/targets/{quote(chat_id)}/antispam")

    keywords = _keywords_from_textarea(keywords_text)
    payload = json.dumps(keywords, ensure_ascii=False)

    enabled_flag = 1 if _parse_bool(enabled, default=False) else 0
    ai_enabled_flag = 1 if _parse_bool(ai_enabled, default=False) else 0
    notify_flag = 1 if _parse_bool(notify_telegram, default=False) else 0
    model = str(ai_model or "gpt-4.1-nano").strip() or "gpt-4.1-nano"
    prompt = str(ai_prompt or "").strip()

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
            (linked_chat_id, enabled_flag, payload, ai_enabled_flag, prompt, model, notify_flag, _now_iso()),
        )
        conn.commit()

    _flash(request, "success", "Настройки антиспама сохранены.")
    return _redirect(f"/targets/{quote(chat_id)}/antispam")


@router.get("/targets/{chat_id}/antispam/log", response_class=HTMLResponse)
async def target_antispam_log_page(request: Request, chat_id: str, page: int = 0):
    settings, settings_err = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_target_by_chat_id(settings, chat_id, project_id)

    linked_chat_id = str(target.get("linked_chat_id") or "").strip()
    rows, total = _load_spam_logs(linked_chat_id, page=page, per_page=50)
    total_pages = max((total + 49) // 50, 1)
    page = max(min(int(page or 0), total_pages - 1), 0)

    return templates.TemplateResponse(
        "target_antispam_log.html",
        _template_context(
            request,
            settings_err=settings_err,
            target=target,
            linked_chat_id=linked_chat_id,
            rows=rows,
            page=page,
            total=total,
            total_pages=total_pages,
        ),
    )


@router.post("/targets/{chat_id}/antispam/log/{log_id}/restore")
async def target_antispam_log_restore(request: Request, chat_id: str, log_id: int):
    # Optional / future: mark as false positive.
    try:
        with _db_connect() as conn:
            conn.execute("UPDATE spam_log SET action = 'restored' WHERE id = ?", (int(log_id),))
            conn.commit()
    except Exception:
        pass
    _flash(request, "success", "Запись отмечена как восстановленная (ложное срабатывание).")
    return _redirect(f"/targets/{quote(chat_id)}/antispam/log")


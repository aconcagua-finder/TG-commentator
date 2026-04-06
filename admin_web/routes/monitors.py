"""Monitor-targets routes."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse

from admin_web.helpers import (
    _active_project_id,
    _auto_pause_commentator,
    _filter_accounts_by_project,
    _filter_by_project,
    _find_monitor_target_by_chat_id,
    _flash,
    _load_accounts,
    _load_settings,
    _parse_int_field,
    _redirect,
    _save_settings,
)
from admin_web.telethon_utils import _derive_target_chat_info
from admin_web.templating import templates, _template_context

router = APIRouter()


@router.get("/monitor-targets", response_class=HTMLResponse)
async def monitor_targets_page(request: Request):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    targets = _filter_by_project(settings.get("monitor_targets", []) or [], project_id)
    targets_sorted = sorted(targets, key=lambda x: x.get("date_added", ""), reverse=True)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse(
        "monitor_targets.html",
        _template_context(request, targets=targets_sorted, accounts=accounts),
    )


@router.get("/monitor-targets/new", response_class=HTMLResponse)
async def monitor_targets_new_page(request: Request):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse("monitor_target_new.html", _template_context(request, accounts=accounts))


@router.post("/monitor-targets/new")
async def monitor_targets_new_submit(
    request: Request,
    chat_input: str = Form(...),
    notification_chat_id: str = Form(...),
    prompt: str = Form(...),
    daily_limit: str = Form("0"),
    min_word_count: str = Form("0"),
    min_post_interval_mins: str = Form("0"),
    ai_provider: str = Form("default"),
    slow_join_interval_mins: str = Form("0"),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    chat_input = chat_input.strip()
    async with _auto_pause_commentator(request, reason="Проверка/вступление в чат (мониторинг)"):
        try:
            chat_info = await _derive_target_chat_info(chat_input)
        except HTTPException as e:
            _flash(request, "danger", str(e.detail))
            return _redirect("/monitor-targets/new")

    chat_id = chat_info["chat_id"]
    existing_targets = _filter_by_project(settings.get("monitor_targets", []) or [], project_id)
    if any(str(t.get("chat_id")) == str(chat_id) for t in existing_targets):
        _flash(request, "warning", "Этот канал уже добавлен в мониторинг.")
        return _redirect(f"/monitor-targets/{quote(chat_id)}")

    try:
        notification_id = int(notification_chat_id.strip())
    except ValueError:
        _flash(request, "danger", "Чат уведомлений: укажите числовой chat_id (например: -100...).")
        return _redirect("/monitor-targets/new")

    new_target: Dict[str, Any] = {
        "chat_id": chat_info["chat_id"],
        "chat_username": chat_info.get("chat_username"),
        "chat_name": chat_info.get("chat_name"),
        "notification_chat_id": notification_id,
        "prompt": prompt.strip(),
        "daily_limit": _parse_int_field(request, daily_limit, default=0, label="Лимит/сутки", min_value=0),
        "min_word_count": _parse_int_field(request, min_word_count, default=0, label="Мин. слов", min_value=0),
        "min_post_interval_mins": _parse_int_field(
            request, min_post_interval_mins, default=0, label="Мин. интервал (мин)", min_value=0
        ),
        "slow_join_interval_mins": _parse_int_field(
            request, slow_join_interval_mins, default=0, label="Медленное вступление (мин)", min_value=0
        ),
        "ai_provider": ai_provider,
        "assigned_accounts": [],
        "date_added": datetime.now(timezone.utc).isoformat(),
        "project_id": project_id,
    }
    settings.setdefault("monitor_targets", []).append(new_target)
    _save_settings(settings)
    _flash(request, "success", "Канал мониторинга добавлен.")
    return _redirect(f"/monitor-targets/{quote(chat_id)}")


@router.get("/monitor-targets/{chat_id}", response_class=HTMLResponse)
async def monitor_target_edit_page(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_monitor_target_by_chat_id(settings, chat_id, project_id)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse(
        "monitor_target_edit.html",
        _template_context(request, target=target, accounts=accounts),
    )


@router.post("/monitor-targets/{chat_id}")
async def monitor_target_edit_save(
    request: Request,
    chat_id: str,
    notification_chat_id: str = Form(""),
    prompt: str = Form(""),
    daily_limit: str = Form(""),
    min_word_count: str = Form(""),
    min_post_interval_mins: str = Form(""),
    ai_provider: str = Form("default"),
    slow_join_interval_mins: str = Form(""),
    select_all: Optional[str] = Form(None),
    assigned_accounts: Optional[List[str]] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, target = _find_monitor_target_by_chat_id(settings, chat_id, project_id)

    if notification_chat_id.strip():
        try:
            target["notification_chat_id"] = int(notification_chat_id.strip())
        except ValueError:
            _flash(request, "warning", "Чат уведомлений: некорректное число, значение не изменено.")
    if prompt.strip():
        target["prompt"] = prompt.strip()
    if daily_limit.strip():
        target["daily_limit"] = _parse_int_field(
            request, daily_limit, default=int(target.get("daily_limit", 0)), label="Лимит/сутки", min_value=0
        )
    if min_word_count.strip():
        target["min_word_count"] = _parse_int_field(
            request, min_word_count, default=int(target.get("min_word_count", 0)), label="Мин. слов", min_value=0
        )
    if min_post_interval_mins.strip():
        target["min_post_interval_mins"] = _parse_int_field(
            request,
            min_post_interval_mins,
            default=int(target.get("min_post_interval_mins", 0)),
            label="Мин. интервал (мин)",
            min_value=0,
        )
    if slow_join_interval_mins.strip():
        target["slow_join_interval_mins"] = _parse_int_field(
            request,
            slow_join_interval_mins,
            default=int(target.get("slow_join_interval_mins", 0)),
            label="Медленное вступление (мин)",
            min_value=0,
        )
    target["ai_provider"] = ai_provider
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

    settings["monitor_targets"][idx] = target
    _save_settings(settings)
    _flash(request, "success", "Настройки мониторинга обновлены.")
    return _redirect(f"/monitor-targets/{quote(chat_id)}")


@router.post("/monitor-targets/{chat_id}/delete")
async def monitor_target_delete(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, _ = _find_monitor_target_by_chat_id(settings, chat_id, project_id)
    settings["monitor_targets"].pop(idx)
    _save_settings(settings)
    _flash(request, "success", "Канал мониторинга удалён.")
    return _redirect("/monitor-targets")

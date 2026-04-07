"""Reaction-targets routes."""

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
    _find_reaction_target_by_chat_id,
    _flash,
    _load_accounts,
    _load_settings,
    _parse_int_field,
    _redirect,
    _save_settings,
)
from admin_web.sort_helpers import apply_sort, resolve_key, template_options
from admin_web.telethon_utils import _derive_target_chat_info
from admin_web.templating import templates, _template_context

router = APIRouter()


@router.get("/reaction-targets", response_class=HTMLResponse)
async def reaction_targets_page(request: Request, sort: str = ""):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    targets = _filter_by_project(settings.get("reaction_targets", []) or [], project_id)
    sort_key = resolve_key(sort, "chat_target")
    targets_sorted = apply_sort(targets, sort_key, "chat_target")
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse(
        "reaction_targets.html",
        _template_context(
            request,
            targets=targets_sorted,
            accounts=accounts,
            sort_options=template_options("chat_target"),
            current_sort=sort_key,
        ),
    )


@router.get("/reaction-targets/new", response_class=HTMLResponse)
async def reaction_targets_new_page(request: Request):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse("reaction_target_new.html", _template_context(request, accounts=accounts))


@router.post("/reaction-targets/new")
async def reaction_targets_new_submit(
    request: Request,
    chat_input: str = Form(...),
    reactions: str = Form("👍"),
    reaction_count: str = Form("1"),
    reaction_chance: str = Form("80"),
    initial_reaction_delay: str = Form("10"),
    delay_between_reactions: str = Form("5"),
    daily_reaction_limit: str = Form("999"),
    slow_join_interval_mins: str = Form("0"),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)

    chat_input = chat_input.strip()
    async with _auto_pause_commentator(request, reason="Проверка/вступление в чат (реакции)"):
        try:
            base = await _derive_target_chat_info(chat_input)
        except HTTPException as e:
            _flash(request, "danger", str(e.detail))
            return _redirect("/reaction-targets/new")

    chat_id = base["chat_id"]
    existing_targets = _filter_by_project(settings.get("reaction_targets", []) or [], project_id)
    if any(str(t.get("chat_id")) == str(chat_id) for t in existing_targets):
        _flash(request, "warning", "Этот чат уже добавлен в цели реакций.")
        return _redirect(f"/reaction-targets/{quote(chat_id)}")

    reaction_list = [r for r in reactions.split() if r.strip()]
    if not reaction_list:
        _flash(request, "warning", "Список реакций пустой.")
        return _redirect("/reaction-targets/new")

    new_target: Dict[str, Any] = {
        **base,
        "reactions": reaction_list,
        "reaction_count": _parse_int_field(request, reaction_count, default=1, label="Кол-во реакций", min_value=1),
        "reaction_chance": _parse_int_field(
            request, reaction_chance, default=80, label="Шанс", min_value=0, max_value=100
        ),
        "initial_reaction_delay": _parse_int_field(
            request, initial_reaction_delay, default=10, label="Пауза после поста", min_value=0
        ),
        "delay_between_reactions": _parse_int_field(
            request, delay_between_reactions, default=5, label="Пауза между реакциями", min_value=0
        ),
        "daily_reaction_limit": _parse_int_field(
            request, daily_reaction_limit, default=999, label="Лимит/сутки", min_value=0
        ),
        "slow_join_interval_mins": _parse_int_field(
            request, slow_join_interval_mins, default=0, label="Медленное вступление (мин)", min_value=0
        ),
        "date_added": datetime.now(timezone.utc).isoformat(),
        "assigned_accounts": [],
        "project_id": project_id,
    }
    settings.setdefault("reaction_targets", []).append(new_target)
    _save_settings(settings)
    _flash(request, "success", f"Цель реакций добавлена: {base.get('chat_name')}")
    return _redirect(f"/reaction-targets/{quote(chat_id)}")


@router.get("/reaction-targets/{chat_id}", response_class=HTMLResponse)
async def reaction_target_edit_page(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_reaction_target_by_chat_id(settings, chat_id, project_id)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse(
        "reaction_target_edit.html",
        _template_context(request, target=target, accounts=accounts),
    )


@router.post("/reaction-targets/{chat_id}")
async def reaction_target_edit_save(
    request: Request,
    chat_id: str,
    reactions: str = Form(""),
    reaction_count: str = Form(""),
    reaction_chance: str = Form(""),
    initial_reaction_delay: str = Form(""),
    delay_between_reactions: str = Form(""),
    daily_reaction_limit: str = Form(""),
    slow_join_interval_mins: str = Form(""),
    select_all: Optional[str] = Form(None),
    assigned_accounts: Optional[List[str]] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, target = _find_reaction_target_by_chat_id(settings, chat_id, project_id)

    if reactions.strip():
        target["reactions"] = [r for r in reactions.split() if r.strip()]
    if reaction_count.strip():
        target["reaction_count"] = _parse_int_field(
            request, reaction_count, default=int(target.get("reaction_count", 1)), label="Кол-во реакций", min_value=1
        )
    if reaction_chance.strip():
        target["reaction_chance"] = _parse_int_field(
            request,
            reaction_chance,
            default=int(target.get("reaction_chance", 80)),
            label="Шанс",
            min_value=0,
            max_value=100,
        )
    if initial_reaction_delay.strip():
        target["initial_reaction_delay"] = _parse_int_field(
            request,
            initial_reaction_delay,
            default=int(target.get("initial_reaction_delay", 10)),
            label="Пауза после поста",
            min_value=0,
        )
    if delay_between_reactions.strip():
        target["delay_between_reactions"] = _parse_int_field(
            request,
            delay_between_reactions,
            default=int(target.get("delay_between_reactions", 5)),
            label="Пауза между реакциями",
            min_value=0,
        )
    if daily_reaction_limit.strip():
        target["daily_reaction_limit"] = _parse_int_field(
            request, daily_reaction_limit, default=int(target.get("daily_reaction_limit", 999)), label="Лимит/сутки", min_value=0
        )
    if slow_join_interval_mins.strip():
        target["slow_join_interval_mins"] = _parse_int_field(
            request,
            slow_join_interval_mins,
            default=int(target.get("slow_join_interval_mins", 0)),
            label="Медленное вступление (мин)",
            min_value=0,
        )

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
    settings["reaction_targets"][idx] = target
    _save_settings(settings)
    _flash(request, "success", "Настройки реакций обновлены.")
    return _redirect(f"/reaction-targets/{quote(chat_id)}")


@router.post("/reaction-targets/{chat_id}/delete")
async def reaction_target_delete(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, _ = _find_reaction_target_by_chat_id(settings, chat_id, project_id)
    settings["reaction_targets"].pop(idx)
    _save_settings(settings)
    _flash(request, "success", "Цель реакций удалена.")
    return _redirect("/reaction-targets")

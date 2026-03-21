"""Authentication and project management routes."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse

from admin_web.helpers import (
    DEFAULT_PROJECT_ID,
    ADMIN_WEB_USERNAME,
    ADMIN_WEB_PASSWORD,
    _load_settings,
    _save_settings,
    _load_accounts,
    _save_accounts,
    _active_project_id,
    _filter_by_project,
    _filter_accounts_by_project,
    _project_id_for,
    _delete_manual_tasks_for_project,
    _move_manual_tasks,
    _flash,
    _redirect,
)
from admin_web.templating import templates, _template_context

router = APIRouter()


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, next: str = "/"):
    return templates.TemplateResponse("login.html", _template_context(request, next=next))


@router.post("/login")
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    next: str = Form("/"),
):
    if username != ADMIN_WEB_USERNAME or password != ADMIN_WEB_PASSWORD:
        _flash(request, "danger", "Неверный логин или пароль.")
        return _redirect(f"/login?next={quote(next)}")

    request.session["user"] = {"username": username}
    _flash(request, "success", "Вы вошли в админку.")
    return _redirect(next or "/")


@router.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return _redirect("/login")


# ---------------------------------------------------------------------------
# Projects
# ---------------------------------------------------------------------------


@router.get("/projects", response_class=HTMLResponse)
async def projects_page(request: Request):
    settings, _ = _load_settings()
    accounts, _ = _load_accounts()
    project_id = _active_project_id(settings)

    project_stats: Dict[str, Dict[str, int]] = {}
    accounts_by_project: Dict[str, List[Dict[str, Any]]] = {}
    targets_by_project: Dict[str, List[Dict[str, Any]]] = {}
    discussion_by_project: Dict[str, List[Dict[str, Any]]] = {}
    reaction_by_project: Dict[str, List[Dict[str, Any]]] = {}
    monitor_by_project: Dict[str, List[Dict[str, Any]]] = {}
    for p in settings.get("projects", []) or []:
        pid = p.get("id")
        if not pid:
            continue
        accounts_by_project[pid] = _filter_accounts_by_project(accounts, pid)
        targets_by_project[pid] = _filter_by_project(settings.get("targets", []) or [], pid)
        discussion_by_project[pid] = _filter_by_project(settings.get("discussion_targets", []) or [], pid)
        reaction_by_project[pid] = _filter_by_project(settings.get("reaction_targets", []) or [], pid)
        monitor_by_project[pid] = _filter_by_project(settings.get("monitor_targets", []) or [], pid)
        project_stats[pid] = {
            "accounts": len(_filter_accounts_by_project(accounts, pid)),
            "targets": len(_filter_by_project(settings.get("targets", []) or [], pid)),
            "discussion_targets": len(_filter_by_project(settings.get("discussion_targets", []) or [], pid)),
            "reaction_targets": len(_filter_by_project(settings.get("reaction_targets", []) or [], pid)),
            "monitor_targets": len(_filter_by_project(settings.get("monitor_targets", []) or [], pid)),
        }

    return templates.TemplateResponse(
        "projects.html",
        _template_context(
            request,
            projects=settings.get("projects", []) or [],
            project_stats=project_stats,
            accounts_by_project=accounts_by_project,
            targets_by_project=targets_by_project,
            discussion_by_project=discussion_by_project,
            reaction_by_project=reaction_by_project,
            monitor_by_project=monitor_by_project,
            active_project_id=project_id,
            default_project_id=DEFAULT_PROJECT_ID,
        ),
    )


@router.post("/projects/new")
async def projects_new(request: Request, name: str = Form(...)):
    name = name.strip()
    if not name:
        _flash(request, "warning", "Название проекта не может быть пустым.")
        return _redirect("/projects")

    settings, _ = _load_settings()
    project_id = uuid.uuid4().hex[:8]
    while any(p.get("id") == project_id for p in settings.get("projects", []) or []):
        project_id = uuid.uuid4().hex[:8]

    settings.setdefault("projects", []).append(
        {"id": project_id, "name": name, "created_at": datetime.now(timezone.utc).isoformat()}
    )
    settings["active_project_id"] = project_id
    _save_settings(settings)
    _flash(request, "success", f"Проект «{name}» создан и выбран.")
    return _redirect("/projects")


@router.post("/projects/select")
async def projects_select(request: Request, project_id: str = Form(...)):
    settings, _ = _load_settings()
    project_id = (project_id or "").strip()
    if not project_id or not any(p.get("id") == project_id for p in settings.get("projects", []) or []):
        _flash(request, "warning", "Проект не найден.")
        return _redirect("/")
    settings["active_project_id"] = project_id
    _save_settings(settings)
    _flash(request, "success", "Проект переключён.")
    return _redirect(request.headers.get("referer") or "/")


@router.post("/projects/{project_id}/rename")
async def projects_rename(request: Request, project_id: str, name: str = Form(...)):
    name = name.strip()
    if not name:
        _flash(request, "warning", "Название проекта не может быть пустым.")
        return _redirect("/projects")
    settings, _ = _load_settings()
    for p in settings.get("projects", []) or []:
        if p.get("id") == project_id:
            p["name"] = name
            _save_settings(settings)
            _flash(request, "success", "Название проекта обновлено.")
            return _redirect("/projects")
    _flash(request, "warning", "Проект не найден.")
    return _redirect("/projects")


@router.post("/projects/{project_id}/delete")
async def projects_delete(request: Request, project_id: str):
    if project_id == DEFAULT_PROJECT_ID:
        _flash(request, "warning", "Стандартный проект удалить нельзя.")
        return _redirect("/projects")

    settings, _ = _load_settings()
    projects = [p for p in settings.get("projects", []) or [] if p.get("id") != project_id]
    if len(projects) == len(settings.get("projects", []) or []):
        _flash(request, "warning", "Проект не найден.")
        return _redirect("/projects")

    settings["projects"] = projects
    for key in ("targets", "discussion_targets", "reaction_targets", "monitor_targets"):
        items = settings.get(key, []) or []
        settings[key] = [t for t in items if _project_id_for(t) != project_id]
    deleted_manual_tasks = _delete_manual_tasks_for_project(project_id)

    if settings.get("active_project_id") == project_id:
        settings["active_project_id"] = DEFAULT_PROJECT_ID

    _save_settings(settings)

    accounts, _ = _load_accounts()
    accounts = [a for a in accounts if _project_id_for(a) != project_id]
    _save_accounts(accounts)

    _flash(request, "success", f"Проект удалён. Ручные задачи очищены: {deleted_manual_tasks}.")
    return _redirect("/projects")


@router.post("/projects/move")
async def projects_move(
    request: Request,
    source_project_id: str = Form(...),
    dest_project_id: str = Form(...),
    move_accounts: Optional[str] = Form(None),
    move_targets: Optional[str] = Form(None),
    move_reaction_targets: Optional[str] = Form(None),
    move_monitor_targets: Optional[str] = Form(None),
    move_manual_queue: Optional[str] = Form(None),
    selected_accounts: Optional[List[str]] = Form(None),
    selected_targets: Optional[List[str]] = Form(None),
    selected_reaction_targets: Optional[List[str]] = Form(None),
    selected_monitor_targets: Optional[List[str]] = Form(None),
):
    source_project_id = (source_project_id or "").strip()
    dest_project_id = (dest_project_id or "").strip()
    if not source_project_id or not dest_project_id:
        _flash(request, "warning", "Нужно выбрать проекты.")
        return _redirect("/projects")
    if source_project_id == dest_project_id:
        _flash(request, "warning", "Проекты совпадают — перемещение не требуется.")
        return _redirect("/projects")

    settings, _ = _load_settings()
    project_ids = {p.get("id") for p in settings.get("projects", []) or [] if p.get("id")}
    if source_project_id not in project_ids or dest_project_id not in project_ids:
        _flash(request, "warning", "Проект не найден.")
        return _redirect("/projects")

    if not any([move_accounts, move_targets, move_reaction_targets, move_monitor_targets, move_manual_queue]):
        _flash(request, "warning", "Выберите, что переносить.")
        return _redirect("/projects")

    accounts, _ = _load_accounts()

    moved_accounts = 0
    moved_targets = 0
    moved_reactions = 0
    moved_monitor = 0
    moved_manual = 0

    if move_accounts:
        selected_set = {s for s in (selected_accounts or []) if s}
        for acc in accounts:
            if _project_id_for(acc) != source_project_id:
                continue
            if selected_set and acc.get("session_name") not in selected_set:
                continue
            acc["project_id"] = dest_project_id
            moved_accounts += 1

    moved_comment_targets: List[Dict[str, Any]] = []
    moved_reaction_targets_list: List[Dict[str, Any]] = []
    moved_monitor_targets_list: List[Dict[str, Any]] = []

    if move_targets:
        selected_set = {s for s in (selected_targets or []) if s}
        for t in settings.get("targets", []) or []:
            if _project_id_for(t) != source_project_id:
                continue
            if selected_set and str(t.get("chat_id")) not in selected_set:
                continue
            t["project_id"] = dest_project_id
            moved_targets += 1
            moved_comment_targets.append(t)

    if move_reaction_targets:
        selected_set = {s for s in (selected_reaction_targets or []) if s}
        for t in settings.get("reaction_targets", []) or []:
            if _project_id_for(t) != source_project_id:
                continue
            if selected_set and str(t.get("chat_id")) not in selected_set:
                continue
            t["project_id"] = dest_project_id
            moved_reactions += 1
            moved_reaction_targets_list.append(t)

    if move_monitor_targets:
        selected_set = {s for s in (selected_monitor_targets or []) if s}
        for t in settings.get("monitor_targets", []) or []:
            if _project_id_for(t) != source_project_id:
                continue
            if selected_set and str(t.get("chat_id")) not in selected_set:
                continue
            t["project_id"] = dest_project_id
            moved_monitor += 1
            moved_monitor_targets_list.append(t)

    if move_manual_queue:
        moved_manual += _move_manual_tasks(source_project_id, dest_project_id)

    dest_sessions = {
        a.get("session_name")
        for a in accounts
        if a.get("session_name") and _project_id_for(a) == dest_project_id
    }

    for t in moved_comment_targets:
        assigned = [s for s in (t.get("assigned_accounts") or []) if s in dest_sessions]
        t["assigned_accounts"] = assigned

    for t in moved_reaction_targets_list:
        assigned = [s for s in (t.get("assigned_accounts") or []) if s in dest_sessions]
        t["assigned_accounts"] = assigned

    for t in moved_monitor_targets_list:
        assigned = [s for s in (t.get("assigned_accounts") or []) if s in dest_sessions]
        t["assigned_accounts"] = assigned

    _save_settings(settings)
    _save_accounts(accounts)

    _flash(
        request,
        "success",
        "Перемещено: "
        f"аккаунтов {moved_accounts}, "
        f"комментирование {moved_targets}, "
        f"реакции {moved_reactions}, "
        f"мониторинг {moved_monitor}, "
        f"ручные задачи {moved_manual}.",
    )
    return _redirect("/projects")

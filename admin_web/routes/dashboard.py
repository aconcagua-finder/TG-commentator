"""Dashboard, guide, warnings, and status routes."""

from __future__ import annotations

from typing import Any, Dict, List

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from admin_web.helpers import (
    _load_settings,
    _save_settings,
    _load_accounts,
    _active_project_id,
    _filter_by_project,
    _filter_accounts_by_project,
    _db_connect,
    _collect_warnings,
    _collect_warnings_for_scope,
    _sync_warning_history,
    _load_resolved_warning_history,
    _load_seen_warning_keys,
    _mark_warning_keys_seen,
    _flash,
    _redirect,
)
from admin_web.templating import templates, _template_context

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    settings, settings_err = _load_settings()
    accounts, accounts_err = _load_accounts()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    targets = _filter_by_project(settings.get("targets") or [], project_id)
    reaction_targets = _filter_by_project(settings.get("reaction_targets") or [], project_id)
    monitor_targets = _filter_by_project(settings.get("monitor_targets") or [], project_id)

    with _db_connect() as conn:
        proxies_total = conn.execute("SELECT COUNT(*) AS c FROM proxies").fetchone()["c"]
        proxies_active = conn.execute("SELECT COUNT(*) AS c FROM proxies WHERE status='active'").fetchone()["c"]
        triggers_total = conn.execute("SELECT COUNT(*) AS c FROM triggers").fetchone()["c"]
        scenarios_total = conn.execute("SELECT COUNT(*) AS c FROM scenarios").fetchone()["c"]
        recent_logs = [
            dict(r)
            for r in conn.execute(
                "SELECT timestamp, account_session_name, destination_chat_id, content "
                "FROM logs ORDER BY id DESC LIMIT 5"
            ).fetchall()
        ]

    return templates.TemplateResponse(
        "dashboard.html",
        _template_context(
            request,
            settings=settings,
            settings_err=settings_err,
            accounts=accounts,
            accounts_err=accounts_err,
            targets=targets,
            reaction_targets=reaction_targets,
            monitor_targets=monitor_targets,
            proxies_total=proxies_total,
            proxies_active=proxies_active,
            triggers_total=triggers_total,
            scenarios_total=scenarios_total,
            recent_logs=recent_logs,
        ),
    )


@router.get("/guide", response_class=HTMLResponse)
async def guide_page(request: Request):
    settings, _ = _load_settings()
    accounts, _ = _load_accounts()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)

    provider = settings.get("ai_provider", "deepseek")
    api_keys = settings.get("api_keys", {}) or {}
    ai_ready = bool(api_keys.get(provider))

    accounts_ready = len(accounts) > 0
    targets_ready = len(_filter_by_project(settings.get("targets", []) or [], project_id)) > 0
    running_ready = settings.get("status") == "running"

    with _db_connect() as conn:
        proxies_active = conn.execute("SELECT COUNT(*) AS c FROM proxies WHERE status='active'").fetchone()["c"]

    steps = [
        {
            "title": "1) Укажите AI провайдера и API ключ",
            "hint": f"Сейчас выбран {provider.upper()}. Нужен ключ для выбранного провайдера, иначе генерация не пойдёт.",
            "done": ai_ready,
            "optional": False,
            "href": "/settings/ai",
            "action": "Открыть AI",
        },
        {
            "title": "2) Добавьте аккаунт(ы) Telegram",
            "hint": "Через session string или вход по телефону. Проверьте статусы, настройте сон/прокси по необходимости.",
            "done": accounts_ready,
            "optional": False,
            "href": "/accounts",
            "action": "Открыть аккаунты",
        },
        {
            "title": "3) Добавьте 1–2 цели комментирования",
            "hint": "Укажите канал/чат, промпт по умолчанию, задержки и лимиты. Дальше можно настроить триггеры/сценарии.",
            "done": targets_ready,
            "optional": False,
            "href": "/targets",
            "action": "Открыть цели",
        },
        {
            "title": "4) (Опционально) Добавьте прокси и назначьте на аккаунты",
            "hint": "Полезно для распределения трафика и стабильности. Прокси проверяются и хранятся в базе.",
            "done": proxies_active > 0,
            "optional": True,
            "href": "/proxies",
            "action": "Открыть прокси",
        },
        {
            "title": "5) Запустите комментатор",
            "hint": "После старта скрипт подхватывает настройки и начинает обработку новых постов.",
            "done": running_ready,
            "optional": False,
            "href": "/",
            "action": "На дашборд",
        },
        {
            "title": "6) Проверьте работу и смотрите статистику",
            "hint": "Статистика показывает, кто и где оставлял комментарии/реакции. Можно выгрузить в Excel.",
            "done": False,
            "optional": True,
            "href": "/stats",
            "action": "Открыть статистику",
        },
    ]

    return templates.TemplateResponse("guide.html", _template_context(request, steps=steps))


@router.get("/warnings", response_class=HTMLResponse)
async def warnings_page(request: Request):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    _sync_warning_history(_collect_warnings_for_scope(accounts, settings, project_id=None))
    warnings = _collect_warnings(accounts, settings)
    project_id = _active_project_id(settings)
    active_sessions = [
        str(account.get("session_name")).strip()
        for account in _filter_accounts_by_project(accounts, project_id)
        if account.get("session_name")
    ]
    keys = [w.get("key") for w in warnings if w.get("key")]
    seen = _load_seen_warning_keys(keys)
    for w in warnings:
        key = w.get("key")
        if key:
            w["is_new"] = key not in seen
    _mark_warning_keys_seen(keys)
    resolved_warnings = _load_resolved_warning_history(active_sessions, limit=50)
    return templates.TemplateResponse(
        "warnings.html",
        _template_context(request, warnings=warnings, resolved_warnings=resolved_warnings),
    )


@router.post("/status/start")
async def status_start(request: Request):
    settings, _ = _load_settings()
    settings["status"] = "running"
    _save_settings(settings)
    _flash(request, "success", "Комментатор запущен (commentator.py подхватит в течение нескольких секунд).")
    return _redirect("/")


@router.post("/status/stop")
async def status_stop(request: Request):
    settings, _ = _load_settings()
    settings["status"] = "stopped"
    _save_settings(settings)
    _flash(request, "success", "Комментатор остановлен.")
    return _redirect("/")

"""Telegram Bot notification helpers."""

from __future__ import annotations

import html
import logging
from copy import deepcopy
from typing import Any, Dict

import httpx

from app_paths import ACCOUNTS_FILE, SETTINGS_FILE
from app_storage import load_json

DEFAULT_PROJECT_ID = "default"

TELEGRAM_BOT_EVENT_OPTIONS: tuple[tuple[str, str], ...] = (
    ("warnings", "Предупреждения системы"),
    ("inbox_dm", "Входящие личные сообщения"),
    ("inbox_replies", "Реплаи на комментарии бота"),
    ("inbox_reactions", "Реакции на сообщения бота"),
    ("monitoring", "Срабатывания мониторинга каналов"),
)

TELEGRAM_BOT_EVENT_DEFAULTS: Dict[str, bool] = {
    "warnings": True,
    "inbox_dm": True,
    "inbox_replies": False,
    "inbox_reactions": False,
    "monitoring": True,
}

logger = logging.getLogger(__name__)


def default_telegram_bot_settings() -> Dict[str, Any]:
    return {
        "enabled": False,
        "bot_token": "",
        "chat_id": "",
        "chat_title": "",
        "chat_username": "",
        "events": dict(TELEGRAM_BOT_EVENT_DEFAULTS),
    }


def normalize_telegram_bot_settings(value: Any) -> Dict[str, Any]:
    base = default_telegram_bot_settings()
    if not isinstance(value, dict):
        return base

    events_raw = value.get("events")
    events: Dict[str, bool] = {}
    if isinstance(events_raw, dict):
        for key, default in TELEGRAM_BOT_EVENT_DEFAULTS.items():
            events[key] = bool(events_raw.get(key, default))
    else:
        events = dict(TELEGRAM_BOT_EVENT_DEFAULTS)

    return {
        "enabled": bool(value.get("enabled", False)),
        "bot_token": str(value.get("bot_token") or "").strip(),
        "chat_id": str(value.get("chat_id") or "").strip(),
        "chat_title": str(value.get("chat_title") or "").strip(),
        "chat_username": str(value.get("chat_username") or "").strip().lstrip("@"),
        "events": events,
    }


def _settings_dict(settings: Any) -> Dict[str, Any]:
    return settings if isinstance(settings, dict) else {}


def _projects_list(settings: Dict[str, Any]) -> list[dict[str, Any]]:
    projects = settings.get("projects")
    if not isinstance(projects, list):
        projects = []
        settings["projects"] = projects
    return [project for project in projects if isinstance(project, dict)]


def _project_id(project_id: str | None, settings: Dict[str, Any]) -> str:
    pid = str(project_id or settings.get("active_project_id") or "").strip()
    return pid or DEFAULT_PROJECT_ID


def get_project_telegram_bot_settings(settings: Any, project_id: str | None = None) -> Dict[str, Any]:
    settings_dict = _settings_dict(settings)
    pid = _project_id(project_id, settings_dict)
    for project in _projects_list(settings_dict):
        if str(project.get("id") or "").strip() == pid:
            return normalize_telegram_bot_settings(project.get("telegram_bot"))
    return default_telegram_bot_settings()


def set_project_telegram_bot_settings(settings: Any, project_id: str | None, value: Any) -> Dict[str, Any]:
    settings_dict = _settings_dict(settings)
    pid = _project_id(project_id, settings_dict)
    normalized = normalize_telegram_bot_settings(value)
    for project in _projects_list(settings_dict):
        if str(project.get("id") or "").strip() == pid:
            project["telegram_bot"] = deepcopy(normalized)
            return normalized
    raise KeyError(f"Project not found: {pid}")


def _load_runtime_settings() -> Dict[str, Any]:
    settings = load_json(SETTINGS_FILE, {})
    return settings if isinstance(settings, dict) else {}


def _load_accounts() -> list[dict[str, Any]]:
    accounts = load_json(ACCOUNTS_FILE, [])
    if not isinstance(accounts, list):
        return []
    return [account for account in accounts if isinstance(account, dict)]


def resolve_project_id_for_session(session_name: str, settings: dict) -> str:
    session_name = str(session_name or "").strip()
    if not session_name:
        return DEFAULT_PROJECT_ID
    for account in _load_accounts():
        if str(account.get("session_name") or "").strip() != session_name:
            continue
        project_id = str(account.get("project_id") or "").strip()
        return project_id or DEFAULT_PROJECT_ID
    return DEFAULT_PROJECT_ID


def escape_html(text) -> str:
    return html.escape(str(text or ""), quote=False)


def _truncate_text(text: Any, *, limit: int = 200) -> str:
    raw = str(text or "").strip()
    if len(raw) <= limit:
        return raw
    if limit <= 1:
        return raw[:limit]
    return f"{raw[: limit - 1].rstrip()}…"


def _compose_person(name: Any, username: Any = "") -> str:
    safe_name = str(name or "").strip()
    safe_username = str(username or "").strip().lstrip("@")
    if safe_name and safe_username:
        return f"{safe_name} (@{safe_username})"
    if safe_name:
        return safe_name
    if safe_username:
        return f"@{safe_username}"
    return "—"


def _join_notification_lines(*lines: Any) -> str:
    return "\n".join(str(line) for line in lines if str(line or "").strip())


def build_inbox_dm_notification(*, session_name: str, sender_name: str, sender_username: str, text: str) -> str:
    return _join_notification_lines(
        "<b>📩 Входящее ЛС</b>",
        f"<b>Аккаунт:</b> {escape_html(session_name)}",
        f"<b>От:</b> {escape_html(_compose_person(sender_name, sender_username))}",
        f"<b>Текст:</b> {escape_html(_truncate_text(text))}",
    )


def build_inbox_reply_notification(*, session_name: str, chat_title: str, sender_name: str, text: str) -> str:
    return _join_notification_lines(
        "<b>💬 Реплай на комментарий</b>",
        f"<b>Аккаунт:</b> {escape_html(session_name)}",
        f"<b>Чат:</b> {escape_html(chat_title or '—')}",
        f"<b>От:</b> {escape_html(sender_name or '—')}",
        f"<b>Текст:</b> {escape_html(_truncate_text(text))}",
    )


def build_reaction_notification(*, session_name: str, chat_title: str, summary: str) -> str:
    return _join_notification_lines(
        "<b>👍 Реакция на сообщение</b>",
        f"<b>Аккаунт:</b> {escape_html(session_name)}",
        f"<b>Чат:</b> {escape_html(chat_title or '—')}",
        f"<b>Реакция:</b> {escape_html(summary or '—')}",
    )


def build_monitoring_notification(*, chat_name: str, post_link: str) -> str:
    return _join_notification_lines(
        "<b>📡 Мониторинг: найден пост</b>",
        f"<b>Канал:</b> {escape_html(chat_name or '—')}",
        f"<b>Ссылка:</b> {escape_html(post_link or '—')}",
    )


def build_warning_notification(*, title: str, detail: str, session_name: str) -> str:
    return _join_notification_lines(
        f"<b>⚠️ {escape_html(title)}</b>",
        escape_html(detail) if str(detail or "").strip() else "",
        f"<b>Аккаунт:</b> {escape_html(session_name or '—')}",
    )


async def send_notification(bot_token, chat_id, text, parse_mode="HTML") -> dict:
    token = str(bot_token or "").strip()
    chat = str(chat_id or "").strip()
    message_text = str(text or "").strip()
    if not token or not chat or not message_text:
        return {"ok": False, "error": "missing_credentials"}

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload: Dict[str, Any] = {
        "chat_id": chat,
        "text": message_text,
    }
    if parse_mode:
        payload["parse_mode"] = str(parse_mode)

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(url, json=payload)

        try:
            data = response.json()
        except Exception:
            data = {"ok": False, "description": response.text}

        if response.is_success and isinstance(data, dict) and data.get("ok") is True:
            return data

        description = ""
        if isinstance(data, dict):
            description = str(data.get("description") or "").strip()
        if not description:
            description = response.text.strip() or f"HTTP {response.status_code}"
        logger.warning("Telegram Bot API sendMessage failed: status=%s, description=%s", response.status_code, description)
        return {
            "ok": False,
            "status_code": response.status_code,
            "description": description,
        }
    except Exception as exc:
        logger.warning("Telegram Bot API sendMessage exception: %s", exc)
        return {"ok": False, "error": str(exc)}


async def send_test_message(bot_token, chat_id) -> dict:
    return await send_notification(bot_token, chat_id, "Бот подключен!")


async def notify_event(event_type, project_id, message_text, settings=None):
    settings_dict = _settings_dict(settings) if settings is not None else _load_runtime_settings()
    bot_settings = get_project_telegram_bot_settings(settings_dict, project_id)
    if not bot_settings.get("enabled"):
        return {"ok": False, "skipped": True, "reason": "disabled"}

    events = bot_settings.get("events") if isinstance(bot_settings.get("events"), dict) else {}
    if not bool(events.get(str(event_type or "").strip(), False)):
        return {"ok": False, "skipped": True, "reason": "event_disabled"}

    if not bot_settings.get("bot_token") or not bot_settings.get("chat_id"):
        return {"ok": False, "skipped": True, "reason": "not_configured"}

    return await send_notification(
        bot_settings.get("bot_token"),
        bot_settings.get("chat_id"),
        str(message_text or ""),
    )

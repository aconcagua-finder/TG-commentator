"""Periodic Telegram bot notifications for new warnings."""

from __future__ import annotations

import logging
import os
import time
from typing import Any
from urllib.parse import quote

from admin_web.helpers import (
    DEFAULT_PROJECT_ID,
    _collect_warnings_for_scope,
    _db_connect,
    _load_accounts,
    _sync_warning_history,
)
from services.telegram_bot import (
    build_warning_notification,
    get_project_telegram_bot_settings,
    notify_event,
)

logger = logging.getLogger(__name__)

WARNING_NOTIFICATION_INTERVAL_SECONDS = max(30, int(os.getenv("TELEGRAM_WARNING_NOTIFICATION_INTERVAL_SECONDS", "300")))

_last_warning_notification_run_ts = 0.0


def _project_ids_with_warning_notifications_enabled(settings: dict[str, Any]) -> list[str]:
    projects = settings.get("projects")
    if not isinstance(projects, list) or not projects:
        projects = [{"id": DEFAULT_PROJECT_ID}]

    result: list[str] = []
    seen: set[str] = set()
    for project in projects:
        if not isinstance(project, dict):
            continue
        project_id = str(project.get("id") or "").strip() or DEFAULT_PROJECT_ID
        if project_id in seen:
            continue
        seen.add(project_id)
        bot_settings = get_project_telegram_bot_settings(settings, project_id)
        events = bot_settings.get("events") if isinstance(bot_settings.get("events"), dict) else {}
        if bool(bot_settings.get("enabled")) and bool(events.get("warnings")):
            result.append(project_id)
    return result


def _existing_warning_keys(keys: list[str]) -> set[str]:
    keys = [str(key).strip() for key in keys if str(key or "").strip()]
    if not keys:
        return set()
    placeholders = ",".join("?" for _ in keys)
    with _db_connect() as conn:
        rows = conn.execute(
            f"SELECT key FROM warning_history WHERE key IN ({placeholders})",
            tuple(keys),
        ).fetchall()
    return {str(row["key"]).strip() for row in rows if row and row["key"]}


def _warning_detail_text(warning: dict[str, Any]) -> str:
    detail_lines = warning.get("detail_lines")
    if isinstance(detail_lines, list):
        normalized = [str(line).strip() for line in detail_lines if str(line or "").strip()]
        if normalized:
            return "\n".join(normalized)
    return str(warning.get("detail") or "").strip()


def _admin_base_url(settings: dict[str, Any], project_id: str) -> str | None:
    candidates: list[Any] = [
        settings.get("admin_base_url"),
        settings.get("admin_url"),
        settings.get("base_url"),
    ]
    projects = settings.get("projects")
    if isinstance(projects, list):
        for project in projects:
            if not isinstance(project, dict):
                continue
            if str(project.get("id") or "").strip() != str(project_id or "").strip():
                continue
            candidates.extend(
                [
                    project.get("admin_base_url"),
                    project.get("admin_url"),
                    project.get("base_url"),
                ]
            )
            break

    for candidate in candidates:
        raw = str(candidate or "").strip().rstrip("/")
        if raw.startswith("http://") or raw.startswith("https://"):
            return raw
    return None


async def check_warning_notifications(*, current_settings: dict[str, Any]) -> int:
    settings = current_settings if isinstance(current_settings, dict) else {}
    accounts, _ = _load_accounts()
    sent = 0

    for project_id in _project_ids_with_warning_notifications_enabled(settings):
        warnings = _collect_warnings_for_scope(accounts, settings, project_id=project_id)
        known_keys = _existing_warning_keys([str(w.get("key") or "") for w in warnings])
        base_url = _admin_base_url(settings, project_id)
        for warning in warnings:
            key = str(warning.get("key") or "").strip()
            if not key or key in known_keys:
                continue
            session_name = str(warning.get("session_name") or "").strip()
            action_url = f"{base_url}/accounts/{quote(session_name)}" if (base_url and session_name) else None
            try:
                await notify_event(
                    "warnings",
                    project_id,
                    build_warning_notification(
                        title=str(warning.get("title") or ""),
                        detail=_warning_detail_text(warning),
                        session_name=session_name,
                        action_url=action_url,
                    ),
                    settings=settings,
                )
                sent += 1
            except Exception as exc:
                logger.warning("Telegram bot warning notification failed: project_id=%s key=%s error=%s", project_id, key, exc)
            known_keys.add(key)

    _sync_warning_history(_collect_warnings_for_scope(accounts, settings, project_id=None))
    return sent


async def process_warning_notifications(*, current_settings: dict[str, Any]) -> int:
    global _last_warning_notification_run_ts

    now = time.monotonic()
    if _last_warning_notification_run_ts and now - _last_warning_notification_run_ts < WARNING_NOTIFICATION_INTERVAL_SECONDS:
        return 0

    _last_warning_notification_run_ts = now
    try:
        return await check_warning_notifications(current_settings=current_settings)
    except Exception as exc:
        logger.warning("Warning notifier iteration failed: %s", exc)
        return 0

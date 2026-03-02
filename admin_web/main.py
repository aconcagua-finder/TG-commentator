from __future__ import annotations

import asyncio
import base64
import configparser
import html
import io
import json
import logging
import os
import re
import sqlite3
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import quote

import httpx
import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from google import genai
import openai
from starlette.middleware.sessions import SessionMiddleware

from app_paths import ACCOUNTS_FILE, CONFIG_FILE, DATA_DIR, DB_FILE, SETTINGS_FILE, ensure_data_dir
from app_storage import load_json, load_json_with_error, save_json

from telethon import TelegramClient
from telethon.errors import (
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    RPCError,
    SessionPasswordNeededError,
    UserDeactivatedBanError,
    UserAlreadyParticipantError,
)
from telethon.sessions import StringSession
from telethon.tl.functions.channels import (
    GetChannelRecommendationsRequest,
    GetFullChannelRequest,
    JoinChannelRequest,
)
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
from telethon.tl.types import InputPeerChannel, PeerChannel

from tg_device import device_kwargs, ensure_device_profile
from role_engine import (
    ACCOUNT_CUSTOM_ROLE_KEY,
    CUSTOM_ROLE_ID,
    CUSTOM_ROLE_NAME,
    DEFAULT_ROLE_ID,
    EMOJI_LEVELS,
    GENDER_OPTIONS,
    ROLE_PRESET_CATEGORIES,
    ensure_accounts_have_roles,
    ensure_role_schema,
    legacy_role_id,
    random_role_profile,
    role_for_account,
    role_presets_for_category,
)

ROOT_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = ROOT_DIR / "templates"
STATIC_DIR = ROOT_DIR / "static"
ACCOUNTS_DIR = Path(os.getenv("APP_ACCOUNTS_DIR", str(ROOT_DIR.parent / "accounts")))
try:
    STATIC_VERSION = str(int((STATIC_DIR / "app.css").stat().st_mtime))
except Exception:
    STATIC_VERSION = "1"

logger = logging.getLogger("admin_web")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return float(default)
    raw = raw.strip()
    if raw == "":
        return float(default)
    try:
        return float(raw)
    except ValueError:
        return float(default)


ADMIN_WEB_DISABLE_AUTH = _env_bool("ADMIN_WEB_DISABLE_AUTH", False)
ADMIN_WEB_USERNAME = os.getenv("ADMIN_WEB_USERNAME", "admin")
ADMIN_WEB_PASSWORD = os.getenv("ADMIN_WEB_PASSWORD", "admin")
ADMIN_WEB_SECRET = os.getenv("ADMIN_WEB_SECRET") or base64.urlsafe_b64encode(os.urandom(32)).decode("utf-8")

DEFAULT_MODELS: Dict[str, str] = {
    "openai_chat": "gpt-5.2-chat-latest",
    "openai_eval": "gpt-5.2",
    "openai_image": "gpt-image-1",
    "openrouter_chat": "x-ai/grok-4.1-fast",
    "openrouter_eval": "openai/gpt-4.1-mini",
    "deepseek_chat": "deepseek-chat",
    "deepseek_eval": "deepseek-chat",
    "gemini_chat": "gemini-3-flash-preview",
    "gemini_eval": "gemini-3-flash-preview",
    "gemini_names": "gemini-3-flash-preview",
}

DEFAULT_PROJECT_ID = "default"
DEFAULT_PROJECT_NAME = "Стандартный проект"
WARNING_FAILURE_THRESHOLD = 3
ACCOUNT_CHECKS_ENABLED = _env_bool("ADMIN_WEB_ACCOUNT_CHECKS_ENABLED", True)
FROZEN_ACCOUNT_PROBE_INVITE_HASH = os.getenv("ADMIN_WEB_FROZEN_ACCOUNT_PROBE_INVITE_HASH", "AAAAAAAAAAAAAAAAAAAA")
ADMIN_WEB_TELETHON_TIMEOUT_SECONDS = _env_float("ADMIN_WEB_TELETHON_TIMEOUT_SECONDS", 25.0)
ADMIN_WEB_TELETHON_TOTAL_TIMEOUT_SECONDS = _env_float("ADMIN_WEB_TELETHON_TOTAL_TIMEOUT_SECONDS", 45.0)


def _deadline_timeout(deadline: float, *, default: float) -> float:
    remaining = float(deadline - time.monotonic())
    if remaining <= 0:
        raise TimeoutError("total_timeout")
    return max(0.1, min(float(default), remaining))


def _mask_secret(value: str | None) -> str:
    if not value:
        return "не задан"
    if len(value) <= 8:
        return "ключ задан"
    return f"{value[:4]}...{value[-4:]}"


def _parse_int(value: str | None, *, default: int | None = None) -> int | None:
    if value is None:
        return default
    value = value.strip()
    if value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _parse_bool(value: str | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raw = str(value).strip().lower()
    if raw == "":
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def _parse_iso_ts(value: Any) -> float | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except Exception:
        return None


def _wants_html(request: Request) -> bool:
    accept = (request.headers.get("accept") or "").lower()
    return "text/html" in accept or "*/*" in accept or accept == ""


def _parse_int_field(
    request: Request,
    value: str | None,
    *,
    default: int,
    label: str,
    min_value: int | None = None,
    max_value: int | None = None,
) -> int:
    raw = (value or "").strip().replace(",", ".")
    if raw == "":
        return default
    try:
        n = int(float(raw))
    except ValueError:
        _flash(request, "warning", f"Поле «{label}»: некорректное число. Использую {default}.")
        return default
    if min_value is not None and n < min_value:
        _flash(request, "warning", f"Поле «{label}»: минимум {min_value}. Исправлено.")
        n = min_value
    if max_value is not None and n > max_value:
        _flash(request, "warning", f"Поле «{label}»: максимум {max_value}. Исправлено.")
        n = max_value
    return n


def _parse_float_field(
    request: Request,
    value: str | None,
    *,
    default: float | None,
    label: str,
    min_value: float | None = None,
    max_value: float | None = None,
) -> float | None:
    raw = (value or "").strip().replace(",", ".")
    if raw == "":
        return default
    try:
        n = float(raw)
    except ValueError:
        _flash(request, "warning", f"Поле «{label}»: некорректное число. Использую значение по умолчанию.")
        return default
    if min_value is not None and n < min_value:
        _flash(request, "warning", f"Поле «{label}»: минимум {min_value}. Исправлено.")
        n = min_value
    if max_value is not None and n > max_value:
        _flash(request, "warning", f"Поле «{label}»: максимум {max_value}. Исправлено.")
        n = max_value
    return n


def _clean_username(value: Any) -> str:
    return str(value).lower().replace("@", "").strip() if value is not None else ""


def _extract_invite_hash(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    m = re.search(r"(?:t\.me/\+|t\.me/joinchat/)([A-Za-z0-9_-]+)", raw)
    return m.group(1) if m else None


def _channel_bare_id(value: Any) -> int | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        if raw.startswith("-100"):
            return int(raw[4:])
        if raw.startswith("-"):
            return int(raw[1:])
        return int(raw)
    except ValueError:
        return None


def _ensure_settings_schema(settings: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(settings, dict):
        settings = {}
    settings.setdefault("status", "stopped")
    settings.setdefault("ai_provider", "deepseek")
    settings.setdefault("api_keys", {})

    models = settings.get("models")
    if not isinstance(models, dict):
        models = {}
    for key, value in DEFAULT_MODELS.items():
        models.setdefault(key, value)
    settings["models"] = models

    settings.setdefault("targets", [])
    settings.setdefault("discussion_targets", [])
    settings.setdefault("discussion_queue", [])
    settings.setdefault("discussion_start_queue", [])
    settings.setdefault("reaction_targets", [])
    settings.setdefault("monitor_targets", [])
    settings.setdefault("humanization", {})
    settings.setdefault("product_knowledge", {})
    settings.setdefault("blacklist", [])
    settings.setdefault("personas", {})
    settings.setdefault("roles", {})
    settings.setdefault("role_presets", {})
    settings.setdefault("default_role_id", DEFAULT_ROLE_ID)
    settings.setdefault("manual_queue", [])
    settings.setdefault("profile_tasks", {})
    ensure_role_schema(settings)
    _ensure_projects_schema(settings)
    _ensure_project_ids(settings)
    return settings


def _ensure_projects_schema(settings: Dict[str, Any]) -> None:
    projects = settings.get("projects")
    if not isinstance(projects, list):
        projects = []

    has_default = False
    normalized: list[dict[str, Any]] = []
    for p in projects:
        if not isinstance(p, dict):
            continue
        pid = str(p.get("id") or "").strip()
        name = str(p.get("name") or "").strip()
        if not pid or not name:
            continue
        if pid == DEFAULT_PROJECT_ID:
            has_default = True
        normalized.append(
            {
                "id": pid,
                "name": name,
                "created_at": p.get("created_at") or datetime.now(timezone.utc).isoformat(),
            }
        )

    if not has_default:
        normalized.insert(
            0,
            {
                "id": DEFAULT_PROJECT_ID,
                "name": DEFAULT_PROJECT_NAME,
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    settings["projects"] = normalized

    active_project_id = str(settings.get("active_project_id") or "").strip()
    project_ids = {p["id"] for p in normalized}
    if active_project_id not in project_ids:
        active_project_id = DEFAULT_PROJECT_ID
    settings["active_project_id"] = active_project_id


def _ensure_project_ids(settings: Dict[str, Any]) -> None:
    for key in ("targets", "discussion_targets", "reaction_targets", "monitor_targets"):
        items = settings.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict) and "project_id" not in item:
                item["project_id"] = DEFAULT_PROJECT_ID


def _active_project_id(settings: Dict[str, Any]) -> str:
    pid = str(settings.get("active_project_id") or "").strip()
    if not pid:
        pid = DEFAULT_PROJECT_ID
    return pid


def _active_project(settings: Dict[str, Any]) -> Dict[str, Any]:
    pid = _active_project_id(settings)
    for p in settings.get("projects", []) or []:
        if isinstance(p, dict) and p.get("id") == pid:
            return p
    return {"id": DEFAULT_PROJECT_ID, "name": DEFAULT_PROJECT_NAME}


def _project_id_for(item: Dict[str, Any]) -> str:
    pid = str(item.get("project_id") or "").strip()
    return pid or DEFAULT_PROJECT_ID


def _filter_by_project(items: List[Dict[str, Any]], project_id: str) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if _project_id_for(item) == project_id:
            result.append(item)
    return result


def _filter_accounts_by_project(accounts: List[Dict[str, Any]], project_id: str) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for acc in accounts:
        if not isinstance(acc, dict):
            continue
        if _project_id_for(acc) == project_id:
            result.append(acc)
    return result


def _find_account_index(accounts: List[Dict[str, Any]], session_name: str, project_id: str) -> Optional[int]:
    for idx, acc in enumerate(accounts):
        if acc.get("session_name") != session_name:
            continue
        if _project_id_for(acc) == project_id:
            return idx
    return None


def _load_settings() -> Tuple[Dict[str, Any], str | None]:
    settings, err = load_json_with_error(SETTINGS_FILE, {})
    settings = _ensure_settings_schema(settings or {})
    changed = False
    changed = _ensure_discussion_target_schema(settings) or changed
    if changed:
        try:
            _save_settings(settings)
        except Exception:
            pass
    return settings, err


def _ensure_discussion_target_schema(settings: Dict[str, Any]) -> bool:
    targets = settings.get("discussion_targets")
    if not isinstance(targets, list):
        return False
    changed = False
    used: set[str] = set()
    for t in targets:
        if not isinstance(t, dict):
            continue
        target_id = str(t.get("id") or "").strip()
        if not target_id or target_id in used:
            target_id = uuid.uuid4().hex
            while target_id in used:
                target_id = uuid.uuid4().hex
            t["id"] = target_id
            changed = True
        used.add(target_id)

        if "title" not in t:
            t["title"] = ""
            changed = True
        if t.get("title") is None:
            t["title"] = ""
            changed = True

        scenes = t.get("scenes")
        if scenes is None:
            continue
        if not isinstance(scenes, list):
            t["scenes"] = []
            changed = True
            continue
        used_scene_ids: set[str] = set()
        cleaned_scenes: list[dict] = []
        for sc in scenes:
            if not isinstance(sc, dict):
                changed = True
                continue
            scene_id = str(sc.get("id") or "").strip()
            if not scene_id or scene_id in used_scene_ids:
                scene_id = uuid.uuid4().hex
                while scene_id in used_scene_ids:
                    scene_id = uuid.uuid4().hex
                sc["id"] = scene_id
                changed = True
            used_scene_ids.add(scene_id)

            if "title" not in sc or sc.get("title") is None:
                sc["title"] = ""
                changed = True
            if "operator_text" not in sc or sc.get("operator_text") is None:
                sc["operator_text"] = ""
                changed = True
            if "vector_prompt" not in sc or sc.get("vector_prompt") is None:
                sc["vector_prompt"] = ""
                changed = True
            cleaned_scenes.append(sc)
        if len(cleaned_scenes) != len(scenes):
            t["scenes"] = cleaned_scenes
            changed = True
    return changed


def _save_settings(settings: Dict[str, Any]) -> None:
    save_json(SETTINGS_FILE, settings)


def _normalize_tg_ref(value: str) -> str:
    v = (value or "").strip()
    if not v:
        return ""
    v = v.replace("https://t.me/", "").replace("http://t.me/", "").replace("t.me/", "")
    v = v.lstrip("@").strip()
    return v


def _upsert_profile_task(settings: Dict[str, Any], session_name: str, patch: Dict[str, Any]) -> None:
    tasks = settings.get("profile_tasks")
    if not isinstance(tasks, dict):
        tasks = {}
        settings["profile_tasks"] = tasks

    existing = tasks.get(session_name)
    if not isinstance(existing, dict):
        existing = {}

    existing.update({k: v for k, v in patch.items() if v is not None})
    existing["status"] = "pending"
    existing["error"] = ""
    existing["updated_at"] = datetime.now(timezone.utc).isoformat()
    tasks[session_name] = existing


def _load_accounts() -> Tuple[List[Dict[str, Any]], str | None]:
    accounts, err = load_json_with_error(ACCOUNTS_FILE, [])
    if not isinstance(accounts, list):
        accounts = []
    return accounts, err


def _ensure_accounts_date_added(accounts: List[Dict[str, Any]]) -> bool:
    updated = False
    for acc in accounts:
        if acc.get("date_added"):
            continue
        session_file = acc.get("session_file") or acc.get("session_name")
        path = _find_session_file_path(str(session_file or ""), ACCOUNTS_DIR) if session_file else None
        ts = None
        if path and os.path.exists(path):
            try:
                ts = os.path.getmtime(path)
            except OSError:
                ts = None
        if ts is None:
            ts = time.time()
        acc["date_added"] = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        updated = True
    return updated


def _save_accounts(accounts: List[Dict[str, Any]]) -> None:
    save_json(ACCOUNTS_FILE, accounts)


def _roles_dict(settings: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    roles = settings.get("roles")
    if not isinstance(roles, dict):
        return {}
    return roles


def _default_role_id(settings: Dict[str, Any]) -> str:
    roles = _roles_dict(settings)
    default_role_id = str(settings.get("default_role_id") or "").strip()
    if default_role_id in roles:
        return default_role_id
    if DEFAULT_ROLE_ID in roles:
        return DEFAULT_ROLE_ID
    return next(iter(roles.keys()), "")


def _resolve_role_id(settings: Dict[str, Any], role_id: str | None) -> str:
    roles = _roles_dict(settings)
    rid = str(role_id or "").strip()
    if rid in roles:
        return rid
    return _default_role_id(settings)


def _role_name_map(settings: Dict[str, Any]) -> Dict[str, str]:
    names: Dict[str, str] = {}
    for rid, role in _roles_dict(settings).items():
        if isinstance(role, dict):
            names[rid] = str(role.get("name") or rid)
    names[CUSTOM_ROLE_ID] = CUSTOM_ROLE_NAME
    return names


def _sorted_role_items(settings: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    items: List[Tuple[str, Dict[str, Any]]] = []
    for rid, role in _roles_dict(settings).items():
        if isinstance(role, dict):
            items.append((rid, role))
    items.sort(key=lambda item: str(item[1].get("name") or item[0]).lower())
    return items


def _ensure_accounts_roles_saved(accounts: List[Dict[str, Any]], settings: Dict[str, Any]) -> None:
    if ensure_accounts_have_roles(accounts, settings):
        _save_accounts(accounts)


def _cleanup_inbox_for_removed_accounts(settings: Dict[str, Any]) -> None:
    """Ensure unread counters don't include removed accounts.

    The inbox tables store historical messages and can contain rows for sessions that were
    removed from accounts.json. We don't want those to keep showing as unread in UI.

    NOTE: This intentionally runs only when settings/accounts are changed (e.g. deletion)
    or on inbox pages, not on every request.
    """

    accounts, _ = _load_accounts()
    # We clean up only sessions that are no longer present in accounts.json at all.
    # This avoids touching inbox state for other projects.
    sessions = [str(a.get("session_name")).strip() for a in accounts if a.get("session_name")]
    sessions = [s for s in sessions if s]
    if not sessions:
        return

    placeholders = ", ".join(["?"] * len(sessions))
    with _db_connect() as conn:
        conn.execute(
            f"UPDATE inbox_messages SET is_read=1 WHERE direction='in' AND is_read=0 AND session_name NOT IN ({placeholders})",
            tuple(sessions),
        )
        conn.commit()


@contextmanager
def _db_connect() -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _manual_task_row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    overrides: Dict[str, Any] = {}
    raw_overrides = row["overrides_json"] if "overrides_json" in row.keys() else None
    if raw_overrides:
        try:
            parsed = json.loads(raw_overrides)
            if isinstance(parsed, dict):
                overrides = parsed
        except Exception:
            overrides = {}
    return {
        "id": row["id"],
        "project_id": row["project_id"],
        "chat_id": row["chat_id"],
        "message_chat_id": row["message_chat_id"],
        "post_id": row["post_id"],
        "added_at": row["created_at"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "status": row["status"],
        "last_error": row["last_error"],
        "overrides": overrides,
    }


def _list_manual_tasks(
    project_id: str,
    *,
    statuses: Tuple[str, ...] = ("pending",),
    limit: int = 200,
) -> List[Dict[str, Any]]:
    statuses = tuple(s for s in statuses if s)
    if not statuses:
        return []
    placeholders = ", ".join(["?"] * len(statuses))
    params = [project_id, *statuses, int(limit)]
    with _db_connect() as conn:
        rows = conn.execute(
            f"""
            SELECT
              id, project_id, chat_id, message_chat_id, post_id,
              overrides_json, status, created_at, started_at, finished_at, last_error
            FROM manual_tasks
            WHERE project_id = ? AND status IN ({placeholders})
            ORDER BY id DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
    return [_manual_task_row_to_dict(row) for row in rows]


def _enqueue_manual_task(
    *,
    project_id: str,
    chat_id: str,
    message_chat_id: str,
    post_id: int,
    overrides: Dict[str, Any] | None = None,
) -> int:
    payload = json.dumps(overrides or {}, ensure_ascii=False)
    now_ts = time.time()
    with _db_connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO manual_tasks (
              project_id, chat_id, message_chat_id, post_id,
              overrides_json, status, created_at
            )
            VALUES (?, ?, ?, ?, ?, 'pending', ?)
            """,
            (project_id, str(chat_id), str(message_chat_id), int(post_id), payload, now_ts),
        )
        return int(cur.lastrowid or 0)


def _clear_manual_tasks(project_id: str, *, statuses: Tuple[str, ...] = ("pending", "processing")) -> int:
    statuses = tuple(s for s in statuses if s)
    if not statuses:
        return 0
    placeholders = ", ".join(["?"] * len(statuses))
    params = [project_id, *statuses]
    with _db_connect() as conn:
        cur = conn.execute(
            f"DELETE FROM manual_tasks WHERE project_id = ? AND status IN ({placeholders})",
            tuple(params),
        )
        return int(cur.rowcount or 0)


def _move_manual_tasks(source_project_id: str, dest_project_id: str) -> int:
    with _db_connect() as conn:
        cur = conn.execute(
            """
            UPDATE manual_tasks
            SET project_id = ?
            WHERE project_id = ? AND status IN ('pending', 'processing')
            """,
            (dest_project_id, source_project_id),
        )
        return int(cur.rowcount or 0)


def _delete_manual_tasks_for_project(project_id: str) -> int:
    with _db_connect() as conn:
        cur = conn.execute("DELETE FROM manual_tasks WHERE project_id = ?", (project_id,))
        return int(cur.rowcount or 0)


def _migrate_legacy_manual_queue(settings: Dict[str, Any]) -> int:
    legacy_queue = settings.get("manual_queue")
    if not isinstance(legacy_queue, list) or not legacy_queue:
        return 0
    moved = 0
    for task in legacy_queue:
        if not isinstance(task, dict):
            continue
        chat_id = str(task.get("chat_id") or "").strip()
        post_id_raw = task.get("post_id")
        if not chat_id or post_id_raw in (None, ""):
            continue
        try:
            post_id = int(post_id_raw)
        except Exception:
            continue
        message_chat_id = str(task.get("message_chat_id") or "").strip() or chat_id
        overrides = task.get("overrides") if isinstance(task.get("overrides"), dict) else {}
        project_id = _project_id_for(task)
        _enqueue_manual_task(
            project_id=project_id,
            chat_id=chat_id,
            message_chat_id=message_chat_id,
            post_id=post_id,
            overrides=overrides,
        )
        moved += 1
    if moved:
        settings["manual_queue"] = []
        _save_settings(settings)
    return moved


def _load_join_status(target_ids: List[str]) -> Dict[str, Dict[str, Dict[str, Any]]]:
    join_status: Dict[str, Dict[str, Dict[str, Any]]] = {}
    target_ids = [str(t) for t in target_ids if t]
    if not target_ids:
        return join_status
    placeholders = ",".join("?" for _ in target_ids)
    with _db_connect() as conn:
        rows = conn.execute(
            f"SELECT session_name, target_id, status, last_error, last_method, last_attempt, retry_count, next_retry_at "
            f"FROM join_status WHERE target_id IN ({placeholders})",
            target_ids,
        ).fetchall()
    for row in rows:
        session = row["session_name"]
        target_id = str(row["target_id"])
        join_status.setdefault(session, {})[target_id] = dict(row)
    return join_status


def _record_account_failure(
    session_name: str, kind: str, *, last_error: str | None = None, last_target: str | None = None
) -> int:
    if not session_name or not kind:
        return 0
    now = time.time()
    with _db_connect() as conn:
        conn.execute(
            """
            INSERT INTO account_failures (session_name, kind, count, last_error, last_attempt, last_target)
            VALUES (?, ?, 1, ?, ?, ?)
            ON CONFLICT(session_name, kind) DO UPDATE SET
                count = account_failures.count + 1,
                last_error = excluded.last_error,
                last_attempt = excluded.last_attempt,
                last_target = excluded.last_target
            """,
            (session_name, kind, last_error, now, last_target),
        )
        row = conn.execute(
            "SELECT count FROM account_failures WHERE session_name = ? AND kind = ?",
            (session_name, kind),
        ).fetchone()
        conn.commit()
    return int(row["count"]) if row else 1


def _clear_account_failure(session_name: str, kind: str) -> None:
    if not session_name or not kind:
        return
    with _db_connect() as conn:
        conn.execute(
            "DELETE FROM account_failures WHERE session_name = ? AND kind = ?",
            (session_name, kind),
        )
        conn.commit()


def _load_account_failures(sessions: List[str], *, min_count: int = 1) -> List[Dict[str, Any]]:
    sessions = [s for s in sessions if s]
    if not sessions:
        return []
    placeholders = ",".join("?" for _ in sessions)
    with _db_connect() as conn:
        rows = conn.execute(
            f"""
            SELECT session_name, kind, count, last_error, last_attempt, last_target
            FROM account_failures
            WHERE session_name IN ({placeholders}) AND count >= ?
            ORDER BY count DESC, last_attempt DESC
            """,
            (*sessions, min_count),
        ).fetchall()
    return [dict(r) for r in rows]


def _warning_key_status(session_name: str, status: str) -> str:
    session_name = str(session_name or "").strip()
    status = str(status or "").strip().lower()
    return f"status:{session_name}:{status}"


def _warning_key_failure(session_name: str, kind: str) -> str:
    session_name = str(session_name or "").strip()
    kind = str(kind or "").strip().lower()
    return f"fail:{session_name}:{kind}"


def _load_seen_warning_keys(keys: List[str]) -> set[str]:
    keys = [str(k).strip() for k in keys if k]
    if not keys:
        return set()
    placeholders = ",".join("?" for _ in keys)
    with _db_connect() as conn:
        rows = conn.execute(
            f"SELECT key FROM warning_seen WHERE key IN ({placeholders})",
            tuple(keys),
        ).fetchall()
    return {str(r["key"]) for r in rows if r and r["key"]}


def _mark_warning_keys_seen(keys: List[str]) -> None:
    keys = [str(k).strip() for k in keys if k]
    if not keys:
        return
    now = time.time()
    rows = [(k, now) for k in dict.fromkeys(keys)]
    with _db_connect() as conn:
        conn.executemany(
            """
            INSERT INTO warning_seen(key, seen_at)
            VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET
              seen_at = excluded.seen_at
            """,
            rows,
        )
        conn.commit()


def _humanize_failure_kind(kind: str) -> str:
    k = str(kind or "").strip().lower()
    return {
        "connect": "подключение",
        "comment": "комментарии",
        "reply": "ответы",
        "discussion": "обсуждения",
        "join": "вступление",
        "reaction": "реакции",
    }.get(k, k or "action")


def _humanize_failure_context(kind: str, last_target: str) -> str | None:
    kind = str(kind or "").strip().lower()
    last_target = str(last_target or "").strip()
    if not last_target:
        return None
    if kind == "connect":
        return {
            "start": "при запуске клиента",
            "manage_clients": "при поддержании соединения",
        }.get(last_target, last_target)
    return None


def _humanize_failure_error(kind: str, last_error: str) -> str:
    kind = str(kind or "").strip().lower()
    raw = str(last_error or "").strip()
    low = raw.lower()
    if not raw:
        return "—"

    if "timeout_after_" in low:
        suffix = " (таймаут — проверьте прокси/интернет)"
        if kind == "connect":
            suffix = " (таймаут при подключении — проверьте прокси/интернет)"
        return f"{raw}{suffix}"

    if low in {"unauthorized", "auth_key_unregistered"}:
        return f"{raw} (аккаунт не авторизован — нужен повторный вход)"
    if low in {"missing_session", "missing_api_credentials"}:
        return f"{raw} (проверьте данные аккаунта)"
    if low in {"session_db_locked"} or "database is locked" in low:
        return f"{raw} (сессия занята другим процессом — остановите commentator и повторите)"
    if low == "start_failed":
        return f"{raw} (не удалось запустить Telegram-клиент — часто аккаунт не авторизован или нет сессии)"

    if "floodwait" in low or "flood_wait" in low:
        return f"{raw} (Telegram попросил подождать — временный лимит)"
    if "chatwriteforbidden" in low or "chat_write_forbidden" in low:
        return f"{raw} (нет прав писать в чат/канал)"

    if kind == "connect" and raw == "connect_returned_disconnected":
        return f"{raw} (подключение вернуло disconnected)"

    return raw


def _init_database() -> None:
    with _db_connect() as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                log_type TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                destination_chat_id INTEGER NOT NULL,
                channel_name TEXT,
                channel_username TEXT,
                source_channel_id INTEGER,
                post_id INTEGER NOT NULL,
                account_session_name TEXT,
                account_first_name TEXT,
                account_username TEXT,
                content TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS proxies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                name TEXT,
                ip TEXT,
                country TEXT,
                status TEXT,
                last_check TEXT
            )
            """
        )
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(proxies)").fetchall()}
        if "name" not in cols:
            conn.execute("ALTER TABLE proxies ADD COLUMN name TEXT")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS warning_seen (
                key TEXT PRIMARY KEY,
                seen_at REAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scenarios (
                chat_id TEXT PRIMARY KEY,
                script_content TEXT,
                current_index INTEGER DEFAULT 0,
                status TEXT DEFAULT 'stopped',
                last_run_time REAL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS post_scenarios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT,
                post_id INTEGER,
                current_index INTEGER DEFAULT 0,
                last_run_time REAL DEFAULT 0,
                UNIQUE(chat_id, post_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS triggers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT NOT NULL,
                trigger_phrase TEXT NOT NULL,
                answer_text TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS outbound_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT,
                reply_to_msg_id INTEGER,
                session_name TEXT,
                text TEXT,
                status TEXT DEFAULT 'pending'
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS inbox_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                direction TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                session_name TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                msg_id INTEGER,
                reply_to_msg_id INTEGER,
                sender_id INTEGER,
                sender_username TEXT,
                sender_name TEXT,
                chat_title TEXT,
                chat_username TEXT,
                text TEXT,
                replied_to_text TEXT,
                reactions_summary TEXT,
                reactions_updated_at TEXT,
                is_read INTEGER DEFAULT 0,
                error TEXT
            )
            """
        )
        inbox_cols = {row["name"] for row in conn.execute("PRAGMA table_info(inbox_messages)").fetchall()}
        if "reactions_summary" not in inbox_cols:
            conn.execute("ALTER TABLE inbox_messages ADD COLUMN reactions_summary TEXT")
        if "reactions_updated_at" not in inbox_cols:
            conn.execute("ALTER TABLE inbox_messages ADD COLUMN reactions_updated_at TEXT")
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_inbox_unique ON inbox_messages(session_name, chat_id, msg_id, direction)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_inbox_kind_unread ON inbox_messages(kind, is_read, id)")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS join_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_name TEXT NOT NULL,
                target_id TEXT NOT NULL,
                status TEXT NOT NULL,
                last_error TEXT,
                last_method TEXT,
                last_attempt REAL,
                retry_count INTEGER DEFAULT 0,
                next_retry_at REAL
            )
            """
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_join_status_unique ON join_status(session_name, target_id)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS account_failures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_name TEXT NOT NULL,
                kind TEXT NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                last_attempt REAL,
                last_target TEXT
            )
            """
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_account_failures_unique ON account_failures(session_name, kind)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_last_post_times (
                kind TEXT NOT NULL,
                chat_key TEXT NOT NULL,
                last_post_ts REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY(kind, chat_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scenario_msg_history (
                chat_id TEXT NOT NULL,
                post_id INTEGER NOT NULL,
                ref_idx INTEGER NOT NULL,
                msg_id INTEGER NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY(chat_id, post_id, ref_idx)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_scenario_msg_hist_post ON scenario_msg_history(chat_id, post_id)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS post_comment_plans (
                chat_key TEXT NOT NULL,
                post_id INTEGER NOT NULL,
                planned_count INTEGER NOT NULL,
                planned_accounts TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY(chat_key, post_id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_logs_dest_post_type ON logs(destination_chat_id, post_id, log_type)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_logs_dest_post_type_account ON logs(destination_chat_id, post_id, log_type, account_session_name)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS used_identities (
                user_id INTEGER PRIMARY KEY,
                date_used TEXT
            )
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS discussion_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id TEXT NOT NULL,
                discussion_target_id TEXT,
                discussion_target_chat_id TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at REAL NOT NULL,
                started_at REAL,
                finished_at REAL,
                schedule_at REAL,
                operator_session_name TEXT,
                seed_msg_id INTEGER,
                seed_text TEXT,
                settings_json TEXT,
                participants_json TEXT,
                error TEXT
            )
            """
        )
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(discussion_sessions)").fetchall()}
        if "discussion_target_id" not in cols:
            conn.execute("ALTER TABLE discussion_sessions ADD COLUMN discussion_target_id TEXT")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_discussion_sessions_target "
            "ON discussion_sessions(project_id, discussion_target_chat_id, created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_discussion_sessions_target_id "
            "ON discussion_sessions(project_id, discussion_target_id, created_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_discussion_sessions_status "
            "ON discussion_sessions(project_id, status, schedule_at)"
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS discussion_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL,
                created_at REAL NOT NULL,
                speaker_type TEXT NOT NULL,
                speaker_session_name TEXT,
                speaker_label TEXT,
                msg_id INTEGER,
                reply_to_msg_id INTEGER,
                text TEXT,
                prompt_info TEXT,
                error TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_discussion_messages_session "
            "ON discussion_messages(session_id, id)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id TEXT NOT NULL,
                chat_id TEXT NOT NULL,
                message_chat_id TEXT,
                post_id INTEGER NOT NULL,
                overrides_json TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at REAL NOT NULL,
                started_at REAL,
                finished_at REAL,
                last_error TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_manual_tasks_project_status "
            "ON manual_tasks(project_id, status, id)"
        )
        conn.commit()


def _load_config(section: str) -> Dict[str, str]:
    parser = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Файл config.ini не найден: {CONFIG_FILE}")
    parser.read(CONFIG_FILE)
    if section not in parser:
        raise KeyError(f"В config.ini не найдена секция [{section}].")
    return dict(parser[section])


def _telethon_credentials() -> Tuple[int, str]:
    cfg = _load_config("telethon_credentials")
    return int(cfg["api_id"]), cfg["api_hash"]


def _parse_proxy_tuple(url: str) -> tuple | None:
    try:
        protocol, rest = url.split("://", 1)
        auth, addr = rest.split("@", 1)
        user, password = auth.split(":", 1)
        host, port_s = addr.split(":", 1)
        return (protocol, host, int(port_s), True, user, password)
    except Exception:
        return None


_PROXY_IP_RE = re.compile(r"^\\d{1,3}(?:\\.\\d{1,3}){3}$")
_PROXY_HOST_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def _looks_like_ip(value: str) -> bool:
    return bool(value and _PROXY_IP_RE.match(value))


def _looks_like_host(value: str) -> bool:
    value = (value or "").strip()
    if not value:
        return False
    if _looks_like_ip(value):
        return True
    if value.lower() == "localhost":
        return True
    if any(ch.isspace() for ch in value):
        return False
    if "/" in value or "@" in value:
        return False
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1]
        return bool(inner and ":" in inner)
    return bool(_PROXY_HOST_RE.match(value))


def _is_port(value: str) -> bool:
    value = (value or "").strip()
    if not value.isdigit():
        return False
    try:
        port = int(value)
    except Exception:
        return False
    return 1 <= port <= 65535


def _normalize_proxy_url(raw: str) -> str | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    if "://" in raw:
        return raw

    if "@" in raw:
        left, right = raw.split("@", 1)
        left_parts = left.split(":")
        right_parts = right.split(":")
        if len(left_parts) == 2 and len(right_parts) >= 2:
            host, port = left_parts
            if _looks_like_host(host) and _is_port(port):
                return f"http://{right}@{host}:{port}"
        if len(right_parts) == 2 and len(left_parts) >= 2:
            host, port = right_parts
            if _looks_like_host(host) and _is_port(port):
                return f"http://{left}@{host}:{port}"
        if len(right_parts) >= 2:
            return f"http://{left}@{right}"

    parts = raw.split(":")
    if len(parts) == 2:
        host, port = parts
        if _looks_like_host(host) and _is_port(port):
            return f"http://{host}:{port}"
    if len(parts) == 4:
        host, port, user, password = parts
        if _looks_like_host(host) and _is_port(port):
            return f"http://{user}:{password}@{host}:{port}"
        user, password, host, port = parts
        if _looks_like_host(host) and _is_port(port):
            return f"http://{user}:{password}@{host}:{port}"

    return None


def _split_proxy_line(line: str) -> tuple[str, str | None]:
    raw = (line or "").strip()
    if not raw:
        return "", None
    for sep in ("|", ";"):
        if sep in raw:
            left, right = raw.split(sep, 1)
            left = left.strip()
            right = right.strip()
            if left and right:
                return right, left
    return raw, None


def _find_session_file_path(session_file: str, accounts_dir: Path) -> str | None:
    if not session_file:
        return None
    raw = str(session_file)
    candidates: list[str] = []
    if os.path.isabs(raw):
        candidates.append(raw)
    if raw.endswith(".session"):
        candidates.append(raw)
    if not os.path.isabs(raw):
        candidates.append(str(accounts_dir / raw))
        candidates.append(str(accounts_dir / f"{raw}.session"))
    for path in candidates:
        if path and os.path.exists(path):
            return path
    return None


def _resolve_account_session(account_data: Dict[str, Any]) -> StringSession | str | None:
    session_string = str(account_data.get("session_string") or "").strip()
    if session_string:
        return StringSession(session_string)

    session_path = account_data.get("session_path")
    if isinstance(session_path, str) and session_path and os.path.exists(session_path):
        return session_path

    session_file = account_data.get("session_file") or account_data.get("session_name")
    if not session_file:
        return None
    return _find_session_file_path(str(session_file), ACCOUNTS_DIR)


def _resolve_account_credentials(
    account_data: Dict[str, Any], fallback_api_id: int, fallback_api_hash: str
) -> Tuple[int, str]:
    api_id = account_data.get("app_id") or account_data.get("api_id") or fallback_api_id
    api_hash = account_data.get("app_hash") or account_data.get("api_hash") or fallback_api_hash
    try:
        api_id = int(api_id)
    except Exception:
        api_id = fallback_api_id
    api_hash = api_hash or fallback_api_hash
    return api_id, api_hash


def _resolve_account_proxy(account_data: Dict[str, Any]) -> tuple | None:
    proxy_url = account_data.get("proxy_url")
    if isinstance(proxy_url, str) and proxy_url.strip():
        return _parse_proxy_tuple(proxy_url.strip())

    proxy = account_data.get("proxy")
    if isinstance(proxy, str) and proxy.strip():
        return _parse_proxy_tuple(proxy.strip())

    if isinstance(proxy, (list, tuple)) and proxy:
        if isinstance(proxy[0], str) and "://" in proxy[0]:
            return _parse_proxy_tuple(proxy[0])
        if len(proxy) >= 3 and isinstance(proxy[0], str):
            return tuple(proxy)

    proxies = account_data.get("proxies")
    if isinstance(proxies, (list, tuple)):
        for item in proxies:
            if isinstance(item, str) and "://" in item:
                return _parse_proxy_tuple(item)

    return None


def _update_join_status(
    session_name: str,
    target_id: str,
    status: str,
    *,
    last_error: str | None = None,
    last_method: str | None = None,
) -> None:
    try:
        with _db_connect() as conn:
            conn.execute(
                """
                INSERT INTO join_status (
                    session_name, target_id, status,
                    last_error, last_method, last_attempt, retry_count, next_retry_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_name, target_id) DO UPDATE SET
                    status=excluded.status,
                    last_error=excluded.last_error,
                    last_method=excluded.last_method,
                    last_attempt=excluded.last_attempt,
                    retry_count=excluded.retry_count,
                    next_retry_at=excluded.next_retry_at
                """,
                (
                    session_name,
                    target_id,
                    status,
                    last_error,
                    last_method,
                    time.time(),
                    0,
                    None,
                ),
            )
            conn.commit()
    except Exception:
        pass


async def _refresh_target_access_hashes(target: Dict[str, Any], settings: Dict[str, Any]) -> bool:
    if not isinstance(target, dict):
        return False
    chat_id = str(target.get("chat_id") or "")
    linked_id = str(target.get("linked_chat_id") or "")
    need_main = not target.get("chat_access_hash")
    need_linked = bool(linked_id) and not target.get("linked_chat_access_hash")
    if not (need_main or need_linked):
        return False

    updated = False
    try:
        client = await _get_any_authorized_client()
    except HTTPException:
        return False

    try:
        entity = None
        username = str(target.get("chat_username") or "").strip().lstrip("@")
        invite_link = str(target.get("invite_link") or "").strip()
        if username:
            try:
                entity = await client.get_entity(username)
            except Exception:
                entity = None
        if entity is None and invite_link:
            try:
                if "t.me/+" in invite_link or "joinchat" in invite_link or "/" not in invite_link:
                    hash_arg = invite_link.split("/")[-1].replace("+", "")
                    invite_info = await client(CheckChatInviteRequest(hash_arg))
                    entity = getattr(invite_info, "chat", None)
            except Exception:
                entity = None

        if entity:
            access_hash = getattr(entity, "access_hash", None)
            if access_hash and need_main:
                target["chat_access_hash"] = access_hash
                updated = True
            if getattr(entity, "username", None) and not target.get("chat_username"):
                target["chat_username"] = entity.username
                updated = True
            if need_linked:
                try:
                    full = await client(GetFullChannelRequest(channel=entity))
                    linked_chat_id_bare = getattr(full.full_chat, "linked_chat_id", None)
                    if linked_chat_id_bare:
                        linked_entity = await client.get_entity(PeerChannel(linked_chat_id_bare))
                        linked_hash = getattr(linked_entity, "access_hash", None)
                        if linked_hash:
                            target["linked_chat_access_hash"] = linked_hash
                            updated = True
                        if not linked_id:
                            target["linked_chat_id"] = f"-100{linked_chat_id_bare}"
                            updated = True
                except Exception:
                    pass
    finally:
        try:
            if client.is_connected():
                await client.disconnect()
        except Exception:
            pass

    if updated:
        _save_settings(settings)
    return updated


async def _attempt_join_target(
    client: TelegramClient, session_name: str, target: Dict[str, Any], target_id: str
) -> Tuple[bool, str | None, str | None]:
    invite_link = target.get("invite_link")
    username = str(target.get("chat_username") or "").strip().lstrip("@")
    linked_chat_id = target.get("linked_chat_id")
    last_error = None
    last_method = None
    chat_id = str(target.get("chat_id") or "")
    linked_id = str(linked_chat_id or "")

    if invite_link:
        try:
            if "t.me/+" in invite_link or "joinchat" in invite_link or "/" not in invite_link:
                hash_arg = invite_link.split("/")[-1].replace("+", "")
                await client(ImportChatInviteRequest(hash_arg))
                return True, None, None
        except UserAlreadyParticipantError:
            return True, None, None
        except Exception as e:
            logger.warning(
                f"[admin_web] [{session_name}] join invite failed for {target_id}: {type(e).__name__}: {e}"
            )
            last_error = str(e)
            last_method = "invite"

    access_hash = None
    if str(target_id) == chat_id:
        access_hash = target.get("chat_access_hash")
    elif str(target_id) == linked_id:
        access_hash = target.get("linked_chat_access_hash")

    if access_hash:
        try:
            channel_id = _channel_bare_id(target_id) or _channel_bare_id(chat_id) or _channel_bare_id(linked_id)
            if channel_id is None:
                raise ValueError("invalid_channel_id")
            peer = InputPeerChannel(channel_id, int(access_hash))
            await client(JoinChannelRequest(peer))
            return True, None, None
        except UserAlreadyParticipantError:
            return True, None, None
        except Exception as e:
            logger.warning(
                f"[admin_web] [{session_name}] join access_hash failed for {target_id}: {type(e).__name__}: {e}"
            )
            last_error = str(e)
            last_method = "access_hash"

    if username and str(target_id) == chat_id:
        try:
            await client(JoinChannelRequest(username))
            return True, None, None
        except UserAlreadyParticipantError:
            return True, None, None
        except Exception as e:
            logger.warning(
                f"[admin_web] [{session_name}] join username failed for {target_id}: {type(e).__name__}: {e}"
            )
            last_error = str(e)
            last_method = "username"

    if username and str(target_id) == linked_id:
        try:
            entity = await client.get_entity(username)
            full = await client(GetFullChannelRequest(entity))
            if full.full_chat.linked_chat_id:
                linked_entity = await client.get_input_entity(full.full_chat.linked_chat_id)
                await client(JoinChannelRequest(linked_entity))
                return True, None, None
        except UserAlreadyParticipantError:
            return True, None, None
        except Exception as e:
            logger.warning(
                f"[admin_web] [{session_name}] join linked failed for {linked_chat_id}: {type(e).__name__}: {e}"
            )
            last_error = str(e)
            last_method = "linked"

    try:
        entity = await client.get_input_entity(int(str(target_id)))
        await client(JoinChannelRequest(entity))
        return True, None, None
    except UserAlreadyParticipantError:
        return True, None, None
    except Exception as e:
        logger.warning(
            f"[admin_web] [{session_name}] join id failed for {target_id}: {type(e).__name__}: {e}"
        )
        last_error = str(e)
        last_method = "id"

    return False, last_error, last_method


def _is_frozen_rpc_error(exc: RPCError) -> bool:
    name = exc.__class__.__name__
    if name == "FrozenMethodInvalidError":
        return True
    try:
        msg = str(exc)
    except Exception:
        msg = ""
    return "FROZEN" in msg.upper()


def _is_expected_invite_hash_error(exc: RPCError) -> bool:
    name = exc.__class__.__name__
    if name in {
        "InviteHashInvalidError",
        "InviteHashEmptyError",
        "InviteHashExpiredError",
    }:
        return True
    try:
        msg = str(exc)
    except Exception:
        msg = ""
    msg_upper = msg.upper()
    return any(
        token in msg_upper
        for token in (
            "INVITE_HASH_INVALID",
            "INVITE_HASH_EMPTY",
            "INVITE_HASH_EXPIRED",
        )
    )


async def _probe_account_frozen(client: TelegramClient) -> Tuple[bool | None, RPCError | None]:
    try:
        await client(CheckChatInviteRequest(FROZEN_ACCOUNT_PROBE_INVITE_HASH))
        return False, None
    except RPCError as exc:
        if _is_frozen_rpc_error(exc):
            return True, None
        if _is_expected_invite_hash_error(exc):
            return False, None
        return None, exc


async def _check_account_entry(
    acc: Dict[str, Any],
    api_id_default: int,
    api_hash_default: str,
) -> Tuple[str, bool]:
    status = str(acc.get("status") or "").lower().strip()
    if status == "banned":
        return "banned", False

    session = _resolve_account_session(acc)
    if not session:
        acc["last_error"] = "missing_session"
        return "error", True

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
        await client.connect()
        if not await client.is_user_authorized():
            acc["status"] = "unauthorized"
            acc.pop("last_error", None)
            return "unauthorized", False

        me = await client.get_me()
        acc.update(
            {
                "user_id": me.id,
                "first_name": me.first_name,
                "last_name": me.last_name or "",
                "username": me.username or "",
            }
        )
        acc.pop("last_error", None)

        frozen, probe_exc = await asyncio.wait_for(
            _probe_account_frozen(client),
            timeout=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS,
        )
        if frozen is True:
            acc["status"] = "frozen"
            return "frozen", False
        if probe_exc is not None:
            name = probe_exc.__class__.__name__
            msg = str(probe_exc)
            if _is_frozen_rpc_error(probe_exc):
                acc["status"] = "frozen"
                return "frozen", False
            acc["last_error"] = f"{name}: {msg}"
            return "error", True

        if str(acc.get("status") or "").lower().strip() not in {
            "banned",
            "frozen",
            "limited",
            "human_check",
        }:
            acc["status"] = "active"

        status = str(acc.get("status") or "active").lower().strip()
        return status or "active", False
    except UserDeactivatedBanError:
        acc["status"] = "banned"
        return "banned", False
    except RPCError as exc:
        name = exc.__class__.__name__
        msg = str(exc)
        if _is_frozen_rpc_error(exc):
            acc["status"] = "frozen"
            return "frozen", False
        acc["last_error"] = f"{name}: {msg}"
        return "error", True
    except sqlite3.OperationalError as exc:
        acc["last_error"] = str(exc)
        return "error", True
    except Exception as exc:
        acc["last_error"] = str(exc)
        return "error", True
    finally:
        if client.is_connected():
            await client.disconnect()


async def _check_proxy_health(proxy_url: str) -> Dict[str, Any]:
    test_url = "http://ip-api.com/json/"
    try:
        timeout = httpx.Timeout(15.0, connect=10.0)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }
        async with httpx.AsyncClient(proxy=proxy_url, timeout=timeout, headers=headers) as client:
            response = await client.get(test_url, follow_redirects=True)
            if response.status_code == 200:
                data = response.json()
                return {"status": "active", "ip": data.get("query"), "country": data.get("country")}
    except Exception:
        pass
    return {"status": "dead", "ip": None, "country": None}


async def _get_any_authorized_client() -> TelegramClient:
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    # Prefer accounts without proxy first (broken proxies can cause long connect timeouts).
    try:
        accounts = sorted(accounts, key=lambda a: 1 if a.get("proxy_url") else 0)
    except Exception:
        pass
    if not accounts:
        raise HTTPException(status_code=400, detail="Нет аккаунтов. Сначала добавьте хотя бы один.")

    api_id_default, api_hash_default = _telethon_credentials()
    blocked_statuses = {"banned", "frozen", "limited", "human_check", "unauthorized", "missing_session"}
    dirty = False
    deadline = time.monotonic() + max(5.0, float(ADMIN_WEB_TELETHON_TOTAL_TIMEOUT_SECONDS))

    for acc in accounts:
        if time.monotonic() > deadline:
            break
        status = str(acc.get("status") or "").lower().strip()
        if status in blocked_statuses:
            continue
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
            authorized = False
            await asyncio.wait_for(
                client.connect(),
                timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
            )
            authorized = await asyncio.wait_for(
                client.is_user_authorized(),
                timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
            )
            if not authorized:
                continue

            try:
                frozen, _ = await asyncio.wait_for(
                    _probe_account_frozen(client),
                    timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
                )
            except Exception:
                frozen = None

            if frozen is True:
                acc["status"] = "frozen"
                dirty = True
                authorized = False
                continue

            if dirty:
                _save_accounts(accounts)
                dirty = False

            return client
        except Exception:
            pass
        finally:
            if client.is_connected() and not authorized:
                await client.disconnect()

    if dirty:
        _save_accounts(accounts)

    raise HTTPException(status_code=400, detail="Нет авторизованных аккаунтов. Проверьте аккаунты.")


async def _resolve_channel_entity(client: TelegramClient, chat_input: str) -> Tuple[Any, str | None]:
    invite_link: str | None = None
    if "t.me/+" in chat_input or "t.me/joinchat/" in chat_input:
        invite_hash = chat_input.split("/")[-1].replace("+", "")
        invite_link = invite_hash
        invite_info = await client(CheckChatInviteRequest(invite_hash))
        entity = invite_info.chat
        return entity, invite_link

    entity = await client.get_entity(chat_input)
    return entity, None


async def _derive_target_chat_info(chat_input: str) -> Dict[str, Any]:
    def _short_exc(exc: Exception) -> str:
        try:
            msg = str(exc).replace("\n", " ").strip()
        except Exception:
            msg = ""
        if msg:
            msg = re.sub(r"\s+", " ", msg)
        if msg and len(msg) > 220:
            msg = msg[:219].rstrip() + "…"
        name = exc.__class__.__name__
        if name == "TimeoutError" and not msg:
            msg = "превышено время ожидания (проверьте прокси/сеть)"
        if name == "FrozenMethodInvalidError":
            hint = " (аккаунт заморожен Telegram — проверьте аккаунты/войдите заново)"
        else:
            hint = ""
        return f"{name}: {msg}{hint}" if msg else f"{name}{hint}"

    chat_input = (chat_input or "").strip()
    if not chat_input:
        raise HTTPException(status_code=400, detail="Пустой ввод.")

    settings, _ = _load_settings()
    accounts, _ = _load_accounts()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    if not accounts:
        raise HTTPException(status_code=400, detail="Нет аккаунтов. Сначала добавьте хотя бы один.")

    api_id_default, api_hash_default = _telethon_credentials()
    blocked_statuses = {"banned", "frozen", "limited", "human_check", "unauthorized", "missing_session"}

    last_error: Exception | None = None
    last_session: str | None = None
    deadline = time.monotonic() + max(5.0, float(ADMIN_WEB_TELETHON_TOTAL_TIMEOUT_SECONDS))

    for acc in accounts:
        if time.monotonic() > deadline:
            last_error = TimeoutError("total_timeout")
            last_session = None
            break
        status = str(acc.get("status") or "").lower().strip()
        if status in blocked_statuses:
            continue

        session_name = str(acc.get("session_name") or "").strip() or "account"
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
            await asyncio.wait_for(
                client.connect(),
                timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
            )
            authorized = await asyncio.wait_for(
                client.is_user_authorized(),
                timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
            )
            if not authorized:
                last_error = RuntimeError("unauthorized")
                last_session = session_name
                continue

            try:
                entity, invite_link = await asyncio.wait_for(
                    _resolve_channel_entity(client, chat_input),
                    timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
                )
            except RPCError as exc:
                last_error = exc
                last_session = session_name
                continue
            except Exception as exc:
                last_error = exc
                last_session = session_name
                continue

            try:
                await asyncio.wait_for(
                    client(JoinChannelRequest(entity)),
                    timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
                )
            except Exception:
                pass

            chat_username = getattr(entity, "username", None)
            chat_access_hash = getattr(entity, "access_hash", None)
            channel_id_str = f"-100{entity.id}"
            chat_name_to_save = getattr(entity, "title", None) or str(entity.id)

            comment_chat_id_str = channel_id_str
            linked_access_hash = None
            linked_chat_name_to_save = None
            linked_chat_username_to_save = None
            try:
                full_channel = await asyncio.wait_for(
                    client(GetFullChannelRequest(channel=entity)),
                    timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
                )
                linked_chat_id_bare = getattr(full_channel.full_chat, "linked_chat_id", None)
                if linked_chat_id_bare:
                    comment_chat_entity = await asyncio.wait_for(
                        client.get_entity(PeerChannel(linked_chat_id_bare)),
                        timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
                    )
                    linked_access_hash = getattr(comment_chat_entity, "access_hash", None)
                    linked_chat_name_to_save = getattr(comment_chat_entity, "title", None) or str(comment_chat_entity.id)
                    linked_chat_username_to_save = getattr(comment_chat_entity, "username", None)
                    try:
                        await asyncio.wait_for(
                            client(JoinChannelRequest(comment_chat_entity)),
                            timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
                        )
                    except Exception:
                        pass
                    comment_chat_id_str = f"-100{comment_chat_entity.id}"
            except RPCError as exc:
                # Try another account (frozen accounts may fail on this method).
                last_error = exc
                last_session = session_name
                continue
            except Exception:
                # Best-effort: linked chat is optional; don't fail for unexpected errors.
                pass

            return {
                "chat_id": channel_id_str,
                "chat_username": chat_username,
                "linked_chat_id": comment_chat_id_str,
                "chat_name": chat_name_to_save,
                **({"linked_chat_name": linked_chat_name_to_save} if linked_chat_name_to_save else {}),
                **({"linked_chat_username": linked_chat_username_to_save} if linked_chat_username_to_save else {}),
                "invite_link": invite_link,
                **({"chat_access_hash": chat_access_hash} if chat_access_hash else {}),
                **({"linked_chat_access_hash": linked_access_hash} if linked_access_hash else {}),
            }
        except Exception as exc:
            last_error = exc
            last_session = session_name
            continue
        finally:
            try:
                if client.is_connected():
                    await client.disconnect()
            except Exception:
                pass

    if last_error is not None:
        session_part = f"[{last_session}] " if last_session else ""
        raise HTTPException(status_code=400, detail=f"Не удалось определить чат: {session_part}{_short_exc(last_error)}")
    raise HTTPException(status_code=400, detail="Не удалось определить чат: нет подходящих авторизованных аккаунтов.")


def _find_target_by_chat_id(
    settings: Dict[str, Any], chat_id: str, project_id: Optional[str] = None
) -> Tuple[int, Dict[str, Any]]:
    targets = settings.get("targets", [])
    pid = project_id or _active_project_id(settings)
    for i, t in enumerate(targets):
        if str(t.get("chat_id")) == str(chat_id) and _project_id_for(t) == pid:
            return i, t
    raise HTTPException(status_code=404, detail="Цель не найдена в текущем проекте")


def _find_reaction_target_by_chat_id(
    settings: Dict[str, Any], chat_id: str, project_id: Optional[str] = None
) -> Tuple[int, Dict[str, Any]]:
    targets = settings.get("reaction_targets", [])
    pid = project_id or _active_project_id(settings)
    for i, t in enumerate(targets):
        if str(t.get("chat_id")) == str(chat_id) and _project_id_for(t) == pid:
            return i, t
    raise HTTPException(status_code=404, detail="Цель реакций не найдена в текущем проекте")


def _find_discussion_target_by_chat_id(
    settings: Dict[str, Any], chat_id: str, project_id: Optional[str] = None
) -> Tuple[int, Dict[str, Any]]:
    targets = settings.get("discussion_targets", [])
    pid = project_id or _active_project_id(settings)
    for i, t in enumerate(targets):
        if str(t.get("chat_id")) == str(chat_id) and _project_id_for(t) == pid:
            return i, t
    raise HTTPException(status_code=404, detail="Цель обсуждений не найдена в текущем проекте")


def _find_discussion_target_by_id(
    settings: Dict[str, Any], target_id: str, project_id: Optional[str] = None
) -> Tuple[int, Dict[str, Any]]:
    targets = settings.get("discussion_targets", [])
    pid = project_id or _active_project_id(settings)
    target_id = str(target_id or "").strip()
    for i, t in enumerate(targets):
        if _project_id_for(t) != pid:
            continue
        if str(t.get("id") or "").strip() == target_id:
            return i, t
    raise HTTPException(status_code=404, detail="Цель обсуждений не найдена в текущем проекте")


def _find_monitor_target_by_chat_id(
    settings: Dict[str, Any], chat_id: str, project_id: Optional[str] = None
) -> Tuple[int, Dict[str, Any]]:
    targets = settings.get("monitor_targets", [])
    pid = project_id or _active_project_id(settings)
    for i, t in enumerate(targets):
        if str(t.get("chat_id")) == str(chat_id) and _project_id_for(t) == pid:
            return i, t
    raise HTTPException(status_code=404, detail="Цель мониторинга не найдена в текущем проекте")


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=303)


def _flash(request: Request, category: str, message: str) -> None:
    flashes = request.session.get("flashes") or []
    flashes.append({"category": category, "message": message})
    request.session["flashes"] = flashes


@asynccontextmanager
async def _auto_pause_commentator(
    request: Request,
    *,
    auto_pause: bool,
    reason: str,
    wait_seconds: float = 3.0,
):
    settings, _ = _load_settings()
    was_running = settings.get("status") == "running"
    paused = False

    if was_running and auto_pause:
        settings["status"] = "stopped"
        _save_settings(settings)
        paused = True
        _flash(
            request,
            "warning",
            f"Комментатор временно остановлен для операции: {reason}. "
            "Комментирование/реакции будут на паузе несколько секунд.",
        )
        await asyncio.sleep(wait_seconds)
    elif was_running and not auto_pause:
        _flash(
            request,
            "warning",
            "Комментатор сейчас работает. Для этой операции лучше временно остановить его, "
            "иначе возможны ошибки доступа к сессии.",
        )

    try:
        yield paused
    finally:
        if paused:
            settings, _ = _load_settings()
            settings["status"] = "running"
            _save_settings(settings)
            _flash(request, "success", "Комментатор снова запущен.")


def _pop_flashes(request: Request) -> List[Dict[str, str]]:
    flashes = request.session.get("flashes") or []
    request.session["flashes"] = []
    return flashes


def _human_dt(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    else:
        s = str(value).strip()
        if not s:
            return "—"
        dt = None
        try:
            if s.replace(".", "", 1).isdigit():
                dt = datetime.fromtimestamp(float(s), tz=timezone.utc)
            else:
                dt = datetime.fromisoformat(s)
        except Exception:
            return s

    if dt.tzinfo is not None:
        try:
            dt = dt.astimezone()
        except Exception:
            pass
    return dt.strftime("%d.%m.%Y %H:%M:%S")


@asynccontextmanager
async def _app_lifespan(_: FastAPI):
    ensure_data_dir()
    _init_database()
    if not os.path.exists(SETTINGS_FILE):
        _save_settings(_ensure_settings_schema({}))
    settings, _ = _load_settings()
    moved_legacy_tasks = _migrate_legacy_manual_queue(settings)
    if moved_legacy_tasks:
        logger.info("Migrated %s legacy manual_queue tasks into manual_tasks table", moved_legacy_tasks)
    if not os.path.exists(ACCOUNTS_FILE):
        _save_accounts([])
    yield


app = FastAPI(title="TG-комментатор (Web Admin)", lifespan=_app_lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.filters["human_dt"] = _human_dt

_raw_template_response = templates.TemplateResponse


def _template_response_compat(name_or_request: Any, context: Any = None, *args: Any, **kwargs: Any):
    # Keep existing call-sites compatible while using Starlette's newer signature:
    # TemplateResponse(request, name, context, ...)
    if isinstance(name_or_request, Request):
        return _raw_template_response(name_or_request, context, *args, **kwargs)
    if not isinstance(context, dict):
        raise TypeError("Template context must be a dict containing request")
    request_obj = context.get("request")
    if request_obj is None:
        raise ValueError("Template context must include request")
    return _raw_template_response(request_obj, name_or_request, context, *args, **kwargs)


templates.TemplateResponse = _template_response_compat  # type: ignore[assignment]


@app.exception_handler(HTTPException)
async def _http_exception_handler(request: Request, exc: HTTPException):
    if _wants_html(request):
        title = f"Ошибка {exc.status_code}"
        detail = str(exc.detail) if exc.detail is not None else ""
        return templates.TemplateResponse(
            "error.html",
            _template_context(request, title=title, detail=detail),
            status_code=exc.status_code,
        )
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.exception_handler(RequestValidationError)
async def _validation_exception_handler(request: Request, exc: RequestValidationError):
    if _wants_html(request):
        return templates.TemplateResponse(
            "error.html",
            _template_context(
                request,
                title="Некорректный ввод",
                detail="Проверьте поля формы и попробуйте снова.",
            ),
            status_code=422,
        )
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error in admin_web", exc_info=exc)
    if _wants_html(request):
        return templates.TemplateResponse(
            "error.html",
            _template_context(
                request,
                title="Ошибка 500",
                detail="Неожиданная ошибка. Посмотрите логи контейнера admin_web.",
            ),
            status_code=500,
        )
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})


@app.middleware("http")
async def _auth_middleware(request: Request, call_next):
    if ADMIN_WEB_DISABLE_AUTH:
        return await call_next(request)
    if request.url.path.startswith("/static"):
        return await call_next(request)
    if request.url.path in {"/login"}:
        return await call_next(request)
    if not request.session.get("user"):
        next_path = quote(request.url.path)
        return RedirectResponse(url=f"/login?next={next_path}", status_code=303)
    return await call_next(request)


# SessionMiddleware должен выполняться ДО auth middleware (последний добавленный middleware выполняется первым)
app.add_middleware(SessionMiddleware, secret_key=ADMIN_WEB_SECRET, same_site="lax")


def _template_context(request: Request, **extra: Any) -> Dict[str, Any]:
    settings, _ = _load_settings()
    active_project = _active_project(settings)
    warnings_count = 0
    inbox_counts = {"dialogs": 0, "quotes": 0}
    if request.session.get("user"):
        try:
            accounts, _ = _load_accounts()
            warnings_count = _warnings_count(accounts, settings)
            project_id = _active_project_id(settings)
            sessions = [
                str(a.get("session_name")).strip()
                for a in _filter_accounts_by_project(accounts, project_id)
                if a.get("session_name")
            ]
            sessions = [s for s in sessions if s]

            if sessions:
                placeholders = ", ".join(["?"] * len(sessions))
                with _db_connect() as conn:
                    inbox_counts["dialogs"] = conn.execute(
                        f"SELECT COUNT(*) AS c FROM inbox_messages WHERE kind='dm' AND direction='in' AND is_read=0 AND session_name IN ({placeholders})",
                        tuple(sessions),
                    ).fetchone()["c"]
                    inbox_counts["quotes"] = conn.execute(
                        f"SELECT COUNT(*) AS c FROM inbox_messages WHERE kind='quote' AND direction='in' AND is_read=0 AND session_name IN ({placeholders})",
                        tuple(sessions),
                    ).fetchone()["c"]
            else:
                inbox_counts = {"dialogs": 0, "quotes": 0}
        except Exception:
            warnings_count = 0
            inbox_counts = {"dialogs": 0, "quotes": 0}
    return {
        "request": request,
        "user": request.session.get("user"),
        "flashes": _pop_flashes(request),
        "static_version": STATIC_VERSION,
        "inbox_counts": inbox_counts,
        "warnings_count": warnings_count,
        "account_checks_enabled": ACCOUNT_CHECKS_ENABLED,
        "projects": settings.get("projects", []) or [],
        "active_project": active_project,
        "active_project_id": active_project.get("id", DEFAULT_PROJECT_ID),
        **extra,
    }


def _collect_warnings(accounts: List[Dict[str, Any]], settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    warnings: List[Dict[str, Any]] = []

    targets = _filter_by_project(settings.get("targets", []) or [], project_id)
    target_by_id: Dict[str, Dict[str, Any]] = {}
    for t in targets:
        if not isinstance(t, dict):
            continue
        if t.get("chat_id"):
            target_by_id[str(t["chat_id"])] = t
        if t.get("linked_chat_id"):
            target_by_id[str(t["linked_chat_id"])] = t

    for acc in accounts:
        session_name = acc.get("session_name") or ""
        status = str(acc.get("status") or "").lower().strip()
        if status in {"banned", "frozen"}:
            warnings.append(
                {
                    "level": "danger",
                    "title": f"{session_name}: статус {status}",
                    "detail": "Аккаунт заблокирован Telegram и не может выполнять действия.",
                    "session_name": session_name,
                    "key": _warning_key_status(session_name, status),
                }
            )
        elif status in {"limited", "human_check", "unauthorized"}:
            warnings.append(
                {
                    "level": "warning",
                    "title": f"{session_name}: статус {status}",
                    "detail": "Аккаунт имеет ограничения или требует проверки.",
                    "session_name": session_name,
                    "key": _warning_key_status(session_name, status),
                }
            )

    sessions = [a.get("session_name") for a in accounts if a.get("session_name")]
    failures = _load_account_failures(sessions, min_count=WARNING_FAILURE_THRESHOLD)
    for f in failures:
        session = f.get("session_name") or ""
        kind = f.get("kind") or "action"
        count = f.get("count") or 0
        last_error = f.get("last_error") or ""
        last_attempt = f.get("last_attempt")
        last_target = str(f.get("last_target") or "")
        target = target_by_id.get(last_target)
        target_url = None
        target_label = None
        if target:
            target_url = f"/targets/{quote(str(target.get('chat_id')))}"
            target_label = target.get("chat_name") or target.get("chat_id")
        kind_human = _humanize_failure_kind(kind)
        ctx_human = _humanize_failure_context(kind, last_target)
        warnings.append(
            {
                "level": "warning",
                "title": f"{session}: повторные ошибки ({kind_human})",
                "detail_lines": [
                    f"Тип: {kind_human} ({kind})",
                    f"Повторов: {count}",
                    f"Последняя попытка: {_human_dt(last_attempt)}" if last_attempt else None,
                    f"Контекст: {ctx_human}" if ctx_human else None,
                    f"Ошибка: {_humanize_failure_error(kind, last_error)}",
                ],
                "session_name": session,
                "target_url": target_url,
                "target_label": target_label,
                "key": _warning_key_failure(session, kind),
                "action": {
                    "label": "Открыть аккаунт",
                    "url": f"/accounts/{quote(str(session))}",
                },
            }
        )

    return warnings


def _warnings_count(accounts: List[Dict[str, Any]], settings: Dict[str, Any]) -> int:
    try:
        warnings = _collect_warnings(accounts, settings)
        keys = [w.get("key") for w in warnings if w.get("key")]
        seen = _load_seen_warning_keys(keys)
        return sum(1 for k in keys if k not in seen)
    except Exception:
        return 0


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, next: str = "/"):
    return templates.TemplateResponse("login.html", _template_context(request, next=next))


@app.post("/login")
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


@app.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return _redirect("/login")


@app.get("/projects", response_class=HTMLResponse)
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


@app.post("/projects/new")
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


@app.post("/projects/select")
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


@app.post("/projects/{project_id}/rename")
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


@app.post("/projects/{project_id}/delete")
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


@app.post("/projects/move")
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
    moved_reaction_targets: List[Dict[str, Any]] = []
    moved_monitor_targets: List[Dict[str, Any]] = []

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
            moved_reaction_targets.append(t)

    if move_monitor_targets:
        selected_set = {s for s in (selected_monitor_targets or []) if s}
        for t in settings.get("monitor_targets", []) or []:
            if _project_id_for(t) != source_project_id:
                continue
            if selected_set and str(t.get("chat_id")) not in selected_set:
                continue
            t["project_id"] = dest_project_id
            moved_monitor += 1
            moved_monitor_targets.append(t)

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

    for t in moved_reaction_targets:
        assigned = [s for s in (t.get("assigned_accounts") or []) if s in dest_sessions]
        t["assigned_accounts"] = assigned

    for t in moved_monitor_targets:
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


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    settings, settings_err = _load_settings()
    accounts, accounts_err = _load_accounts()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)

    with _db_connect() as conn:
        proxies_total = conn.execute("SELECT COUNT(*) AS c FROM proxies").fetchone()["c"]
        proxies_active = conn.execute("SELECT COUNT(*) AS c FROM proxies WHERE status='active'").fetchone()["c"]
        triggers_total = conn.execute("SELECT COUNT(*) AS c FROM triggers").fetchone()["c"]
        scenarios_total = conn.execute("SELECT COUNT(*) AS c FROM scenarios").fetchone()["c"]

    return templates.TemplateResponse(
        "dashboard.html",
        _template_context(
            request,
            settings=settings,
            settings_err=settings_err,
            accounts=accounts,
            accounts_err=accounts_err,
            proxies_total=proxies_total,
            proxies_active=proxies_active,
            triggers_total=triggers_total,
            scenarios_total=scenarios_total,
        ),
    )


@app.get("/guide", response_class=HTMLResponse)
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


@app.get("/warnings", response_class=HTMLResponse)
async def warnings_page(request: Request):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    warnings = _collect_warnings(accounts, settings)
    keys = [w.get("key") for w in warnings if w.get("key")]
    seen = _load_seen_warning_keys(keys)
    for w in warnings:
        key = w.get("key")
        if key:
            w["is_new"] = key not in seen
    _mark_warning_keys_seen(keys)
    return templates.TemplateResponse(
        "warnings.html",
        _template_context(request, warnings=warnings),
    )


@app.post("/status/start")
async def status_start(request: Request):
    settings, _ = _load_settings()
    settings["status"] = "running"
    _save_settings(settings)
    _flash(request, "success", "Комментатор запущен (commentator.py подхватит в течение нескольких секунд).")
    return _redirect("/")


@app.post("/status/stop")
async def status_stop(request: Request):
    settings, _ = _load_settings()
    settings["status"] = "stopped"
    _save_settings(settings)
    _flash(request, "success", "Комментатор остановлен.")
    return _redirect("/")


@app.get("/settings/ai", response_class=HTMLResponse)
async def ai_settings_page(request: Request):
    settings, settings_err = _load_settings()
    keys = settings.get("api_keys", {})
    return templates.TemplateResponse(
        "ai_settings.html",
        _template_context(
            request,
            settings=settings,
            settings_err=settings_err,
            api_keys_masked={k: _mask_secret(v) for k, v in keys.items()},
        ),
    )


@app.get("/settings/ai/models", response_class=HTMLResponse)
async def ai_models_page(request: Request, provider: str = "", q: str = ""):
    settings, settings_err = _load_settings()
    provider = provider.strip() or settings.get("ai_provider", "deepseek")
    if provider not in {"gemini", "openai", "openrouter", "deepseek"}:
        raise HTTPException(status_code=400, detail="Некорректный провайдер")

    api_key = (settings.get("api_keys", {}) or {}).get(provider)
    query = q.strip().lower()

    models: List[Dict[str, str]] = []
    models_err: str | None = None

    if provider != "openrouter" and not api_key:
        models_err = f"Для провайдера {provider.upper()} не задан API ключ."
    else:
        try:
            if provider == "gemini":
                async with genai.Client(api_key=api_key, http_options={"timeout": 10_000}).aio as aclient:
                    pager = await aclient.models.list(config={"page_size": 200})
                    items: List[Any] = []
                    if hasattr(pager, "__aiter__"):
                        async for item in pager:
                            items.append(item)
                            if len(items) >= 200:
                                break
                    else:
                        for item in pager:
                            items.append(item)
                            if len(items) >= 200:
                                break

                for item in items:
                    raw_name = getattr(item, "name", None) or str(item)
                    model_id = raw_name.split("/")[-1] if isinstance(raw_name, str) else str(raw_name)
                    if query and query not in model_id.lower() and query not in str(raw_name).lower():
                        continue
                    methods = getattr(item, "supported_generation_methods", None)
                    meta = ""
                    if isinstance(methods, (list, tuple)) and methods:
                        meta = ", ".join(str(m) for m in methods)
                    models.append({"id": model_id, "raw": str(raw_name), "meta": meta})
            elif provider == "openrouter":
                headers = {}
                if api_key:
                    headers["Authorization"] = f"Bearer {api_key}"
                async with httpx.AsyncClient(timeout=10.0) as http_client:
                    resp = await http_client.get("https://openrouter.ai/api/v1/models", headers=headers)
                    resp.raise_for_status()
                    payload = resp.json()

                items = payload.get("data", []) if isinstance(payload, dict) else []
                limit = 500 if query else 200
                count = 0
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    model_id = str(item.get("id") or "").strip()
                    if not model_id:
                        continue
                    name = str(item.get("name") or model_id)
                    haystack = f"{model_id} {name}".lower()
                    if query and query not in haystack:
                        continue

                    meta_parts: List[str] = []
                    ctx = item.get("context_length")
                    if isinstance(ctx, int) and ctx > 0:
                        meta_parts.append(f"ctx={ctx}")
                    top = item.get("top_provider") if isinstance(item.get("top_provider"), dict) else {}
                    max_out = top.get("max_completion_tokens") if isinstance(top, dict) else None
                    if isinstance(max_out, int) and max_out > 0:
                        meta_parts.append(f"max_out={max_out}")
                    params = item.get("supported_parameters")
                    if isinstance(params, list) and params:
                        short = ", ".join(str(p) for p in params[:10])
                        meta_parts.append(f"params={short}{'…' if len(params) > 10 else ''}")

                    models.append({"id": model_id, "raw": name, "meta": " · ".join(meta_parts)})
                    count += 1
                    if count >= limit:
                        break
            else:
                base_url = "https://api.deepseek.com" if provider == "deepseek" else None
                client = openai.AsyncOpenAI(api_key=api_key, base_url=base_url, timeout=10.0)
                resp = await client.models.list()
                for item in getattr(resp, "data", []) or []:
                    model_id = getattr(item, "id", None) or str(item)
                    if query and query not in str(model_id).lower():
                        continue
                    models.append({"id": str(model_id), "raw": str(model_id), "meta": ""})
        except Exception as e:
            models_err = str(e)

    return templates.TemplateResponse(
        "ai_models.html",
        _template_context(
            request,
            settings=settings,
            settings_err=settings_err,
            provider=provider,
            q=q,
            models=models,
            models_err=models_err,
        ),
    )


@app.post("/settings/ai/provider")
async def ai_settings_provider(request: Request, ai_provider: str = Form(...)):
    if ai_provider not in {"gemini", "openai", "openrouter", "deepseek"}:
        raise HTTPException(status_code=400, detail="Некорректный провайдер")
    settings, _ = _load_settings()
    settings["ai_provider"] = ai_provider
    _save_settings(settings)
    _flash(request, "success", f"Провайдер по умолчанию: {ai_provider.upper()}")
    return _redirect("/settings/ai")


@app.post("/settings/ai/api-keys")
async def ai_settings_api_keys(
    request: Request,
    gemini_key: str = Form(""),
    openai_key: str = Form(""),
    openrouter_key: str = Form(""),
    deepseek_key: str = Form(""),
    clear_gemini: Optional[str] = Form(None),
    clear_openai: Optional[str] = Form(None),
    clear_openrouter: Optional[str] = Form(None),
    clear_deepseek: Optional[str] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    settings.setdefault("api_keys", {})

    if clear_gemini:
        settings["api_keys"].pop("gemini", None)
    elif gemini_key.strip():
        settings["api_keys"]["gemini"] = gemini_key.strip()

    if clear_openai:
        settings["api_keys"].pop("openai", None)
    elif openai_key.strip():
        settings["api_keys"]["openai"] = openai_key.strip()

    if clear_openrouter:
        settings["api_keys"].pop("openrouter", None)
    elif openrouter_key.strip():
        settings["api_keys"]["openrouter"] = openrouter_key.strip()

    if clear_deepseek:
        settings["api_keys"].pop("deepseek", None)
    elif deepseek_key.strip():
        settings["api_keys"]["deepseek"] = deepseek_key.strip()

    _save_settings(settings)
    _flash(request, "success", "API ключи обновлены.")
    return _redirect("/settings/ai")


@app.post("/settings/ai/models")
async def ai_settings_models(
    request: Request,
    openai_chat: str = Form(""),
    openai_eval: str = Form(""),
    openai_image: str = Form(""),
    openrouter_chat: str = Form(""),
    openrouter_eval: str = Form(""),
    deepseek_chat: str = Form(""),
    deepseek_eval: str = Form(""),
    gemini_chat: str = Form(""),
    gemini_eval: str = Form(""),
    gemini_names: str = Form(""),
    reset_models: Optional[str] = Form(None),
):
    settings, _ = _load_settings()

    if reset_models:
        settings["models"] = dict(DEFAULT_MODELS)
        _save_settings(settings)
        _flash(request, "success", "Модели сброшены к рекомендуемым.")
        return _redirect("/settings/ai")

    models = settings.get("models")
    if not isinstance(models, dict):
        models = {}

    def set_model(value: str, key: str) -> None:
        v = value.strip()
        if v:
            models[key] = v

    set_model(openai_chat, "openai_chat")
    set_model(openai_eval, "openai_eval")
    set_model(openai_image, "openai_image")
    set_model(openrouter_chat, "openrouter_chat")
    set_model(openrouter_eval, "openrouter_eval")
    set_model(deepseek_chat, "deepseek_chat")
    set_model(deepseek_eval, "deepseek_eval")
    set_model(gemini_chat, "gemini_chat")
    set_model(gemini_eval, "gemini_eval")
    set_model(gemini_names, "gemini_names")

    settings["models"] = models
    _save_settings(settings)
    _flash(request, "success", "Модели обновлены.")
    return _redirect("/settings/ai")


@app.get("/settings/humanization", response_class=HTMLResponse)
async def humanization_page(request: Request):
    settings, settings_err = _load_settings()
    h = settings.get("humanization", {}) or {}
    pk = settings.get("product_knowledge", {}) or {}
    return templates.TemplateResponse(
        "humanization.html",
        _template_context(request, settings_err=settings_err, h=h, pk=pk, settings=settings),
    )


@app.post("/settings/humanization")
async def humanization_save(
    request: Request,
    temperature: str = Form(""),
    repetition_penalty: str = Form("0"),
    typo_chance: str = Form("0"),
    lowercase_chance: str = Form("80"),
    split_chance: str = Form("60"),
    comma_skip_chance: str = Form("30"),
    max_words: str = Form("20"),
    max_tokens: str = Form("60"),
    similarity_threshold: str = Form("0.78"),
    similarity_max_retries: str = Form("1"),
    short_post_diversify: str | None = Form(None),
    short_post_diversity_words: str = Form("10"),
    short_post_min_new_tokens: str = Form("2"),
    custom_rules: str = Form(""),
    product_knowledge_prompt: str = Form(""),
):
    settings, _ = _load_settings()
    settings.setdefault("humanization", {})
    settings.setdefault("product_knowledge", {})

    settings["humanization"]["temperature"] = _parse_float_field(
        request,
        temperature,
        default=None,
        label="Температура",
        min_value=0.0,
        max_value=2.0,
    )
    settings["humanization"]["repetition_penalty"] = _parse_int_field(
        request, repetition_penalty, default=0, label="Штраф повторов", min_value=0, max_value=100
    )
    settings["humanization"]["typo_chance"] = _parse_int_field(
        request, typo_chance, default=0, label="Опечатки", min_value=0, max_value=100
    )
    settings["humanization"]["lowercase_chance"] = _parse_int_field(
        request, lowercase_chance, default=80, label="lowercase", min_value=0, max_value=100
    )
    settings["humanization"]["split_chance"] = _parse_int_field(
        request, split_chance, default=60, label="Разбив", min_value=0, max_value=100
    )
    settings["humanization"]["comma_skip_chance"] = _parse_int_field(
        request, comma_skip_chance, default=30, label="Пропуск запятых", min_value=0, max_value=100
    )
    settings["humanization"]["max_words"] = _parse_int_field(request, max_words, default=20, label="Макс. слов", min_value=0)
    settings["humanization"]["max_tokens"] = _parse_int_field(
        request, max_tokens, default=60, label="Лимит ответа (токены)", min_value=1
    )
    settings["humanization"]["similarity_threshold"] = _parse_float_field(
        request,
        similarity_threshold,
        default=0.78,
        label="Порог схожести",
        min_value=0.0,
        max_value=1.0,
    )
    settings["humanization"]["similarity_max_retries"] = _parse_int_field(
        request,
        similarity_max_retries,
        default=1,
        label="Перегенераций",
        min_value=0,
        max_value=3,
    )

    settings["humanization"]["short_post_diversify"] = _parse_bool(short_post_diversify, default=False)
    settings["humanization"]["short_post_diversity_words"] = _parse_int_field(
        request,
        short_post_diversity_words,
        default=10,
        label="Короткий пост (смысловых слов)",
        min_value=0,
        max_value=50,
    )
    settings["humanization"]["short_post_min_new_tokens"] = _parse_int_field(
        request,
        short_post_min_new_tokens,
        default=2,
        label="Мин. новых смысловых слов",
        min_value=0,
        max_value=6,
    )

    rules = custom_rules.strip()
    if rules:
        settings["humanization"]["custom_rules"] = rules
    else:
        settings["humanization"].pop("custom_rules", None)

    pk_prompt = str(product_knowledge_prompt or "").strip()
    if pk_prompt:
        settings["product_knowledge"]["prompt"] = pk_prompt
    else:
        try:
            settings["product_knowledge"].pop("prompt", None)
        except Exception:
            pass

    _save_settings(settings)
    _flash(request, "success", "Общие параметры обновлены.")
    return _redirect("/settings/humanization")


@app.get("/settings/blacklist", response_class=HTMLResponse)
async def blacklist_page(request: Request):
    settings, settings_err = _load_settings()
    blacklist = settings.get("blacklist", []) or []
    return templates.TemplateResponse(
        "blacklist.html",
        _template_context(request, settings_err=settings_err, blacklist=blacklist),
    )


@app.post("/settings/blacklist/add")
async def blacklist_add(request: Request, words: str = Form(...)):
    settings, _ = _load_settings()
    settings.setdefault("blacklist", [])
    existing_lower = {w.lower() for w in settings["blacklist"] if isinstance(w, str)}
    added = 0
    for w in words.replace("\n", ",").split(","):
        w = w.strip()
        if not w:
            continue
        if w.lower() in existing_lower:
            continue
        settings["blacklist"].append(w)
        existing_lower.add(w.lower())
        added += 1
    _save_settings(settings)
    _flash(request, "success", f"Добавлено слов: {added}")
    return _redirect("/settings/blacklist")


@app.post("/settings/blacklist/delete")
async def blacklist_delete(request: Request, word: str = Form(...)):
    settings, _ = _load_settings()
    settings.setdefault("blacklist", [])
    settings["blacklist"] = [w for w in settings["blacklist"] if str(w) != word]
    _save_settings(settings)
    _flash(request, "success", f"Удалено: {word}")
    return _redirect("/settings/blacklist")


@app.post("/settings/blacklist/clear")
async def blacklist_clear(request: Request):
    settings, _ = _load_settings()
    settings["blacklist"] = []
    _save_settings(settings)
    _flash(request, "success", "Чёрный список очищен.")
    return _redirect("/settings/blacklist")


@app.get("/accounts", response_class=HTMLResponse)
async def accounts_page(request: Request):
    accounts, accounts_err = _load_accounts()
    if _ensure_accounts_date_added(accounts):
        _save_accounts(accounts)
    settings, _ = _load_settings()
    _ensure_accounts_roles_saved(accounts, settings)
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    role_names = _role_name_map(settings)

    with _db_connect() as conn:
        proxies = conn.execute(
            "SELECT id, url, name, ip, country, status FROM proxies ORDER BY id DESC"
        ).fetchall()
    proxy_names = {p["url"]: p["name"] for p in proxies if p["name"]}

    return templates.TemplateResponse(
        "accounts.html",
        _template_context(
            request,
            accounts=accounts,
            accounts_err=accounts_err,
            role_names=role_names,
            proxies=proxies,
            proxy_names=proxy_names,
        ),
    )


@app.post("/accounts/check")
async def accounts_check(request: Request, auto_pause: Optional[str] = Form(None)):
    if not ACCOUNT_CHECKS_ENABLED:
        _flash(request, "warning", "Проверки аккаунтов отключены.")
        return _redirect("/accounts")
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    if not _filter_accounts_by_project(accounts, project_id):
        _flash(request, "warning", "Нет аккаунтов для проверки.")
        return _redirect("/accounts")

    auto_pause = _parse_bool(auto_pause, default=True)
    api_id_default, api_hash_default = _telethon_credentials()
    status_counts = {
        "active": 0,
        "banned": 0,
        "unauthorized": 0,
        "limited": 0,
        "frozen": 0,
        "human_check": 0,
    }
    errors = 0

    async with _auto_pause_commentator(request, auto_pause=auto_pause, reason="Проверка аккаунтов"):
        for acc in accounts:
            if _project_id_for(acc) != project_id:
                continue
            status, is_error = await _check_account_entry(
                acc,
                api_id_default,
                api_hash_default,
            )
            if status in status_counts:
                status_counts[status] += 1
            elif not is_error:
                status_counts["active"] += 1
            if is_error:
                errors += 1

    _save_accounts(accounts)
    _flash(
        request,
        "success",
        "Проверка завершена: "
        f"OK={status_counts['active']}, "
        f"limited={status_counts['limited']}, "
        f"frozen={status_counts['frozen']}, "
        f"human_check={status_counts['human_check']}, "
        f"banned={status_counts['banned']}, "
        f"unauthorized={status_counts['unauthorized']}, "
        f"errors={errors}",
    )
    return _redirect("/accounts")


@app.get("/accounts/new", response_class=HTMLResponse)
async def account_new_page(request: Request):
    settings, _ = _load_settings()
    roles = _sorted_role_items(settings)
    default_role_id = _default_role_id(settings)
    with _db_connect() as conn:
        proxies = conn.execute(
            "SELECT id, ip, country, url, name FROM proxies WHERE status='active' ORDER BY id DESC"
        ).fetchall()
    return templates.TemplateResponse(
        "account_new.html",
        _template_context(request, roles=roles, default_role_id=default_role_id, proxies=proxies),
    )


@app.post("/accounts/new/session")
async def account_new_session(
    request: Request,
    session_name: str = Form(...),
    session_string: str = Form(...),
    proxy_id: str = Form(""),
    role_id: str = Form(""),
):
    session_name = session_name.strip()
    session_string = session_string.strip()
    if not session_name:
        raise HTTPException(status_code=400, detail="session_name пустой")
    if not session_string:
        raise HTTPException(status_code=400, detail="session_string пустой")

    accounts, _ = _load_accounts()
    if any(a.get("session_name") == session_name for a in accounts):
        raise HTTPException(status_code=400, detail="Аккаунт с таким session_name уже существует")

    settings, _ = _load_settings()
    selected_role_id = _resolve_role_id(settings, role_id)
    if not selected_role_id:
        raise HTTPException(status_code=400, detail="Не найдена роль по умолчанию")

    proxy_url: str | None = None
    if proxy_id.strip():
        try:
            proxy_id_int = int(proxy_id)
        except ValueError:
            proxy_id_int = None
            _flash(request, "warning", "Прокси: некорректный ID, будет добавлено без прокси.")
        if proxy_id_int is not None:
            with _db_connect() as conn:
                row = conn.execute("SELECT url FROM proxies WHERE id = ?", (proxy_id_int,)).fetchone()
                if row:
                    proxy_url = row["url"]

    api_id, api_hash = _telethon_credentials()
    proxy_tuple = _parse_proxy_tuple(proxy_url) if proxy_url else None
    tmp_acc: Dict[str, Any] = {"session_name": session_name, "session_string": session_string}
    ensure_device_profile(tmp_acc)
    client = TelegramClient(
        StringSession(session_string),
        api_id,
        api_hash,
        proxy=proxy_tuple,
        **device_kwargs(tmp_acc),
    )
    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise HTTPException(status_code=400, detail="Сессия не авторизована")
        me = await client.get_me()
    finally:
        if client.is_connected():
            await client.disconnect()

    settings, _ = _load_settings()
    selected_role_id = _resolve_role_id(settings, role_id)
    if not selected_role_id:
        raise HTTPException(status_code=400, detail="Не найдена роль по умолчанию")
    project_id = _active_project_id(settings)
    new_acc: Dict[str, Any] = {
        "session_name": session_name,
        "session_string": session_string,
        "user_id": me.id,
        "first_name": me.first_name,
        "last_name": me.last_name or "",
        "username": me.username or "",
        "status": "active",
        "project_id": project_id,
        "date_added": datetime.now(timezone.utc).isoformat(),
        "role_id": selected_role_id,
    }
    for key in ("device_type", "device_model", "system_version", "app_version", "lang_code", "system_lang_code"):
        if tmp_acc.get(key):
            new_acc[key] = tmp_acc[key]
    if proxy_url:
        new_acc["proxy_url"] = proxy_url

    accounts.append(new_acc)
    _save_accounts(accounts)

    _flash(request, "success", f"Аккаунт '{session_name}' добавлен.")
    return _redirect("/accounts")


@dataclass
class _PhoneLoginState:
    token: str
    created_at: float
    client: TelegramClient
    session_name: str
    phone: str
    phone_code_hash: str
    proxy_url: str | None
    device_profile: Dict[str, Any]
    role_id: str


PHONE_LOGINS: Dict[str, _PhoneLoginState] = {}


def _phone_logins_gc(max_age_seconds: int = 10 * 60) -> None:
    now = time.time()
    for token, st in list(PHONE_LOGINS.items()):
        if now - st.created_at > max_age_seconds:
            try:
                if st.client.is_connected():
                    asyncio.create_task(st.client.disconnect())
            except Exception:
                pass
            PHONE_LOGINS.pop(token, None)


@app.post("/accounts/new/phone/start", response_class=HTMLResponse)
async def account_new_phone_start(
    request: Request,
    session_name: str = Form(...),
    phone: str = Form(...),
    proxy_id: str = Form(""),
    role_id: str = Form(""),
):
    _phone_logins_gc()

    session_name = session_name.strip()
    phone = phone.strip()
    if not session_name or not phone:
        raise HTTPException(status_code=400, detail="Нужно указать session_name и phone")

    accounts, _ = _load_accounts()
    if any(a.get("session_name") == session_name for a in accounts):
        raise HTTPException(status_code=400, detail="Аккаунт с таким session_name уже существует")

    settings, _ = _load_settings()
    selected_role_id = _resolve_role_id(settings, role_id)
    if not selected_role_id:
        raise HTTPException(status_code=400, detail="Не найдена роль по умолчанию")

    proxy_url: str | None = None
    if proxy_id.strip():
        try:
            proxy_id_int = int(proxy_id)
        except ValueError:
            proxy_id_int = None
            _flash(request, "warning", "Прокси: некорректный ID, вход будет без прокси.")
        if proxy_id_int is not None:
            with _db_connect() as conn:
                row = conn.execute("SELECT url FROM proxies WHERE id = ?", (proxy_id_int,)).fetchone()
                if row:
                    proxy_url = row["url"]

    api_id, api_hash = _telethon_credentials()
    proxy_tuple = _parse_proxy_tuple(proxy_url) if proxy_url else None
    tmp_acc: Dict[str, Any] = {"session_name": session_name, "phone": phone}
    ensure_device_profile(tmp_acc)
    device_profile = {k: tmp_acc.get(k) for k in ("device_type", "device_model", "system_version", "app_version", "lang_code", "system_lang_code") if tmp_acc.get(k)}

    client = TelegramClient(
        StringSession(),
        api_id,
        api_hash,
        proxy=proxy_tuple,
        **device_kwargs(tmp_acc),
    )
    await client.connect()

    try:
        sent_code = await client.send_code_request(phone)
    except RPCError as e:
        await client.disconnect()
        raise HTTPException(status_code=400, detail=f"Ошибка Telegram API: {e}") from e

    token = uuid.uuid4().hex
    PHONE_LOGINS[token] = _PhoneLoginState(
        token=token,
        created_at=time.time(),
        client=client,
        session_name=session_name,
        phone=phone,
        phone_code_hash=sent_code.phone_code_hash,
        proxy_url=proxy_url,
        device_profile=device_profile,
        role_id=selected_role_id,
    )

    return templates.TemplateResponse(
        "account_phone_code.html",
        _template_context(request, token=token, session_name=session_name, phone=phone),
    )


@app.post("/accounts/new/phone/{token}/cancel")
async def account_new_phone_cancel(request: Request, token: str):
    st = PHONE_LOGINS.pop(token, None)
    if st:
        try:
            if st.client.is_connected():
                await st.client.disconnect()
        except Exception:
            pass
    _flash(request, "success", "Вход по телефону отменён.")
    return _redirect("/accounts/new")


@app.post("/accounts/new/phone/{token}/code", response_class=HTMLResponse)
async def account_new_phone_code(request: Request, token: str, code: str = Form(...)):
    st = PHONE_LOGINS.get(token)
    if not st:
        _flash(request, "danger", "Сессия входа устарела. Начните заново.")
        return _redirect("/accounts/new")

    code = code.strip()
    try:
        await st.client.sign_in(st.phone, code, phone_code_hash=st.phone_code_hash)
    except (PhoneCodeInvalidError, PhoneCodeExpiredError):
        _flash(request, "danger", "Неверный или истекший код. Попробуйте снова.")
        return templates.TemplateResponse(
            "account_phone_code.html",
            _template_context(request, token=token, session_name=st.session_name, phone=st.phone),
        )
    except SessionPasswordNeededError:
        return templates.TemplateResponse(
            "account_phone_password.html",
            _template_context(request, token=token, session_name=st.session_name, phone=st.phone),
        )
    except Exception as e:
        PHONE_LOGINS.pop(token, None)
        try:
            if st.client.is_connected():
                await st.client.disconnect()
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=f"Ошибка входа: {e}") from e

    return await _finalize_phone_login(request, token)


@app.post("/accounts/new/phone/{token}/password", response_class=HTMLResponse)
async def account_new_phone_password(request: Request, token: str, password: str = Form(...)):
    st = PHONE_LOGINS.get(token)
    if not st:
        _flash(request, "danger", "Сессия входа устарела. Начните заново.")
        return _redirect("/accounts/new")

    try:
        await st.client.sign_in(password=password.strip())
    except PasswordHashInvalidError:
        _flash(request, "danger", "Неверный пароль 2FA. Попробуйте снова.")
        return templates.TemplateResponse(
            "account_phone_password.html",
            _template_context(request, token=token, session_name=st.session_name, phone=st.phone),
        )
    except Exception as e:
        PHONE_LOGINS.pop(token, None)
        try:
            if st.client.is_connected():
                await st.client.disconnect()
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=f"Ошибка 2FA: {e}") from e

    return await _finalize_phone_login(request, token)


async def _finalize_phone_login(request: Request, token: str):
    st = PHONE_LOGINS.pop(token, None)
    if not st:
        _flash(request, "danger", "Сессия входа устарела. Начните заново.")
        return _redirect("/accounts/new")

    try:
        me = await st.client.get_me()
        session_string = st.client.session.save()
    finally:
        try:
            if st.client.is_connected():
                await st.client.disconnect()
        except Exception:
            pass

    settings, _ = _load_settings()
    selected_role_id = _resolve_role_id(settings, st.role_id)
    if not selected_role_id:
        raise HTTPException(status_code=400, detail="Не найдена роль по умолчанию")
    project_id = _active_project_id(settings)
    accounts, _ = _load_accounts()
    _ensure_accounts_roles_saved(accounts, settings)
    new_acc: Dict[str, Any] = {
        "session_name": st.session_name,
        "session_string": session_string,
        "user_id": me.id,
        "first_name": me.first_name,
        "last_name": me.last_name or "",
        "username": me.username or "",
        "status": "active",
        "project_id": project_id,
        "date_added": datetime.now(timezone.utc).isoformat(),
        "role_id": selected_role_id,
        **({"proxy_url": st.proxy_url} if st.proxy_url else {}),
        **(st.device_profile or {}),
    }
    accounts.append(new_acc)
    _save_accounts(accounts)

    _flash(request, "success", f"Аккаунт '{st.session_name}' добавлен.")
    return _redirect("/accounts")


@app.get("/accounts/{session_name}", response_class=HTMLResponse)
async def account_edit_page(request: Request, session_name: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    _ensure_accounts_roles_saved(accounts, settings)
    project_id = _active_project_id(settings)
    account = next(
        (a for a in accounts if a.get("session_name") == session_name and _project_id_for(a) == project_id),
        None,
    )
    if not account:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    def _is_account_awake(acc: Dict[str, Any], hour: int) -> bool:
        ss = acc.get("sleep_settings") or {}
        try:
            start_hour = int(ss.get("start_hour", 8))
        except (TypeError, ValueError):
            start_hour = 8
        try:
            end_hour = int(ss.get("end_hour", 23))
        except (TypeError, ValueError):
            end_hour = 23

        start_hour = max(0, min(23, start_hour))
        end_hour = max(0, min(23, end_hour))

        if start_hour == end_hour:
            return True
        if start_hour < end_hour:
            return start_hour <= hour < end_hour
        return hour >= start_hour or hour < end_hour

    roles = _sorted_role_items(settings)
    default_role_id = _default_role_id(settings)
    profile_task = None
    tasks = settings.get("profile_tasks")
    if isinstance(tasks, dict):
        profile_task = tasks.get(session_name)

    with _db_connect() as conn:
        proxies = conn.execute(
            "SELECT id, ip, country, url, name FROM proxies WHERE status='active' ORDER BY id DESC"
        ).fetchall()

    server_now = datetime.now(timezone.utc)
    server_now_label = server_now.strftime("%Y-%m-%d %H:%M UTC")
    server_hour = server_now.hour
    awake_now = _is_account_awake(account, server_hour)
    has_custom_role = isinstance(account.get(ACCOUNT_CUSTOM_ROLE_KEY), dict) and bool(account.get(ACCOUNT_CUSTOM_ROLE_KEY))

    comment_targets = _filter_by_project(settings.get("targets", []) or [], project_id)
    reaction_targets = _filter_by_project(settings.get("reaction_targets", []) or [], project_id)
    monitor_targets = _filter_by_project(settings.get("monitor_targets", []) or [], project_id)

    def _sort_targets(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(
            items,
            key=lambda t: (
                str(t.get("chat_name") or "").lower(),
                str(t.get("chat_username") or "").lower(),
                str(t.get("chat_id") or ""),
            ),
        )

    comment_targets = _sort_targets(comment_targets)
    reaction_targets = _sort_targets(reaction_targets)
    monitor_targets = _sort_targets(monitor_targets)

    join_target_ids: List[str] = []
    for t in comment_targets:
        if not isinstance(t, dict):
            continue
        if t.get("chat_id"):
            join_target_ids.append(str(t.get("chat_id")))
        if t.get("linked_chat_id"):
            join_target_ids.append(str(t.get("linked_chat_id")))
    join_status = _load_join_status(join_target_ids)

    return templates.TemplateResponse(
        "account_edit.html",
        _template_context(
            request,
            account=account,
            proxies=proxies,
            roles=roles,
            default_role_id=default_role_id,
            custom_role_id=CUSTOM_ROLE_ID,
            custom_role_name=CUSTOM_ROLE_NAME,
            has_custom_role=has_custom_role,
            profile_task=profile_task,
            server_now=server_now_label,
            server_hour=server_hour,
            awake_now=awake_now,
            comment_targets=comment_targets,
            reaction_targets=reaction_targets,
            monitor_targets=monitor_targets,
            join_status=join_status,
        ),
    )


@app.post("/accounts/{session_name}/sleep")
async def account_update_sleep(
    request: Request,
    session_name: str,
    start_hour: str = Form(...),
    end_hour: str = Form(...),
):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    a = accounts[idx]
    a.setdefault("sleep_settings", {"start_hour": 8, "end_hour": 23})
    start_val = _parse_int(start_hour, default=8)
    end_val = _parse_int(end_hour, default=23)
    if start_val is None:
        start_val = 8
    if end_val is None:
        end_val = 23
    a["sleep_settings"]["start_hour"] = max(0, min(23, int(start_val)))
    a["sleep_settings"]["end_hour"] = max(0, min(23, int(end_val)))
    _save_accounts(accounts)
    _flash(request, "success", "Время сна обновлено.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.post("/accounts/{session_name}/proxy")
async def account_update_proxy(
    request: Request,
    session_name: str,
    proxy_id: str = Form(""),
):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    proxy_url: str | None = None
    if proxy_id.strip():
        try:
            proxy_id_int = int(proxy_id)
        except ValueError:
            proxy_id_int = None
            _flash(request, "warning", "Прокси: некорректный ID, значение не изменено.")
        if proxy_id_int is not None:
            with _db_connect() as conn:
                row = conn.execute("SELECT url FROM proxies WHERE id = ?", (proxy_id_int,)).fetchone()
                if row:
                    proxy_url = row["url"]

    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    if proxy_url:
        accounts[idx]["proxy_url"] = proxy_url
    else:
        accounts[idx].pop("proxy_url", None)
    _save_accounts(accounts)
    _flash(request, "success", "Прокси обновлён.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.post("/accounts/{session_name}/targets")
async def account_update_targets(
    request: Request,
    session_name: str,
    comment_target_ids: Optional[List[str]] = Form(None),
    reaction_target_ids: Optional[List[str]] = Form(None),
    monitor_target_ids: Optional[List[str]] = Form(None),
):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    if _find_account_index(accounts, session_name, project_id) is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    selected_comment = {str(x).strip() for x in (comment_target_ids or []) if str(x).strip()}
    selected_reaction = {str(x).strip() for x in (reaction_target_ids or []) if str(x).strip()}
    selected_monitor = {str(x).strip() for x in (monitor_target_ids or []) if str(x).strip()}

    def _normalize_assigned_accounts(value: Any) -> List[str]:
        if not isinstance(value, list):
            return []
        result: List[str] = []
        seen: set[str] = set()
        for item in value:
            s = str(item or "").strip()
            if not s or s in seen:
                continue
            seen.add(s)
            result.append(s)
        return result

    def _apply(key: str, selected_ids: set[str]) -> int:
        items = settings.get(key)
        if not isinstance(items, list):
            return 0
        changed = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            if _project_id_for(item) != project_id:
                continue
            chat_id = str(item.get("chat_id") or "").strip()
            if not chat_id:
                continue
            assigned = _normalize_assigned_accounts(item.get("assigned_accounts"))
            should_assign = chat_id in selected_ids
            has_assign = session_name in assigned
            if should_assign and not has_assign:
                assigned.append(session_name)
                item["assigned_accounts"] = assigned
                changed += 1
            elif not should_assign and has_assign:
                item["assigned_accounts"] = [s for s in assigned if s != session_name]
                changed += 1
        return changed

    changed = 0
    changed += _apply("targets", selected_comment)
    changed += _apply("reaction_targets", selected_reaction)
    changed += _apply("monitor_targets", selected_monitor)

    _save_settings(settings)
    _flash(request, "success", "Подключения обновлены." if changed else "Изменений нет.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.post("/accounts/{session_name}/persona")
@app.post("/accounts/{session_name}/role")
async def account_update_role(
    request: Request,
    session_name: str,
    role_id: str = Form(""),
    persona_id: str = Form(""),
):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    _ensure_accounts_roles_saved(accounts, settings)
    project_id = _active_project_id(settings)
    wanted_role_id = role_id.strip()
    legacy_persona_id = persona_id.strip()
    if not wanted_role_id and legacy_persona_id:
        wanted_role_id = legacy_role_id(legacy_persona_id)

    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    if wanted_role_id == CUSTOM_ROLE_ID:
        has_custom = isinstance(accounts[idx].get(ACCOUNT_CUSTOM_ROLE_KEY), dict) and bool(
            accounts[idx].get(ACCOUNT_CUSTOM_ROLE_KEY)
        )
        if not has_custom:
            _flash(request, "warning", "У аккаунта нет кастомной роли. Сначала настройте её.")
            return _redirect(f"/accounts/{quote(session_name)}")
        accounts[idx]["role_id"] = CUSTOM_ROLE_ID
        _save_accounts(accounts)
        _flash(request, "success", "Роль обновлена.")
        return _redirect(f"/accounts/{quote(session_name)}")

    selected_role_id = _resolve_role_id(settings, wanted_role_id)
    if not selected_role_id:
        raise HTTPException(status_code=400, detail="Роль не найдена")
    accounts[idx]["role_id"] = selected_role_id
    _save_accounts(accounts)

    _flash(request, "success", "Роль обновлена.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.get("/accounts/{session_name}/role/custom", response_class=HTMLResponse)
async def account_custom_role_page(request: Request, session_name: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    _ensure_accounts_roles_saved(accounts, settings)
    project_id = _active_project_id(settings)

    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    account = accounts[idx]

    role_presets = {
        category: sorted(
            list(role_presets_for_category(settings, category).items()),
            key=lambda item: str(item[1].get("name") or item[0]).lower(),
        )
        for category in ROLE_PRESET_CATEGORIES
    }

    has_custom_role = isinstance(account.get(ACCOUNT_CUSTOM_ROLE_KEY), dict) and bool(account.get(ACCOUNT_CUSTOM_ROLE_KEY))
    role_src_account = {**account, "role_id": CUSTOM_ROLE_ID} if has_custom_role else account
    _, role = role_for_account(role_src_account, settings)
    role = {**(role or {}), "name": CUSTOM_ROLE_NAME}

    role_is_active = str(account.get("role_id") or "").strip() == CUSTOM_ROLE_ID

    return templates.TemplateResponse(
        "account_role_custom.html",
        _template_context(
            request,
            account=account,
            role=role,
            role_presets=role_presets,
            emoji_levels=EMOJI_LEVELS,
            gender_options=GENDER_OPTIONS,
            has_custom_role=has_custom_role,
            role_is_active=role_is_active,
        ),
    )


@app.post("/accounts/{session_name}/role/custom")
async def account_custom_role_update(
    request: Request,
    session_name: str,
    character_preset_id: str = Form(""),
    behavior_preset_id: str = Form(""),
    mood_preset_ids: Optional[List[str]] = Form(None),
    humanization_preset_id: str = Form(""),
    character_prompt_override: str = Form(""),
    behavior_prompt_override: str = Form(""),
    humanization_prompt_override: str = Form(""),
    emoji_level: str = Form("minimal"),
    gender: str = Form("neutral"),
    custom_prompt: str = Form(""),
):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    _ensure_accounts_roles_saved(accounts, settings)
    project_id = _active_project_id(settings)

    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    account = accounts[idx]
    presets = {category: role_presets_for_category(settings, category) for category in ROLE_PRESET_CATEGORIES}

    has_custom = isinstance(account.get(ACCOUNT_CUSTOM_ROLE_KEY), dict) and bool(account.get(ACCOUNT_CUSTOM_ROLE_KEY))
    fallback_src = {**account, "role_id": CUSTOM_ROLE_ID} if has_custom else account
    _, fallback = role_for_account(fallback_src, settings)

    if character_preset_id not in presets["character"]:
        character_preset_id = str(fallback.get("character_preset_id") or "character_balanced")
    if behavior_preset_id not in presets["behavior"]:
        behavior_preset_id = str(fallback.get("behavior_preset_id") or "behavior_balanced")
    if humanization_preset_id not in presets["humanization"]:
        humanization_preset_id = str(fallback.get("humanization_preset_id") or "human_natural")

    mood_ids = [m for m in (mood_preset_ids or []) if m in presets["mood"]]
    if not mood_ids:
        prev_moods = fallback.get("mood_preset_ids") if isinstance(fallback.get("mood_preset_ids"), list) else []
        mood_ids = [m for m in prev_moods if m in presets["mood"]]
    if not mood_ids:
        mood_ids = ["mood_neutral"] if "mood_neutral" in presets["mood"] else list(presets["mood"].keys())[:1]

    emoji_level = str(emoji_level or fallback.get("emoji_level") or "minimal").strip().lower()
    if emoji_level not in EMOJI_LEVELS:
        emoji_level = "minimal"

    gender = str(gender or fallback.get("gender") or "neutral").strip().lower()
    if gender not in GENDER_OPTIONS:
        gender = "neutral"

    existing = account.get(ACCOUNT_CUSTOM_ROLE_KEY) if isinstance(account.get(ACCOUNT_CUSTOM_ROLE_KEY), dict) else {}
    now = datetime.now(timezone.utc).isoformat()
    role_payload: Dict[str, Any] = {
        "name": CUSTOM_ROLE_NAME,
        "character_preset_id": character_preset_id,
        "behavior_preset_id": behavior_preset_id,
        "mood_preset_ids": mood_ids,
        "humanization_preset_id": humanization_preset_id,
        "character_prompt_override": character_prompt_override.strip(),
        "behavior_prompt_override": behavior_prompt_override.strip(),
        "humanization_prompt_override": humanization_prompt_override.strip(),
        "emoji_level": emoji_level,
        "gender": gender,
        "custom_prompt": custom_prompt.strip(),
        "created_at": existing.get("created_at") or now,
        "updated_at": now,
        "builtin": False,
    }

    account[ACCOUNT_CUSTOM_ROLE_KEY] = role_payload
    account["role_id"] = CUSTOM_ROLE_ID
    accounts[idx] = account
    _save_accounts(accounts)

    _flash(request, "success", "Кастомная роль сохранена и назначена аккаунту.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.post("/accounts/{session_name}/role/custom/delete")
async def account_custom_role_delete(request: Request, session_name: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    _ensure_accounts_roles_saved(accounts, settings)
    project_id = _active_project_id(settings)

    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    account = accounts[idx]
    removed = account.pop(ACCOUNT_CUSTOM_ROLE_KEY, None)
    if str(account.get("role_id") or "").strip() == CUSTOM_ROLE_ID:
        account["role_id"] = _default_role_id(settings)

    accounts[idx] = account
    _save_accounts(accounts)

    if removed:
        _flash(request, "success", "Кастомная роль удалена.")
    else:
        _flash(request, "warning", "У аккаунта нет кастомной роли.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.post("/accounts/{session_name}/profile")
async def account_update_profile(
    request: Request,
    session_name: str,
    first_name: str = Form(...),
    last_name: str = Form(""),
    username: str = Form(""),
    bio: str = Form(""),
):
    first_name = first_name.strip()
    last_name = last_name.strip()
    bio = bio.strip()
    if not first_name:
        _flash(request, "warning", "Имя не может быть пустым.")
        return _redirect(f"/accounts/{quote(session_name)}")

    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    username_clean = _clean_username(_normalize_tg_ref(username))
    if "profile_username" in accounts[idx]:
        current_username = _clean_username(accounts[idx].get("profile_username"))
    else:
        current_username = _clean_username(accounts[idx].get("username"))
    username_patch: str | None = None
    if username_clean != current_username:
        username_patch = username_clean
    accounts[idx]["first_name"] = first_name
    accounts[idx]["last_name"] = last_name
    accounts[idx]["profile_bio"] = bio
    if username_patch is not None:
        accounts[idx]["profile_username"] = username_patch
    _save_accounts(accounts)

    task_patch: Dict[str, Any] = {"first_name": first_name, "last_name": last_name, "bio": bio}
    if username_patch is not None:
        task_patch["username"] = username_patch
        task_patch["username_clear"] = username_patch == ""
    _upsert_profile_task(
        settings,
        session_name,
        task_patch,
    )
    _save_settings(settings)
    _flash(request, "success", "Задача на обновление профиля создана.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.post("/accounts/{session_name}/avatar")
async def account_update_avatar(
    request: Request,
    session_name: str,
    avatar: UploadFile = File(...),
):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    if _find_account_index(accounts, session_name, project_id) is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    data = await avatar.read()
    if not data:
        _flash(request, "warning", "Файл пустой.")
        return _redirect(f"/accounts/{quote(session_name)}")
    if len(data) > 5 * 1024 * 1024:
        _flash(request, "warning", "Файл слишком большой (макс 5MB).")
        return _redirect(f"/accounts/{quote(session_name)}")

    ext = (Path(avatar.filename or "").suffix or "").lower()
    if ext not in {".jpg", ".jpeg", ".png", ".webp"}:
        ext = ".jpg"

    uploads_dir = DATA_DIR / "uploads" / "avatars"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"{session_name}_{int(time.time())}_{uuid.uuid4().hex[:8]}{ext}"
    file_path = uploads_dir / file_name
    file_path.write_bytes(data)

    _upsert_profile_task(
        settings,
        session_name,
        {"avatar_path": str(file_path), "avatar_clear": False},
    )
    _save_settings(settings)
    _flash(request, "success", "Задача на обновление аватара создана.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.post("/accounts/{session_name}/avatar/clear")
async def account_clear_avatar(request: Request, session_name: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    if _find_account_index(accounts, session_name, project_id) is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    _upsert_profile_task(
        settings,
        session_name,
        {"avatar_clear": True, "avatar_path": ""},
    )
    _save_settings(settings)
    _flash(request, "success", "Задача на удаление аватара создана.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.post("/accounts/{session_name}/personal-channel")
async def account_set_personal_channel(
    request: Request,
    session_name: str,
    personal_channel: str = Form(""),
):
    ref = _normalize_tg_ref(personal_channel)
    if not ref:
        _flash(request, "warning", "Укажите @username или ссылку на канал (t.me/...).")
        return _redirect(f"/accounts/{quote(session_name)}")

    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    accounts[idx]["profile_personal_channel"] = ref
    _save_accounts(accounts)

    _upsert_profile_task(
        settings,
        session_name,
        {"personal_channel": ref, "personal_channel_clear": False},
    )
    _save_settings(settings)
    _flash(request, "success", "Задача на установку персонального канала создана.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.post("/accounts/{session_name}/personal-channel/clear")
async def account_clear_personal_channel(request: Request, session_name: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    accounts[idx].pop("profile_personal_channel", None)
    _save_accounts(accounts)

    _upsert_profile_task(
        settings,
        session_name,
        {"personal_channel_clear": True, "personal_channel": ""},
    )
    _save_settings(settings)
    _flash(request, "success", "Задача на очистку персонального канала создана.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.post("/accounts/{session_name}/delete")
async def account_delete(request: Request, session_name: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    new_accounts = [
        a
        for a in accounts
        if not (a.get("session_name") == session_name and _project_id_for(a) == project_id)
    ]
    if len(new_accounts) == len(accounts):
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    _save_accounts(new_accounts)

    # Keep inbox unread indicators in sync after removing accounts.
    try:
        _cleanup_inbox_for_removed_accounts(settings)
    except Exception:
        pass

    _flash(request, "success", f"Аккаунт '{session_name}' удалён.")
    return _redirect("/accounts")


@app.get("/targets", response_class=HTMLResponse)
async def targets_page(request: Request):
    settings, settings_err = _load_settings()
    project_id = _active_project_id(settings)
    targets = _filter_by_project(settings.get("targets", []) or [], project_id)
    targets_sorted = sorted(targets, key=lambda x: x.get("date_added", ""), reverse=True)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    accounts_by_session = {a.get("session_name"): a for a in accounts}
    return templates.TemplateResponse(
        "targets.html",
        _template_context(
            request,
            settings_err=settings_err,
            targets=targets_sorted,
            accounts_by_session=accounts_by_session,
        ),
    )


@app.get("/targets/new", response_class=HTMLResponse)
async def targets_new_page(request: Request, chat_input: str = ""):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse(
        "target_new.html",
        _template_context(request, accounts=accounts, chat_input_prefill=chat_input),
    )


@app.get("/targets/search", response_class=HTMLResponse)
async def targets_search_page(request: Request, source: str = "", auto_pause: str = "1"):
    results: List[Dict[str, Any]] = []
    error: str | None = None

    source = source.strip()
    auto_pause_flag = _parse_bool(auto_pause, default=True)
    if source:
        async with _auto_pause_commentator(request, auto_pause=auto_pause_flag, reason="Поиск каналов"):
            try:
                client = await _get_any_authorized_client()
                try:
                    source_entity = await client.get_entity(source)
                    rec = await client(GetChannelRecommendationsRequest(channel=source_entity))

                    for chat in getattr(rec, "chats", []) or []:
                        if getattr(chat, "megagroup", False):
                            continue
                        try:
                            full_channel = await client(GetFullChannelRequest(channel=chat))
                            linked_chat_id_bare = getattr(full_channel.full_chat, "linked_chat_id", None)
                            if not linked_chat_id_bare:
                                continue
                            comment_chat_entity = await client.get_entity(PeerChannel(linked_chat_id_bare))
                            results.append(
                                {
                                    "chat_id": f"-100{chat.id}",
                                    "chat_username": getattr(chat, "username", None),
                                    "chat_name": getattr(chat, "title", str(chat.id)),
                                    "linked_chat_id": f"-100{comment_chat_entity.id}",
                                }
                            )
                        except Exception:
                            continue
                finally:
                    if client.is_connected():
                        await client.disconnect()
            except Exception as e:
                error = str(e)

    return templates.TemplateResponse(
        "target_search.html",
        _template_context(request, source=source, results=results, error=error),
    )


@app.post("/targets/new")
async def targets_new_submit(
    request: Request,
    chat_input: str = Form(...),
    slow_join_interval_mins: str = Form("0"),
    initial_comment_delay: str = Form("10"),
    delay_between_accounts: str = Form("10"),
    comment_chance: str = Form("100"),
    tag_comment_chance: str = Form("50"),
    accounts_per_post_min: str = Form("0"),
    accounts_per_post_max: str = Form("0"),
    daily_comment_limit: str = Form("50"),
    ai_provider: str = Form("default"),
    min_word_count: str = Form("0"),
    min_post_interval_mins: str = Form("0"),
    min_meaningful_words: str = Form("2"),
    media_min_meaningful_words: str = Form("6"),
    skip_promotional_posts: Optional[str] = Form(None),
    skip_short_media_posts: Optional[str] = Form(None),
    auto_pause: Optional[str] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)

    chat_input = chat_input.strip()
    auto_pause_flag = _parse_bool(auto_pause, default=True)
    async with _auto_pause_commentator(request, auto_pause=auto_pause_flag, reason="Проверка/вступление в чат"):
        try:
            chat_info = await _derive_target_chat_info(chat_input)
        except HTTPException as e:
            _flash(request, "danger", str(e.detail))
            return _redirect(f"/targets/new?chat_input={quote(chat_input)}")

    chat_id = chat_info["chat_id"]
    existing_targets = _filter_by_project(settings.get("targets", []) or [], project_id)
    if any(str(t.get("chat_id")) == str(chat_id) for t in existing_targets):
        _flash(request, "warning", "Этот чат уже добавлен в цели комментирования.")
        return _redirect(f"/targets/{quote(chat_id)}")

    new_target: Dict[str, Any] = {
        **chat_info,
        "slow_join_interval_mins": _parse_int_field(
            request, slow_join_interval_mins, default=0, label="Медленное вступление (мин)", min_value=0
        ),
        "initial_comment_delay": _parse_int_field(
            request, initial_comment_delay, default=180, label="Пауза после поста", min_value=0
        ),
        "delay_between_accounts": _parse_int_field(
            request, delay_between_accounts, default=240, label="Пауза между аккаунтами", min_value=0
        ),
        "comment_chance": _parse_int_field(
            request, comment_chance, default=100, label="Комментировать посты (%)", min_value=0, max_value=100
        ),
        "tag_comment_chance": _parse_int_field(
            request, tag_comment_chance, default=50, label="Цитировать пост (%)", min_value=0, max_value=100
        ),
        "accounts_per_post_min": _parse_int_field(
            request, accounts_per_post_min, default=1, label="Аккаунтов на пост (мин)", min_value=0
        ),
        "accounts_per_post_max": _parse_int_field(
            request, accounts_per_post_max, default=50, label="Аккаунтов на пост (макс)", min_value=0
        ),
        "daily_comment_limit": _parse_int_field(
            request, daily_comment_limit, default=50, label="Лимит комментариев/сутки", min_value=0
        ),
        "ai_provider": ai_provider,
        "date_added": datetime.now(timezone.utc).isoformat(),
        "min_word_count": _parse_int_field(request, min_word_count, default=0, label="Мин. слов", min_value=0),
        "min_post_interval_mins": _parse_int_field(
            request, min_post_interval_mins, default=0, label="Мин. интервал (мин)", min_value=0
        ),
        "min_meaningful_words": _parse_int_field(
            request, min_meaningful_words, default=6, label="Мин. смысловых слов", min_value=0
        ),
        "media_min_meaningful_words": _parse_int_field(
            request, media_min_meaningful_words, default=12, label="Мин. слов для медиа", min_value=0
        ),
        "skip_promotional_posts": bool(skip_promotional_posts),
        "skip_short_media_posts": bool(skip_short_media_posts),
        "assigned_accounts": [],
        "project_id": project_id,
    }

    min_acc = int(new_target.get("accounts_per_post_min", 0) or 0)
    max_acc = int(new_target.get("accounts_per_post_max", 0) or 0)
    if max_acc < min_acc:
        _flash(request, "warning", "Диапазон аккаунтов на пост: максимум меньше минимума, исправлено.")
        new_target["accounts_per_post_max"] = min_acc

    settings.setdefault("targets", []).append(new_target)
    _save_settings(settings)
    _flash(request, "success", f"Цель комментирования добавлена: {chat_info.get('chat_name')}")
    return _redirect(f"/targets/{quote(chat_id)}")


@app.get("/targets/{chat_id}", response_class=HTMLResponse)
async def target_edit_page(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_target_by_chat_id(settings, chat_id, project_id)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    join_status = _load_join_status([target.get("chat_id"), target.get("linked_chat_id")])
    return templates.TemplateResponse(
        "target_edit.html",
        _template_context(request, target=target, accounts=accounts, join_status=join_status),
    )


@app.post("/targets/{chat_id}")
async def target_edit_save(
    request: Request,
    chat_id: str,
    ai_enabled: Optional[str] = Form(None),
    ai_provider: str = Form("default"),
    slow_join_interval_mins: str = Form(""),
    initial_comment_delay: str = Form(""),
    delay_between_accounts: str = Form(""),
    comment_chance: str = Form(""),
    tag_comment_chance: str = Form(""),
    accounts_per_post_min: str = Form(""),
    accounts_per_post_max: str = Form(""),
    daily_comment_limit: str = Form(""),
    min_word_count: str = Form(""),
    min_post_interval_mins: str = Form(""),
    min_meaningful_words: str = Form(""),
    media_min_meaningful_words: str = Form(""),
    skip_promotional_posts: Optional[str] = Form(None),
    skip_short_media_posts: Optional[str] = Form(None),
    reply_chance: str = Form(""),
    intervention_chance: str = Form(""),
    tag_reply_chance: str = Form(""),
    reply_delay_min: str = Form(""),
    reply_delay_max: str = Form(""),
    max_dialogue_depth: str = Form(""),
    max_dialogue_ai_replies: str = Form(""),
    select_all: Optional[str] = Form(None),
    assigned_accounts: Optional[List[str]] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, target = _find_target_by_chat_id(settings, chat_id, project_id)

    target["ai_enabled"] = bool(ai_enabled)
    target["ai_provider"] = ai_provider

    if slow_join_interval_mins.strip():
        target["slow_join_interval_mins"] = _parse_int_field(
            request,
            slow_join_interval_mins,
            default=int(target.get("slow_join_interval_mins", 0)),
            label="Медленное вступление (мин)",
            min_value=0,
        )

    if initial_comment_delay.strip():
        target["initial_comment_delay"] = _parse_int_field(
            request,
            initial_comment_delay,
            default=int(target.get("initial_comment_delay", 10)),
            label="Пауза после поста",
            min_value=0,
        )
    if delay_between_accounts.strip():
        target["delay_between_accounts"] = _parse_int_field(
            request,
            delay_between_accounts,
            default=int(target.get("delay_between_accounts", 10)),
            label="Пауза между аккаунтами",
            min_value=0,
        )
    if comment_chance.strip():
        target["comment_chance"] = _parse_int_field(
            request,
            comment_chance,
            default=int(target.get("comment_chance", 100)),
            label="Комментировать посты (%)",
            min_value=0,
            max_value=100,
        )
    if tag_comment_chance.strip():
        target["tag_comment_chance"] = _parse_int_field(
            request,
            tag_comment_chance,
            default=int(target.get("tag_comment_chance", 50)),
            label="Цитировать пост (%)",
            min_value=0,
            max_value=100,
        )
    if accounts_per_post_min.strip():
        target["accounts_per_post_min"] = _parse_int_field(
            request,
            accounts_per_post_min,
            default=int(target.get("accounts_per_post_min", 0)),
            label="Аккаунтов на пост (мин)",
            min_value=0,
        )
    if accounts_per_post_max.strip():
        target["accounts_per_post_max"] = _parse_int_field(
            request,
            accounts_per_post_max,
            default=int(target.get("accounts_per_post_max", 0)),
            label="Аккаунтов на пост (макс)",
            min_value=0,
        )
    if accounts_per_post_min.strip() or accounts_per_post_max.strip():
        min_acc = int(target.get("accounts_per_post_min", 0) or 0)
        max_acc = int(target.get("accounts_per_post_max", 0) or 0)
        if max_acc < min_acc:
            _flash(request, "warning", "Диапазон аккаунтов на пост: максимум меньше минимума, исправлено.")
            target["accounts_per_post_max"] = min_acc
    if "accounts_per_post" in target:
        target.pop("accounts_per_post", None)
    if daily_comment_limit.strip():
        target["daily_comment_limit"] = _parse_int_field(
            request,
            daily_comment_limit,
            default=int(target.get("daily_comment_limit", 50)),
            label="Лимит комментариев/сутки",
            min_value=0,
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
    if min_meaningful_words.strip():
        target["min_meaningful_words"] = _parse_int_field(
            request,
            min_meaningful_words,
            default=int(target.get("min_meaningful_words", 2)),
            label="Мин. смысловых слов",
            min_value=0,
        )
    if media_min_meaningful_words.strip():
        target["media_min_meaningful_words"] = _parse_int_field(
            request,
            media_min_meaningful_words,
            default=int(target.get("media_min_meaningful_words", 6)),
            label="Мин. слов для медиа",
            min_value=0,
        )

    target["skip_promotional_posts"] = bool(skip_promotional_posts) if skip_promotional_posts is not None else False
    target["skip_short_media_posts"] = bool(skip_short_media_posts) if skip_short_media_posts is not None else False

    if reply_chance.strip():
        target["reply_chance"] = _parse_int_field(
            request, reply_chance, default=int(target.get("reply_chance", 0)), label="Шанс ответа", min_value=0, max_value=100
        )
    if intervention_chance.strip():
        target["intervention_chance"] = _parse_int_field(
            request,
            intervention_chance,
            default=int(target.get("intervention_chance", 30)),
            label="Шанс вмешательства",
            min_value=0,
            max_value=100,
        )
    if tag_reply_chance.strip():
        target["tag_reply_chance"] = _parse_int_field(
            request,
            tag_reply_chance,
            default=int(target.get("tag_reply_chance", 50)),
            label="Шанс Reply-тега",
            min_value=0,
            max_value=100,
        )
    if reply_delay_min.strip():
        target["reply_delay_min"] = _parse_int_field(
            request,
            reply_delay_min,
            default=int(target.get("reply_delay_min", 30)),
            label="Мин. задержка (сек)",
            min_value=0,
        )
    if reply_delay_max.strip():
        target["reply_delay_max"] = _parse_int_field(
            request,
            reply_delay_max,
            default=int(target.get("reply_delay_max", 120)),
            label="Макс. задержка (сек)",
            min_value=0,
        )
    if max_dialogue_depth.strip():
        target["max_dialogue_depth"] = _parse_int_field(
            request,
            max_dialogue_depth,
            default=int(target.get("max_dialogue_depth", 10)),
            label="Глубина контекста",
            min_value=1,
        )
    if max_dialogue_ai_replies.strip():
        target["max_dialogue_ai_replies"] = _parse_int_field(
            request,
            max_dialogue_ai_replies,
            default=int(target.get("max_dialogue_ai_replies", 2)),
            label="Лимит ответов ИИ",
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
    elif assigned_accounts is None:
        target["assigned_accounts"] = []
    else:
        target["assigned_accounts"] = [s for s in list(assigned_accounts) if s in allowed_set]

    settings["targets"][idx] = target
    _save_settings(settings)
    _flash(request, "success", "Настройки чата обновлены.")
    return _redirect(f"/targets/{quote(chat_id)}")


@app.post("/targets/{chat_id}/join")
async def target_join_attempt(
    request: Request,
    chat_id: str,
    session_name: str = Form(""),
    target_id: str = Form(""),
    auto_pause: Optional[str] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_target_by_chat_id(settings, chat_id, project_id)
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
        return _redirect(f"/targets/{quote(chat_id)}")

    auto_pause_flag = _parse_bool(auto_pause, default=True)
    api_id_default, api_hash_default = _telethon_credentials()
    target_ids: List[str] = []
    if target_id:
        target_ids = [str(target_id)]
    else:
        if target.get("chat_id"):
            target_ids.append(str(target.get("chat_id")))
        if target.get("linked_chat_id"):
            target_ids.append(str(target.get("linked_chat_id")))

    total_joined = 0
    total_failed = 0
    had_lock = False

    async with _auto_pause_commentator(request, auto_pause=auto_pause_flag, reason="Вступление в чат"):
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
                except sqlite3.OperationalError as exc:
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
            except sqlite3.OperationalError as exc:
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
                except sqlite3.OperationalError:
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
    return _redirect(f"/targets/{quote(chat_id)}")


@app.post("/targets/{chat_id}/delete")
async def target_delete(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    settings["targets"].pop(idx)
    _save_settings(settings)
    _flash(request, "success", "Цель удалена.")
    return _redirect("/targets")


@app.get("/discussions", response_class=HTMLResponse)
async def discussions_page(request: Request):
    settings, settings_err = _load_settings()
    project_id = _active_project_id(settings)
    targets = _filter_by_project(settings.get("discussion_targets", []) or [], project_id)
    targets_sorted = sorted(targets, key=lambda x: x.get("date_added", ""), reverse=True)
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
        ),
    )


@app.get("/discussions/new", response_class=HTMLResponse)
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


@app.post("/discussions/new")
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
    auto_pause: Optional[str] = Form(None),
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
        auto_pause_flag = _parse_bool(auto_pause, default=True)
        async with _auto_pause_commentator(
            request, auto_pause=auto_pause_flag, reason="Проверка/вступление в чат (обсуждения)"
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
            svector = (scene_vector_list[i] if i < len(scene_vector_list) else "") or ""
            stmin = (scene_turns_min_list[i] if i < len(scene_turns_min_list) else "") or ""
            stmax = (scene_turns_max_list[i] if i < len(scene_turns_max_list) else "") or ""
            sdmin = (scene_initial_delay_min_list[i] if i < len(scene_initial_delay_min_list) else "") or ""
            sdmax = (scene_initial_delay_max_list[i] if i < len(scene_initial_delay_max_list) else "") or ""
            sbmin = (scene_delay_between_min_list[i] if i < len(scene_delay_between_min_list) else "") or ""
            sbmax = (scene_delay_between_max_list[i] if i < len(scene_delay_between_max_list) else "") or ""

            stitle = str(stitle or "").strip()
            sop = str(sop or "").strip()
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
            auto_pause=auto_pause,
        )
    return _redirect(f"/discussions/targets/{quote(str(new_target.get('id')))}")


@app.get("/discussions/targets/{target_id}", response_class=HTMLResponse)
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


@app.get("/discussions/{chat_id}", response_class=HTMLResponse)
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


@app.get("/discussions/sessions/{session_id}", response_class=HTMLResponse)
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


@app.post("/discussions/targets/{target_id}")
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


@app.post("/discussions/targets/{target_id}/rename")
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


@app.post("/discussions/targets/{target_id}/refresh")
async def discussion_target_refresh_chat_info(
    request: Request,
    target_id: str,
    auto_pause: Optional[str] = Form(None),
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

    auto_pause_flag = _parse_bool(auto_pause, default=True)
    async with _auto_pause_commentator(
        request, auto_pause=auto_pause_flag, reason="Обновление информации о чате (обсуждения)"
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


@app.post("/discussions/targets/{target_id}/join")
async def discussion_target_join_attempt(
    request: Request,
    target_id: str,
    session_name: str = Form(""),
    join_target_id: str = Form(""),
    auto_pause: Optional[str] = Form(None),
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

    auto_pause_flag = _parse_bool(auto_pause, default=True)
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

    async with _auto_pause_commentator(request, auto_pause=auto_pause_flag, reason="Вступление в чат (обсуждения)"):
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
                except sqlite3.OperationalError as exc:
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
            except sqlite3.OperationalError as exc:
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
                except sqlite3.OperationalError:
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


@app.post("/discussions/targets/{target_id}/start")
async def discussion_target_start(
    request: Request,
    target_id: str,
    seed_text: str = Form(...),
    auto_pause: Optional[str] = Form(None),
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
            session_id = int(cur.lastrowid)
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


@app.post("/discussions/targets/{target_id}/delete")
async def discussion_target_delete(request: Request, target_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, _ = _find_discussion_target_by_id(settings, target_id, project_id)
    settings["discussion_targets"].pop(idx)
    _save_settings(settings)
    _flash(request, "success", "Цель обсуждений удалена.")
    return _redirect("/discussions")


@app.get("/targets/{chat_id}/prompts", response_class=HTMLResponse)
async def target_prompts_page(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    _flash(request, "info", "Промпты чата отключены. Используйте раздел «Роли».")
    return _redirect(f"/targets/{quote(chat_id)}")


@app.post("/targets/{chat_id}/prompts")
async def target_prompts_save(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    _flash(request, "info", "Промпты чата отключены. Роль задаётся на уровне аккаунта.")
    return _redirect(f"/targets/{quote(chat_id)}")


@app.get("/targets/{chat_id}/scenario", response_class=HTMLResponse)
async def target_scenario_page(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_target_by_chat_id(settings, chat_id, project_id)

    with _db_connect() as conn:
        row = conn.execute(
            "SELECT script_content, current_index, status FROM scenarios WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()

    scenario = dict(row) if row else {"script_content": "", "current_index": 0, "status": "stopped"}
    return templates.TemplateResponse(
        "target_scenario.html",
        _template_context(request, target=target, scenario=scenario),
    )


@app.post("/targets/{chat_id}/scenario/save")
async def target_scenario_save(request: Request, chat_id: str, script_content: str = Form("")):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)

    script_content = script_content.strip()
    if not script_content:
        _flash(request, "warning", "Сценарий пустой — нечего сохранять.")
        return _redirect(f"/targets/{quote(chat_id)}/scenario")

    with _db_connect() as conn:
        conn.execute(
            """
            INSERT INTO scenarios (chat_id, script_content, current_index, status)
            VALUES (?, ?, 0, 'stopped')
            ON CONFLICT(chat_id) DO UPDATE SET
                script_content=excluded.script_content,
                current_index=0,
                status='stopped'
            """,
            (chat_id, script_content),
        )
        conn.commit()

    _flash(request, "success", "Сценарий сохранён (статус: остановлен, прогресс сброшен).")
    return _redirect(f"/targets/{quote(chat_id)}/scenario")


@app.post("/targets/{chat_id}/scenario/toggle")
async def target_scenario_toggle(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    with _db_connect() as conn:
        row = conn.execute("SELECT status FROM scenarios WHERE chat_id = ?", (chat_id,)).fetchone()
        current = row["status"] if row else "stopped"
        new_status = "stopped" if current == "running" else "running"
        conn.execute("UPDATE scenarios SET status = ? WHERE chat_id = ?", (new_status, chat_id))
        conn.commit()
    _flash(request, "success", f"Сценарий: {new_status}")
    return _redirect(f"/targets/{quote(chat_id)}/scenario")


@app.post("/targets/{chat_id}/scenario/reset")
async def target_scenario_reset(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    with _db_connect() as conn:
        conn.execute("UPDATE scenarios SET current_index = 0 WHERE chat_id = ?", (chat_id,))
        conn.commit()
    _flash(request, "success", "Прогресс сценария сброшен в 0.")
    return _redirect(f"/targets/{quote(chat_id)}/scenario")


@app.post("/targets/{chat_id}/scenario/toggle-reply")
async def target_scenario_toggle_reply(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, target = _find_target_by_chat_id(settings, chat_id, project_id)
    target["scenario_reply_mode"] = not bool(target.get("scenario_reply_mode", False))
    settings["targets"][idx] = target
    _save_settings(settings)
    _flash(request, "success", f"Связный Reply: {'вкл' if target['scenario_reply_mode'] else 'выкл'}")
    return _redirect(f"/targets/{quote(chat_id)}/scenario")


@app.get("/targets/{chat_id}/triggers", response_class=HTMLResponse)
async def target_triggers_page(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_target_by_chat_id(settings, chat_id, project_id)
    with _db_connect() as conn:
        triggers = conn.execute(
            "SELECT id, trigger_phrase, answer_text FROM triggers WHERE chat_id = ? ORDER BY id DESC",
            (chat_id,),
        ).fetchall()
    return templates.TemplateResponse(
        "target_triggers.html",
        _template_context(request, target=target, triggers=triggers),
    )


@app.post("/targets/{chat_id}/triggers/add")
async def target_triggers_add(
    request: Request,
    chat_id: str,
    trigger_phrase: str = Form(...),
    answer_text: str = Form(...),
):
    trigger_phrase = trigger_phrase.strip().lower()
    answer_text = answer_text.strip()
    if not trigger_phrase or not answer_text:
        raise HTTPException(status_code=400, detail="Нужно указать фразу и ответ")

    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    with _db_connect() as conn:
        conn.execute(
            "INSERT INTO triggers (chat_id, trigger_phrase, answer_text) VALUES (?, ?, ?)",
            (chat_id, trigger_phrase, answer_text),
        )
        conn.commit()
    _flash(request, "success", f"Триггер добавлен: {trigger_phrase}")
    return _redirect(f"/targets/{quote(chat_id)}/triggers")


@app.post("/targets/{chat_id}/triggers/{trigger_id}/delete")
async def target_triggers_delete(request: Request, chat_id: str, trigger_id: int):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    with _db_connect() as conn:
        conn.execute("DELETE FROM triggers WHERE id = ?", (trigger_id,))
        conn.commit()
    _flash(request, "success", "Триггер удалён.")
    return _redirect(f"/targets/{quote(chat_id)}/triggers")


@app.get("/reaction-targets", response_class=HTMLResponse)
async def reaction_targets_page(request: Request):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    targets = _filter_by_project(settings.get("reaction_targets", []) or [], project_id)
    targets_sorted = sorted(targets, key=lambda x: x.get("date_added", ""), reverse=True)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse(
        "reaction_targets.html",
        _template_context(request, targets=targets_sorted, accounts=accounts),
    )


@app.get("/reaction-targets/new", response_class=HTMLResponse)
async def reaction_targets_new_page(request: Request):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse("reaction_target_new.html", _template_context(request, accounts=accounts))


@app.post("/reaction-targets/new")
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
    auto_pause: Optional[str] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)

    chat_input = chat_input.strip()
    auto_pause_flag = _parse_bool(auto_pause, default=True)
    async with _auto_pause_commentator(request, auto_pause=auto_pause_flag, reason="Проверка/вступление в чат (реакции)"):
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


@app.get("/reaction-targets/{chat_id}", response_class=HTMLResponse)
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


@app.post("/reaction-targets/{chat_id}")
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


@app.post("/reaction-targets/{chat_id}/delete")
async def reaction_target_delete(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, _ = _find_reaction_target_by_chat_id(settings, chat_id, project_id)
    settings["reaction_targets"].pop(idx)
    _save_settings(settings)
    _flash(request, "success", "Цель реакций удалена.")
    return _redirect("/reaction-targets")


@app.get("/monitor-targets", response_class=HTMLResponse)
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


@app.get("/monitor-targets/new", response_class=HTMLResponse)
async def monitor_targets_new_page(request: Request):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse("monitor_target_new.html", _template_context(request, accounts=accounts))


@app.post("/monitor-targets/new")
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
    auto_pause: Optional[str] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    chat_input = chat_input.strip()
    auto_pause_flag = _parse_bool(auto_pause, default=True)
    async with _auto_pause_commentator(request, auto_pause=auto_pause_flag, reason="Проверка/вступление в чат (мониторинг)"):
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


@app.get("/monitor-targets/{chat_id}", response_class=HTMLResponse)
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


@app.post("/monitor-targets/{chat_id}")
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


@app.post("/monitor-targets/{chat_id}/delete")
async def monitor_target_delete(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, _ = _find_monitor_target_by_chat_id(settings, chat_id, project_id)
    settings["monitor_targets"].pop(idx)
    _save_settings(settings)
    _flash(request, "success", "Канал мониторинга удалён.")
    return _redirect("/monitor-targets")


@app.get("/personas", response_class=HTMLResponse)
async def personas_page(request: Request):
    settings, settings_err = _load_settings()
    accounts, _ = _load_accounts()
    _ensure_accounts_roles_saved(accounts, settings)
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    role_presets = {
        category: sorted(
            list(role_presets_for_category(settings, category).items()),
            key=lambda item: str(item[1].get("name") or item[0]).lower(),
        )
        for category in ROLE_PRESET_CATEGORIES
    }
    roles = _sorted_role_items(settings)
    default_role_id = _default_role_id(settings)
    return templates.TemplateResponse(
        "personas.html",
        _template_context(
            request,
            settings_err=settings_err,
            accounts=accounts,
            roles=roles,
            default_role_id=default_role_id,
            role_presets=role_presets,
            emoji_levels=EMOJI_LEVELS,
            gender_options=GENDER_OPTIONS,
        ),
    )


@app.post("/personas/new")
async def persona_new(
    request: Request,
    name: str = Form(""),
    character_preset_id: str = Form(""),
    behavior_preset_id: str = Form(""),
    mood_preset_ids: Optional[List[str]] = Form(None),
    humanization_preset_id: str = Form(""),
    emoji_level: str = Form("minimal"),
    gender: str = Form("neutral"),
    custom_prompt: str = Form(""),
    randomize: Optional[str] = Form(None),
):
    settings, _ = _load_settings()
    presets = {category: role_presets_for_category(settings, category) for category in ROLE_PRESET_CATEGORIES}

    use_random = randomize is not None
    if use_random:
        rand = random_role_profile(settings)
        character_preset_id = rand.get("character_preset_id", character_preset_id)
        behavior_preset_id = rand.get("behavior_preset_id", behavior_preset_id)
        mood_preset_ids = rand.get("mood_preset_ids", mood_preset_ids or [])
        humanization_preset_id = rand.get("humanization_preset_id", humanization_preset_id)
        emoji_level = rand.get("emoji_level", emoji_level)
        gender = rand.get("gender", gender)

    name = name.strip()
    if not name:
        name = f"Роль {datetime.now().strftime('%H:%M:%S')}"

    if character_preset_id not in presets["character"]:
        character_preset_id = "character_balanced"
    if behavior_preset_id not in presets["behavior"]:
        behavior_preset_id = "behavior_balanced"
    if humanization_preset_id not in presets["humanization"]:
        humanization_preset_id = "human_natural"

    mood_ids = [m for m in (mood_preset_ids or []) if m in presets["mood"]]
    if not mood_ids:
        mood_ids = ["mood_neutral"] if "mood_neutral" in presets["mood"] else list(presets["mood"].keys())[:1]

    emoji_level = str(emoji_level or "minimal").strip().lower()
    if emoji_level not in EMOJI_LEVELS:
        emoji_level = "minimal"

    gender = str(gender or "neutral").strip().lower()
    if gender not in GENDER_OPTIONS:
        gender = "neutral"

    role_id = str(int(time.time() * 1000))
    role_payload: Dict[str, Any] = {
        "name": name,
        "character_preset_id": character_preset_id,
        "behavior_preset_id": behavior_preset_id,
        "mood_preset_ids": mood_ids,
        "humanization_preset_id": humanization_preset_id,
        "emoji_level": emoji_level,
        "gender": gender,
        "custom_prompt": custom_prompt.strip(),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "builtin": False,
    }
    settings.setdefault("roles", {})[role_id] = role_payload
    _save_settings(settings)
    _flash(request, "success", f"Роль создана: {name}")
    return _redirect(f"/personas/{role_id}")


@app.get("/personas/{persona_id}", response_class=HTMLResponse)
async def persona_edit_page(request: Request, persona_id: str):
    settings, _ = _load_settings()
    roles = _roles_dict(settings)
    role = roles.get(persona_id)
    if not role:
        raise HTTPException(status_code=404, detail="Роль не найдена")
    accounts, _ = _load_accounts()
    _ensure_accounts_roles_saved(accounts, settings)
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    role_presets = {
        category: sorted(
            list(role_presets_for_category(settings, category).items()),
            key=lambda item: str(item[1].get("name") or item[0]).lower(),
        )
        for category in ROLE_PRESET_CATEGORIES
    }
    return templates.TemplateResponse(
        "persona_edit.html",
        _template_context(
            request,
            role_id=persona_id,
            role=role,
            accounts=accounts,
            default_role_id=_default_role_id(settings),
            role_presets=role_presets,
            emoji_levels=EMOJI_LEVELS,
            gender_options=GENDER_OPTIONS,
        ),
    )


@app.post("/personas/{persona_id}/assign")
async def persona_assign(
    request: Request,
    persona_id: str,
    sessions: Optional[List[str]] = Form(None),
):
    settings, _ = _load_settings()
    if persona_id not in _roles_dict(settings):
        raise HTTPException(status_code=404, detail="Роль не найдена")
    project_id = _active_project_id(settings)
    default_role_id = _default_role_id(settings)

    wanted = set(sessions or [])
    accounts, _ = _load_accounts()
    _ensure_accounts_roles_saved(accounts, settings)
    for acc in accounts:
        s = acc.get("session_name")
        if not s:
            continue
        if _project_id_for(acc) != project_id:
            continue
        if s in wanted:
            acc["role_id"] = persona_id
        else:
            if acc.get("role_id") == persona_id and default_role_id:
                acc["role_id"] = default_role_id
    _save_accounts(accounts)

    _flash(request, "success", "Назначения ролей сохранены.")
    return _redirect(f"/personas/{quote(persona_id)}")


@app.post("/personas/{persona_id}/update")
async def persona_update(
    request: Request,
    persona_id: str,
    name: str = Form(""),
    character_preset_id: str = Form(""),
    behavior_preset_id: str = Form(""),
    mood_preset_ids: Optional[List[str]] = Form(None),
    humanization_preset_id: str = Form(""),
    emoji_level: str = Form("minimal"),
    gender: str = Form("neutral"),
    custom_prompt: str = Form(""),
    set_default: Optional[str] = Form(None),
    randomize: Optional[str] = Form(None),
):
    settings, _ = _load_settings()
    roles = _roles_dict(settings)
    existing = roles.get(persona_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Роль не найдена")

    presets = {category: role_presets_for_category(settings, category) for category in ROLE_PRESET_CATEGORIES}
    use_random = randomize is not None
    if use_random:
        rand = random_role_profile(settings)
        character_preset_id = rand.get("character_preset_id", character_preset_id)
        behavior_preset_id = rand.get("behavior_preset_id", behavior_preset_id)
        mood_preset_ids = rand.get("mood_preset_ids", mood_preset_ids or [])
        humanization_preset_id = rand.get("humanization_preset_id", humanization_preset_id)
        emoji_level = rand.get("emoji_level", emoji_level)
        gender = rand.get("gender", gender)

    name = name.strip()
    if not name:
        _flash(request, "warning", "Название роли не может быть пустым.")
        return _redirect(f"/personas/{quote(persona_id)}")

    if character_preset_id not in presets["character"]:
        character_preset_id = str(existing.get("character_preset_id") or "character_balanced")
    if behavior_preset_id not in presets["behavior"]:
        behavior_preset_id = str(existing.get("behavior_preset_id") or "behavior_balanced")
    if humanization_preset_id not in presets["humanization"]:
        humanization_preset_id = str(existing.get("humanization_preset_id") or "human_natural")

    mood_ids = [m for m in (mood_preset_ids or []) if m in presets["mood"]]
    if not mood_ids:
        prev_moods = existing.get("mood_preset_ids") if isinstance(existing.get("mood_preset_ids"), list) else []
        mood_ids = [m for m in prev_moods if m in presets["mood"]]
    if not mood_ids:
        mood_ids = ["mood_neutral"] if "mood_neutral" in presets["mood"] else list(presets["mood"].keys())[:1]

    emoji_level = str(emoji_level or existing.get("emoji_level") or "minimal").strip().lower()
    if emoji_level not in EMOJI_LEVELS:
        emoji_level = "minimal"

    gender = str(gender or existing.get("gender") or "neutral").strip().lower()
    if gender not in GENDER_OPTIONS:
        gender = "neutral"

    roles[persona_id] = {
        "name": name,
        "character_preset_id": character_preset_id,
        "behavior_preset_id": behavior_preset_id,
        "mood_preset_ids": mood_ids,
        "humanization_preset_id": humanization_preset_id,
        "emoji_level": emoji_level,
        "gender": gender,
        "custom_prompt": custom_prompt.strip(),
        "created_at": existing.get("created_at") or datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "builtin": bool(existing.get("builtin", False)),
    }
    settings["roles"] = roles
    if set_default is not None:
        settings["default_role_id"] = persona_id
    _save_settings(settings)
    _flash(request, "success", "Роль обновлена.")
    return _redirect(f"/personas/{quote(persona_id)}")


@app.post("/personas/{persona_id}/duplicate")
async def persona_duplicate(request: Request, persona_id: str):
    settings, _ = _load_settings()
    roles = _roles_dict(settings)
    role = roles.get(persona_id)
    if not role:
        raise HTTPException(status_code=404, detail="Роль не найдена")

    new_id = str(int(time.time() * 1000))
    base_name = str(role.get("name") or "Роль").strip() or "Роль"
    duplicated = {
        **role,
        "name": f"{base_name} (копия)",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "builtin": False,
    }
    roles[new_id] = duplicated
    settings["roles"] = roles
    _save_settings(settings)
    _flash(request, "success", "Роль продублирована.")
    return _redirect(f"/personas/{quote(new_id)}")


@app.post("/personas/{persona_id}/delete")
async def persona_delete(request: Request, persona_id: str):
    settings, _ = _load_settings()
    roles = _roles_dict(settings)
    if persona_id == _default_role_id(settings):
        _flash(request, "warning", "Нельзя удалить роль по умолчанию. Сначала выберите другую роль по умолчанию.")
        return _redirect(f"/personas/{quote(persona_id)}")
    removed = roles.pop(persona_id, None)
    if not removed:
        raise HTTPException(status_code=404, detail="Роль не найдена")
    settings["roles"] = roles
    _save_settings(settings)

    accounts, _ = _load_accounts()
    _ensure_accounts_roles_saved(accounts, settings)
    fallback_role_id = _default_role_id(settings)
    updated = False
    for acc in accounts:
        if acc.get("role_id") == persona_id and fallback_role_id:
            acc["role_id"] = fallback_role_id
            updated = True
    if updated:
        _save_accounts(accounts)

    _flash(request, "success", f"Роль удалена: {removed.get('name')}")
    return _redirect("/personas")


@app.post("/personas/{persona_id}/default")
async def persona_set_default(request: Request, persona_id: str):
    settings, _ = _load_settings()
    if persona_id not in _roles_dict(settings):
        raise HTTPException(status_code=404, detail="Роль не найдена")
    settings["default_role_id"] = persona_id
    _save_settings(settings)
    _flash(request, "success", "Роль по умолчанию обновлена.")
    return _redirect(f"/personas/{quote(persona_id)}")


@app.post("/personas/presets/{category}/new")
async def persona_preset_new(
    request: Request,
    category: str,
    name: str = Form(""),
    prompt: str = Form(""),
):
    category = str(category or "").strip()
    if category not in ROLE_PRESET_CATEGORIES:
        raise HTTPException(status_code=400, detail="Неизвестная категория пресета")

    name = name.strip()
    prompt = prompt.strip()
    if not name or not prompt:
        _flash(request, "warning", "Для пресета нужны название и текст.")
        return _redirect("/personas")

    settings, _ = _load_settings()
    settings.setdefault("role_presets", {})
    category_store = settings["role_presets"].setdefault(category, {})
    preset_id = f"custom_{category}_{int(time.time() * 1000)}"
    category_store[preset_id] = {"name": name, "prompt": prompt, "builtin": False}
    _save_settings(settings)
    _flash(request, "success", f"Пресет добавлен: {name}")
    return _redirect("/personas")


@app.post("/personas/presets/{category}/{preset_id}/delete")
async def persona_preset_delete(request: Request, category: str, preset_id: str):
    category = str(category or "").strip()
    if category not in ROLE_PRESET_CATEGORIES:
        raise HTTPException(status_code=400, detail="Неизвестная категория пресета")

    settings, _ = _load_settings()
    category_store = role_presets_for_category(settings, category)
    preset = category_store.get(preset_id)
    if not preset:
        raise HTTPException(status_code=404, detail="Пресет не найден")
    if bool(preset.get("builtin")):
        _flash(request, "warning", "Системные пресеты удалять нельзя.")
        return _redirect("/personas")

    roles = _roles_dict(settings)
    for rid, role in roles.items():
        if not isinstance(role, dict):
            continue
        if category == "mood":
            moods = role.get("mood_preset_ids") if isinstance(role.get("mood_preset_ids"), list) else []
            if preset_id in moods:
                _flash(request, "warning", f"Пресет используется в роли «{role.get('name', rid)}».")
                return _redirect("/personas")
            continue
        role_key = f"{category}_preset_id"
        if str(role.get(role_key) or "") == preset_id:
            _flash(request, "warning", f"Пресет используется в роли «{role.get('name', rid)}».")
            return _redirect("/personas")

    category_store.pop(preset_id, None)
    _save_settings(settings)
    _flash(request, "success", "Пресет удалён.")
    return _redirect("/personas")


@app.get("/proxies", response_class=HTMLResponse)
async def proxies_page(request: Request):
    with _db_connect() as conn:
        proxies = conn.execute(
            "SELECT id, url, name, ip, country, status, last_check FROM proxies ORDER BY id DESC LIMIT 200"
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) AS c FROM proxies").fetchone()["c"]
        active = conn.execute("SELECT COUNT(*) AS c FROM proxies WHERE status='active'").fetchone()["c"]
        dead = conn.execute("SELECT COUNT(*) AS c FROM proxies WHERE status='dead'").fetchone()["c"]

    return templates.TemplateResponse(
        "proxies.html",
        _template_context(request, proxies=proxies, total=total, active=active, dead=dead),
    )


@app.post("/proxies/add")
async def proxies_add(
    request: Request,
    proxies_text: str = Form(...),
    proxy_name: str = Form(""),
):
    lines = [l.strip() for l in proxies_text.splitlines() if l.strip()]
    if not lines:
        _flash(request, "warning", "Список пустой.")
        return _redirect("/proxies")

    added = 0
    dup = 0
    invalid = 0
    base_name = proxy_name.strip() or None
    for line in lines:
        raw_url, line_name = _split_proxy_line(line)
        url = _normalize_proxy_url(raw_url)
        if not url:
            invalid += 1
            continue
        name = line_name or base_name
        res = await _check_proxy_health(url)
        with _db_connect() as conn:
            try:
                conn.execute(
                    "INSERT INTO proxies (url, name, ip, country, status, last_check) VALUES (?, ?, ?, ?, ?, ?)",
                    (url, name, res["ip"], res["country"], res["status"], datetime.now().isoformat()),
                )
                conn.commit()
                added += 1
            except sqlite3.IntegrityError:
                dup += 1

    msg = f"Импорт завершён: добавлено={added}, дубликаты={dup}"
    if invalid:
        msg += f", пропущено={invalid}"
    _flash(request, "success", msg)
    return _redirect("/proxies")


@app.post("/proxies/check-all")
async def proxies_check_all(request: Request):
    with _db_connect() as conn:
        rows = conn.execute("SELECT id, url FROM proxies").fetchall()

    active = 0
    dead = 0
    for r in rows:
        res = await _check_proxy_health(r["url"])
        with _db_connect() as conn:
            conn.execute(
                "UPDATE proxies SET status=?, ip=?, country=?, last_check=? WHERE id=?",
                (res["status"], res["ip"], res["country"], datetime.now().isoformat(), r["id"]),
            )
            conn.commit()
        if res["status"] == "active":
            active += 1
        else:
            dead += 1

    _flash(request, "success", f"Проверка завершена: active={active}, dead={dead}")
    return _redirect("/proxies")


@app.post("/proxies/{proxy_id}/check")
async def proxies_check_one(request: Request, proxy_id: int):
    with _db_connect() as conn:
        row = conn.execute("SELECT url FROM proxies WHERE id=?", (proxy_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Прокси не найден")
        url = row["url"]

    res = await _check_proxy_health(url)
    with _db_connect() as conn:
        conn.execute(
            "UPDATE proxies SET status=?, ip=?, country=?, last_check=? WHERE id=?",
            (res["status"], res["ip"], res["country"], datetime.now().isoformat(), proxy_id),
        )
        conn.commit()

    _flash(request, "success", f"Прокси обновлён: {res['status']}, IP={res['ip']}")
    return _redirect("/proxies")


@app.post("/proxies/{proxy_id}/name")
async def proxies_update_name(request: Request, proxy_id: int, name: str = Form("")):
    name = name.strip()
    with _db_connect() as conn:
        row = conn.execute("SELECT id FROM proxies WHERE id=?", (proxy_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Прокси не найден")
        conn.execute("UPDATE proxies SET name=? WHERE id=?", (name or None, proxy_id))
        conn.commit()
    _flash(request, "success", "Название прокси обновлено.")
    return _redirect("/proxies")


@app.post("/proxies/{proxy_id}/delete")
async def proxies_delete_one(request: Request, proxy_id: int):
    with _db_connect() as conn:
        conn.execute("DELETE FROM proxies WHERE id=?", (proxy_id,))
        conn.commit()
    _flash(request, "success", "Прокси удалён.")
    return _redirect("/proxies")


@app.post("/proxies/delete-dead")
async def proxies_delete_dead(request: Request):
    with _db_connect() as conn:
        dead_urls = [r["url"] for r in conn.execute("SELECT url FROM proxies WHERE status='dead'").fetchall()]
        conn.execute("DELETE FROM proxies WHERE status='dead'")
        conn.commit()

    accounts, _ = _load_accounts()
    updated = False
    for acc in accounts:
        if acc.get("proxy_url") in dead_urls:
            acc.pop("proxy_url", None)
            updated = True
    if updated:
        _save_accounts(accounts)

    _flash(request, "success", f"Удалено нерабочих прокси: {len(dead_urls)}")
    return _redirect("/proxies")


def _get_logs_for_period(period: str, page: int, items_per_page: int = 20) -> Tuple[List[sqlite3.Row], int]:
    if period == "day":
        period_filter = "timestamp >= datetime('now', '-1 day', 'localtime')"
    elif period == "week":
        period_filter = "timestamp >= datetime('now', '-7 days', 'localtime')"
    else:
        period_filter = "timestamp >= datetime('now', '-30 days', 'localtime')"

    offset = page * items_per_page

    with _db_connect() as conn:
        total_items = conn.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM (
                SELECT 1 FROM logs
                WHERE {period_filter}
                GROUP BY post_id, account_session_name, content
            )
            """
        ).fetchone()["c"]

        rows = conn.execute(
            f"""
            SELECT * FROM logs
            WHERE {period_filter}
            GROUP BY post_id, account_session_name, content
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
            """,
            (items_per_page, offset),
        ).fetchall()

    return rows, total_items


@app.get("/stats", response_class=HTMLResponse)
async def stats_page(request: Request, period: str = "day", page: int = 0):
    if period not in {"day", "week", "month"}:
        period = "day"
    rows, total = _get_logs_for_period(period, page)

    total_pages = max((total + 19) // 20, 1)
    page = max(min(page, total_pages - 1), 0)

    return templates.TemplateResponse(
        "stats.html",
        _template_context(request, period=period, page=page, total=total, total_pages=total_pages, rows=rows),
    )


@app.get("/stats/export")
async def stats_export(period: str = "day"):
    if period not in {"day", "week", "month"}:
        period = "day"
    rows, _ = _get_logs_for_period(period, 0, items_per_page=100000)

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
        if channel_username:
            post_link = f"https://t.me/{channel_username}/{post_id}"
        else:
            chat_id_clean = str(row["source_channel_id"] or "").replace("-100", "")
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


@app.get("/rebrand", response_class=HTMLResponse)
async def rebrand_page(request: Request):
    return templates.TemplateResponse("rebrand.html", _template_context(request))


@app.post("/rebrand")
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


@app.get("/manual", response_class=HTMLResponse)
async def manual_page(request: Request):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    q = _list_manual_tasks(project_id, statuses=("pending", "processing"), limit=500)
    return templates.TemplateResponse("manual.html", _template_context(request, queue=q))


@app.get("/tasks", response_class=HTMLResponse)
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
            conn.row_factory = sqlite3.Row
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
            conn.row_factory = sqlite3.Row
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
            conn.row_factory = sqlite3.Row
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
                    "kind_label": "slow‑join",
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


@app.post("/manual/run")
async def manual_run(
    request: Request,
    link: str = Form(...),
    auto_pause: Optional[str] = Form(None),
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
        auto_pause_flag = _parse_bool(auto_pause, default=True)
        async with _auto_pause_commentator(
            request,
            auto_pause=auto_pause_flag,
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


@app.post("/manual/clear")
async def manual_clear(request: Request):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    removed = _clear_manual_tasks(project_id, statuses=("pending", "processing"))
    _flash(request, "success", f"Очередь ручных заданий очищена ({removed}).")
    return _redirect("/manual")


def _telegram_message_link(chat_username: str | None, chat_id: str | None, msg_id: int | None) -> str | None:
    if not msg_id:
        return None
    if chat_username:
        return f"https://t.me/{chat_username}/{msg_id}"
    if not chat_id:
        return None
    raw = str(chat_id)
    if raw.startswith("-100"):
        return f"https://t.me/c/{raw.replace('-100', '')}/{msg_id}"
    if raw.startswith("-"):
        return f"https://t.me/c/{raw.replace('-', '')}/{msg_id}"
    return None


def _safe_local_redirect_path(path: str | None, default: str) -> str:
    raw = (path or "").strip()
    if not raw.startswith("/") or raw.startswith("//"):
        return default
    return raw


@app.get("/dialogs", response_class=HTMLResponse)
async def dialogs_page(request: Request, session_name: str = ""):
    settings, _ = _load_settings()
    try:
        _cleanup_inbox_for_removed_accounts(settings)
    except Exception:
        pass
    project_id = _active_project_id(settings)
    accounts, _ = _load_accounts()
    sessions = sorted(
        [str(a.get("session_name")) for a in _filter_accounts_by_project(accounts, project_id) if a.get("session_name")]
    )
    selected_session = (session_name or "").strip()
    if selected_session not in sessions:
        selected_session = ""
    active_sessions = [selected_session] if selected_session else sessions
    rows = []
    if active_sessions:
        placeholders = ", ".join(["?"] * len(active_sessions))
        params = tuple(active_sessions) + tuple(active_sessions)
        with _db_connect() as conn:
            rows = conn.execute(
                f"""
                WITH last AS (
                  SELECT session_name, chat_id, MAX(id) AS last_id
                  FROM inbox_messages
                  WHERE kind='dm' AND session_name IN ({placeholders})
                  GROUP BY session_name, chat_id
                ),
                unread AS (
                  SELECT session_name, chat_id,
                         SUM(CASE WHEN direction='in' AND is_read=0 THEN 1 ELSE 0 END) AS unread
                  FROM inbox_messages
                  WHERE kind='dm' AND session_name IN ({placeholders})
                  GROUP BY session_name, chat_id
                )
                SELECT m.*,
                       COALESCE(u.unread, 0) AS unread
                FROM inbox_messages m
                JOIN last l ON l.last_id = m.id
                LEFT JOIN unread u ON u.session_name = m.session_name AND u.chat_id = m.chat_id
                ORDER BY m.id DESC
                LIMIT 200
                """,
                params,
            ).fetchall()

    return_suffix = f"?session_name={quote(selected_session)}" if selected_session else ""
    return_to = f"/dialogs{return_suffix}"
    threads: List[Dict[str, Any]] = []
    for r in rows:
        title = r["chat_title"] or r["sender_name"] or r["chat_username"] or r["sender_username"] or r["chat_id"]
        threads.append(
            {
                "session_name": r["session_name"],
                "chat_id": r["chat_id"],
                "title": title,
                "last_text": r["text"] or "",
                "reactions_summary": r["reactions_summary"] or "",
                "last_at": r["created_at"],
                "unread": int(r["unread"] or 0),
                "url": f"/dialogs/{quote(str(r['session_name']))}/{quote(str(r['chat_id']))}",
                "delete_url": f"/dialogs/{quote(str(r['session_name']))}/{quote(str(r['chat_id']))}/delete",
            }
        )

    return templates.TemplateResponse(
        "dialogs.html",
        _template_context(
            request,
            threads=threads,
            sessions=sessions,
            selected_session=selected_session,
            return_to=return_to,
        ),
    )


@app.get("/dialogs/{session_name}/{chat_id}", response_class=HTMLResponse)
async def dialog_thread_page(request: Request, session_name: str, chat_id: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    if not any(a.get("session_name") == session_name and _project_id_for(a) == project_id for a in accounts):
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    with _db_connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM inbox_messages
            WHERE kind='dm' AND session_name=? AND chat_id=?
            ORDER BY id DESC
            LIMIT 400
            """,
            (session_name, chat_id),
        ).fetchall()
        conn.execute(
            "UPDATE inbox_messages SET is_read=1 WHERE kind='dm' AND session_name=? AND chat_id=? AND direction='in'",
            (session_name, chat_id),
        )
        conn.commit()

    messages = list(reversed([dict(r) for r in rows]))
    title = None
    for m in reversed(messages):
        title = m.get("chat_title") or m.get("sender_name") or m.get("chat_username") or m.get("chat_id")
        if title:
            break
    title = title or chat_id

    back_url = f"/dialogs?session_name={quote(session_name)}"
    return templates.TemplateResponse(
        "dialog_thread.html",
        _template_context(
            request,
            session_name=session_name,
            chat_id=chat_id,
            title=title,
            messages=messages,
            back_url=back_url,
        ),
    )


@app.post("/dialogs/{session_name}/{chat_id}/send")
async def dialog_send_message(request: Request, session_name: str, chat_id: str, text: str = Form(...)):
    text = text.strip()
    if not text:
        _flash(request, "warning", "Сообщение пустое.")
        return _redirect(f"/dialogs/{quote(session_name)}/{quote(chat_id)}")

    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    if not any(a.get("session_name") == session_name and _project_id_for(a) == project_id for a in accounts):
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    now = datetime.now(timezone.utc).isoformat()
    with _db_connect() as conn:
        conn.execute(
            """
            INSERT INTO inbox_messages (
                kind, direction, status, created_at,
                session_name, chat_id,
                text, is_read
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("dm", "out", "queued", now, session_name, chat_id, text, 1),
        )
        conn.execute(
            """
            INSERT INTO outbound_queue (chat_id, reply_to_msg_id, session_name, text, status)
            VALUES (?, ?, ?, ?, 'pending')
            """,
            (chat_id, None, session_name, text),
        )
        conn.commit()

    _flash(request, "success", "Сообщение поставлено в очередь на отправку.")
    return _redirect(f"/dialogs/{quote(session_name)}/{quote(chat_id)}")


@app.post("/dialogs/{session_name}/{chat_id}/delete")
async def dialog_delete_thread(
    request: Request,
    session_name: str,
    chat_id: str,
    return_to: str = Form("/dialogs"),
):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    if not any(a.get("session_name") == session_name and _project_id_for(a) == project_id for a in accounts):
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    with _db_connect() as conn:
        conn.execute("DELETE FROM inbox_messages WHERE kind='dm' AND session_name=? AND chat_id=?", (session_name, chat_id))
        conn.execute(
            "DELETE FROM outbound_queue WHERE session_name=? AND chat_id=? AND (reply_to_msg_id IS NULL OR reply_to_msg_id='')",
            (session_name, chat_id),
        )
        conn.commit()

    _flash(request, "success", "Переписка удалена.")
    return _redirect(_safe_local_redirect_path(return_to, "/dialogs"))


@app.get("/quotes", response_class=HTMLResponse)
async def quotes_page(request: Request, session_name: str = ""):
    settings, _ = _load_settings()
    try:
        _cleanup_inbox_for_removed_accounts(settings)
    except Exception:
        pass
    project_id = _active_project_id(settings)
    accounts, _ = _load_accounts()
    sessions = sorted(
        [str(a.get("session_name")) for a in _filter_accounts_by_project(accounts, project_id) if a.get("session_name")]
    )
    selected_session = (session_name or "").strip()
    if selected_session not in sessions:
        selected_session = ""
    active_sessions = [selected_session] if selected_session else sessions
    rows = []
    if active_sessions:
        placeholders = ", ".join(["?"] * len(active_sessions))
        with _db_connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM inbox_messages
                WHERE kind='quote' AND session_name IN ({placeholders})
                ORDER BY id DESC
                LIMIT 300
                """,
                tuple(active_sessions),
            ).fetchall()

    suffix = f"?session_name={quote(selected_session)}" if selected_session else ""
    return_to = f"/quotes{suffix}"
    items: List[Dict[str, Any]] = []
    for r in rows:
        link = _telegram_message_link(r["chat_username"], r["chat_id"], r["msg_id"])
        items.append(
            {
                **dict(r),
                "title": r["chat_title"] or r["chat_username"] or r["chat_id"],
                "sender": r["sender_name"] or r["sender_username"] or (str(r["sender_id"]) if r["sender_id"] else ""),
                "is_unread": bool(r["is_read"] == 0),
                "link": link,
                "url": f"/quotes/{r['id']}{suffix}",
                "delete_url": f"/quotes/{r['id']}/delete",
            }
        )

    return templates.TemplateResponse(
        "quotes.html",
        _template_context(
            request,
            items=items,
            sessions=sessions,
            selected_session=selected_session,
            return_to=return_to,
        ),
    )


@app.get("/quotes/{inbox_id}", response_class=HTMLResponse)
async def quote_detail_page(request: Request, inbox_id: int, session_name: str = ""):
    with _db_connect() as conn:
        row = conn.execute("SELECT * FROM inbox_messages WHERE id = ? AND kind='quote'", (inbox_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Сообщение не найдено")
        settings, _ = _load_settings()
        project_id = _active_project_id(settings)
        accounts, _ = _load_accounts()
        if not any(
            a.get("session_name") == row["session_name"] and _project_id_for(a) == project_id for a in accounts
        ):
            raise HTTPException(status_code=404, detail="Сообщение не найдено в текущем проекте")
        conn.execute("UPDATE inbox_messages SET is_read=1 WHERE id = ?", (inbox_id,))
        conn.commit()

    link = _telegram_message_link(row["chat_username"], row["chat_id"], row["msg_id"])
    item = {
        **dict(row),
        "title": row["chat_title"] or row["chat_username"] or row["chat_id"],
        "sender": row["sender_name"] or row["sender_username"] or (str(row["sender_id"]) if row["sender_id"] else ""),
        "link": link,
    }
    selected_session = (session_name or "").strip()
    back_url = "/quotes"
    if selected_session:
        back_url = f"/quotes?session_name={quote(selected_session)}"
    return templates.TemplateResponse("quote_detail.html", _template_context(request, item=item, back_url=back_url))


@app.post("/quotes/{inbox_id}/reply")
async def quote_reply(request: Request, inbox_id: int, text: str = Form(...)):
    text = text.strip()
    if not text:
        _flash(request, "warning", "Сообщение пустое.")
        return _redirect(f"/quotes/{inbox_id}")

    with _db_connect() as conn:
        row = conn.execute("SELECT * FROM inbox_messages WHERE id = ? AND kind='quote'", (inbox_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Сообщение не найдено")
        settings, _ = _load_settings()
        project_id = _active_project_id(settings)
        accounts, _ = _load_accounts()
        if not any(
            a.get("session_name") == row["session_name"] and _project_id_for(a) == project_id for a in accounts
        ):
            raise HTTPException(status_code=404, detail="Сообщение не найдено в текущем проекте")

        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """
            INSERT INTO inbox_messages (
                kind, direction, status, created_at,
                session_name, chat_id,
                reply_to_msg_id,
                text, is_read
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("quote", "out", "queued", now, row["session_name"], row["chat_id"], row["msg_id"], text, 1),
        )
        conn.execute(
            """
            INSERT INTO outbound_queue (chat_id, reply_to_msg_id, session_name, text, status)
            VALUES (?, ?, ?, ?, 'pending')
            """,
            (row["chat_id"], row["msg_id"], row["session_name"], text),
        )
        conn.execute("UPDATE inbox_messages SET is_read=1 WHERE id = ?", (inbox_id,))
        conn.commit()

    _flash(request, "success", "Ответ поставлен в очередь на отправку.")
    return _redirect(f"/quotes/{inbox_id}")


@app.post("/quotes/{inbox_id}/delete")
async def quote_delete(request: Request, inbox_id: int, return_to: str = Form("/quotes")):
    with _db_connect() as conn:
        row = conn.execute("SELECT * FROM inbox_messages WHERE id = ? AND kind='quote'", (inbox_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Сообщение не найдено")
        settings, _ = _load_settings()
        project_id = _active_project_id(settings)
        accounts, _ = _load_accounts()
        if not any(
            a.get("session_name") == row["session_name"] and _project_id_for(a) == project_id for a in accounts
        ):
            raise HTTPException(status_code=404, detail="Сообщение не найдено в текущем проекте")
        conn.execute("DELETE FROM inbox_messages WHERE id = ?", (inbox_id,))
        conn.commit()

    _flash(request, "success", "Запись удалена.")
    return _redirect(_safe_local_redirect_path(return_to, "/quotes"))

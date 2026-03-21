from __future__ import annotations

import asyncio
import base64
import configparser
import json
import logging
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

from fastapi import HTTPException, Request
from fastapi.responses import RedirectResponse

from app_paths import ACCOUNTS_FILE, CONFIG_FILE, SETTINGS_FILE
from app_storage import load_json_with_error, save_json
from role_engine import (
    CUSTOM_ROLE_ID,
    CUSTOM_ROLE_NAME,
    DEFAULT_ROLE_ID,
    ensure_accounts_have_roles,
    ensure_role_schema,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

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

ACCOUNT_CHECK_HOUR = int(os.getenv("ADMIN_WEB_ACCOUNT_CHECK_HOUR", "3"))

# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Settings / project management
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Session file helpers
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Accounts
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------


def _db_connect():
    """Get a database connection (sync context manager).

    Uses PostgreSQL when DB_URL is set, otherwise falls back to SQLite.
    """
    from db.connection import get_connection
    return get_connection()


def _init_database() -> None:
    from db.schema import init_database as _init_schema
    with _db_connect() as conn:
        _init_schema(conn)


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


# ---------------------------------------------------------------------------
# Manual tasks
# ---------------------------------------------------------------------------


def _manual_task_row_to_dict(row) -> Dict[str, Any]:
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


# ---------------------------------------------------------------------------
# Join status
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Account failures
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Warnings
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def _load_config(section: str) -> Dict[str, str]:
    parser = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Файл config.ini не найден: {CONFIG_FILE}")
    parser.read(CONFIG_FILE)
    if section not in parser:
        raise KeyError(f"В config.ini не найдена секция [{section}].")
    return dict(parser[section])


# ---------------------------------------------------------------------------
# Target finders
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Request helpers
# ---------------------------------------------------------------------------


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=303)


def _flash(request: Request, category: str, message: str) -> None:
    flashes = request.session.get("flashes") or []
    flashes.append({"category": category, "message": message})
    request.session["flashes"] = flashes


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


# ---------------------------------------------------------------------------
# Auto pause
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Message helpers
# ---------------------------------------------------------------------------


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

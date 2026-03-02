import ast
import configparser
import json
import logging
import asyncio
import random
import hashlib
import httpx
import os
import re
import difflib
import base64
import time
import collections
import sqlite3
import uuid
from datetime import datetime, timezone
from contextlib import contextmanager
from typing import Any, Iterator, Literal

from google import genai
from google.genai import types as genai_types
import openai
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import (
    UserDeactivatedBanError, FloodWaitError, ChatWriteForbiddenError,
    ChannelPrivateError, ChatAdminRequiredError, ReactionInvalidError,
    ReactionsTooManyError, UserAlreadyParticipantError, InviteHashExpiredError,
    InviteHashInvalidError, RPCError
)
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from telethon.tl.functions.messages import SendReactionRequest, ImportChatInviteRequest, GetDiscussionMessageRequest
from telethon.tl.functions.photos import UploadProfilePhotoRequest, DeletePhotosRequest
from telethon import utils as tg_utils
from telethon.tl import types as tl_types
from telethon.tl.functions.account import UpdatePersonalChannelRequest, UpdateProfileRequest, UpdateUsernameRequest
from telethon.tl.types import InputPeerChannel, ReactionEmoji

from app_paths import ACCOUNTS_FILE, CONFIG_FILE, DB_FILE, OLD_LOGS_FILE, PROXIES_FILE, SETTINGS_FILE, ensure_data_dir
from app_storage import load_json, save_json
from tg_device import device_kwargs, ensure_device_profile
from role_engine import (
    build_role_prompt,
    enforce_emoji_level,
    ensure_accounts_have_roles,
    ensure_role_schema,
    role_for_account,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Avoid leaking secrets (e.g., Telegram bot token is part of the URL).
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

SCRIPT_START_TIME = datetime.now(timezone.utc)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ACCOUNTS_DIR = os.getenv("APP_ACCOUNTS_DIR", os.path.join(BASE_DIR, "accounts"))

current_settings = {}
active_clients = {}
handled_posts_for_comments = collections.deque(maxlen=500)
handled_posts_for_reactions = collections.deque(maxlen=500)
handled_posts_for_monitoring = collections.deque(maxlen=500)
handled_grouped_ids = collections.deque(maxlen=200)
CHANNEL_LAST_POST_TIME = {}
MONITOR_CHANNEL_LAST_POST_TIME = {}
COMMENTER_ROTATION = {}
EVENT_HANDLER_LOCK = asyncio.Lock()
LATEST_CHANNEL_POSTS = {}
JOINED_CACHE = set()
PROCESSING_CACHE = set()
PROCESSED_BURST_IDS = set()
CHAT_REPLY_COOLDOWN = {}
REPLY_PROCESS_CACHE = set()
POST_PROCESS_CACHE = set()
POST_PROCESS_CACHE_ORDER = collections.deque()
POST_PROCESS_CACHE_MAX = 5000
DISCUSSION_START_CACHE = set()
DISCUSSION_START_CACHE_ORDER = collections.deque()
DISCUSSION_START_CACHE_MAX = 2000
DISCUSSION_ACTIVE_TASKS = {}
DISCUSSION_START_SUPPRESS_CHAT_IDS = set()
PENDING_TASKS = set()
SCENARIO_CONTEXT = {}
CLIENT_CATCH_UP_STATUS = set()
RECENT_GENERATED_MESSAGES = collections.deque(maxlen=100)


DEFAULT_MODELS = {
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
DISABLED_ACCOUNT_STATUSES = {"banned", "frozen", "limited", "human_check", "unauthorized", "missing_session"}
JOIN_MAX_RETRIES = 5
JOIN_RETRY_BACKOFF = [0, 30, 120, 300, 900]

# Connection watchdog settings: keep the service self-healing when Telethon gives up reconnecting.
CONNECT_RETRY_BACKOFF_SECONDS = [0, 5, 15, 30, 60, 120, 300, 600]
CONNECT_ATTEMPT_TIMEOUT_SECONDS = 25.0
SEND_ATTEMPT_TIMEOUT_SECONDS = 35.0
CONNECT_FAILURE_LOG_INTERVAL_SECONDS = 60.0
CLIENT_CONNECT_STATE = {}
CLIENT_CONNECT_LOCKS = {}


def _swallow_task_exception(task: asyncio.Task) -> None:
    try:
        task.exception()
    except asyncio.CancelledError:
        pass
    except Exception:
        pass


async def _run_with_soft_timeout(awaitable: Any, timeout_seconds: float) -> Any:
    timeout = float(timeout_seconds or 0.0)
    if timeout <= 0:
        return await awaitable
    task = asyncio.create_task(awaitable)
    done, _pending = await asyncio.wait({task}, timeout=timeout)
    if task in done:
        return task.result()
    task.cancel()
    task.add_done_callback(_swallow_task_exception)
    raise TimeoutError(f"timeout_after_{timeout_seconds}s")


def _connect_state(session_name: str) -> dict:
    state = CLIENT_CONNECT_STATE.get(session_name)
    if not isinstance(state, dict):
        state = {"attempts": 0, "next_attempt_at": 0.0, "last_log_at": 0.0, "last_error": None}
        CLIENT_CONNECT_STATE[session_name] = state
    return state


def _connect_backoff_delay_seconds(attempt: int) -> float:
    if attempt <= 0:
        return 0.0
    base = CONNECT_RETRY_BACKOFF_SECONDS[min(attempt - 1, len(CONNECT_RETRY_BACKOFF_SECONDS) - 1)]
    if base <= 0:
        return 0.0
    return float(base) + random.uniform(0.0, min(5.0, base * 0.25))


def _connect_backoff_ready(session_name: str) -> bool:
    state = CLIENT_CONNECT_STATE.get(session_name)
    if not isinstance(state, dict):
        return True
    try:
        next_attempt = float(state.get("next_attempt_at") or 0.0)
    except Exception:
        return True
    return time.time() >= next_attempt


def _schedule_connect_backoff(session_name: str, *, error: str | None = None, reason: str | None = None) -> None:
    if not session_name:
        return
    now = time.time()
    state = _connect_state(session_name)
    attempts = int(state.get("attempts") or 0) + 1
    state["attempts"] = attempts
    state["last_error"] = error
    state["next_attempt_at"] = now + _connect_backoff_delay_seconds(attempts)
    if error:
        _record_account_failure(session_name, "connect", last_error=str(error), last_target=reason)


async def ensure_client_connected(client_wrapper, *, reason: str = "unknown") -> bool:
    if not client_wrapper:
        return False
    session_name = str(getattr(client_wrapper, "session_name", "") or "").strip()
    client = getattr(client_wrapper, "client", None)
    if client is None:
        return False

    lock = None
    if session_name:
        lock = CLIENT_CONNECT_LOCKS.get(session_name)
        if lock is None:
            lock = asyncio.Lock()
            CLIENT_CONNECT_LOCKS[session_name] = lock

    async def _do_connect() -> bool:
        if client.is_connected():
            if session_name:
                CLIENT_CONNECT_STATE.pop(session_name, None)
            return True

        if session_name and not _connect_backoff_ready(session_name):
            return False

        now = time.time()
        if session_name:
            state = _connect_state(session_name)
            state["attempts"] = int(state.get("attempts") or 0) + 1
            attempt = int(state["attempts"])
            state["next_attempt_at"] = now + _connect_backoff_delay_seconds(attempt)

            last_log_at = float(state.get("last_log_at") or 0.0)
            if attempt == 1 or now - last_log_at >= CONNECT_FAILURE_LOG_INTERVAL_SECONDS:
                logger.warning(f"🔌 [{session_name}] клиент отключен, пробую переподключить (attempt {attempt})...")
                state["last_log_at"] = now

        try:
            await _run_with_soft_timeout(client.connect(), CONNECT_ATTEMPT_TIMEOUT_SECONDS)
            if not client.is_connected():
                raise ConnectionError("connect_returned_disconnected")
            # Sanity check: ensure requests are sendable.
            await _run_with_soft_timeout(client.get_me(), CONNECT_ATTEMPT_TIMEOUT_SECONDS)
            if session_name:
                CLIENT_CONNECT_STATE.pop(session_name, None)
                _clear_account_failure(session_name, "connect")
            return True
        except Exception as e:
            err_text = f"{type(e).__name__}: {e}"
            if session_name:
                st = _connect_state(session_name)
                st["last_error"] = err_text
                _record_account_failure(session_name, "connect", last_error=err_text, last_target=reason)
            try:
                if client.is_connected():
                    await client.disconnect()
            except Exception:
                pass
            return False

    if lock is not None:
        async with lock:
            return await _do_connect()
    return await _do_connect()


def _is_account_active(account_data: dict) -> bool:
    status = str(account_data.get("status") or "active").lower().strip()
    return status not in DISABLED_ACCOUNT_STATUSES


def _is_account_assigned(target: dict, session_name: str) -> bool:
    assigned = target.get("assigned_accounts") or []
    return session_name in assigned


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


def _mark_post_processed(unique_id: str) -> None:
    if not unique_id:
        return
    if unique_id in POST_PROCESS_CACHE:
        return
    POST_PROCESS_CACHE.add(unique_id)
    POST_PROCESS_CACHE_ORDER.append(unique_id)
    while len(POST_PROCESS_CACHE_ORDER) > POST_PROCESS_CACHE_MAX:
        old = POST_PROCESS_CACHE_ORDER.popleft()
        POST_PROCESS_CACHE.discard(old)


def _mark_discussion_started(unique_key: str) -> bool:
    if not unique_key:
        return False
    if unique_key in DISCUSSION_START_CACHE:
        return False
    DISCUSSION_START_CACHE.add(unique_key)
    DISCUSSION_START_CACHE_ORDER.append(unique_key)
    while len(DISCUSSION_START_CACHE_ORDER) > DISCUSSION_START_CACHE_MAX:
        old = DISCUSSION_START_CACHE_ORDER.popleft()
        DISCUSSION_START_CACHE.discard(old)
    return True


def _extract_discussion_seed(text: str, prefix: str) -> str | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    prefix = str(prefix or "")
    if prefix:
        if not raw.startswith(prefix):
            return None
        raw = raw[len(prefix) :].lstrip()
        if not raw:
            return None
    return raw


def _extract_discussion_seed_optional_prefix(text: str, prefix: str) -> str | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    prefix = str(prefix or "")
    if prefix and raw.startswith(prefix):
        raw = raw[len(prefix) :].lstrip()
        if not raw:
            return None
    return raw


def _channel_bare_id(value) -> int | None:
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


def _get_join_status(session_name: str, target_id: str) -> dict | None:
    try:
        with _db_connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM join_status WHERE session_name = ? AND target_id = ?",
                (session_name, target_id),
            ).fetchone()
            return dict(row) if row else None
    except Exception:
        return None


def _compute_slow_join_next_retry_at(target_id: str, interval_mins: int) -> float | None:
    try:
        mins = int(interval_mins or 0)
    except Exception:
        mins = 0
    if mins <= 0:
        return None

    interval_sec = float(mins) * 60.0
    now = time.time()

    max_scheduled = None
    max_attempt = None
    try:
        with _db_connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT MAX(next_retry_at) AS max_scheduled
                FROM join_status
                WHERE target_id = ? AND status = 'scheduled' AND next_retry_at IS NOT NULL
                """,
                (str(target_id),),
            ).fetchone()
            if row is not None and row["max_scheduled"] is not None:
                max_scheduled = float(row["max_scheduled"])

            row = conn.execute(
                """
                SELECT MAX(last_attempt) AS max_attempt
                FROM join_status
                WHERE target_id = ? AND last_attempt IS NOT NULL
                """,
                (str(target_id),),
            ).fetchone()
            if row is not None and row["max_attempt"] is not None:
                max_attempt = float(row["max_attempt"])
    except Exception:
        max_scheduled = None
        max_attempt = None

    slot = None
    if max_scheduled is not None and max_attempt is not None:
        slot = max(max_scheduled, max_attempt)
    elif max_scheduled is not None:
        slot = max_scheduled
    elif max_attempt is not None:
        slot = max_attempt

    if slot is None:
        scheduled = now
    else:
        scheduled = max(now, float(slot) + interval_sec)

    # Add a small jitter so joins don't look "robotic" (caps at 60s).
    jitter_max = min(60.0, interval_sec * 0.2)
    if jitter_max > 0:
        try:
            scheduled += random.uniform(0.0, float(jitter_max))
        except Exception:
            pass

    return float(scheduled)


def _record_account_failure(session_name: str, kind: str, *, last_error: str | None = None, last_target: str | None = None) -> int:
    if not session_name or not kind:
        return 0
    now = time.time()
    try:
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
            return int(row[0]) if row else 1
    except Exception:
        return 0


def _clear_account_failure(session_name: str, kind: str) -> None:
    if not session_name or not kind:
        return
    try:
        with _db_connect() as conn:
            conn.execute(
                "DELETE FROM account_failures WHERE session_name = ? AND kind = ?",
                (session_name, kind),
            )
            conn.commit()
    except Exception:
        pass


def _parse_iso_ts(value) -> float | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except Exception:
        return None


def _upsert_join_status(
    session_name: str,
    target_id: str,
    status: str,
    *,
    last_error: str | None = None,
    last_method: str | None = None,
    retry_count: int | None = None,
    next_retry_at: float | None = None,
):
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
                    retry_count if retry_count is not None else 0,
                    next_retry_at,
                ),
            )
            conn.commit()
    except Exception:
        pass


def _active_project_id(settings=None):
    if isinstance(settings, dict):
        raw = settings.get("active_project_id")
    else:
        raw = current_settings.get("active_project_id") if isinstance(current_settings, dict) else None
    pid = str(raw or "").strip()
    return pid or DEFAULT_PROJECT_ID


def _project_id_for(item):
    if not isinstance(item, dict):
        return DEFAULT_PROJECT_ID
    pid = str(item.get("project_id") or "").strip()
    return pid or DEFAULT_PROJECT_ID


def _filter_project_items(items, project_id):
    if not isinstance(items, list):
        return []
    return [i for i in items if isinstance(i, dict) and _project_id_for(i) == project_id]


def get_project_targets(settings=None):
    s = settings if isinstance(settings, dict) else current_settings
    pid = _active_project_id(s)
    return _filter_project_items((s or {}).get("targets", []) or [], pid)


def get_project_discussion_targets(settings=None):
    s = settings if isinstance(settings, dict) else current_settings
    pid = _active_project_id(s)
    return _filter_project_items((s or {}).get("discussion_targets", []) or [], pid)


def ensure_discussion_targets_schema(settings: dict) -> bool:
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
        if "title" not in t or t.get("title") is None:
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


def get_project_reaction_targets(settings=None):
    s = settings if isinstance(settings, dict) else current_settings
    pid = _active_project_id(s)
    return _filter_project_items((s or {}).get("reaction_targets", []) or [], pid)


def get_project_monitor_targets(settings=None):
    s = settings if isinstance(settings, dict) else current_settings
    pid = _active_project_id(s)
    return _filter_project_items((s or {}).get("monitor_targets", []) or [], pid)


def get_project_manual_queue(settings=None):
    s = settings if isinstance(settings, dict) else current_settings
    pid = _active_project_id(s)
    return _filter_project_items((s or {}).get("manual_queue", []) or [], pid)


def _parse_manual_overrides(raw_overrides):
    if not raw_overrides:
        return {}
    if isinstance(raw_overrides, dict):
        return raw_overrides
    try:
        parsed = json.loads(raw_overrides)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    return {}


def _claim_project_manual_tasks(project_id, limit=50):
    project_id = str(project_id or DEFAULT_PROJECT_ID).strip() or DEFAULT_PROJECT_ID
    limit = max(1, int(limit))
    now_ts = time.time()
    claimed = []
    with _db_connect() as conn:
        rows = conn.execute(
            """
            SELECT id, project_id, chat_id, message_chat_id, post_id, overrides_json
            FROM manual_tasks
            WHERE project_id = ? AND status = 'pending'
            ORDER BY id ASC
            LIMIT ?
            """,
            (project_id, limit),
        ).fetchall()
        for row in rows:
            task_id = int(row["id"])
            cur = conn.execute(
                """
                UPDATE manual_tasks
                SET status = 'processing', started_at = ?, last_error = NULL
                WHERE id = ? AND status = 'pending'
                """,
                (now_ts, task_id),
            )
            if int(cur.rowcount or 0) != 1:
                continue
            claimed.append(
                {
                    "id": task_id,
                    "project_id": str(row["project_id"] or DEFAULT_PROJECT_ID),
                    "chat_id": str(row["chat_id"] or "").strip(),
                    "message_chat_id": str(row["message_chat_id"] or "").strip(),
                    "post_id": row["post_id"],
                    "overrides": _parse_manual_overrides(row["overrides_json"]),
                }
            )
    return claimed


def _set_manual_task_status(task_id, status, error=None):
    if not task_id:
        return
    status = str(status or "").strip().lower()
    if status not in {"pending", "processing", "done", "failed"}:
        status = "failed"
    now_ts = time.time()
    with _db_connect() as conn:
        if status == "pending":
            conn.execute(
                """
                UPDATE manual_tasks
                SET status = 'pending', started_at = NULL, finished_at = NULL, last_error = ?
                WHERE id = ?
                """,
                (str(error or "")[:1000] or None, int(task_id)),
            )
            return
        conn.execute(
            """
            UPDATE manual_tasks
            SET status = ?, finished_at = ?, last_error = ?
            WHERE id = ?
            """,
            (status, now_ts, str(error or "")[:1000] or None, int(task_id)),
        )


def _migrate_legacy_manual_queue_to_db():
    global current_settings
    legacy_queue = current_settings.get("manual_queue")
    if not isinstance(legacy_queue, list) or not legacy_queue:
        return 0
    moved = 0
    now_ts = time.time()
    with _db_connect() as conn:
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
            project_id = _project_id_for(task)
            overrides = task.get("overrides") if isinstance(task.get("overrides"), dict) else {}
            conn.execute(
                """
                INSERT INTO manual_tasks (
                    project_id, chat_id, message_chat_id, post_id,
                    overrides_json, status, created_at
                )
                VALUES (?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    project_id,
                    chat_id,
                    message_chat_id,
                    post_id,
                    json.dumps(overrides, ensure_ascii=False),
                    now_ts,
                ),
            )
            moved += 1
    if moved:
        current_settings["manual_queue"] = []
        save_data(SETTINGS_FILE, current_settings)
    return moved


def get_project_discussion_queue(settings=None):
    s = settings if isinstance(settings, dict) else current_settings
    pid = _active_project_id(s)
    return _filter_project_items((s or {}).get("discussion_queue", []) or [], pid)


def get_project_discussion_start_queue(settings=None):
    s = settings if isinstance(settings, dict) else current_settings
    pid = _active_project_id(s)
    return _filter_project_items((s or {}).get("discussion_start_queue", []) or [], pid)


def load_project_accounts(settings=None):
    settings_obj = settings if isinstance(settings, dict) else current_settings
    if not isinstance(settings_obj, dict):
        settings_obj = {}
    ensure_role_schema(settings_obj)

    pid = _active_project_id(settings_obj)
    accounts = load_json_data(ACCOUNTS_FILE, [])
    changed = False
    if isinstance(accounts, list):
        for acc in accounts:
            if isinstance(acc, dict) and ensure_device_profile(acc):
                changed = True
        if ensure_accounts_have_roles(accounts, settings_obj):
            changed = True
    if changed:
        try:
            save_json(ACCOUNTS_FILE, accounts)
        except Exception:
            pass
    accounts = _filter_project_items(accounts, pid)
    dir_accounts = _load_accounts_from_dir(pid, settings_obj)
    return _merge_accounts_by_session_name(accounts, dir_accounts)


def _merge_accounts_by_session_name(primary: list, secondary: list) -> list:
    merged: dict[str, dict] = {}
    for acc in secondary or []:
        if isinstance(acc, dict):
            name = str(acc.get("session_name") or "").strip()
            if name:
                merged[name] = acc
    for acc in primary or []:
        if isinstance(acc, dict):
            name = str(acc.get("session_name") or "").strip()
            if not name:
                continue
            if name in merged:
                merged[name] = {**merged[name], **acc}
            else:
                merged[name] = acc
    return list(merged.values())


def _load_accounts_from_dir(project_id: str, settings: dict | None = None) -> list[dict]:
    accounts_dir = ACCOUNTS_DIR
    if not accounts_dir or not os.path.isdir(accounts_dir):
        return []

    settings_obj = settings if isinstance(settings, dict) else {}
    if settings_obj:
        ensure_role_schema(settings_obj)

    accounts: list[dict] = []
    try:
        entries = sorted(os.listdir(accounts_dir))
    except Exception:
        return []

    for filename in entries:
        if not filename.endswith(".json"):
            continue
        path = os.path.join(accounts_dir, filename)
        data = load_json_data(path, None)
        if not isinstance(data, dict):
            continue

        if ensure_device_profile(data):
            try:
                save_json(path, data)
            except Exception:
                pass

        session_file = str(data.get("session_file") or data.get("phone") or os.path.splitext(filename)[0] or "").strip()
        if not session_file:
            continue

        session_name = str(data.get("session_name") or session_file).strip()
        session_path = _find_session_file_path(session_file, accounts_dir)

        sleep_settings = data.get("sleep_settings")
        if not isinstance(sleep_settings, dict):
            sleep_settings = {"start_hour": 0, "end_hour": 23}

        account = {
            "session_name": session_name,
            "session_file": session_file,
            "session_path": session_path,
            "app_id": data.get("app_id"),
            "app_hash": data.get("app_hash"),
            "proxy": data.get("proxy"),
            "user_id": data.get("user_id"),
            "first_name": data.get("first_name") or "",
            "last_name": data.get("last_name") or "",
            "username": data.get("username") or "",
            "status": data.get("status") or "active",
            "sleep_settings": sleep_settings,
            "project_id": data.get("project_id") or project_id,
            "device_type": data.get("device_type"),
            "device_model": data.get("device_model"),
            "system_version": data.get("system_version"),
            "app_version": data.get("app_version"),
            "lang_code": data.get("lang_code"),
            "system_lang_code": data.get("system_lang_code"),
            "role_id": data.get("role_id"),
            "persona_id": data.get("persona_id"),
        }
        if settings_obj:
            resolved_role_id, _ = role_for_account(account, settings_obj)
            if resolved_role_id:
                account["role_id"] = resolved_role_id
        accounts.append(account)

    return accounts


def _find_session_file_path(session_file: str, accounts_dir: str) -> str | None:
    if not session_file:
        return None
    candidates = []
    raw = str(session_file)
    if os.path.isabs(raw):
        candidates.append(raw)
    if raw.endswith(".session"):
        candidates.append(raw)
    if not os.path.isabs(raw):
        candidates.append(os.path.join(accounts_dir, raw))
        candidates.append(os.path.join(accounts_dir, f"{raw}.session"))
    for path in candidates:
        if path and os.path.exists(path):
            return path
    return None


def _resolve_account_credentials(account_data: dict, fallback_api_id: int, fallback_api_hash: str) -> tuple[int, str | None]:
    api_id = account_data.get("app_id") or account_data.get("api_id") or fallback_api_id
    api_hash = account_data.get("app_hash") or account_data.get("api_hash") or fallback_api_hash
    try:
        api_id = int(api_id)
    except Exception:
        api_id = fallback_api_id
    return api_id, api_hash


def _resolve_account_session(account_data: dict) -> StringSession | str | None:
    session_string = (account_data.get("session_string") or "").strip()
    if session_string:
        return StringSession(session_string)

    session_path = account_data.get("session_path")
    if isinstance(session_path, str) and session_path and os.path.exists(session_path):
        return session_path

    session_file = account_data.get("session_file") or account_data.get("session_name")
    if not session_file:
        return None
    return _find_session_file_path(str(session_file), ACCOUNTS_DIR)


def _resolve_account_proxy(account_data: dict):
    proxy_url = account_data.get("proxy_url")
    if proxy_url:
        return _parse_proxy_url(proxy_url)

    proxy_tuple = account_data.get("proxy")
    if not isinstance(proxy_tuple, (list, tuple)) or len(proxy_tuple) < 3:
        return None

    proxy_type_raw = proxy_tuple[0]
    host = proxy_tuple[1]
    port = proxy_tuple[2]
    user = proxy_tuple[3] if len(proxy_tuple) > 3 else None
    password = proxy_tuple[4] if len(proxy_tuple) > 4 else None

    if not host or not port:
        return None

    proxy_type = "socks5"
    if isinstance(proxy_type_raw, str) and proxy_type_raw:
        proxy_type = proxy_type_raw.lower()
    elif isinstance(proxy_type_raw, int):
        if proxy_type_raw == 1:
            proxy_type = "socks4"
        elif proxy_type_raw in (2, 3):
            proxy_type = "http" if proxy_type_raw == 3 else "socks5"
        else:
            proxy_type = "socks5"

    try:
        port = int(port)
    except Exception:
        return None

    return (proxy_type, host, port, True, user, password)


def get_model_setting(settings, key, default_value=None):
    if default_value is None:
        default_value = DEFAULT_MODELS.get(key, "")
    models = settings.get("models", {}) if isinstance(settings, dict) else {}
    if isinstance(models, dict):
        value = models.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return default_value


def gemini_model_candidates(settings, key):
    primary = get_model_setting(settings, key)
    candidates = [primary]

    for fallback in ["gemini-2.5-flash", "gemini-1.5-flash"]:
        if fallback != primary:
            candidates.append(fallback)

    seen = set()
    unique = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            unique.append(candidate)

    return unique


def is_model_unavailable_error(exc):
    text = str(exc).lower()
    if "model" not in text:
        return False
    for phrase in [
        "does not exist",
        "not found",
        "no such model",
        "unknown model",
        "you do not have access",
        "not supported",
        "invalid model",
    ]:
        if phrase in text:
            return True
    return False


def openai_model_candidates(settings, key):
    primary = get_model_setting(settings, key)
    candidates = [primary]

    if key == "openai_chat":
        candidates.extend(["gpt-5.2-chat-latest", "gpt-5.2", "gpt-5-mini", "gpt-4.1", "gpt-4.1-mini", "gpt-4o"])
    elif key == "openai_eval":
        candidates.extend(["gpt-5.2", "gpt-5-mini", "gpt-4.1-mini", "gpt-4o-mini"])

    seen = set()
    unique = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            unique.append(candidate)

    return unique


def guess_image_mime_type(image_bytes):
    if not image_bytes:
        return "image/jpeg"
    if image_bytes.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if image_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if image_bytes[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if image_bytes.startswith(b"RIFF") and image_bytes[8:12] == b"WEBP":
        return "image/webp"
    return "image/jpeg"


def is_bot_awake(account_data):
    now = datetime.now().hour
    start_hour = account_data.get('sleep_settings', {}).get('start_hour', 8)
    end_hour = account_data.get('sleep_settings', {}).get('end_hour', 23)

    if start_hour == end_hour:
        return True

    if start_hour < end_hour:
        if start_hour <= now < end_hour:
            return True
    else:
        if now >= start_hour or now < end_hour:
            return True

    return False


def init_database():
    try:
        with _db_connect() as conn:
            conn.execute('PRAGMA journal_mode=WAL;')
            conn.execute('PRAGMA synchronous=NORMAL;')

            conn.execute('''
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, log_type TEXT NOT NULL, timestamp TEXT NOT NULL,
                    destination_chat_id INTEGER NOT NULL, channel_name TEXT, channel_username TEXT,
                    source_channel_id INTEGER, post_id INTEGER NOT NULL, account_session_name TEXT,
                    account_first_name TEXT, account_username TEXT, content TEXT
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS proxies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT NOT NULL UNIQUE,
                    ip TEXT,
                    country TEXT,
                    status TEXT,
                    last_check TEXT
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS scenarios (
                    chat_id TEXT PRIMARY KEY,
                    script_content TEXT,
                    current_index INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'stopped',
                    last_run_time REAL DEFAULT 0
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS post_scenarios (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id TEXT,
                    post_id INTEGER,
                    current_index INTEGER DEFAULT 0,
                    last_run_time REAL DEFAULT 0,
                    UNIQUE(chat_id, post_id)
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS triggers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id TEXT NOT NULL,
                    trigger_phrase TEXT NOT NULL,
                    answer_text TEXT NOT NULL
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS outbound_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id TEXT,
                    reply_to_msg_id INTEGER,
                    session_name TEXT,
                    text TEXT,
                    status TEXT DEFAULT 'pending'
                )
            ''')
            conn.execute('''
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
            ''')
            inbox_cols = {row[1] for row in conn.execute("PRAGMA table_info(inbox_messages)").fetchall()}
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

    except sqlite3.Error as e:
        logger.critical(f"Критическая ошибка при инициализации БД: {e}")
        exit()


def migrate_json_to_sqlite(conn):
    logger.warning("Обнаружен старый файл comment_logs.json. Начинаю миграцию данных в SQLite...")
    try:
        with open(OLD_LOGS_FILE, 'r', encoding='utf-8') as f:
            old_logs = json.load(f)

        cursor = conn.cursor()
        migrated_count = 0
        for chat_id, data in old_logs.items():
            for log in data.get('all_logs', []):
                content = ""
                if log.get('type') == 'reaction':
                    content = ' '.join(log.get('reactions', []))
                else:
                    content = log.get('comment', '')

                cursor.execute('''
                    INSERT INTO logs (
                        log_type, timestamp, destination_chat_id, channel_name, channel_username,
                        source_channel_id, post_id, account_session_name, account_first_name,
                        account_username, content
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    log.get('type', 'comment'),
                    log.get('date'),
                    log.get('target', {}).get('destination_chat_id', chat_id),
                    log.get('target', {}).get('chat_name'),
                    log.get('target', {}).get('chat_username'),
                    log.get('target', {}).get('channel_id'),
                    log.get('post_id'),
                    log.get('account', {}).get('session_name'),
                    log.get('account', {}).get('first_name'),
                    log.get('account', {}).get('username'),
                    content
                ))
                migrated_count += 1
        conn.commit()
        logger.info(f"Миграция завершена. Перенесено {migrated_count} записей.")
        os.rename(OLD_LOGS_FILE, f"{OLD_LOGS_FILE}.migrated")
        logger.info(f"Старый файл логов переименован в {OLD_LOGS_FILE}.migrated")
    except Exception as e:
        logger.error(f"Ошибка во время миграции данных: {e}")


def get_daily_action_count_from_db(chat_id, action_type='comment'):
    try:
        chat_id_str = str(chat_id).replace('-100', '')

        with _db_connect() as conn:
            cursor = conn.cursor()
            today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')

            variants = [chat_id, int(chat_id_str)]
            if str(chat_id).startswith("-100") or (str(chat_id).strip().lstrip("-").isdigit() and int(chat_id) > 0):
                variants.append(int(f"-100{chat_id_str}"))
            placeholders = ','.join('?' for _ in variants)

            query = f'''
                SELECT COUNT(*) FROM logs 
                WHERE log_type = ? 
                AND destination_chat_id IN ({placeholders}) 
                AND timestamp LIKE ?
            '''

            args = [action_type] + variants + [f"{today_str}%"]

            cursor.execute(query, args)
            result = cursor.fetchone()
            return result[0] if result else 0
    except Exception as e:
        logger.error(f"Ошибка получения счетчика из БД: {e}")
        return 9999


def check_if_already_commented(destination_chat_id, post_id):
    try:
        chat_id_str = str(destination_chat_id).replace('-100', '')
        norm_id = int(chat_id_str)

        variants = set()
        variants.add(norm_id)
        variants.add(str(norm_id))
        if str(destination_chat_id).startswith("-100") or norm_id > 0:
            variants.add(int(f"-100{norm_id}"))
            variants.add(f"-100{norm_id}")

        variants.add(destination_chat_id)
        variants.add(str(destination_chat_id))

        placeholders = ','.join('?' for _ in variants)

        query = f'''
            SELECT COUNT(*) FROM logs 
            WHERE (post_id = ? OR post_id = ?)
            AND destination_chat_id IN ({placeholders}) 
            AND log_type IN ('comment', 'comment_reply', 'forbidden')
        '''

        with _db_connect() as conn:
            cursor = conn.cursor()
            args = [post_id, str(post_id)] + list(variants)

            cursor.execute(query, args)
            result = cursor.fetchone()
            return result[0] > 0
    except Exception as e:
        logger.error(f"Ошибка БД при проверке комментария: {e}")
        return True


def _dt_to_utc(dt: datetime) -> datetime:
    if not isinstance(dt, datetime):
        return datetime.now(timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    try:
        return dt.astimezone(timezone.utc)
    except Exception:
        return dt


def _db_get_last_post_time(kind: str, chat_key: str) -> datetime | None:
    kind = (kind or "").strip()
    chat_key = (chat_key or "").strip()
    if not kind or not chat_key:
        return None
    try:
        with _db_connect() as conn:
            row = conn.execute(
                "SELECT last_post_ts FROM chat_last_post_times WHERE kind = ? AND chat_key = ?",
                (kind, chat_key),
            ).fetchone()
        if not row:
            return None
        ts = float(row[0] or 0.0)
        if ts <= 0:
            return None
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        return None


def _db_set_last_post_time(kind: str, chat_key: str, post_time: datetime) -> None:
    kind = (kind or "").strip()
    chat_key = (chat_key or "").strip()
    if not kind or not chat_key:
        return
    dt = _dt_to_utc(post_time)
    now = time.time()
    try:
        with _db_connect() as conn:
            conn.execute(
                """
                INSERT INTO chat_last_post_times(kind, chat_key, last_post_ts, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(kind, chat_key) DO UPDATE SET
                    last_post_ts = excluded.last_post_ts,
                    updated_at = excluded.updated_at
                """,
                (kind, chat_key, float(dt.timestamp()), now),
            )
            conn.commit()
    except Exception:
        return


def _scenario_history_load(chat_id: str, post_id: int) -> dict[int, int]:
    chat_id = (chat_id or "").strip()
    if not chat_id or not post_id:
        return {}
    try:
        with _db_connect() as conn:
            rows = conn.execute(
                """
                SELECT ref_idx, msg_id
                FROM scenario_msg_history
                WHERE chat_id = ? AND post_id = ?
                """,
                (chat_id, int(post_id)),
            ).fetchall()
        out: dict[int, int] = {}
        for ref_idx, msg_id in rows or []:
            try:
                out[int(ref_idx)] = int(msg_id)
            except Exception:
                continue
        return out
    except Exception:
        return {}


def _scenario_history_set(chat_id: str, post_id: int, ref_idx: int, msg_id: int) -> None:
    chat_id = (chat_id or "").strip()
    if not chat_id or not post_id or not ref_idx or not msg_id:
        return
    now = time.time()
    try:
        with _db_connect() as conn:
            conn.execute(
                """
                INSERT INTO scenario_msg_history(chat_id, post_id, ref_idx, msg_id, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(chat_id, post_id, ref_idx) DO UPDATE SET
                    msg_id = excluded.msg_id,
                    updated_at = excluded.updated_at
                """,
                (chat_id, int(post_id), int(ref_idx), int(msg_id), now),
            )
            conn.commit()
    except Exception:
        return


def _scenario_history_clear(chat_id: str, post_id: int) -> None:
    chat_id = (chat_id or "").strip()
    if not chat_id or not post_id:
        return
    try:
        with _db_connect() as conn:
            conn.execute(
                "DELETE FROM scenario_msg_history WHERE chat_id = ? AND post_id = ?",
                (chat_id, int(post_id)),
            )
            conn.commit()
    except Exception:
        return


def _post_plan_seed(chat_key: str, post_id: int) -> int:
    base = f"{chat_key}:{post_id}".encode("utf-8")
    return int(hashlib.sha256(base).hexdigest()[:16], 16)


def _load_post_comment_plan(chat_key: str, post_id: int) -> tuple[int, list[str]] | None:
    if not chat_key or not post_id:
        return None
    try:
        with _db_connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT planned_count, planned_accounts FROM post_comment_plans WHERE chat_key = ? AND post_id = ?",
                (str(chat_key), int(post_id)),
            ).fetchone()
            if not row:
                return None
            planned_count = int(row["planned_count"] or 0)
            raw = row["planned_accounts"]
            planned_accounts = []
            if raw:
                try:
                    planned_accounts = json.loads(raw) or []
                except Exception:
                    planned_accounts = []
            planned_accounts = [str(x) for x in planned_accounts if str(x).strip()]
            return planned_count, planned_accounts
    except Exception:
        return None


def _save_post_comment_plan(chat_key: str, post_id: int, planned_count: int, planned_accounts: list[str]) -> None:
    if not chat_key or not post_id:
        return
    now = time.time()
    try:
        payload = json.dumps(list(planned_accounts or []), ensure_ascii=False)
    except Exception:
        payload = "[]"
    try:
        with _db_connect() as conn:
            conn.execute(
                """
                INSERT INTO post_comment_plans(chat_key, post_id, planned_count, planned_accounts, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_key, post_id) DO UPDATE SET
                    planned_count = excluded.planned_count,
                    planned_accounts = excluded.planned_accounts,
                    updated_at = excluded.updated_at
                """,
                (str(chat_key), int(post_id), int(planned_count), payload, now, now),
            )
            conn.commit()
    except Exception:
        return


def _comment_range_for_target(target_chat: dict, available: int) -> tuple[int, int] | None:
    if available <= 0:
        return None
    try:
        range_min = int(target_chat.get("accounts_per_post_min", 0) or 0)
    except Exception:
        range_min = 0
    try:
        range_max = int(target_chat.get("accounts_per_post_max", 0) or 0)
    except Exception:
        range_max = 0

    range_min = max(range_min, 0)
    range_max = max(range_max, 0)

    if range_min == 0 and range_max == 0:
        return (available, available)

    if range_max < range_min:
        range_max = range_min
    if range_max == 0:
        range_max = range_min
    if range_min == 0:
        range_min = 1

    range_min = min(range_min, available)
    range_max = min(range_max, available)
    if range_max < range_min:
        range_max = range_min
    return (range_min, range_max)


def _get_post_our_accounts_from_db(destination_chat_id: int, post_id: int) -> set[str]:
    """
    Returns session_names of *our* accounts that already acted on this post in this destination chat.
    Used to avoid duplicate comments after restarts/catch-up.
    """
    try:
        raw = str(destination_chat_id).strip()
        variants: set[int] = set()
        try:
            v = int(raw)
            variants.add(v)
        except Exception:
            variants = set()

        if raw.startswith("-100"):
            try:
                bare = int(raw[4:])
                variants.add(bare)
                variants.add(int(f"-100{bare}"))
            except Exception:
                pass
        else:
            # For safety keep both signed/unsigned variants (older logs might normalize).
            try:
                bare = int(raw.lstrip("-"))
                if bare:
                    variants.add(bare)
                    variants.add(-bare)
            except Exception:
                pass

        variants_list = list(variants)
        if not variants_list:
            return set()

        placeholders = ",".join("?" for _ in variants_list)
        query = f"""
            SELECT DISTINCT account_session_name
            FROM logs
            WHERE destination_chat_id IN ({placeholders})
              AND (post_id = ? OR post_id = ?)
              AND log_type IN ('comment', 'comment_reply', 'forbidden')
              AND account_session_name IS NOT NULL
              AND account_session_name != ''
        """
        with _db_connect() as conn:
            rows = conn.execute(query, (*variants_list, int(post_id), str(post_id))).fetchall()
        return {str(r[0]) for r in rows if r and r[0]}
    except Exception:
        return set()


def _ensure_post_comment_plan(
    *,
    chat_key: str,
    post_id: int,
    target_chat: dict,
    eligible_session_names: list[str],
) -> tuple[int, list[str]]:
    existing = _load_post_comment_plan(chat_key, post_id)
    if existing:
        planned_count, planned_accounts = existing
        if planned_count <= 0:
            planned_count = 0
        if planned_accounts:
            return planned_count, planned_accounts

    available = len(eligible_session_names)
    if available <= 0:
        planned_count = 0
        planned_accounts = []
        _save_post_comment_plan(chat_key, post_id, planned_count, planned_accounts)
        return planned_count, planned_accounts

    r = _comment_range_for_target(target_chat, available)
    if not r:
        planned_count = 0
    else:
        rmin, rmax = r
        rnd = random.Random(_post_plan_seed(str(chat_key), int(post_id)))
        planned_count = rnd.randint(rmin, rmax)

    rnd = random.Random(_post_plan_seed(str(chat_key), int(post_id)) ^ 0xA5A5A5A5)
    planned_accounts = eligible_session_names.copy()
    rnd.shuffle(planned_accounts)

    _save_post_comment_plan(chat_key, post_id, planned_count, planned_accounts)
    return planned_count, planned_accounts


def _select_accounts_for_post(
    *,
    chat_key: str,
    post_id: int,
    destination_chat_id: int,
    target_chat: dict,
    eligible_clients: list,
) -> tuple[list, int, int, set[str]]:
    if not eligible_clients:
        return [], 0, 0, set()

    eligible_by_name = {c.session_name: c for c in eligible_clients if getattr(c, "session_name", None)}
    eligible_names = list(eligible_by_name.keys())

    planned_count, planned_accounts = _ensure_post_comment_plan(
        chat_key=str(chat_key),
        post_id=int(post_id),
        target_chat=target_chat,
        eligible_session_names=eligible_names,
    )

    already_accounts = _get_post_our_accounts_from_db(int(destination_chat_id), int(post_id))
    already_count = len(already_accounts)
    remaining_needed = max(int(planned_count) - already_count, 0)
    if remaining_needed <= 0:
        return [], planned_count, already_count, already_accounts

    remaining_names = [n for n in eligible_names if n not in already_accounts]
    remaining_set = set(remaining_names)
    ordered: list[str] = [n for n in planned_accounts if n in remaining_set]

    # If eligible set changed since plan creation, fill from the rest in deterministic order.
    extras = [n for n in remaining_names if n not in set(ordered)]
    if extras:
        rnd = random.Random(_post_plan_seed(str(chat_key), int(post_id)) ^ 0x5C5C5C5C)
        rnd.shuffle(extras)
        ordered.extend(extras)

    selected_names = ordered[:remaining_needed]
    return [eligible_by_name[n] for n in selected_names if n in eligible_by_name], planned_count, already_count, already_accounts


def log_action_to_db(log_entry):
    content = ""
    if log_entry.get('type') == 'reaction':
        content = ' '.join(log_entry.get('reactions', []))
    else:
        content = log_entry.get('comment', '')

    try:
        with _db_connect() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO logs (
                    log_type, timestamp, destination_chat_id, channel_name, channel_username,
                    source_channel_id, post_id, account_session_name, account_first_name,
                    account_username, content
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                log_entry.get('type'),
                log_entry.get('date'),
                log_entry.get('target', {}).get('destination_chat_id'),
                log_entry.get('target', {}).get('chat_name'),
                log_entry.get('target', {}).get('chat_username'),
                log_entry.get('target', {}).get('channel_id'),
                log_entry.get('post_id'),
                log_entry.get('account', {}).get('session_name'),
                log_entry.get('account', {}).get('first_name'),
                log_entry.get('account', {}).get('username'),
                content
            ))
            conn.commit()
        logger.info(
            f"Подробный лог ({log_entry.get('type')}) сохранен в БД для аккаунта {log_entry.get('account', {}).get('session_name')}")
    except sqlite3.Error as e:
        logger.error(f"Ошибка при записи лога в БД: {e}")


def _safe_json_dumps(value) -> str | None:
    if value is None:
        return None
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        try:
            return json.dumps(str(value), ensure_ascii=False)
        except Exception:
            return None


def _db_create_discussion_session(
    *,
    project_id: str,
    discussion_target_id: str | None = None,
    discussion_target_chat_id: str,
    chat_id: str,
    status: str,
    operator_session_name: str | None = None,
    seed_msg_id: int | None = None,
    seed_text: str | None = None,
    settings: dict | None = None,
    participants: list | None = None,
    schedule_at: float | None = None,
    error: str | None = None,
) -> int | None:
    project_id = str(project_id or "").strip() or DEFAULT_PROJECT_ID
    discussion_target_id = str(discussion_target_id or "").strip() or None
    discussion_target_chat_id = str(discussion_target_chat_id or "").strip()
    chat_id = str(chat_id or "").strip()
    status = str(status or "").strip() or "planned"
    if not discussion_target_chat_id or not chat_id:
        return None
    now = time.time()
    started_at = now if status == "running" else None
    try:
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
                    discussion_target_id,
                    discussion_target_chat_id,
                    chat_id,
                    status,
                    float(now),
                    float(started_at) if started_at is not None else None,
                    None,
                    float(schedule_at) if schedule_at is not None else None,
                    str(operator_session_name or "").strip() or None,
                    int(seed_msg_id) if seed_msg_id is not None else None,
                    str(seed_text or "") if seed_text is not None else None,
                    _safe_json_dumps(settings),
                    _safe_json_dumps(participants),
                    str(error or "") if error else None,
                ),
            )
            return int(cur.lastrowid)
    except Exception:
        return None


def _db_update_discussion_session(session_id: int, **fields) -> None:
    if not session_id:
        return
    allowed = {
        "project_id",
        "discussion_target_id",
        "discussion_target_chat_id",
        "chat_id",
        "status",
        "created_at",
        "started_at",
        "finished_at",
        "schedule_at",
        "operator_session_name",
        "seed_msg_id",
        "seed_text",
        "settings_json",
        "participants_json",
        "error",
    }
    updates = []
    params = []
    for key, value in fields.items():
        if key not in allowed:
            continue
        updates.append(f"{key} = ?")
        params.append(value)
    if not updates:
        return
    params.append(int(session_id))
    try:
        with _db_connect() as conn:
            conn.execute(
                f"UPDATE discussion_sessions SET {', '.join(updates)} WHERE id = ?",
                tuple(params),
            )
            conn.commit()
    except Exception:
        pass


def _db_add_discussion_message(
    *,
    session_id: int,
    speaker_type: str,
    speaker_session_name: str | None = None,
    speaker_label: str | None = None,
    msg_id: int | None = None,
    reply_to_msg_id: int | None = None,
    text: str | None = None,
    prompt_info: str | None = None,
    error: str | None = None,
) -> None:
    if not session_id:
        return
    speaker_type = str(speaker_type or "").strip() or "bot"
    now = time.time()
    try:
        with _db_connect() as conn:
            conn.execute(
                """
                INSERT INTO discussion_messages (
                    session_id, created_at, speaker_type,
                    speaker_session_name, speaker_label,
                    msg_id, reply_to_msg_id,
                    text, prompt_info, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(session_id),
                    float(now),
                    speaker_type,
                    str(speaker_session_name or "").strip() or None,
                    str(speaker_label or "").strip() or None,
                    int(msg_id) if msg_id is not None else None,
                    int(reply_to_msg_id) if reply_to_msg_id is not None else None,
                    str(text or "") if text is not None else None,
                    str(prompt_info or "") if prompt_info else None,
                    str(error or "") if error else None,
                ),
            )
            conn.commit()
    except Exception:
        pass


def log_comment_skip_to_db(post_id, target_chat, destination_chat_id, reason):
    try:
        log_action_to_db(
            {
                "type": "comment_skip",
                "post_id": post_id,
                "comment": str(reason or "").strip(),
                "date": datetime.now(timezone.utc).isoformat(),
                "account": {"session_name": ""},
                "target": {
                    "chat_name": target_chat.get("chat_name") if isinstance(target_chat, dict) else None,
                    "chat_username": target_chat.get("chat_username") if isinstance(target_chat, dict) else None,
                    "channel_id": target_chat.get("chat_id") if isinstance(target_chat, dict) else None,
                    "destination_chat_id": destination_chat_id,
                },
            }
        )
    except Exception:
        pass


def log_inbox_message_to_db(
    *,
    kind: str,
    direction: str,
    status: str,
    session_name: str,
    chat_id: str,
    msg_id: int | None = None,
    reply_to_msg_id: int | None = None,
    sender_id: int | None = None,
    sender_username: str | None = None,
    sender_name: str | None = None,
    chat_title: str | None = None,
    chat_username: str | None = None,
    text: str | None = None,
    replied_to_text: str | None = None,
    reactions_summary: str | None = None,
    reactions_updated_at: str | None = None,
    is_read: int = 0,
    error: str | None = None,
) -> int | None:
    kind = (kind or "").strip() or "dm"
    direction = (direction or "").strip() or "in"
    status = (status or "").strip() or "received"
    session_name = (session_name or "").strip()
    chat_id = (chat_id or "").strip()
    if not session_name or not chat_id:
        return None

    created_at = datetime.now(timezone.utc).isoformat()
    try:
        with _db_connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR IGNORE INTO inbox_messages (
                    kind, direction, status, created_at,
                    session_name, chat_id, msg_id, reply_to_msg_id,
                    sender_id, sender_username, sender_name,
                    chat_title, chat_username,
                    text, replied_to_text,
                    reactions_summary, reactions_updated_at,
                    is_read, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    kind,
                    direction,
                    status,
                    created_at,
                    session_name,
                    chat_id,
                    msg_id,
                    reply_to_msg_id,
                    sender_id,
                    (sender_username or "").strip() or None,
                    (sender_name or "").strip() or None,
                    (chat_title or "").strip() or None,
                    (chat_username or "").strip() or None,
                    (text or "").strip() or None,
                    (replied_to_text or "").strip() or None,
                    (reactions_summary or "").strip() or None,
                    (reactions_updated_at or "").strip() or None,
                    int(bool(is_read)),
                    (error or "").strip() or None,
                ),
            )
            conn.commit()
            return cursor.lastrowid or None
    except Exception:
        return None


def _message_text_preview(message) -> str:
    text = getattr(message, "text", None) or getattr(message, "message", None) or ""
    if text:
        return text
    try:
        if getattr(message, "photo", None):
            return "[фото]"
        if getattr(message, "video", None) or getattr(message, "gif", None):
            return "[видео]"
        if getattr(message, "voice", None):
            return "[голосовое]"
        if getattr(message, "audio", None):
            return "[аудио]"
        if getattr(message, "document", None) or getattr(message, "file", None):
            return "[файл]"
    except Exception:
        pass
    return ""


def _peer_chat_id(peer) -> str | None:
    if isinstance(peer, tl_types.PeerChannel):
        return f"-100{peer.channel_id}"
    if isinstance(peer, tl_types.PeerChat):
        return f"-{peer.chat_id}"
    if isinstance(peer, tl_types.PeerUser):
        return str(peer.user_id)
    return None


def _reaction_label(reaction_obj) -> str:
    if not reaction_obj:
        return ""
    emoticon = getattr(reaction_obj, "emoticon", None)
    if emoticon:
        return str(emoticon)
    if hasattr(reaction_obj, "document_id"):
        return "кастом"
    if reaction_obj.__class__.__name__ == "ReactionPaid":
        return "paid"
    return ""


def _reaction_summary_from_update(update) -> str:
    grouped: collections.OrderedDict[str, int] = collections.OrderedDict()

    def add(label: str, count: int) -> None:
        if not label:
            return
        grouped[label] = grouped.get(label, 0) + max(int(count or 0), 0)

    if isinstance(update, tl_types.UpdateMessageReactions):
        for item in (getattr(getattr(update, "reactions", None), "results", None) or []):
            add(_reaction_label(getattr(item, "reaction", None)), int(getattr(item, "count", 0) or 0))
    elif isinstance(update, tl_types.UpdateBotMessageReactions):
        for item in (getattr(update, "reactions", None) or []):
            add(_reaction_label(getattr(item, "reaction", None)), int(getattr(item, "count", 0) or 0))
    elif isinstance(update, tl_types.UpdateBotMessageReaction):
        for reaction_obj in (getattr(update, "new_reactions", None) or []):
            add(_reaction_label(reaction_obj), 1)

    parts: list[str] = []
    for label, count in grouped.items():
        if count <= 0:
            continue
        parts.append(f"{label}×{count}" if count > 1 else label)
    return " ".join(parts)


def _store_message_reaction_event(
    *,
    session_name: str,
    chat_id: str,
    msg_id: int,
    kind: str,
    text: str | None,
    chat_title: str | None,
    chat_username: str | None,
    reactions_summary: str | None,
) -> None:
    if not session_name or not chat_id or not msg_id:
        return
    now = datetime.now(timezone.utc).isoformat()
    summary = (reactions_summary or "").strip()

    with _db_connect() as conn:
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            INSERT INTO inbox_messages (
                kind, direction, status, created_at,
                session_name, chat_id, msg_id,
                chat_title, chat_username, text,
                reactions_summary, reactions_updated_at,
                is_read
            )
            VALUES (?, 'out', 'sent', ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(session_name, chat_id, msg_id, direction) DO UPDATE SET
                kind=excluded.kind,
                status='sent',
                chat_title=COALESCE(excluded.chat_title, inbox_messages.chat_title),
                chat_username=COALESCE(excluded.chat_username, inbox_messages.chat_username),
                text=COALESCE(excluded.text, inbox_messages.text),
                reactions_summary=CASE
                    WHEN excluded.reactions_summary IS NULL OR excluded.reactions_summary = ''
                    THEN NULL
                    ELSE excluded.reactions_summary
                END,
                reactions_updated_at=CASE
                    WHEN excluded.reactions_summary IS NULL OR excluded.reactions_summary = ''
                    THEN NULL
                    ELSE excluded.reactions_updated_at
                END
            """,
            (
                kind,
                now,
                session_name,
                chat_id,
                int(msg_id),
                (chat_title or "").strip() or None,
                (chat_username or "").strip() or None,
                (text or "").strip() or None,
                summary or None,
                now if summary else None,
            ),
        )

        if summary:
            event_text = f"Реакция на сообщение бота: {summary}"
            conn.execute(
                """
                INSERT INTO inbox_messages (
                    kind, direction, status, created_at,
                    session_name, chat_id, msg_id, reply_to_msg_id,
                    chat_title, chat_username, text, replied_to_text,
                    reactions_summary, reactions_updated_at,
                    is_read
                )
                VALUES (?, 'in', 'reaction', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(session_name, chat_id, msg_id, direction) DO UPDATE SET
                    kind=excluded.kind,
                    status='reaction',
                    created_at=excluded.created_at,
                    reply_to_msg_id=excluded.reply_to_msg_id,
                    chat_title=COALESCE(excluded.chat_title, inbox_messages.chat_title),
                    chat_username=COALESCE(excluded.chat_username, inbox_messages.chat_username),
                    text=excluded.text,
                    replied_to_text=COALESCE(excluded.replied_to_text, inbox_messages.replied_to_text),
                    reactions_summary=excluded.reactions_summary,
                    reactions_updated_at=excluded.reactions_updated_at,
                    is_read=0,
                    error=NULL
                """,
                (
                    kind,
                    now,
                    session_name,
                    chat_id,
                    int(msg_id),
                    int(msg_id),
                    (chat_title or "").strip() or None,
                    (chat_username or "").strip() or None,
                    event_text,
                    (text or "").strip() or None,
                    summary,
                    now,
                ),
            )
        else:
            conn.execute(
                """
                DELETE FROM inbox_messages
                WHERE session_name=? AND chat_id=? AND msg_id=? AND direction='in' AND status='reaction'
                """,
                (session_name, chat_id, int(msg_id)),
            )

        conn.commit()


def _queued_outgoing_exists(
    *,
    kind: str,
    session_name: str,
    chat_id: str,
    text: str | None,
    reply_to_msg_id: int | None,
) -> bool:
    if not session_name or not chat_id:
        return False
    try:
        with _db_connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM inbox_messages
                WHERE kind=?
                  AND direction='out'
                  AND status='queued'
                  AND session_name=?
                  AND chat_id=?
                  AND COALESCE(text, '') = COALESCE(?, '')
                  AND COALESCE(reply_to_msg_id, -1) = COALESCE(?, -1)
                LIMIT 1
                """,
                (kind, session_name, str(chat_id), text or "", reply_to_msg_id),
            ).fetchone()
            return row is not None
    except Exception:
        return False


def load_config(section):
    parser = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Файл config.ini не найден: {CONFIG_FILE}")
    parser.read(CONFIG_FILE)
    if section not in parser:
        raise KeyError(f"В config.ini не найдена секция [{section}].")
    return parser[section]


def load_json_data(file_path, default_data=None):
    if default_data is None:
        default_data = {}
    return load_json(file_path, default_data)


def load_proxies():
    if not os.path.exists(PROXIES_FILE):
        logger.warning(f"Файл {PROXIES_FILE} не найден")
        return []
    try:
        with open(PROXIES_FILE, 'r') as f:
            proxies = [line.strip() for line in f if line.strip()]
            logger.debug(f"Загружено {len(proxies)} прокси из {PROXIES_FILE}")
            return proxies
    except Exception as e:
        logger.error(f"Ошибка при чтении файла с прокси {PROXIES_FILE}: {e}")
        return []


async def generate_comment(
    post_text,
    target_chat,
    session_name,
    image_bytes=None,
    is_reply_mode=False,
    reply_to_name=None,
    extra_instructions=None,
):
    global current_settings, RECENT_GENERATED_MESSAGES

    provider = target_chat.get('ai_provider', 'default')
    if provider == 'default':
        provider = current_settings.get('ai_provider', 'gemini')

    accounts_data = load_project_accounts(current_settings)
    account = next((a for a in accounts_data if a['session_name'] == session_name), None)
    role_id, role_data = role_for_account(account or {}, current_settings)
    role_prompt, role_meta = build_role_prompt(role_data, current_settings)
    role_name = str(role_data.get("name") or role_id or "Роль")
    mood_name = str(role_meta.get("mood") or "").strip()
    prompt_info = f"Роль: {role_name}" + (f" · настроение: {mood_name}" if mood_name else "")
    emoji_level = str(role_meta.get("emoji_level") or role_data.get("emoji_level") or "minimal")

    if not role_prompt:
        role_prompt = (
            "Пиши коротко и по теме поста как живой пользователь Telegram. "
            "Избегай канцелярита, штампов и тона нейросети."
        )

    global_blacklist = current_settings.get('blacklist', [])
    h_set = current_settings.get('humanization', {})
    custom_rules = h_set.get('custom_rules', "")
    vector_prompt = ""
    try:
        vector_prompt = str((target_chat or {}).get("vector_prompt") or "").strip()
    except Exception:
        vector_prompt = ""
    product_knowledge_prompt = ""
    try:
        product_knowledge_prompt = str(
            ((current_settings.get("product_knowledge", {}) or {}).get("prompt") or "")
        ).strip()
    except Exception:
        product_knowledge_prompt = ""
    raw_penalty = float(h_set.get('repetition_penalty', 0))
    frequency_penalty_val = min(max(raw_penalty / 50, 0.0), 2.0)
    try:
        max_tokens_val = int(h_set.get('max_tokens', 100))
    except Exception:
        max_tokens_val = 100
    if max_tokens_val <= 0:
        logger.warning("⚠️ humanization.max_tokens <= 0; использую 90 по умолчанию.")
        max_tokens_val = 90
    custom_temp = h_set.get('temperature')

    system_prompt = f"ТВОЯ РОЛЬ (ОТЫГРЫВАЙ ЕЕ ДОСЛОВНО):\n{role_prompt}\n\n"
    if vector_prompt:
        system_prompt += (
            "ВЕКТОР / ТЕМА (ОБЯЗАТЕЛЬНО):\n"
            f"{vector_prompt}\n\n"
            "Твоя реплика должна соответствовать этому вектору. "
            "Если в векторе перечислены конкретные объекты/модели/сервисы/проблемы — "
            "естественно упоминай 1–2 из них по месту.\n\n"
        )
    if product_knowledge_prompt:
        system_prompt += (
            "ЗНАНИЕ О ПРОДУКТЕ (ДОП. КОНТЕКСТ):\n"
            f"{product_knowledge_prompt}\n\n"
            "Используй это знание только если оно уместно по роли и текущему контексту. "
            "Не обязано проявляться в каждом ответе. Не противоречь теме беседы и не выдумывай факты.\n\n"
        )
    system_prompt += f"ПРАВИЛА ОФОРМЛЕНИЯ ТЕКСТА:\n{custom_rules}\n"
    if global_blacklist:
        system_prompt += f"\nНЕ ИСПОЛЬЗУЙ СЛОВА: {', '.join(global_blacklist)}"

    if is_reply_mode:
        context_prefix = f"ТЕБЕ ГОВОРИТ {reply_to_name}: " if reply_to_name else ""
        user_template = (
            "КОНТЕКСТ ДИАЛОГА:\n{context}{post}\n\n"
            "Ответь согласно своей роли.\n"
            "{length_hint}\n"
            "{style_hint}\n"
            "{question_hint}"
        )
    else:
        context_prefix = ""
        user_template = (
            "ТЕКСТ ПОСТА:\n{post}\n\n"
            "Напиши комментарий от своей роли.\n"
            "Если текста поста нет — не проси прислать текст и не пиши, что ты его не видишь. "
            "Просто оставь короткую нейтральную реплику по ситуации, без вопросов.\n"
            "{length_hint}\n"
            "{style_hint}\n"
            "{question_hint}"
        )

    base_extra = (extra_instructions or "").strip()

    api_keys = current_settings.get('api_keys', {})
    main_api_key = api_keys.get(provider)
    if not main_api_key:
        return None, f"{prompt_info} · FAIL: missing_api_key({provider})"

    def _short_exc(e: Exception) -> str:
        try:
            msg = str(e).replace("\n", " ").strip()
        except Exception:
            msg = ""
        if msg:
            msg = re.sub(r"\\s+", " ", msg)
        if msg and len(msg) > 220:
            msg = msg[:219].rstrip() + "…"
        return f"{type(e).__name__}: {msg}" if msg else type(e).__name__

    retry_cfg = current_settings.get("ai_retry", {}) if isinstance(current_settings, dict) else {}
    try:
        max_attempts = int(retry_cfg.get("max_attempts", 3) or 0)
    except Exception:
        max_attempts = 3
    max_attempts = max(min(max_attempts, 5), 1)

    try:
        timeout_sec = float(retry_cfg.get("timeout_sec", 45) or 0)
    except Exception:
        timeout_sec = 45.0
    timeout_sec = max(min(timeout_sec, 180.0), 5.0)

    try:
        base_backoff_sec = float(retry_cfg.get("base_backoff_sec", 0.8) or 0)
    except Exception:
        base_backoff_sec = 0.8
    base_backoff_sec = max(min(base_backoff_sec, 10.0), 0.0)

    try:
        max_backoff_sec = float(retry_cfg.get("max_backoff_sec", 6.0) or 0)
    except Exception:
        max_backoff_sec = 6.0
    max_backoff_sec = max(max_backoff_sec, base_backoff_sec)

    def _is_fatal_failure(failure: str) -> bool:
        s = (failure or "").lower()
        fatal_phrases = [
            "insufficient_quota",
            "exceeded your current quota",
            "quota exceeded",
            "billing",
            "payment required",
            "please check your plan",
            "invalid api key",
            "invalid_api_key",
            "incorrect api key",
            "no api key",
            "authentication",
            "unauthorized",
            "forbidden",
            "account has been disabled",
            "organization has been disabled",
        ]
        return any(p in s for p in fatal_phrases)

    def _is_context_too_long(failure: str) -> bool:
        s = (failure or "").lower()
        phrases = [
            "context_length_exceeded",
            "maximum context length",
            "too many tokens",
            "context length",
            "token limit",
        ]
        return any(p in s for p in phrases)

    def _compute_retry_delay(attempt_index: int, failure: str) -> float:
        if attempt_index <= 0:
            return 0.0
        s = (failure or "").lower()
        fast_fail = any(p in s for p in ["empty_response", "empty_or_too_short", "tool_calls"])
        base = 0.2 if fast_fail else base_backoff_sec
        delay = min(max_backoff_sec, base * (2 ** (attempt_index - 1)))
        jitter = min(0.35, delay * 0.25)
        return max(delay + random.uniform(0, jitter), 0.0)

    def _truncate_for_prompt(text: str, limit: int) -> str:
        t = str(text or "")
        if limit <= 0 or len(t) <= limit:
            return t
        return t[: max(limit - 1, 1)].rstrip() + "…"

    def _sample_format_hints() -> tuple[str, str]:
        roll = random.randint(1, 100)
        if roll <= 30:
            return (
                "Длина: 1 короткое предложение, примерно 4-12 слов.",
                "Подача: ленивая бытовая реплика без длинного вступления.",
            )
        if roll <= 68:
            return (
                "Длина: 2 коротких предложения, примерно 10-24 слова.",
                "Подача: первая фраза - реакция, вторая - короткое уточнение по той же мысли.",
            )
        if roll <= 85:
            return (
                "Длина: 2 коротких предложения, примерно 14-30 слов.",
                "Подача: мягкое согласие или сомнение + одна конкретная деталь из поста.",
            )
        return (
            "Длина: 2-3 коротких предложения, примерно 18-36 слов, максимум 4 предложения.",
            "Подача: разверни мысль в 2-3 короткие фразы, без воды и без абзацев.",
        )

    def _sample_question_hint() -> str:
        # Questions should be rare to avoid repetitive "interview style" comments.
        roll = random.randint(1, 100)
        if roll <= 25:
            return "Вопросительный знак допустим, но только один и только если реально уместно."
        return "Предпочти формат без вопросительного знака: утверждение, сомнение или наблюдение."

    def _retry_adjustments(attempt_index: int, failure: str, base_max_tokens: int) -> tuple[str, int, int | None]:
        s = (failure or "").lower()
        extra_lines: list[str] = []
        new_max_tokens = int(base_max_tokens or 0)
        if new_max_tokens <= 0:
            new_max_tokens = 90

        post_char_limit: int | None = None

        if "finish_reason=length" in s:
            extra_lines.append("Ответь короче: 1 короткое предложение (до 160 символов).")
            new_max_tokens = min(max(new_max_tokens, 128), 256)

        if "empty_or_too_short" in s or "empty_response" in s:
            extra_lines.append("Ответ не должен быть пустым. 6–20 слов, строго по теме.")
            new_max_tokens = max(new_max_tokens, 96)

        if _is_context_too_long(s):
            post_char_limit = 3500
            extra_lines.append("Если текст очень длинный — комментируй по одному ключевому тезису, кратко.")

        if attempt_index >= 2:
            extra_lines.append("Не используй markdown. Выведи только текст комментария.")

        return "\n".join([l for l in extra_lines if l]).strip(), int(new_max_tokens), post_char_limit

    last_failure = "unknown_error"
    last_model = None

    post_text_for_prompt = str(post_text or "")
    for attempt in range(max_attempts):
        retry_extra = ""
        attempt_max_tokens_val = max_tokens_val
        if attempt > 0:
            if _is_fatal_failure(last_failure):
                break
            delay = _compute_retry_delay(attempt, last_failure)
            if delay > 0:
                model_part = f"{provider}:{last_model}" if last_model else provider
                logger.info(
                    f"🔁 AI retry {attempt + 1}/{max_attempts} через {delay:.1f}с ({model_part}): {last_failure}"
                )
                await asyncio.sleep(delay)
            retry_extra, attempt_max_tokens_val, post_char_limit = _retry_adjustments(
                attempt, last_failure, max_tokens_val
            )
            if post_char_limit:
                post_text_for_prompt = _truncate_for_prompt(post_text_for_prompt, post_char_limit)
        try:
            final_temp = None
            if custom_temp is not None and str(custom_temp).strip() != "":
                try:
                    final_temp = float(custom_temp)
                except Exception:
                    final_temp = None
            generated_text = None

            length_hint, style_hint = _sample_format_hints()
            question_hint = _sample_question_hint()
            user_message_content = user_template.format(
                context=context_prefix,
                post=post_text_for_prompt,
                length_hint=length_hint,
                style_hint=style_hint,
                question_hint=question_hint,
            )
            if base_extra:
                user_message_content = f"{user_message_content}\n\n{base_extra}"
            if retry_extra:
                user_message_content = f"{user_message_content}\n\n{retry_extra}"

            if provider in {"openai", "openrouter", "deepseek"}:
                base_url = None
                default_headers = None
                if provider == "deepseek":
                    base_url = "https://api.deepseek.com"
                elif provider == "openrouter":
                    base_url = "https://openrouter.ai/api/v1"
                    default_headers = {
                        "HTTP-Referer": os.getenv("OPENROUTER_REFERRER", "http://localhost"),
                        "X-Title": os.getenv("OPENROUTER_TITLE", "AI-Центр"),
                    }

                if provider == "deepseek":
                    models_to_try = [get_model_setting(current_settings, "deepseek_chat")]
                elif provider == "openrouter":
                    models_to_try = [get_model_setting(current_settings, "openrouter_chat")]
                else:
                    models_to_try = openai_model_candidates(current_settings, "openai_chat")

                client_kwargs = {"api_key": main_api_key}
                if base_url:
                    client_kwargs["base_url"] = base_url
                if default_headers:
                    client_kwargs["default_headers"] = default_headers
                try:
                    client_kwargs["timeout"] = timeout_sec
                    client = openai.AsyncOpenAI(**client_kwargs)
                except TypeError:
                    client_kwargs.pop("timeout", None)
                    client = openai.AsyncOpenAI(**client_kwargs)
                user_content = user_message_content
                if provider in {"openai", "openrouter"} and image_bytes:
                    mime_type = guess_image_mime_type(image_bytes)
                    base64_image = base64.b64encode(image_bytes).decode('utf-8')
                    user_content = [
                        {"type": "text", "text": user_message_content},
                        {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{base64_image}"}},
                    ]

                completion = None
                for model_name in models_to_try:
                    try:
                        last_model = model_name
                        create_kwargs = {
                            "model": model_name,
                            "messages": [
                                {"role": "system", "content": system_prompt},
                                {"role": "user", "content": user_content},
                            ],
                        }
                        if final_temp is not None:
                            create_kwargs["temperature"] = final_temp
                        if provider != "openrouter":
                            create_kwargs["frequency_penalty"] = frequency_penalty_val
                        if provider == "openai":
                            create_kwargs["max_completion_tokens"] = attempt_max_tokens_val
                        else:
                            create_kwargs["max_tokens"] = attempt_max_tokens_val
                        completion = await asyncio.wait_for(
                            client.chat.completions.create(**create_kwargs), timeout=timeout_sec + 5
                        )
                    except Exception as e:
                        last_failure = _short_exc(e)
                        if provider == "openai" and is_model_unavailable_error(e):
                            continue
                        raise
                    if completion is None:
                        continue

                    try:
                        choice0 = completion.choices[0]
                        msg0 = choice0.message
                    except Exception:
                        choice0 = None
                        msg0 = None

                    finish_reason = getattr(choice0, "finish_reason", None) if choice0 is not None else None
                    refusal = getattr(msg0, "refusal", None) if msg0 is not None else None
                    tool_calls = getattr(msg0, "tool_calls", None) if msg0 is not None else None
                    raw_content = getattr(msg0, "content", None) if msg0 is not None else None

                    raw_text = raw_content if isinstance(raw_content, str) else ""
                    generated_text = raw_text.strip()

                    if generated_text:
                        break

                    # If we received a response but no usable content, classify it and try the next model candidate.
                    out_tokens = None
                    in_tokens = None
                    try:
                        usage = getattr(completion, "usage", None)
                        out_tokens = getattr(usage, "completion_tokens", None) if usage is not None else None
                        in_tokens = getattr(usage, "prompt_tokens", None) if usage is not None else None
                    except Exception:
                        pass

                    if isinstance(refusal, str) and refusal.strip():
                        last_failure = f"refusal: {refusal.strip()[:220]}"
                        break
                    if tool_calls:
                        try:
                            last_failure = f"tool_calls({len(tool_calls)})"
                        except Exception:
                            last_failure = "tool_calls"
                        continue

                    if raw_text and not raw_text.strip():
                        details = []
                        if finish_reason:
                            details.append(f"finish_reason={finish_reason}")
                        if out_tokens is not None:
                            details.append(f"output_tokens={out_tokens}")
                        if in_tokens is not None:
                            details.append(f"prompt_tokens={in_tokens}")
                        detail_str = ", ".join(details)
                        last_failure = f"empty_response(whitespace_only{', ' + detail_str if detail_str else ''})"
                        continue

                    details = []
                    if finish_reason:
                        details.append(f"finish_reason={finish_reason}")
                    if out_tokens is not None:
                        details.append(f"output_tokens={out_tokens}")
                    if in_tokens is not None:
                        details.append(f"prompt_tokens={in_tokens}")
                    if details:
                        last_failure = "empty_response(" + ", ".join(details) + ")"
                    else:
                        last_failure = "empty_response"
                    continue
            elif provider == 'gemini':
                contents = [user_message_content]
                if image_bytes:
                    mime_type = guess_image_mime_type(image_bytes)
                    contents.append(genai_types.Part.from_bytes(data=image_bytes, mime_type=mime_type))

                for model_name in gemini_model_candidates(current_settings, "gemini_chat"):
                    try:
                        last_model = model_name
                        config_kwargs = {
                            "system_instruction": system_prompt,
                            "max_output_tokens": attempt_max_tokens_val,
                        }
                        if final_temp is not None:
                            config_kwargs["temperature"] = final_temp
                        async with genai.Client(api_key=main_api_key).aio as aclient:
                            response = await asyncio.wait_for(
                                aclient.models.generate_content(
                                    model=model_name,
                                    contents=contents,
                                    config=genai_types.GenerateContentConfig(**config_kwargs),
                                ),
                                timeout=timeout_sec + 5,
                            )
                        generated_text = (response.text or "").strip()
                        break
                    except Exception as e:
                        last_failure = _short_exc(e)
                        generated_text = None
                        continue

            if generated_text:
                generated_text = enforce_emoji_level(generated_text, emoji_level)
                clean_gen = generated_text.replace('"', '').replace("'", "").lower().strip()
                if len(clean_gen) < 2:
                    last_failure = "empty_or_too_short(output)"
                    continue
                RECENT_GENERATED_MESSAGES.append(generated_text)
                return generated_text, prompt_info
            if last_failure == "unknown_error":
                last_failure = "empty_or_too_short(output)"
        except Exception as e:
            last_failure = _short_exc(e)
            continue

    model_part = f"{provider}:{last_model}" if last_model else provider
    return None, f"{prompt_info} · FAIL({model_part}): {last_failure}"


def normalize_id(chat_id):
    if not chat_id:
        return 0
    try:
        return int(str(chat_id).replace('-100', ''))
    except ValueError:
        return 0


def _select_accounts_with_rotation(chat_key: str, eligible_clients: list, count: int) -> list:
    if not eligible_clients:
        return []

    eligible_names = [c.session_name for c in eligible_clients]
    eligible_set = set(eligible_names)
    if count <= 0:
        count = len(eligible_names)
    if count >= len(eligible_names):
        count = len(eligible_names)

    state = COMMENTER_ROTATION.setdefault(chat_key, {"remaining": [], "used": set()})
    remaining = [n for n in state.get("remaining", []) if n in eligible_set]
    used = {n for n in state.get("used", set()) if n in eligible_set}

    for name in eligible_names:
        if name not in used and name not in remaining:
            remaining.append(name)

    if not remaining:
        remaining = eligible_names.copy()
        random.shuffle(remaining)
        used = set()

    selected_names = []
    selected_set = set()
    while len(selected_names) < count:
        if not remaining:
            remaining = eligible_names.copy()
            random.shuffle(remaining)
            used = set(selected_set)
            remaining = [n for n in remaining if n not in used]
            if not remaining:
                break
        take = min(count - len(selected_names), len(remaining))
        picked = remaining[:take]
        for name in picked:
            if name in selected_set:
                continue
            selected_names.append(name)
            selected_set.add(name)
            used.add(name)
        remaining = remaining[take:]

    state["remaining"] = remaining
    state["used"] = used

    by_name = {c.session_name: c for c in eligible_clients}
    return [by_name[name] for name in selected_names if name in by_name]


def make_fallback_comment_variant(base_text: str, session_name: str, msg_id: int) -> str:
    text = (base_text or "").strip()
    if not text:
        return ""

    # Deterministic per account + message, to avoid identical duplicates.
    try:
        import hashlib

        seed = int(hashlib.sha256(f"{session_name}:{msg_id}".encode("utf-8")).hexdigest()[:8], 16)
        rnd = random.Random(seed)
    except Exception:
        rnd = random

    prefixes = ["Ну", "Кстати", "Честно", "Имхо", "По-моему", "Согласен", "Мне кажется"]
    suffixes = [" (имхо)", " 👍", " 😅", " 🤷‍♂️", " 🤝"]

    prefix = rnd.choice(prefixes)
    suffix = rnd.choice(suffixes)

    out = text
    if not out.lower().startswith(prefix.lower()):
        out = f"{prefix}, {out.lstrip()}"

    out = out.rstrip()
    if not out.endswith(suffix.strip()):
        out = f"{out}{suffix}"

    return out.strip()


COMMENT_DIVERSITY_MODES = [
    "Ленивая бытовая реплика в 1 короткую фразу.",
    "Короткая реакция + короткое уточнение второй фразой.",
    "Мягкое сомнение по одной детали из поста без агрессии.",
    "Нейтральное согласие или несогласие + личное наблюдение.",
    "Спокойный практичный комментарий без умных формулировок.",
    "Лёгкая ирония без грубости и без шуток в лоб.",
]

SEMANTIC_DIVERSITY_ANGLES = [
    "Уточни детали: задай один конкретный вопрос по теме.",
    "Дай практический совет/следующий шаг (без категоричности).",
    "Озвучь ограничение/условие: когда это может не сработать.",
    "Добавь возможное последствие/влияние (в перспективе).",
    "Предложи критерий/метрику: как понять, что получилось.",
    "Приведи мягкий пример «из жизни» без выдуманных фактов.",
    "Мягко не согласись по одной детали (без токсичности).",
    "Добавь личное наблюдение/опыт (без конкретных фактов/цифр).",
    "Сформулируй альтернативный взгляд: другой приоритет/цель.",
    "Спроси про условия/границы: для кого/когда это актуально.",
    "Отметь риск/подводный камень и как его снизить.",
    "Сделай короткое сравнение с похожим кейсом (без ссылок/имен).",
    "Займи позицию «скепсис, но без хейта»: что нужно проверить.",
    "Поддержи автора и добавь одно уточнение по делу.",
]


def _normalize_for_similarity(text: str) -> str:
    t = str(text or "").lower()
    t = re.sub(r"https?://\S+", "", t)
    t = re.sub(r"[@#][\w_]+", "", t)
    t = re.sub(r"[^\w\s]+", " ", t, flags=re.UNICODE)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _word_tokens(text: str) -> list[str]:
    t = _normalize_for_similarity(text)
    if not t:
        return []
    return [w for w in t.split() if len(w) > 2]


_URL_RE = re.compile(r"(https?://\S+|t\.me/\S+|www\.\S+)", re.IGNORECASE)
_PROMO_EXPLICIT_RE = re.compile(
    r"(#\s*)?(реклама|sponsored|ad\b|спонсор\w*|партн[её]р\w*|промокод|promo(code)?|sale|скидк\w*|акци\w*|розыгрыш|giveaway)",
    re.IGNORECASE,
)
_CTA_RE = re.compile(
    r"(куп(и|ить|ите|ай)\b|закаж(и|ать|ите)\b|оформ(и|ить)\b|переходи(те)?\b|жми\b|ссылка\s+в\s+(био|описании|профиле)\b|подпис(ывайся|ывайтесь)\b|забира(й|йте)\b|регист(рируйся|рируйтесь|рация)\b)",
    re.IGNORECASE,
)
_PRICE_RE = re.compile(r"(?:\b\d{2,}\s*(?:₽|руб(?:\.|ля|лей)?|rur|\$|usd|€|eur)\b)", re.IGNORECASE)
_TRACKING_RE = re.compile(r"(utm_[a-z_]+|ref=|aff=|promo=|coupon|promocode)", re.IGNORECASE)


def _is_promotional_post_text(text: str) -> tuple[bool, str]:
    t = str(text or "")
    if not t.strip():
        return False, ""
    if _PROMO_EXPLICIT_RE.search(t):
        return True, "explicit_marker"

    has_url = bool(_URL_RE.search(t))
    has_cta = bool(_CTA_RE.search(t))
    has_price = bool(_PRICE_RE.search(t))
    has_tracking = bool(_TRACKING_RE.search(t))

    if has_url and (has_cta or has_price or has_tracking):
        return True, "link_cta_or_price"
    if has_price and has_cta:
        return True, "price_cta"
    return False, ""


def _non_image_media_kind(message) -> str | None:
    if not message:
        return None
    try:
        if getattr(message, "voice", None):
            return "voice"
        if getattr(message, "audio", None):
            return "audio"
        if getattr(message, "video", None):
            return "video"
        if getattr(message, "gif", None):
            return "gif"
        if getattr(message, "photo", None):
            return None
        if getattr(message, "file", None):
            mime_type = getattr(message.file, "mime_type", None) or ""
            if isinstance(mime_type, str) and mime_type.lower().startswith("image/"):
                return None
            if isinstance(mime_type, str) and mime_type:
                if mime_type.lower().startswith("video/"):
                    return "video"
                if mime_type.lower().startswith("audio/"):
                    return "audio"
                return "file"
        if getattr(message, "document", None):
            return "file"
    except Exception:
        return "media"
    return None


def should_skip_post_for_commenting(message, post_text: str, target_chat: dict) -> tuple[bool, str]:
    try:
        meaningful_words = len(_word_tokens(post_text))
    except Exception:
        meaningful_words = 0

    skip_ads = bool(target_chat.get("skip_promotional_posts", True))
    if skip_ads:
        is_ad, why = _is_promotional_post_text(post_text)
        if is_ad:
            return True, f"похоже на рекламу ({why})"

    try:
        min_meaningful_words = int(target_chat.get("min_meaningful_words", 2) or 0)
    except Exception:
        min_meaningful_words = 2
    min_meaningful_words = max(min_meaningful_words, 0)

    if min_meaningful_words > 0 and meaningful_words < min_meaningful_words:
        return True, f"слишком мало текста ({meaningful_words}/{min_meaningful_words} смысловых слов)"

    skip_short_media = bool(target_chat.get("skip_short_media_posts", True))
    if skip_short_media:
        media_kind = _non_image_media_kind(message)
        if media_kind:
            try:
                media_min_words = int(target_chat.get("media_min_meaningful_words", 6) or 0)
            except Exception:
                media_min_words = 6
            media_min_words = max(media_min_words, 0)
            if media_min_words > 0 and meaningful_words < media_min_words:
                return True, f"{media_kind} + мало текста ({meaningful_words}/{media_min_words})"

    return False, ""


def _opening_signature(text: str, n: int = 4) -> tuple[str, ...]:
    tokens = _word_tokens(text)
    return tuple(tokens[:n])


def comment_similarity_score(a: str, b: str) -> float:
    na = _normalize_for_similarity(a)
    nb = _normalize_for_similarity(b)
    if not na or not nb:
        return 0.0
    ratio = difflib.SequenceMatcher(None, na, nb).ratio()
    aw = set(_word_tokens(na))
    bw = set(_word_tokens(nb))
    jaccard = (len(aw & bw) / max(len(aw | bw), 1)) if (aw or bw) else 0.0
    return max(ratio, jaccard)


def is_comment_too_similar(candidate: str, existing: list[str], threshold: float) -> tuple[bool, float, str | None]:
    best_score = 0.0
    best_text = None
    for prev in existing or []:
        score = comment_similarity_score(candidate, prev)
        if score > best_score:
            best_score = score
            best_text = prev

    too_similar = best_score >= threshold
    if best_text:
        open_a = _opening_signature(candidate, 4)
        open_b = _opening_signature(best_text, 4)
        if open_a and open_a == open_b:
            too_similar = True

    return too_similar, best_score, best_text


def _truncate_one_line(text: str, limit: int = 240) -> str:
    t = str(text or "").replace("\n", " ").strip()
    t = re.sub(r"\\s+", " ", t)
    if len(t) <= limit:
        return t
    return t[: limit - 1].rstrip() + "…"


def _extract_opening_phrases(texts: list[str], max_phrases: int = 6) -> list[str]:
    phrases: list[str] = []
    seen = set()
    for t in texts or []:
        tokens = _word_tokens(t)
        if len(tokens) < 2:
            continue
        phrase = " ".join(tokens[:4]).strip()
        if not phrase:
            continue
        if phrase in seen:
            continue
        seen.add(phrase)
        phrases.append(phrase)
        if len(phrases) >= max_phrases:
            break
    return phrases


def build_comment_diversity_instructions(
    existing_comments: list[str],
    mode_hint: str | None = None,
    strict: bool = False,
    previous_candidate: str | None = None,
) -> str:
    parts: list[str] = []

    if mode_hint:
        parts.append(f"СТИЛЕВОЙ РЕЖИМ: {mode_hint}")

    if existing_comments:
        parts.append(
            "ВАЖНО: не повторяй и не перефразируй комментарии ниже. "
            "Сделай заметно другой угол/мысль/формулировки."
        )
        openings = _extract_opening_phrases(existing_comments)
        if openings:
            parts.append('Не начинай так же. Запрещённые начала: "' + '"; "'.join(openings) + '"')
        for i, c in enumerate(existing_comments[-3:], start=1):
            parts.append(f"{i}) {_truncate_one_line(c)}")

    if strict:
        parts.append(
            "Проверка на похожесть сработала. Перепиши так, чтобы совпадений по словам было минимально "
            "(другие вводные, другие конструкции, другая подача)."
        )

    if previous_candidate:
        parts.append("ТВОЙ ПРОШЛЫЙ ВАРИАНТ (НЕ ПОВТОРЯЙ): " + _truncate_one_line(previous_candidate))

    return "\n".join([p for p in parts if p]).strip()


_RU_STOPWORDS = {
    "и",
    "а",
    "но",
    "да",
    "нет",
    "это",
    "как",
    "что",
    "в",
    "на",
    "по",
    "за",
    "к",
    "у",
    "из",
    "для",
    "с",
    "со",
    "же",
    "то",
    "тут",
    "там",
    "вот",
    "ну",
    "типа",
    "просто",
    "вообще",
    "всё",
    "все",
    "еще",
    "ещё",
    "если",
    "или",
    "когда",
    "где",
    "почему",
    "зачем",
    "потому",
    "кстати",
    "имхо",
}


def _extract_keywords(text: str, max_keywords: int = 2) -> list[str]:
    tokens = [t for t in _word_tokens(text) if t not in _RU_STOPWORDS and not t.isdigit() and len(t) >= 4]
    if not tokens:
        return []
    counts = collections.Counter(tokens)
    return [w for (w, _) in counts.most_common(max_keywords)]


def _stable_seed_int(seed_text: str) -> int:
    try:
        import hashlib

        return int(hashlib.sha256(str(seed_text).encode("utf-8")).hexdigest()[:8], 16)
    except Exception:
        return abs(hash(str(seed_text))) % (2**31)


def _stable_shuffled(items: list[str], seed_text: str) -> list[str]:
    if not items:
        return []
    out = items.copy()
    rnd = random.Random(_stable_seed_int(seed_text))
    rnd.shuffle(out)
    return out


def _content_tokens(text: str) -> list[str]:
    return [t for t in _word_tokens(text) if t not in _RU_STOPWORDS and not t.isdigit() and len(t) >= 4]


def build_semantic_diversity_instructions(
    post_text: str,
    *,
    angle_hint: str | None = None,
    strict: bool = False,
    previous_candidate: str | None = None,
) -> str:
    kws = _extract_keywords(post_text, max_keywords=2)
    kw_line = f"Ключевые слова поста: {', '.join(kws)}." if kws else ""

    parts: list[str] = [
        "ВАЖНО: не пересказывай и не перефразируй уже написанное другими нашими аккаунтами под этим постом.",
        "Сделай комментарий по теме поста, но с ДРУГИМ смысловым ходом (новый аспект/угол).",
    ]
    if angle_hint:
        parts.append(f"СМЫСЛОВОЙ УГОЛ (обязателен): {angle_hint}")
    if kw_line:
        parts.append(kw_line)
    parts.append("Опирайся на 1 деталь из поста, чтобы было естественно и по теме.")
    parts.append("Если контекста не хватает — лучше задай один уточняющий вопрос, чем делай утверждения.")

    if strict:
        parts.append(
            "Проверка разнообразия сработала: перепиши так, чтобы это была ДРУГАЯ мысль/ход (вопрос/совет/последствие/пример)."
        )
    if previous_candidate:
        parts.append("ТВОЙ ПРОШЛЫЙ ВАРИАНТ (НЕ ПОВТОРЯЙ): " + _truncate_one_line(previous_candidate))

    return "\n".join([p for p in parts if p]).strip()


def comment_needs_more_novelty(
    candidate: str,
    *,
    post_text: str,
    existing_comments: list[str],
    min_new_tokens: int,
) -> tuple[bool, int]:
    if min_new_tokens <= 0:
        return False, 0
    if not (candidate or "").strip():
        return True, 0
    if not existing_comments:
        return False, 0

    base = set(_content_tokens(post_text))
    seen = set()
    for c in existing_comments or []:
        seen.update(_content_tokens(c))

    cand = set(_content_tokens(candidate))
    if not cand:
        return True, 0

    new = {t for t in cand if t not in base and t not in seen}
    return (len(new) < min_new_tokens), len(new)


def make_emergency_comment(
    post_text: str,
    session_name: str,
    msg_id: int,
    existing_comments: list[str] | None = None,
    threshold: float = 0.78,
) -> str:
    try:
        import hashlib

        seed = int(hashlib.sha256(f"emg:{session_name}:{msg_id}".encode("utf-8")).hexdigest()[:8], 16)
        rnd = random.Random(seed)
    except Exception:
        rnd = random

    kw = _extract_keywords(post_text, max_keywords=1)
    keyword = kw[0] if kw else ""

    templates_kw = [
        "интересно, а {kw} тут как считают/проверяют?",
        "звучит логично, но где подводные по {kw}?",
        "а есть примеры/цифры по {kw}?",
        "всё упрётся в {kw} на практике, кмк.",
        "{kw} тут решает больше всего, остальное вторично.",
    ]
    templates_plain = [
        "интересно, а на практике это как работает?",
        "звучит нормально, но что с подводными камнями?",
        "а есть примеры/цифры/кейсы?",
        "ну посмотрим, как оно в жизни пойдёт.",
        "в целом ок, но детали решают.",
    ]

    templates = templates_kw if keyword else templates_plain
    pool = templates.copy()
    rnd.shuffle(pool)

    existing = existing_comments or []
    for t in pool:
        text = (t.format(kw=keyword) if keyword else t).strip()
        if not text:
            continue
        if existing:
            too_sim, _, _ = is_comment_too_similar(text, existing, threshold)
            if too_sim:
                continue
        return text

    return (pool[0].format(kw=keyword) if keyword else pool[0]).strip()


def _normalize_post_text_for_compare(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _extract_message_text(message) -> str:
    if not message:
        return ""
    text = getattr(message, "message", None)
    if text is None:
        text = getattr(message, "text", None)
    if text is None:
        try:
            text = getattr(message, "raw_text", None)
        except Exception:
            text = None
    return str(text or "")


def _message_media_fingerprint(message) -> str:
    if not message:
        return ""
    try:
        photo = getattr(message, "photo", None)
        if photo is not None:
            pid = getattr(photo, "id", None)
            if pid:
                return f"photo:{pid}"
        document = getattr(message, "document", None)
        if document is not None:
            did = getattr(document, "id", None)
            mime = getattr(document, "mime_type", None)
            if not mime:
                msg_file = getattr(message, "file", None)
                mime = getattr(msg_file, "mime_type", None) if msg_file else None
            if did or mime:
                return f"doc:{did}:{mime}"
        msg_file = getattr(message, "file", None)
        if msg_file is not None:
            mime = getattr(msg_file, "mime_type", None)
            size = getattr(msg_file, "size", None)
            if mime or size:
                return f"file:{mime}:{size}"
    except Exception:
        return ""
    return ""


def _message_has_image(message) -> bool:
    if not message:
        return False
    try:
        if getattr(message, "photo", None):
            return True
        msg_file = getattr(message, "file", None)
        mime_type = getattr(msg_file, "mime_type", None) if msg_file else None
        return isinstance(mime_type, str) and mime_type.lower().startswith("image/")
    except Exception:
        return False


async def _download_message_image_bytes(message):
    if not _message_has_image(message):
        return None
    try:
        return await _run_with_soft_timeout(
            message.download_media(file=bytes),
            SEND_ATTEMPT_TIMEOUT_SECONDS,
        )
    except Exception:
        return None


async def _refetch_post_message(client, chat_id: int, msg_id: int):
    if client is None:
        return None
    try:
        entity = await _run_with_soft_timeout(
            client.get_input_entity(int(chat_id)),
            SEND_ATTEMPT_TIMEOUT_SECONDS,
        )
        messages = await _run_with_soft_timeout(
            client.get_messages(entity, ids=[int(msg_id)]),
            SEND_ATTEMPT_TIMEOUT_SECONDS,
        )
        if messages and isinstance(messages, list):
            return messages[0]
    except Exception:
        return None
    return None


async def catch_up_missed_posts(client_wrapper, target_chat):
    global POST_PROCESS_CACHE, handled_grouped_ids, PENDING_TASKS
    task = asyncio.current_task()
    PENDING_TASKS.add(task)
    try:
        chat_id_raw = target_chat.get('linked_chat_id', target_chat.get('chat_id'))
        chat_id = int(str(chat_id_raw).replace('-100', ''))
        destination_chat_id = int(str(chat_id_raw))
        chat_name = target_chat.get('chat_name')

        SCAN_LIMIT = 5
        TIME_LIMIT_SECONDS = 600

        daily_limit = target_chat.get('daily_comment_limit', 999)

        try:
            current_count = get_daily_action_count_from_db(destination_chat_id, 'comment')
        except Exception:
            current_count = 999
        if current_count >= daily_limit:
            return

        try:
            entity = await client_wrapper.client.get_input_entity(chat_id)
        except Exception:
            return

        messages_to_scan = []
        async for message in client_wrapper.client.iter_messages(entity, limit=SCAN_LIMIT):
            messages_to_scan.append(message)

        me = await client_wrapper.client.get_me()
        my_id = me.id
        posts_replied_by_me = set()

        for message in messages_to_scan:
            if message.sender_id == my_id and message.reply_to_msg_id:
                posts_replied_by_me.add(message.reply_to_msg_id)

        for message in messages_to_scan:
            msg_date = message.date
            if msg_date.tzinfo is None:
                msg_date = msg_date.replace(tzinfo=timezone.utc)

            if (datetime.now(timezone.utc) - msg_date).total_seconds() > TIME_LIMIT_SECONDS:
                continue

            is_channel_post = False
            if message.fwd_from and message.fwd_from.channel_post:
                is_channel_post = True
            elif message.post:
                is_channel_post = True

            if not is_channel_post:
                continue

            if message.id in posts_replied_by_me:
                continue

            if message.grouped_id:
                if message.grouped_id in handled_grouped_ids:
                    continue
                handled_grouped_ids.append(message.grouped_id)

            unique_process_id = f"{target_chat.get('chat_id')}_{message.id}"
            if unique_process_id in POST_PROCESS_CACHE:
                continue
            if unique_process_id in PROCESSING_CACHE:
                continue

            logger.info(f"💡 [CATCH-UP] Нашел свежий пропущенный пост {message.id} в {chat_name}")

            try:
                await client_wrapper.client.send_read_acknowledge(entity, message=message)
            except:
                pass

            event_mock = collections.namedtuple('EventMock', ['message', 'chat_id'])
            mock_event = event_mock(message=message, chat_id=destination_chat_id)

            await process_new_post(mock_event, target_chat, from_catch_up=True)
            await asyncio.sleep(random.randint(2, 5))

    except asyncio.CancelledError:
        pass
    finally:
        PENDING_TASKS.discard(task)


async def process_new_post(event, target_chat, from_catch_up=False, is_manual=False):
    global active_clients, PENDING_TASKS, current_settings, SCENARIO_CONTEXT, PROCESSING_CACHE
    task = asyncio.current_task()
    PENDING_TASKS.add(task)
    unique_id = None
    processing_added = False
    any_comment_sent = False
    try:
        channel_id = target_chat.get('chat_id')
        msg_id = event.message.id
        destination_chat_id_for_logs = event.chat_id
        unique_id = f"{channel_id}_{msg_id}"

        if not is_manual and unique_id in POST_PROCESS_CACHE:
            return

        if unique_id in PROCESSING_CACHE:
            return
        PROCESSING_CACHE.add(unique_id)
        processing_added = True

        raw_id = str(channel_id)
        norm_id = raw_id.replace('-100', '')
        ids_to_check = [raw_id, norm_id, f"-100{norm_id}"]

        has_scenario = False

        try:
            with _db_connect() as conn:
                cursor = conn.cursor()
                placeholders = ','.join('?' for _ in ids_to_check)
                query = f"SELECT chat_id, script_content, status FROM scenarios WHERE chat_id IN ({placeholders})"
                cursor.execute(query, ids_to_check)
                row = cursor.fetchone()

                if row:
                    found_chat_id, content, status = row
                    if content and content.strip() and status != 'stopped':
                        if from_catch_up:
                            _mark_post_processed(unique_id)
                            return

                        has_scenario = True
                        conn.execute("""
                            INSERT OR IGNORE INTO post_scenarios (chat_id, post_id, current_index, last_run_time)
                            VALUES (?, ?, 0, ?)
                        """, (found_chat_id, msg_id, time.time()))
                        conn.commit()
                        logger.info(f"🎬 [SCENARIO ADD] Новый пост {msg_id} добавлен в сценарий (Chat: {found_chat_id})")
                    else:
                        pass
        except Exception as e:
            logger.error(f"❌ Ошибка SQL при поиске сценария: {e}")

        if has_scenario:
            SCENARIO_CONTEXT[f"{channel_id}_{msg_id}"] = msg_id
            _mark_post_processed(unique_id)
            return

        chat_id_check = target_chat.get('chat_id')
        actual_ai_enabled = True
        found_fresh_settings = False
        for t in get_project_targets(current_settings):
            if t.get('chat_id') == chat_id_check:
                actual_ai_enabled = t.get('ai_enabled', True)
                found_fresh_settings = True
                break

        if found_fresh_settings and not actual_ai_enabled and not is_manual:
            _mark_post_processed(unique_id)
            return
        if not found_fresh_settings and not target_chat.get('ai_enabled', True) and not is_manual:
            _mark_post_processed(unique_id)
            return

        post_text = str(getattr(event.message, "message", None) or "")
        if not post_text:
            try:
                post_text = str(getattr(event.message, "text", None) or "")
            except Exception:
                post_text = ""

        try:
            min_words = int(target_chat.get("min_word_count", 0) or 0)
        except Exception:
            min_words = 0
        if not is_manual and min_words > 0:
            wc = len(post_text.split())
            if wc < min_words:
                logger.info(f"⏭️ Пост {msg_id} пропущен: слишком короткий ({wc}/{min_words} слов).")
                log_comment_skip_to_db(
                    msg_id,
                    target_chat,
                    destination_chat_id_for_logs,
                    f"слишком короткий ({wc}/{min_words} слов)",
                )
                _mark_post_processed(unique_id)
                return

        try:
            comment_chance = int(target_chat.get("comment_chance", 100) or 0)
        except Exception:
            comment_chance = 100
        comment_chance = max(min(comment_chance, 100), 0)
        if not is_manual and comment_chance < 100 and random.randint(1, 100) > comment_chance:
            logger.info(f"🙈 Пост {msg_id} пропущен: шанс коммента {comment_chance}%.")
            log_comment_skip_to_db(
                msg_id,
                target_chat,
                destination_chat_id_for_logs,
                f"шанс коммента {comment_chance}%",
            )
            _mark_post_processed(unique_id)
            return

        if not is_manual:
            try:
                skip, reason = should_skip_post_for_commenting(event.message, post_text, target_chat)
            except Exception:
                skip, reason = False, ""
            if skip:
                logger.info(f"⏭️ Пост {msg_id} пропущен: {reason}.")
                log_comment_skip_to_db(
                    msg_id,
                    target_chat,
                    destination_chat_id_for_logs,
                    str(reason or ""),
                )
                _mark_post_processed(unique_id)
                return

        accounts_data = load_project_accounts()
        eligible_clients = [
            c
            for c in list(active_clients.values())
            if is_bot_awake(next((a for a in accounts_data if a["session_name"] == c.session_name), {}))
            and _is_account_assigned(target_chat, c.session_name)
        ]

        if not eligible_clients:
            log_comment_skip_to_db(
                msg_id,
                target_chat,
                destination_chat_id_for_logs,
                "нет подходящих аккаунтов (все спят / не назначены / не подключены)",
            )
            _mark_post_processed(unique_id)
            return

        channel_key = normalize_id(target_chat.get("chat_id")) or target_chat.get("chat_id") or event.chat_id

        try:
            min_interval_mins = int(target_chat.get("min_post_interval_mins", 0) or 0)
        except Exception:
            min_interval_mins = 0
        if not is_manual and min_interval_mins > 0:
            msg_date = event.message.date
            if isinstance(msg_date, datetime) and msg_date.tzinfo is None:
                msg_date = msg_date.replace(tzinfo=timezone.utc)

            if channel_key not in CHANNEL_LAST_POST_TIME:
                persisted = _db_get_last_post_time("comment", str(channel_key))
                if persisted:
                    CHANNEL_LAST_POST_TIME[channel_key] = persisted
            last_time = CHANNEL_LAST_POST_TIME.get(channel_key)
            if last_time:
                try:
                    delta_sec = (msg_date - last_time).total_seconds()
                except Exception:
                    delta_sec = None
                if delta_sec is not None and delta_sec < (min_interval_mins * 60):
                    logger.info(
                        f"⏭️ Пост {msg_id} пропущен: мин. интервал {min_interval_mins} мин (прошло {int(delta_sec)} сек)."
                    )
                    log_comment_skip_to_db(
                        msg_id,
                        target_chat,
                        destination_chat_id_for_logs,
                        f"мин. интервал {min_interval_mins} мин (прошло {int(delta_sec)} сек)",
                    )
                    _mark_post_processed(unique_id)
                    return

            msg_date = _dt_to_utc(msg_date)
            CHANNEL_LAST_POST_TIME[channel_key] = msg_date
            _db_set_last_post_time("comment", str(channel_key), msg_date)

        selected_clients, planned_count, already_count, already_accounts = _select_accounts_for_post(
            chat_key=str(channel_key),
            post_id=int(msg_id),
            destination_chat_id=int(destination_chat_id_for_logs),
            target_chat=target_chat,
            eligible_clients=eligible_clients,
        )
        if not selected_clients:
            if planned_count > 0 and already_count >= planned_count:
                reason = f"уже есть комментарии от {already_count}/{planned_count} аккаунтов"
            elif already_accounts:
                reason = f"уже комментировали: {', '.join(sorted(already_accounts))}"
            else:
                reason = "не нужно выбирать аккаунты (planned_count=0)"
            logger.info(f"⏭️ Пост {msg_id} пропущен: {reason}.")
            log_comment_skip_to_db(msg_id, target_chat, destination_chat_id_for_logs, reason)
            _mark_post_processed(unique_id)
            return

        eligible_clients = selected_clients

        logger.info(
            f"👥 Аккаунты для коммента поста {msg_id}: {', '.join([c.session_name for c in eligible_clients])}"
        )

        if is_manual:
            wait_time = random.randint(2, 5)
        else:
            base_delay = target_chat.get('initial_comment_delay', 30)
            wait_time = random.randint(base_delay, base_delay + 30)

        logger.info(f"⏳ Жду {wait_time} сек перед генерацией для поста {msg_id}...")
        await asyncio.sleep(wait_time)

        image_bytes = None
        try:
            if getattr(event.message, "photo", None):
                image_bytes = await event.message.download_media(file=bytes)
            else:
                msg_file = getattr(event.message, "file", None)
                mime_type = getattr(msg_file, "mime_type", None) if msg_file else None
                if isinstance(mime_type, str) and mime_type.lower().startswith("image/"):
                    image_bytes = await event.message.download_media(file=bytes)
        except Exception:
            image_bytes = None

        post_media_fingerprint = _message_media_fingerprint(event.message)
        post_last_refresh_at = 0.0

        async def _refresh_post_content(client_wrapper, *, force: bool = False):
            nonlocal post_text, image_bytes, post_media_fingerprint, post_last_refresh_at

            now_ts = time.time()
            if (not force) and post_last_refresh_at and (now_ts - post_last_refresh_at) < 3.0:
                return False
            post_last_refresh_at = now_ts

            latest_msg = await _refetch_post_message(client_wrapper.client, int(event.chat_id), int(msg_id))
            if latest_msg is None:
                return False

            latest_text = _extract_message_text(latest_msg)
            text_changed = _normalize_post_text_for_compare(latest_text) != _normalize_post_text_for_compare(post_text)

            latest_media_fp = _message_media_fingerprint(latest_msg)
            media_changed = latest_media_fp != (post_media_fingerprint or "")
            if media_changed:
                post_media_fingerprint = latest_media_fp
                image_bytes = await _download_message_image_bytes(latest_msg)

            if text_changed:
                post_text = latest_text

            return bool(text_changed or media_changed)

        destination_chat_id_for_logs = event.chat_id
        daily_limit = int(target_chat.get("daily_comment_limit", 999) or 0)
        delay_between = max(int(target_chat.get("delay_between_accounts", 10) or 0), 0)

        h_set = current_settings.get("humanization", {}) or {}
        try:
            similarity_threshold = float(h_set.get("similarity_threshold", 0.78))
        except Exception:
            similarity_threshold = 0.78
        similarity_threshold = max(min(similarity_threshold, 1.0), 0.0)
        try:
            similarity_retries = int(h_set.get("similarity_max_retries", 1) or 0)
        except Exception:
            similarity_retries = 1
        similarity_retries = max(min(similarity_retries, 3), 0)

        try:
            semantic_diversify = bool(h_set.get("short_post_diversify", True))
        except Exception:
            semantic_diversify = True
        try:
            semantic_min_new_tokens = int(h_set.get("short_post_min_new_tokens", 2) or 0)
        except Exception:
            semantic_min_new_tokens = 2
        semantic_min_new_tokens = max(min(semantic_min_new_tokens, 6), 0)

        angle_pool = []
        if semantic_diversify:
            angle_seed = f"angles:{destination_chat_id_for_logs}:{msg_id}"
            angle_pool = _stable_shuffled(SEMANTIC_DIVERSITY_ANGLES, angle_seed)

        sent_comments: list[str] = []
        use_modes = True
        mode_seed = f"modes:{destination_chat_id_for_logs}:{msg_id}"
        mode_pool = _stable_shuffled(COMMENT_DIVERSITY_MODES, mode_seed)

        for idx, client_wrapper in enumerate(eligible_clients):
            try:
                attempted_send = False
                if daily_limit > 0:
                    current_daily_count = get_daily_action_count_from_db(destination_chat_id_for_logs, "comment")
                    if current_daily_count >= daily_limit:
                        logger.info(
                            f"🧾 Лимит комментариев/сутки достигнут ({current_daily_count}/{daily_limit}) для {destination_chat_id_for_logs}. Останавливаюсь."
                        )
                        break

                if not await ensure_client_connected(client_wrapper, reason="comment"):
                    continue

                media_fp_for_generation = post_media_fingerprint
                try:
                    if await _refresh_post_content(client_wrapper):
                        logger.info(f"✏️ Пост {msg_id} обновлён — беру актуальный текст перед комментированием.")
                        media_fp_for_generation = post_media_fingerprint
                except Exception:
                    pass

                mode_hint = None
                if use_modes and mode_pool:
                    mode_hint = mode_pool[idx % len(mode_pool)]

                angle_hint = None
                if semantic_diversify and angle_pool:
                    angle_hint = angle_pool[idx % len(angle_pool)]

                extra_base = build_comment_diversity_instructions(
                    sent_comments,
                    mode_hint=mode_hint,
                )
                short_extra = ""
                if semantic_diversify:
                    short_extra = build_semantic_diversity_instructions(post_text, angle_hint=angle_hint)
                extra = "\n\n".join([p for p in [extra_base, short_extra] if p]).strip()

                post_text_for_generation = post_text
                generated_text = None
                prompt_info = None
                failure_reason = None
                for attempt in range(similarity_retries + 1):
                    candidate, pinfo = await generate_comment(
                        post_text,
                        target_chat,
                        client_wrapper.session_name,
                        image_bytes=image_bytes,
                        extra_instructions=extra,
                    )
                    if candidate:
                        generated_text, prompt_info = candidate, pinfo
                    else:
                        failure_reason = pinfo or "generation_failed"

                    if not generated_text:
                        break
                    if not sent_comments:
                        break

                    too_similar, score, best = is_comment_too_similar(
                        generated_text, sent_comments, similarity_threshold
                    )
                    needs_novelty = False
                    new_token_count = 0
                    required_new_tokens = 0
                    if semantic_min_new_tokens > 0:
                        required_new_tokens = semantic_min_new_tokens + (1 if len(sent_comments) >= 2 else 0)
                    if semantic_diversify and required_new_tokens > 0:
                        needs_novelty, new_token_count = comment_needs_more_novelty(
                            generated_text,
                            post_text=post_text,
                            existing_comments=sent_comments,
                            min_new_tokens=required_new_tokens,
                        )
                    if (not too_similar) and (not needs_novelty):
                        break

                    if too_similar:
                        failure_reason = f"too_similar(score={score:.2f})"
                        logger.info(
                            f"♻️ [{client_wrapper.session_name}] комментарий слишком похож (score={score:.2f}). Перегенерирую..."
                        )
                    else:
                        failure_reason = f"low_novelty(new_tokens={new_token_count})"
                        logger.info(
                            f"🧩 [{client_wrapper.session_name}] комментарий выглядит как перефраз ({new_token_count} новых слов). Перегенерирую..."
                        )

                    if attempt >= similarity_retries:
                        try:
                            emg = make_emergency_comment(
                                post_text,
                                client_wrapper.session_name,
                                msg_id,
                                existing_comments=sent_comments,
                                threshold=similarity_threshold,
                            )
                        except Exception:
                            emg = ""
                        if emg:
                            generated_text = emg
                            prompt_info = (prompt_info or "comment") + " · EMG"
                            break

                    extra_base = build_comment_diversity_instructions(
                        sent_comments,
                        mode_hint=mode_hint,
                        strict=True,
                        previous_candidate=generated_text,
                    )
                    short_extra = ""
                    if semantic_diversify:
                        short_extra = build_semantic_diversity_instructions(
                            post_text,
                            angle_hint=angle_hint,
                            strict=True,
                            previous_candidate=generated_text,
                        )
                    extra = "\n\n".join([p for p in [extra_base, short_extra] if p]).strip()
                    generated_text = None
                    prompt_info = None

                if not generated_text:
                    reason = failure_reason or prompt_info or "generation_failed"
                    logger.warning(
                        f"⚠️ [{client_wrapper.session_name}] не отправил комментарий к посту {msg_id}: {reason}"
                    )
                    log_action_to_db(
                        {
                            "type": "comment_failed",
                            "post_id": msg_id,
                            "comment": reason,
                            "date": datetime.now(timezone.utc).isoformat(),
                            "account": {"session_name": client_wrapper.session_name},
                            "target": {
                                "chat_name": target_chat.get("chat_name"),
                                "chat_username": target_chat.get("chat_username"),
                                "channel_id": target_chat.get("chat_id"),
                                "destination_chat_id": destination_chat_id_for_logs,
                            },
                        }
                    )
                    continue

                attempted_send = True
                tag_chance = target_chat.get("tag_comment_chance", 50)
                try:
                    tag_chance = int(tag_chance or 0)
                except Exception:
                    tag_chance = 50
                tag_chance = max(min(tag_chance, 100), 0)
                actual_reply_id = msg_id if (is_manual or random.randint(1, 100) <= tag_chance) else None
                thread_top_id = int(msg_id) if actual_reply_id is None else None

                # Re-check the post right before sending: SMM may have edited the text last-minute.
                try:
                    if await _refresh_post_content(client_wrapper, force=True):
                        text_changed = _normalize_post_text_for_compare(post_text) != _normalize_post_text_for_compare(
                            post_text_for_generation
                        )
                        media_changed = post_media_fingerprint != (media_fp_for_generation or "")
                        if text_changed or media_changed:
                            logger.info(f"✏️ Пост {msg_id} изменился прямо перед отправкой — перегенерирую комментарий.")
                            regen_extra_base = build_comment_diversity_instructions(
                                sent_comments,
                                mode_hint=mode_hint,
                                strict=True,
                                previous_candidate=generated_text,
                            )
                            regen_short_extra = ""
                            if semantic_diversify:
                                regen_short_extra = build_semantic_diversity_instructions(
                                    post_text,
                                    angle_hint=angle_hint,
                                    strict=True,
                                    previous_candidate=generated_text,
                                )
                            regen_extra = "\n\n".join([p for p in [regen_extra_base, regen_short_extra] if p]).strip()

                            regen_text, regen_info = await generate_comment(
                                post_text,
                                target_chat,
                                client_wrapper.session_name,
                                image_bytes=image_bytes,
                                extra_instructions=regen_extra,
                            )
                            if regen_text:
                                generated_text = regen_text
                                prompt_info = (regen_info or prompt_info or "comment") + " · UPD"
                except Exception:
                    pass

                await human_type_and_send(
                    client_wrapper.client,
                    event.chat_id,
                    generated_text,
                    reply_to_msg_id=actual_reply_id,
                    thread_top_msg_id=thread_top_id,
                    split_mode="smart_ru_no_comma",
                )
                any_comment_sent = True
                me = await client_wrapper.client.get_me()
                logger.info(f"✅ [{client_wrapper.session_name}] прокомментировал пост {msg_id} ({prompt_info})")
                sent_comments.append(generated_text)

                log_content = f"[{prompt_info}] {generated_text}"
                log_action_to_db(
                    {
                        "type": "comment",
                        "post_id": msg_id,
                        "comment": log_content,
                        "date": datetime.now(timezone.utc).isoformat(),
                        "account": {
                            "session_name": client_wrapper.session_name,
                            "first_name": me.first_name,
                            "username": me.username,
                        },
                        "target": {
                            "chat_name": target_chat.get("chat_name"),
                            "chat_username": target_chat.get("chat_username"),
                            "channel_id": target_chat.get("chat_id"),
                            "destination_chat_id": destination_chat_id_for_logs,
                        },
                    }
                )
                _clear_account_failure(client_wrapper.session_name, "comment")

                if delay_between > 0 and idx != len(eligible_clients) - 1:
                    await asyncio.sleep(delay_between)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"❌ Ошибка комментирования ({client_wrapper.session_name}): {e}")
                if attempted_send:
                    _record_account_failure(
                        client_wrapper.session_name,
                        "comment",
                        last_error=str(e),
                        last_target=str(destination_chat_id_for_logs),
                    )

        if any_comment_sent:
            _mark_post_processed(unique_id)
    except asyncio.CancelledError:
        pass
    finally:
        if processing_added and unique_id:
            PROCESSING_CACHE.discard(unique_id)
        PENDING_TASKS.discard(task)


async def generate_post_evaluation(post_text, target_chat_settings, session_name, image_bytes=None):
    global current_settings

    provider = target_chat_settings.get('ai_provider', 'default')
    if provider == 'default':
        provider = current_settings.get('ai_provider', 'gemini')

    user_goal = target_chat_settings.get('prompt', 'релевантный пост')
    api_key = current_settings.get('api_keys', {}).get(provider)
    if not api_key:
        return False

    try:
        instruction = f"Проверь, соответствует ли пост теме: '{user_goal}'. Ответь только ОДНИМ словом: ДА или НЕТ."
        if provider in {"openai", "openrouter", "deepseek"}:
            base_url = None
            default_headers = None
            if provider == "deepseek":
                base_url = "https://api.deepseek.com"
            elif provider == "openrouter":
                base_url = "https://openrouter.ai/api/v1"
                default_headers = {
                    "HTTP-Referer": os.getenv("OPENROUTER_REFERRER", "http://localhost"),
                    "X-Title": os.getenv("OPENROUTER_TITLE", "AI-Центр"),
                }
            client = openai.AsyncOpenAI(api_key=api_key, base_url=base_url, default_headers=default_headers)

            text_part = f"{instruction}\nТекст: {post_text}"
            if provider == "openai":
                user_content = [{"type": "text", "text": text_part}]
                if image_bytes:
                    mime_type = guess_image_mime_type(image_bytes)
                    base64_image = base64.b64encode(image_bytes).decode('utf-8')
                    user_content.append(
                        {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{base64_image}"}}
                    )
                user_message = {"role": "user", "content": user_content}
            else:
                user_message = {"role": "user", "content": text_part}

            if provider == "deepseek":
                models_to_try = [get_model_setting(current_settings, "deepseek_eval")]
            elif provider == "openrouter":
                models_to_try = [get_model_setting(current_settings, "openrouter_eval")]
            else:
                models_to_try = openai_model_candidates(current_settings, "openai_eval")

            completion = None
            for model_name in models_to_try:
                try:
                    create_kwargs = {
                        "model": model_name,
                        "messages": [user_message],
                        "temperature": 0,
                    }
                    if provider == "openai":
                        create_kwargs["max_completion_tokens"] = 4
                    else:
                        create_kwargs["max_tokens"] = 4
                    completion = await client.chat.completions.create(**create_kwargs)
                    break
                except Exception as e:
                    if provider == "openai" and is_model_unavailable_error(e):
                        continue
                    raise

            return "ДА" in ((completion.choices[0].message.content or "").upper() if completion else "")
        elif provider == 'gemini':
            content = []
            if image_bytes:
                mime_type = guess_image_mime_type(image_bytes)
                content.append(genai_types.Part.from_bytes(data=image_bytes, mime_type=mime_type))
            content.append(f"{instruction}\nТекст: {post_text}")

            for model_name in gemini_model_candidates(current_settings, "gemini_eval"):
                try:
                    async with genai.Client(api_key=api_key).aio as aclient:
                        response = await aclient.models.generate_content(
                            model=model_name,
                            contents=content,
                            config=genai_types.GenerateContentConfig(
                                temperature=0,
                                max_output_tokens=4,
                            ),
                        )
                    return "ДА" in (response.text or "").upper()
                except Exception:
                    continue
    except Exception:
        return False

    return False


def _extract_existing_reaction_emojis(message):
    emojis = []
    if not message:
        return emojis
    reactions = getattr(message, "reactions", None)
    results = getattr(reactions, "results", None) if reactions else None
    if not results:
        return emojis
    for r in results:
        reaction_obj = getattr(r, "reaction", None)
        emoticon = getattr(reaction_obj, "emoticon", None) if reaction_obj else None
        if emoticon:
            emojis.append(emoticon)
    return emojis


def _select_reaction_emojis(desired, existing, count):
    desired = [str(x).strip() for x in (desired or []) if str(x).strip()]
    existing = [str(x).strip() for x in (existing or []) if str(x).strip()]

    pool = []
    if existing:
        intersect = [e for e in desired if e in existing]
        pool = intersect or existing
    else:
        pool = desired

    if not pool:
        return []
    count = max(int(count or 1), 1)
    if count == 1 or len(pool) == 1:
        return [random.choice(pool)]
    return random.sample(pool, min(count, len(pool)))


async def process_new_post_for_reaction(source_channel_peer, original_post_id, reaction_target, message=None):
    global active_clients, PENDING_TASKS
    task = asyncio.current_task()
    PENDING_TASKS.add(task)
    try:
        chance = reaction_target.get('reaction_chance', 80)
        if random.randint(1, 100) > chance:
            return
        destination_chat_id_for_logs = int(reaction_target.get('chat_id', reaction_target.get('linked_chat_id')))
        daily_limit = reaction_target.get('daily_reaction_limit', 999)
        current_daily_count = get_daily_action_count_from_db(destination_chat_id_for_logs, 'reaction')
        if current_daily_count >= daily_limit:
            return
        accounts_data = load_project_accounts()
        eligible_clients = []
        for c in list(active_clients.values()):
            acc_data = next((a for a in accounts_data if a['session_name'] == c.session_name), None)
            if acc_data and is_bot_awake(acc_data) and _is_account_assigned(reaction_target, c.session_name):
                eligible_clients.append(c)
        if not eligible_clients:
            return
        random.shuffle(eligible_clients)
        initial_delay = max(reaction_target.get('initial_reaction_delay', 10), 0)
        if initial_delay > 0:
            await asyncio.sleep(initial_delay)
        delay_between = max(reaction_target.get('delay_between_reactions', 5), 0)
        desired_reactions = reaction_target.get("reactions", []) or []
        existing_reactions = _extract_existing_reaction_emojis(message)
        for client_wrapper in eligible_clients:
            try:
                attempted_send = False
                current_daily_count = get_daily_action_count_from_db(destination_chat_id_for_logs, 'reaction')
                if current_daily_count >= daily_limit:
                    break
                if not desired_reactions and not existing_reactions:
                    continue

                if not await ensure_client_connected(client_wrapper, reason="reaction"):
                    continue

                num_to_set = reaction_target.get("reaction_count", 1)
                reactions_to_set_str = _select_reaction_emojis(desired_reactions, existing_reactions, num_to_set)
                if not reactions_to_set_str:
                    continue

                tl_reactions = [ReactionEmoji(emoticon=r) for r in reactions_to_set_str]
                actual_peer = None
                try:
                    actual_peer = await client_wrapper.client.get_input_entity(destination_chat_id_for_logs)
                except Exception:
                    actual_peer = source_channel_peer
                try:
                    await client_wrapper.client.send_read_acknowledge(actual_peer, message=original_post_id)
                except Exception:
                    pass
                await asyncio.sleep(random.uniform(1, 3))
                try:
                    attempted_send = True
                    await client_wrapper.client(
                        SendReactionRequest(peer=actual_peer, msg_id=original_post_id, reaction=tl_reactions)
                    )
                except ReactionsTooManyError:
                    if not existing_reactions:
                        try:
                            msg = await client_wrapper.client.get_messages(actual_peer, ids=original_post_id)
                            msg = msg[0] if isinstance(msg, list) else msg
                            existing_reactions = _extract_existing_reaction_emojis(msg)
                        except Exception:
                            existing_reactions = []

                    fallback = _select_reaction_emojis(desired_reactions, existing_reactions, 1)
                    if not fallback:
                        raise
                    reactions_to_set_str = fallback
                    tl_reactions = [ReactionEmoji(emoticon=r) for r in reactions_to_set_str]
                    attempted_send = True
                    await client_wrapper.client(
                        SendReactionRequest(peer=actual_peer, msg_id=original_post_id, reaction=tl_reactions)
                    )

                me = await client_wrapper.client.get_me()
                log_action_to_db({
                    'type': 'reaction', 'post_id': original_post_id, 'reactions': reactions_to_set_str,
                    'date': datetime.now(timezone.utc).isoformat(),
                    'account': {'session_name': client_wrapper.session_name, 'first_name': me.first_name,
                                'username': me.username},
                    'target': {'chat_name': reaction_target.get('chat_name'),
                               'chat_username': reaction_target.get('chat_username'),
                               'channel_id': reaction_target.get('chat_id'),
                               'destination_chat_id': destination_chat_id_for_logs}
                })
                _clear_account_failure(client_wrapper.session_name, "reaction")
                if delay_between > 0 and client_wrapper != eligible_clients[-1]:
                    await asyncio.sleep(delay_between)
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + 2)
            except Exception as e:
                logger.error(f"❌ Ошибка отправки реакции ({client_wrapper.session_name}): {e}")
                if attempted_send:
                    _record_account_failure(
                        client_wrapper.session_name,
                        "reaction",
                        last_error=str(e),
                        last_target=str(destination_chat_id_for_logs),
                    )
    except asyncio.CancelledError:
        pass
    finally:
        PENDING_TASKS.discard(task)


async def process_post_for_monitoring(event, monitor_target):
    global active_clients, MONITOR_CHANNEL_LAST_POST_TIME, PENDING_TASKS
    task = asyncio.current_task()
    PENDING_TASKS.add(task)
    try:
        channel_id = event.chat_id
        channel_key = normalize_id(channel_id) or channel_id
        post_content = event.message.text or ""
        post_id = event.message.id
        msg_date = _dt_to_utc(event.message.date)
        min_interval = monitor_target.get('min_post_interval_mins', 0)
        if min_interval > 0:
            if channel_key not in MONITOR_CHANNEL_LAST_POST_TIME:
                persisted = _db_get_last_post_time("monitor", str(channel_key))
                if persisted:
                    MONITOR_CHANNEL_LAST_POST_TIME[channel_key] = persisted
            last_post_time = MONITOR_CHANNEL_LAST_POST_TIME.get(channel_key)
            if last_post_time and (msg_date - last_post_time).total_seconds() < min_interval * 60:
                return
            MONITOR_CHANNEL_LAST_POST_TIME[channel_key] = msg_date
            _db_set_last_post_time("monitor", str(channel_key), msg_date)
        if len(post_content.split()) < monitor_target.get('min_word_count', 0):
            return
        daily_limit = monitor_target.get('daily_limit', 999)
        if daily_limit > 0 and get_daily_action_count_from_db(channel_id, 'monitoring') >= daily_limit:
            return
        eligible_clients = [
            c for c in list(active_clients.values()) if _is_account_assigned(monitor_target, c.session_name)
        ]
        if not eligible_clients:
            return
        client_wrapper = random.choice(eligible_clients)
        image_bytes = None
        if event.message.photo:
            image_bytes = await event.message.download_media(file=bytes)
        is_relevant = await generate_post_evaluation(post_content, monitor_target, client_wrapper.session_name, image_bytes)
        if is_relevant:
            notification_chat_id = monitor_target['notification_chat_id']
            try:
                await client_wrapper.client.forward_messages(notification_chat_id, event.message)
            except Exception:
                channel_username = monitor_target.get('chat_username')
                channel_id_str = str(monitor_target.get('chat_id', '')).replace('-100', '')
                post_link = f"https://t.me/{channel_username}/{post_id}" if channel_username else f"https://t.me/c/{channel_id_str}/{post_id}"
                message_text = f"❗️ <b>Найден пост</b>\n\n<b>Канал:</b> {monitor_target.get('chat_name', 'N/A')}\n<b>Ссылка:</b> {post_link}"
                await client_wrapper.client.send_message(notification_chat_id, message_text, parse_mode='html', link_preview=False)
            me = await client_wrapper.client.get_me()
            log_action_to_db({
                'type': 'monitoring', 'post_id': post_id, 'date': datetime.now(timezone.utc).isoformat(),
                'account': {'session_name': client_wrapper.session_name, 'first_name': me.first_name, 'username': me.username},
                'target': {'chat_name': monitor_target.get('chat_name'), 'channel_id': channel_id, 'destination_chat_id': channel_id},
                'comment': f"Found post, notified {notification_chat_id}"
            })
    except asyncio.CancelledError:
        pass
    finally:
        PENDING_TASKS.discard(task)


async def process_trigger(event, found_target, our_ids):
    global active_clients, REPLY_PROCESS_CACHE

    msg_id = event.message.id
    if msg_id in REPLY_PROCESS_CACHE:
        return

    post_text = (event.message.text or "").lower()
    if not post_text:
        return

    answer_text = None
    try:
        chat_id_target = str(found_target.get('chat_id'))

        with _db_connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT trigger_phrase, answer_text FROM triggers WHERE chat_id = ?",
                (chat_id_target,)
            )
            rows = cursor.fetchall()

            for phrase, answer in rows:
                clean_phrase = phrase.strip().lower()
                if clean_phrase and clean_phrase in post_text:
                    answer_text = answer
                    break
    except Exception as e:
        logger.error(f"Ошибка поиска триггера: {e}")
        return

    if answer_text:
        REPLY_PROCESS_CACHE.add(msg_id)

        accounts_data = load_project_accounts(current_settings)
        eligible = []

        for c in list(active_clients.values()):
            acc_conf = next((a for a in accounts_data if a['session_name'] == c.session_name), None)
            if acc_conf and is_bot_awake(acc_conf):
                if _is_account_assigned(found_target, c.session_name):
                    eligible.append(c)

        if not eligible:
            REPLY_PROCESS_CACHE.discard(msg_id)
            return

        client_wrapper = random.choice(eligible)

        await asyncio.sleep(random.uniform(3, 7))

        try:
            attempted_send = True
            await human_type_and_send(client_wrapper.client, event.chat_id, answer_text, reply_to_msg_id=msg_id)
            logger.info(f"⚡ [{client_wrapper.session_name}] ответил по триггеру на сообщение {msg_id}")
            _clear_account_failure(client_wrapper.session_name, "reply")
        except Exception as e:
            logger.error(f"Ошибка отправки триггера: {e}")
            _record_account_failure(
                client_wrapper.session_name,
                "reply",
                last_error=str(e),
                last_target=str(event.chat_id),
            )
            REPLY_PROCESS_CACHE.discard(msg_id)


class CommentatorClient:
    def __init__(self, account_data, api_id, api_hash):
        self.session_name = account_data['session_name']
        self.session_string = account_data.get('session_string')
        self.session_file = account_data.get("session_file") or account_data.get("session_path")
        self.api_id, self.api_hash = _resolve_account_credentials(account_data, api_id, api_hash)
        self.proxy = _resolve_account_proxy(account_data)
        self.client = None
        self._init_error = None

        if not self.api_id or not self.api_hash:
            self._init_error = "missing_api_credentials"
            return

        session = _resolve_account_session(account_data)
        if not session:
            self._init_error = "missing_session"
            return

        try:
            self.client = TelegramClient(
                session,
                self.api_id,
                self.api_hash,
                proxy=self.proxy,
                **device_kwargs(account_data),
            )
        except Exception as e:
            self._init_error = f"init_error:{e}"

    def _parse_proxy(self, url):
        try:
            protocol, rest = url.split('://')
            auth, addr = rest.split('@')
            user, password = auth.split(':')
            host, port = addr.split(':')
            return (protocol, host, int(port), True, user, password)
        except Exception:
            return None

    async def start(self):
        try:
            if not self.client:
                if self._init_error:
                    logger.error(f"Ошибка инициализации {self.session_name}: {self._init_error}")
                return False
            await self.client.connect()
            if not await self.client.is_user_authorized():
                self._init_error = "unauthorized"
                return False
            me = await self.client.get_me()
            self.user_id = me.id
            self.client.add_event_handler(self.event_handler, events.NewMessage)
            self.client.add_event_handler(
                self.reaction_event_handler,
                events.Raw(types=(tl_types.UpdateMessageReactions, tl_types.UpdateBotMessageReaction, tl_types.UpdateBotMessageReactions)),
            )
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения {self.session_name}: {e}")
            return False

    async def stop(self):
        if self.client.is_connected():
            await self.client.disconnect()

    async def reaction_event_handler(self, update):
        try:
            peer = getattr(update, "peer", None)
            msg_id = int(getattr(update, "msg_id", 0) or 0)
            chat_id = _peer_chat_id(peer)
            if not chat_id or msg_id <= 0:
                return

            summary = _reaction_summary_from_update(update)
            entity = await self.client.get_entity(peer)
            msg = await self.client.get_messages(entity, ids=msg_id)
            if isinstance(msg, list):
                msg = msg[0] if msg else None
            if not msg:
                return

            if getattr(msg, "sender_id", None) != getattr(self, "user_id", None):
                return

            kind = "dm" if isinstance(peer, tl_types.PeerUser) else "quote"
            chat_username = getattr(entity, "username", None)
            if isinstance(peer, tl_types.PeerUser):
                first_name = getattr(entity, "first_name", "") or ""
                last_name = getattr(entity, "last_name", "") or ""
                chat_title = (f"{first_name} {last_name}").strip() or chat_username or chat_id
            else:
                chat_title = getattr(entity, "title", None) or chat_username or chat_id

            _store_message_reaction_event(
                session_name=self.session_name,
                chat_id=str(chat_id),
                msg_id=msg_id,
                kind=kind,
                text=_message_text_preview(msg),
                chat_title=chat_title,
                chat_username=chat_username,
                reactions_summary=summary,
            )
        except Exception:
            return

    async def event_handler(self, event):
        async with EVENT_HANDLER_LOCK:
            global current_settings, handled_posts_for_comments, handled_posts_for_reactions, handled_posts_for_monitoring, handled_grouped_ids, REPLY_PROCESS_CACHE

            try:
                event_chat_id = int(str(event.chat_id).replace('-100', ''))
            except:
                event_chat_id = event.chat_id

            msg_id = event.message.id
            sender_id = event.sender_id

            is_channel_post = event.message.post or (event.message.fwd_from and event.message.fwd_from.channel_post)

            # Inbox: private DMs + replies to our messages in groups/chats ("цитирование").
            try:
                our_ids = get_all_our_user_ids()
            except Exception:
                our_ids = set()

            if event.out:
                try:
                    text = event.message.text or ""
                    if not text:
                        try:
                            if getattr(event.message, "photo", None):
                                text = "[фото]"
                            elif getattr(event.message, "video", None) or getattr(event.message, "gif", None):
                                text = "[видео]"
                            elif getattr(event.message, "voice", None):
                                text = "[голосовое]"
                            elif getattr(event.message, "audio", None):
                                text = "[аудио]"
                            elif getattr(event.message, "document", None) or getattr(event.message, "file", None):
                                text = "[файл]"
                        except Exception:
                            pass

                    if event.is_private:
                        if _queued_outgoing_exists(
                            kind="dm",
                            session_name=self.session_name,
                            chat_id=str(event.chat_id),
                            text=text,
                            reply_to_msg_id=getattr(event.message, "reply_to_msg_id", None),
                        ):
                            return

                        chat = None
                        try:
                            chat = await event.get_chat()
                        except Exception:
                            chat = None
                        chat_username = getattr(chat, "username", "") if chat else ""
                        chat_title = ""
                        if chat:
                            fn = getattr(chat, "first_name", "") or ""
                            ln = getattr(chat, "last_name", "") or ""
                            chat_title = (f"{fn} {ln}").strip() or (chat_username or "")

                        log_inbox_message_to_db(
                            kind="dm",
                            direction="out",
                            status="sent",
                            session_name=self.session_name,
                            chat_id=str(event.chat_id),
                            msg_id=msg_id,
                            reply_to_msg_id=getattr(event.message, "reply_to_msg_id", None),
                            sender_id=getattr(self, "user_id", None),
                            chat_title=chat_title or None,
                            chat_username=chat_username or None,
                            text=text,
                            is_read=1,
                        )

                    elif event.is_reply:
                        reply_id = getattr(event.message, "reply_to_msg_id", None)
                        if reply_id:
                            with _db_connect() as conn:
                                found = conn.execute(
                                    """
                                    SELECT 1 FROM inbox_messages
                                    WHERE kind='quote' AND direction='in'
                                      AND session_name=? AND chat_id=? AND msg_id=?
                                    LIMIT 1
                                    """,
                                    (self.session_name, str(event.chat_id), reply_id),
                                ).fetchone()
                            if found:
                                if _queued_outgoing_exists(
                                    kind="quote",
                                    session_name=self.session_name,
                                    chat_id=str(event.chat_id),
                                    text=text,
                                    reply_to_msg_id=reply_id,
                                ):
                                    return

                                chat = None
                                try:
                                    chat = await event.get_chat()
                                except Exception:
                                    chat = None
                                chat_title = getattr(chat, "title", "") if chat else ""
                                chat_username = getattr(chat, "username", "") if chat else ""

                                reply_msg = None
                                try:
                                    reply_msg = await event.get_reply_message()
                                except Exception:
                                    reply_msg = None

                                replied_to_text = ""
                                if reply_msg:
                                    replied_to_text = (
                                        getattr(reply_msg, "text", None)
                                        or getattr(reply_msg, "message", None)
                                        or ""
                                    )

                                log_inbox_message_to_db(
                                    kind="quote",
                                    direction="out",
                                    status="sent",
                                    session_name=self.session_name,
                                    chat_id=str(event.chat_id),
                                    msg_id=msg_id,
                                    reply_to_msg_id=reply_id,
                                    sender_id=getattr(self, "user_id", None),
                                    chat_title=(chat_title or "").strip() or None,
                                    chat_username=(chat_username or "").strip() or None,
                                    text=text,
                                    replied_to_text=replied_to_text or None,
                                    is_read=1,
                                )
                except Exception:
                    pass
                return

            if sender_id and sender_id in our_ids:
                our_ids = our_ids  # keep for later logic
            try:
                if event.is_private and sender_id and sender_id not in our_ids:
                    sender = None
                    try:
                        sender = await event.get_sender()
                    except Exception:
                        sender = None
                    sender_username = getattr(sender, "username", "") if sender else ""
                    sender_name = ""
                    if sender:
                        fn = getattr(sender, "first_name", "") or ""
                        ln = getattr(sender, "last_name", "") or ""
                        sender_name = (f"{fn} {ln}").strip() or (sender_username or "")
                    if not sender_name and sender_id:
                        sender_name = str(sender_id)

                    text = event.message.text or ""
                    if not text:
                        try:
                            if getattr(event.message, "photo", None):
                                text = "[фото]"
                            elif getattr(event.message, "video", None) or getattr(event.message, "gif", None):
                                text = "[видео]"
                            elif getattr(event.message, "voice", None):
                                text = "[голосовое]"
                            elif getattr(event.message, "audio", None):
                                text = "[аудио]"
                            elif getattr(event.message, "document", None) or getattr(event.message, "file", None):
                                text = "[файл]"
                        except Exception:
                            pass

                    log_inbox_message_to_db(
                        kind="dm",
                        direction="in",
                        status="received",
                        session_name=self.session_name,
                        chat_id=str(event.chat_id),
                        msg_id=msg_id,
                        sender_id=sender_id,
                        sender_username=sender_username,
                        sender_name=sender_name,
                        chat_title=sender_name,
                        chat_username=sender_username,
                        text=text,
                        is_read=0,
                    )

                if event.is_reply and sender_id and sender_id not in our_ids and not event.is_private:
                    reply_msg = None
                    try:
                        reply_msg = await event.get_reply_message()
                    except Exception:
                        reply_msg = None

                    if reply_msg and getattr(reply_msg, "sender_id", None) == getattr(self, "user_id", None):
                        sender = None
                        try:
                            sender = await event.get_sender()
                        except Exception:
                            sender = None
                        sender_username = getattr(sender, "username", "") if sender else ""
                        sender_name = ""
                        if sender:
                            fn = getattr(sender, "first_name", "") or ""
                            ln = getattr(sender, "last_name", "") or ""
                            sender_name = (f"{fn} {ln}").strip() or (sender_username or "")
                        if not sender_name and sender_id:
                            sender_name = str(sender_id)

                        chat = None
                        try:
                            chat = await event.get_chat()
                        except Exception:
                            chat = None
                        chat_title = getattr(chat, "title", "") if chat else ""
                        chat_username = getattr(chat, "username", "") if chat else ""

                        text = event.message.text or ""
                        if not text:
                            try:
                                if getattr(event.message, "photo", None):
                                    text = "[фото]"
                                elif getattr(event.message, "video", None) or getattr(event.message, "gif", None):
                                    text = "[видео]"
                                elif getattr(event.message, "voice", None):
                                    text = "[голосовое]"
                                elif getattr(event.message, "audio", None):
                                    text = "[аудио]"
                                elif getattr(event.message, "document", None) or getattr(event.message, "file", None):
                                    text = "[файл]"
                            except Exception:
                                pass

                        replied_to_text = getattr(reply_msg, "text", None) or getattr(reply_msg, "message", None) or ""
                        log_inbox_message_to_db(
                            kind="quote",
                            direction="in",
                            status="received",
                            session_name=self.session_name,
                            chat_id=str(event.chat_id),
                            msg_id=msg_id,
                            reply_to_msg_id=getattr(event.message, "reply_to_msg_id", None),
                            sender_id=sender_id,
                            sender_username=sender_username,
                            sender_name=sender_name,
                            chat_title=chat_title,
                            chat_username=chat_username,
                            text=text,
                            replied_to_text=replied_to_text,
                            is_read=0,
                        )
            except Exception:
                pass

            found_target = None
            for t in get_project_targets(current_settings):
                t_linked = int(str(t.get('linked_chat_id', 0)).replace('-100', ''))
                t_main = int(str(t.get('chat_id', 0)).replace('-100', ''))
                if event_chat_id == t_linked or event_chat_id == t_main:
                    found_target = t
                    break

            discussion_targets = []
            for t in get_project_discussion_targets(current_settings):
                try:
                    t_linked = int(str(t.get("linked_chat_id", 0)).replace("-100", ""))
                    t_main = int(str(t.get("chat_id", 0)).replace("-100", ""))
                except Exception:
                    continue
                if event_chat_id == t_linked or event_chat_id == t_main:
                    discussion_targets.append(t)

            if discussion_targets and (not event.message.fwd_from) and event.is_group:
                # Prevent double-start when we auto-send the operator seed from the start-queue.
                if event.out and event_chat_id in DISCUSSION_START_SUPPRESS_CHAT_IDS:
                    discussion_targets = []

                if discussion_targets and event.out:
                    msg_text = getattr(event.message, "text", None) or ""
                    raw = str(msg_text or "").strip()
                    matches: list[dict] = []
                    for t in discussion_targets:
                        if not bool(t.get("enabled", True)):
                            continue
                        operator_session = str(t.get("operator_session_name") or "").strip()
                        if not operator_session or operator_session != self.session_name:
                            continue

                        start_prefix = str(t.get("start_prefix") or "")
                        start_on_operator_message = bool(t.get("start_on_operator_message", False))

                        seed: str | None = None
                        if start_on_operator_message:
                            if event.is_reply:
                                seed = _extract_discussion_seed(raw, start_prefix) if start_prefix else None
                            else:
                                seed = _extract_discussion_seed_optional_prefix(raw, start_prefix)
                        else:
                            seed = _extract_discussion_seed(raw, start_prefix)
                        if not seed:
                            continue

                        explicit_prefix = bool(start_prefix and raw.startswith(start_prefix))
                        matches.append(
                            {
                                "target": t,
                                "seed": seed,
                                "start_prefix": start_prefix,
                                "explicit_prefix": explicit_prefix,
                            }
                        )

                    chosen = None
                    if matches:
                        explicit = [m for m in matches if m.get("explicit_prefix")]
                        if explicit:
                            explicit.sort(key=lambda m: len(str(m.get("start_prefix") or "")), reverse=True)
                            best_len = len(str(explicit[0].get("start_prefix") or ""))
                            tied = [m for m in explicit if len(str(m.get("start_prefix") or "")) == best_len]
                            if len(tied) == 1:
                                chosen = tied[0]
                            else:
                                ids = [
                                    str(m.get("target", {}).get("id") or m.get("target", {}).get("chat_id"))
                                    for m in tied
                                ]
                                logger.warning(
                                    f"⚠️ [discussion] неоднозначный старт по префиксу в чате {event_chat_id}: {ids}"
                                )
                        else:
                            # Start-without-prefix is allowed only when the choice is unambiguous.
                            if len(matches) == 1:
                                chosen = matches[0]
                            else:
                                ids = [
                                    str(m.get("target", {}).get("id") or m.get("target", {}).get("chat_id"))
                                    for m in matches
                                ]
                                logger.warning(
                                    f"⚠️ [discussion] неоднозначный старт без префикса в чате {event_chat_id}: {ids}"
                                )

                    if chosen:
                        _schedule_discussion_run(
                            chat_bare_id=event_chat_id,
                            chat_id=event.chat_id,
                            seed_msg_id=msg_id,
                            seed_text=str(chosen.get("seed") or "").strip(),
                            target=chosen.get("target") or {},
                        )

            if found_target:
                our_ids = get_all_our_user_ids()

                if sender_id and sender_id not in our_ids:
                    asyncio.create_task(process_trigger(event, found_target, our_ids))

                    if (event.is_reply or event.is_private) and msg_id not in REPLY_PROCESS_CACHE:
                        is_reply_to_us = False
                        if event.is_reply:
                            reply_msg = await event.get_reply_message()
                            if reply_msg and reply_msg.sender_id == self.user_id:
                                is_reply_to_us = True
                        elif event.is_private:
                            is_reply_to_us = True

                        if is_reply_to_us:
                            REPLY_PROCESS_CACHE.add(msg_id)

                if is_channel_post:
                    if event.message.grouped_id:
                        if event.message.grouped_id in handled_grouped_ids: return
                        handled_grouped_ids.append(event.message.grouped_id)

                    unique_id = f"{event_chat_id}_{msg_id}"

                    t_linked_check = int(str(found_target.get('linked_chat_id', 0)).replace('-100', ''))
                    t_main_check = int(str(found_target.get('chat_id', 0)).replace('-100', ''))

                    if not (t_linked_check and t_linked_check != t_main_check and event_chat_id == t_main_check):
                        if unique_id not in handled_posts_for_comments:
                            handled_posts_for_comments.append(unique_id)
                            asyncio.create_task(process_new_post(event, found_target))


            if is_channel_post:
                unique_id = f"{event_chat_id}_{msg_id}"
                for r_target in get_project_reaction_targets(current_settings):
                    try:
                        if event_chat_id != int(str(r_target.get("chat_id", 0)).replace("-100", "")):
                            continue
                    except Exception:
                        continue
                    if unique_id not in handled_posts_for_reactions:
                        handled_posts_for_reactions.append(unique_id)
                        asyncio.create_task(
                            process_new_post_for_reaction(event.input_chat, msg_id, r_target, message=event.message)
                        )

            if not event.message.fwd_from and event.is_group and found_target:
                if found_target.get('ai_enabled', True) and found_target.get('reply_chance', 0) > 0:
                    asyncio.create_task(process_reply_to_comment(event, found_target))

            if event.is_channel and not event.message.fwd_from:
                for m_t in get_project_monitor_targets(current_settings):
                    if int(str(m_t.get('chat_id', 0)).replace('-100', '')) == event_chat_id:
                        if event.message.grouped_id and event.message.grouped_id in handled_grouped_ids: return
                        if event.message.grouped_id: handled_grouped_ids.append(event.message.grouped_id)

                        unique_mon_id = f"mon_{event_chat_id}_{msg_id}"
                        if unique_mon_id not in handled_posts_for_monitoring:
                            handled_posts_for_monitoring.append(unique_mon_id)
                            asyncio.create_task(process_post_for_monitoring(event, m_t))


async def ensure_account_joined(client_wrapper, target_config, *, force: bool = False):
    global JOINED_CACHE

    targets_to_join = set()

    main_chat_id = target_config.get('chat_id')
    if main_chat_id:
        targets_to_join.add(str(main_chat_id))

    linked_chat_id = target_config.get('linked_chat_id')
    if linked_chat_id and str(linked_chat_id) != str(main_chat_id):
        targets_to_join.add(str(linked_chat_id))

    username = target_config.get('chat_username')
    invite_link = target_config.get('invite_link')
    chat_id = str(target_config.get('chat_id') or '')
    linked_id = str(target_config.get('linked_chat_id') or '')

    all_success = True

    for target_id in targets_to_join:
        cache_key = (client_wrapper.session_name, target_id)

        if cache_key in JOINED_CACHE:
            continue

        row = _get_join_status(client_wrapper.session_name, target_id)
        now = time.time()
        if row:
            if row.get("status") == "joined":
                JOINED_CACHE.add(cache_key)
                continue
            next_retry = row.get("next_retry_at")
            retry_count = int(row.get("retry_count") or 0)
            if retry_count >= JOIN_MAX_RETRIES and not force:
                continue
            if (not force) and next_retry and now < float(next_retry):
                continue
        else:
            try:
                slow_join_mins = int(target_config.get("slow_join_interval_mins", 0) or 0)
            except Exception:
                slow_join_mins = 0
            if slow_join_mins > 0 and not force:
                scheduled_ts = _compute_slow_join_next_retry_at(str(target_id), slow_join_mins)
                if scheduled_ts is None:
                    scheduled_ts = now
                _upsert_join_status(
                    client_wrapper.session_name,
                    str(target_id),
                    "scheduled",
                    last_error=None,
                    last_method="slow_join",
                    retry_count=0,
                    next_retry_at=float(scheduled_ts),
                )
                try:
                    delay_sec = max(float(scheduled_ts) - now, 0.0)
                    if delay_sec >= 1.0:
                        logger.info(
                            f"[{client_wrapper.session_name}] Медленное вступление: {target_id} через ~{int(delay_sec)} сек (интервал={slow_join_mins} мин)"
                        )
                except Exception:
                    pass
                all_success = False
                continue

        joined = False
        last_error = None
        last_method = None

        if invite_link and not joined:
            try:
                if "t.me/+" in invite_link or "joinchat" in invite_link or "/" not in invite_link:
                    hash_arg = invite_link.split('/')[-1].replace('+', '')
                    await client_wrapper.client(ImportChatInviteRequest(hash_arg))
                    logger.info(f"[{client_wrapper.session_name}] Вступил по инвайту в {target_id}")
                    joined = True
            except UserAlreadyParticipantError:
                joined = True
            except Exception as e:
                last_error = e
                last_method = "invite"
                logger.warning(
                    f"[{client_wrapper.session_name}] Не удалось вступить по инвайту в {target_id}: {type(e).__name__}: {e}"
                )

        access_hash = None
        if str(target_id) == chat_id:
            access_hash = target_config.get("chat_access_hash")
        elif str(target_id) == linked_id:
            access_hash = target_config.get("linked_chat_access_hash")

        if access_hash and not joined:
            try:
                channel_id = _channel_bare_id(target_id) or _channel_bare_id(chat_id) or _channel_bare_id(linked_id)
                if channel_id is None:
                    raise ValueError("invalid_channel_id")
                peer = InputPeerChannel(channel_id, int(access_hash))
                await client_wrapper.client(JoinChannelRequest(peer))
                logger.info(f"[{client_wrapper.session_name}] Вступил по access_hash в {target_id}")
                joined = True
            except UserAlreadyParticipantError:
                joined = True
            except Exception as e:
                last_error = e
                last_method = "access_hash"
                logger.warning(
                    f"[{client_wrapper.session_name}] Не удалось вступить по access_hash в {target_id}: {type(e).__name__}: {e}"
                )

        if username and not joined and str(target_id) == chat_id:
            try:
                await client_wrapper.client(JoinChannelRequest(username))
                joined = True

            except UserAlreadyParticipantError:
                joined = True
            except Exception as e:
                last_error = e
                last_method = "username"
                logger.warning(
                    f"[{client_wrapper.session_name}] Не удалось вступить по username в {target_id}: {type(e).__name__}: {e}"
                )

        if username and not joined and str(target_id) == linked_id:
            try:
                entity = await client_wrapper.client.get_entity(username)
                full = await client_wrapper.client(GetFullChannelRequest(entity))
                if full.full_chat.linked_chat_id:
                    linked_entity = await client_wrapper.client.get_input_entity(full.full_chat.linked_chat_id)
                    await client_wrapper.client(JoinChannelRequest(linked_entity))
                    logger.info(f"[{client_wrapper.session_name}] Довступил в привязанный чат через канал")
                    joined = True
            except UserAlreadyParticipantError:
                joined = True
            except Exception as e:
                last_error = e
                last_method = "linked"
                logger.warning(
                    f"[{client_wrapper.session_name}] Не удалось довступить в привязанный чат {linked_chat_id}: {type(e).__name__}: {e}"
                )

        if not joined:
            try:
                entity = await client_wrapper.client.get_input_entity(int(target_id))
                await client_wrapper.client(JoinChannelRequest(entity))
                logger.info(f"[{client_wrapper.session_name}] Вступил по ID в {target_id}")
                joined = True
            except UserAlreadyParticipantError:
                joined = True
            except Exception as e:
                last_error = e
                last_method = "id"
                logger.warning(
                    f"[{client_wrapper.session_name}] Не удалось вступить по ID в {target_id}: {type(e).__name__}: {e}"
                )

        if joined:
            JOINED_CACHE.add(cache_key)
            _upsert_join_status(
                client_wrapper.session_name,
                target_id,
                "joined",
                last_error=None,
                last_method=None,
                retry_count=0,
                next_retry_at=None,
            )
        else:
            all_success = False
            row_retry = _get_join_status(client_wrapper.session_name, target_id)
            retry_count = int((row_retry or {}).get("retry_count") or 0) + 1
            if retry_count >= JOIN_MAX_RETRIES:
                next_retry = None
            else:
                backoff = JOIN_RETRY_BACKOFF[min(retry_count, len(JOIN_RETRY_BACKOFF) - 1)]
                next_retry = now + backoff
            _upsert_join_status(
                client_wrapper.session_name,
                target_id,
                "failed",
                last_error=str(last_error) if last_error else "unknown_error",
                last_method=last_method,
                retry_count=retry_count,
                next_retry_at=next_retry,
            )
            _record_account_failure(
                client_wrapper.session_name,
                "join",
                last_error=str(last_error) if last_error else None,
                last_target=str(target_id),
            )

    if all_success:
        _clear_account_failure(client_wrapper.session_name, "join")

    return all_success


async def manage_clients(api_id, api_hash):
    global active_clients, current_settings, CLIENT_CATCH_UP_STATUS

    current_settings = load_json_data(SETTINGS_FILE)
    if not isinstance(current_settings, dict):
        current_settings = {}
    ensure_role_schema(current_settings)
    accounts_from_file = load_project_accounts(current_settings)

    file_session_names = {acc['session_name'] for acc in accounts_from_file if _is_account_active(acc)}
    for session_name in list(active_clients.keys()):
        acc_data = next((a for a in accounts_from_file if a['session_name'] == session_name), None)
        if session_name not in file_session_names or not acc_data:
            client_to_stop = active_clients.pop(session_name)
            await client_to_stop.stop()
            keys_to_remove = [k for k in CLIENT_CATCH_UP_STATUS if k.startswith(f"{session_name}_")]
            for k in keys_to_remove:
                CLIENT_CATCH_UP_STATUS.discard(k)
            CLIENT_CONNECT_STATE.pop(session_name, None)
            logger.info(f"Клиент {session_name} остановлен (удален или время сна).")

    for account_data in accounts_from_file:
        session_name = account_data['session_name']

        if not _is_account_active(account_data):
            continue

        client_wrapper = active_clients.get(session_name)
        just_reconnected = False

        if client_wrapper is None:
            if not _connect_backoff_ready(session_name):
                continue
            client_wrapper = CommentatorClient(account_data, api_id, api_hash)
            try:
                if await client_wrapper.start():
                    active_clients[session_name] = client_wrapper
                    just_reconnected = True
                    CLIENT_CONNECT_STATE.pop(session_name, None)
                    logger.info(f"Клиент {session_name} запущен.")
                else:
                    err = getattr(client_wrapper, "_init_error", None) or "start_failed"
                    _schedule_connect_backoff(session_name, error=str(err), reason="start")
                    await client_wrapper.stop()
                    continue
            except Exception as e:
                _schedule_connect_backoff(session_name, error=str(e), reason="start")
                try:
                    await client_wrapper.stop()
                except Exception:
                    pass
                continue
        else:
            was_connected = bool(getattr(client_wrapper, "client", None) and client_wrapper.client.is_connected())
            if not await ensure_client_connected(client_wrapper, reason="manage_clients"):
                continue
            if not was_connected and client_wrapper.client.is_connected():
                just_reconnected = True

        if just_reconnected:
            keys_to_remove = [k for k in CLIENT_CATCH_UP_STATUS if k.startswith(f"{session_name}_")]
            for k in keys_to_remove:
                CLIENT_CATCH_UP_STATUS.discard(k)

        for target in get_project_targets(current_settings):
            if _is_account_assigned(target, session_name):
                joined_ok = await ensure_account_joined(client_wrapper, target)

                catch_up_key = f"{session_name}_{target.get('chat_id')}"
                if joined_ok and catch_up_key not in CLIENT_CATCH_UP_STATUS:
                    CLIENT_CATCH_UP_STATUS.add(catch_up_key)
                    asyncio.create_task(catch_up_missed_posts(client_wrapper, target))

        for r_target in get_project_reaction_targets(current_settings):
            if _is_account_assigned(r_target, session_name):
                await ensure_account_joined(client_wrapper, r_target)

        for d_target in get_project_discussion_targets(current_settings):
            operator_session = str(d_target.get("operator_session_name") or "").strip()
            if session_name == operator_session or _is_account_assigned(d_target, session_name):
                await ensure_account_joined(client_wrapper, d_target)

        for m_target in get_project_monitor_targets(current_settings):
            if _is_account_assigned(m_target, session_name):
                await ensure_account_joined(client_wrapper, m_target)


async def check_dialogue_depth(client, message_object, max_depth):
    try:
        if not message_object:
            return True

        current_depth = 0
        reply_ptr = message_object.reply_to

        while reply_ptr:
            current_depth += 1
            if current_depth >= max_depth:
                return False

            try:
                next_id = reply_ptr.reply_to_msg_id
                if not next_id:
                    break

                parent_msg = await client.get_messages(message_object.chat_id, ids=next_id)
                if not parent_msg:
                    break

                reply_ptr = parent_msg.reply_to
            except Exception:
                break

        return True
    except Exception as e:
        logger.error(f"Ошибка проверки глубины: {e}")
        return True


async def count_dialogue_ai_replies(
    client,
    message_object,
    our_ids: set,
    max_depth: int | None = None,
    include_current: bool = False,
    early_stop: int | None = None,
) -> int:
    try:
        if not message_object or not our_ids:
            return 0

        count = 0
        if include_current and getattr(message_object, "sender_id", None) in our_ids:
            count += 1
            if early_stop is not None and count >= early_stop:
                return count

        depth = 0
        reply_ptr = getattr(message_object, "reply_to", None)
        chat_id = getattr(message_object, "chat_id", None)
        if chat_id is None:
            return count

        while reply_ptr:
            depth += 1
            if max_depth is not None and depth >= int(max_depth):
                break

            next_id = getattr(reply_ptr, "reply_to_msg_id", None)
            if not next_id:
                break

            parent_msg = await client.get_messages(chat_id, ids=next_id)
            if isinstance(parent_msg, list):
                parent_msg = parent_msg[0] if parent_msg else None
            if not parent_msg:
                break

            if getattr(parent_msg, "sender_id", None) in our_ids:
                count += 1
                if early_stop is not None and count >= early_stop:
                    return count

            reply_ptr = getattr(parent_msg, "reply_to", None)

        return count
    except Exception:
        return int(early_stop) if early_stop is not None else 0


def get_all_our_user_ids():
    ids: set[int] = set()

    try:
        for client_wrapper in list(active_clients.values()):
            uid = getattr(client_wrapper, "user_id", None)
            if uid is None or uid == "":
                continue
            try:
                ids.add(int(uid))
            except Exception:
                continue
    except Exception:
        pass

    if ids:
        return ids

    accounts = load_project_accounts()
    for acc in accounts:
        if not isinstance(acc, dict):
            continue
        uid = acc.get("user_id")
        if uid is None or uid == "":
            continue
        try:
            ids.add(int(uid))
        except Exception:
            continue

    return ids


async def get_user_burst_messages(client, chat_id, original_msg):
    user_id = original_msg.sender_id
    burst_msgs = [original_msg]
    last_msg_id = original_msg.id

    try:
        async for msg in client.iter_messages(chat_id, min_id=original_msg.id, limit=5):
            if msg.sender_id == user_id and not msg.out:
                burst_msgs.append(msg)
                if msg.id > last_msg_id:
                    last_msg_id = msg.id
            else:
                break
    except Exception as e:
        logger.error(f"Error getting burst: {e}")

    burst_msgs.sort(key=lambda x: x.id)
    return burst_msgs, last_msg_id


async def execute_reply_with_fallback(candidate_list, chat_id, target_chat, prompt_base, delay, reply_to_msg_id, reply_to_name=None, is_intervention=False):
    global PENDING_TASKS
    task = asyncio.current_task()
    PENDING_TASKS.add(task)
    attempted_send = False
    active_client = None
    try:
        await asyncio.sleep(delay)
        actual_reply_id = reply_to_msg_id
        for client_wrapper in candidate_list:
            reply_text, prompt_info = await generate_comment(
                prompt_base,
                target_chat,
                client_wrapper.session_name,
                image_bytes=None,
                is_reply_mode=True,
                reply_to_name=reply_to_name
            )
            if reply_text:
                attempted_send = True
                active_client = client_wrapper
                await human_type_and_send(
                    client_wrapper.client,
                    chat_id,
                    reply_text,
                    reply_to_msg_id=actual_reply_id,
                )
                me = await client_wrapper.client.get_me()
                action_label = "ВМЕШАТЕЛЬСТВО" if is_intervention else "ОТВЕТ"
                logger.info(f"✅ [{client_wrapper.session_name}] ({action_label}) на сообщение {reply_to_msg_id} ({prompt_info})")
                log_content = f"[{prompt_info}] [{action_label}] {reply_text}"
                log_action_to_db({
                    'type': 'comment_reply',
                    'post_id': reply_to_msg_id,
                    'comment': log_content,
                    'date': datetime.now(timezone.utc).isoformat(),
                    'account': {'session_name': client_wrapper.session_name, 'first_name': me.first_name,
                                'username': me.username},
                    'target': {'chat_name': target_chat.get('chat_name'), 'destination_chat_id': chat_id}
                })
                _clear_account_failure(client_wrapper.session_name, "reply")
                return
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Ошибка в цепочке ответов: {e}")
        if attempted_send and active_client:
            _record_account_failure(
                active_client.session_name,
                "reply",
                last_error=str(e),
                last_target=str(chat_id),
            )
    finally:
        PENDING_TASKS.discard(task)


async def get_thread_context(client, event, our_ids):
    target_user_id = None
    target_name = "участник"
    is_intervention = True
    reply_to_id = event.message.reply_to_msg_id
    if reply_to_id:
        try:
            parent_msg = await client.get_messages(event.chat_id, ids=reply_to_id)
            if parent_msg and parent_msg.sender_id:
                target_user_id = parent_msg.sender_id
                sender_entity = await parent_msg.get_sender()
                if sender_entity:
                    target_name = getattr(sender_entity, 'first_name', 'участник')
        except Exception:
            pass
    else:
        try:
            async for msg in client.iter_messages(event.chat_id, limit=2, offset_id=event.message.id + 1):
                if msg.id != event.message.id:
                    target_user_id = msg.sender_id
                    sender_entity = await msg.get_sender()
                    if sender_entity:
                        target_name = getattr(sender_entity, 'first_name', 'участник')
                    break
        except Exception:
            pass
    return target_user_id, target_name


async def process_reply_to_comment(event, target_chat):
    global active_clients, REPLY_PROCESS_CACHE, PENDING_TASKS
    msg_id = event.message.id
    if msg_id in REPLY_PROCESS_CACHE:
        return
    REPLY_PROCESS_CACHE.add(msg_id)
    chat_id = event.chat_id
    sender_id = event.message.sender_id
    accounts_data = load_project_accounts(current_settings)
    eligible_candidates = []
    for c in list(active_clients.values()):
        acc_conf = next((a for a in accounts_data if a['session_name'] == c.session_name), None)
        if acc_conf and is_bot_awake(acc_conf) and getattr(c, 'user_id', None) != sender_id:
            if _is_account_assigned(target_chat, c.session_name):
                eligible_candidates.append(c)
    if not eligible_candidates:
        logger.info(f"⏭ Нет доступных аккаунтов для ответа на {msg_id}")
        return

    intervention_chance = target_chat.get('intervention_chance', 30)
    roll = random.randint(1, 100)
    if roll > intervention_chance:
        logger.info(f"🎲 Шанс не сработал ({roll} > {intervention_chance}%) для {msg_id}. Никто не ответил")
        return

    max_history = target_chat.get('max_dialogue_depth', 6)
    if not await check_dialogue_depth(event.client, event.message, max_history):
        logger.info(f"⏭ Сообщение {msg_id} пропущено: превышена глубина диалога ({max_history})")
        return

    our_ids = get_all_our_user_ids()

    max_ai_replies = target_chat.get("max_dialogue_ai_replies", 2)
    try:
        max_ai_replies = int(max_ai_replies or 0)
    except Exception:
        max_ai_replies = 2
    max_ai_replies = max(max_ai_replies, 0)

    if max_ai_replies > 0:
        ai_replies = await count_dialogue_ai_replies(
            event.client,
            event.message,
            our_ids=our_ids,
            max_depth=max_history,
            include_current=True,
            early_stop=max_ai_replies,
        )
        if ai_replies >= max_ai_replies:
            logger.info(f"⏭ Сообщение {msg_id} пропущено: лимит ответов ИИ в диалоге ({max_ai_replies})")
            return

    target_user_id, target_name = await get_thread_context(event.client, event, our_ids)
    is_reply_to_us = target_user_id in our_ids
    triggered_client = random.choice(eligible_candidates)
    is_intervention = getattr(triggered_client, 'user_id', None) != target_user_id
    d_min, d_max = target_chat.get('reply_delay_min', 20), target_chat.get('reply_delay_max', 80)
    personal_delay = random.randint(min(d_min, d_max), max(d_min, d_max))
    action_type = "ВМЕШАЕТСЯ" if is_intervention else "ОТВЕТИТ"
    to_who = f"нашему боту ({target_name})" if is_reply_to_us else f"пользователю {target_name}"
    logger.info(f"🤖 [{triggered_client.session_name}] {action_type} {to_who} через {personal_delay}с (шанс {roll}%)")
    asyncio.create_task(execute_reply_with_fallback([triggered_client], chat_id, target_chat,
                                                    f"{event.message.text}", personal_delay,
                                                    msg_id, reply_to_name=target_name, is_intervention=is_intervention))


def _schedule_discussion_run(
    *,
    chat_bare_id: int,
    chat_id: int,
    seed_msg_id: int,
    seed_text: str,
    target: dict,
    session_id: int | None = None,
) -> None:
    if not seed_text or not seed_msg_id:
        return
    try:
        chat_bare_id = int(chat_bare_id)
    except Exception:
        return

    unique_key = f"discussion:{chat_bare_id}:{seed_msg_id}"
    if not _mark_discussion_started(unique_key):
        return

    existing = DISCUSSION_ACTIVE_TASKS.get(chat_bare_id)
    if existing is not None and not existing.done():
        logger.info(f"⏭ [discussion] уже идёт обсуждение в чате {chat_bare_id}; пропускаю триггер {seed_msg_id}")
        return

    if session_id is None:
        try:
            project_id = _active_project_id(current_settings)
        except Exception:
            project_id = DEFAULT_PROJECT_ID
        operator_session = str((target or {}).get("operator_session_name") or "").strip() or None
        target_id = str((target or {}).get("id") or "").strip() or None
        settings_snapshot = {"target": target} if isinstance(target, dict) else {"target": {}}
        session_id = _db_create_discussion_session(
            project_id=str(project_id),
            discussion_target_id=target_id,
            discussion_target_chat_id=str((target or {}).get("chat_id") or "").strip() or str(chat_id),
            chat_id=str(chat_id),
            status="running",
            operator_session_name=operator_session,
            seed_msg_id=int(seed_msg_id),
            seed_text=str(seed_text),
            settings=settings_snapshot,
        )
        if session_id:
            _db_add_discussion_message(
                session_id=int(session_id),
                speaker_type="operator",
                speaker_session_name=operator_session,
                speaker_label="Оператор",
                msg_id=int(seed_msg_id),
                reply_to_msg_id=None,
                text=str(seed_text),
            )

    REPLY_PROCESS_CACHE.add(seed_msg_id)
    task = asyncio.create_task(
        run_discussion_session(
            chat_id=chat_id,
            chat_bare_id=chat_bare_id,
            seed_msg_id=seed_msg_id,
            seed_text=seed_text,
            target=target,
            session_id=int(session_id) if session_id else None,
        )
    )
    try:
        setattr(task, "discussion_session_id", int(session_id) if session_id else None)
    except Exception:
        pass
    DISCUSSION_ACTIVE_TASKS[chat_bare_id] = task

    def _cleanup(done_task: asyncio.Task) -> None:  # noqa: ANN001
        cur = DISCUSSION_ACTIVE_TASKS.get(chat_bare_id)
        if cur is done_task:
            DISCUSSION_ACTIVE_TASKS.pop(chat_bare_id, None)

    task.add_done_callback(_cleanup)


async def run_discussion_session(
    *,
    chat_id: int,
    chat_bare_id: int,
    seed_msg_id: int,
    seed_text: str,
    target: dict,
    session_id: int | None = None,
) -> None:
    global active_clients, current_settings, PENDING_TASKS
    current_task = asyncio.current_task()
    if current_task:
        PENDING_TASKS.add(current_task)

    try:
        seed_text = str(seed_text or "").strip()
        if not seed_text:
            return

        session_id_int: int | None = None
        try:
            session_id_int = int(session_id) if session_id else None
        except Exception:
            session_id_int = None

        if session_id_int:
            try:
                _db_update_discussion_session(session_id_int, status="running", started_at=float(time.time()))
            except Exception:
                pass

        target = target if isinstance(target, dict) else {}
        operator_session = str(target.get("operator_session_name") or "").strip()
        base_vector = str(target.get("vector_prompt") or "").strip()

        extra_scenes_raw = target.get("scenes")
        extra_scenes: list[dict] = []
        if isinstance(extra_scenes_raw, list):
            extra_scenes = [sc for sc in extra_scenes_raw if isinstance(sc, dict)]
        total_scenes = 1 + len(extra_scenes)

        def _int_setting_from(
            scene: dict,
            key: str,
            default: int,
            *,
            min_value: int | None = None,
            max_value: int | None = None,
        ) -> int:
            raw = None
            if isinstance(scene, dict) and key in scene:
                raw = scene.get(key)
            if raw is None or (isinstance(raw, str) and raw.strip() == ""):
                raw = target.get(key, default)
            if raw is None or (isinstance(raw, str) and raw.strip() == ""):
                raw = default
            try:
                value = int(raw)
            except Exception:
                value = int(default)
            if min_value is not None:
                value = max(value, int(min_value))
            if max_value is not None:
                value = min(value, int(max_value))
            return int(value)

        def _vector_for(scene: dict) -> str:
            v = str((scene or {}).get("vector_prompt") or "").strip()
            return v if v else base_vector

        def _assigned_accounts_for(scene: dict) -> list[str]:
            raw = (scene or {}).get("assigned_accounts")
            if isinstance(raw, list):
                items = [str(s).strip() for s in raw if str(s).strip()]
                if items:
                    return items
            return [str(s).strip() for s in (target.get("assigned_accounts") or []) if str(s).strip()]

        accounts_data = load_project_accounts(current_settings)
        account_by_session = {
            str(a.get("session_name")).strip(): a
            for a in (accounts_data or [])
            if isinstance(a, dict) and a.get("session_name")
        }

        labels: dict[str, str] = {}
        next_label = {"idx": 1}
        participants_snapshot: list[dict] = []
        participants_seen: set[str] = set()
        excluded_sessions: set[str] = set()

        def _ensure_labels_for(clients: list) -> None:
            for c in clients:
                sess = str(getattr(c, "session_name", "") or "").strip()
                if not sess:
                    continue
                if sess not in labels:
                    labels[sess] = f"Участник {next_label['idx']}"
                    next_label["idx"] += 1

        def _update_participants_snapshot(clients: list) -> None:
            if not session_id_int:
                return
            changed_participants = False
            for c in clients:
                sess = str(getattr(c, "session_name", "") or "").strip()
                if not sess or sess in participants_seen:
                    continue
                participants_seen.add(sess)
                acc_conf = account_by_session.get(sess, {}) if isinstance(account_by_session, dict) else {}
                role_id, role_data = role_for_account(acc_conf or {}, current_settings)
                role_prompt, role_meta = build_role_prompt(role_data or {}, current_settings)
                participants_snapshot.append(
                    {
                        "session_name": sess,
                        "label": labels.get(sess),
                        "role_id": role_id,
                        "role_name": str((role_data or {}).get("name") or role_id or "Роль"),
                        "role_prompt": role_prompt,
                        "role_meta": role_meta,
                        "persona_id": acc_conf.get("persona_id") if isinstance(acc_conf, dict) else None,
                    }
                )
                changed_participants = True
            if changed_participants:
                try:
                    _db_update_discussion_session(
                        session_id_int,
                        participants_json=_safe_json_dumps(participants_snapshot),
                    )
                except Exception:
                    pass

        def _eligible_clients_for(assigned_list: list[str]) -> list:
            eligible: list = []
            for client_wrapper in list(active_clients.values()):
                session_name = str(getattr(client_wrapper, "session_name", "") or "").strip()
                if not session_name or session_name not in assigned_list:
                    continue
                if session_name in excluded_sessions:
                    continue
                if operator_session and session_name == operator_session:
                    continue
                acc_conf = account_by_session.get(session_name)
                if acc_conf and is_bot_awake(acc_conf):
                    eligible.append(client_wrapper)
            return eligible

        last_speaker = "Оператор"
        history: list[dict[str, str]] = []
        reply_to_msg_id: int = int(seed_msg_id)
        last_sender_session: str | None = None

        def _truncate_memory_line(value: str, limit: int = 280) -> str:
            s = re.sub(r"\s+", " ", str(value or "")).strip()
            if limit > 0 and len(s) > limit:
                return s[: limit - 1].rstrip() + "…"
            return s

        def _build_memory_block(items: list[dict[str, str]], max_items: int, *, exclude_last: bool = True) -> str:
            if max_items <= 0:
                return ""
            mem = items[-max_items:] if items else []
            if exclude_last and mem:
                mem = mem[:-1]
            lines: list[str] = []
            for it in mem:
                speaker = str(it.get("speaker") or "Участник").strip() or "Участник"
                text = _truncate_memory_line(str(it.get("text") or ""))
                if text:
                    lines.append(f"{speaker}: {text}")
            return "\n".join(lines).strip()

        for scene_number, scene in enumerate([{}, *extra_scenes], start=1):
            scene_title = str((scene or {}).get("title") or "").strip()
            scene_vector = _vector_for(scene)
            assigned = _assigned_accounts_for(scene)

            if not assigned:
                logger.info(f"⏭ [discussion] чат {chat_bare_id}: нет assigned_accounts — обсуждение не запускаю")
                if session_id_int and scene_number == 1:
                    try:
                        _db_update_discussion_session(
                            session_id_int,
                            status="failed",
                            finished_at=float(time.time()),
                            error="no_assigned_accounts",
                        )
                    except Exception:
                        pass
                return

            eligible_clients = _eligible_clients_for(assigned)
            if not eligible_clients:
                if scene_number == 1:
                    logger.info(f"⏭ [discussion] чат {chat_bare_id}: нет доступных аккаунтов‑участников")
                    if session_id_int:
                        try:
                            _db_update_discussion_session(
                                session_id_int,
                                status="failed",
                                finished_at=float(time.time()),
                                error="no_available_participants",
                            )
                        except Exception:
                            pass
                    return
                logger.info(f"⏭ [discussion] сцена {scene_number}/{total_scenes} пропущена: нет доступных участников")
                continue

            random.shuffle(eligible_clients)
            _ensure_labels_for(eligible_clients)
            _update_participants_snapshot(eligible_clients)

            if scene_number == 1:
                scene_seed_text = seed_text
                if not history:
                    history.append({"speaker": "Оператор", "text": scene_seed_text})
            else:
                operator_text = str((scene or {}).get("operator_text") or "").strip()
                if not operator_text:
                    logger.info(f"⏭ [discussion] сцена {scene_number}/{total_scenes} пропущена: пустая фраза оператора")
                    continue
                if not operator_session:
                    logger.warning(
                        f"⚠️ [discussion] сцена {scene_number}/{total_scenes}: не задан operator_session_name — остановка"
                    )
                    break

                prev_reply_to = int(reply_to_msg_id) if reply_to_msg_id else None
                op_wrapper = active_clients.get(operator_session) if operator_session else None
                temp_client = None
                op_client = None
                if op_wrapper is not None and getattr(op_wrapper, "client", None) is not None:
                    if not await ensure_client_connected(op_wrapper, reason="discussion_scene"):
                        raise RuntimeError("operator_connect_failed")
                    op_client = op_wrapper.client
                else:
                    telethon_config = load_config('telethon_credentials')
                    api_id, api_hash = int(telethon_config['api_id']), telethon_config['api_hash']
                    acc_conf = account_by_session.get(operator_session)
                    if not acc_conf:
                        raise KeyError("operator_account_not_found")
                    temp_client = await _connect_temp_client(acc_conf, api_id, api_hash)
                    op_client = temp_client

                sent_op = None
                try:
                    DISCUSSION_START_SUPPRESS_CHAT_IDS.add(int(chat_bare_id))
                    sent_op = await human_type_and_send(
                        op_client,
                        chat_id,
                        operator_text,
                        reply_to_msg_id=prev_reply_to,
                        skip_processing=True,
                        split_mode="off",
                    )
                finally:
                    try:
                        DISCUSSION_START_SUPPRESS_CHAT_IDS.discard(int(chat_bare_id))
                    except Exception:
                        pass
                    if temp_client is not None:
                        try:
                            if temp_client.is_connected():
                                await temp_client.disconnect()
                        except Exception:
                            pass

                op_msg_id = getattr(sent_op, "id", None)
                if not op_msg_id:
                    logger.warning(f"⚠️ [discussion] сцена {scene_number}/{total_scenes}: не удалось отправить фразу оператора")
                    break

                try:
                    REPLY_PROCESS_CACHE.add(int(op_msg_id))
                except Exception:
                    pass

                if session_id_int:
                    try:
                        _db_add_discussion_message(
                            session_id=session_id_int,
                            speaker_type="operator",
                            speaker_session_name=str(operator_session or "").strip() or None,
                            speaker_label="Оператор",
                            msg_id=int(op_msg_id),
                            reply_to_msg_id=int(prev_reply_to) if prev_reply_to else None,
                            text=str(operator_text),
                            prompt_info=f"sc{scene_number}/{total_scenes}",
                        )
                    except Exception:
                        pass

                reply_to_msg_id = int(op_msg_id)
                last_speaker = "Оператор"
                scene_seed_text = operator_text
                history.append({"speaker": "Оператор", "text": operator_text.strip()})

            turns_min = _int_setting_from(scene, "turns_min", 6, min_value=1, max_value=200)
            turns_max = _int_setting_from(scene, "turns_max", 10, min_value=1, max_value=200)
            if turns_max < turns_min:
                turns_max = turns_min
            total_turns = random.randint(turns_min, turns_max)

            start_delay_min = _int_setting_from(scene, "initial_delay_min", 10, min_value=0, max_value=86400)
            start_delay_max = _int_setting_from(scene, "initial_delay_max", 40, min_value=0, max_value=86400)
            if start_delay_max < start_delay_min:
                start_delay_max = start_delay_min

            between_delay_min = _int_setting_from(scene, "delay_between_min", 20, min_value=0, max_value=86400)
            between_delay_max = _int_setting_from(scene, "delay_between_max", 80, min_value=0, max_value=86400)
            if between_delay_max < between_delay_min:
                between_delay_max = between_delay_min

            if start_delay_max > 0:
                await asyncio.sleep(random.uniform(float(start_delay_min), float(start_delay_max)))

            for turn_idx in range(total_turns):
                if turn_idx > 0 and between_delay_max > 0:
                    await asyncio.sleep(random.uniform(float(between_delay_min), float(between_delay_max)))

                # Pick next speaker; avoid immediate repeats when possible.
                candidates = [c for c in eligible_clients if c.session_name != last_sender_session] or list(eligible_clients)
                random.shuffle(candidates)
                client_wrapper = None

                for cand in list(candidates):
                    if not await ensure_client_connected(cand, reason="discussion"):
                        _record_account_failure(
                            cand.session_name,
                            "discussion",
                            last_error="connect_failed",
                            last_target=str(chat_id),
                        )
                        excluded_sessions.add(str(cand.session_name))
                        eligible_clients = [c for c in eligible_clients if c.session_name != cand.session_name]
                        continue
                    client_wrapper = cand
                    break

                if client_wrapper is None:
                    logger.warning(
                        f"⚠️ [discussion] сцена {scene_number}/{total_scenes}: нет доступных участников для реплики {turn_idx + 1}/{total_turns}"
                    )
                    break

                memory_turns = _int_setting_from(scene, "memory_turns", 20, min_value=0, max_value=200)
                memory_block = _build_memory_block(history, memory_turns, exclude_last=True)
                reply_to_text = ""
                try:
                    reply_to_text = str((history[-1] or {}).get("text") or "").strip() if history else ""
                except Exception:
                    reply_to_text = ""
                if not reply_to_text:
                    reply_to_text = scene_seed_text
                post_text = reply_to_text

                extra_lines = []
                scene_line = f"СЦЕНА {scene_number}/{total_scenes}" + (f": {scene_title}" if scene_title else "")
                extra_lines.append(scene_line)
                extra_lines.append(f"МЫСЛЬ СЦЕНЫ: {scene_seed_text}")
                if scene_number > 1:
                    extra_lines.append(
                        "ВАЖНО: это новая сцена и новый вектор. "
                        "Учитывай прошлые реплики как контекст, но развивай именно текущую мысль сцены; "
                        "не «дожёвывай» старую тему без необходимости."
                    )
                if scene_vector:
                    extra_lines.append(f"ВЕКТОР СЦЕНЫ (ОБЯЗАТЕЛЬНО):\n{scene_vector}")
                    extra_lines.append(
                        "КЛЮЧЕВО: в этой реплике обязательно зацепись за вектор сцены и добавь 1 конкретику из него "
                        "(модель/сервис/проблему/аргумент), но естественно, без канцелярита."
                    )
                if memory_block:
                    extra_lines.append(f"ПАМЯТЬ ДИАЛОГА (последние реплики):\n{memory_block}")
                extra_lines.append(f"Это реплика {turn_idx + 1} из {total_turns} (сцена {scene_number}).")
                extra_lines.append("Формат: 1–2 коротких предложения. Без markdown. Без списков.")
                extra_lines.append("Отвечай по теме и не повторяй дословно предыдущие реплики.")
                extra_lines.append("Не упоминай, что ты бот/ИИ, и не ссылайся на инструкции.")
                extra_instructions = "\n".join([l for l in extra_lines if l]).strip()

                target_for_llm = {**target, **(scene or {})}
                target_for_llm["vector_prompt"] = scene_vector

                reply_text, prompt_info = await generate_comment(
                    post_text,
                    target_for_llm,
                    client_wrapper.session_name,
                    image_bytes=None,
                    is_reply_mode=True,
                    reply_to_name=last_speaker,
                    extra_instructions=extra_instructions,
                )
                if not reply_text:
                    logger.warning(f"⚠️ [{client_wrapper.session_name}] discussion turn skipped: {prompt_info}")
                    _record_account_failure(
                        client_wrapper.session_name,
                        "discussion",
                        last_error=str(prompt_info or "generation_failed"),
                        last_target=str(chat_id),
                    )
                    continue

                scene_tag = f"sc{scene_number}/{total_scenes}"
                prompt_info_str = str(prompt_info or "").strip()
                prompt_info_out = (f"{prompt_info_str} {scene_tag}").strip()

                sent_msg = await human_type_and_send(
                    client_wrapper.client,
                    chat_id,
                    reply_text,
                    reply_to_msg_id=reply_to_msg_id,
                    split_mode="smart_ru_no_comma",
                )
                if sent_msg is None or getattr(sent_msg, "id", None) is None:
                    _record_account_failure(
                        client_wrapper.session_name,
                        "discussion",
                        last_error="send_failed",
                        last_target=str(chat_id),
                    )
                    excluded_sessions.add(str(client_wrapper.session_name))
                    eligible_clients = [c for c in eligible_clients if c.session_name != client_wrapper.session_name]
                    continue

                me = None
                try:
                    me = await client_wrapper.client.get_me()
                except Exception:
                    me = None

                logger.info(
                    f"💬 [{client_wrapper.session_name}] discussion {turn_idx + 1}/{total_turns} in {chat_bare_id} ({prompt_info_out})"
                )

                msg_id = getattr(sent_msg, "id", None)
                if session_id_int:
                    try:
                        _db_add_discussion_message(
                            session_id=session_id_int,
                            speaker_type="bot",
                            speaker_session_name=str(client_wrapper.session_name),
                            speaker_label=labels.get(client_wrapper.session_name),
                            msg_id=int(msg_id) if msg_id else None,
                            reply_to_msg_id=int(reply_to_msg_id) if reply_to_msg_id else None,
                            text=str(reply_text or ""),
                            prompt_info=str(prompt_info_out or ""),
                        )
                    except Exception:
                        pass

                try:
                    log_action_to_db(
                        {
                            "type": "discussion",
                            "post_id": seed_msg_id,
                            "comment": f"[{prompt_info_out}] {reply_text}",
                            "date": datetime.now(timezone.utc).isoformat(),
                            "account": {
                                "session_name": client_wrapper.session_name,
                                "first_name": getattr(me, "first_name", "") if me else "",
                                "username": getattr(me, "username", "") if me else "",
                            },
                            "target": {
                                "chat_name": target.get("chat_name"),
                                "chat_username": target.get("chat_username"),
                                "destination_chat_id": chat_id,
                            },
                        }
                    )
                except Exception:
                    pass

                _clear_account_failure(client_wrapper.session_name, "discussion")
                if msg_id:
                    try:
                        reply_to_msg_id = int(msg_id)
                        REPLY_PROCESS_CACHE.add(int(msg_id))
                    except Exception:
                        pass

                speaker_label = labels.get(client_wrapper.session_name, "Участник")
                history.append({"speaker": speaker_label, "text": reply_text.strip()})
                last_speaker = speaker_label
                last_sender_session = client_wrapper.session_name
    except asyncio.CancelledError:
        if session_id_int:
            try:
                _db_update_discussion_session(
                    session_id_int,
                    status="canceled",
                    finished_at=float(time.time()),
                    error="canceled",
                )
            except Exception:
                pass
        raise
    except Exception as e:
        logger.error(f"❌ [discussion] ошибка в чате {chat_bare_id}: {e}")
        if session_id_int:
            try:
                _db_update_discussion_session(
                    session_id_int,
                    status="failed",
                    finished_at=float(time.time()),
                    error=str(e),
                )
            except Exception:
                pass
    finally:
        if session_id_int:
            try:
                row = None
                with _db_connect() as conn:
                    row = conn.execute(
                        "SELECT status FROM discussion_sessions WHERE id = ?",
                        (int(session_id_int),),
                    ).fetchone()
                cur_status = ""
                if row is not None:
                    try:
                        cur_status = str(row["status"] or "")
                    except Exception:
                        cur_status = ""
                if cur_status == "running":
                    _db_update_discussion_session(
                        session_id_int,
                        status="completed",
                        finished_at=float(time.time()),
                    )
            except Exception:
                pass
        if current_task:
            PENDING_TASKS.discard(current_task)


async def mark_account_as_banned(session_name):
    accounts = load_json_data(ACCOUNTS_FILE, [])
    updated = False
    for acc in accounts:
        if acc['session_name'] == session_name:
            acc['status'] = 'banned'
            acc['banned_at'] = datetime.now(timezone.utc).isoformat()
            updated = True
            break
    if updated:
        with open(ACCOUNTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(accounts, f, indent=2, ensure_ascii=False)
    logger.error(f"Аккаунт {session_name} помечен как ЗАБАНЕН в {ACCOUNTS_FILE}")


def post_process_text(text):
    if not text:
        return text
    global current_settings
    h_set = current_settings.get('humanization', {}) or {}

    typo_chance = h_set.get('typo_chance', 0) / 100
    lower_chance = h_set.get('lowercase_chance', 0) / 100
    comma_chance = h_set.get('comma_skip_chance', 0) / 100
    try:
        max_words = int(h_set.get('max_words', 40) or 40)
    except Exception:
        max_words = 40
    if max_words <= 0:
        max_words = 40

    text = text.strip()

    formal_words = ["уважаемые", "благодарю", "данный пост", "согласно", "ввиду", "ассистент", "внимание", "пожалуйста",
                    "я ии", "виртуальный", "интеллект"]
    for word in formal_words:
        if word in text.lower():
            text = text.replace(word, "").replace(word.capitalize(), "")

    text = text.replace('—', '-').replace('–', '-')
    text = text.replace('"', '').replace("'", "")
    text = text.replace("«", "").replace("»", "")
    text = text.replace("“", "").replace("”", "").replace("„", "")

    while '!!!' in text:
        text = text.replace('!!!', '!!')

    if len(text) < 80 and text.endswith('.'):
        text = text[:-1]

    words = text.split()

    processed_words = []
    for word in words:
        if ',' in word and random.random() < comma_chance:
            word = word.replace(',', '')

        if typo_chance > 0 and random.random() < typo_chance and len(word) > 4:
            idx = random.randint(1, len(word) - 2)
            w_list = list(word)
            w_list[idx], w_list[idx + 1] = w_list[idx + 1], w_list[idx]
            word = "".join(w_list)

        processed_words.append(word)

    processed_words = processed_words[:max_words]

    res = " ".join(processed_words)
    res = re.sub(r"\s{2,}", " ", res).strip()

    # Hard guardrail against "essay mode": at most 4 short sentences.
    sentence_parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+", res) if part.strip()]
    if len(sentence_parts) > 4:
        res = " ".join(sentence_parts[:4]).strip()

    limited_words = res.split()
    if len(limited_words) > max_words:
        res = " ".join(limited_words[:max_words]).strip()

    if random.random() < lower_chance:
        res = res.lower()
    elif res and random.random() < lower_chance:
        parts = res.split()
        if parts:
            parts[0] = parts[0].lower()
            res = " ".join(parts)

    return res


def split_text_smart_ru_no_comma(text: str) -> list[str]:
    s = (text or "").strip()
    if not s:
        return []

    def _is_ok(left: str, right: str, *, min_words_left: int, min_words_right: int, min_chars_left: int, min_chars_right: int) -> bool:
        if not left or not right:
            return False
        if len(left) < min_chars_left or len(right) < min_chars_right:
            return False
        if len(left.split()) < min_words_left or len(right.split()) < min_words_right:
            return False
        return True

    def _best(parts: list[tuple[str, str]]) -> list[str] | None:
        if not parts:
            return None
        best_left, best_right = min(parts, key=lambda p: abs(len(p[0]) - len(p[1])))
        return [best_left, best_right]

    # 1) Sentence boundaries (. ! ? …) + whitespace
    sentence_candidates: list[tuple[str, str]] = []
    for m in re.finditer(r"[.!?…]+(?:\s+|$)", s):
        split_at = m.end()
        left = s[:split_at].rstrip()
        right = s[split_at:].lstrip()
        if _is_ok(left, right, min_words_left=2, min_words_right=2, min_chars_left=8, min_chars_right=8):
            sentence_candidates.append((left, right))
    best_sentence = _best(sentence_candidates)
    if best_sentence:
        return best_sentence

    # 2) Colon
    colon_candidates: list[tuple[str, str]] = []
    for m in re.finditer(r":\s+", s):
        split_at = m.end()
        left = s[:split_at].rstrip()
        right = s[split_at:].lstrip()
        if _is_ok(left, right, min_words_left=1, min_words_right=2, min_chars_left=6, min_chars_right=8):
            colon_candidates.append((left, right))
    best_colon = _best(colon_candidates)
    if best_colon:
        return best_colon

    # 3) Semicolon
    semicolon_candidates: list[tuple[str, str]] = []
    for m in re.finditer(r";\s+", s):
        split_at = m.end()
        left = s[:split_at].rstrip()
        right = s[split_at:].lstrip()
        if _is_ok(left, right, min_words_left=1, min_words_right=2, min_chars_left=6, min_chars_right=8):
            semicolon_candidates.append((left, right))
    best_semicolon = _best(semicolon_candidates)
    if best_semicolon:
        return best_semicolon

    # 4) " - " where the dash stays with the second part ("- ...")
    dash_candidates: list[tuple[str, str]] = []
    start = 0
    while True:
        idx = s.find(" - ", start)
        if idx < 0:
            break
        left = s[:idx].rstrip()
        right = s[idx + 1 :].lstrip()
        if _is_ok(left, right, min_words_left=2, min_words_right=2, min_chars_left=8, min_chars_right=8):
            dash_candidates.append((left, right))
        start = idx + 3
    best_dash = _best(dash_candidates)
    if best_dash:
        return best_dash

    return [s]


async def human_type_and_send(
    client,
    chat_id,
    text,
    reply_to_msg_id=None,
    skip_processing=False,
    thread_top_msg_id: int | None = None,
    split_mode: Literal["legacy", "smart_ru_no_comma", "off"] = "legacy",
):
    if not text:
        return
    global current_settings

    if skip_processing:
        processed_text = text
    else:
        processed_text = post_process_text(text)

    split_chance = current_settings.get('humanization', {}).get('split_chance', 0) / 100
    message_parts = []

    if split_mode != "off" and not skip_processing and len(processed_text) > 50 and random.random() < split_chance:
        if split_mode == "smart_ru_no_comma":
            message_parts = split_text_smart_ru_no_comma(processed_text) or [processed_text]
        else:
            delimiters = [', ', '. ', '! ', '? ']
            split_done = False
            for d in delimiters:
                if d in processed_text:
                    parts = processed_text.split(d, 1)
                    message_parts = [parts[0], parts[1]]
                    split_done = True
                    break
            if not split_done:
                message_parts = [processed_text]
    else:
        message_parts = [processed_text]

    last_msg = None
    original_reply_id = reply_to_msg_id

    async def _send_to_thread_without_quote(part_text: str, top_id: int):
        peer = await _run_with_soft_timeout(client.get_input_entity(chat_id), SEND_ATTEMPT_TIMEOUT_SECONDS)
        req = functions.messages.SendMessageRequest(
            peer=peer,
            message=part_text,
            reply_to=types.InputReplyToMessage(reply_to_msg_id=0, top_msg_id=int(top_id)),
            random_id=helpers.generate_random_long(),
        )
        res = await _run_with_soft_timeout(client(req), SEND_ATTEMPT_TIMEOUT_SECONDS)
        try:
            for upd in (getattr(res, "updates", None) or []):
                msg = getattr(upd, "message", None)
                if msg is not None:
                    return msg
        except Exception:
            pass
        return None

    for part in message_parts:
        part = part.strip()
        if not part: continue

        await asyncio.sleep(random.uniform(2, 4))

        typing_time = min(len(part) * 0.06, 6)

        async def _typing_sleep():
            async with client.action(chat_id, 'typing'):
                await asyncio.sleep(typing_time)

        try:
            await _run_with_soft_timeout(_typing_sleep(), SEND_ATTEMPT_TIMEOUT_SECONDS)
        except (ChatAdminRequiredError, RPCError, Exception):
            await asyncio.sleep(typing_time)

        try:
            if original_reply_id is None and thread_top_msg_id:
                try:
                    last_msg = await _send_to_thread_without_quote(part, int(thread_top_msg_id))
                except Exception:
                    # Fallback to plain reply to the thread root (may show quote, but stays in thread).
                    last_msg = await _run_with_soft_timeout(
                        client.send_message(chat_id, part, reply_to=int(thread_top_msg_id)),
                        SEND_ATTEMPT_TIMEOUT_SECONDS,
                    )
            else:
                last_msg = await _run_with_soft_timeout(
                    client.send_message(chat_id, part, reply_to=original_reply_id),
                    SEND_ATTEMPT_TIMEOUT_SECONDS,
                )
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке сообщения: {e}")
            try:
                if getattr(client, "is_connected", None) and client.is_connected():
                    await client.disconnect()
            except Exception:
                pass
            break

    return last_msg


async def generate_batch_identities(topic, count, provider, api_key):
    system_prompt = (
        f"Ты — генератор профилей для Telegram. Тематика: '{topic}'.\n"
        f"Сгенерируй список из {count} УНИКАЛЬНЫХ имен и фамилий.\n"
        f"Имена должны быть разными, креативными, реалистичными или сленговыми.\n"
        f"Верни ответ ТОЛЬКО в формате чистого списка Python, без лишних слов:\n"
        f"['Имя Фамилия', 'Имя Фамилия', ...]"
    )

    try:
        content = ""
        if provider in {"openai", "openrouter", "deepseek"}:
            base_url = None
            default_headers = None
            if provider == "deepseek":
                base_url = "https://api.deepseek.com"
            elif provider == "openrouter":
                base_url = "https://openrouter.ai/api/v1"
                default_headers = {
                    "HTTP-Referer": os.getenv("OPENROUTER_REFERRER", "http://localhost"),
                    "X-Title": os.getenv("OPENROUTER_TITLE", "AI-Центр"),
                }

            model_key = "deepseek_chat" if provider == "deepseek" else ("openrouter_chat" if provider == "openrouter" else "openai_chat")
            model_name = get_model_setting(current_settings, model_key)
            client = openai.AsyncOpenAI(api_key=api_key, base_url=base_url, default_headers=default_headers)
            create_kwargs = {
                "messages": [{"role": "user", "content": system_prompt}],
                "model": model_name,
                "temperature": 1.4,
            }
            if provider == "openai":
                create_kwargs["max_completion_tokens"] = 512
            else:
                create_kwargs["max_tokens"] = 512
            completion = await client.chat.completions.create(**create_kwargs)
            content = completion.choices[0].message.content.strip()
        elif provider == 'gemini':
            for model_name in gemini_model_candidates(current_settings, "gemini_names"):
                try:
                    async with genai.Client(api_key=api_key).aio as aclient:
                        response = await aclient.models.generate_content(
                            model=model_name,
                            contents=system_prompt,
                            config=genai_types.GenerateContentConfig(
                                temperature=1.4,
                                max_output_tokens=256,
                            ),
                        )
                    content = (response.text or "").strip()
                    break
                except Exception:
                    content = ""
                    continue

        content = content.replace("```python", "").replace("```json", "").replace("```", "")
        names_list = ast.literal_eval(content)
        if isinstance(names_list, list):
            return names_list
        return []
    except Exception as e:
        logger.error(f"Ошибка массовой генерации имен: {e}")
        return []


async def get_real_identities_from_channel(client, source_channel, limit=200):
    identities = []
    seen_ids = set()
    scan_depth = 4000

    try:
        with _db_connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id FROM used_identities")
            rows = cursor.fetchall()
            for row in rows:
                seen_ids.add(row[0])

    except Exception as e:
        logger.error(f"Ошибка загрузки использованных ID из БД: {e}")

    try:
        if not client.is_connected():
            await client.connect()

        async for message in client.iter_messages(source_channel, limit=scan_depth):
            if not message.sender or not hasattr(message.sender, 'id'):
                continue

            user_id = message.sender.id
            if user_id in seen_ids:
                continue

            if hasattr(message.sender, 'bot') and message.sender.bot:
                continue

            if hasattr(message.sender, 'scam') and message.sender.scam:
                continue

            seen_ids.add(user_id)

            first_name = getattr(message.sender, 'first_name', '') or ''
            last_name = getattr(message.sender, 'last_name', '') or ''

            if not first_name.strip():
                continue

            identities.append({
                'user_id': user_id,
                'first_name': first_name,
                'last_name': last_name,
                'user_entity': message.sender,
                'has_photo': getattr(message.sender, 'photo', None) is not None
            })

            if len(identities) >= limit * 4:
                break

    except Exception as e:
        logger.error(f"Ошибка при сборе реальных профилей: {e}")

    random.shuffle(identities)
    return identities[:limit]


async def run_rebrand_logic(api_id, api_hash):
    global current_settings, active_clients
    task = current_settings.get('rebrand_task')
    if not task or task.get('status') != 'pending':
        return

    raw_source = task.get('source_value', task.get('source_channel'))
    is_channel = task.get('is_channel', True)

    if raw_source:
        source_val = raw_source.strip().replace('https://t.me/', '').replace('http://t.me/', '').replace('t.me/', '').replace('@', '')
        if '+' in source_val and not source_val.startswith('+'):
            pass
    else:
        source_val = "abstract"

    logger.info(f"🚀 Запуск процесса ребрендинга: {task['topic']} | Источник: {source_val}")

    provider = current_settings.get('ai_provider', 'gemini')
    api_key = current_settings.get('api_keys', {}).get(provider)
    openai_key = current_settings.get('api_keys', {}).get('openai')

    import httpx
    import urllib.parse
    from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest

    clients_list = list(active_clients.values())
    real_identities = []
    generated_names = []

    using_real_profiles = False
    successful_parser = None

    if is_channel and clients_list:
        logger.info(f"📥 Начинаем поиск донора для парсинга канала @{source_val}...")

        for parser_wrapper in clients_list:
            try:
                parser_client = parser_wrapper.client
                if not parser_client.is_connected():
                    await parser_client.connect()

                try:
                    entity = await parser_client.get_entity(source_val)
                except ValueError:
                    try:
                        await parser_client(JoinChannelRequest(source_val))
                        entity = await parser_client.get_entity(source_val)
                    except Exception:
                        raise Exception(f"Не удалось найти канал {source_val}")

                target_entity = entity
                try:
                    full_channel = await parser_client(GetFullChannelRequest(entity))
                    if full_channel.full_chat.linked_chat_id:
                        linked_id = full_channel.full_chat.linked_chat_id
                        target_entity = await parser_client.get_entity(linked_id)
                        logger.info(f"💬 У канала есть чат с обсуждениями: {target_entity.title}. Будем парсить его.")
                    else:
                        logger.warning("⚠️ У канала НЕТ привязанного чата. Попробуем парсить сам канал (если это группа).")
                except Exception as e:
                    logger.warning(f"Не удалось получить инфо о привязанном чате: {e}")

                logger.info(f"🔄 Пробуем парсить через аккаунт: {parser_wrapper.session_name}")

                needed_count = len(clients_list) + 10
                real_identities = await get_real_identities_from_channel(parser_client, target_entity, limit=needed_count)

                if real_identities:
                    logger.info(f"✅ Успешно собрано {len(real_identities)} профилей через {parser_wrapper.session_name}")
                    using_real_profiles = True
                    successful_parser = parser_client
                    break
                else:
                    logger.warning(f"⚠️ Аккаунт {parser_wrapper.session_name} зашел, но не нашел сообщений с людьми.")
            except Exception as e:
                logger.warning(f"⚠️ Аккаунт {parser_wrapper.session_name} не смог спарсить канал: {e}")
                continue

        if not using_real_profiles:
            logger.error("❌ Ни один аккаунт не смог получить доступ к каналу-донору. Переключаюсь на генерацию AI.")
            is_channel = False

    if not using_real_profiles:
        needed_count = len(clients_list) + 5
        generated_names = await generate_batch_identities(task['topic'], needed_count, provider, api_key)
        logger.info(f"✅ Сгенерировано {len(generated_names)} имен через AI.")

    identity_index = 0

    for client_wrapper in clients_list:
        try:
            photo_path = os.path.join(BASE_DIR, f"avatar_{client_wrapper.session_name}.jpg")
            got_photo = False
            first_name = ""
            last_name = ""
            current_identity_user_id = None

            if using_real_profiles and successful_parser:
                identity = real_identities[identity_index % len(real_identities)]
                first_name = identity['first_name']
                last_name = identity['last_name']
                current_identity_user_id = identity.get('user_id')

                if identity['has_photo']:
                    try:
                        await successful_parser.download_profile_photo(identity['user_entity'], file=photo_path)
                        if os.path.exists(photo_path):
                            got_photo = True
                    except Exception as e:
                        logger.error(f"Ошибка скачивания фото реального юзера: {e}")

                identity_index += 1
            else:
                if generated_names:
                    full_name = generated_names.pop(0)
                    parts = full_name.split(' ', 1)
                    first_name = parts[0].replace('"', '').replace("'", "")
                    last_name = (parts[1] if len(parts) > 1 else "").replace('"', '').replace("'", "")
                else:
                    first_name = f"User{random.randint(100, 999)}"
                    last_name = ""

                if not got_photo and openai_key:
                    try:
                        image_model = get_model_setting(current_settings, "openai_image")
                        logger.info(f"🎨 Генерирую аватар через OpenAI ({image_model}) для {client_wrapper.session_name}...")
                        openai_client = openai.AsyncOpenAI(api_key=openai_key)
                        dalle_prompt = f"Avatar profile picture for social media, topic: {raw_source}, style: realistic, high quality, professional headshot"

                        for model_to_try in [image_model, "dall-e-3"]:
                            if not model_to_try:
                                continue
                            try:
                                params = {"model": model_to_try, "prompt": dalle_prompt, "size": "1024x1024", "n": 1}
                                if model_to_try == "dall-e-3":
                                    params["quality"] = "standard"
                                response = await openai_client.images.generate(**params)

                                image_item = response.data[0]
                                b64_json = getattr(image_item, "b64_json", None)
                                image_url = getattr(image_item, "url", None)

                                if b64_json:
                                    image_bytes = base64.b64decode(b64_json)
                                    with open(photo_path, 'wb') as f:
                                        f.write(image_bytes)
                                    got_photo = True
                                    break

                                if image_url:
                                    async with httpx.AsyncClient(timeout=30.0) as http_client:
                                        resp = await http_client.get(image_url)
                                        if resp.status_code == 200:
                                            with open(photo_path, 'wb') as f:
                                                f.write(resp.content)
                                            got_photo = True
                                            break
                            except Exception:
                                continue
                    except Exception:
                        pass

                if not got_photo:
                    encoded_query = urllib.parse.quote(source_val)
                    seed = random.randint(1, 9999999)
                    url = f"https://image.pollinations.ai/prompt/avatar%20{encoded_query}%20{seed}?width=500&height=500&nologo=true&model=flux"

                    async with httpx.AsyncClient(timeout=60.0) as http_client:
                        resp = await http_client.get(url)
                        if resp.status_code == 200:
                            with open(photo_path, 'wb') as f:
                                f.write(resp.content)
                            got_photo = True

                    await asyncio.sleep(3)

            await update_account_profile(
                client_wrapper.client,
                first_name=first_name,
                last_name=last_name,
                avatar_path=photo_path if got_photo else None,
            )
            success = True

            if success and current_identity_user_id:
                try:
                    with _db_connect() as conn:
                        conn.execute("INSERT OR IGNORE INTO used_identities (user_id, date_used) VALUES (?, ?)",
                                     (current_identity_user_id, datetime.now(timezone.utc).isoformat()))
                        conn.commit()
                except Exception as db_e:
                    logger.error(f"Ошибка сохранения использованного ID {current_identity_user_id}: {db_e}")

            if got_photo and os.path.exists(photo_path):
                os.remove(photo_path)

            logger.info(f"✅ Аккаунт {client_wrapper.session_name} обновлен: {first_name} {last_name}")

            await asyncio.sleep(random.randint(5, 12))

        except Exception as e:
            logger.error(f"Ошибка ребрендинга для {client_wrapper.session_name}: {repr(e)}")

    current_settings['rebrand_task']['status'] = 'completed'
    save_json(SETTINGS_FILE, current_settings)
    logger.info("🏁 Задача по ребрендингу завершена.")


async def proxy_auto_checker(bot_token, owner_ids):
    while True:
        with _db_connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, url FROM proxies")
            proxies = cursor.fetchall()
            for p_id, url in proxies:
                res = await check_proxy_health(url)
                cursor.execute("UPDATE proxies SET status = ?, last_check = ?, ip = ?, country = ? WHERE id = ?",
                               (res['status'], datetime.now().isoformat(), res['ip'], res['country'], p_id))
                if res['status'] == 'dead':
                    timeout = httpx.Timeout(20.0, connect=10.0)
                    async with httpx.AsyncClient(timeout=timeout) as client:
                        for oid in owner_ids:
                            try:
                                safe_url = str(url).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                                text = f"⚠️ <b>Прокси не работает!</b>\nURL: <code>{safe_url}</code>"
                                await client.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",
                                                  json={
                                                      "chat_id": oid,
                                                      "text": text,
                                                      "parse_mode": "HTML",
                                                      "link_preview_options": {"is_disabled": True},
                                                  })
                            except: pass
            conn.commit()
        await asyncio.sleep(86400)


async def process_scenarios():
    global active_clients, current_settings, SCENARIO_CONTEXT

    if not hasattr(process_scenarios, "last_log_time"):
        process_scenarios.last_log_time = {}

    if not hasattr(process_scenarios, "msg_history"):
        process_scenarios.msg_history = {}

    tasks_to_process = []

    try:
        with _db_connect() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT ps.id, CAST(ps.chat_id AS TEXT) as chat_id, ps.post_id, ps.current_index, ps.last_run_time, s.script_content 
                FROM post_scenarios ps
                JOIN scenarios s ON CAST(ps.chat_id AS TEXT) = CAST(s.chat_id AS TEXT)
                WHERE s.status != 'stopped'
            """)
            rows = cursor.fetchall()
            for row in rows:
                tasks_to_process.append(dict(row))
    except Exception as e:
        logger.error(f"❌ Ошибка чтения БД сценариев: {e}")
        return

    if not tasks_to_process:
        return

    accounts_data = load_project_accounts(current_settings)
    ordered_accounts = [acc for acc in accounts_data if _is_account_active(acc)]

    all_targets = (current_settings.get('targets', []) or []) if isinstance(current_settings, dict) else []

    for task in tasks_to_process:
        row_id = task['id']
        chat_id_str = task['chat_id']
        post_id = task['post_id']
        idx = task['current_index']
        last_run = task['last_run_time']
        content = task['script_content']

        target_settings = None
        for t in get_project_targets(current_settings):
            t_id = str(t.get('chat_id'))
            if t_id == chat_id_str or t_id.replace('-100', '') == chat_id_str.replace('-100', ''):
                target_settings = t
                break

        if not target_settings:
            has_any_target = False
            for t in all_targets:
                t_id = str(t.get('chat_id'))
                if t_id == chat_id_str or t_id.replace('-100', '') == chat_id_str.replace('-100', ''):
                    has_any_target = True
                    break
            if not has_any_target:
                with _db_connect() as conn:
                    conn.execute("DELETE FROM post_scenarios WHERE id = ?", (row_id,))
                _scenario_history_clear(chat_id_str, post_id)
            continue

        destination_id_str = target_settings.get('linked_chat_id', target_settings.get('chat_id'))

        lines = [l.strip() for l in content.split('\n') if l.strip()]

        if idx >= len(lines):
            with _db_connect() as conn:
                conn.execute("DELETE FROM post_scenarios WHERE id = ?", (row_id,))

            hist_key = f"{chat_id_str}_{post_id}"
            if hist_key in process_scenarios.msg_history:
                del process_scenarios.msg_history[hist_key]
            _scenario_history_clear(chat_id_str, post_id)

            logger.info(f"🏁 Сценарий для поста {post_id} завершен.")
            continue

        line = lines[idx]

        match = re.search(r'\[(\d+)\]\s*[\|¦]?\s*([\d\.,]+)\s*[-–—]\s*([\d\.,]+)[сcCcSsа-яА-Яa-zA-Z]*\s*[\|¦]?\s*(.+)',
                          line)

        if not match:
            logger.warning(f"⚠️ [SKIP] Неверный формат строки {idx + 1}: '{line}'")
            with _db_connect() as conn:
                conn.execute("UPDATE post_scenarios SET current_index = current_index + 1 WHERE id = ?", (row_id,))
            continue

        acc_idx_raw = int(match.group(1))
        min_delay = float(match.group(2).replace(',', '.'))
        max_delay = float(match.group(3).replace(',', '.'))
        text = match.group(4).strip()

        time_passed = time.time() - last_run
        log_key = f"{row_id}_{idx}"

        if time_passed < min_delay:
            if time.time() - process_scenarios.last_log_time.get(log_key, 0) > 10:
                logger.info(f"⏳ [WAIT] Пост {post_id}: Шаг {idx + 1}. Ждем еще {min_delay - time_passed:.1f}с")
                process_scenarios.last_log_time[log_key] = time.time()
            continue

        if log_key in process_scenarios.last_log_time:
            del process_scenarios.last_log_time[log_key]

        logger.info(f"🚀 [START] Пост {post_id}: Начинаю выполнение шага {idx + 1}...")

        acc_id = acc_idx_raw - 1
        client_wrapper = None
        session_name = "Unknown"

        if 0 <= acc_id < len(ordered_accounts):
            session_name = ordered_accounts[acc_id]['session_name']
            client_wrapper = active_clients.get(session_name)

        if not client_wrapper:
            if active_clients:
                client_wrapper = random.choice(list(active_clients.values()))
                session_name = client_wrapper.session_name
                logger.warning(f"⚠️ Аккаунт {acc_idx_raw} недоступен, подменил на {session_name}")
            else:
                logger.error("❌ Нет активных клиентов для сценария.")
                with _db_connect() as conn:
                    conn.execute("UPDATE post_scenarios SET current_index = current_index + 1 WHERE id = ?", (row_id,))
                continue

        try:
            hist_key = f"{chat_id_str}_{post_id}"
            if hist_key not in process_scenarios.msg_history:
                process_scenarios.msg_history[hist_key] = _scenario_history_load(chat_id_str, post_id)

            reply_to_id = post_id
            use_reply_mode = target_settings.get('scenario_reply_mode', False)

            tags = re.findall(r'\{(\d+)\}', text)
            for t_num in tags:
                ref_idx = int(t_num)

                if ref_idx in process_scenarios.msg_history[hist_key]:
                    reply_to_id = process_scenarios.msg_history[hist_key][ref_idx]

                text = text.replace(f"{{{t_num}}}", "")
                text = re.sub(f"@{re.escape('{' + t_num + '}')}", "", text)

            text = " ".join(text.split())

            if not use_reply_mode and not tags:
                reply_to_id = None

            logger.info(f"🔍 [{session_name}] Ищу чат {destination_id_str}...")
            norm_dest_id = int(str(destination_id_str).replace('-100', ''))

            try:
                entity = await asyncio.wait_for(
                    client_wrapper.client.get_input_entity(norm_dest_id),
                    timeout=15.0
                )
            except asyncio.TimeoutError:
                logger.error(f"❌ [{session_name}] Тайм-аут поиска чата. Пропускаю шаг.")
                with _db_connect() as conn:
                    conn.execute("UPDATE post_scenarios SET current_index = current_index + 1 WHERE id = ?", (row_id,))
                continue
            except Exception as e:
                try:
                    entity = await client_wrapper.client.get_entity(norm_dest_id)
                except:
                    logger.error(f"❌ [{session_name}] Чат не найден: {e}")
                    with _db_connect() as conn:
                        conn.execute("UPDATE post_scenarios SET current_index = current_index + 1 WHERE id = ?",
                                     (row_id,))
                    continue

            wait_real = random.uniform(0, max(0, max_delay - min_delay))
            if wait_real > 0:
                logger.info(f"⏱ [{session_name}] Пауза перед вводом {wait_real:.1f}с...")
                await asyncio.sleep(wait_real)

            logger.info(f"✍️ [{session_name}] Печатает сообщение...")

            sent_msg = await human_type_and_send(client_wrapper.client, entity, text, reply_to_msg_id=reply_to_id, skip_processing=True)

            if sent_msg:
                logger.info(f"✅ [{session_name}] УСПЕШНО отправил: {text[:20]}...")

                process_scenarios.msg_history[hist_key][acc_idx_raw] = sent_msg.id
                _scenario_history_set(chat_id_str, post_id, acc_idx_raw, sent_msg.id)

                me = await client_wrapper.client.get_me()
                log_action_to_db({
                    'type': 'comment',
                    'post_id': post_id,
                    'comment': f"[SCENARIO STEP {idx + 1}] {text}",
                    'date': datetime.now(timezone.utc).isoformat(),
                    'account': {'session_name': session_name, 'first_name': me.first_name, 'username': me.username},
                    'target': {'chat_name': 'Scenario', 'destination_chat_id': destination_id_str}
                })

            with _db_connect() as conn:
                conn.execute(
                    "UPDATE post_scenarios SET current_index = current_index + 1, last_run_time = ? WHERE id = ?",
                    (time.time(), row_id))

        except Exception as e:
            logger.error(f"❌ Ошибка выполнения шага (Post {post_id}): {e}", exc_info=True)
            with _db_connect() as conn:
                conn.execute("UPDATE post_scenarios SET current_index = current_index + 1 WHERE id = ?", (row_id,))


async def process_outbound_queue():
    global active_clients
    try:
        project_sessions = {
            a.get("session_name") for a in load_project_accounts(current_settings) if a.get("session_name")
        }
        with _db_connect() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM outbound_queue WHERE status = 'pending'")
            tasks = cursor.fetchall()

        if not tasks:
            return

        for task in tasks:
            t_id = task['id']
            session_name = task['session_name']
            chat_id_str = task['chat_id']
            reply_id = task['reply_to_msg_id']
            text = task['text']
            if session_name not in project_sessions:
                continue

            client_wrapper = active_clients.get(session_name)
            temp_client = None
            client = client_wrapper.client if client_wrapper else None
            if client is None:
                try:
                    telethon_config = load_config('telethon_credentials')
                    api_id, api_hash = int(telethon_config['api_id']), telethon_config['api_hash']
                    accounts_data = load_project_accounts(current_settings)
                    account_data = next((a for a in accounts_data if a.get('session_name') == session_name), None)
                    if not account_data:
                        raise KeyError("account_not_found")
                    temp_client = await _connect_temp_client(account_data, api_id, api_hash)
                    client = temp_client
                except Exception as e:
                    with _db_connect() as conn:
                        conn.execute("UPDATE outbound_queue SET status = 'failed_no_client' WHERE id = ?", (t_id,))
                    kind = "quote" if reply_id else "dm"
                    with _db_connect() as conn:
                        cur = conn.cursor()
                        cur.execute(
                            """
                            UPDATE inbox_messages
                            SET status='error', error=?
                            WHERE id = (
                              SELECT id
                              FROM inbox_messages
                              WHERE kind=? AND direction='out' AND status='queued'
                                AND session_name=? AND chat_id=? AND text=?
                              ORDER BY id DESC
                              LIMIT 1
                            )
                            """,
                            (f"no_client:{e}", kind, session_name, str(chat_id_str), text),
                        )
                        if cur.rowcount == 0:
                            conn.execute(
                                """
                                INSERT INTO inbox_messages (
                                  kind, direction, status, created_at,
                                  session_name, chat_id, reply_to_msg_id,
                                  text, is_read, error
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    kind,
                                    "out",
                                    "error",
                                    datetime.now(timezone.utc).isoformat(),
                                    session_name,
                                    str(chat_id_str),
                                    reply_id,
                                    text,
                                    1,
                                    f"no_client:{e}",
                                ),
                            )
                        conn.commit()
                    continue

            try:
                dest_chat = int(str(chat_id_str).replace('-100', ''))
                entity = await client.get_input_entity(dest_chat)

                sent_msg = await client.send_message(entity, text, reply_to=reply_id)
                logger.info(f"✅ Ручной ответ отправлен от {session_name} в {dest_chat}")

                with _db_connect() as conn:
                    conn.execute("UPDATE outbound_queue SET status = 'sent' WHERE id = ?", (t_id,))

                # Mark the queued row (if any) as sent; otherwise insert a fresh row.
                now = datetime.now(timezone.utc).isoformat()
                kind = "quote" if reply_id else "dm"
                msg_id = getattr(sent_msg, "id", None) if sent_msg else None
                with _db_connect() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        UPDATE inbox_messages
                        SET status='sent', msg_id=?, reply_to_msg_id=?, error=NULL
                        WHERE id = (
                          SELECT id
                          FROM inbox_messages
                          WHERE kind=? AND direction='out' AND status='queued'
                            AND session_name=? AND chat_id=? AND text=?
                          ORDER BY id DESC
                          LIMIT 1
                        )
                        """,
                        (msg_id, reply_id, kind, session_name, str(chat_id_str), text),
                    )
                    if cur.rowcount == 0:
                        conn.execute(
                            """
                            INSERT INTO inbox_messages (
                              kind, direction, status, created_at,
                              session_name, chat_id, msg_id, reply_to_msg_id,
                              text, is_read
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (kind, "out", "sent", now, session_name, str(chat_id_str), msg_id, reply_id, text, 1),
                        )
                    conn.commit()

            except Exception as e:
                logger.error(f"Ошибка отправки ручного ответа (ID {t_id}): {e}")
                with _db_connect() as conn:
                    conn.execute("UPDATE outbound_queue SET status = 'error' WHERE id = ?", (t_id,))
                kind = "quote" if reply_id else "dm"
                with _db_connect() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        UPDATE inbox_messages
                        SET status='error', error=?
                        WHERE id = (
                          SELECT id
                          FROM inbox_messages
                          WHERE kind=? AND direction='out' AND status='queued'
                            AND session_name=? AND chat_id=? AND text=?
                          ORDER BY id DESC
                          LIMIT 1
                        )
                        """,
                        (str(e), kind, session_name, str(chat_id_str), text),
                    )
                    if cur.rowcount == 0:
                        conn.execute(
                            """
                            INSERT INTO inbox_messages (
                              kind, direction, status, created_at,
                              session_name, chat_id, msg_id, reply_to_msg_id,
                              text, is_read, error
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                kind,
                                "out",
                                "error",
                                datetime.now(timezone.utc).isoformat(),
                                session_name,
                                str(chat_id_str),
                                None,
                                reply_id,
                                text,
                                1,
                                str(e),
                            ),
                        )
                    conn.commit()
            finally:
                if temp_client is not None:
                    try:
                        if temp_client.is_connected():
                            await temp_client.disconnect()
                    except Exception:
                        pass

    except Exception as e:
        logger.error(f"Ошибка в outbound_queue: {e}")


async def process_discussion_start_queue():
    global current_settings, active_clients

    queue = current_settings.get("discussion_start_queue")
    if not isinstance(queue, list) or not queue:
        return

    tasks = get_project_discussion_start_queue(current_settings)
    if not tasks:
        return

    now_ts = time.time()
    tasks_to_remove: list[dict] = []
    tasks_updated = False

    def _should_try_other_discussion_chat(exc: Exception) -> bool:
        name = (exc.__class__.__name__ or "").lower()
        text = str(exc).lower()
        tokens = [
            "chatadminrequired",
            "chat admin privileges",
            "chat_admin_required",
            "chatwriteforbidden",
            "chat_write_forbidden",
            "chat_send_plain_forbidden",
            "chat_send",
            "write forbidden",
            "you can't write",
            "cannot write",
            "not enough rights",
            "sendmessagerequest",
            "channelprivate",
            "channel private",
            "channelinvalid",
            "channel invalid",
            "userbannedinchannel",
            "user banned in channel",
        ]
        return any(t in name or t in text for t in tokens)

    for task in tasks:
        try:
            try:
                next_retry_at = float(task.get("next_retry_at") or 0.0)
            except Exception:
                next_retry_at = 0.0
            if next_retry_at and now_ts < next_retry_at:
                continue

            target_id = str(task.get("discussion_target_id") or "").strip()
            target_chat_id = str(task.get("discussion_target_chat_id") or "").strip()
            seed_text = str(task.get("seed_text") or "").strip()
            operator_session = str(task.get("operator_session_name") or "").strip()
            force_restart = bool(task.get("force_restart", False))

            if not ((target_id or target_chat_id) and seed_text and operator_session):
                tasks_to_remove.append(task)
                continue

            all_targets = get_project_discussion_targets(current_settings)
            target: dict | None = None
            if target_id:
                for t in all_targets:
                    if str(t.get("id") or "").strip() == target_id:
                        target = t
                        break
            else:
                chat_matches = [
                    t for t in all_targets if str(t.get("chat_id") or "").strip() == target_chat_id
                ]
                if len(chat_matches) == 1:
                    target = chat_matches[0]
                elif len(chat_matches) > 1:
                    logger.warning(
                        f"⚠️ [discussion_start] ambiguous target for chat_id={target_chat_id}: need discussion_target_id in queue task"
                    )
                    tasks_to_remove.append(task)
                    continue

            if not target or not bool(target.get("enabled", True)):
                tasks_to_remove.append(task)
                continue

            if not target_id:
                target_id = str(target.get("id") or "").strip()
            if not target_chat_id:
                target_chat_id = str(target.get("chat_id") or "").strip()

            # Prefer the current operator setting from the target if present.
            operator_from_target = str(target.get("operator_session_name") or "").strip()
            if operator_from_target:
                operator_session = operator_from_target

            session_id_int: int | None
            try:
                session_id_int = int(task.get("session_id") or 0) or None
            except Exception:
                session_id_int = None

            if session_id_int is None:
                try:
                    project_id = _active_project_id(current_settings)
                except Exception:
                    project_id = DEFAULT_PROJECT_ID

                base_chat_id = (
                    str(task.get("chat_id") or "").strip()
                    or str(target.get("linked_chat_id") or "").strip()
                    or str(target.get("chat_id") or "").strip()
                )
                session_id_int = _db_create_discussion_session(
                    project_id=str(project_id),
                    discussion_target_id=str(target_id) or None,
                    discussion_target_chat_id=str(target_chat_id),
                    chat_id=str(base_chat_id or target_chat_id),
                    status="planned",
                    operator_session_name=operator_session,
                    seed_text=seed_text,
                    settings={"target": target},
                )
                if session_id_int:
                    task["session_id"] = int(session_id_int)
                    tasks_updated = True

            client_wrapper = active_clients.get(operator_session)
            temp_client = None
            client = None

            if client_wrapper is not None and getattr(client_wrapper, "client", None) is not None:
                if not await ensure_client_connected(client_wrapper, reason="discussion_start"):
                    raise RuntimeError("connect_failed")
                client = client_wrapper.client
            else:
                telethon_config = load_config("telethon_credentials")
                api_id, api_hash = int(telethon_config["api_id"]), telethon_config["api_hash"]
                accounts_data = load_project_accounts(current_settings)
                account_data = next(
                    (a for a in accounts_data if str(a.get("session_name") or "").strip() == operator_session),
                    None,
                )
                if not account_data:
                    raise KeyError("operator_account_not_found")
                temp_client = await _connect_temp_client(account_data, api_id, api_hash)
                client = temp_client

            try:
                # Ensure the operator can write in the discussion chat (best-effort).
                try:
                    if client_wrapper is not None and getattr(client_wrapper, "client", None) is not None:
                        await ensure_account_joined(client_wrapper, target, force=True)
                    else:
                        pseudo = type("_Tmp", (), {"session_name": operator_session, "client": client})()
                        await ensure_account_joined(pseudo, target, force=True)
                except Exception:
                    pass

                candidate_raw = [
                    str(task.get("chat_id") or "").strip(),
                    str(target.get("linked_chat_id") or "").strip(),
                    str(target.get("chat_id") or "").strip(),
                ]
                candidate_chat_ids: list[int] = []
                seen: set[int] = set()
                for raw in candidate_raw:
                    if not raw:
                        continue
                    try:
                        cid = int(raw)
                    except Exception:
                        continue
                    if cid in seen:
                        continue
                    seen.add(cid)
                    candidate_chat_ids.append(cid)

                if not candidate_chat_ids:
                    tasks_to_remove.append(task)
                    continue

                # Prevent double-start when the seed message we send triggers the outbound message handler.
                suppressed_chat_bare_ids: list[int] = []
                try:
                    for cid in candidate_chat_ids:
                        bare = _channel_bare_id(str(cid))
                        if bare is None:
                            continue
                        b = int(bare)
                        suppressed_chat_bare_ids.append(b)
                        DISCUSSION_START_SUPPRESS_CHAT_IDS.add(b)
                except Exception:
                    suppressed_chat_bare_ids = []

                sent_msg = None
                sent_chat_id_int: int | None = None
                last_send_exc: Exception | None = None
                try:
                    for idx, cid in enumerate(candidate_chat_ids):
                        try:
                            sent_msg = await asyncio.wait_for(
                                client.send_message(int(cid), seed_text),
                                timeout=35.0,
                            )
                            sent_chat_id_int = int(cid)
                            break
                        except Exception as exc:
                            last_send_exc = exc
                            if idx < (len(candidate_chat_ids) - 1) and _should_try_other_discussion_chat(exc):
                                continue
                            raise
                finally:
                    for b in suppressed_chat_bare_ids:
                        try:
                            DISCUSSION_START_SUPPRESS_CHAT_IDS.discard(int(b))
                        except Exception:
                            pass

                if sent_msg is None or sent_chat_id_int is None:
                    raise last_send_exc or RuntimeError("send_failed")

                try:
                    seed_msg_id = int(getattr(sent_msg, "id", None) or 0) or None
                except Exception:
                    seed_msg_id = None
                if not seed_msg_id:
                    raise RuntimeError("missing_msg_id")

                start_prefix = str(target.get("start_prefix") or "")
                seed_clean = (
                    _extract_discussion_seed_optional_prefix(seed_text, start_prefix) or seed_text
                ).strip()

                chat_bare_id = _channel_bare_id(str(sent_chat_id_int))
                if chat_bare_id is None:
                    chat_bare_id = int(str(sent_chat_id_int).replace("-100", "").replace("-", ""))

                if force_restart:
                    existing = DISCUSSION_ACTIVE_TASKS.get(int(chat_bare_id))
                    if existing is not None and not existing.done():
                        try:
                            prev_sid = getattr(existing, "discussion_session_id", None)
                            if prev_sid:
                                _db_update_discussion_session(
                                    int(prev_sid),
                                    status="canceled",
                                    finished_at=float(time.time()),
                                    error="force_restart",
                                )
                        except Exception:
                            pass
                        existing.cancel()
                        try:
                            await asyncio.wait_for(existing, timeout=2.0)
                        except Exception:
                            pass
                        DISCUSSION_ACTIVE_TASKS.pop(int(chat_bare_id), None)

                if session_id_int:
                    try:
                        _db_update_discussion_session(
                            int(session_id_int),
                            discussion_target_id=str(target_id) or None,
                            status="running",
                            started_at=float(time.time()),
                            chat_id=str(sent_chat_id_int),
                            operator_session_name=operator_session,
                            seed_msg_id=int(seed_msg_id),
                            seed_text=seed_clean,
                            schedule_at=None,
                            error=None,
                        )
                        _db_add_discussion_message(
                            session_id=int(session_id_int),
                            speaker_type="operator",
                            speaker_session_name=operator_session,
                            speaker_label="Оператор",
                            msg_id=int(seed_msg_id),
                            reply_to_msg_id=None,
                            text=str(seed_text),
                        )
                    except Exception:
                        pass

                _schedule_discussion_run(
                    chat_bare_id=int(chat_bare_id),
                    chat_id=int(sent_chat_id_int),
                    seed_msg_id=int(seed_msg_id),
                    seed_text=seed_clean,
                    target=target,
                    session_id=int(session_id_int) if session_id_int else None,
                )
                logger.info(
                    f"🗣 [discussion_start] operator {operator_session} sent msg_id={seed_msg_id} in {chat_bare_id}"
                )
                tasks_to_remove.append(task)
            finally:
                if temp_client is not None:
                    try:
                        if temp_client.is_connected():
                            await temp_client.disconnect()
                    except Exception:
                        pass
        except Exception as e:
            tries = 0
            try:
                tries = int(task.get("tries", 0) or 0)
            except Exception:
                tries = 0
            tries += 1
            task["tries"] = tries
            task["last_error"] = str(e)
            backoff = min(60 * max(1, tries), 600)
            next_retry_at = float(time.time() + backoff)
            task["next_retry_at"] = next_retry_at
            tasks_updated = True
            logger.error(f"❌ [discussion_start] ошибка: {e} (retry in {backoff}s)")

            max_tries = 10
            sid_raw = task.get("session_id")
            sid_int = None
            try:
                sid_int = int(sid_raw) if sid_raw else None
            except Exception:
                sid_int = None
            if sid_int:
                try:
                    if tries >= max_tries:
                        _db_update_discussion_session(
                            int(sid_int),
                            status="failed",
                            finished_at=float(time.time()),
                            error=str(e),
                        )
                    else:
                        _db_update_discussion_session(
                            int(sid_int),
                            status="planned",
                            schedule_at=float(next_retry_at),
                            error=str(e),
                        )
                except Exception:
                    pass
            if tries >= max_tries:
                tasks_to_remove.append(task)

    if tasks_to_remove:
        new_queue = [t for t in queue if t not in tasks_to_remove]
        current_settings["discussion_start_queue"] = new_queue
        tasks_updated = True

    if tasks_updated:
        save_data(SETTINGS_FILE, current_settings)


async def process_discussion_queue():
    global current_settings

    queue = current_settings.get("discussion_queue")
    if not isinstance(queue, list) or not queue:
        return

    tasks = get_project_discussion_queue(current_settings)
    if not tasks:
        return

    tasks_to_remove: list[dict] = []

    for task in tasks:
        try:
            target_id = str(task.get("discussion_target_id") or "").strip()
            target_chat_id = str(task.get("discussion_target_chat_id") or "").strip()
            chat_id_raw = str(task.get("chat_id") or "").strip()
            seed_text = str(task.get("seed_text") or "").strip()
            seed_msg_id_raw = task.get("seed_msg_id")

            if not ((target_id or target_chat_id) and chat_id_raw and seed_text and seed_msg_id_raw):
                tasks_to_remove.append(task)
                continue

            all_targets = get_project_discussion_targets(current_settings)
            target: dict | None = None
            if target_id:
                for t in all_targets:
                    if str(t.get("id") or "").strip() == target_id:
                        target = t
                        break
            else:
                chat_matches = [
                    t for t in all_targets if str(t.get("chat_id") or "").strip() == target_chat_id
                ]
                if len(chat_matches) == 1:
                    target = chat_matches[0]
                elif len(chat_matches) > 1:
                    logger.warning(
                        f"⚠️ [discussion_queue] ambiguous target for chat_id={target_chat_id}: need discussion_target_id in queue task"
                    )
                    tasks_to_remove.append(task)
                    continue

            if not target:
                tasks_to_remove.append(task)
                continue

            try:
                chat_id_int = int(chat_id_raw)
            except Exception:
                tasks_to_remove.append(task)
                continue

            try:
                seed_msg_id = int(seed_msg_id_raw)
            except Exception:
                tasks_to_remove.append(task)
                continue

            try:
                chat_bare_id = int(str(chat_id_int).replace("-100", ""))
            except Exception:
                chat_bare_id = chat_id_int

            _schedule_discussion_run(
                chat_bare_id=chat_bare_id,
                chat_id=chat_id_int,
                seed_msg_id=seed_msg_id,
                seed_text=seed_text,
                target=target,
            )
            tasks_to_remove.append(task)
        except Exception as e:
            logger.error(f"Ошибка в discussion_queue: {e}")
            tasks_to_remove.append(task)

    if tasks_to_remove:
        new_queue = [t for t in queue if t not in tasks_to_remove]
        current_settings["discussion_queue"] = new_queue
        save_data(SETTINGS_FILE, current_settings)


async def process_manual_tasks():
    global current_settings, active_clients, POST_PROCESS_CACHE

    tasks = _claim_project_manual_tasks(_active_project_id(current_settings), limit=100)
    if not tasks:
        return

    now_ts = time.time()
    last_log_at = getattr(process_manual_tasks, "_last_summary_log_at", 0.0)
    last_count = getattr(process_manual_tasks, "_last_summary_count", None)
    if last_count != len(tasks) or (now_ts - last_log_at) >= 60.0:
        logger.info(f"🚀 [MANUAL] Найдено {len(tasks)} ручных задач на обработку...")
        process_manual_tasks._last_summary_log_at = now_ts
        process_manual_tasks._last_summary_count = len(tasks)

    for task in tasks:
        task_id = int(task.get("id") or 0)
        if not isinstance(task, dict):
            _set_manual_task_status(task_id, "failed", "manual_task_invalid_payload")
            continue

        target_chat_id_raw = str(task.get("chat_id") or "").strip()
        message_chat_id_raw = str(task.get("message_chat_id") or "").strip() or target_chat_id_raw
        post_id = task.get("post_id")
        if not target_chat_id_raw or not post_id:
            _set_manual_task_status(task_id, "failed", "manual_task_missing_chat_or_post")
            continue

        target_chat = None
        for t in get_project_targets(current_settings):
            if str(t.get("chat_id") or "").strip() == target_chat_id_raw:
                target_chat = t
                break
        if not target_chat:
            # Backward compatibility: some tasks might store linked_chat_id in chat_id.
            for t in get_project_targets(current_settings):
                if str(t.get("linked_chat_id") or "").strip() == target_chat_id_raw:
                    target_chat = t
                    break

        if not target_chat:
            _set_manual_task_status(task_id, "failed", f"target_not_found:{target_chat_id_raw}")
            continue

        effective_target_chat = target_chat
        overrides = task.get("overrides") if isinstance(task, dict) else None
        if isinstance(overrides, dict) and overrides:
            effective_target_chat = dict(target_chat)
            for key in [
                "vector_prompt",
                "accounts_per_post_min",
                "accounts_per_post_max",
                "delay_between_accounts",
                "daily_comment_limit",
            ]:
                if key in overrides and overrides.get(key) is not None:
                    effective_target_chat[key] = overrides.get(key)
            try:
                logger.info(
                    f"⚙️ [MANUAL] Overrides применены (keys={list(overrides.keys())}) для post_id={post_id} target={target_chat_id_raw}"
                )
            except Exception:
                pass

        eligible_clients = [
            c
            for c in list(active_clients.values())
            if _is_account_assigned(effective_target_chat, c.session_name)
            and getattr(c, "client", None) is not None
            and c.client.is_connected()
        ]

        if not eligible_clients:
            # Try to reconnect assigned accounts once (backoff applies).
            assigned = [
                c
                for c in list(active_clients.values())
                if _is_account_assigned(effective_target_chat, c.session_name)
                and getattr(c, "client", None) is not None
            ]
            for c in assigned:
                await ensure_client_connected(c, reason="manual")

            eligible_clients = [
                c
                for c in assigned
                if getattr(c, "client", None) is not None and c.client.is_connected()
            ]

        if not eligible_clients:
            last_warn_map = getattr(process_manual_tasks, "_last_no_clients_warn", None)
            if not isinstance(last_warn_map, dict):
                last_warn_map = {}
                process_manual_tasks._last_no_clients_warn = last_warn_map
            last_warn_at = float(last_warn_map.get(str(target_chat_id_raw)) or 0.0)
            if (now_ts - last_warn_at) >= 60.0:
                logger.warning(f"⚠️ Нет подключенных клиентов для ручной задачи в {target_chat_id_raw}")
                last_warn_map[str(target_chat_id_raw)] = now_ts
            _set_manual_task_status(task_id, "pending", "no_connected_clients")
            continue

        client_wrapper = random.choice(eligible_clients)

        try:
            destination_chat_id = int(str(message_chat_id_raw))
            try:
                entity = await client_wrapper.client.get_input_entity(destination_chat_id)
            except Exception:
                await ensure_account_joined(client_wrapper, effective_target_chat, force=True)
                entity = await client_wrapper.client.get_input_entity(destination_chat_id)

            messages = await client_wrapper.client.get_messages(entity, ids=[post_id])
            if messages and messages[0]:
                msg = messages[0]

                final_chat_id = destination_chat_id

                # If we fetched the message from the main channel but the target has a linked discussion chat,
                # re-map to the linked chat message so comments go to the correct place.
                try:
                    should_map = False
                    linked_chat_id_cfg = str(effective_target_chat.get("linked_chat_id") or "").strip()
                    target_chat_id_cfg = str(effective_target_chat.get("chat_id") or "").strip()
                    if (
                        linked_chat_id_cfg
                        and target_chat_id_cfg
                        and str(destination_chat_id) == str(target_chat_id_cfg)
                        and str(linked_chat_id_cfg) != str(target_chat_id_cfg)
                    ):
                        should_map = True

                    if should_map:
                        discussion_res = await client_wrapper.client(
                            GetDiscussionMessageRequest(peer=entity, msg_id=post_id)
                        )
                        if discussion_res.messages:
                            found_msg = None
                            for m in discussion_res.messages:
                                try:
                                    if getattr(m, "chat_id", None) and int(getattr(m, "chat_id")) != int(
                                        destination_chat_id
                                    ):
                                        found_msg = m
                                        break
                                except Exception:
                                    continue
                            if not found_msg:
                                found_msg = discussion_res.messages[0]

                            linked_chat_id = getattr(found_msg, "chat_id", None) or destination_chat_id
                            linked_msg_id = getattr(found_msg, "id", None) or post_id

                            # Re-fetch message to ensure it's bound to the client (text/media access is more reliable).
                            refetched_msg = None
                            try:
                                linked_entity = await client_wrapper.client.get_input_entity(int(linked_chat_id))
                                refetched = await client_wrapper.client.get_messages(
                                    linked_entity, ids=[int(linked_msg_id)]
                                )
                                if refetched and refetched[0]:
                                    refetched_msg = refetched[0]
                            except Exception:
                                try:
                                    await ensure_account_joined(client_wrapper, effective_target_chat, force=True)
                                    linked_entity = await client_wrapper.client.get_input_entity(int(linked_chat_id))
                                    refetched = await client_wrapper.client.get_messages(
                                        linked_entity, ids=[int(linked_msg_id)]
                                    )
                                    if refetched and refetched[0]:
                                        refetched_msg = refetched[0]
                                except Exception:
                                    refetched_msg = None

                            msg = refetched_msg or found_msg
                            final_chat_id = getattr(msg, "chat_id", None) or linked_chat_id
                            logger.info(
                                f"🔄 [MANUAL] Переадресация: Пост Канала {post_id} -> Пост Группы {linked_msg_id}"
                            )
                except Exception as e:
                    logger.warning(
                        f"⚠️ [MANUAL] Не удалось найти Linked-сообщение (возможно нет комментариев): {e}"
                    )

                event_mock = collections.namedtuple("EventMock", ["message", "chat_id"])
                mock_event = event_mock(message=msg, chat_id=final_chat_id)

                logger.info(f"⚡ [MANUAL] Принудительный запуск обработки поста {msg.id} в {final_chat_id}")

                asyncio.create_task(
                    process_new_post(
                        mock_event,
                        effective_target_chat,
                        from_catch_up=False,
                        is_manual=True,
                    )
                )
                _set_manual_task_status(task_id, "done")
            else:
                logger.warning(f"❌ [MANUAL] Не удалось найти сообщение {post_id} в {message_chat_id_raw}")
                _set_manual_task_status(task_id, "failed", f"message_not_found:{post_id}@{message_chat_id_raw}")

        except Exception as e:
            logger.error(f"Ошибка ручной обработки поста: {e}")
            _set_manual_task_status(task_id, "pending", f"processing_error:{type(e).__name__}:{e}")

def _parse_proxy_url(url: str | None):
    if not url:
        return None
    try:
        protocol, rest = url.split("://", 1)
        auth, addr = rest.split("@", 1)
        user, password = auth.split(":", 1)
        host, port = addr.split(":", 1)
        return (protocol, host, int(port), True, user, password)
    except Exception:
        return None


async def _connect_temp_client(account_data: dict, api_id: int, api_hash: str):
    api_id, api_hash = _resolve_account_credentials(account_data, api_id, api_hash)
    if not api_id or not api_hash:
        raise RuntimeError("missing_api_credentials")
    proxy = _resolve_account_proxy(account_data)
    session = _resolve_account_session(account_data)
    if not session:
        raise RuntimeError("missing_session")
    client = TelegramClient(
        session,
        api_id,
        api_hash,
        proxy=proxy,
        **device_kwargs(account_data),
    )
    await client.connect()
    if not await client.is_user_authorized():
        try:
            await client.disconnect()
        except Exception:
            pass
        raise RuntimeError("account_not_authorized")
    return client


async def _clear_profile_photo(client: TelegramClient) -> None:
    try:
        photos = await client.get_profile_photos("me", limit=1)
    except Exception:
        photos = []
    if not photos:
        return
    try:
        input_photos = [tg_utils.get_input_photo(p) for p in photos if p]
        input_photos = [p for p in input_photos if p]
        if input_photos:
            await client(DeletePhotosRequest(id=input_photos))
    except Exception:
        pass


async def update_account_profile(
    client: TelegramClient,
    *,
    first_name: str | None = None,
    last_name: str | None = None,
    username: str | None = None,
    username_clear: bool = False,
    bio: str | None = None,
    avatar_path: str | None = None,
    avatar_clear: bool = False,
    personal_channel: str | None = None,
    personal_channel_clear: bool = False,
) -> None:
    if first_name is not None:
        first_name = str(first_name).strip()
        if not first_name:
            raise ValueError("first_name_empty")
    if last_name is not None:
        last_name = str(last_name).strip()
    if bio is not None:
        bio = str(bio).strip()
    if username is not None:
        username = str(username).strip()
        username = username.replace("https://t.me/", "").replace("http://t.me/", "").replace("t.me/", "")
        username = username.lstrip("@").strip().lower()
        if not username:
            username_clear = True

    if first_name is not None or last_name is not None or bio is not None:
        await client(
            UpdateProfileRequest(
                first_name=first_name,
                last_name=last_name,
                about=bio,
            )
        )

    if username_clear:
        await client(UpdateUsernameRequest(username=""))
    elif username is not None:
        await client(UpdateUsernameRequest(username=username))

    if avatar_clear:
        await _clear_profile_photo(client)

    if avatar_path:
        p = str(avatar_path).strip()
        if p and os.path.exists(p):
            uploaded = await client.upload_file(p)
            await client(UploadProfilePhotoRequest(file=uploaded))
        else:
            raise FileNotFoundError("avatar_file_not_found")

    if personal_channel_clear:
        await client(UpdatePersonalChannelRequest(channel=tl_types.InputChannelEmpty()))
    elif personal_channel:
        ref = str(personal_channel).strip().replace("@", "")
        if not ref:
            raise ValueError("personal_channel_empty")
        entity = await client.get_entity(ref)
        input_channel = tg_utils.get_input_channel(entity)
        await client(UpdatePersonalChannelRequest(channel=input_channel))


async def process_profile_tasks(api_id: int, api_hash: str) -> None:
    global current_settings, active_clients
    tasks = current_settings.get("profile_tasks")
    if not isinstance(tasks, dict) or not tasks:
        return

    accounts_data = load_project_accounts(current_settings)
    accounts_by_session = {
        a.get("session_name"): a for a in accounts_data if isinstance(a, dict) and a.get("session_name")
    }
    allowed_sessions = set(accounts_by_session.keys())

    for session_name, task in list(tasks.items()):
        if not isinstance(task, dict):
            continue
        if task.get("status") != "pending":
            continue
        if session_name not in allowed_sessions:
            continue

        task["status"] = "processing"
        task["started_at"] = datetime.now(timezone.utc).isoformat()
        task["error"] = ""
        save_json(SETTINGS_FILE, current_settings)

        temp_client = None
        try:
            account_data = accounts_by_session.get(session_name)
            if not account_data:
                raise KeyError("account_not_found")

            wrapper = active_clients.get(session_name)
            client = wrapper.client if wrapper else None
            if client is None:
                temp_client = await _connect_temp_client(account_data, api_id, api_hash)
                client = temp_client

            avatar_path = str(task.get("avatar_path") or "").strip() or None
            bio = task.get("bio")
            first_name = task.get("first_name")
            last_name = task.get("last_name")
            username = task.get("username")
            username_clear = bool(task.get("username_clear"))
            personal_channel = task.get("personal_channel")
            avatar_clear = bool(task.get("avatar_clear"))
            personal_channel_clear = bool(task.get("personal_channel_clear"))

            await update_account_profile(
                client,
                first_name=first_name,
                last_name=last_name,
                username=username,
                username_clear=username_clear,
                bio=bio,
                avatar_path=avatar_path,
                avatar_clear=avatar_clear,
                personal_channel=personal_channel,
                personal_channel_clear=personal_channel_clear,
            )

            me_username = None
            try:
                me = await client.get_me()
                if me:
                    account_data["user_id"] = getattr(me, "id", account_data.get("user_id"))
                    account_data["first_name"] = getattr(me, "first_name", account_data.get("first_name"))
                    account_data["last_name"] = getattr(me, "last_name", "") or ""
                    me_username = getattr(me, "username", None)
                    account_data["username"] = me_username or ""
            except Exception:
                pass

            if bio is not None:
                account_data["profile_bio"] = str(bio)
            if username is not None or username_clear:
                if username_clear:
                    account_data["profile_username"] = ""
                elif me_username is not None:
                    account_data["profile_username"] = me_username or ""
                else:
                    u = str(username or "").strip()
                    u = u.replace("https://t.me/", "").replace("http://t.me/", "").replace("t.me/", "")
                    u = u.lstrip("@").strip().lower()
                    account_data["profile_username"] = u
            if personal_channel_clear:
                account_data.pop("profile_personal_channel", None)
            elif personal_channel:
                account_data["profile_personal_channel"] = str(personal_channel)

            save_json(ACCOUNTS_FILE, accounts_data)

            task["status"] = "done"
            task["finished_at"] = datetime.now(timezone.utc).isoformat()
            task["error"] = ""
            save_json(SETTINGS_FILE, current_settings)

            if avatar_path and os.path.exists(avatar_path):
                try:
                    os.remove(avatar_path)
                except Exception:
                    pass
            logger.info(f"🧩 [{session_name}] профиль обновлён.")
        except Exception as e:
            task["status"] = "failed"
            task["finished_at"] = datetime.now(timezone.utc).isoformat()
            task["error"] = str(e)
            save_json(SETTINGS_FILE, current_settings)
            logger.error(f"🧩 [{session_name}] ошибка обновления профиля: {e}")
        finally:
            if temp_client is not None:
                try:
                    if temp_client.is_connected():
                        await temp_client.disconnect()
                except Exception:
                    pass


def save_data(file_path, data):
    try:
        save_json(file_path, data)
    except Exception as e:
        logger.error(f"Ошибка сохранения {file_path}: {e}")


async def main():
    global current_settings, active_clients, PENDING_TASKS
    ensure_data_dir()
    init_database()
    logger.info(">>> AI-Комментатор запускается...")
    try:
        telethon_config = load_config('telethon_credentials')
        api_id, api_hash = int(telethon_config['api_id']), telethon_config['api_hash']
    except Exception as e:
        logger.critical(f"Критическая ошибка конфигурации: {e}")
        return
    while True:
        try:
            new_settings = load_json_data(SETTINGS_FILE)
            current_settings = new_settings if isinstance(new_settings, dict) else {}
            ensure_role_schema(current_settings)
            if ensure_discussion_targets_schema(current_settings):
                try:
                    save_json(SETTINGS_FILE, current_settings)
                except Exception:
                    pass
            migrated_manual = _migrate_legacy_manual_queue_to_db()
            if migrated_manual:
                logger.info(f"✅ migrated {migrated_manual} legacy manual_queue tasks to manual_tasks")
            status = current_settings.get('status')
            if status == 'running':
                await manage_clients(api_id, api_hash)

                await process_discussion_start_queue()
                await process_discussion_queue()
                await process_profile_tasks(api_id, api_hash)
                await process_scenarios()
                await process_outbound_queue()
                await process_manual_tasks()

                if not active_clients:
                    logger.info("Статус: Работает, но нет активных аккаунтов (спят или не добавлены)")
                if 'rebrand_task' in current_settings and current_settings['rebrand_task'].get('status') == 'pending':
                    await run_rebrand_logic(api_id, api_hash)
            else:
                if PENDING_TASKS:
                    logger.info(f"⏳ Завершаю фоновые задачи ({len(PENDING_TASKS)})...")
                    for t in list(PENDING_TASKS):
                        t.cancel()
                    await asyncio.gather(*PENDING_TASKS, return_exceptions=True)
                if active_clients:
                    logger.info("Статус изменился на 'Остановлен'. Выключаю клиентов...")
                    for client in list(active_clients.values()):
                        await client.stop()
                    active_clients.clear()
                await process_profile_tasks(api_id, api_hash)
            await asyncio.sleep(2)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.critical(f"Критическая ошибка в главном цикле: {e}", exc_info=True)
            await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(main())

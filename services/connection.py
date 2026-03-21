"""Connection management, backoff logic, and account failure tracking.

Extracted from commentator.py — connection retry/backoff state machine,
soft timeout helper, account status management, join status helpers.
"""

import asyncio
import logging
import random
import time
from datetime import datetime
from typing import Any

from app_paths import ACCOUNTS_FILE
from app_storage import load_json, save_json
from db.connection import get_connection as _get_db_connection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DISABLED_ACCOUNT_STATUSES = {"banned", "frozen", "limited", "human_check", "unauthorized", "missing_session", "unavailable"}

JOIN_MAX_RETRIES = 5
JOIN_RETRY_BACKOFF = [0, 30, 120, 300, 900]

# Connection watchdog settings: keep the service self-healing when Telethon gives up reconnecting.
CONNECT_RETRY_BACKOFF_SECONDS = [0, 5, 15, 30, 60, 120, 300, 600]
CONNECT_ATTEMPT_TIMEOUT_SECONDS = 25.0
SEND_ATTEMPT_TIMEOUT_SECONDS = 35.0
CONNECT_FAILURE_LOG_INTERVAL_SECONDS = 60.0
CONNECT_MAX_RETRIES = 10  # After this many consecutive failures, mark account as unavailable

# Mutable connection state — shared across the process.
CLIENT_CONNECT_STATE = {}
CLIENT_CONNECT_LOCKS = {}


# ---------------------------------------------------------------------------
# DB helper
# ---------------------------------------------------------------------------

def _db_connect():
    """Get a database connection (sync context manager).

    Uses PostgreSQL when DB_URL is set, otherwise falls back to SQLite.
    """
    return _get_db_connection()


# ---------------------------------------------------------------------------
# Async timeout helper
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Connection backoff state machine
# ---------------------------------------------------------------------------

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


def _schedule_connect_backoff(session_name: str, *, error: str | None = None, reason: str | None = None) -> bool:
    """Schedule a retry backoff for a failed connection.

    Returns True if the account has exceeded CONNECT_MAX_RETRIES and should be
    marked unavailable (caller is responsible for persisting the status change).
    """
    if not session_name:
        return False
    now = time.time()
    state = _connect_state(session_name)
    attempts = int(state.get("attempts") or 0) + 1
    state["attempts"] = attempts
    state["last_error"] = error
    state["next_attempt_at"] = now + _connect_backoff_delay_seconds(attempts)
    if error:
        _record_account_failure(session_name, "connect", last_error=str(error), last_target=reason)
    if attempts >= CONNECT_MAX_RETRIES:
        logger.warning(
            "[%s] Exceeded %d connect retries — marking account as unavailable. Last error: %s",
            session_name,
            CONNECT_MAX_RETRIES,
            error,
        )
        return True
    return False


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

            if attempt >= CONNECT_MAX_RETRIES:
                err = state.get("last_error") or "max retries exceeded"
                logger.warning(
                    "[%s] Exceeded %d reconnect retries — marking unavailable. Last error: %s",
                    session_name,
                    CONNECT_MAX_RETRIES,
                    err,
                )
                _mark_account_unavailable(session_name, error=str(err))
                CLIENT_CONNECT_STATE.pop(session_name, None)
                return False

            last_log_at = float(state.get("last_log_at") or 0.0)
            if attempt == 1 or now - last_log_at >= CONNECT_FAILURE_LOG_INTERVAL_SECONDS:
                logger.warning(f"🔌 [{session_name}] клиент отключен, пробую переподключить (attempt {attempt}/{CONNECT_MAX_RETRIES})...")
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


# ---------------------------------------------------------------------------
# Account status helpers
# ---------------------------------------------------------------------------

def _mark_account_unavailable(session_name: str, error: str | None = None) -> None:
    """Set the account status to 'unavailable' in accounts.json so it won't be retried."""
    try:
        accounts = load_json(ACCOUNTS_FILE, [])
        for acc in accounts:
            if acc.get("session_name") == session_name:
                acc["status"] = "unavailable"
                if error:
                    acc["last_error"] = error
                break
        save_json(ACCOUNTS_FILE, accounts)
    except Exception:
        logger.exception("[%s] Failed to persist unavailable status", session_name)


def _is_account_active(account_data: dict) -> bool:
    status = str(account_data.get("status") or "active").lower().strip()
    return status not in DISABLED_ACCOUNT_STATUSES


def _is_account_assigned(target: dict, session_name: str) -> bool:
    assigned = target.get("assigned_accounts") or []
    return session_name in assigned


# ---------------------------------------------------------------------------
# Account failure tracking (DB-backed)
# ---------------------------------------------------------------------------

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
            # Also write to the immutable log so history is never lost.
            conn.execute(
                "INSERT INTO account_failure_log (session_name, kind, error, target, created_at) VALUES (?, ?, ?, ?, ?)",
                (session_name, kind, last_error, last_target, now),
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


# ---------------------------------------------------------------------------
# Join status helpers
# ---------------------------------------------------------------------------

def _get_join_status(session_name: str, target_id: str) -> dict | None:
    try:
        with _db_connect() as conn:

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


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------

def _parse_iso_ts(value) -> float | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except Exception:
        return None


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

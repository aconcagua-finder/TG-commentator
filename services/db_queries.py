"""Database query helpers for comment tracking, logging, planning, and discussions.

Extracted from commentator.py — pure DB-backed functions with no dependency on
global mutable state (except _db_connect).
"""

import hashlib
import json
import logging
import random
import time
from datetime import datetime, timezone

from db.connection import get_connection as _get_db_connection
from services.project import DEFAULT_PROJECT_ID

logger = logging.getLogger(__name__)


def _db_connect():
    return _get_db_connection()


# ---------------------------------------------------------------------------
# Daily action counts & duplicate checks
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Post time tracking
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Scenario message history
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Post comment planning
# ---------------------------------------------------------------------------

def _post_plan_seed(chat_key: str, post_id: int) -> int:
    base = f"{chat_key}:{post_id}".encode("utf-8")
    return int(hashlib.sha256(base).hexdigest()[:16], 16)


def _load_post_comment_plan(chat_key: str, post_id: int) -> tuple[int, list[str]] | None:
    if not chat_key or not post_id:
        return None
    try:
        with _db_connect() as conn:

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


# ---------------------------------------------------------------------------
# Action logging
# ---------------------------------------------------------------------------

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
                    source_channel_id, post_id, msg_id, account_session_name, account_first_name,
                    account_username, content
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                log_entry.get('type'),
                log_entry.get('date'),
                log_entry.get('target', {}).get('destination_chat_id'),
                log_entry.get('target', {}).get('chat_name'),
                log_entry.get('target', {}).get('chat_username'),
                log_entry.get('target', {}).get('channel_id'),
                log_entry.get('post_id'),
                log_entry.get('msg_id'),
                log_entry.get('account', {}).get('session_name'),
                log_entry.get('account', {}).get('first_name'),
                log_entry.get('account', {}).get('username'),
                content
            ))
            conn.commit()
        logger.info(
            f"Подробный лог ({log_entry.get('type')}) сохранен в БД для аккаунта {log_entry.get('account', {}).get('session_name')}")
    except Exception as e:
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


# ---------------------------------------------------------------------------
# Discussion session DB
# ---------------------------------------------------------------------------

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

"""Unified database schema — single source of truth for all tables.

Supports both PostgreSQL and SQLite via dialect detection.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

DB_URL: str | None = os.getenv("DB_URL")


def _is_postgres() -> bool:
    return bool(DB_URL)


def _serial_pk() -> str:
    return "SERIAL PRIMARY KEY" if _is_postgres() else "INTEGER PRIMARY KEY AUTOINCREMENT"


# ---------------------------------------------------------------------------
# Table definitions
# ---------------------------------------------------------------------------

def _table_logs() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS logs (
        id {_serial_pk()},
        log_type TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        destination_chat_id BIGINT NOT NULL,
        channel_name TEXT,
        channel_username TEXT,
        source_channel_id BIGINT,
        post_id BIGINT NOT NULL,
        msg_id BIGINT,
        account_session_name TEXT,
        account_first_name TEXT,
        account_username TEXT,
        content TEXT
    )
    """


def _table_proxies() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS proxies (
        id {_serial_pk()},
        url TEXT NOT NULL UNIQUE,
        name TEXT,
        ip TEXT,
        country TEXT,
        status TEXT,
        last_check TEXT
    )
    """


def _table_scenarios() -> str:
    return """
    CREATE TABLE IF NOT EXISTS scenarios (
        chat_id TEXT PRIMARY KEY,
        script_content TEXT,
        current_index INTEGER DEFAULT 0,
        status TEXT DEFAULT 'stopped',
        last_run_time REAL DEFAULT 0
    )
    """


def _table_post_scenarios() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS post_scenarios (
        id {_serial_pk()},
        chat_id TEXT,
        post_id INTEGER,
        current_index INTEGER DEFAULT 0,
        last_run_time REAL DEFAULT 0,
        UNIQUE(chat_id, post_id)
    )
    """


def _table_triggers() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS triggers (
        id {_serial_pk()},
        chat_id TEXT NOT NULL,
        trigger_phrase TEXT NOT NULL,
        answer_text TEXT NOT NULL
    )
    """


def _table_alert_context() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS alert_context (
        id {_serial_pk()},
        chat_id TEXT,
        msg_id INTEGER,
        session_name TEXT,
        created_at REAL
    )
    """


def _table_outbound_queue() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS outbound_queue (
        id {_serial_pk()},
        chat_id TEXT,
        reply_to_msg_id INTEGER,
        session_name TEXT,
        text TEXT,
        status TEXT DEFAULT 'pending'
    )
    """


def _table_inbox_messages() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS inbox_messages (
        id {_serial_pk()},
        kind TEXT NOT NULL,
        direction TEXT NOT NULL,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL,
        session_name TEXT NOT NULL,
        chat_id TEXT NOT NULL,
        msg_id INTEGER,
        reply_to_msg_id INTEGER,
        sender_id BIGINT,
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


def _table_join_status() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS join_status (
        id {_serial_pk()},
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


def _table_account_failures() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS account_failures (
        id {_serial_pk()},
        session_name TEXT NOT NULL,
        kind TEXT NOT NULL,
        count INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        last_attempt REAL,
        last_target TEXT
    )
    """


def _table_account_failure_log() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS account_failure_log (
        id {_serial_pk()},
        session_name TEXT NOT NULL,
        kind TEXT NOT NULL,
        error TEXT,
        target TEXT,
        created_at REAL NOT NULL
    )
    """


def _table_chat_last_post_times() -> str:
    return """
    CREATE TABLE IF NOT EXISTS chat_last_post_times (
        kind TEXT NOT NULL,
        chat_key TEXT NOT NULL,
        last_post_ts REAL NOT NULL,
        updated_at REAL NOT NULL,
        PRIMARY KEY(kind, chat_key)
    )
    """


def _table_scenario_msg_history() -> str:
    return """
    CREATE TABLE IF NOT EXISTS scenario_msg_history (
        chat_id TEXT NOT NULL,
        post_id INTEGER NOT NULL,
        ref_idx INTEGER NOT NULL,
        msg_id INTEGER NOT NULL,
        updated_at REAL NOT NULL,
        PRIMARY KEY(chat_id, post_id, ref_idx)
    )
    """


def _table_post_comment_plans() -> str:
    return """
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


def _table_used_identities() -> str:
    return """
    CREATE TABLE IF NOT EXISTS used_identities (
        user_id BIGINT PRIMARY KEY,
        date_used TEXT
    )
    """


def _table_discussion_sessions() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS discussion_sessions (
        id {_serial_pk()},
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


def _table_discussion_messages() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS discussion_messages (
        id {_serial_pk()},
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


def _table_manual_tasks() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS manual_tasks (
        id {_serial_pk()},
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


def _table_warning_seen() -> str:
    return """
    CREATE TABLE IF NOT EXISTS warning_seen (
        key TEXT PRIMARY KEY,
        seen_at REAL
    )
    """


def _table_warning_history() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS warning_history (
        id {_serial_pk()},
        key TEXT NOT NULL,
        level TEXT NOT NULL DEFAULT 'warning',
        title TEXT NOT NULL,
        detail TEXT,
        session_name TEXT,
        created_at REAL NOT NULL,
        resolved_at REAL
    )
    """


# ---------------------------------------------------------------------------
# Indexes
# ---------------------------------------------------------------------------

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_logs_dest_post_type ON logs(destination_chat_id, post_id, log_type)",
    "CREATE INDEX IF NOT EXISTS idx_logs_dest_post_type_account ON logs(destination_chat_id, post_id, log_type, account_session_name)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_inbox_unique ON inbox_messages(session_name, chat_id, msg_id, direction)",
    "CREATE INDEX IF NOT EXISTS idx_inbox_kind_unread ON inbox_messages(kind, is_read, id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_join_status_unique ON join_status(session_name, target_id)",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_account_failures_unique ON account_failures(session_name, kind)",
    "CREATE INDEX IF NOT EXISTS idx_account_failure_log_session ON account_failure_log(session_name, kind, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_scenario_msg_hist_post ON scenario_msg_history(chat_id, post_id)",
    "CREATE INDEX IF NOT EXISTS idx_discussion_sessions_target ON discussion_sessions(project_id, discussion_target_chat_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_discussion_sessions_target_id ON discussion_sessions(project_id, discussion_target_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_discussion_sessions_status ON discussion_sessions(project_id, status, schedule_at)",
    "CREATE INDEX IF NOT EXISTS idx_discussion_messages_session ON discussion_messages(session_id, id)",
    "CREATE INDEX IF NOT EXISTS idx_manual_tasks_project_status ON manual_tasks(project_id, status, id)",
    "CREATE INDEX IF NOT EXISTS idx_warning_history_key ON warning_history(key)",
    "CREATE INDEX IF NOT EXISTS idx_warning_history_resolved ON warning_history(resolved_at)",
]


# ---------------------------------------------------------------------------
# Init
# ---------------------------------------------------------------------------

def init_database(conn) -> None:
    """Create all tables and indexes.

    Args:
        conn: A connection object with .execute() method.
              Works with both sqlite3.Connection and PgConnectionWrapper.
    """
    table_funcs = [
        _table_logs,
        _table_proxies,
        _table_scenarios,
        _table_post_scenarios,
        _table_triggers,
        _table_alert_context,
        _table_outbound_queue,
        _table_inbox_messages,
        _table_join_status,
        _table_account_failures,
        _table_account_failure_log,
        _table_chat_last_post_times,
        _table_scenario_msg_history,
        _table_post_comment_plans,
        _table_used_identities,
        _table_discussion_sessions,
        _table_discussion_messages,
        _table_manual_tasks,
        _table_warning_seen,
        _table_warning_history,
    ]

    for fn in table_funcs:
        sql = fn().strip()
        if sql:
            conn.execute(sql)

    if _is_postgres():
        conn.execute("ALTER TABLE logs ADD COLUMN IF NOT EXISTS msg_id BIGINT")
    else:
        try:
            conn.execute("ALTER TABLE logs ADD COLUMN msg_id BIGINT")
        except Exception:
            pass

    for idx_sql in INDEXES:
        conn.execute(idx_sql)

    # PostgreSQL-specific: set up WAL-equivalent (already default in PG)
    if not _is_postgres():
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
        except Exception:
            pass

    try:
        conn.commit()
    except AttributeError:
        pass

    logger.info("Database schema initialized (%s)", "PostgreSQL" if _is_postgres() else "SQLite")

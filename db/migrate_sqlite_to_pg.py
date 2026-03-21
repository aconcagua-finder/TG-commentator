"""Migrate data from SQLite to PostgreSQL.

Usage:
    DB_URL=postgres://user:pass@host:5432/dbname python -m db.migrate_sqlite_to_pg [sqlite_path]

If sqlite_path is not given, uses the default from app_paths.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    logger.error("DB_URL environment variable is required")
    sys.exit(1)


TABLES_TO_MIGRATE = [
    "logs",
    "proxies",
    "scenarios",
    "post_scenarios",
    "triggers",
    "alert_context",
    "outbound_queue",
    "inbox_messages",
    "join_status",
    "account_failures",
    "account_failure_log",
    "chat_last_post_times",
    "scenario_msg_history",
    "post_comment_plans",
    "used_identities",
    "discussion_sessions",
    "discussion_messages",
    "manual_tasks",
    "warning_seen",
]


def migrate(sqlite_path: str) -> None:
    import psycopg2

    logger.info("Connecting to SQLite: %s", sqlite_path)
    src = sqlite3.connect(sqlite_path)
    src.row_factory = sqlite3.Row

    logger.info("Connecting to PostgreSQL: %s", DB_URL.split("@")[-1] if "@" in DB_URL else "***")
    dst = psycopg2.connect(DB_URL)
    dst_cur = dst.cursor()

    # Init schema in PG
    from db.schema import init_database
    from db.connection import PgConnectionWrapper
    wrapper = PgConnectionWrapper(dst)
    init_database(wrapper)
    dst.commit()

    for table in TABLES_TO_MIGRATE:
        try:
            rows = src.execute(f"SELECT * FROM {table}").fetchall()
        except sqlite3.OperationalError:
            logger.warning("Table %s does not exist in SQLite, skipping", table)
            continue

        if not rows:
            logger.info("Table %s: 0 rows, skipping", table)
            continue

        cols = rows[0].keys()
        # Skip 'id' column for SERIAL tables
        cols_no_id = [c for c in cols if c != "id"]

        placeholders = ", ".join(["%s"] * len(cols_no_id))
        col_names = ", ".join(cols_no_id)
        insert_sql = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

        batch = []
        for row in rows:
            values = tuple(row[c] for c in cols_no_id)
            batch.append(values)

        try:
            dst_cur.executemany(insert_sql, batch)
            dst.commit()
            logger.info("Table %s: migrated %d rows", table, len(batch))
        except Exception as e:
            dst.rollback()
            logger.error("Table %s: migration failed: %s", table, e)

    # Reset sequences for SERIAL columns
    for table in TABLES_TO_MIGRATE:
        try:
            dst_cur.execute(
                f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), COALESCE(MAX(id), 1)) FROM {table}"
            )
            dst.commit()
        except Exception:
            dst.rollback()

    src.close()
    dst_cur.close()
    dst.close()
    logger.info("Migration complete!")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        path = sys.argv[1]
    else:
        from app_paths import DB_FILE
        path = DB_FILE

    if not os.path.exists(path):
        logger.error("SQLite file not found: %s", path)
        sys.exit(1)

    migrate(path)

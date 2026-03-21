"""Database connection management.

Supports PostgreSQL (via psycopg2 for sync, asyncpg for async)
with SQLite fallback when DB_URL is not set.

Configuration via environment:
    DB_URL  — PostgreSQL connection string, e.g. postgres://user:pass@host:5432/dbname
              If not set, falls back to SQLite at DB_FILE path.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Iterator

logger = logging.getLogger(__name__)

DB_URL: str | None = os.getenv("DB_URL")

# ---------------------------------------------------------------------------
# PostgreSQL pool (lazy-initialized)
# ---------------------------------------------------------------------------
_pg_pool = None
_pg_async_pool = None


def _get_pg_pool():
    """Lazy-init sync psycopg2 connection pool."""
    global _pg_pool
    if _pg_pool is None:
        import psycopg2
        from psycopg2 import pool as pg_pool

        _pg_pool = pg_pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=DB_URL,
        )
        logger.info("PostgreSQL sync pool initialized")
    return _pg_pool


async def _get_pg_async_pool():
    """Lazy-init async asyncpg connection pool."""
    global _pg_async_pool
    if _pg_async_pool is None:
        import asyncpg

        _pg_async_pool = await asyncpg.create_pool(
            dsn=DB_URL,
            min_size=1,
            max_size=10,
        )
        logger.info("PostgreSQL async pool initialized")
    return _pg_async_pool


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------
_sqlite_db_file: str | None = None


def _get_sqlite_path() -> str:
    """Resolve SQLite file path from app_paths."""
    global _sqlite_db_file
    if _sqlite_db_file is None:
        from app_paths import DB_FILE
        _sqlite_db_file = DB_FILE
    return _sqlite_db_file


# ---------------------------------------------------------------------------
# Unified wrappers that normalize PG results to look like sqlite3.Row
# ---------------------------------------------------------------------------

class DictRow(dict):
    """Dict-like row that also supports index-based access (row[0], row[1])."""

    def __init__(self, keys: list[str], values: list):
        super().__init__(zip(keys, values))
        self._values = list(values)

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return super().__getitem__(key)

    def get(self, key, default=None):
        if isinstance(key, int):
            try:
                return self._values[key]
            except IndexError:
                return default
        return super().get(key, default)


class PgConnectionWrapper:
    """Wraps a psycopg2 connection to provide sqlite3-compatible interface.

    - Translates ? placeholders to %s
    - Returns DictRow objects instead of tuples
    """

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql: str, params=None):
        sql = _translate_placeholders(sql)
        cur = self._conn.cursor()
        cur.execute(sql, params or ())
        return PgCursorWrapper(cur)

    def cursor(self):
        """Return a cursor-like object compatible with sqlite3 cursor usage."""
        return PgCursorAsConnection(self)

    def commit(self):
        self._conn.commit()

    def close(self):
        pass  # Handled by pool

    @property
    def raw(self):
        return self._conn


class PgCursorWrapper:
    """Wraps a psycopg2 cursor to return DictRow objects."""

    def __init__(self, cursor):
        self._cursor = cursor

    def fetchone(self):
        row = self._cursor.fetchone()
        if row is None:
            return None
        if self._cursor.description:
            keys = [desc[0] for desc in self._cursor.description]
            return DictRow(keys, list(row))
        return row

    def fetchall(self):
        rows = self._cursor.fetchall()
        if not rows or not self._cursor.description:
            return rows
        keys = [desc[0] for desc in self._cursor.description]
        return [DictRow(keys, list(row)) for row in rows]

    @property
    def lastrowid(self):
        return getattr(self._cursor, "lastrowid", None)

    @property
    def rowcount(self):
        return self._cursor.rowcount


class PgCursorAsConnection:
    """Mimics sqlite3 cursor — execute() returns self, with fetchone/fetchall."""

    def __init__(self, wrapper: PgConnectionWrapper):
        self._wrapper = wrapper
        self._last_result: PgCursorWrapper | None = None

    def execute(self, sql: str, params=None):
        self._last_result = self._wrapper.execute(sql, params)
        return self._last_result

    def fetchone(self):
        if self._last_result:
            return self._last_result.fetchone()
        return None

    def fetchall(self):
        if self._last_result:
            return self._last_result.fetchall()
        return []

    @property
    def lastrowid(self):
        if self._last_result:
            return self._last_result.lastrowid
        return None

    @property
    def rowcount(self):
        if self._last_result:
            return self._last_result.rowcount
        return 0


def _translate_placeholders(sql: str) -> str:
    """Convert SQLite-style ? placeholders to PostgreSQL-style %s.

    Skips ? inside string literals.
    """
    result = []
    in_string = False
    quote_char = None
    for ch in sql:
        if in_string:
            result.append(ch)
            if ch == quote_char:
                in_string = False
        elif ch in ("'", '"'):
            in_string = True
            quote_char = ch
            result.append(ch)
        elif ch == "?":
            result.append("%s")
        else:
            result.append(ch)
    return "".join(result)


def _translate_schema_sql(sql: str) -> str:
    """Convert SQLite DDL to PostgreSQL DDL."""
    # AUTOINCREMENT → handled by SERIAL
    sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    # PRAGMA statements are SQLite-specific
    if sql.strip().upper().startswith("PRAGMA"):
        return ""
    # SQLite boolean literals
    sql = sql.replace("DEFAULT 0", "DEFAULT 0")  # noop, same syntax
    return sql


# ---------------------------------------------------------------------------
# Public sync interface
# ---------------------------------------------------------------------------

@contextmanager
def get_connection() -> Iterator[Any]:
    """Get a database connection (sync).

    Usage:
        with get_connection() as conn:
            conn.execute("SELECT ...", (param,))

    Returns a connection-like object with .execute(), .commit().
    Results support both dict-style and index-style access.
    """
    if DB_URL:
        pool = _get_pg_pool()
        raw_conn = pool.getconn()
        try:
            wrapper = PgConnectionWrapper(raw_conn)
            yield wrapper
            raw_conn.commit()
        except Exception:
            raw_conn.rollback()
            raise
        finally:
            pool.putconn(raw_conn)
    else:
        conn = sqlite3.connect(_get_sqlite_path())
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Public async interface
# ---------------------------------------------------------------------------

@asynccontextmanager
async def get_async_connection():
    """Get a database connection (async).

    For now, wraps sync connection in executor for compatibility.
    Full asyncpg support can be added later for performance.
    """
    # Use sync connection for now — asyncpg has different API (no .execute with ?)
    # and would require rewriting all queries. This keeps compatibility.
    if DB_URL:
        pool = _get_pg_pool()
        raw_conn = pool.getconn()
        try:
            wrapper = PgConnectionWrapper(raw_conn)
            yield wrapper
            raw_conn.commit()
        except Exception:
            raw_conn.rollback()
            raise
        finally:
            pool.putconn(raw_conn)
    else:
        conn = sqlite3.connect(_get_sqlite_path())
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Cross-dialect exception helpers
# ---------------------------------------------------------------------------

def is_integrity_error(exc: Exception) -> bool:
    """Check if exception is an IntegrityError (works with both SQLite and psycopg2)."""
    import sqlite3 as _sqlite3
    if isinstance(exc, _sqlite3.IntegrityError):
        return True
    try:
        import psycopg2
        if isinstance(exc, psycopg2.IntegrityError):
            return True
    except ImportError:
        pass
    return False


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def close_pool():
    """Close connection pools. Call on shutdown."""
    global _pg_pool, _pg_async_pool
    if _pg_pool is not None:
        try:
            _pg_pool.closeall()
        except Exception:
            pass
        _pg_pool = None

    if _pg_async_pool is not None:
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(_pg_async_pool.close())
            else:
                loop.run_until_complete(_pg_async_pool.close())
        except Exception:
            pass
        _pg_async_pool = None

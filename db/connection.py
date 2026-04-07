"""Database connection management.

PostgreSQL only — psycopg2 for sync, asyncpg for async.

Configuration via environment:
    DB_URL — PostgreSQL connection string,
             e.g. postgres://user:pass@host:5432/dbname
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Iterator

logger = logging.getLogger(__name__)

DB_URL: str | None = os.getenv("DB_URL")

# ---------------------------------------------------------------------------
# PostgreSQL pools (lazy-initialized)
# ---------------------------------------------------------------------------
_pg_pool = None
_pg_async_pool = None


def _get_pg_pool():
    """Lazy-init sync psycopg2 connection pool."""
    global _pg_pool
    if _pg_pool is None:
        if not DB_URL:
            raise RuntimeError(
                "DB_URL is not set — PostgreSQL is required. "
                "Set DB_URL=postgres://user:pass@host:5432/dbname."
            )
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
        if not DB_URL:
            raise RuntimeError(
                "DB_URL is not set — PostgreSQL is required. "
                "Set DB_URL=postgres://user:pass@host:5432/dbname."
            )
        import asyncpg

        _pg_async_pool = await asyncpg.create_pool(
            dsn=DB_URL,
            min_size=1,
            max_size=10,
        )
        logger.info("PostgreSQL async pool initialized")
    return _pg_async_pool


# ---------------------------------------------------------------------------
# Wrapper classes — give psycopg2 a sqlite3-style API the rest of the
# codebase already uses (`conn.execute(...)`, `row["col"]`, `row[0]`).
# Migrating every call site to native psycopg2 would be a huge churn for
# zero functional gain, so we keep this thin adapter.
# ---------------------------------------------------------------------------

class DictRow(dict):
    """Dict-like row that also supports index-based access (row[0], row[1]).

    Note: ``__iter__`` is intentionally NOT overridden, so ``dict(row)`` and
    other dict consumers keep working. Code that needs positional unpacking
    must use indices explicitly: ``a, b = row[0], row[1]``.
    """

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

    Returns ``DictRow`` objects so callers can use ``row["col"]`` and ``row[0]``.
    All SQL passed in must use ``%s`` placeholders (psycopg2 native style).
    """

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql: str, params=None):
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
    def rowcount(self):
        if self._last_result:
            return self._last_result.rowcount
        return 0


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


# ---------------------------------------------------------------------------
# Public async interface
# ---------------------------------------------------------------------------

@asynccontextmanager
async def get_async_connection():
    """Get a database connection (async).

    For now, wraps the sync psycopg2 pool. Full asyncpg support can be added
    later for performance — but it would require rewriting all queries that
    rely on the PgConnectionWrapper API.
    """
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


# ---------------------------------------------------------------------------
# Cross-dialect exception helpers
# ---------------------------------------------------------------------------

def is_integrity_error(exc: Exception) -> bool:
    """Check if exception is a PostgreSQL IntegrityError.

    Kept as a thin abstraction so call sites don't need to import psycopg2
    directly. The function name is historic (used to also check sqlite3).
    """
    try:
        import psycopg2
        return isinstance(exc, psycopg2.IntegrityError)
    except ImportError:
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

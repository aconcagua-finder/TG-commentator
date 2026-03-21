"""Database abstraction layer.

Provides a unified interface for PostgreSQL (production) and SQLite (fallback).
All services (commentator, admin_web, admin_bot) use this module.

Usage:
    from db import get_connection, init_database

    # Sync context manager
    with get_connection() as conn:
        conn.execute("SELECT ...", (param,))

    # Async context manager (admin_web)
    async with get_async_connection() as conn:
        await conn.execute("SELECT ...", (param,))
"""

from db.connection import get_connection, get_async_connection, close_pool
from db.schema import init_database

__all__ = [
    "get_connection",
    "get_async_connection",
    "close_pool",
    "init_database",
]

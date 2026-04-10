"""Health check — real diagnostic of proxies, accounts and join statuses.

Performs actual network checks (not just DB reads) and updates the
respective tables with fresh results.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List

from admin_web.helpers import (
    _load_settings,
    _load_accounts,
    _active_project_id,
    _filter_by_project,
    _filter_accounts_by_project,
    _db_connect,
)
from admin_web.telethon_utils import (
    _check_proxy_health,
    _telethon_credentials,
    _resolve_account_session,
    _resolve_account_credentials,
    _resolve_account_proxy,
    device_kwargs,
)

from telethon import TelegramClient
from telethon.sessions import StringSession

logger = logging.getLogger(__name__)

_CONNECT_TIMEOUT = 15.0
_ENTITY_TIMEOUT = 10.0
_PROXY_CONCURRENCY = 5
_ACCOUNT_CONCURRENCY = 3

BLOCKED_STATUSES = {
    "banned", "frozen", "limited", "human_check",
    "unauthorized", "missing_session", "unavailable",
}


# ---------------------------------------------------------------------------
# Proxy check
# ---------------------------------------------------------------------------

async def _check_all_proxies() -> Dict[str, Any]:
    """Ping all proxies and update DB statuses. Returns summary."""
    with _db_connect() as conn:
        rows = conn.execute("SELECT id, url, name, status FROM proxies").fetchall()

    if not rows:
        return {"total": 0, "alive": 0, "dead": 0, "changed": []}

    sem = asyncio.Semaphore(_PROXY_CONCURRENCY)

    async def _check_one(row):
        async with sem:
            result = await _check_proxy_health(row["url"])
            return {**row, "new_status": result["status"], "ip": result.get("ip")}

    results = await asyncio.gather(
        *[_check_one(dict(r)) for r in rows],
        return_exceptions=True,
    )

    alive = 0
    dead = 0
    changed = []
    now = time.time()

    with _db_connect() as conn:
        for r in results:
            if isinstance(r, Exception):
                dead += 1
                continue
            new_status = r["new_status"]
            if new_status == "active":
                alive += 1
            else:
                dead += 1
            if r["status"] != new_status:
                changed.append({"name": r.get("name") or r["url"], "old": r["status"], "new": new_status})
            conn.execute(
                "UPDATE proxies SET status = %s, ip = COALESCE(%s, ip), last_check = %s WHERE id = %s",
                (new_status, r.get("ip"), now, r["id"]),
            )
        conn.commit()

    return {"total": len(rows), "alive": alive, "dead": dead, "changed": changed}


# ---------------------------------------------------------------------------
# Account connectivity check
# ---------------------------------------------------------------------------

async def _check_all_accounts(accounts: list, settings: dict) -> Dict[str, Any]:
    """Connect to Telegram with each account, check auth. Returns summary."""
    project_id = _active_project_id(settings)
    proj_accounts = _filter_accounts_by_project(accounts, project_id)
    api_id_default, api_hash_default = _telethon_credentials()

    ok = 0
    problems = []
    sem = asyncio.Semaphore(_ACCOUNT_CONCURRENCY)

    async def _check_one(acc):
        session_name = str(acc.get("session_name") or "")
        status = str(acc.get("status") or "active").lower().strip()
        if status in BLOCKED_STATUSES:
            return {"name": session_name, "ok": False, "reason": f"status={status}"}

        session = _resolve_account_session(acc)
        if not session:
            return {"name": session_name, "ok": False, "reason": "no_session"}

        api_id, api_hash = _resolve_account_credentials(acc, api_id_default, api_hash_default)
        proxy_tuple = _resolve_account_proxy(acc)

        client = TelegramClient(
            session, api_id, api_hash,
            proxy=proxy_tuple,
            **device_kwargs(acc),
        )
        try:
            async with sem:
                await asyncio.wait_for(client.connect(), timeout=_CONNECT_TIMEOUT)
                authorized = await asyncio.wait_for(
                    client.is_user_authorized(), timeout=_CONNECT_TIMEOUT,
                )
                if not authorized:
                    return {"name": session_name, "ok": False, "reason": "unauthorized"}
                return {"name": session_name, "ok": True, "reason": None}
        except asyncio.TimeoutError:
            return {"name": session_name, "ok": False, "reason": "timeout"}
        except Exception as e:
            return {"name": session_name, "ok": False, "reason": str(e)[:120]}
        finally:
            try:
                if client.is_connected():
                    await client.disconnect()
            except Exception:
                pass

    results = await asyncio.gather(
        *[_check_one(a) for a in proj_accounts],
        return_exceptions=True,
    )

    for r in results:
        if isinstance(r, Exception):
            problems.append({"name": "?", "reason": str(r)[:120]})
            continue
        if r["ok"]:
            ok += 1
        else:
            problems.append(r)

    return {"total": len(proj_accounts), "ok": ok, "problems": problems}


# ---------------------------------------------------------------------------
# Join status verification
# ---------------------------------------------------------------------------

async def _verify_joins(accounts: list, settings: dict) -> Dict[str, Any]:
    """For each target's linked_chat_id, check if at least one account can access it."""
    project_id = _active_project_id(settings)
    targets = _filter_by_project(settings.get("targets") or [], project_id)

    if not targets:
        return {"checked": 0, "ok": 0, "stale": 0, "details": []}

    # Get one working client
    proj_accounts = _filter_accounts_by_project(accounts, project_id)
    api_id_default, api_hash_default = _telethon_credentials()

    client = None
    client_session_name = None
    for acc in proj_accounts:
        status = str(acc.get("status") or "active").lower().strip()
        if status in BLOCKED_STATUSES:
            continue
        session = _resolve_account_session(acc)
        if not session:
            continue
        api_id, api_hash = _resolve_account_credentials(acc, api_id_default, api_hash_default)
        proxy_tuple = _resolve_account_proxy(acc)
        cl = TelegramClient(session, api_id, api_hash, proxy=proxy_tuple, **device_kwargs(acc))
        try:
            await asyncio.wait_for(cl.connect(), timeout=_CONNECT_TIMEOUT)
            if await asyncio.wait_for(cl.is_user_authorized(), timeout=_CONNECT_TIMEOUT):
                client = cl
                client_session_name = str(acc.get("session_name") or "")
                break
            else:
                await cl.disconnect()
        except Exception:
            try:
                await cl.disconnect()
            except Exception:
                pass

    if not client:
        return {"checked": 0, "ok": 0, "stale": 0, "details": [], "error": "no_authorized_client"}

    checked = 0
    ok_count = 0
    stale_count = 0
    details = []

    try:
        for t in targets:
            linked = str(t.get("linked_chat_id") or "").strip()
            main_id = str(t.get("chat_id") or "").strip()
            chat_name = t.get("chat_name") or main_id

            # Check linked chat (where comments are sent)
            target_id = linked if linked and linked != main_id else main_id
            if not target_id:
                continue

            checked += 1
            try:
                entity_id = int(target_id)
                await asyncio.wait_for(
                    client.get_input_entity(entity_id),
                    timeout=_ENTITY_TIMEOUT,
                )
                ok_count += 1
            except Exception as e:
                stale_count += 1
                details.append({"chat_name": chat_name, "target_id": target_id, "error": str(e)[:120]})

                # Mark all assigned accounts as stale for this target
                with _db_connect() as conn:
                    conn.execute(
                        "UPDATE join_status SET status = 'stale', last_error = %s, last_attempt = %s "
                        "WHERE target_id = %s AND status = 'joined'",
                        (str(e)[:500], time.time(), target_id),
                    )
                    conn.commit()
    finally:
        try:
            await client.disconnect()
        except Exception:
            pass

    return {"checked": checked, "ok": ok_count, "stale": stale_count, "details": details}


# ---------------------------------------------------------------------------
# Main health check orchestrator
# ---------------------------------------------------------------------------

async def run_health_check() -> Dict[str, Any]:
    """Run all health checks and return combined results."""
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()

    # Run proxy and account checks in parallel, joins sequentially (needs one client)
    proxy_task = asyncio.create_task(_check_all_proxies())
    account_task = asyncio.create_task(_check_all_accounts(accounts, settings))

    proxy_result = await proxy_task
    account_result = await account_task
    join_result = await _verify_joins(accounts, settings)

    return {
        "proxies": proxy_result,
        "accounts": account_result,
        "joins": join_result,
    }

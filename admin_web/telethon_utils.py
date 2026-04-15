from __future__ import annotations

import asyncio
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import httpx
from fastapi import HTTPException

from telethon import TelegramClient
from telethon.errors import (
    RPCError,
    UserAlreadyParticipantError,
    UserDeactivatedBanError,
)
from telethon.sessions import StringSession
from telethon.tl.functions.channels import (
    GetFullChannelRequest,
    JoinChannelRequest,
)
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
from telethon.tl.types import InputPeerChannel, PeerChannel

from tg_device import device_kwargs

from admin_web.helpers import (
    ACCOUNTS_DIR,
    ADMIN_WEB_TELETHON_TIMEOUT_SECONDS,
    ADMIN_WEB_TELETHON_TOTAL_TIMEOUT_SECONDS,
    FROZEN_ACCOUNT_PROBE_INVITE_HASH,
    _active_project_id,
    _channel_bare_id,
    _deadline_timeout,
    _filter_accounts_by_project,
    _find_session_file_path,
    _load_accounts,
    _load_config,
    _load_settings,
    _save_accounts,
    _save_settings,
    logger,
)


# ---------------------------------------------------------------------------
# Telethon credentials
# ---------------------------------------------------------------------------

def _telethon_credentials() -> Tuple[int, str]:
    cfg = _load_config("telethon_credentials")
    return int(cfg["api_id"]), cfg["api_hash"]


def _parse_proxy_tuple(url: str) -> tuple | None:
    try:
        protocol, rest = url.split("://", 1)
        auth, addr = rest.split("@", 1)
        user, password = auth.split(":", 1)
        host, port_s = addr.split(":", 1)
        return (protocol, host, int(port_s), True, user, password)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Proxy helpers
# ---------------------------------------------------------------------------

_PROXY_IP_RE = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")
_PROXY_HOST_RE = re.compile(r"^[A-Za-z0-9._-]+$")


def _looks_like_ip(value: str) -> bool:
    return bool(value and _PROXY_IP_RE.match(value))


def _looks_like_host(value: str) -> bool:
    value = (value or "").strip()
    if not value:
        return False
    if _looks_like_ip(value):
        return True
    if value.lower() == "localhost":
        return True
    if any(ch.isspace() for ch in value):
        return False
    if "/" in value or "@" in value:
        return False
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1]
        return bool(inner and ":" in inner)
    return bool(_PROXY_HOST_RE.match(value))


def _is_port(value: str) -> bool:
    value = (value or "").strip()
    if not value.isdigit():
        return False
    try:
        port = int(value)
    except Exception:
        return False
    return 1 <= port <= 65535


def _normalize_proxy_url(raw: str) -> str | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    if "://" in raw:
        return raw

    if "@" in raw:
        left, right = raw.split("@", 1)
        left_parts = left.split(":")
        right_parts = right.split(":")
        if len(left_parts) == 2 and len(right_parts) >= 2:
            host, port = left_parts
            if _looks_like_host(host) and _is_port(port):
                return f"http://{right}@{host}:{port}"
        if len(right_parts) == 2 and len(left_parts) >= 2:
            host, port = right_parts
            if _looks_like_host(host) and _is_port(port):
                return f"http://{left}@{host}:{port}"
        if len(right_parts) >= 2:
            return f"http://{left}@{right}"

    parts = raw.split(":")
    if len(parts) == 2:
        host, port = parts
        if _looks_like_host(host) and _is_port(port):
            return f"http://{host}:{port}"
    if len(parts) == 4:
        host, port, user, password = parts
        if _looks_like_host(host) and _is_port(port):
            return f"http://{user}:{password}@{host}:{port}"
        user, password, host, port = parts
        if _looks_like_host(host) and _is_port(port):
            return f"http://{user}:{password}@{host}:{port}"

    return None


def _split_proxy_line(line: str) -> tuple[str, str | None]:
    raw = (line or "").strip()
    if not raw:
        return "", None
    for sep in ("|", ";"):
        if sep in raw:
            left, right = raw.split(sep, 1)
            left = left.strip()
            right = right.strip()
            if left and right:
                return right, left
    return raw, None


# ---------------------------------------------------------------------------
# Session / account resolution
# ---------------------------------------------------------------------------

def _resolve_account_session(account_data: Dict[str, Any]) -> StringSession | str | None:
    session_string = str(account_data.get("session_string") or "").strip()
    if session_string:
        return StringSession(session_string)

    session_path = account_data.get("session_path")
    if isinstance(session_path, str) and session_path and os.path.exists(session_path):
        return session_path

    session_file = account_data.get("session_file") or account_data.get("session_name")
    if not session_file:
        return None
    return _find_session_file_path(str(session_file), ACCOUNTS_DIR)


def _resolve_account_credentials(
    account_data: Dict[str, Any], fallback_api_id: int, fallback_api_hash: str
) -> Tuple[int, str]:
    api_id = account_data.get("app_id") or account_data.get("api_id") or fallback_api_id
    api_hash = account_data.get("app_hash") or account_data.get("api_hash") or fallback_api_hash
    try:
        api_id = int(api_id)
    except Exception:
        api_id = fallback_api_id
    api_hash = api_hash or fallback_api_hash
    return api_id, api_hash


def _resolve_account_proxy(account_data: Dict[str, Any]) -> tuple | None:
    proxy_url = account_data.get("proxy_url")
    if isinstance(proxy_url, str) and proxy_url.strip():
        return _parse_proxy_tuple(proxy_url.strip())

    proxy = account_data.get("proxy")
    if isinstance(proxy, str) and proxy.strip():
        return _parse_proxy_tuple(proxy.strip())

    if isinstance(proxy, (list, tuple)) and proxy:
        if isinstance(proxy[0], str) and "://" in proxy[0]:
            return _parse_proxy_tuple(proxy[0])
        if len(proxy) >= 3 and isinstance(proxy[0], str):
            return tuple(proxy)

    proxies = account_data.get("proxies")
    if isinstance(proxies, (list, tuple)):
        for item in proxies:
            if isinstance(item, str) and "://" in item:
                return _parse_proxy_tuple(item)

    return None


# ---------------------------------------------------------------------------
# Access-hash refresh
# ---------------------------------------------------------------------------

async def _refresh_target_access_hashes(target: Dict[str, Any], settings: Dict[str, Any]) -> bool:
    if not isinstance(target, dict):
        return False
    chat_id = str(target.get("chat_id") or "")
    linked_id = str(target.get("linked_chat_id") or "")
    need_main = not target.get("chat_access_hash")
    need_linked = bool(linked_id) and not target.get("linked_chat_access_hash")
    if not (need_main or need_linked):
        return False

    updated = False
    try:
        client = await _get_any_authorized_client()
    except HTTPException:
        return False

    try:
        entity = None
        username = str(target.get("chat_username") or "").strip().lstrip("@")
        invite_link = str(target.get("invite_link") or "").strip()
        if username:
            try:
                entity = await client.get_entity(username)
            except Exception:
                entity = None
        if entity is None and invite_link:
            try:
                if "t.me/+" in invite_link or "joinchat" in invite_link or "/" not in invite_link:
                    hash_arg = invite_link.split("/")[-1].replace("+", "")
                    invite_info = await client(CheckChatInviteRequest(hash_arg))
                    entity = getattr(invite_info, "chat", None)
            except Exception:
                entity = None

        if entity:
            access_hash = getattr(entity, "access_hash", None)
            if access_hash and need_main:
                target["chat_access_hash"] = access_hash
                updated = True
            if getattr(entity, "username", None) and not target.get("chat_username"):
                target["chat_username"] = entity.username
                updated = True
            if need_linked:
                try:
                    full = await client(GetFullChannelRequest(channel=entity))
                    linked_chat_id_bare = getattr(full.full_chat, "linked_chat_id", None)
                    if linked_chat_id_bare:
                        linked_entity = await client.get_entity(PeerChannel(linked_chat_id_bare))
                        linked_hash = getattr(linked_entity, "access_hash", None)
                        if linked_hash:
                            target["linked_chat_access_hash"] = linked_hash
                            updated = True
                        if not linked_id:
                            target["linked_chat_id"] = f"-100{linked_chat_id_bare}"
                            updated = True
                except Exception:
                    pass
    finally:
        try:
            if client.is_connected():
                await client.disconnect()
        except Exception:
            pass

    if updated:
        _save_settings(settings)
    return updated


# ---------------------------------------------------------------------------
# Join helpers
# ---------------------------------------------------------------------------

async def _attempt_join_target(
    client: TelegramClient, session_name: str, target: Dict[str, Any], target_id: str
) -> Tuple[bool, str | None, str | None]:
    invite_link = target.get("invite_link")
    username = str(target.get("chat_username") or "").strip().lstrip("@")
    linked_chat_id = target.get("linked_chat_id")
    last_error = None
    last_method = None
    chat_id = str(target.get("chat_id") or "")
    linked_id = str(linked_chat_id or "")

    if invite_link:
        try:
            if "t.me/+" in invite_link or "joinchat" in invite_link or "/" not in invite_link:
                hash_arg = invite_link.split("/")[-1].replace("+", "")
                await client(ImportChatInviteRequest(hash_arg))
                return True, None, None
        except UserAlreadyParticipantError:
            return True, None, None
        except Exception as e:
            logger.warning(
                f"[admin_web] [{session_name}] join invite failed for {target_id}: {type(e).__name__}: {e}"
            )
            last_error = str(e)
            last_method = "invite"

    access_hash = None
    if str(target_id) == chat_id:
        access_hash = target.get("chat_access_hash")
    elif str(target_id) == linked_id:
        access_hash = target.get("linked_chat_access_hash")

    if access_hash:
        try:
            channel_id = _channel_bare_id(target_id) or _channel_bare_id(chat_id) or _channel_bare_id(linked_id)
            if channel_id is None:
                raise ValueError("invalid_channel_id")
            peer = InputPeerChannel(channel_id, int(access_hash))
            await client(JoinChannelRequest(peer))
            return True, None, None
        except UserAlreadyParticipantError:
            return True, None, None
        except Exception as e:
            logger.warning(
                f"[admin_web] [{session_name}] join access_hash failed for {target_id}: {type(e).__name__}: {e}"
            )
            last_error = str(e)
            last_method = "access_hash"

    if username and str(target_id) == chat_id:
        try:
            await client(JoinChannelRequest(username))
            return True, None, None
        except UserAlreadyParticipantError:
            return True, None, None
        except Exception as e:
            logger.warning(
                f"[admin_web] [{session_name}] join username failed for {target_id}: {type(e).__name__}: {e}"
            )
            last_error = str(e)
            last_method = "username"

    if username and str(target_id) == linked_id:
        try:
            entity = await client.get_entity(username)
            full = await client(GetFullChannelRequest(entity))
            if full.full_chat.linked_chat_id:
                linked_entity = await client.get_input_entity(full.full_chat.linked_chat_id)
                await client(JoinChannelRequest(linked_entity))
                return True, None, None
        except UserAlreadyParticipantError:
            return True, None, None
        except Exception as e:
            logger.warning(
                f"[admin_web] [{session_name}] join linked failed for {linked_chat_id}: {type(e).__name__}: {e}"
            )
            last_error = str(e)
            last_method = "linked"

    try:
        entity = await client.get_input_entity(int(str(target_id)))
        await client(JoinChannelRequest(entity))
        return True, None, None
    except UserAlreadyParticipantError:
        return True, None, None
    except Exception as e:
        logger.warning(
            f"[admin_web] [{session_name}] join id failed for {target_id}: {type(e).__name__}: {e}"
        )
        last_error = str(e)
        last_method = "id"

    return False, last_error, last_method


# ---------------------------------------------------------------------------
# Frozen-account detection
# ---------------------------------------------------------------------------

def _is_frozen_rpc_error(exc: RPCError) -> bool:
    name = exc.__class__.__name__
    if name == "FrozenMethodInvalidError":
        return True
    try:
        msg = str(exc)
    except Exception:
        msg = ""
    return "FROZEN" in msg.upper()


def _is_expected_invite_hash_error(exc: RPCError) -> bool:
    name = exc.__class__.__name__
    if name in {
        "InviteHashInvalidError",
        "InviteHashEmptyError",
        "InviteHashExpiredError",
    }:
        return True
    try:
        msg = str(exc)
    except Exception:
        msg = ""
    msg_upper = msg.upper()
    return any(
        token in msg_upper
        for token in (
            "INVITE_HASH_INVALID",
            "INVITE_HASH_EMPTY",
            "INVITE_HASH_EXPIRED",
        )
    )


async def _probe_account_frozen(client: TelegramClient) -> Tuple[bool | None, RPCError | None]:
    try:
        await client(CheckChatInviteRequest(FROZEN_ACCOUNT_PROBE_INVITE_HASH))
        return False, None
    except RPCError as exc:
        if _is_frozen_rpc_error(exc):
            return True, None
        if _is_expected_invite_hash_error(exc):
            return False, None
        return None, exc


# ---------------------------------------------------------------------------
# Account / proxy health checks
# ---------------------------------------------------------------------------

async def _check_account_entry(
    acc: Dict[str, Any],
    api_id_default: int,
    api_hash_default: str,
) -> Tuple[str, bool]:
    """Check if account is reachable: connect, authorize, get_me.

    Returns (status, is_error).
    Statuses: active, unauthorized, unavailable, banned, error.
    """
    session = _resolve_account_session(acc)
    if not session:
        acc["status"] = "unavailable"
        acc["last_error"] = "missing_session"
        acc["last_checked"] = datetime.now(timezone.utc).isoformat()
        return "unavailable", True

    api_id, api_hash = _resolve_account_credentials(acc, api_id_default, api_hash_default)
    proxy_tuple = _resolve_account_proxy(acc)
    client = TelegramClient(
        session,
        api_id,
        api_hash,
        proxy=proxy_tuple,
        **device_kwargs(acc),
    )

    try:
        await asyncio.wait_for(client.connect(), timeout=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS)

        if not await asyncio.wait_for(client.is_user_authorized(), timeout=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS):
            acc["status"] = "unauthorized"
            acc.pop("last_error", None)
            acc["last_checked"] = datetime.now(timezone.utc).isoformat()
            return "unauthorized", False

        me = await asyncio.wait_for(client.get_me(), timeout=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS)
        if not me:
            acc["status"] = "unavailable"
            acc["last_error"] = "get_me returned None"
            acc["last_checked"] = datetime.now(timezone.utc).isoformat()
            return "unavailable", True

        me_phone = getattr(me, "phone", None)
        phone_value = (
            (f"+{me_phone}" if me_phone and not str(me_phone).startswith("+") else me_phone)
            or acc.get("phone")
            or ""
        )
        acc.update(
            {
                "user_id": me.id,
                "first_name": me.first_name,
                "last_name": me.last_name or "",
                "username": me.username or "",
                "phone": phone_value,
            }
        )
        acc["status"] = "active"
        acc.pop("last_error", None)
        acc["last_checked"] = datetime.now(timezone.utc).isoformat()
        return "active", False
    except UserDeactivatedBanError:
        acc["status"] = "banned"
        acc.pop("last_error", None)
        acc["last_checked"] = datetime.now(timezone.utc).isoformat()
        return "banned", False
    except (ConnectionError, asyncio.TimeoutError, OSError) as exc:
        acc["status"] = "unavailable"
        acc["last_error"] = f"{type(exc).__name__}: {exc}"
        acc["last_checked"] = datetime.now(timezone.utc).isoformat()
        return "unavailable", True
    except RPCError as exc:
        acc["status"] = "unavailable"
        acc["last_error"] = f"{exc.__class__.__name__}: {exc}"
        acc["last_checked"] = datetime.now(timezone.utc).isoformat()
        return "unavailable", True
    except Exception as exc:
        acc["status"] = "unavailable"
        acc["last_error"] = f"{type(exc).__name__}: {exc}"
        acc["last_checked"] = datetime.now(timezone.utc).isoformat()
        return "unavailable", True
    finally:
        if client.is_connected():
            await client.disconnect()


async def _check_proxy_health(proxy_url: str) -> Dict[str, Any]:
    test_url = "http://ip-api.com/json/"
    try:
        timeout = httpx.Timeout(15.0, connect=10.0)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }
        async with httpx.AsyncClient(proxy=proxy_url, timeout=timeout, headers=headers) as client:
            response = await client.get(test_url, follow_redirects=True)
            if response.status_code == 200:
                data = response.json()
                return {"status": "active", "ip": data.get("query"), "country": data.get("country")}
    except Exception:
        pass
    return {"status": "dead", "ip": None, "country": None}


# ---------------------------------------------------------------------------
# Authorized client acquisition
# ---------------------------------------------------------------------------

async def _get_any_authorized_client() -> TelegramClient:
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    # Prefer accounts without proxy first (broken proxies can cause long connect timeouts).
    try:
        accounts = sorted(accounts, key=lambda a: 1 if a.get("proxy_url") else 0)
    except Exception:
        pass
    if not accounts:
        raise HTTPException(status_code=400, detail="Нет аккаунтов. Сначала добавьте хотя бы один.")

    api_id_default, api_hash_default = _telethon_credentials()
    blocked_statuses = {"banned", "frozen", "limited", "human_check", "unauthorized", "missing_session"}
    dirty = False
    deadline = time.monotonic() + max(5.0, float(ADMIN_WEB_TELETHON_TOTAL_TIMEOUT_SECONDS))

    for acc in accounts:
        if time.monotonic() > deadline:
            break
        status = str(acc.get("status") or "").lower().strip()
        if status in blocked_statuses:
            continue
        session = _resolve_account_session(acc)
        if not session:
            continue
        api_id, api_hash = _resolve_account_credentials(acc, api_id_default, api_hash_default)
        proxy_tuple = _resolve_account_proxy(acc)
        client = TelegramClient(
            session,
            api_id,
            api_hash,
            proxy=proxy_tuple,
            **device_kwargs(acc),
        )
        try:
            authorized = False
            await asyncio.wait_for(
                client.connect(),
                timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
            )
            authorized = await asyncio.wait_for(
                client.is_user_authorized(),
                timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
            )
            if not authorized:
                continue

            try:
                frozen, _ = await asyncio.wait_for(
                    _probe_account_frozen(client),
                    timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
                )
            except Exception:
                frozen = None

            if frozen is True:
                acc["status"] = "frozen"
                dirty = True
                authorized = False
                continue

            if dirty:
                _save_accounts(accounts)
                dirty = False

            return client
        except Exception:
            pass
        finally:
            if client.is_connected() and not authorized:
                await client.disconnect()

    if dirty:
        _save_accounts(accounts)

    raise HTTPException(status_code=400, detail="Нет авторизованных аккаунтов. Проверьте аккаунты.")


# ---------------------------------------------------------------------------
# Channel entity resolution
# ---------------------------------------------------------------------------

async def _resolve_channel_entity(client: TelegramClient, chat_input: str) -> Tuple[Any, str | None]:
    invite_link: str | None = None
    if "t.me/+" in chat_input or "t.me/joinchat/" in chat_input:
        invite_hash = chat_input.split("/")[-1].replace("+", "")
        invite_link = invite_hash
        invite_info = await client(CheckChatInviteRequest(invite_hash))
        entity = invite_info.chat
        return entity, invite_link

    entity = await client.get_entity(chat_input)
    return entity, None


# ---------------------------------------------------------------------------
# Derive target chat info
# ---------------------------------------------------------------------------

async def _derive_target_chat_info(chat_input: str) -> Dict[str, Any]:
    def _short_exc(exc: Exception) -> str:
        try:
            msg = str(exc).replace("\n", " ").strip()
        except Exception:
            msg = ""
        if msg:
            msg = re.sub(r"\s+", " ", msg)
        if msg and len(msg) > 220:
            msg = msg[:219].rstrip() + "…"
        name = exc.__class__.__name__
        if name == "TimeoutError" and not msg:
            msg = "превышено время ожидания (проверьте прокси/сеть)"
        if name == "FrozenMethodInvalidError":
            hint = " (аккаунт заморожен Telegram — проверьте аккаунты/войдите заново)"
        else:
            hint = ""
        return f"{name}: {msg}{hint}" if msg else f"{name}{hint}"

    chat_input = (chat_input or "").strip()
    if not chat_input:
        raise HTTPException(status_code=400, detail="Пустой ввод.")

    settings, _ = _load_settings()
    accounts, _ = _load_accounts()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    if not accounts:
        raise HTTPException(status_code=400, detail="Нет аккаунтов. Сначала добавьте хотя бы один.")

    api_id_default, api_hash_default = _telethon_credentials()
    blocked_statuses = {"banned", "frozen", "limited", "human_check", "unauthorized", "missing_session"}

    last_error: Exception | None = None
    last_session: str | None = None
    deadline = time.monotonic() + max(5.0, float(ADMIN_WEB_TELETHON_TOTAL_TIMEOUT_SECONDS))

    for acc in accounts:
        if time.monotonic() > deadline:
            last_error = TimeoutError("total_timeout")
            last_session = None
            break
        status = str(acc.get("status") or "").lower().strip()
        if status in blocked_statuses:
            continue

        session_name = str(acc.get("session_name") or "").strip() or "account"
        session = _resolve_account_session(acc)
        if not session:
            continue
        api_id, api_hash = _resolve_account_credentials(acc, api_id_default, api_hash_default)
        proxy_tuple = _resolve_account_proxy(acc)
        client = TelegramClient(
            session,
            api_id,
            api_hash,
            proxy=proxy_tuple,
            **device_kwargs(acc),
        )
        try:
            await asyncio.wait_for(
                client.connect(),
                timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
            )
            authorized = await asyncio.wait_for(
                client.is_user_authorized(),
                timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
            )
            if not authorized:
                last_error = RuntimeError("unauthorized")
                last_session = session_name
                continue

            try:
                entity, invite_link = await asyncio.wait_for(
                    _resolve_channel_entity(client, chat_input),
                    timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
                )
            except RPCError as exc:
                last_error = exc
                last_session = session_name
                continue
            except Exception as exc:
                last_error = exc
                last_session = session_name
                continue

            try:
                await asyncio.wait_for(
                    client(JoinChannelRequest(entity)),
                    timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
                )
            except Exception:
                pass

            chat_username = getattr(entity, "username", None)
            chat_access_hash = getattr(entity, "access_hash", None)
            channel_id_str = f"-100{entity.id}"
            chat_name_to_save = getattr(entity, "title", None) or str(entity.id)

            comment_chat_id_str = channel_id_str
            linked_access_hash = None
            linked_chat_name_to_save = None
            linked_chat_username_to_save = None
            try:
                full_channel = await asyncio.wait_for(
                    client(GetFullChannelRequest(channel=entity)),
                    timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
                )
                linked_chat_id_bare = getattr(full_channel.full_chat, "linked_chat_id", None)
                if linked_chat_id_bare:
                    comment_chat_entity = await asyncio.wait_for(
                        client.get_entity(PeerChannel(linked_chat_id_bare)),
                        timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
                    )
                    linked_access_hash = getattr(comment_chat_entity, "access_hash", None)
                    linked_chat_name_to_save = getattr(comment_chat_entity, "title", None) or str(comment_chat_entity.id)
                    linked_chat_username_to_save = getattr(comment_chat_entity, "username", None)
                    try:
                        await asyncio.wait_for(
                            client(JoinChannelRequest(comment_chat_entity)),
                            timeout=_deadline_timeout(deadline, default=ADMIN_WEB_TELETHON_TIMEOUT_SECONDS),
                        )
                    except Exception:
                        pass
                    comment_chat_id_str = f"-100{comment_chat_entity.id}"
            except RPCError as exc:
                # Try another account (frozen accounts may fail on this method).
                last_error = exc
                last_session = session_name
                continue
            except Exception:
                # Best-effort: linked chat is optional; don't fail for unexpected errors.
                pass

            return {
                "chat_id": channel_id_str,
                "chat_username": chat_username,
                "linked_chat_id": comment_chat_id_str,
                "chat_name": chat_name_to_save,
                **({"linked_chat_name": linked_chat_name_to_save} if linked_chat_name_to_save else {}),
                **({"linked_chat_username": linked_chat_username_to_save} if linked_chat_username_to_save else {}),
                "invite_link": invite_link,
                **({"chat_access_hash": chat_access_hash} if chat_access_hash else {}),
                **({"linked_chat_access_hash": linked_access_hash} if linked_access_hash else {}),
            }
        except Exception as exc:
            last_error = exc
            last_session = session_name
            continue
        finally:
            try:
                if client.is_connected():
                    await client.disconnect()
            except Exception:
                pass

    if last_error is not None:
        session_part = f"[{last_session}] " if last_session else ""
        raise HTTPException(status_code=400, detail=f"Не удалось определить чат: {session_part}{_short_exc(last_error)}")
    raise HTTPException(status_code=400, detail="Не удалось определить чат: нет подходящих авторизованных аккаунтов.")

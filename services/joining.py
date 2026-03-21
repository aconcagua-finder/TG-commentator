"""Account join logic — ensure accounts are joined to target chats.

Extracted from commentator.py.
"""

import logging
import time

from telethon.errors import UserAlreadyParticipantError
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import InputPeerChannel

from services.connection import (
    _channel_bare_id,
    _get_join_status,
    _compute_slow_join_next_retry_at,
    _upsert_join_status,
    _record_account_failure,
    _clear_account_failure,
    JOIN_MAX_RETRIES,
    JOIN_RETRY_BACKOFF,
)

logger = logging.getLogger(__name__)


async def ensure_account_joined(
    client_wrapper,
    target_config,
    *,
    force: bool = False,
    joined_cache: set,
):
    """Ensure the account has joined the target chat(s).

    Parameters
    ----------
    client_wrapper :
        CommentatorClient wrapper with .client and .session_name.
    target_config : dict
        Target configuration with chat_id, linked_chat_id, invite_link, etc.
    force : bool
        If True, bypass retry limits and backoff.
    joined_cache : set
        Shared mutable set of (session_name, target_id) tuples for caching.
    """
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

        if cache_key in joined_cache:
            continue

        row = _get_join_status(client_wrapper.session_name, target_id)
        now = time.time()
        if row:
            if row.get("status") == "joined":
                joined_cache.add(cache_key)
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
            joined_cache.add(cache_key)
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

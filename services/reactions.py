"""Reaction processing — emoji extraction, selection, and automated reaction sending.

Extracted from commentator.py.
"""

import asyncio
import logging
import random
from datetime import datetime, timezone

from telethon.errors import FloodWaitError, ReactionsTooManyError
from telethon.tl.functions.messages import SendReactionRequest
from telethon.tl.types import ReactionEmoji

from services.account_utils import load_project_accounts, is_bot_awake
from services.connection import ensure_client_connected, _is_account_assigned, _record_account_failure, _clear_account_failure
from services.db_queries import get_daily_action_count_from_db, log_action_to_db

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Extract existing reaction emojis from a message
# ---------------------------------------------------------------------------

def _extract_existing_reaction_emojis(message):
    emojis = []
    if not message:
        return emojis
    reactions = getattr(message, "reactions", None)
    results = getattr(reactions, "results", None) if reactions else None
    if not results:
        return emojis
    for r in results:
        reaction_obj = getattr(r, "reaction", None)
        emoticon = getattr(reaction_obj, "emoticon", None) if reaction_obj else None
        if emoticon:
            emojis.append(emoticon)
    return emojis


# ---------------------------------------------------------------------------
# Select reaction emojis from desired/existing pools
# ---------------------------------------------------------------------------

def _select_reaction_emojis(desired, existing, count):
    desired = [str(x).strip() for x in (desired or []) if str(x).strip()]
    existing = [str(x).strip() for x in (existing or []) if str(x).strip()]

    pool = []
    if existing:
        intersect = [e for e in desired if e in existing]
        pool = intersect or existing
    else:
        pool = desired

    if not pool:
        return []
    count = max(int(count or 1), 1)
    if count == 1 or len(pool) == 1:
        return [random.choice(pool)]
    return random.sample(pool, min(count, len(pool)))


# ---------------------------------------------------------------------------
# Process a new post for reaction
# ---------------------------------------------------------------------------

async def process_new_post_for_reaction(
    source_channel_peer,
    original_post_id,
    reaction_target,
    message=None,
    *,
    active_clients: dict,
    pending_tasks: set,
    current_settings: dict,
):
    """Send reactions to a new post.

    Parameters
    ----------
    active_clients : dict
        session_name -> CommentatorClient mapping.
    pending_tasks : set
        Shared set of pending asyncio tasks (for graceful shutdown tracking).
    current_settings : dict
        Global settings dict.
    """
    task = asyncio.current_task()
    pending_tasks.add(task)
    try:
        chance = reaction_target.get('reaction_chance', 80)
        if random.randint(1, 100) > chance:
            return
        destination_chat_id_for_logs = int(reaction_target.get('chat_id', reaction_target.get('linked_chat_id')))
        daily_limit = reaction_target.get('daily_reaction_limit', 999)
        current_daily_count = get_daily_action_count_from_db(destination_chat_id_for_logs, 'reaction')
        if current_daily_count >= daily_limit:
            return
        accounts_data = load_project_accounts(current_settings)
        eligible_clients = []
        for c in list(active_clients.values()):
            acc_data = next((a for a in accounts_data if a['session_name'] == c.session_name), None)
            if acc_data and is_bot_awake(acc_data) and _is_account_assigned(reaction_target, c.session_name):
                eligible_clients.append(c)
        if not eligible_clients:
            return
        random.shuffle(eligible_clients)
        initial_delay = max(reaction_target.get('initial_reaction_delay', 10), 0)
        if initial_delay > 0:
            await asyncio.sleep(initial_delay)
        delay_between = max(reaction_target.get('delay_between_reactions', 5), 0)
        desired_reactions = reaction_target.get("reactions", []) or []
        existing_reactions = _extract_existing_reaction_emojis(message)
        for client_wrapper in eligible_clients:
            try:
                attempted_send = False
                current_daily_count = get_daily_action_count_from_db(destination_chat_id_for_logs, 'reaction')
                if current_daily_count >= daily_limit:
                    break
                if not desired_reactions and not existing_reactions:
                    continue

                if not await ensure_client_connected(client_wrapper, reason="reaction"):
                    continue

                num_to_set = reaction_target.get("reaction_count", 1)
                reactions_to_set_str = _select_reaction_emojis(desired_reactions, existing_reactions, num_to_set)
                if not reactions_to_set_str:
                    continue

                tl_reactions = [ReactionEmoji(emoticon=r) for r in reactions_to_set_str]
                actual_peer = None
                try:
                    actual_peer = await client_wrapper.client.get_input_entity(destination_chat_id_for_logs)
                except Exception:
                    actual_peer = source_channel_peer
                try:
                    await client_wrapper.client.send_read_acknowledge(actual_peer, message=original_post_id)
                except Exception:
                    pass
                await asyncio.sleep(random.uniform(1, 3))
                try:
                    attempted_send = True
                    await client_wrapper.client(
                        SendReactionRequest(peer=actual_peer, msg_id=original_post_id, reaction=tl_reactions)
                    )
                except ReactionsTooManyError:
                    if not existing_reactions:
                        try:
                            msg = await client_wrapper.client.get_messages(actual_peer, ids=original_post_id)
                            msg = msg[0] if isinstance(msg, list) else msg
                            existing_reactions = _extract_existing_reaction_emojis(msg)
                        except Exception:
                            existing_reactions = []

                    fallback = _select_reaction_emojis(desired_reactions, existing_reactions, 1)
                    if not fallback:
                        raise
                    reactions_to_set_str = fallback
                    tl_reactions = [ReactionEmoji(emoticon=r) for r in reactions_to_set_str]
                    attempted_send = True
                    await client_wrapper.client(
                        SendReactionRequest(peer=actual_peer, msg_id=original_post_id, reaction=tl_reactions)
                    )

                me = await client_wrapper.client.get_me()
                log_action_to_db({
                    'type': 'reaction', 'post_id': original_post_id, 'reactions': reactions_to_set_str,
                    'date': datetime.now(timezone.utc).isoformat(),
                    'account': {'session_name': client_wrapper.session_name, 'first_name': me.first_name,
                                'username': me.username},
                    'target': {'chat_name': reaction_target.get('chat_name'),
                               'chat_username': reaction_target.get('chat_username'),
                               'channel_id': reaction_target.get('chat_id'),
                               'destination_chat_id': destination_chat_id_for_logs}
                })
                _clear_account_failure(client_wrapper.session_name, "reaction")
                if delay_between > 0 and client_wrapper != eligible_clients[-1]:
                    await asyncio.sleep(delay_between)
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + 2)
            except Exception as e:
                logger.error(f"❌ Ошибка отправки реакции ({client_wrapper.session_name}): {e}")
                if attempted_send:
                    _record_account_failure(
                        client_wrapper.session_name,
                        "reaction",
                        last_error=str(e),
                        last_target=str(destination_chat_id_for_logs),
                        context={
                            "chat_id": str(destination_chat_id_for_logs),
                            "chat_name": reaction_target.get("chat_name"),
                            "chat_username": reaction_target.get("chat_username"),
                            "post_id": original_post_id,
                            "project_id": reaction_target.get("project_id"),
                        },
                    )
    except asyncio.CancelledError:
        pass
    finally:
        pending_tasks.discard(task)

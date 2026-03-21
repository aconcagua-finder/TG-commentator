"""Dialogue utilities — thread depth checking, AI reply counting,
burst message collection, thread context extraction, user ID collection.

Extracted from commentator.py.
"""

import logging

from services.account_utils import load_project_accounts

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Check dialogue depth (reply chain)
# ---------------------------------------------------------------------------

async def check_dialogue_depth(client, message_object, max_depth):
    try:
        if not message_object:
            return True

        current_depth = 0
        reply_ptr = message_object.reply_to

        while reply_ptr:
            current_depth += 1
            if current_depth >= max_depth:
                return False

            try:
                next_id = reply_ptr.reply_to_msg_id
                if not next_id:
                    break

                parent_msg = await client.get_messages(message_object.chat_id, ids=next_id)
                if not parent_msg:
                    break

                reply_ptr = parent_msg.reply_to
            except Exception:
                break

        return True
    except Exception as e:
        logger.error(f"Ошибка проверки глубины: {e}")
        return True


# ---------------------------------------------------------------------------
# Count how many replies in a chain are from our accounts
# ---------------------------------------------------------------------------

async def count_dialogue_ai_replies(
    client,
    message_object,
    our_ids: set,
    max_depth: int | None = None,
    include_current: bool = False,
    early_stop: int | None = None,
) -> int:
    try:
        if not message_object or not our_ids:
            return 0

        count = 0
        if include_current and getattr(message_object, "sender_id", None) in our_ids:
            count += 1
            if early_stop is not None and count >= early_stop:
                return count

        depth = 0
        reply_ptr = getattr(message_object, "reply_to", None)
        chat_id = getattr(message_object, "chat_id", None)
        if chat_id is None:
            return count

        while reply_ptr:
            depth += 1
            if max_depth is not None and depth >= int(max_depth):
                break

            next_id = getattr(reply_ptr, "reply_to_msg_id", None)
            if not next_id:
                break

            parent_msg = await client.get_messages(chat_id, ids=next_id)
            if isinstance(parent_msg, list):
                parent_msg = parent_msg[0] if parent_msg else None
            if not parent_msg:
                break

            if getattr(parent_msg, "sender_id", None) in our_ids:
                count += 1
                if early_stop is not None and count >= early_stop:
                    return count

            reply_ptr = getattr(parent_msg, "reply_to", None)

        return count
    except Exception:
        return int(early_stop) if early_stop is not None else 0


# ---------------------------------------------------------------------------
# Collect all user IDs belonging to our accounts
# ---------------------------------------------------------------------------

def get_all_our_user_ids(*, active_clients: dict, current_settings: dict) -> set[int]:
    """Collect all user IDs belonging to our accounts.

    Parameters
    ----------
    active_clients : dict
        session_name -> CommentatorClient mapping.
    current_settings : dict
        Global settings dict (used as fallback to load accounts from file).
    """
    ids: set[int] = set()

    try:
        for client_wrapper in list(active_clients.values()):
            uid = getattr(client_wrapper, "user_id", None)
            if uid is None or uid == "":
                continue
            try:
                ids.add(int(uid))
            except Exception:
                continue
    except Exception:
        pass

    if ids:
        return ids

    accounts = load_project_accounts(current_settings)
    for acc in accounts:
        if not isinstance(acc, dict):
            continue
        uid = acc.get("user_id")
        if uid is None or uid == "":
            continue
        try:
            ids.add(int(uid))
        except Exception:
            continue

    return ids


# ---------------------------------------------------------------------------
# Get burst messages from the same user
# ---------------------------------------------------------------------------

async def get_user_burst_messages(client, chat_id, original_msg):
    user_id = original_msg.sender_id
    burst_msgs = [original_msg]
    last_msg_id = original_msg.id

    try:
        async for msg in client.iter_messages(chat_id, min_id=original_msg.id, limit=5):
            if msg.sender_id == user_id and not msg.out:
                burst_msgs.append(msg)
                if msg.id > last_msg_id:
                    last_msg_id = msg.id
            else:
                break
    except Exception as e:
        logger.error(f"Error getting burst: {e}")

    burst_msgs.sort(key=lambda x: x.id)
    return burst_msgs, last_msg_id


# ---------------------------------------------------------------------------
# Extract thread context (who we're replying to)
# ---------------------------------------------------------------------------

async def get_thread_context(client, event, our_ids):
    target_user_id = None
    target_name = "участник"
    is_intervention = True
    reply_to_id = event.message.reply_to_msg_id
    if reply_to_id:
        try:
            parent_msg = await client.get_messages(event.chat_id, ids=reply_to_id)
            if parent_msg and parent_msg.sender_id:
                target_user_id = parent_msg.sender_id
                sender_entity = await parent_msg.get_sender()
                if sender_entity:
                    target_name = getattr(sender_entity, 'first_name', 'участник')
        except Exception:
            pass
    else:
        try:
            async for msg in client.iter_messages(event.chat_id, limit=2, offset_id=event.message.id + 1):
                if msg.id != event.message.id:
                    target_user_id = msg.sender_id
                    sender_entity = await msg.get_sender()
                    if sender_entity:
                        target_name = getattr(sender_entity, 'first_name', 'участник')
                    break
        except Exception:
            pass
    return target_user_id, target_name

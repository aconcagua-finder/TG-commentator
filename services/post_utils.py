"""Post utilities — image download, message re-fetch, catch-up logic.

Extracted from commentator.py.
"""

import asyncio
import collections
import logging
import random
from datetime import datetime, timezone

from services.connection import _run_with_soft_timeout, SEND_ATTEMPT_TIMEOUT_SECONDS
from services.db_queries import get_daily_action_count_from_db
from services.text_analysis import _message_has_image

logger = logging.getLogger(__name__)


async def download_message_image_bytes(message):
    """Download image bytes from a Telegram message, if it has an image."""
    if not _message_has_image(message):
        return None
    try:
        return await _run_with_soft_timeout(
            message.download_media(file=bytes),
            SEND_ATTEMPT_TIMEOUT_SECONDS,
        )
    except Exception:
        return None


async def refetch_post_message(client, chat_id: int, msg_id: int):
    """Re-fetch a post message by ID to get fresh data."""
    if client is None:
        return None
    try:
        entity = await _run_with_soft_timeout(
            client.get_input_entity(int(chat_id)),
            SEND_ATTEMPT_TIMEOUT_SECONDS,
        )
        messages = await _run_with_soft_timeout(
            client.get_messages(entity, ids=[int(msg_id)]),
            SEND_ATTEMPT_TIMEOUT_SECONDS,
        )
        if messages and isinstance(messages, list):
            return messages[0]
    except Exception:
        return None
    return None


async def catch_up_missed_posts(
    client_wrapper,
    target_chat,
    *,
    post_process_cache: set,
    handled_grouped_ids,
    pending_tasks: set,
    processing_cache: set,
    process_new_post_fn,
):
    """Scan recent messages and process missed posts.

    Parameters
    ----------
    post_process_cache : set
        Shared mutable set of processed post unique IDs.
    handled_grouped_ids : deque
        Shared mutable deque for grouped message dedup.
    pending_tasks : set
        Shared mutable set of pending asyncio tasks.
    processing_cache : set
        Shared mutable set of currently-processing unique IDs.
    process_new_post_fn : callable
        Reference to process_new_post function (async).
    """
    task = asyncio.current_task()
    pending_tasks.add(task)
    try:
        chat_id_raw = target_chat.get('linked_chat_id', target_chat.get('chat_id'))
        chat_id = int(str(chat_id_raw).replace('-100', ''))
        destination_chat_id = int(str(chat_id_raw))
        chat_name = target_chat.get('chat_name')

        SCAN_LIMIT = 5
        TIME_LIMIT_SECONDS = 600

        daily_limit = target_chat.get('daily_comment_limit', 999)

        try:
            current_count = get_daily_action_count_from_db(destination_chat_id, 'comment')
        except Exception:
            current_count = 999
        if current_count >= daily_limit:
            return

        try:
            entity = await client_wrapper.client.get_input_entity(chat_id)
        except Exception:
            return

        messages_to_scan = []
        async for message in client_wrapper.client.iter_messages(entity, limit=SCAN_LIMIT):
            messages_to_scan.append(message)

        me = await client_wrapper.client.get_me()
        my_id = me.id
        posts_replied_by_me = set()

        for message in messages_to_scan:
            if message.sender_id == my_id and message.reply_to_msg_id:
                posts_replied_by_me.add(message.reply_to_msg_id)

        for message in messages_to_scan:
            msg_date = message.date
            if msg_date.tzinfo is None:
                msg_date = msg_date.replace(tzinfo=timezone.utc)

            if (datetime.now(timezone.utc) - msg_date).total_seconds() > TIME_LIMIT_SECONDS:
                continue

            is_channel_post = False
            if message.fwd_from and message.fwd_from.channel_post:
                is_channel_post = True
            elif message.post:
                is_channel_post = True

            if not is_channel_post:
                continue

            if message.id in posts_replied_by_me:
                continue

            if message.grouped_id:
                if message.grouped_id in handled_grouped_ids:
                    continue
                handled_grouped_ids.append(message.grouped_id)

            unique_process_id = f"{target_chat.get('chat_id')}_{message.id}"
            if unique_process_id in post_process_cache:
                continue
            if unique_process_id in processing_cache:
                continue

            logger.info(f"💡 [CATCH-UP] Нашел свежий пропущенный пост {message.id} в {chat_name}")

            try:
                await client_wrapper.client.send_read_acknowledge(entity, message=message)
            except:
                pass

            event_mock = collections.namedtuple('EventMock', ['message', 'chat_id'])
            mock_event = event_mock(message=message, chat_id=destination_chat_id)

            await process_new_post_fn(mock_event, target_chat, from_catch_up=True)
            await asyncio.sleep(random.randint(2, 5))

    except asyncio.CancelledError:
        pass
    finally:
        pending_tasks.discard(task)

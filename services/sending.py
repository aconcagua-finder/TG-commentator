"""Message sending with human-like typing simulation and text splitting.

Extracted from commentator.py.
"""

import asyncio
import logging
import random
from typing import Literal

from telethon import functions, types, helpers
from telethon.errors import ChatAdminRequiredError, RPCError

from services.connection import _run_with_soft_timeout, SEND_ATTEMPT_TIMEOUT_SECONDS
from services.text_processing import post_process_text, split_text_smart_ru_no_comma

logger = logging.getLogger(__name__)


async def human_type_and_send(
    client,
    chat_id,
    text,
    reply_to_msg_id=None,
    skip_processing=False,
    thread_top_msg_id: int | None = None,
    split_mode: Literal["legacy", "smart_ru_no_comma", "off"] = "legacy",
    *,
    humanization_settings: dict | None = None,
):
    """Send a message with typing simulation and optional text humanization.

    Parameters
    ----------
    humanization_settings : dict | None
        The ``current_settings.get('humanization', {})`` dict.
        When *None* an empty dict is used (no transformations / no splitting).
    """
    if not text:
        return

    h_set = humanization_settings or {}

    if skip_processing:
        processed_text = text
    else:
        processed_text = post_process_text(text, h_set)

    split_chance = h_set.get('split_chance', 0) / 100
    message_parts = []

    if split_mode != "off" and not skip_processing and len(processed_text) > 50 and random.random() < split_chance:
        if split_mode == "smart_ru_no_comma":
            message_parts = split_text_smart_ru_no_comma(processed_text) or [processed_text]
        else:
            delimiters = [', ', '. ', '! ', '? ']
            split_done = False
            for d in delimiters:
                if d in processed_text:
                    parts = processed_text.split(d, 1)
                    message_parts = [parts[0], parts[1]]
                    split_done = True
                    break
            if not split_done:
                message_parts = [processed_text]
    else:
        message_parts = [processed_text]

    last_msg = None
    original_reply_id = reply_to_msg_id

    async def _send_to_thread_without_quote(part_text: str, top_id: int):
        # Для комментария в тред поста канала InputReplyToMessage требует
        # валидный reply_to_msg_id (reply_to_msg_id=0 Telegram молча игнорирует и
        # отправляет сообщение как обычное без привязки к треду — из-за этого
        # комментарии «без цитаты» ранее терялись вместо появления под постом).
        # Передаём reply_to_msg_id = top_id: это корректная форма reply на корень
        # дискуссионного треда. В UI комментариев под постом плашка-цитата не
        # показывается — сам пост уже отображён сверху.
        peer = await _run_with_soft_timeout(client.get_input_entity(chat_id), SEND_ATTEMPT_TIMEOUT_SECONDS)
        req = functions.messages.SendMessageRequest(
            peer=peer,
            message=part_text,
            reply_to=types.InputReplyToMessage(reply_to_msg_id=int(top_id), top_msg_id=int(top_id)),
            random_id=helpers.generate_random_long(),
        )
        res = await _run_with_soft_timeout(client(req), SEND_ATTEMPT_TIMEOUT_SECONDS)
        try:
            for upd in (getattr(res, "updates", None) or []):
                msg = getattr(upd, "message", None)
                if msg is not None:
                    return msg
        except Exception:
            pass
        return None

    for part in message_parts:
        part = part.strip()
        if not part:
            continue

        await asyncio.sleep(random.uniform(2, 4))

        typing_time = min(len(part) * 0.06, 6)

        async def _typing_sleep():
            async with client.action(chat_id, 'typing'):
                await asyncio.sleep(typing_time)

        try:
            await _run_with_soft_timeout(_typing_sleep(), SEND_ATTEMPT_TIMEOUT_SECONDS)
        except (ChatAdminRequiredError, RPCError, Exception):
            await asyncio.sleep(typing_time)

        try:
            if original_reply_id is None and thread_top_msg_id:
                try:
                    last_msg = await _send_to_thread_without_quote(part, int(thread_top_msg_id))
                except Exception:
                    # Fallback to plain reply to the thread root (may show quote, but stays in thread).
                    last_msg = await _run_with_soft_timeout(
                        client.send_message(chat_id, part, reply_to=int(thread_top_msg_id)),
                        SEND_ATTEMPT_TIMEOUT_SECONDS,
                    )
            else:
                last_msg = await _run_with_soft_timeout(
                    client.send_message(chat_id, part, reply_to=original_reply_id),
                    SEND_ATTEMPT_TIMEOUT_SECONDS,
                )
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке сообщения: {e}")
            try:
                if getattr(client, "is_connected", None) and client.is_connected():
                    await client.disconnect()
            except Exception:
                pass
            break

    return last_msg

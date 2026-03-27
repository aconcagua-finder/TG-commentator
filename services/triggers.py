"""Trigger-based auto-replies — match trigger phrases in messages.

Extracted from commentator.py.
"""

import asyncio
import logging
import random

from services.account_utils import load_project_accounts, is_bot_awake
from services.connection import (
    _is_account_assigned,
    _record_account_failure,
    _clear_account_failure,
)
from services.sending import human_type_and_send

logger = logging.getLogger(__name__)


def _db_connect():
    from db.connection import get_connection
    return get_connection()


async def process_trigger(
    event,
    found_target,
    our_ids,
    *,
    active_clients: dict,
    current_settings: dict,
    reply_process_cache: set,
):
    """Check if message matches a trigger phrase and send auto-reply.

    Parameters
    ----------
    active_clients : dict
        session_name -> CommentatorClient mapping.
    current_settings : dict
        Global settings dict.
    reply_process_cache : set
        Shared mutable set to prevent duplicate processing.
    """
    msg_id = event.message.id
    if msg_id in reply_process_cache:
        return

    post_text = (event.message.text or "").lower()
    if not post_text:
        return

    answer_text = None
    try:
        chat_id_target = str(found_target.get('chat_id'))

        with _db_connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT trigger_phrase, answer_text FROM triggers WHERE chat_id = ?",
                (chat_id_target,)
            )
            rows = cursor.fetchall()

            for phrase, answer in rows:
                clean_phrase = phrase.strip().lower()
                if clean_phrase and clean_phrase in post_text:
                    answer_text = answer
                    break
    except Exception as e:
        logger.error(f"Ошибка поиска триггера: {e}")
        return

    if answer_text:
        reply_process_cache.add(msg_id)

        accounts_data = load_project_accounts(current_settings)
        eligible = []

        for c in list(active_clients.values()):
            acc_conf = next((a for a in accounts_data if a['session_name'] == c.session_name), None)
            if acc_conf and is_bot_awake(acc_conf):
                if _is_account_assigned(found_target, c.session_name):
                    eligible.append(c)

        if not eligible:
            reply_process_cache.discard(msg_id)
            return

        client_wrapper = random.choice(eligible)

        await asyncio.sleep(random.uniform(3, 7))

        try:
            await human_type_and_send(
                client_wrapper.client, event.chat_id, answer_text,
                reply_to_msg_id=msg_id,
                humanization_settings=current_settings.get('humanization', {}),
            )
            logger.info(f"⚡ [{client_wrapper.session_name}] ответил по триггеру на сообщение {msg_id}")
            _clear_account_failure(client_wrapper.session_name, "reply")
        except Exception as e:
            logger.error(f"Ошибка отправки триггера: {e}")
            _record_account_failure(
                client_wrapper.session_name,
                "reply",
                last_error=str(e),
                last_target=str(event.chat_id),
                context={
                    "chat_id": str(event.chat_id),
                    "chat_name": found_target.get("chat_name") if isinstance(found_target, dict) else None,
                    "chat_username": found_target.get("chat_username") if isinstance(found_target, dict) else None,
                    "post_id": msg_id,
                    "project_id": found_target.get("project_id") if isinstance(found_target, dict) else None,
                },
            )
            reply_process_cache.discard(msg_id)

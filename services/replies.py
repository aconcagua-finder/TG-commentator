"""Reply processing — automated replies to user comments.

Extracted from commentator.py.
"""

import asyncio
import logging
import random
from datetime import datetime, timezone

from services.account_utils import load_project_accounts, is_bot_awake
from services.comments import generate_comment
from services.connection import (
    _is_account_assigned,
    _record_account_failure,
    _clear_account_failure,
)
from services.db_queries import log_action_to_db
from services.dialogue import (
    build_reply_context,
    check_dialogue_depth,
    count_dialogue_ai_replies,
    get_all_our_user_ids,
    get_thread_context,
)
from services.sending import human_type_and_send

logger = logging.getLogger(__name__)


async def execute_reply_with_fallback(
    candidate_list,
    chat_id,
    target_chat,
    prompt_base,
    delay,
    reply_to_msg_id,
    reply_to_name=None,
    is_intervention=False,
    *,
    pending_tasks: set,
    current_settings: dict,
    recent_generated_messages,
):
    """Generate and send a reply with fallback across candidates.

    Parameters
    ----------
    pending_tasks : set
        Shared mutable set of pending asyncio tasks.
    current_settings : dict
        Global settings dict.
    recent_generated_messages : deque
        Shared mutable deque for deduplication.
    """
    task = asyncio.current_task()
    pending_tasks.add(task)
    attempted_send = False
    active_client = None
    try:
        await asyncio.sleep(delay)
        actual_reply_id = reply_to_msg_id
        for client_wrapper in candidate_list:
            reply_text, prompt_info = await generate_comment(
                prompt_base,
                target_chat,
                client_wrapper.session_name,
                image_bytes=None,
                is_reply_mode=True,
                reply_to_name=reply_to_name,
                current_settings=current_settings,
                recent_messages=recent_generated_messages,
            )
            if reply_text:
                attempted_send = True
                active_client = client_wrapper
                await human_type_and_send(
                    client_wrapper.client,
                    chat_id,
                    reply_text,
                    reply_to_msg_id=actual_reply_id,
                    humanization_settings=current_settings.get('humanization', {}),
                )
                me = await client_wrapper.client.get_me()
                action_label = "ВМЕШАТЕЛЬСТВО" if is_intervention else "ОТВЕТ"
                logger.info(f"✅ [{client_wrapper.session_name}] ({action_label}) на сообщение {reply_to_msg_id} ({prompt_info})")
                log_content = f"[{prompt_info}] [{action_label}] {reply_text}"
                log_action_to_db({
                    'type': 'comment_reply',
                    'post_id': reply_to_msg_id,
                    'comment': log_content,
                    'date': datetime.now(timezone.utc).isoformat(),
                    'account': {'session_name': client_wrapper.session_name, 'first_name': me.first_name,
                                'username': me.username},
                    'target': {'chat_name': target_chat.get('chat_name'), 'destination_chat_id': chat_id}
                })
                _clear_account_failure(client_wrapper.session_name, "reply")
                return
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Ошибка в цепочке ответов: {e}")
        if attempted_send and active_client:
            _record_account_failure(
                active_client.session_name,
                "reply",
                last_error=str(e),
                last_target=str(chat_id),
                context={
                    "chat_id": str(chat_id),
                    "chat_name": target_chat.get("chat_name"),
                    "chat_username": target_chat.get("chat_username"),
                    "post_id": reply_to_msg_id,
                    "project_id": target_chat.get("project_id"),
                },
            )
    finally:
        pending_tasks.discard(task)


async def process_reply_to_comment(
    event,
    target_chat,
    *,
    active_clients: dict,
    current_settings: dict,
    reply_process_cache: set,
    pending_tasks: set,
    recent_generated_messages,
    spam_blocked_msgs: set | None = None,
):
    """Decide whether to reply to a user comment and schedule the reply.

    Parameters
    ----------
    active_clients : dict
        session_name -> CommentatorClient mapping.
    current_settings : dict
        Global settings dict.
    reply_process_cache : set
        Shared mutable set to prevent duplicate reply processing.
    pending_tasks : set
        Shared mutable set of pending asyncio tasks.
    recent_generated_messages : deque
        Shared mutable deque for deduplication.
    """
    msg_id = event.message.id
    if msg_id in reply_process_cache:
        return
    reply_process_cache.add(msg_id)

    if isinstance(spam_blocked_msgs, set):
        try:
            reply_to_id = int(getattr(event.message, "reply_to_msg_id", None) or 0)
        except Exception:
            reply_to_id = 0
        if msg_id in spam_blocked_msgs or (reply_to_id and reply_to_id in spam_blocked_msgs):
            return

    chat_id = event.chat_id
    sender_id = event.message.sender_id
    accounts_data = load_project_accounts(current_settings)
    eligible_candidates = []
    for c in list(active_clients.values()):
        acc_conf = next((a for a in accounts_data if a['session_name'] == c.session_name), None)
        if acc_conf and is_bot_awake(acc_conf) and getattr(c, 'user_id', None) != sender_id:
            if _is_account_assigned(target_chat, c.session_name):
                eligible_candidates.append(c)
    if not eligible_candidates:
        logger.info(f"⏭ Нет доступных аккаунтов для ответа на {msg_id}")
        return

    intervention_chance = target_chat.get('intervention_chance', 30)
    roll = random.randint(1, 100)
    if roll > intervention_chance:
        logger.info(f"🎲 Шанс не сработал ({roll} > {intervention_chance}%) для {msg_id}. Никто не ответил")
        return

    max_history = target_chat.get('max_dialogue_depth', 6)
    if not await check_dialogue_depth(event.client, event.message, max_history):
        logger.info(f"⏭ Сообщение {msg_id} пропущено: превышена глубина диалога ({max_history})")
        return

    our_ids = get_all_our_user_ids(active_clients=active_clients, current_settings=current_settings)

    max_ai_replies = target_chat.get("max_dialogue_ai_replies", 2)
    try:
        max_ai_replies = int(max_ai_replies or 0)
    except Exception:
        max_ai_replies = 2
    max_ai_replies = max(max_ai_replies, 0)

    if max_ai_replies > 0:
        ai_replies = await count_dialogue_ai_replies(
            event.client,
            event.message,
            our_ids=our_ids,
            max_depth=max_history,
            include_current=True,
            early_stop=max_ai_replies,
        )
        if ai_replies >= max_ai_replies:
            logger.info(f"⏭ Сообщение {msg_id} пропущено: лимит ответов ИИ в диалоге ({max_ai_replies})")
            return

    target_user_id, target_name = await get_thread_context(event.client, event, our_ids)
    is_reply_to_us = target_user_id in our_ids
    triggered_client = random.choice(eligible_candidates)
    is_intervention = getattr(triggered_client, 'user_id', None) != target_user_id
    d_min, d_max = target_chat.get('reply_delay_min', 20), target_chat.get('reply_delay_max', 80)
    personal_delay = random.randint(min(d_min, d_max), max(d_min, d_max))
    action_type = "ВМЕШАЕТСЯ" if is_intervention else "ОТВЕТИТ"
    to_who = f"нашему боту ({target_name})" if is_reply_to_us else f"пользователю {target_name}"
    logger.info(f"🤖 [{triggered_client.session_name}] {action_type} {to_who} через {personal_delay}с (шанс {roll}%)")

    # Build a rich prompt_base: original channel post + thread history +
    # the trigger message. Falls back to the raw trigger text if thread
    # resolution fails for any reason — same behaviour as before, so we
    # never regress into "no prompt at all".
    try:
        prompt_base = await build_reply_context(
            event.client,
            event.message,
            max_chain=max_history,
        )
    except Exception as exc:
        logger.warning(f"build_reply_context failed for {msg_id}: {exc}")
        prompt_base = ""
    if not prompt_base:
        prompt_base = f"{event.message.text or ''}"

    asyncio.create_task(execute_reply_with_fallback(
        [triggered_client], chat_id, target_chat,
        prompt_base, personal_delay,
        msg_id, reply_to_name=target_name, is_intervention=is_intervention,
        pending_tasks=pending_tasks,
        current_settings=current_settings,
        recent_generated_messages=recent_generated_messages,
    ))

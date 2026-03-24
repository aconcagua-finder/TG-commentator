"""Post monitoring — AI-based post evaluation and monitoring automation.

Extracted from commentator.py.
"""

import asyncio
import base64
import logging
import os
import random
from datetime import datetime, timezone

import openai
from google import genai
from google.genai import types as genai_types

from services.account_utils import (
    get_model_setting,
    gemini_model_candidates,
    openai_model_candidates,
    is_model_unavailable_error,
    guess_image_mime_type,
)
from services.connection import _is_account_assigned
from services.db_queries import (
    get_daily_action_count_from_db,
    _dt_to_utc,
    _db_get_last_post_time,
    _db_set_last_post_time,
    log_action_to_db,
)
from services.telegram_bot import build_monitoring_notification, notify_event
from services.text_analysis import normalize_id

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# AI-based post evaluation (is post relevant to monitoring criteria?)
# ---------------------------------------------------------------------------

async def generate_post_evaluation(
    post_text,
    target_chat_settings,
    session_name,
    image_bytes=None,
    *,
    current_settings: dict,
):
    """Evaluate whether a post matches a monitoring target's criteria.

    Parameters
    ----------
    current_settings : dict
        Global settings dict (for API keys, model settings).
    """
    provider = target_chat_settings.get('ai_provider', 'default')
    if provider == 'default':
        provider = current_settings.get('ai_provider', 'gemini')

    user_goal = target_chat_settings.get('prompt', 'релевантный пост')
    api_key = current_settings.get('api_keys', {}).get(provider)
    if not api_key:
        return False

    try:
        instruction = f"Проверь, соответствует ли пост теме: '{user_goal}'. Ответь только ОДНИМ словом: ДА или НЕТ."
        if provider in {"openai", "openrouter", "deepseek"}:
            base_url = None
            default_headers = None
            if provider == "deepseek":
                base_url = "https://api.deepseek.com"
            elif provider == "openrouter":
                base_url = "https://openrouter.ai/api/v1"
                default_headers = {
                    "HTTP-Referer": os.getenv("OPENROUTER_REFERRER", "http://localhost"),
                    "X-Title": os.getenv("OPENROUTER_TITLE", "AI-Центр"),
                }
            client = openai.AsyncOpenAI(api_key=api_key, base_url=base_url, default_headers=default_headers)

            text_part = f"{instruction}\nТекст: {post_text}"
            if provider == "openai":
                user_content = [{"type": "text", "text": text_part}]
                if image_bytes:
                    mime_type = guess_image_mime_type(image_bytes)
                    base64_image = base64.b64encode(image_bytes).decode('utf-8')
                    user_content.append(
                        {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{base64_image}"}}
                    )
                user_message = {"role": "user", "content": user_content}
            else:
                user_message = {"role": "user", "content": text_part}

            if provider == "deepseek":
                models_to_try = [get_model_setting(current_settings, "deepseek_eval")]
            elif provider == "openrouter":
                models_to_try = [get_model_setting(current_settings, "openrouter_eval")]
            else:
                models_to_try = openai_model_candidates(current_settings, "openai_eval")

            completion = None
            for model_name in models_to_try:
                try:
                    create_kwargs = {
                        "model": model_name,
                        "messages": [user_message],
                        "temperature": 0,
                    }
                    if provider == "openai":
                        create_kwargs["max_completion_tokens"] = 4
                    else:
                        create_kwargs["max_tokens"] = 4
                    completion = await client.chat.completions.create(**create_kwargs)
                    break
                except Exception as e:
                    if provider == "openai" and is_model_unavailable_error(e):
                        continue
                    raise

            return "ДА" in ((completion.choices[0].message.content or "").upper() if completion else "")
        elif provider == 'gemini':
            content = []
            if image_bytes:
                mime_type = guess_image_mime_type(image_bytes)
                content.append(genai_types.Part.from_bytes(data=image_bytes, mime_type=mime_type))
            content.append(f"{instruction}\nТекст: {post_text}")

            for model_name in gemini_model_candidates(current_settings, "gemini_eval"):
                try:
                    async with genai.Client(api_key=api_key).aio as aclient:
                        response = await aclient.models.generate_content(
                            model=model_name,
                            contents=content,
                            config=genai_types.GenerateContentConfig(
                                temperature=0,
                                max_output_tokens=4,
                            ),
                        )
                    return "ДА" in (response.text or "").upper()
                except Exception:
                    continue
    except Exception:
        return False

    return False


# ---------------------------------------------------------------------------
# Process a post for monitoring (evaluate + notify)
# ---------------------------------------------------------------------------

async def process_post_for_monitoring(
    event,
    monitor_target,
    *,
    active_clients: dict,
    monitor_channel_last_post_time: dict,
    pending_tasks: set,
    current_settings: dict,
):
    """Process a channel post for monitoring relevance.

    Parameters
    ----------
    active_clients : dict
        session_name -> CommentatorClient mapping.
    monitor_channel_last_post_time : dict
        Mutable dict tracking last post time per channel (for min_post_interval).
    pending_tasks : set
        Shared set of pending asyncio tasks.
    current_settings : dict
        Global settings dict.
    """
    task = asyncio.current_task()
    pending_tasks.add(task)
    try:
        channel_id = event.chat_id
        channel_key = normalize_id(channel_id) or channel_id
        post_content = event.message.text or ""
        post_id = event.message.id
        msg_date = _dt_to_utc(event.message.date)
        min_interval = monitor_target.get('min_post_interval_mins', 0)
        if min_interval > 0:
            if channel_key not in monitor_channel_last_post_time:
                persisted = _db_get_last_post_time("monitor", str(channel_key))
                if persisted:
                    monitor_channel_last_post_time[channel_key] = persisted
            last_post_time = monitor_channel_last_post_time.get(channel_key)
            if last_post_time and (msg_date - last_post_time).total_seconds() < min_interval * 60:
                return
            monitor_channel_last_post_time[channel_key] = msg_date
            _db_set_last_post_time("monitor", str(channel_key), msg_date)
        if len(post_content.split()) < monitor_target.get('min_word_count', 0):
            return
        daily_limit = monitor_target.get('daily_limit', 999)
        if daily_limit > 0 and get_daily_action_count_from_db(channel_id, 'monitoring') >= daily_limit:
            return
        eligible_clients = [
            c for c in list(active_clients.values()) if _is_account_assigned(monitor_target, c.session_name)
        ]
        if not eligible_clients:
            return
        client_wrapper = random.choice(eligible_clients)
        image_bytes = None
        if event.message.photo:
            image_bytes = await event.message.download_media(file=bytes)
        is_relevant = await generate_post_evaluation(
            post_content, monitor_target, client_wrapper.session_name, image_bytes,
            current_settings=current_settings,
        )
        if is_relevant:
            notification_chat_id = monitor_target['notification_chat_id']
            channel_username = monitor_target.get('chat_username')
            channel_id_str = str(monitor_target.get('chat_id', '')).replace('-100', '')
            post_link = f"https://t.me/{channel_username}/{post_id}" if channel_username else f"https://t.me/c/{channel_id_str}/{post_id}"
            try:
                await client_wrapper.client.forward_messages(notification_chat_id, event.message)
            except Exception:
                message_text = f"❗️ <b>Найден пост</b>\n\n<b>Канал:</b> {monitor_target.get('chat_name', 'N/A')}\n<b>Ссылка:</b> {post_link}"
                await client_wrapper.client.send_message(notification_chat_id, message_text, parse_mode='html', link_preview=False)
            me = await client_wrapper.client.get_me()
            log_action_to_db({
                'type': 'monitoring', 'post_id': post_id, 'date': datetime.now(timezone.utc).isoformat(),
                'account': {'session_name': client_wrapper.session_name, 'first_name': me.first_name, 'username': me.username},
                'target': {'chat_name': monitor_target.get('chat_name'), 'channel_id': channel_id, 'destination_chat_id': channel_id},
                'comment': f"Found post, notified {notification_chat_id}"
            })
            project_id = str(monitor_target.get("project_id") or "default").strip() or "default"
            try:
                await notify_event(
                    "monitoring",
                    project_id,
                    build_monitoring_notification(
                        chat_name=str(monitor_target.get("chat_name") or ""),
                        post_link=post_link,
                    ),
                    settings=current_settings,
                )
            except Exception as exc:
                logger.warning("Telegram bot monitoring notification failed: project_id=%s error=%s", project_id, exc)
    except asyncio.CancelledError:
        pass
    finally:
        pending_tasks.discard(task)

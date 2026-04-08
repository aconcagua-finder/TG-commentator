"""Comment generation — AI-powered comment/reply generation with retry logic.

Extracted from commentator.py.
"""

import asyncio
import base64
import logging
import os
import random
import re
from collections import deque

import openai
from google import genai
from google.genai import types as genai_types

from role_engine import build_role_prompt, enforce_emoji_level, role_for_account
from services.account_utils import (
    load_project_accounts,
    get_model_setting,
    gemini_model_candidates,
    openai_model_candidates,
    is_model_unavailable_error,
    guess_image_mime_type,
)

logger = logging.getLogger(__name__)


async def generate_comment(
    post_text,
    target_chat,
    session_name,
    image_bytes=None,
    is_reply_mode=False,
    reply_to_name=None,
    extra_instructions=None,
    *,
    current_settings: dict,
    recent_messages: deque,
):
    """Generate a comment or reply using AI providers.

    Parameters
    ----------
    current_settings : dict
        Global settings dict (API keys, humanization, blacklist, etc.).
    recent_messages : deque
        Shared deque of recently generated messages (for deduplication).
        Successfully generated comments are appended to this deque.
    """
    provider = target_chat.get('ai_provider', 'default')
    if provider == 'default':
        provider = current_settings.get('ai_provider', 'gemini')

    accounts_data = load_project_accounts(current_settings)
    account = next((a for a in accounts_data if a['session_name'] == session_name), None)
    role_id, role_data = role_for_account(account or {}, current_settings)
    role_prompt, role_meta = build_role_prompt(role_data, current_settings)
    role_name = str(role_data.get("name") or role_id or "Роль")
    mood_name = str(role_meta.get("mood") or "").strip()
    prompt_info = f"Роль: {role_name}" + (f" · настроение: {mood_name}" if mood_name else "")
    emoji_level = str(role_meta.get("emoji_level") or role_data.get("emoji_level") or "minimal")

    if not role_prompt:
        role_prompt = (
            "Пиши коротко и по теме поста как живой пользователь Telegram. "
            "Избегай канцелярита, штампов и тона нейросети."
        )

    global_blacklist = current_settings.get('blacklist', [])
    h_set = current_settings.get('humanization', {})
    custom_rules = h_set.get('custom_rules', "")
    vector_prompt = ""
    try:
        vector_prompt = str((target_chat or {}).get("vector_prompt") or "").strip()
    except Exception:
        vector_prompt = ""
    product_knowledge_prompt = ""
    try:
        product_knowledge_prompt = str(
            ((current_settings.get("product_knowledge", {}) or {}).get("prompt") or "")
        ).strip()
    except Exception:
        product_knowledge_prompt = ""
    raw_penalty = float(h_set.get('repetition_penalty', 0))
    frequency_penalty_val = min(max(raw_penalty / 50, 0.0), 2.0)
    try:
        max_tokens_val = int(h_set.get('max_tokens', 100))
    except Exception:
        max_tokens_val = 100
    if max_tokens_val <= 0:
        logger.warning("⚠️ humanization.max_tokens <= 0; использую 90 по умолчанию.")
        max_tokens_val = 90
    custom_temp = h_set.get('temperature')

    system_prompt = f"ТВОЯ РОЛЬ (ОТЫГРЫВАЙ ЕЕ ДОСЛОВНО):\n{role_prompt}\n\n"
    if vector_prompt:
        system_prompt += (
            "ВЕКТОР / ТЕМА (ОБЯЗАТЕЛЬНО):\n"
            f"{vector_prompt}\n\n"
            "Твоя реплика должна соответствовать этому вектору. "
            "Если в векторе перечислены конкретные объекты/модели/сервисы/проблемы — "
            "естественно упоминай 1–2 из них по месту.\n\n"
        )
    if product_knowledge_prompt:
        system_prompt += (
            "ЗНАНИЕ О ПРОДУКТЕ (ДОП. КОНТЕКСТ):\n"
            f"{product_knowledge_prompt}\n\n"
            "Используй это знание только если оно уместно по роли и текущему контексту. "
            "Не обязано проявляться в каждом ответе. Не противоречь теме беседы и не выдумывай факты.\n\n"
        )

    # Per-target кастомный промпт: общий "default" для чата + опциональный персональный для конкретного session_name.
    # Хранится в target.prompts ({"default": "...", "<session_name>": "..."}). Редактируется на странице
    # /targets/{chat_id}/prompts. Персональный имеет приоритет; если нет — берём default.
    target_prompts_map = {}
    try:
        raw_prompts = (target_chat or {}).get("prompts")
        if isinstance(raw_prompts, dict):
            target_prompts_map = raw_prompts
    except Exception:
        target_prompts_map = {}

    target_default_prompt = str(target_prompts_map.get("default") or "").strip()
    target_personal_prompt = str(target_prompts_map.get(str(session_name) or "") or "").strip()

    if target_default_prompt:
        system_prompt += (
            "ДОПОЛНИТЕЛЬНЫЕ ИНСТРУКЦИИ ДЛЯ ЭТОГО ЧАТА:\n"
            f"{target_default_prompt}\n\n"
        )
    if target_personal_prompt:
        system_prompt += (
            "ПЕРСОНАЛЬНЫЕ ИНСТРУКЦИИ ДЛЯ ЭТОГО АККАУНТА В ЭТОМ ЧАТЕ (приоритет над общими):\n"
            f"{target_personal_prompt}\n\n"
        )

    # Глобальный антиповтор: подмешиваем недавние ответы других наших аккаунтов в system_prompt,
    # чтобы модель не повторяла яркие формулировки и опенинги между постами/обсуждениями.
    if recent_messages:
        try:
            recent_list = list(recent_messages)
        except Exception:
            recent_list = []
        tail = recent_list[-10:]
        if tail:
            try:
                from services.text_analysis import _extract_opening_phrases, _truncate_one_line
                openings = _extract_opening_phrases(tail)
            except Exception:
                openings = []
                _truncate_one_line = lambda s, _l=160: str(s or "")[:_l]  # noqa: E731
            system_prompt += "НЕДАВНИЕ ОТВЕТЫ ДРУГИХ НАШИХ АККАУНТОВ — НЕ ПОВТОРЯЙ ЭТИ ФОРМУЛИРОВКИ И ОПЕНИНГИ:\n"
            for i, t in enumerate(tail[-5:], start=1):
                system_prompt += f"  {i}) {_truncate_one_line(t, 160)}\n"
            if openings:
                system_prompt += 'Запрещённые начала: "' + '"; "'.join(openings) + '"\n'
            system_prompt += "\n"

    system_prompt += f"ПРАВИЛА ОФОРМЛЕНИЯ ТЕКСТА:\n{custom_rules}\n"
    if global_blacklist:
        system_prompt += f"\nНЕ ИСПОЛЬЗУЙ СЛОВА: {', '.join(global_blacklist)}"

    if is_reply_mode:
        context_prefix = f"ТЕБЕ ГОВОРИТ {reply_to_name}: " if reply_to_name else ""
        user_template = (
            "КОНТЕКСТ ДИАЛОГА:\n{context}{post}\n\n"
            "Ответь согласно своей роли.\n"
            "{length_hint}\n"
            "{style_hint}\n"
            "{question_hint}"
        )
    else:
        context_prefix = ""
        user_template = (
            "ТЕКСТ ПОСТА:\n{post}\n\n"
            "Напиши комментарий от своей роли.\n"
            "Если текста поста нет — не проси прислать текст и не пиши, что ты его не видишь. "
            "Просто оставь короткую нейтральную реплику по ситуации, без вопросов.\n"
            "{length_hint}\n"
            "{style_hint}\n"
            "{question_hint}"
        )

    base_extra = (extra_instructions or "").strip()

    api_keys = current_settings.get('api_keys', {})
    main_api_key = api_keys.get(provider)
    if not main_api_key:
        return None, f"{prompt_info} · FAIL: missing_api_key({provider})"

    def _short_exc(e: Exception) -> str:
        try:
            msg = str(e).replace("\n", " ").strip()
        except Exception:
            msg = ""
        if msg:
            msg = re.sub(r"\\s+", " ", msg)
        if msg and len(msg) > 220:
            msg = msg[:219].rstrip() + "…"
        return f"{type(e).__name__}: {msg}" if msg else type(e).__name__

    retry_cfg = current_settings.get("ai_retry", {}) if isinstance(current_settings, dict) else {}
    try:
        max_attempts = int(retry_cfg.get("max_attempts", 3) or 0)
    except Exception:
        max_attempts = 3
    max_attempts = max(min(max_attempts, 5), 1)

    try:
        timeout_sec = float(retry_cfg.get("timeout_sec", 45) or 0)
    except Exception:
        timeout_sec = 45.0
    timeout_sec = max(min(timeout_sec, 180.0), 5.0)

    try:
        base_backoff_sec = float(retry_cfg.get("base_backoff_sec", 0.8) or 0)
    except Exception:
        base_backoff_sec = 0.8
    base_backoff_sec = max(min(base_backoff_sec, 10.0), 0.0)

    try:
        max_backoff_sec = float(retry_cfg.get("max_backoff_sec", 6.0) or 0)
    except Exception:
        max_backoff_sec = 6.0
    max_backoff_sec = max(max_backoff_sec, base_backoff_sec)

    def _is_fatal_failure(failure: str) -> bool:
        s = (failure or "").lower()
        fatal_phrases = [
            "insufficient_quota",
            "exceeded your current quota",
            "quota exceeded",
            "billing",
            "payment required",
            "please check your plan",
            "invalid api key",
            "invalid_api_key",
            "incorrect api key",
            "no api key",
            "authentication",
            "unauthorized",
            "forbidden",
            "account has been disabled",
            "organization has been disabled",
        ]
        return any(p in s for p in fatal_phrases)

    def _is_context_too_long(failure: str) -> bool:
        s = (failure or "").lower()
        phrases = [
            "context_length_exceeded",
            "maximum context length",
            "too many tokens",
            "context length",
            "token limit",
        ]
        return any(p in s for p in phrases)

    def _compute_retry_delay(attempt_index: int, failure: str) -> float:
        if attempt_index <= 0:
            return 0.0
        s = (failure or "").lower()
        fast_fail = any(p in s for p in ["empty_response", "empty_or_too_short", "tool_calls"])
        base = 0.2 if fast_fail else base_backoff_sec
        delay = min(max_backoff_sec, base * (2 ** (attempt_index - 1)))
        jitter = min(0.35, delay * 0.25)
        return max(delay + random.uniform(0, jitter), 0.0)

    def _truncate_for_prompt(text: str, limit: int) -> str:
        t = str(text or "")
        if limit <= 0 or len(t) <= limit:
            return t
        return t[: max(limit - 1, 1)].rstrip() + "…"

    def _sample_format_hints() -> tuple[str, str]:
        roll = random.randint(1, 100)
        if roll <= 30:
            return (
                "Длина: 1 короткое предложение, примерно 4-12 слов.",
                "Подача: ленивая бытовая реплика без длинного вступления.",
            )
        if roll <= 68:
            return (
                "Длина: 2 коротких предложения, примерно 10-24 слова.",
                "Подача: первая фраза - реакция, вторая - короткое уточнение по той же мысли.",
            )
        if roll <= 85:
            return (
                "Длина: 2 коротких предложения, примерно 14-30 слов.",
                "Подача: мягкое согласие или сомнение + одна конкретная деталь из поста.",
            )
        return (
            "Длина: 2-3 коротких предложения, примерно 18-36 слов, максимум 4 предложения.",
            "Подача: разверни мысль в 2-3 короткие фразы, без воды и без абзацев.",
        )

    def _sample_question_hint() -> str:
        # Questions should be rare to avoid repetitive "interview style" comments.
        roll = random.randint(1, 100)
        if roll <= 25:
            return "Вопросительный знак допустим, но только один и только если реально уместно."
        return "Предпочти формат без вопросительного знака: утверждение, сомнение или наблюдение."

    def _retry_adjustments(attempt_index: int, failure: str, base_max_tokens: int) -> tuple[str, int, int | None]:
        s = (failure or "").lower()
        extra_lines: list[str] = []
        new_max_tokens = int(base_max_tokens or 0)
        if new_max_tokens <= 0:
            new_max_tokens = 90

        post_char_limit: int | None = None

        if "finish_reason=length" in s:
            extra_lines.append("Ответь короче: 1 короткое предложение (до 160 символов).")
            new_max_tokens = min(max(new_max_tokens, 128), 256)

        if "empty_or_too_short" in s or "empty_response" in s:
            extra_lines.append("Ответ не должен быть пустым. 6–20 слов, строго по теме.")
            new_max_tokens = max(new_max_tokens, 96)

        if _is_context_too_long(s):
            post_char_limit = 3500
            extra_lines.append("Если текст очень длинный — комментируй по одному ключевому тезису, кратко.")

        if attempt_index >= 2:
            extra_lines.append("Не используй markdown. Выведи только текст комментария.")

        return "\n".join([l for l in extra_lines if l]).strip(), int(new_max_tokens), post_char_limit

    last_failure = "unknown_error"
    last_model = None

    post_text_for_prompt = str(post_text or "")
    for attempt in range(max_attempts):
        retry_extra = ""
        attempt_max_tokens_val = max_tokens_val
        if attempt > 0:
            if _is_fatal_failure(last_failure):
                break
            delay = _compute_retry_delay(attempt, last_failure)
            if delay > 0:
                model_part = f"{provider}:{last_model}" if last_model else provider
                logger.info(
                    f"🔁 AI retry {attempt + 1}/{max_attempts} через {delay:.1f}с ({model_part}): {last_failure}"
                )
                await asyncio.sleep(delay)
            retry_extra, attempt_max_tokens_val, post_char_limit = _retry_adjustments(
                attempt, last_failure, max_tokens_val
            )
            if post_char_limit:
                post_text_for_prompt = _truncate_for_prompt(post_text_for_prompt, post_char_limit)
        try:
            final_temp = None
            if custom_temp is not None and str(custom_temp).strip() != "":
                try:
                    final_temp = float(custom_temp)
                except Exception:
                    final_temp = None
            generated_text = None

            length_hint, style_hint = _sample_format_hints()
            question_hint = _sample_question_hint()
            user_message_content = user_template.format(
                context=context_prefix,
                post=post_text_for_prompt,
                length_hint=length_hint,
                style_hint=style_hint,
                question_hint=question_hint,
            )
            if base_extra:
                user_message_content = f"{user_message_content}\n\n{base_extra}"
            if retry_extra:
                user_message_content = f"{user_message_content}\n\n{retry_extra}"

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

                if provider == "deepseek":
                    models_to_try = [get_model_setting(current_settings, "deepseek_chat")]
                elif provider == "openrouter":
                    models_to_try = [get_model_setting(current_settings, "openrouter_chat")]
                else:
                    models_to_try = openai_model_candidates(current_settings, "openai_chat")

                client_kwargs = {"api_key": main_api_key}
                if base_url:
                    client_kwargs["base_url"] = base_url
                if default_headers:
                    client_kwargs["default_headers"] = default_headers
                try:
                    client_kwargs["timeout"] = timeout_sec
                    client = openai.AsyncOpenAI(**client_kwargs)
                except TypeError:
                    client_kwargs.pop("timeout", None)
                    client = openai.AsyncOpenAI(**client_kwargs)
                user_content = user_message_content
                if provider in {"openai", "openrouter"} and image_bytes:
                    mime_type = guess_image_mime_type(image_bytes)
                    base64_image = base64.b64encode(image_bytes).decode('utf-8')
                    user_content = [
                        {"type": "text", "text": user_message_content},
                        {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{base64_image}"}},
                    ]

                completion = None
                for model_name in models_to_try:
                    try:
                        last_model = model_name
                        create_kwargs = {
                            "model": model_name,
                            "messages": [
                                {"role": "system", "content": system_prompt},
                                {"role": "user", "content": user_content},
                            ],
                        }
                        if final_temp is not None:
                            create_kwargs["temperature"] = final_temp
                        if provider != "openrouter":
                            create_kwargs["frequency_penalty"] = frequency_penalty_val
                        if provider == "openai":
                            create_kwargs["max_completion_tokens"] = attempt_max_tokens_val
                        else:
                            create_kwargs["max_tokens"] = attempt_max_tokens_val
                        completion = await asyncio.wait_for(
                            client.chat.completions.create(**create_kwargs), timeout=timeout_sec + 5
                        )
                    except Exception as e:
                        last_failure = _short_exc(e)
                        if provider == "openai" and is_model_unavailable_error(e):
                            continue
                        raise
                    if completion is None:
                        continue

                    try:
                        choice0 = completion.choices[0]
                        msg0 = choice0.message
                    except Exception:
                        choice0 = None
                        msg0 = None

                    finish_reason = getattr(choice0, "finish_reason", None) if choice0 is not None else None
                    refusal = getattr(msg0, "refusal", None) if msg0 is not None else None
                    tool_calls = getattr(msg0, "tool_calls", None) if msg0 is not None else None
                    raw_content = getattr(msg0, "content", None) if msg0 is not None else None

                    raw_text = raw_content if isinstance(raw_content, str) else ""
                    generated_text = raw_text.strip()

                    if generated_text:
                        break

                    # If we received a response but no usable content, classify it and try the next model candidate.
                    out_tokens = None
                    in_tokens = None
                    try:
                        usage = getattr(completion, "usage", None)
                        out_tokens = getattr(usage, "completion_tokens", None) if usage is not None else None
                        in_tokens = getattr(usage, "prompt_tokens", None) if usage is not None else None
                    except Exception:
                        pass

                    if isinstance(refusal, str) and refusal.strip():
                        last_failure = f"refusal: {refusal.strip()[:220]}"
                        break
                    if tool_calls:
                        try:
                            last_failure = f"tool_calls({len(tool_calls)})"
                        except Exception:
                            last_failure = "tool_calls"
                        continue

                    if raw_text and not raw_text.strip():
                        details = []
                        if finish_reason:
                            details.append(f"finish_reason={finish_reason}")
                        if out_tokens is not None:
                            details.append(f"output_tokens={out_tokens}")
                        if in_tokens is not None:
                            details.append(f"prompt_tokens={in_tokens}")
                        detail_str = ", ".join(details)
                        last_failure = f"empty_response(whitespace_only{', ' + detail_str if detail_str else ''})"
                        continue

                    details = []
                    if finish_reason:
                        details.append(f"finish_reason={finish_reason}")
                    if out_tokens is not None:
                        details.append(f"output_tokens={out_tokens}")
                    if in_tokens is not None:
                        details.append(f"prompt_tokens={in_tokens}")
                    if details:
                        last_failure = "empty_response(" + ", ".join(details) + ")"
                    else:
                        last_failure = "empty_response"
                    continue
            elif provider == 'gemini':
                contents = [user_message_content]
                if image_bytes:
                    mime_type = guess_image_mime_type(image_bytes)
                    contents.append(genai_types.Part.from_bytes(data=image_bytes, mime_type=mime_type))

                for model_name in gemini_model_candidates(current_settings, "gemini_chat"):
                    try:
                        last_model = model_name
                        config_kwargs = {
                            "system_instruction": system_prompt,
                            "max_output_tokens": attempt_max_tokens_val,
                        }
                        if final_temp is not None:
                            config_kwargs["temperature"] = final_temp
                        async with genai.Client(api_key=main_api_key).aio as aclient:
                            response = await asyncio.wait_for(
                                aclient.models.generate_content(
                                    model=model_name,
                                    contents=contents,
                                    config=genai_types.GenerateContentConfig(**config_kwargs),
                                ),
                                timeout=timeout_sec + 5,
                            )
                        generated_text = (response.text or "").strip()
                        break
                    except Exception as e:
                        last_failure = _short_exc(e)
                        generated_text = None
                        continue

            if generated_text:
                generated_text = enforce_emoji_level(generated_text, emoji_level)
                clean_gen = generated_text.replace('"', '').replace("'", "").lower().strip()
                if len(clean_gen) < 2:
                    last_failure = "empty_or_too_short(output)"
                    continue
                recent_messages.append(generated_text)
                return generated_text, prompt_info
            if last_failure == "unknown_error":
                last_failure = "empty_or_too_short(output)"
        except Exception as e:
            last_failure = _short_exc(e)
            continue

    model_part = f"{provider}:{last_model}" if last_model else provider
    return None, f"{prompt_info} · FAIL({model_part}): {last_failure}"

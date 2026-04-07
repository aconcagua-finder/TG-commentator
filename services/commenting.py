"""Core commenting logic — process new posts and generate/send comments.

Extracted from commentator.py.
"""

import asyncio
import logging
import random
import time
from datetime import datetime, timezone

from services.account_utils import load_project_accounts, is_bot_awake
from services.comments import generate_comment
from services.connection import (
    ensure_client_connected,
    _is_account_assigned,
    _record_account_failure,
    _clear_account_failure,
)
from services.db_queries import (
    get_daily_action_count_from_db,
    _dt_to_utc,
    _db_get_last_post_time,
    _db_set_last_post_time,
    _select_accounts_for_post,
    log_action_to_db,
    log_comment_skip_to_db,
)
from services.post_utils import (
    download_message_image_bytes as _download_message_image_bytes,
    refetch_post_message as _refetch_post_message,
)
from services.project import get_project_targets
from services.sending import human_type_and_send
from services.text_analysis import (
    normalize_id,
    should_skip_post_for_commenting,
    _normalize_post_text_for_compare,
    _extract_message_text,
    _message_media_fingerprint,
    _stable_shuffled,
    COMMENT_DIVERSITY_MODES,
    SEMANTIC_DIVERSITY_ANGLES,
    build_comment_diversity_instructions,
    build_semantic_diversity_instructions,
    is_comment_too_similar,
    comment_needs_more_novelty,
    make_emergency_comment,
)

logger = logging.getLogger(__name__)


def _db_connect():
    from db.connection import get_connection
    return get_connection()


def _mark_post_processed(unique_id: str, *, post_process_cache: set, post_process_cache_order, post_process_cache_max: int) -> None:
    if not unique_id:
        return
    if unique_id in post_process_cache:
        return
    post_process_cache.add(unique_id)
    post_process_cache_order.append(unique_id)
    while len(post_process_cache_order) > post_process_cache_max:
        old = post_process_cache_order.popleft()
        post_process_cache.discard(old)


async def process_new_post(
    event,
    target_chat,
    from_catch_up=False,
    is_manual=False,
    *,
    active_clients: dict,
    current_settings: dict,
    pending_tasks: set,
    scenario_context: dict,
    processing_cache: set,
    post_process_cache: set,
    post_process_cache_order,
    post_process_cache_max: int,
    spam_blocked_msgs: set | None = None,
    channel_last_post_time: dict,
    recent_generated_messages,
):
    """Process a new post: generate and send AI comments.

    Parameters
    ----------
    active_clients : dict
        session_name -> CommentatorClient mapping.
    current_settings : dict
        Global settings dict.
    pending_tasks : set
        Shared mutable set of pending asyncio tasks.
    scenario_context : dict
        Shared mutable dict for scenario post tracking.
    processing_cache : set
        Shared mutable set of currently-processing unique IDs.
    post_process_cache : set
        Shared mutable set of processed post unique IDs.
    post_process_cache_order : deque
        Shared mutable deque for LRU eviction of post_process_cache.
    post_process_cache_max : int
        Max size for post_process_cache.
    channel_last_post_time : dict
        Shared mutable dict of channel_key -> last post datetime.
    recent_generated_messages : deque
        Shared mutable deque for deduplication.
    """
    task = asyncio.current_task()
    pending_tasks.add(task)
    unique_id = None
    processing_added = False
    any_comment_sent = False

    def _mark_processed(uid):
        _mark_post_processed(
            uid,
            post_process_cache=post_process_cache,
            post_process_cache_order=post_process_cache_order,
            post_process_cache_max=post_process_cache_max,
        )

    try:
        channel_id = target_chat.get('chat_id')
        msg_id = event.message.id
        destination_chat_id_for_logs = event.chat_id
        unique_id = f"{channel_id}_{msg_id}"

        if not is_manual and unique_id in post_process_cache:
            return

        if unique_id in processing_cache:
            return
        processing_cache.add(unique_id)
        processing_added = True

        if not is_manual and isinstance(spam_blocked_msgs, set) and msg_id in spam_blocked_msgs:
            logger.info(f"🛑 Пост {msg_id} пропущен: антиспам (в треде был спам).")
            log_comment_skip_to_db(
                msg_id,
                target_chat,
                destination_chat_id_for_logs,
                "антиспам: спам в треде",
            )
            _mark_processed(unique_id)
            return

        raw_id = str(channel_id)
        norm_id = raw_id.replace('-100', '')
        ids_to_check = [raw_id, norm_id, f"-100{norm_id}"]

        has_scenario = False

        try:
            with _db_connect() as conn:
                cursor = conn.cursor()
                placeholders = ','.join('%s' for _ in ids_to_check)
                query = f"SELECT chat_id, script_content, status FROM scenarios WHERE chat_id IN ({placeholders})"
                cursor.execute(query, ids_to_check)
                row = cursor.fetchone()

                if row:
                    # DictRow inherits from dict, so positional unpacking
                    # would yield column names. Use index access explicitly.
                    found_chat_id, content, status = row[0], row[1], row[2]
                    if content and content.strip() and status != 'stopped':
                        if from_catch_up:
                            _mark_processed(unique_id)
                            return

                        has_scenario = True
                        conn.execute("""
                            INSERT INTO post_scenarios (chat_id, post_id, current_index, last_run_time)
                            VALUES (%s, %s, 0, %s)
                            ON CONFLICT DO NOTHING
                        """, (found_chat_id, msg_id, time.time()))
                        conn.commit()
                        logger.info(f"🎬 [SCENARIO ADD] Новый пост {msg_id} добавлен в сценарий (Chat: {found_chat_id})")
                    else:
                        pass
        except Exception as e:
            logger.error(f"❌ Ошибка SQL при поиске сценария: {e}")

        if has_scenario:
            scenario_context[f"{channel_id}_{msg_id}"] = msg_id
            _mark_processed(unique_id)
            return

        chat_id_check = target_chat.get('chat_id')
        actual_ai_enabled = True
        found_fresh_settings = False
        for t in get_project_targets(current_settings):
            if t.get('chat_id') == chat_id_check:
                actual_ai_enabled = t.get('ai_enabled', True)
                found_fresh_settings = True
                break

        if found_fresh_settings and not actual_ai_enabled and not is_manual:
            _mark_processed(unique_id)
            return
        if not found_fresh_settings and not target_chat.get('ai_enabled', True) and not is_manual:
            _mark_processed(unique_id)
            return

        post_text = str(getattr(event.message, "message", None) or "")
        if not post_text:
            try:
                post_text = str(getattr(event.message, "text", None) or "")
            except Exception:
                post_text = ""

        try:
            min_words = int(target_chat.get("min_word_count", 0) or 0)
        except Exception:
            min_words = 0
        if not is_manual and min_words > 0:
            wc = len(post_text.split())
            if wc < min_words:
                logger.info(f"⏭️ Пост {msg_id} пропущен: слишком короткий ({wc}/{min_words} слов).")
                log_comment_skip_to_db(
                    msg_id,
                    target_chat,
                    destination_chat_id_for_logs,
                    f"слишком короткий ({wc}/{min_words} слов)",
                )
                _mark_processed(unique_id)
                return

        try:
            comment_chance = int(target_chat.get("comment_chance", 100) or 0)
        except Exception:
            comment_chance = 100
        comment_chance = max(min(comment_chance, 100), 0)
        if not is_manual and comment_chance < 100 and random.randint(1, 100) > comment_chance:
            logger.info(f"🙈 Пост {msg_id} пропущен: шанс коммента {comment_chance}%.")
            log_comment_skip_to_db(
                msg_id,
                target_chat,
                destination_chat_id_for_logs,
                f"шанс коммента {comment_chance}%",
            )
            _mark_processed(unique_id)
            return

        if not is_manual:
            try:
                skip, reason = should_skip_post_for_commenting(event.message, post_text, target_chat)
            except Exception:
                skip, reason = False, ""
            if skip:
                logger.info(f"⏭️ Пост {msg_id} пропущен: {reason}.")
                log_comment_skip_to_db(
                    msg_id,
                    target_chat,
                    destination_chat_id_for_logs,
                    str(reason or ""),
                )
                _mark_processed(unique_id)
                return

        accounts_data = load_project_accounts(current_settings)
        eligible_clients = [
            c
            for c in list(active_clients.values())
            if is_bot_awake(next((a for a in accounts_data if a["session_name"] == c.session_name), {}))
            and _is_account_assigned(target_chat, c.session_name)
        ]

        if not eligible_clients:
            log_comment_skip_to_db(
                msg_id,
                target_chat,
                destination_chat_id_for_logs,
                "нет подходящих аккаунтов (все спят / не назначены / не подключены)",
            )
            _mark_processed(unique_id)
            return

        channel_key = normalize_id(target_chat.get("chat_id")) or target_chat.get("chat_id") or event.chat_id

        try:
            min_interval_mins = int(target_chat.get("min_post_interval_mins", 0) or 0)
        except Exception:
            min_interval_mins = 0
        if not is_manual and min_interval_mins > 0:
            msg_date = event.message.date
            if isinstance(msg_date, datetime) and msg_date.tzinfo is None:
                msg_date = msg_date.replace(tzinfo=timezone.utc)

            if channel_key not in channel_last_post_time:
                persisted = _db_get_last_post_time("comment", str(channel_key))
                if persisted:
                    channel_last_post_time[channel_key] = persisted
            last_time = channel_last_post_time.get(channel_key)
            if last_time:
                try:
                    delta_sec = (msg_date - last_time).total_seconds()
                except Exception:
                    delta_sec = None
                if delta_sec is not None and delta_sec < (min_interval_mins * 60):
                    logger.info(
                        f"⏭️ Пост {msg_id} пропущен: мин. интервал {min_interval_mins} мин (прошло {int(delta_sec)} сек)."
                    )
                    log_comment_skip_to_db(
                        msg_id,
                        target_chat,
                        destination_chat_id_for_logs,
                        f"мин. интервал {min_interval_mins} мин (прошло {int(delta_sec)} сек)",
                    )
                    _mark_processed(unique_id)
                    return

            msg_date = _dt_to_utc(msg_date)
            channel_last_post_time[channel_key] = msg_date
            _db_set_last_post_time("comment", str(channel_key), msg_date)

        selected_clients, planned_count, already_count, already_accounts = _select_accounts_for_post(
            chat_key=str(channel_key),
            post_id=int(msg_id),
            destination_chat_id=int(destination_chat_id_for_logs),
            target_chat=target_chat,
            eligible_clients=eligible_clients,
        )
        if not selected_clients:
            if planned_count > 0 and already_count >= planned_count:
                reason = f"уже есть комментарии от {already_count}/{planned_count} аккаунтов"
            elif already_accounts:
                reason = f"уже комментировали: {', '.join(sorted(already_accounts))}"
            else:
                reason = "не нужно выбирать аккаунты (planned_count=0)"
            logger.info(f"⏭️ Пост {msg_id} пропущен: {reason}.")
            log_comment_skip_to_db(msg_id, target_chat, destination_chat_id_for_logs, reason)
            _mark_processed(unique_id)
            return

        eligible_clients = selected_clients

        logger.info(
            f"👥 Аккаунты для коммента поста {msg_id}: {', '.join([c.session_name for c in eligible_clients])}"
        )

        if is_manual:
            wait_time = random.randint(2, 5)
        else:
            base_delay = target_chat.get('initial_comment_delay', 30)
            wait_time = random.randint(base_delay, base_delay + 30)

        logger.info(f"⏳ Жду {wait_time} сек перед генерацией для поста {msg_id}...")
        await asyncio.sleep(wait_time)

        image_bytes = None
        try:
            if getattr(event.message, "photo", None):
                image_bytes = await event.message.download_media(file=bytes)
            else:
                msg_file = getattr(event.message, "file", None)
                mime_type = getattr(msg_file, "mime_type", None) if msg_file else None
                if isinstance(mime_type, str) and mime_type.lower().startswith("image/"):
                    image_bytes = await event.message.download_media(file=bytes)
        except Exception:
            image_bytes = None

        post_media_fingerprint = _message_media_fingerprint(event.message)
        post_last_refresh_at = 0.0

        async def _refresh_post_content(client_wrapper, *, force: bool = False):
            nonlocal post_text, image_bytes, post_media_fingerprint, post_last_refresh_at

            now_ts = time.time()
            if (not force) and post_last_refresh_at and (now_ts - post_last_refresh_at) < 3.0:
                return False
            post_last_refresh_at = now_ts

            latest_msg = await _refetch_post_message(client_wrapper.client, int(event.chat_id), int(msg_id))
            if latest_msg is None:
                return False

            latest_text = _extract_message_text(latest_msg)
            text_changed = _normalize_post_text_for_compare(latest_text) != _normalize_post_text_for_compare(post_text)

            latest_media_fp = _message_media_fingerprint(latest_msg)
            media_changed = latest_media_fp != (post_media_fingerprint or "")
            if media_changed:
                post_media_fingerprint = latest_media_fp
                image_bytes = await _download_message_image_bytes(latest_msg)

            if text_changed:
                post_text = latest_text

            return bool(text_changed or media_changed)

        destination_chat_id_for_logs = event.chat_id
        daily_limit = int(target_chat.get("daily_comment_limit", 999) or 0)
        delay_between = max(int(target_chat.get("delay_between_accounts", 10) or 0), 0)

        h_set = current_settings.get("humanization", {}) or {}
        try:
            similarity_threshold = float(h_set.get("similarity_threshold", 0.78))
        except Exception:
            similarity_threshold = 0.78
        similarity_threshold = max(min(similarity_threshold, 1.0), 0.0)
        try:
            similarity_retries = int(h_set.get("similarity_max_retries", 1) or 0)
        except Exception:
            similarity_retries = 1
        similarity_retries = max(min(similarity_retries, 3), 0)

        try:
            semantic_diversify = bool(h_set.get("short_post_diversify", True))
        except Exception:
            semantic_diversify = True
        try:
            semantic_min_new_tokens = int(h_set.get("short_post_min_new_tokens", 2) or 0)
        except Exception:
            semantic_min_new_tokens = 2
        semantic_min_new_tokens = max(min(semantic_min_new_tokens, 6), 0)

        angle_pool = []
        if semantic_diversify:
            angle_seed = f"angles:{destination_chat_id_for_logs}:{msg_id}"
            angle_pool = _stable_shuffled(SEMANTIC_DIVERSITY_ANGLES, angle_seed)

        sent_comments: list[str] = []
        use_modes = True
        mode_seed = f"modes:{destination_chat_id_for_logs}:{msg_id}"
        mode_pool = _stable_shuffled(COMMENT_DIVERSITY_MODES, mode_seed)

        for idx, client_wrapper in enumerate(eligible_clients):
            try:
                attempted_send = False
                if daily_limit > 0:
                    current_daily_count = get_daily_action_count_from_db(destination_chat_id_for_logs, "comment")
                    if current_daily_count >= daily_limit:
                        logger.info(
                            f"🧾 Лимит комментариев/сутки достигнут ({current_daily_count}/{daily_limit}) для {destination_chat_id_for_logs}. Останавливаюсь."
                        )
                        break

                if not await ensure_client_connected(client_wrapper, reason="comment"):
                    continue

                media_fp_for_generation = post_media_fingerprint
                try:
                    if await _refresh_post_content(client_wrapper):
                        logger.info(f"✏️ Пост {msg_id} обновлён — беру актуальный текст перед комментированием.")
                        media_fp_for_generation = post_media_fingerprint
                except Exception:
                    pass

                mode_hint = None
                if use_modes and mode_pool:
                    mode_hint = mode_pool[idx % len(mode_pool)]

                angle_hint = None
                if semantic_diversify and angle_pool:
                    angle_hint = angle_pool[idx % len(angle_pool)]

                extra_base = build_comment_diversity_instructions(
                    sent_comments,
                    mode_hint=mode_hint,
                )
                short_extra = ""
                if semantic_diversify:
                    short_extra = build_semantic_diversity_instructions(post_text, angle_hint=angle_hint)
                extra = "\n\n".join([p for p in [extra_base, short_extra] if p]).strip()

                post_text_for_generation = post_text
                generated_text = None
                prompt_info = None
                failure_reason = None
                for attempt in range(similarity_retries + 1):
                    candidate, pinfo = await generate_comment(
                        post_text,
                        target_chat,
                        client_wrapper.session_name,
                        image_bytes=image_bytes,
                        extra_instructions=extra,
                        current_settings=current_settings,
                        recent_messages=recent_generated_messages,
                    )
                    if candidate:
                        generated_text, prompt_info = candidate, pinfo
                    else:
                        failure_reason = pinfo or "generation_failed"

                    if not generated_text:
                        break
                    if not sent_comments:
                        break

                    too_similar, score, best = is_comment_too_similar(
                        generated_text, sent_comments, similarity_threshold
                    )
                    needs_novelty = False
                    new_token_count = 0
                    required_new_tokens = 0
                    if semantic_min_new_tokens > 0:
                        required_new_tokens = semantic_min_new_tokens + (1 if len(sent_comments) >= 2 else 0)
                    if semantic_diversify and required_new_tokens > 0:
                        needs_novelty, new_token_count = comment_needs_more_novelty(
                            generated_text,
                            post_text=post_text,
                            existing_comments=sent_comments,
                            min_new_tokens=required_new_tokens,
                        )
                    if (not too_similar) and (not needs_novelty):
                        break

                    if too_similar:
                        failure_reason = f"too_similar(score={score:.2f})"
                        logger.info(
                            f"♻️ [{client_wrapper.session_name}] комментарий слишком похож (score={score:.2f}). Перегенерирую..."
                        )
                    else:
                        failure_reason = f"low_novelty(new_tokens={new_token_count})"
                        logger.info(
                            f"🧩 [{client_wrapper.session_name}] комментарий выглядит как перефраз ({new_token_count} новых слов). Перегенерирую..."
                        )

                    if attempt >= similarity_retries:
                        try:
                            emg = make_emergency_comment(
                                post_text,
                                client_wrapper.session_name,
                                msg_id,
                                existing_comments=sent_comments,
                                threshold=similarity_threshold,
                            )
                        except Exception:
                            emg = ""
                        if emg:
                            generated_text = emg
                            prompt_info = (prompt_info or "comment") + " · EMG"
                            break

                    extra_base = build_comment_diversity_instructions(
                        sent_comments,
                        mode_hint=mode_hint,
                        strict=True,
                        previous_candidate=generated_text,
                    )
                    short_extra = ""
                    if semantic_diversify:
                        short_extra = build_semantic_diversity_instructions(
                            post_text,
                            angle_hint=angle_hint,
                            strict=True,
                            previous_candidate=generated_text,
                        )
                    extra = "\n\n".join([p for p in [extra_base, short_extra] if p]).strip()
                    generated_text = None
                    prompt_info = None

                if not generated_text:
                    reason = failure_reason or prompt_info or "generation_failed"
                    logger.warning(
                        f"⚠️ [{client_wrapper.session_name}] не отправил комментарий к посту {msg_id}: {reason}"
                    )
                    log_action_to_db(
                        {
                            "type": "comment_failed",
                            "post_id": msg_id,
                            "comment": reason,
                            "date": datetime.now(timezone.utc).isoformat(),
                            "account": {"session_name": client_wrapper.session_name},
                            "target": {
                                "chat_name": target_chat.get("chat_name"),
                                "chat_username": target_chat.get("chat_username"),
                                "channel_id": target_chat.get("chat_id"),
                                "destination_chat_id": destination_chat_id_for_logs,
                            },
                        }
                    )
                    continue

                attempted_send = True
                tag_chance = target_chat.get("tag_comment_chance", 50)
                try:
                    tag_chance = int(tag_chance or 0)
                except Exception:
                    tag_chance = 50
                tag_chance = max(min(tag_chance, 100), 0)
                actual_reply_id = msg_id if (is_manual or random.randint(1, 100) <= tag_chance) else None
                thread_top_id = int(msg_id) if actual_reply_id is None else None

                # Re-check the post right before sending: SMM may have edited the text last-minute.
                try:
                    if await _refresh_post_content(client_wrapper, force=True):
                        text_changed = _normalize_post_text_for_compare(post_text) != _normalize_post_text_for_compare(
                            post_text_for_generation
                        )
                        media_changed = post_media_fingerprint != (media_fp_for_generation or "")
                        if text_changed or media_changed:
                            logger.info(f"✏️ Пост {msg_id} изменился прямо перед отправкой — перегенерирую комментарий.")
                            regen_extra_base = build_comment_diversity_instructions(
                                sent_comments,
                                mode_hint=mode_hint,
                                strict=True,
                                previous_candidate=generated_text,
                            )
                            regen_short_extra = ""
                            if semantic_diversify:
                                regen_short_extra = build_semantic_diversity_instructions(
                                    post_text,
                                    angle_hint=angle_hint,
                                    strict=True,
                                    previous_candidate=generated_text,
                                )
                            regen_extra = "\n\n".join([p for p in [regen_extra_base, regen_short_extra] if p]).strip()

                            regen_text, regen_info = await generate_comment(
                                post_text,
                                target_chat,
                                client_wrapper.session_name,
                                image_bytes=image_bytes,
                                extra_instructions=regen_extra,
                                current_settings=current_settings,
                                recent_messages=recent_generated_messages,
                            )
                            if regen_text:
                                generated_text = regen_text
                                prompt_info = (regen_info or prompt_info or "comment") + " · UPD"
                except Exception:
                    pass

                sent_msg = await human_type_and_send(
                    client_wrapper.client,
                    event.chat_id,
                    generated_text,
                    reply_to_msg_id=actual_reply_id,
                    thread_top_msg_id=thread_top_id,
                    split_mode="smart_ru_no_comma",
                    humanization_settings=current_settings.get('humanization', {}),
                )
                any_comment_sent = True
                me = await client_wrapper.client.get_me()
                logger.info(f"✅ [{client_wrapper.session_name}] прокомментировал пост {msg_id} ({prompt_info})")
                sent_comments.append(generated_text)

                log_content = f"[{prompt_info}] {generated_text}"
                log_action_to_db(
                    {
                        "type": "comment",
                        "post_id": msg_id,
                        "msg_id": getattr(sent_msg, "id", None) if sent_msg else None,
                        "comment": log_content,
                        "date": datetime.now(timezone.utc).isoformat(),
                        "account": {
                            "session_name": client_wrapper.session_name,
                            "first_name": me.first_name,
                            "username": me.username,
                        },
                        "target": {
                            "chat_name": target_chat.get("chat_name"),
                            "chat_username": target_chat.get("chat_username"),
                            "channel_id": target_chat.get("chat_id"),
                            "destination_chat_id": destination_chat_id_for_logs,
                        },
                    }
                )
                _clear_account_failure(client_wrapper.session_name, "comment")

                if delay_between > 0 and idx != len(eligible_clients) - 1:
                    await asyncio.sleep(delay_between)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"❌ Ошибка комментирования ({client_wrapper.session_name}): {e}")
                if attempted_send:
                    _record_account_failure(
                        client_wrapper.session_name,
                        "comment",
                        last_error=str(e),
                        last_target=str(destination_chat_id_for_logs),
                        context={
                            "chat_id": str(destination_chat_id_for_logs),
                            "chat_name": target_chat.get("chat_name"),
                            "chat_username": target_chat.get("chat_username"),
                            "post_id": msg_id,
                            "project_id": target_chat.get("project_id"),
                        },
                    )

        if any_comment_sent:
            _mark_processed(unique_id)
    except asyncio.CancelledError:
        pass
    finally:
        if processing_added and unique_id:
            processing_cache.discard(unique_id)
        pending_tasks.discard(task)

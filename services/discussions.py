"""Discussion automation — schedule and run multi-turn discussion sessions.

Extracted from commentator.py.
"""

import asyncio
import configparser
import logging
import os
import random
import re
import time
from datetime import datetime, timezone

from app_paths import CONFIG_FILE
from role_engine import build_role_prompt, role_for_account
from services.account_utils import load_project_accounts, is_bot_awake
from services.connection import (
    ensure_client_connected,
    _record_account_failure,
    _clear_account_failure,
    _channel_bare_id,
)
from services.comments import generate_comment
from services.db_queries import (
    _db_create_discussion_session,
    _db_update_discussion_session,
    _db_add_discussion_message,
    _safe_json_dumps,
    log_action_to_db,
)
from services.profile import _connect_temp_client
from services.project import (
    _active_project_id,
    get_project_discussion_queue,
    get_project_discussion_start_queue,
    get_project_discussion_targets,
    DEFAULT_PROJECT_ID,
)
from services.connection import _extract_discussion_seed_optional_prefix
from services.joining import ensure_account_joined
from services.sending import human_type_and_send
from services.text_analysis import (
    is_comment_too_similar,
    build_comment_diversity_instructions,
    make_emergency_comment,
)

logger = logging.getLogger(__name__)


def _load_config_section(section: str):
    """Load a section from config.ini."""
    parser = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Файл config.ini не найден: {CONFIG_FILE}")
    parser.read(CONFIG_FILE)
    if section not in parser:
        raise KeyError(f"В config.ini не найдена секция [{section}].")
    return parser[section]


def _db_connect():
    """Lazy import to avoid circular dependency."""
    from db.connection import get_connection
    return get_connection()


def _mark_discussion_started(unique_key: str, *, discussion_start_cache: set, discussion_start_cache_order, discussion_start_cache_max: int) -> bool:
    """Check and mark that a discussion has been started (dedup).

    Parameters
    ----------
    discussion_start_cache : set
        Shared mutable set of started unique keys.
    discussion_start_cache_order : deque
        Shared mutable deque for LRU eviction.
    discussion_start_cache_max : int
        Max cache size.
    """
    if not unique_key:
        return False
    if unique_key in discussion_start_cache:
        return False
    discussion_start_cache.add(unique_key)
    discussion_start_cache_order.append(unique_key)
    while len(discussion_start_cache_order) > discussion_start_cache_max:
        old = discussion_start_cache_order.popleft()
        discussion_start_cache.discard(old)
    return True


def schedule_discussion_run(
    *,
    chat_bare_id: int,
    chat_id: int,
    seed_msg_id: int,
    seed_text: str,
    target: dict,
    session_id: int | None = None,
    # Shared mutable state
    active_clients: dict,
    current_settings: dict,
    discussion_active_tasks: dict,
    discussion_start_cache: set,
    discussion_start_cache_order,
    discussion_start_cache_max: int,
    reply_process_cache: set,
    pending_tasks: set,
    discussion_start_suppress_chat_ids: set,
    recent_generated_messages,
    spam_blocked_msgs: set | None = None,
) -> None:
    """Schedule (create asyncio task for) a discussion session.

    Parameters
    ----------
    All shared mutable state dicts/sets are passed explicitly
    to avoid global variable dependencies.
    """
    if not seed_text or not seed_msg_id:
        return
    try:
        chat_bare_id = int(chat_bare_id)
    except Exception:
        return
    if isinstance(spam_blocked_msgs, set) and int(seed_msg_id) in spam_blocked_msgs:
        return

    unique_key = f"discussion:{chat_bare_id}:{seed_msg_id}"
    if not _mark_discussion_started(
        unique_key,
        discussion_start_cache=discussion_start_cache,
        discussion_start_cache_order=discussion_start_cache_order,
        discussion_start_cache_max=discussion_start_cache_max,
    ):
        return

    existing = discussion_active_tasks.get(chat_bare_id)
    if existing is not None and not existing.done():
        logger.info(f"⏭ [discussion] уже идёт обсуждение в чате {chat_bare_id}; пропускаю триггер {seed_msg_id}")
        return

    if session_id is None:
        try:
            project_id = _active_project_id(current_settings)
        except Exception:
            project_id = DEFAULT_PROJECT_ID
        operator_session = str((target or {}).get("operator_session_name") or "").strip() or None
        target_id = str((target or {}).get("id") or "").strip() or None
        settings_snapshot = {"target": target} if isinstance(target, dict) else {"target": {}}
        session_id = _db_create_discussion_session(
            project_id=str(project_id),
            discussion_target_id=target_id,
            discussion_target_chat_id=str((target or {}).get("chat_id") or "").strip() or str(chat_id),
            chat_id=str(chat_id),
            status="running",
            operator_session_name=operator_session,
            seed_msg_id=int(seed_msg_id),
            seed_text=str(seed_text),
            settings=settings_snapshot,
        )
        if session_id:
            _db_add_discussion_message(
                session_id=int(session_id),
                speaker_type="operator",
                speaker_session_name=operator_session,
                speaker_label="Оператор",
                msg_id=int(seed_msg_id),
                reply_to_msg_id=None,
                text=str(seed_text),
            )

    reply_process_cache.add(seed_msg_id)
    task = asyncio.create_task(
        run_discussion_session(
            chat_id=chat_id,
            chat_bare_id=chat_bare_id,
            seed_msg_id=seed_msg_id,
            seed_text=seed_text,
            target=target,
            session_id=int(session_id) if session_id else None,
            active_clients=active_clients,
            current_settings=current_settings,
            pending_tasks=pending_tasks,
            reply_process_cache=reply_process_cache,
            discussion_start_suppress_chat_ids=discussion_start_suppress_chat_ids,
            recent_generated_messages=recent_generated_messages,
        )
    )
    try:
        setattr(task, "discussion_session_id", int(session_id) if session_id else None)
    except Exception:
        pass
    discussion_active_tasks[chat_bare_id] = task

    def _cleanup(done_task: asyncio.Task) -> None:  # noqa: ANN001
        cur = discussion_active_tasks.get(chat_bare_id)
        if cur is done_task:
            discussion_active_tasks.pop(chat_bare_id, None)

    task.add_done_callback(_cleanup)


async def run_discussion_session(
    *,
    chat_id: int,
    chat_bare_id: int,
    seed_msg_id: int,
    seed_text: str,
    target: dict,
    session_id: int | None = None,
    # Shared mutable state
    active_clients: dict,
    current_settings: dict,
    pending_tasks: set,
    reply_process_cache: set,
    discussion_start_suppress_chat_ids: set,
    recent_generated_messages,
) -> None:
    """Run a multi-turn discussion session.

    Parameters
    ----------
    All shared mutable state dicts/sets are passed explicitly
    to avoid global variable dependencies.
    """
    current_task = asyncio.current_task()
    if current_task:
        pending_tasks.add(current_task)

    try:
        seed_text = str(seed_text or "").strip()
        if not seed_text:
            return

        session_id_int: int | None = None
        try:
            session_id_int = int(session_id) if session_id else None
        except Exception:
            session_id_int = None

        if session_id_int:
            try:
                _db_update_discussion_session(session_id_int, status="running", started_at=float(time.time()))
            except Exception:
                pass

        target = target if isinstance(target, dict) else {}
        operator_session = str(target.get("operator_session_name") or "").strip()
        base_vector = str(target.get("vector_prompt") or "").strip()

        extra_scenes_raw = target.get("scenes")
        extra_scenes: list[dict] = []
        if isinstance(extra_scenes_raw, list):
            extra_scenes = [sc for sc in extra_scenes_raw if isinstance(sc, dict)]
        total_scenes = 1 + len(extra_scenes)

        def _int_setting_from(
            scene: dict,
            key: str,
            default: int,
            *,
            min_value: int | None = None,
            max_value: int | None = None,
        ) -> int:
            raw = None
            if isinstance(scene, dict) and key in scene:
                raw = scene.get(key)
            if raw is None or (isinstance(raw, str) and raw.strip() == ""):
                raw = target.get(key, default)
            if raw is None or (isinstance(raw, str) and raw.strip() == ""):
                raw = default
            try:
                value = int(raw)
            except Exception:
                value = int(default)
            if min_value is not None:
                value = max(value, int(min_value))
            if max_value is not None:
                value = min(value, int(max_value))
            return int(value)

        def _vector_for(scene: dict) -> str:
            v = str((scene or {}).get("vector_prompt") or "").strip()
            return v if v else base_vector

        def _str_setting_from(scene: dict, key: str, default: str) -> str:
            raw = None
            if isinstance(scene, dict) and key in scene:
                raw = scene.get(key)
            if raw is None or (isinstance(raw, str) and not raw.strip()):
                raw = target.get(key, default)
            if raw is None or (isinstance(raw, str) and not raw.strip()):
                raw = default
            return str(raw).strip()

        def _assigned_accounts_for(scene: dict) -> list[str]:
            raw = (scene or {}).get("assigned_accounts")
            if isinstance(raw, list):
                items = [str(s).strip() for s in raw if str(s).strip()]
                if items:
                    return items
            return [str(s).strip() for s in (target.get("assigned_accounts") or []) if str(s).strip()]

        accounts_data = load_project_accounts(current_settings)
        account_by_session = {
            str(a.get("session_name")).strip(): a
            for a in (accounts_data or [])
            if isinstance(a, dict) and a.get("session_name")
        }

        labels: dict[str, str] = {}
        next_label = {"idx": 1}
        participants_snapshot: list[dict] = []
        participants_seen: set[str] = set()
        excluded_sessions: set[str] = set()

        def _ensure_labels_for(clients: list) -> None:
            for c in clients:
                sess = str(getattr(c, "session_name", "") or "").strip()
                if not sess:
                    continue
                if sess not in labels:
                    labels[sess] = f"Участник {next_label['idx']}"
                    next_label["idx"] += 1

        def _update_participants_snapshot(clients: list) -> None:
            if not session_id_int:
                return
            changed_participants = False
            for c in clients:
                sess = str(getattr(c, "session_name", "") or "").strip()
                if not sess or sess in participants_seen:
                    continue
                participants_seen.add(sess)
                acc_conf = account_by_session.get(sess, {}) if isinstance(account_by_session, dict) else {}
                role_id, role_data = role_for_account(acc_conf or {}, current_settings)
                role_prompt, role_meta = build_role_prompt(role_data or {}, current_settings)
                participants_snapshot.append(
                    {
                        "session_name": sess,
                        "label": labels.get(sess),
                        "role_id": role_id,
                        "role_name": str((role_data or {}).get("name") or role_id or "Роль"),
                        "role_prompt": role_prompt,
                        "role_meta": role_meta,
                        "persona_id": acc_conf.get("persona_id") if isinstance(acc_conf, dict) else None,
                    }
                )
                changed_participants = True
            if changed_participants:
                try:
                    _db_update_discussion_session(
                        session_id_int,
                        participants_json=_safe_json_dumps(participants_snapshot),
                    )
                except Exception:
                    pass

        def _eligible_clients_for(assigned_list: list[str]) -> list:
            eligible: list = []
            for client_wrapper in list(active_clients.values()):
                session_name = str(getattr(client_wrapper, "session_name", "") or "").strip()
                if not session_name or session_name not in assigned_list:
                    continue
                if session_name in excluded_sessions:
                    continue
                if operator_session and session_name == operator_session:
                    continue
                acc_conf = account_by_session.get(session_name)
                if acc_conf and is_bot_awake(acc_conf):
                    eligible.append(client_wrapper)
            return eligible

        last_speaker = "Оператор"
        history: list[dict[str, str]] = []
        discussion_bot_texts: list[str] = []
        reply_to_msg_id: int = int(seed_msg_id)
        last_operator_msg_id: int | None = int(seed_msg_id)
        last_sender_session: str | None = None

        def _float_setting_from(
            scene: dict,
            key: str,
            default: float,
            *,
            min_value: float | None = None,
            max_value: float | None = None,
        ) -> float:
            raw = None
            if isinstance(scene, dict) and key in scene:
                raw = scene.get(key)
            if raw is None or (isinstance(raw, str) and str(raw).strip() == ""):
                raw = target.get(key, default)
            if raw is None or (isinstance(raw, str) and str(raw).strip() == ""):
                raw = default
            try:
                value = float(raw)
            except Exception:
                value = float(default)
            if min_value is not None:
                value = max(value, float(min_value))
            if max_value is not None:
                value = min(value, float(max_value))
            return float(value)

        def _bool_setting_from(scene: dict, key: str, default: bool) -> bool:
            raw = None
            if isinstance(scene, dict) and key in scene:
                raw = scene.get(key)
            if raw is None:
                raw = target.get(key, default)
            if isinstance(raw, bool):
                return raw
            if isinstance(raw, (int, float)):
                return bool(raw)
            if isinstance(raw, str):
                return raw.strip().lower() in {"1", "true", "yes", "on", "y", "t"}
            return bool(default)

        def _truncate_memory_line(value: str, limit: int = 280) -> str:
            s = re.sub(r"\s+", " ", str(value or "")).strip()
            if limit > 0 and len(s) > limit:
                return s[: limit - 1].rstrip() + "…"
            return s

        def _build_memory_block(items: list[dict[str, str]], max_items: int, *, exclude_last: bool = True) -> str:
            if max_items <= 0:
                return ""
            mem = items[-max_items:] if items else []
            if exclude_last and mem:
                mem = mem[:-1]
            lines: list[str] = []
            for it in mem:
                speaker = str(it.get("speaker") or "Участник").strip() or "Участник"
                text = _truncate_memory_line(str(it.get("text") or ""))
                if text:
                    lines.append(f"{speaker}: {text}")
            return "\n".join(lines).strip()

        for scene_number, scene in enumerate([{}, *extra_scenes], start=1):
            scene_title = str((scene or {}).get("title") or "").strip()
            scene_vector = _vector_for(scene)
            assigned = _assigned_accounts_for(scene)

            if not assigned:
                logger.info(f"⏭ [discussion] чат {chat_bare_id}: нет assigned_accounts — обсуждение не запускаю")
                if session_id_int and scene_number == 1:
                    try:
                        _db_update_discussion_session(
                            session_id_int,
                            status="failed",
                            finished_at=float(time.time()),
                            error="no_assigned_accounts",
                        )
                    except Exception:
                        pass
                return

            eligible_clients = _eligible_clients_for(assigned)
            if not eligible_clients:
                if scene_number == 1:
                    logger.info(f"⏭ [discussion] чат {chat_bare_id}: нет доступных аккаунтов‑участников")
                    if session_id_int:
                        try:
                            _db_update_discussion_session(
                                session_id_int,
                                status="failed",
                                finished_at=float(time.time()),
                                error="no_available_participants",
                            )
                        except Exception:
                            pass
                    return
                logger.info(f"⏭ [discussion] сцена {scene_number}/{total_scenes} пропущена: нет доступных участников")
                continue

            random.shuffle(eligible_clients)
            _ensure_labels_for(eligible_clients)
            _update_participants_snapshot(eligible_clients)

            if scene_number == 1:
                scene_seed_text = seed_text
                if not history:
                    history.append({"speaker": "Оператор", "text": scene_seed_text})
            else:
                operator_text = str((scene or {}).get("operator_text") or "").strip()
                if not operator_text:
                    logger.info(f"⏭ [discussion] сцена {scene_number}/{total_scenes} пропущена: пустая фраза оператора")
                    continue

                scene_operator = str((scene or {}).get("operator_session_name") or "").strip() or operator_session
                if not scene_operator:
                    logger.warning(
                        f"⚠️ [discussion] сцена {scene_number}/{total_scenes}: не задан operator_session_name — остановка"
                    )
                    break

                quote_mode = _str_setting_from(scene, "operator_quote_mode", "operator_prev")
                if quote_mode == "operator_prev":
                    prev_reply_to = int(last_operator_msg_id) if last_operator_msg_id else None
                elif quote_mode == "none":
                    prev_reply_to = None
                else:
                    prev_reply_to = int(reply_to_msg_id) if reply_to_msg_id else None
                op_wrapper = active_clients.get(scene_operator) if scene_operator else None
                temp_client = None
                op_client = None
                if op_wrapper is not None and getattr(op_wrapper, "client", None) is not None:
                    if not await ensure_client_connected(op_wrapper, reason="discussion_scene"):
                        raise RuntimeError("operator_connect_failed")
                    op_client = op_wrapper.client
                else:
                    telethon_config = _load_config_section('telethon_credentials')
                    api_id, api_hash = int(telethon_config['api_id']), telethon_config['api_hash']
                    acc_conf = account_by_session.get(scene_operator)
                    if not acc_conf:
                        raise KeyError("operator_account_not_found")
                    temp_client = await _connect_temp_client(acc_conf, api_id, api_hash)
                    op_client = temp_client

                sent_op = None
                try:
                    discussion_start_suppress_chat_ids.add(int(chat_bare_id))
                    sent_op = await human_type_and_send(
                        op_client,
                        chat_id,
                        operator_text,
                        reply_to_msg_id=prev_reply_to,
                        skip_processing=True,
                        split_mode="off",
                        humanization_settings=current_settings.get('humanization', {}),
                    )
                finally:
                    try:
                        discussion_start_suppress_chat_ids.discard(int(chat_bare_id))
                    except Exception:
                        pass
                    if temp_client is not None:
                        try:
                            if temp_client.is_connected():
                                await temp_client.disconnect()
                        except Exception:
                            pass

                op_msg_id = getattr(sent_op, "id", None)
                if not op_msg_id:
                    logger.warning(f"⚠️ [discussion] сцена {scene_number}/{total_scenes}: не удалось отправить фразу оператора")
                    break

                try:
                    reply_process_cache.add(int(op_msg_id))
                except Exception:
                    pass

                if session_id_int:
                    try:
                        _db_add_discussion_message(
                            session_id=session_id_int,
                            speaker_type="operator",
                            speaker_session_name=str(scene_operator or "").strip() or None,
                            speaker_label="Оператор",
                            msg_id=int(op_msg_id),
                            reply_to_msg_id=int(prev_reply_to) if prev_reply_to else None,
                            text=str(operator_text),
                            prompt_info=f"sc{scene_number}/{total_scenes}",
                        )
                    except Exception:
                        pass

                reply_to_msg_id = int(op_msg_id)
                last_operator_msg_id = int(op_msg_id)
                last_speaker = "Оператор"
                scene_seed_text = operator_text
                history.append({"speaker": "Оператор", "text": operator_text.strip()})

            turns_min = _int_setting_from(scene, "turns_min", 6, min_value=1, max_value=200)
            turns_max = _int_setting_from(scene, "turns_max", 10, min_value=1, max_value=200)
            if turns_max < turns_min:
                turns_max = turns_min
            total_turns = random.randint(turns_min, turns_max)

            start_delay_min = _int_setting_from(scene, "initial_delay_min", 10, min_value=0, max_value=86400)
            start_delay_max = _int_setting_from(scene, "initial_delay_max", 40, min_value=0, max_value=86400)
            if start_delay_max < start_delay_min:
                start_delay_max = start_delay_min

            between_delay_min = _int_setting_from(scene, "delay_between_min", 20, min_value=0, max_value=86400)
            between_delay_max = _int_setting_from(scene, "delay_between_max", 80, min_value=0, max_value=86400)
            if between_delay_max < between_delay_min:
                between_delay_max = between_delay_min

            if start_delay_max > 0:
                await asyncio.sleep(random.uniform(float(start_delay_min), float(start_delay_max)))

            for turn_idx in range(total_turns):
                if turn_idx > 0 and between_delay_max > 0:
                    await asyncio.sleep(random.uniform(float(between_delay_min), float(between_delay_max)))

                # Pick next speaker; avoid immediate repeats when possible.
                candidates = [c for c in eligible_clients if c.session_name != last_sender_session] or list(eligible_clients)
                random.shuffle(candidates)
                client_wrapper = None

                for cand in list(candidates):
                    if not await ensure_client_connected(cand, reason="discussion"):
                        _record_account_failure(
                            cand.session_name,
                            "discussion",
                            last_error="connect_failed",
                            last_target=str(chat_id),
                            context={
                                "chat_id": str(chat_id),
                                "chat_name": target.get("chat_name"),
                                "chat_username": target.get("chat_username"),
                                "post_id": seed_msg_id,
                                "project_id": target.get("project_id"),
                            },
                        )
                        excluded_sessions.add(str(cand.session_name))
                        eligible_clients = [c for c in eligible_clients if c.session_name != cand.session_name]
                        continue
                    client_wrapper = cand
                    break

                if client_wrapper is None:
                    logger.warning(
                        f"⚠️ [discussion] сцена {scene_number}/{total_scenes}: нет доступных участников для реплики {turn_idx + 1}/{total_turns}"
                    )
                    break

                memory_turns = _int_setting_from(scene, "memory_turns", 20, min_value=0, max_value=200)
                memory_block = _build_memory_block(history, memory_turns, exclude_last=True)
                reply_to_text = ""
                try:
                    reply_to_text = str((history[-1] or {}).get("text") or "").strip() if history else ""
                except Exception:
                    reply_to_text = ""
                if not reply_to_text:
                    reply_to_text = scene_seed_text
                post_text = reply_to_text

                extra_lines = []
                scene_line = f"СЦЕНА {scene_number}/{total_scenes}" + (f": {scene_title}" if scene_title else "")
                extra_lines.append(scene_line)
                extra_lines.append(f"МЫСЛЬ СЦЕНЫ: {scene_seed_text}")
                if scene_number > 1:
                    extra_lines.append(
                        "ВАЖНО: это новая сцена и новый вектор. "
                        "Учитывай прошлые реплики как контекст, но развивай именно текущую мысль сцены; "
                        "не «дожёвывай» старую тему без необходимости."
                    )
                if scene_vector:
                    extra_lines.append(f"ВЕКТОР СЦЕНЫ (ОБЯЗАТЕЛЬНО):\n{scene_vector}")
                    extra_lines.append(
                        "КЛЮЧЕВО: в этой реплике обязательно зацепись за вектор сцены и добавь 1 конкретику из него "
                        "(модель/сервис/проблему/аргумент), но естественно, без канцелярита."
                    )
                if memory_block:
                    extra_lines.append(f"ПАМЯТЬ ДИАЛОГА (последние реплики):\n{memory_block}")
                extra_lines.append(f"Это реплика {turn_idx + 1} из {total_turns} (сцена {scene_number}).")
                extra_lines.append("Формат: 1–2 коротких предложения. Без markdown. Без списков.")
                extra_lines.append("Отвечай по теме и не повторяй дословно предыдущие реплики.")
                extra_lines.append("Не упоминай, что ты бот/ИИ, и не ссылайся на инструкции.")
                extra_instructions = "\n".join([l for l in extra_lines if l]).strip()

                target_for_llm = {**target, **(scene or {})}
                target_for_llm["vector_prompt"] = scene_vector

                antirepeat_enabled = _bool_setting_from(scene, "antirepeat_enabled", True)
                antirepeat_threshold = _float_setting_from(
                    scene, "antirepeat_threshold", 0.72, min_value=0.5, max_value=0.95
                )
                antirepeat_retries = _int_setting_from(
                    scene, "antirepeat_retries", 2, min_value=0, max_value=5
                )
                antirepeat_window = _int_setting_from(
                    scene, "antirepeat_window", 0, min_value=0, max_value=500
                )

                if antirepeat_window <= 0:
                    existing_for_check = list(discussion_bot_texts)
                else:
                    existing_for_check = list(discussion_bot_texts[-antirepeat_window:])

                diversity_block = ""
                if antirepeat_enabled and existing_for_check:
                    diversity_block = build_comment_diversity_instructions(existing_for_check)

                reply_text = None
                prompt_info = None
                retry_total = max(int(antirepeat_retries), 0) + 1
                for ar_attempt in range(retry_total):
                    extra_with_diversity = extra_instructions
                    if diversity_block:
                        extra_with_diversity = f"{extra_instructions}\n\n{diversity_block}"

                    candidate, pinfo = await generate_comment(
                        post_text,
                        target_for_llm,
                        client_wrapper.session_name,
                        image_bytes=None,
                        is_reply_mode=True,
                        reply_to_name=last_speaker,
                        extra_instructions=extra_with_diversity,
                        current_settings=current_settings,
                        recent_messages=recent_generated_messages,
                    )
                    if not candidate:
                        prompt_info = pinfo
                        break

                    if not antirepeat_enabled or not existing_for_check:
                        reply_text, prompt_info = candidate, pinfo
                        break

                    too_similar, score, _best = is_comment_too_similar(
                        candidate, existing_for_check, antirepeat_threshold
                    )
                    if not too_similar:
                        reply_text, prompt_info = candidate, pinfo
                        break

                    logger.info(
                        f"♻️ [{client_wrapper.session_name}] discussion reply too similar "
                        f"(score={score:.2f}), retry {ar_attempt + 1}/{antirepeat_retries}"
                    )
                    diversity_block = build_comment_diversity_instructions(
                        existing_for_check,
                        strict=True,
                        previous_candidate=candidate,
                    )
                    reply_text, prompt_info = None, pinfo

                if not reply_text and antirepeat_enabled and existing_for_check:
                    try:
                        emg = make_emergency_comment(
                            post_text=post_text,
                            session_name=client_wrapper.session_name,
                            msg_id=int(seed_msg_id),
                            existing_comments=existing_for_check,
                            threshold=antirepeat_threshold,
                        )
                    except Exception:
                        emg = ""
                    if emg:
                        reply_text = emg
                        prompt_info = (prompt_info or "discussion") + " · EMG"

                if not reply_text:
                    logger.warning(f"⚠️ [{client_wrapper.session_name}] discussion turn skipped: {prompt_info}")
                    _record_account_failure(
                        client_wrapper.session_name,
                        "discussion",
                        last_error=str(prompt_info or "generation_failed"),
                        last_target=str(chat_id),
                        context={
                            "chat_id": str(chat_id),
                            "chat_name": target.get("chat_name"),
                            "chat_username": target.get("chat_username"),
                            "post_id": seed_msg_id,
                            "project_id": target.get("project_id"),
                        },
                    )
                    continue

                scene_tag = f"sc{scene_number}/{total_scenes}"
                prompt_info_str = str(prompt_info or "").strip()
                prompt_info_out = (f"{prompt_info_str} {scene_tag}").strip()

                sent_msg = await human_type_and_send(
                    client_wrapper.client,
                    chat_id,
                    reply_text,
                    reply_to_msg_id=reply_to_msg_id,
                    split_mode="smart_ru_no_comma",
                    humanization_settings=current_settings.get('humanization', {}),
                )
                if sent_msg is None or getattr(sent_msg, "id", None) is None:
                    _record_account_failure(
                        client_wrapper.session_name,
                        "discussion",
                        last_error="send_failed",
                        last_target=str(chat_id),
                        context={
                            "chat_id": str(chat_id),
                            "chat_name": target.get("chat_name"),
                            "chat_username": target.get("chat_username"),
                            "post_id": seed_msg_id,
                            "project_id": target.get("project_id"),
                        },
                    )
                    excluded_sessions.add(str(client_wrapper.session_name))
                    eligible_clients = [c for c in eligible_clients if c.session_name != client_wrapper.session_name]
                    continue

                me = None
                try:
                    me = await client_wrapper.client.get_me()
                except Exception:
                    me = None

                logger.info(
                    f"💬 [{client_wrapper.session_name}] discussion {turn_idx + 1}/{total_turns} in {chat_bare_id} ({prompt_info_out})"
                )

                msg_id = getattr(sent_msg, "id", None)
                if session_id_int:
                    try:
                        _db_add_discussion_message(
                            session_id=session_id_int,
                            speaker_type="bot",
                            speaker_session_name=str(client_wrapper.session_name),
                            speaker_label=labels.get(client_wrapper.session_name),
                            msg_id=int(msg_id) if msg_id else None,
                            reply_to_msg_id=int(reply_to_msg_id) if reply_to_msg_id else None,
                            text=str(reply_text or ""),
                            prompt_info=str(prompt_info_out or ""),
                        )
                    except Exception:
                        pass

                try:
                    log_action_to_db(
                        {
                            "type": "discussion",
                            "post_id": seed_msg_id,
                            "comment": f"[{prompt_info_out}] {reply_text}",
                            "date": datetime.now(timezone.utc).isoformat(),
                            "account": {
                                "session_name": client_wrapper.session_name,
                                "first_name": getattr(me, "first_name", "") if me else "",
                                "username": getattr(me, "username", "") if me else "",
                            },
                            "target": {
                                "chat_name": target.get("chat_name"),
                                "chat_username": target.get("chat_username"),
                                "destination_chat_id": chat_id,
                            },
                        }
                    )
                except Exception:
                    pass

                _clear_account_failure(client_wrapper.session_name, "discussion")
                if msg_id:
                    try:
                        reply_to_msg_id = int(msg_id)
                        reply_process_cache.add(int(msg_id))
                    except Exception:
                        pass

                speaker_label = labels.get(client_wrapper.session_name, "Участник")
                history.append({"speaker": speaker_label, "text": reply_text.strip()})
                discussion_bot_texts.append(reply_text.strip())
                last_speaker = speaker_label
                last_sender_session = client_wrapper.session_name
    except asyncio.CancelledError:
        if session_id_int:
            try:
                _db_update_discussion_session(
                    session_id_int,
                    status="canceled",
                    finished_at=float(time.time()),
                    error="canceled",
                )
            except Exception:
                pass
        raise
    except Exception as e:
        logger.error(f"❌ [discussion] ошибка в чате {chat_bare_id}: {e}")
        if session_id_int:
            try:
                _db_update_discussion_session(
                    session_id_int,
                    status="failed",
                    finished_at=float(time.time()),
                    error=str(e),
                )
            except Exception:
                pass
    finally:
        if session_id_int:
            try:
                row = None
                with _db_connect() as conn:
                    row = conn.execute(
                        "SELECT status FROM discussion_sessions WHERE id = %s",
                        (int(session_id_int),),
                    ).fetchone()
                cur_status = ""
                if row is not None:
                    try:
                        cur_status = str(row["status"] or "")
                    except Exception:
                        cur_status = ""
                if cur_status == "running":
                    _db_update_discussion_session(
                        session_id_int,
                        status="completed",
                        finished_at=float(time.time()),
                    )
            except Exception:
                pass
        if current_task:
            pending_tasks.discard(current_task)


async def process_discussion_queue(
    *,
    current_settings: dict,
    active_clients: dict,
    discussion_active_tasks: dict,
    discussion_start_cache: set,
    discussion_start_cache_order,
    discussion_start_cache_max: int,
    reply_process_cache: set,
    pending_tasks: set,
    discussion_start_suppress_chat_ids: set,
    recent_generated_messages,
    save_settings_fn,
):
    """Process queued discussion triggers (seed already sent, just schedule).

    Parameters
    ----------
    save_settings_fn : callable
        Called as save_settings_fn() to persist current_settings after queue changes.
    All shared mutable state dicts/sets are passed explicitly.
    """
    queue = current_settings.get("discussion_queue")
    if not isinstance(queue, list) or not queue:
        return

    tasks = get_project_discussion_queue(current_settings)
    if not tasks:
        return

    tasks_to_remove: list[dict] = []

    for task in tasks:
        try:
            target_id = str(task.get("discussion_target_id") or "").strip()
            target_chat_id = str(task.get("discussion_target_chat_id") or "").strip()
            chat_id_raw = str(task.get("chat_id") or "").strip()
            seed_text = str(task.get("seed_text") or "").strip()
            seed_msg_id_raw = task.get("seed_msg_id")

            if not ((target_id or target_chat_id) and chat_id_raw and seed_text and seed_msg_id_raw):
                tasks_to_remove.append(task)
                continue

            all_targets = get_project_discussion_targets(current_settings)
            target: dict | None = None
            if target_id:
                for t in all_targets:
                    if str(t.get("id") or "").strip() == target_id:
                        target = t
                        break
            else:
                chat_matches = [
                    t for t in all_targets if str(t.get("chat_id") or "").strip() == target_chat_id
                ]
                if len(chat_matches) == 1:
                    target = chat_matches[0]
                elif len(chat_matches) > 1:
                    logger.warning(
                        f"⚠️ [discussion_queue] ambiguous target for chat_id={target_chat_id}: need discussion_target_id in queue task"
                    )
                    tasks_to_remove.append(task)
                    continue

            if not target:
                tasks_to_remove.append(task)
                continue

            try:
                chat_id_int = int(chat_id_raw)
            except Exception:
                tasks_to_remove.append(task)
                continue

            try:
                seed_msg_id = int(seed_msg_id_raw)
            except Exception:
                tasks_to_remove.append(task)
                continue

            try:
                chat_bare_id = int(str(chat_id_int).replace("-100", ""))
            except Exception:
                chat_bare_id = chat_id_int

            schedule_discussion_run(
                chat_bare_id=chat_bare_id,
                chat_id=chat_id_int,
                seed_msg_id=seed_msg_id,
                seed_text=seed_text,
                target=target,
                active_clients=active_clients,
                current_settings=current_settings,
                discussion_active_tasks=discussion_active_tasks,
                discussion_start_cache=discussion_start_cache,
                discussion_start_cache_order=discussion_start_cache_order,
                discussion_start_cache_max=discussion_start_cache_max,
                reply_process_cache=reply_process_cache,
                pending_tasks=pending_tasks,
                discussion_start_suppress_chat_ids=discussion_start_suppress_chat_ids,
                recent_generated_messages=recent_generated_messages,
            )
            tasks_to_remove.append(task)
        except Exception as e:
            logger.error(f"Ошибка в discussion_queue: {e}")
            tasks_to_remove.append(task)

    if tasks_to_remove:
        new_queue = [t for t in queue if t not in tasks_to_remove]
        current_settings["discussion_queue"] = new_queue
        save_settings_fn()


def _should_try_other_discussion_chat(exc: Exception) -> bool:
    """Check if the send error is a permissions issue worth retrying with another chat."""
    name = (exc.__class__.__name__ or "").lower()
    text = str(exc).lower()
    tokens = [
        "chatadminrequired",
        "chat admin privileges",
        "chat_admin_required",
        "chatwriteforbidden",
        "chat_write_forbidden",
        "chat_send_plain_forbidden",
        "chat_send",
        "write forbidden",
        "you can't write",
        "cannot write",
        "not enough rights",
        "sendmessagerequest",
        "channelprivate",
        "channel private",
        "channelinvalid",
        "channel invalid",
        "userbannedinchannel",
        "user banned in channel",
    ]
    return any(t in name or t in text for t in tokens)


async def process_discussion_start_queue(
    *,
    current_settings: dict,
    active_clients: dict,
    discussion_active_tasks: dict,
    discussion_start_cache: set,
    discussion_start_cache_order,
    discussion_start_cache_max: int,
    discussion_start_suppress_chat_ids: set,
    reply_process_cache: set,
    pending_tasks: set,
    recent_generated_messages,
    joined_cache: set,
    save_settings_fn,
):
    """Process the discussion_start_queue — send seed messages and start discussions.

    Parameters
    ----------
    save_settings_fn : callable
        Called as save_settings_fn() to persist current_settings after queue changes.
    joined_cache : set
        Shared mutable set for ensure_account_joined.
    All shared mutable state dicts/sets are passed explicitly.
    """
    queue = current_settings.get("discussion_start_queue")
    if not isinstance(queue, list) or not queue:
        return

    tasks = get_project_discussion_start_queue(current_settings)
    if not tasks:
        return

    now_ts = time.time()
    tasks_to_remove: list[dict] = []
    tasks_updated = False

    for task in tasks:
        try:
            try:
                next_retry_at = float(task.get("next_retry_at") or 0.0)
            except Exception:
                next_retry_at = 0.0
            if next_retry_at and now_ts < next_retry_at:
                continue

            target_id = str(task.get("discussion_target_id") or "").strip()
            target_chat_id = str(task.get("discussion_target_chat_id") or "").strip()
            seed_text = str(task.get("seed_text") or "").strip()
            operator_session = str(task.get("operator_session_name") or "").strip()
            force_restart = bool(task.get("force_restart", False))

            if not ((target_id or target_chat_id) and seed_text and operator_session):
                tasks_to_remove.append(task)
                continue

            all_targets = get_project_discussion_targets(current_settings)
            target: dict | None = None
            if target_id:
                for t in all_targets:
                    if str(t.get("id") or "").strip() == target_id:
                        target = t
                        break
            else:
                chat_matches = [
                    t for t in all_targets if str(t.get("chat_id") or "").strip() == target_chat_id
                ]
                if len(chat_matches) == 1:
                    target = chat_matches[0]
                elif len(chat_matches) > 1:
                    logger.warning(
                        f"⚠️ [discussion_start] ambiguous target for chat_id={target_chat_id}: need discussion_target_id in queue task"
                    )
                    tasks_to_remove.append(task)
                    continue

            if not target or not bool(target.get("enabled", True)):
                tasks_to_remove.append(task)
                continue

            if not target_id:
                target_id = str(target.get("id") or "").strip()
            if not target_chat_id:
                target_chat_id = str(target.get("chat_id") or "").strip()

            # Prefer the current operator setting from the target if present.
            operator_from_target = str(target.get("operator_session_name") or "").strip()
            if operator_from_target:
                operator_session = operator_from_target

            session_id_int: int | None
            try:
                session_id_int = int(task.get("session_id") or 0) or None
            except Exception:
                session_id_int = None

            if session_id_int is None:
                try:
                    project_id = _active_project_id(current_settings)
                except Exception:
                    project_id = DEFAULT_PROJECT_ID

                base_chat_id = (
                    str(task.get("chat_id") or "").strip()
                    or str(target.get("linked_chat_id") or "").strip()
                    or str(target.get("chat_id") or "").strip()
                )
                session_id_int = _db_create_discussion_session(
                    project_id=str(project_id),
                    discussion_target_id=str(target_id) or None,
                    discussion_target_chat_id=str(target_chat_id),
                    chat_id=str(base_chat_id or target_chat_id),
                    status="planned",
                    operator_session_name=operator_session,
                    seed_text=seed_text,
                    settings={"target": target},
                )
                if session_id_int:
                    task["session_id"] = int(session_id_int)
                    tasks_updated = True

            client_wrapper = active_clients.get(operator_session)
            temp_client = None
            client = None

            if client_wrapper is not None and getattr(client_wrapper, "client", None) is not None:
                if not await ensure_client_connected(client_wrapper, reason="discussion_start"):
                    raise RuntimeError("connect_failed")
                client = client_wrapper.client
            else:
                telethon_config = _load_config_section("telethon_credentials")
                api_id, api_hash = int(telethon_config["api_id"]), telethon_config["api_hash"]
                accounts_data = load_project_accounts(current_settings)
                account_data = next(
                    (a for a in accounts_data if str(a.get("session_name") or "").strip() == operator_session),
                    None,
                )
                if not account_data:
                    raise KeyError("operator_account_not_found")
                temp_client = await _connect_temp_client(account_data, api_id, api_hash)
                client = temp_client

            try:
                # Ensure the operator can write in the discussion chat (best-effort).
                try:
                    if client_wrapper is not None and getattr(client_wrapper, "client", None) is not None:
                        await ensure_account_joined(client_wrapper, target, force=True, joined_cache=joined_cache)
                    else:
                        pseudo = type("_Tmp", (), {"session_name": operator_session, "client": client})()
                        await ensure_account_joined(pseudo, target, force=True, joined_cache=joined_cache)
                except Exception:
                    pass

                candidate_raw = [
                    str(task.get("chat_id") or "").strip(),
                    str(target.get("linked_chat_id") or "").strip(),
                    str(target.get("chat_id") or "").strip(),
                ]
                candidate_chat_ids: list[int] = []
                seen: set[int] = set()
                for raw in candidate_raw:
                    if not raw:
                        continue
                    try:
                        cid = int(raw)
                    except Exception:
                        continue
                    if cid in seen:
                        continue
                    seen.add(cid)
                    candidate_chat_ids.append(cid)

                if not candidate_chat_ids:
                    tasks_to_remove.append(task)
                    continue

                # Prevent double-start when the seed message we send triggers the outbound message handler.
                suppressed_chat_bare_ids: list[int] = []
                try:
                    for cid in candidate_chat_ids:
                        bare = _channel_bare_id(str(cid))
                        if bare is None:
                            continue
                        b = int(bare)
                        suppressed_chat_bare_ids.append(b)
                        discussion_start_suppress_chat_ids.add(b)
                except Exception:
                    suppressed_chat_bare_ids = []

                sent_msg = None
                sent_chat_id_int: int | None = None
                last_send_exc: Exception | None = None
                try:
                    for idx, cid in enumerate(candidate_chat_ids):
                        try:
                            sent_msg = await asyncio.wait_for(
                                client.send_message(int(cid), seed_text),
                                timeout=35.0,
                            )
                            sent_chat_id_int = int(cid)
                            break
                        except Exception as exc:
                            last_send_exc = exc
                            if idx < (len(candidate_chat_ids) - 1) and _should_try_other_discussion_chat(exc):
                                continue
                            raise
                finally:
                    for b in suppressed_chat_bare_ids:
                        try:
                            discussion_start_suppress_chat_ids.discard(int(b))
                        except Exception:
                            pass

                if sent_msg is None or sent_chat_id_int is None:
                    raise last_send_exc or RuntimeError("send_failed")

                try:
                    seed_msg_id = int(getattr(sent_msg, "id", None) or 0) or None
                except Exception:
                    seed_msg_id = None
                if not seed_msg_id:
                    raise RuntimeError("missing_msg_id")

                start_prefix = str(target.get("start_prefix") or "")
                seed_clean = (
                    _extract_discussion_seed_optional_prefix(seed_text, start_prefix) or seed_text
                ).strip()

                chat_bare_id = _channel_bare_id(str(sent_chat_id_int))
                if chat_bare_id is None:
                    chat_bare_id = int(str(sent_chat_id_int).replace("-100", "").replace("-", ""))

                if force_restart:
                    existing = discussion_active_tasks.get(int(chat_bare_id))
                    if existing is not None and not existing.done():
                        try:
                            prev_sid = getattr(existing, "discussion_session_id", None)
                            if prev_sid:
                                _db_update_discussion_session(
                                    int(prev_sid),
                                    status="canceled",
                                    finished_at=float(time.time()),
                                    error="force_restart",
                                )
                        except Exception:
                            pass
                        existing.cancel()
                        try:
                            await asyncio.wait_for(existing, timeout=2.0)
                        except Exception:
                            pass
                        discussion_active_tasks.pop(int(chat_bare_id), None)

                if session_id_int:
                    try:
                        _db_update_discussion_session(
                            int(session_id_int),
                            discussion_target_id=str(target_id) or None,
                            status="running",
                            started_at=float(time.time()),
                            chat_id=str(sent_chat_id_int),
                            operator_session_name=operator_session,
                            seed_msg_id=int(seed_msg_id),
                            seed_text=seed_clean,
                            schedule_at=None,
                            error=None,
                        )
                        _db_add_discussion_message(
                            session_id=int(session_id_int),
                            speaker_type="operator",
                            speaker_session_name=operator_session,
                            speaker_label="Оператор",
                            msg_id=int(seed_msg_id),
                            reply_to_msg_id=None,
                            text=str(seed_text),
                        )
                    except Exception:
                        pass

                schedule_discussion_run(
                    chat_bare_id=int(chat_bare_id),
                    chat_id=int(sent_chat_id_int),
                    seed_msg_id=int(seed_msg_id),
                    seed_text=seed_clean,
                    target=target,
                    session_id=int(session_id_int) if session_id_int else None,
                    active_clients=active_clients,
                    current_settings=current_settings,
                    discussion_active_tasks=discussion_active_tasks,
                    discussion_start_cache=discussion_start_cache,
                    discussion_start_cache_order=discussion_start_cache_order,
                    discussion_start_cache_max=discussion_start_cache_max,
                    reply_process_cache=reply_process_cache,
                    pending_tasks=pending_tasks,
                    discussion_start_suppress_chat_ids=discussion_start_suppress_chat_ids,
                    recent_generated_messages=recent_generated_messages,
                )
                logger.info(
                    f"🗣 [discussion_start] operator {operator_session} sent msg_id={seed_msg_id} in {chat_bare_id}"
                )
                tasks_to_remove.append(task)
            finally:
                if temp_client is not None:
                    try:
                        if temp_client.is_connected():
                            await temp_client.disconnect()
                    except Exception:
                        pass
        except Exception as e:
            tries = 0
            try:
                tries = int(task.get("tries", 0) or 0)
            except Exception:
                tries = 0
            tries += 1
            task["tries"] = tries
            task["last_error"] = str(e)
            backoff = min(60 * max(1, tries), 600)
            next_retry_at = float(time.time() + backoff)
            task["next_retry_at"] = next_retry_at
            tasks_updated = True
            logger.error(f"❌ [discussion_start] ошибка: {e} (retry in {backoff}s)")

            max_tries = 10
            sid_raw = task.get("session_id")
            sid_int = None
            try:
                sid_int = int(sid_raw) if sid_raw else None
            except Exception:
                sid_int = None
            if sid_int:
                try:
                    if tries >= max_tries:
                        _db_update_discussion_session(
                            int(sid_int),
                            status="failed",
                            finished_at=float(time.time()),
                            error=str(e),
                        )
                    else:
                        _db_update_discussion_session(
                            int(sid_int),
                            status="planned",
                            schedule_at=float(next_retry_at),
                            error=str(e),
                        )
                except Exception:
                    pass
            if tries >= max_tries:
                tasks_to_remove.append(task)

    if tasks_to_remove:
        new_queue = [t for t in queue if t not in tasks_to_remove]
        current_settings["discussion_start_queue"] = new_queue
        tasks_updated = True

    if tasks_updated:
        save_settings_fn()

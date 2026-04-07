"""Scenario execution — process scripted comment sequences on posts.

Extracted from commentator.py.
"""

import asyncio
import logging
import random
import re
import time
from datetime import datetime, timezone

from services.account_utils import load_project_accounts, is_bot_awake
from services.connection import _is_account_active
from services.db_queries import (
    _scenario_history_load,
    _scenario_history_set,
    _scenario_history_clear,
    log_action_to_db,
)
from services.project import get_project_targets
from services.sending import human_type_and_send

logger = logging.getLogger(__name__)

# Module-level state (replaces process_scenarios function attributes).
_last_log_time: dict = {}
_msg_history: dict = {}


def _db_connect():
    """Lazy import to avoid circular dependency."""
    from db.connection import get_connection
    return get_connection()


async def process_scenarios(
    *,
    active_clients: dict,
    current_settings: dict,
):
    """Execute pending scenario steps.

    Parameters
    ----------
    active_clients : dict
        session_name -> CommentatorClient mapping.
    current_settings : dict
        Global settings dict.
    """
    tasks_to_process = []

    try:
        with _db_connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT ps.id, CAST(ps.chat_id AS TEXT) as chat_id, ps.post_id, ps.current_index, ps.last_run_time, s.script_content
                FROM post_scenarios ps
                JOIN scenarios s ON CAST(ps.chat_id AS TEXT) = CAST(s.chat_id AS TEXT)
                WHERE s.status != 'stopped'
            """)
            rows = cursor.fetchall()
            for row in rows:
                tasks_to_process.append(dict(row))
    except Exception as e:
        logger.error(f"❌ Ошибка чтения БД сценариев: {e}")
        return

    if not tasks_to_process:
        return

    accounts_data = load_project_accounts(current_settings)
    ordered_accounts = [acc for acc in accounts_data if _is_account_active(acc)]

    all_targets = (current_settings.get('targets', []) or []) if isinstance(current_settings, dict) else []

    for task in tasks_to_process:
        row_id = task['id']
        chat_id_str = task['chat_id']
        post_id = task['post_id']
        idx = task['current_index']
        last_run = task['last_run_time']
        content = task['script_content']

        target_settings = None
        for t in get_project_targets(current_settings):
            t_id = str(t.get('chat_id'))
            if t_id == chat_id_str or t_id.replace('-100', '') == chat_id_str.replace('-100', ''):
                target_settings = t
                break

        if not target_settings:
            has_any_target = False
            for t in all_targets:
                t_id = str(t.get('chat_id'))
                if t_id == chat_id_str or t_id.replace('-100', '') == chat_id_str.replace('-100', ''):
                    has_any_target = True
                    break
            if not has_any_target:
                with _db_connect() as conn:
                    conn.execute("DELETE FROM post_scenarios WHERE id = %s", (row_id,))
                _scenario_history_clear(chat_id_str, post_id)
            continue

        destination_id_str = target_settings.get('linked_chat_id', target_settings.get('chat_id'))

        lines = [l.strip() for l in content.split('\n') if l.strip()]

        if idx >= len(lines):
            with _db_connect() as conn:
                conn.execute("DELETE FROM post_scenarios WHERE id = %s", (row_id,))

            hist_key = f"{chat_id_str}_{post_id}"
            if hist_key in _msg_history:
                del _msg_history[hist_key]
            _scenario_history_clear(chat_id_str, post_id)

            logger.info(f"🏁 Сценарий для поста {post_id} завершен.")
            continue

        line = lines[idx]

        match = re.search(r'\[(\d+)\]\s*[\|¦]?\s*([\d\.,]+)\s*[-–—]\s*([\d\.,]+)[сcCcSsа-яА-Яa-zA-Z]*\s*[\|¦]?\s*(.+)',
                          line)

        if not match:
            logger.warning(f"⚠️ [SKIP] Неверный формат строки {idx + 1}: '{line}'")
            with _db_connect() as conn:
                conn.execute("UPDATE post_scenarios SET current_index = current_index + 1 WHERE id = %s", (row_id,))
            continue

        acc_idx_raw = int(match.group(1))
        min_delay = float(match.group(2).replace(',', '.'))
        max_delay = float(match.group(3).replace(',', '.'))
        text = match.group(4).strip()

        time_passed = time.time() - last_run
        log_key = f"{row_id}_{idx}"

        if time_passed < min_delay:
            if time.time() - _last_log_time.get(log_key, 0) > 10:
                logger.info(f"⏳ [WAIT] Пост {post_id}: Шаг {idx + 1}. Ждем еще {min_delay - time_passed:.1f}с")
                _last_log_time[log_key] = time.time()
            continue

        if log_key in _last_log_time:
            del _last_log_time[log_key]

        logger.info(f"🚀 [START] Пост {post_id}: Начинаю выполнение шага {idx + 1}...")

        acc_id = acc_idx_raw - 1
        client_wrapper = None
        session_name = "Unknown"

        if 0 <= acc_id < len(ordered_accounts):
            session_name = ordered_accounts[acc_id]['session_name']
            client_wrapper = active_clients.get(session_name)

        if not client_wrapper:
            if active_clients:
                client_wrapper = random.choice(list(active_clients.values()))
                session_name = client_wrapper.session_name
                logger.warning(f"⚠️ Аккаунт {acc_idx_raw} недоступен, подменил на {session_name}")
            else:
                logger.error("❌ Нет активных клиентов для сценария.")
                with _db_connect() as conn:
                    conn.execute("UPDATE post_scenarios SET current_index = current_index + 1 WHERE id = %s", (row_id,))
                continue

        try:
            hist_key = f"{chat_id_str}_{post_id}"
            if hist_key not in _msg_history:
                _msg_history[hist_key] = _scenario_history_load(chat_id_str, post_id)

            reply_to_id = post_id
            use_reply_mode = target_settings.get('scenario_reply_mode', False)

            tags = re.findall(r'\{(\d+)\}', text)
            for t_num in tags:
                ref_idx = int(t_num)

                if ref_idx in _msg_history[hist_key]:
                    reply_to_id = _msg_history[hist_key][ref_idx]

                text = text.replace(f"{{{t_num}}}", "")
                text = re.sub(f"@{re.escape('{' + t_num + '}')}", "", text)

            text = " ".join(text.split())

            if not use_reply_mode and not tags:
                reply_to_id = None

            logger.info(f"🔍 [{session_name}] Ищу чат {destination_id_str}...")
            norm_dest_id = int(str(destination_id_str).replace('-100', ''))

            try:
                entity = await asyncio.wait_for(
                    client_wrapper.client.get_input_entity(norm_dest_id),
                    timeout=15.0
                )
            except asyncio.TimeoutError:
                logger.error(f"❌ [{session_name}] Тайм-аут поиска чата. Пропускаю шаг.")
                with _db_connect() as conn:
                    conn.execute("UPDATE post_scenarios SET current_index = current_index + 1 WHERE id = %s", (row_id,))
                continue
            except Exception as e:
                try:
                    entity = await client_wrapper.client.get_entity(norm_dest_id)
                except:
                    logger.error(f"❌ [{session_name}] Чат не найден: {e}")
                    with _db_connect() as conn:
                        conn.execute("UPDATE post_scenarios SET current_index = current_index + 1 WHERE id = %s",
                                     (row_id,))
                    continue

            wait_real = random.uniform(0, max(0, max_delay - min_delay))
            if wait_real > 0:
                logger.info(f"⏱ [{session_name}] Пауза перед вводом {wait_real:.1f}с...")
                await asyncio.sleep(wait_real)

            logger.info(f"✍️ [{session_name}] Печатает сообщение...")

            sent_msg = await human_type_and_send(client_wrapper.client, entity, text, reply_to_msg_id=reply_to_id, skip_processing=True, humanization_settings=current_settings.get('humanization', {}))

            if sent_msg:
                logger.info(f"✅ [{session_name}] УСПЕШНО отправил: {text[:20]}...")

                _msg_history[hist_key][acc_idx_raw] = sent_msg.id
                _scenario_history_set(chat_id_str, post_id, acc_idx_raw, sent_msg.id)

                me = await client_wrapper.client.get_me()
                log_action_to_db({
                    'type': 'comment',
                    'post_id': post_id,
                    'comment': f"[SCENARIO STEP {idx + 1}] {text}",
                    'date': datetime.now(timezone.utc).isoformat(),
                    'account': {'session_name': session_name, 'first_name': me.first_name, 'username': me.username},
                    'target': {'chat_name': 'Scenario', 'destination_chat_id': destination_id_str}
                })

            with _db_connect() as conn:
                conn.execute(
                    "UPDATE post_scenarios SET current_index = current_index + 1, last_run_time = %s WHERE id = %s",
                    (time.time(), row_id))

        except Exception as e:
            logger.error(f"❌ Ошибка выполнения шага (Post {post_id}): {e}", exc_info=True)
            with _db_connect() as conn:
                conn.execute("UPDATE post_scenarios SET current_index = current_index + 1 WHERE id = %s", (row_id,))

import configparser
import json
import logging
import asyncio
import random
import httpx
import os
import re
import base64
import time
import collections
import sqlite3
from datetime import datetime, timezone

import google.generativeai as genai
import openai
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import (
    UserDeactivatedBanError, FloodWaitError, ChatWriteForbiddenError,
    ChannelPrivateError, ChatAdminRequiredError, ReactionInvalidError,
    ReactionsTooManyError, UserAlreadyParticipantError, InviteHashExpiredError,
    InviteHashInvalidError
)
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from telethon.tl.functions.messages import SendReactionRequest, ImportChatInviteRequest, GetDiscussionMessageRequest
from telethon.tl.functions.photos import UploadProfilePhotoRequest, DeletePhotosRequest
from telethon.tl.functions.account import UpdateProfileRequest
from telethon.tl.types import ReactionEmoji

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_START_TIME = datetime.now(timezone.utc)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SETTINGS_FILE = os.path.join(BASE_DIR, 'ai_settings.json')
ACCOUNTS_FILE = os.path.join(BASE_DIR, 'accounts.json')
PROXIES_FILE = os.path.join(BASE_DIR, 'proxies.txt')
DB_FILE = os.path.join(BASE_DIR, 'actions.sqlite')
OLD_LOGS_FILE = os.path.join(BASE_DIR, 'comment_logs.json')

current_settings = {}
active_clients = {}
handled_posts_for_comments = collections.deque(maxlen=500)
handled_posts_for_reactions = collections.deque(maxlen=500)
handled_posts_for_monitoring = collections.deque(maxlen=500)
handled_grouped_ids = collections.deque(maxlen=200)
CHANNEL_LAST_POST_TIME = {}
MONITOR_CHANNEL_LAST_POST_TIME = {}
EVENT_HANDLER_LOCK = asyncio.Lock()
LATEST_CHANNEL_POSTS = {}
JOINED_CACHE = set()
PROCESSING_CACHE = set()
PROCESSED_BURST_IDS = set()
CHAT_REPLY_COOLDOWN = {}
REPLY_PROCESS_CACHE = set()
POST_PROCESS_CACHE = set()
PENDING_TASKS = set()
SCENARIO_CONTEXT = {}
CLIENT_CATCH_UP_STATUS = set()
RECENT_GENERATED_MESSAGES = collections.deque(maxlen=100)


def is_bot_awake(account_data):
    now = datetime.now().hour
    start_hour = account_data.get('sleep_settings', {}).get('start_hour', 8)
    end_hour = account_data.get('sleep_settings', {}).get('end_hour', 23)

    if start_hour == end_hour:
        return True

    if start_hour < end_hour:
        if start_hour <= now < end_hour:
            return True
    else:
        if now >= start_hour or now < end_hour:
            return True

    return False


def init_database():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('PRAGMA journal_mode=WAL;')
            conn.execute('PRAGMA synchronous=NORMAL;')

            conn.execute('''
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, log_type TEXT NOT NULL, timestamp TEXT NOT NULL,
                    destination_chat_id INTEGER NOT NULL, channel_name TEXT, channel_username TEXT,
                    source_channel_id INTEGER, post_id INTEGER NOT NULL, account_session_name TEXT,
                    account_first_name TEXT, account_username TEXT, content TEXT
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS proxies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT NOT NULL UNIQUE,
                    ip TEXT,
                    country TEXT,
                    status TEXT,
                    last_check TEXT
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS scenarios (
                    chat_id TEXT PRIMARY KEY,
                    script_content TEXT,
                    current_index INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'stopped',
                    last_run_time REAL DEFAULT 0
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS post_scenarios (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id TEXT,
                    post_id INTEGER,
                    current_index INTEGER DEFAULT 0,
                    last_run_time REAL DEFAULT 0,
                    UNIQUE(chat_id, post_id)
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS triggers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id TEXT NOT NULL,
                    trigger_phrase TEXT NOT NULL,
                    answer_text TEXT NOT NULL
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS alert_context (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id TEXT,
                    msg_id INTEGER,
                    session_name TEXT,
                    created_at REAL
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS outbound_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id TEXT,
                    reply_to_msg_id INTEGER,
                    session_name TEXT,
                    text TEXT,
                    status TEXT DEFAULT 'pending'
                )
            ''')

            conn.execute("DELETE FROM post_scenarios")
            conn.commit()

    except sqlite3.Error as e:
        logger.critical(f"Критическая ошибка при инициализации БД: {e}")
        exit()


def migrate_json_to_sqlite(conn):
    logger.warning("Обнаружен старый файл comment_logs.json. Начинаю миграцию данных в SQLite...")
    try:
        with open(OLD_LOGS_FILE, 'r', encoding='utf-8') as f:
            old_logs = json.load(f)

        cursor = conn.cursor()
        migrated_count = 0
        for chat_id, data in old_logs.items():
            for log in data.get('all_logs', []):
                content = ""
                if log.get('type') == 'reaction':
                    content = ' '.join(log.get('reactions', []))
                else:
                    content = log.get('comment', '')

                cursor.execute('''
                    INSERT INTO logs (
                        log_type, timestamp, destination_chat_id, channel_name, channel_username,
                        source_channel_id, post_id, account_session_name, account_first_name,
                        account_username, content
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    log.get('type', 'comment'),
                    log.get('date'),
                    log.get('target', {}).get('destination_chat_id', chat_id),
                    log.get('target', {}).get('chat_name'),
                    log.get('target', {}).get('chat_username'),
                    log.get('target', {}).get('channel_id'),
                    log.get('post_id'),
                    log.get('account', {}).get('session_name'),
                    log.get('account', {}).get('first_name'),
                    log.get('account', {}).get('username'),
                    content
                ))
                migrated_count += 1
        conn.commit()
        logger.info(f"Миграция завершена. Перенесено {migrated_count} записей.")
        os.rename(OLD_LOGS_FILE, f"{OLD_LOGS_FILE}.migrated")
        logger.info(f"Старый файл логов переименован в {OLD_LOGS_FILE}.migrated")
    except Exception as e:
        logger.error(f"Ошибка во время миграции данных: {e}")


def get_daily_action_count_from_db(chat_id, action_type='comment'):
    try:
        chat_id_str = str(chat_id).replace('-100', '')

        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')

            variants = [chat_id, int(chat_id_str), int(f"-100{chat_id_str}")]
            placeholders = ','.join('?' for _ in variants)

            query = f'''
                SELECT COUNT(*) FROM logs 
                WHERE log_type = ? 
                AND destination_chat_id IN ({placeholders}) 
                AND timestamp LIKE ?
            '''

            args = [action_type] + variants + [f"{today_str}%"]

            cursor.execute(query, args)
            result = cursor.fetchone()
            return result[0] if result else 0
    except Exception as e:
        logger.error(f"Ошибка получения счетчика из БД: {e}")
        return 9999


def check_if_already_commented(destination_chat_id, post_id):
    try:
        chat_id_str = str(destination_chat_id).replace('-100', '')
        norm_id = int(chat_id_str)

        variants = set()
        variants.add(norm_id)
        variants.add(str(norm_id))
        variants.add(int(f"-100{norm_id}"))
        variants.add(f"-100{norm_id}")

        variants.add(destination_chat_id)
        variants.add(str(destination_chat_id))

        placeholders = ','.join('?' for _ in variants)

        query = f'''
            SELECT COUNT(*) FROM logs 
            WHERE (post_id = ? OR post_id = ?)
            AND destination_chat_id IN ({placeholders}) 
            AND log_type IN ('comment', 'comment_reply', 'forbidden')
        '''

        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            args = [post_id, str(post_id)] + list(variants)

            cursor.execute(query, args)
            result = cursor.fetchone()
            return result[0] > 0
    except Exception as e:
        logger.error(f"Ошибка БД при проверке комментария: {e}")
        return True


def log_action_to_db(log_entry):
    content = ""
    if log_entry.get('type') == 'reaction':
        content = ' '.join(log_entry.get('reactions', []))
    else:
        content = log_entry.get('comment', '')

    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO logs (
                    log_type, timestamp, destination_chat_id, channel_name, channel_username,
                    source_channel_id, post_id, account_session_name, account_first_name,
                    account_username, content
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                log_entry.get('type'),
                log_entry.get('date'),
                log_entry.get('target', {}).get('destination_chat_id'),
                log_entry.get('target', {}).get('chat_name'),
                log_entry.get('target', {}).get('chat_username'),
                log_entry.get('target', {}).get('channel_id'),
                log_entry.get('post_id'),
                log_entry.get('account', {}).get('session_name'),
                log_entry.get('account', {}).get('first_name'),
                log_entry.get('account', {}).get('username'),
                content
            ))
            conn.commit()
        logger.info(
            f"Подробный лог ({log_entry.get('type')}) сохранен в БД для аккаунта {log_entry.get('account', {}).get('session_name')}")
    except sqlite3.Error as e:
        logger.error(f"Ошибка при записи лога в БД: {e}")


def load_config(section):
    parser = configparser.ConfigParser()
    if not os.path.exists('config.ini'):
        raise FileNotFoundError("Файл config.ini не найден.")
    parser.read('config.ini')
    if section not in parser:
        raise KeyError(f"В config.ini не найдена секция [{section}].")
    return parser[section]


def load_json_data(file_path, default_data=None):
    if default_data is None:
        default_data = {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default_data


def load_proxies():
    if not os.path.exists(PROXIES_FILE):
        logger.warning(f"Файл {PROXIES_FILE} не найден")
        return []
    try:
        with open(PROXIES_FILE, 'r') as f:
            proxies = [line.strip() for line in f if line.strip()]
            logger.debug(f"Загружено {len(proxies)} прокси из {PROXIES_FILE}")
            return proxies
    except Exception as e:
        logger.error(f"Ошибка при чтении файла с прокси {PROXIES_FILE}: {e}")
        return []


async def generate_comment(post_text, target_chat, session_name, image_bytes=None, is_reply_mode=False, reply_to_name=None):
    global current_settings, RECENT_GENERATED_MESSAGES
    provider = target_chat.get('ai_provider', 'default')
    if provider == 'default':
        provider = current_settings.get('ai_provider', 'gemini')
    accounts_data = load_json_data(ACCOUNTS_FILE, [])
    account = next((a for a in accounts_data if a['session_name'] == session_name), None)
    user_persona = None
    prompt_info = "Общий промпт"
    if account and 'persona_id' in account:
        p_id = str(account['persona_id'])
        personas = current_settings.get('personas', {})
        if p_id in personas:
            user_persona = personas[p_id].get('prompt', '')
            prompt_info = f"Роль: {personas[p_id].get('name')}"
    if not user_persona:
        prompts_dict = target_chat.get('prompts', {})
        if session_name in prompts_dict:
            user_persona = prompts_dict[session_name]
            prompt_info = "Персональный промпт"
        else:
            user_persona = prompts_dict.get('default', target_chat.get('prompt', ''))
    if not user_persona:
        user_persona = "Ты обычный пользователь Telegram. Общайся кратко, на 'ты'."
    global_blacklist = current_settings.get('blacklist', [])
    h_set = current_settings.get('humanization', {})
    custom_rules = h_set.get('custom_rules', "")
    raw_penalty = float(h_set.get('repetition_penalty', 0))
    frequency_penalty_val = min(max(raw_penalty / 50, 0.0), 2.0)
    max_tokens_val = int(h_set.get('max_tokens', 100))
    custom_temp = h_set.get('temperature')
    system_prompt = (
        f"ТВОЯ РОЛЬ (ОТЫГРЫВАЙ ЕЕ ДОСЛОВНО):\n{user_persona}\n\n"
        f"ПРАВИЛА ОФОРМЛЕНИЯ ТЕКСТА:\n{custom_rules}\n"
    )
    if global_blacklist:
        system_prompt += f"\nНЕ ИСПОЛЬЗУЙ СЛОВА: {', '.join(global_blacklist)}"
    if is_reply_mode:
        context_prefix = f"ТЕБЕ ГОВОРИТ {reply_to_name}: " if reply_to_name else ""
        user_message_content = f"КОНТЕКСТ ДИАЛОГА:\n{context_prefix}{post_text}\n\nОтветь согласно своей роли:"
    else:
        user_message_content = f"ТЕКСТ ПОСТА:\n{post_text}\n\nНапиши комментарий от своей роли:"
    api_keys = current_settings.get('api_keys', {})
    main_api_key = api_keys.get(provider)
    if not main_api_key: return None, None
    for attempt in range(3):
        try:
            final_temp = (float(custom_temp) if custom_temp is not None else 1.0)
            if provider == 'openai' or provider == 'deepseek':
                base_url = "https://api.deepseek.com" if provider == 'deepseek' else None
                model_name = "deepseek-chat" if provider == 'deepseek' else "gpt-4o"
                client = openai.AsyncOpenAI(api_key=main_api_key, base_url=base_url)
                completion = await client.chat.completions.create(
                    model=model_name,
                    messages=[{"role": "system", "content": system_prompt},
                              {"role": "user", "content": user_message_content}],
                    max_tokens=max_tokens_val,
                    temperature=final_temp,
                    frequency_penalty=frequency_penalty_val
                )
                generated_text = completion.choices[0].message.content.strip()
            elif provider == 'gemini':
                genai.configure(api_key=main_api_key)
                model = genai.GenerativeModel('gemini-1.5-flash', system_instruction=system_prompt)
                response = await model.generate_content_async(user_message_content,
                                                              generation_config=genai.types.GenerationConfig(
                                                                  temperature=final_temp,
                                                                  max_output_tokens=max_tokens_val))
                generated_text = response.text.strip()
            if generated_text:
                clean_gen = generated_text.replace('"', '').replace("'", "").lower().strip()
                if len(clean_gen) < 2: continue
                RECENT_GENERATED_MESSAGES.append(generated_text)
                return generated_text, prompt_info
        except Exception:
            continue
    return None, None


def normalize_id(chat_id):
    if not chat_id:
        return 0
    try:
        return int(str(chat_id).replace('-100', ''))
    except ValueError:
        return 0


async def catch_up_missed_posts(client_wrapper, target_chat):
    global POST_PROCESS_CACHE, handled_grouped_ids, PENDING_TASKS
    task = asyncio.current_task()
    PENDING_TASKS.add(task)
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

        for message in messages_to_scan:
            if message.grouped_id:
                if check_if_already_commented(destination_chat_id, message.id):
                    if message.grouped_id not in handled_grouped_ids:
                        handled_grouped_ids.append(message.grouped_id)

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
            if unique_process_id in POST_PROCESS_CACHE:
                continue

            if check_if_already_commented(destination_chat_id, message.id):
                continue

            POST_PROCESS_CACHE.add(unique_process_id)
            logger.info(f"💡 [CATCH-UP] Нашел свежий пропущенный пост {message.id} в {chat_name}")

            try:
                await client_wrapper.client.send_read_acknowledge(entity, message=message)
            except:
                pass

            event_mock = collections.namedtuple('EventMock', ['message', 'chat_id'])
            mock_event = event_mock(message=message, chat_id=destination_chat_id)

            await process_new_post(mock_event, target_chat, from_catch_up=True)
            await asyncio.sleep(random.randint(2, 5))

    except asyncio.CancelledError:
        pass
    finally:
        PENDING_TASKS.discard(task)


async def process_new_post(event, target_chat, from_catch_up=False, is_manual=False):
    global active_clients, POST_PROCESS_CACHE, PENDING_TASKS, current_settings, SCENARIO_CONTEXT
    task = asyncio.current_task()
    PENDING_TASKS.add(task)
    try:
        channel_id = target_chat.get('chat_id')
        msg_id = event.message.id
        unique_id = f"{channel_id}_{msg_id}"

        if not from_catch_up and not is_manual:
            if unique_id in POST_PROCESS_CACHE: return
            POST_PROCESS_CACHE.add(unique_id)

        raw_id = str(channel_id)
        norm_id = raw_id.replace('-100', '')
        ids_to_check = [raw_id, norm_id, f"-100{norm_id}"]

        has_scenario = False

        try:
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                placeholders = ','.join('?' for _ in ids_to_check)
                query = f"SELECT chat_id, script_content, status FROM scenarios WHERE chat_id IN ({placeholders})"
                cursor.execute(query, ids_to_check)
                row = cursor.fetchone()

                if row:
                    found_chat_id, content, status = row
                    if content and content.strip() and status != 'stopped':
                        if from_catch_up:
                            return

                        has_scenario = True
                        conn.execute("""
                            INSERT OR IGNORE INTO post_scenarios (chat_id, post_id, current_index, last_run_time)
                            VALUES (?, ?, 0, ?)
                        """, (found_chat_id, msg_id, time.time()))
                        conn.commit()
                        logger.info(f"🎬 [SCENARIO ADD] Новый пост {msg_id} добавлен в сценарий (Chat: {found_chat_id})")
                    else:
                        pass
        except Exception as e:
            logger.error(f"❌ Ошибка SQL при поиске сценария: {e}")

        if has_scenario:
            SCENARIO_CONTEXT[f"{channel_id}_{msg_id}"] = msg_id
            return

        chat_id_check = target_chat.get('chat_id')
        actual_ai_enabled = True
        found_fresh_settings = False
        if current_settings and 'targets' in current_settings:
            for t in current_settings['targets']:
                if t.get('chat_id') == chat_id_check:
                    actual_ai_enabled = t.get('ai_enabled', True)
                    found_fresh_settings = True
                    break

        if found_fresh_settings and not actual_ai_enabled and not is_manual: return
        if not found_fresh_settings and not target_chat.get('ai_enabled', True) and not is_manual: return

        post_text = event.message.text or ""
        image_bytes = None
        if event.message.media:
            try:
                image_bytes = await event.message.download_media(bytes)
            except:
                pass

        accounts_data = load_json_data(ACCOUNTS_FILE, [])
        eligible_clients = [c for c in list(active_clients.values())
                            if is_bot_awake(next((a for a in accounts_data if a['session_name'] == c.session_name), {}))
                            and (not target_chat.get('assigned_accounts', []) or c.session_name in target_chat.get(
                'assigned_accounts', []))]

        if not eligible_clients: return
        random.shuffle(eligible_clients)

        if is_manual:
            wait_time = random.randint(2, 5)
        else:
            base_delay = target_chat.get('initial_comment_delay', 30)
            wait_time = random.randint(base_delay, base_delay + 30)

        logger.info(f"⏳ Жду {wait_time} сек перед генерацией для поста {msg_id}...")
        await asyncio.sleep(wait_time)

        for client_wrapper in eligible_clients:
            generated_text, prompt_info = await generate_comment(f"ПРОКОММЕНТИРУЙ ПОСТ:\n{post_text}", target_chat,
                                                                 client_wrapper.session_name, image_bytes=image_bytes)
            if generated_text:
                await human_type_and_send(client_wrapper.client, event.chat_id, generated_text, reply_to_msg_id=msg_id)
                me = await client_wrapper.client.get_me()
                logger.info(f"✅ [{client_wrapper.session_name}] прокомментировал пост {msg_id} ({prompt_info})")
                log_content = f"[{prompt_info}] {generated_text}"
                log_action_to_db({'type': 'new_post_comment', 'post_id': msg_id, 'comment': log_content,
                                  'date': datetime.now(timezone.utc).isoformat(),
                                  'account': {'session_name': client_wrapper.session_name, 'first_name': me.first_name,
                                              'username': me.username},
                                  'target': {'chat_name': target_chat.get('chat_name'),
                                             'destination_chat_id': event.chat_id}})
                return
    except asyncio.CancelledError:
        pass
    finally:
        PENDING_TASKS.discard(task)


async def generate_post_evaluation(post_text, target_chat_settings, session_name, image_bytes=None):
    global current_settings
    provider = target_chat_settings.get('ai_provider', 'default')
    if provider == 'default':
        provider = current_settings.get('ai_provider', 'gemini')
    user_goal = target_chat_settings.get('prompt', 'релевантный пост')
    api_key = current_settings.get('api_keys', {}).get(provider)
    if not api_key:
        return False
    try:
        instruction = f"Проверь, соответствует ли пост теме: '{user_goal}'. Ответь только ОДНИМ словом: ДА или НЕТ."
        if provider == 'openai':
            client = openai.AsyncOpenAI(api_key=api_key)
            user_content = [{"type": "text", "text": f"{instruction}\nТекст: {post_text}"}]
            if image_bytes:
                base64_image = base64.b64encode(image_bytes).decode('utf-8')
                user_content.append({"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"}})
            completion = await client.chat.completions.create(model="gpt-4o", messages=[{"role": "user", "content": user_content}])
            return "ДА" in completion.choices[0].message.content.upper()
        elif provider == 'gemini':
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel('gemini-1.5-flash')
            content = []
            if image_bytes:
                content.append({'mime_type': 'image/jpeg', 'data': image_bytes})
            content.append(f"{instruction}\nТекст: {post_text}")
            response = await model.generate_content_async(content)
            return "ДА" in response.text.upper()
    except Exception:
        return False
    return False


async def process_new_post_for_reaction(source_channel_peer, original_post_id, reaction_target):
    global active_clients, PENDING_TASKS
    task = asyncio.current_task()
    PENDING_TASKS.add(task)
    try:
        chance = reaction_target.get('reaction_chance', 80)
        if random.randint(1, 100) > chance:
            return
        destination_chat_id_for_logs = int(reaction_target.get('linked_chat_id', reaction_target.get('chat_id')))
        daily_limit = reaction_target.get('daily_reaction_limit', 999)
        current_daily_count = get_daily_action_count_from_db(destination_chat_id_for_logs, 'reaction')
        if current_daily_count >= daily_limit:
            return
        accounts_data = load_json_data(ACCOUNTS_FILE, [])
        eligible_clients = []
        for c in list(active_clients.values()):
            acc_data = next((a for a in accounts_data if a['session_name'] == c.session_name), None)
            if acc_data and is_bot_awake(acc_data):
                if not reaction_target.get('assigned_accounts', []) or c.session_name in reaction_target.get('assigned_accounts', []):
                    eligible_clients.append(c)
        if not eligible_clients:
            return
        random.shuffle(eligible_clients)
        initial_delay = max(reaction_target.get('initial_reaction_delay', 10), 0)
        if initial_delay > 0:
            await asyncio.sleep(initial_delay)
        delay_between = max(reaction_target.get('delay_between_reactions', 5), 0)
        for client_wrapper in eligible_clients:
            try:
                current_daily_count = get_daily_action_count_from_db(destination_chat_id_for_logs, 'reaction')
                if current_daily_count >= daily_limit:
                    break
                available_reactions = reaction_target.get('reactions', [])
                num_to_set = reaction_target.get('reaction_count', 1)
                if not available_reactions:
                    continue
                reactions_to_set_str = [available_reactions[0]]
                if num_to_set > 1 and len(available_reactions) > 1:
                    reactions_to_set_str.extend(random.sample(available_reactions[1:], min(len(available_reactions) - 1, num_to_set - 1)))
                tl_reactions = [ReactionEmoji(emoticon=r) for r in reactions_to_set_str]
                actual_peer = None
                try:
                    actual_peer = await client_wrapper.client.get_input_entity(destination_chat_id_for_logs)
                except Exception:
                    actual_peer = source_channel_peer
                try:
                    await client_wrapper.client.send_read_acknowledge(actual_peer, message=original_post_id)
                except Exception:
                    pass
                await asyncio.sleep(random.uniform(1, 3))
                await client_wrapper.client(SendReactionRequest(peer=actual_peer, msg_id=original_post_id, reaction=tl_reactions))
                me = await client_wrapper.client.get_me()
                log_action_to_db({
                    'type': 'reaction', 'post_id': original_post_id, 'reactions': reactions_to_set_str,
                    'date': datetime.now(timezone.utc).isoformat(),
                    'account': {'session_name': client_wrapper.session_name, 'first_name': me.first_name,
                                'username': me.username},
                    'target': {'chat_name': reaction_target.get('chat_name'),
                               'chat_username': reaction_target.get('chat_username'),
                               'channel_id': reaction_target.get('chat_id'),
                               'destination_chat_id': destination_chat_id_for_logs}
                })
                if delay_between > 0 and client_wrapper != eligible_clients[-1]:
                    await asyncio.sleep(delay_between)
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + 2)
            except Exception as e:
                logger.error(f"❌ Ошибка отправки реакции ({client_wrapper.session_name}): {e}")
    except asyncio.CancelledError:
        pass
    finally:
        PENDING_TASKS.discard(task)


async def process_post_for_monitoring(event, monitor_target):
    global active_clients, MONITOR_CHANNEL_LAST_POST_TIME, PENDING_TASKS
    task = asyncio.current_task()
    PENDING_TASKS.add(task)
    try:
        channel_id = event.chat_id
        post_content = event.message.text or ""
        post_id = event.message.id
        min_interval = monitor_target.get('min_post_interval_mins', 0)
        if min_interval > 0:
            last_post_time = MONITOR_CHANNEL_LAST_POST_TIME.get(channel_id)
            if last_post_time and (event.message.date - last_post_time).total_seconds() < min_interval * 60:
                return
            MONITOR_CHANNEL_LAST_POST_TIME[channel_id] = event.message.date
        if len(post_content.split()) < monitor_target.get('min_word_count', 0):
            return
        daily_limit = monitor_target.get('daily_limit', 999)
        if daily_limit > 0 and get_daily_action_count_from_db(channel_id, 'monitoring') >= daily_limit:
            return
        eligible_clients = [c for c in list(active_clients.values()) if not monitor_target.get('assigned_accounts', []) or c.session_name in monitor_target.get('assigned_accounts', [])]
        if not eligible_clients:
            return
        client_wrapper = random.choice(eligible_clients)
        image_bytes = None
        if event.message.photo:
            image_bytes = await event.message.download_media(file=bytes)
        is_relevant = await generate_post_evaluation(post_content, monitor_target, client_wrapper.session_name, image_bytes)
        if is_relevant:
            notification_chat_id = monitor_target['notification_chat_id']
            try:
                await client_wrapper.client.forward_messages(notification_chat_id, event.message)
            except Exception:
                channel_username = monitor_target.get('chat_username')
                channel_id_str = str(monitor_target.get('chat_id', '')).replace('-100', '')
                post_link = f"https://t.me/{channel_username}/{post_id}" if channel_username else f"https://t.me/c/{channel_id_str}/{post_id}"
                message_text = f"❗️ <b>Найден пост</b>\n\n<b>Канал:</b> {monitor_target.get('chat_name', 'N/A')}\n<b>Ссылка:</b> {post_link}"
                await client_wrapper.client.send_message(notification_chat_id, message_text, parse_mode='html', link_preview=False)
            me = await client_wrapper.client.get_me()
            log_action_to_db({
                'type': 'monitoring', 'post_id': post_id, 'date': datetime.now(timezone.utc).isoformat(),
                'account': {'session_name': client_wrapper.session_name, 'first_name': me.first_name, 'username': me.username},
                'target': {'chat_name': monitor_target.get('chat_name'), 'channel_id': channel_id, 'destination_chat_id': channel_id},
                'comment': f"Found post, notified {notification_chat_id}"
            })
    except asyncio.CancelledError:
        pass
    finally:
        PENDING_TASKS.discard(task)


async def send_admin_notification(text, reply_context=None):
    try:
        admin_conf = load_config('admin_bot')
        token = admin_conf['token']
        owners_str = admin_conf.get('allowed_ids', '')

        if not owners_str: return
        owners = [oid.strip() for oid in owners_str.split(',') if oid.strip()]

        reply_markup = None
        if reply_context:
            try:
                with sqlite3.connect(DB_FILE) as conn:
                    cursor = conn.cursor()
                    cursor.execute('''
                        INSERT INTO alert_context (chat_id, msg_id, session_name, created_at)
                        VALUES (?, ?, ?, ?)
                    ''', (reply_context['chat_id'], reply_context['msg_id'], reply_context['session_name'],
                          time.time()))
                    alert_id = cursor.lastrowid
                    conn.commit()

                reply_markup = {
                    "inline_keyboard": [[
                        {"text": "🗣 Ответить", "callback_data": f"reply_alert_{alert_id}"}
                    ]]
                }
            except Exception as e:
                logger.error(f"Ошибка сохранения контекста ответа: {e}")

        safe_text = text.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

        safe_text = safe_text.replace('&lt;b&gt;', '<b>').replace('&lt;/b&gt;', '</b>')
        safe_text = safe_text.replace('&lt;i&gt;', '<i>').replace('&lt;/i&gt;', '</i>')
        safe_text = safe_text.replace('&lt;a href=', '<a href=').replace('&lt;/a&gt;', '</a>')
        safe_text = safe_text.replace("'&gt;", "'>").replace('"&gt;', '">')

        async with httpx.AsyncClient() as client:
            for owner_id in owners:
                try:
                    payload = {
                        "chat_id": int(owner_id),
                        "text": text,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True
                    }
                    if reply_markup:
                        payload["reply_markup"] = reply_markup

                    await client.post(f"https://api.telegram.org/bot{token}/sendMessage", json=payload)
                except Exception:
                    try:
                        payload["parse_mode"] = None
                        await client.post(f"https://api.telegram.org/bot{token}/sendMessage", json=payload)
                    except:
                        pass
    except Exception as e:
        logger.error(f"Не удалось отправить уведомление админу: {e}")


async def process_trigger(event, found_target, our_ids):
    global active_clients, REPLY_PROCESS_CACHE

    msg_id = event.message.id
    if msg_id in REPLY_PROCESS_CACHE:
        return

    post_text = (event.message.text or "").lower()
    if not post_text:
        return

    answer_text = None
    try:
        chat_id_target = str(found_target.get('chat_id'))

        with sqlite3.connect(DB_FILE) as conn:
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
        REPLY_PROCESS_CACHE.add(msg_id)

        accounts_data = load_json_data(ACCOUNTS_FILE, [])
        eligible = []

        for c in list(active_clients.values()):
            acc_conf = next((a for a in accounts_data if a['session_name'] == c.session_name), None)
            if acc_conf and is_bot_awake(acc_conf):
                assigned = found_target.get('assigned_accounts', [])
                if not assigned or c.session_name in assigned:
                    eligible.append(c)

        if not eligible:
            REPLY_PROCESS_CACHE.discard(msg_id)
            return

        client_wrapper = random.choice(eligible)

        await asyncio.sleep(random.uniform(3, 7))

        try:
            await human_type_and_send(client_wrapper.client, event.chat_id, answer_text, reply_to_msg_id=msg_id)
            logger.info(f"⚡ [{client_wrapper.session_name}] ответил по триггеру на сообщение {msg_id}")
        except Exception as e:
            logger.error(f"Ошибка отправки триггера: {e}")
            REPLY_PROCESS_CACHE.discard(msg_id)


class CommentatorClient:
    def __init__(self, account_data, api_id, api_hash):
        self.session_name = account_data['session_name']
        self.session_string = account_data['session_string']
        self.proxy_url = account_data.get('proxy_url')
        self.proxy = self._parse_proxy(self.proxy_url) if self.proxy_url else None
        self.client = TelegramClient(StringSession(self.session_string), api_id, api_hash, proxy=self.proxy)

    def _parse_proxy(self, url):
        try:
            protocol, rest = url.split('://')
            auth, addr = rest.split('@')
            user, password = auth.split(':')
            host, port = addr.split(':')
            return (protocol, host, int(port), True, user, password)
        except Exception:
            return None

    async def start(self):
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized():
                return False
            me = await self.client.get_me()
            self.user_id = me.id
            self.client.add_event_handler(self.event_handler, events.NewMessage)
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения {self.session_name}: {e}")
            return False

    async def stop(self):
        if self.client.is_connected():
            await self.client.disconnect()

    async def event_handler(self, event):
        async with EVENT_HANDLER_LOCK:
            if event.out:
                return

            global current_settings, handled_posts_for_comments, handled_posts_for_reactions, handled_posts_for_monitoring, handled_grouped_ids, REPLY_PROCESS_CACHE

            try:
                event_chat_id = int(str(event.chat_id).replace('-100', ''))
            except:
                event_chat_id = event.chat_id

            msg_id = event.message.id
            sender_id = event.sender_id

            found_target = None
            for t in current_settings.get('targets', []):
                t_linked = int(str(t.get('linked_chat_id', 0)).replace('-100', ''))
                t_main = int(str(t.get('chat_id', 0)).replace('-100', ''))
                if event_chat_id == t_linked or event_chat_id == t_main:
                    found_target = t
                    break

            if found_target:
                our_ids = get_all_our_user_ids()

                if sender_id and sender_id not in our_ids:
                    asyncio.create_task(process_trigger(event, found_target, our_ids))

                    if (event.is_reply or event.is_private) and msg_id not in REPLY_PROCESS_CACHE:
                        is_reply_to_us = False
                        if event.is_reply:
                            reply_msg = await event.get_reply_message()
                            if reply_msg and reply_msg.sender_id == self.user_id:
                                is_reply_to_us = True
                        elif event.is_private:
                            is_reply_to_us = True

                        if is_reply_to_us:
                            REPLY_PROCESS_CACHE.add(msg_id)
                            post_text = event.message.text or ""
                            chat_title = "Чат"
                            try:
                                chat = await event.get_chat()
                                chat_title = chat.title
                            except:
                                pass

                            user_name = "Неизвестный"
                            try:
                                sender = await event.get_sender()
                                user_name = f"{getattr(sender, 'first_name', '')} {getattr(sender, 'last_name', '')}".strip()
                                if not user_name: user_name = f"ID: {sender_id}"
                            except:
                                pass

                            msg_link = f"https://t.me/c/{str(event_chat_id)}/{msg_id}"
                            alert_text = (
                                f"🚨 <b>Живой человек</b>\n"
                                f"Ответил боту: <b>{self.session_name}</b>\n"
                                f"В чате: {chat_title}\n"
                                f"Кто: {user_name}\n\n"
                                f"💬: <i>{post_text}</i>\n\n"
                                f"👉 <a href='{msg_link}'>Перейти к сообщению</a>"
                            )
                            ctx = {'chat_id': str(event.chat_id), 'msg_id': msg_id, 'session_name': self.session_name}
                            asyncio.create_task(send_admin_notification(alert_text, reply_context=ctx))

                is_channel_post = event.message.post or (event.message.fwd_from and event.message.fwd_from.channel_post)
                if is_channel_post:
                    if event.message.grouped_id:
                        if event.message.grouped_id in handled_grouped_ids: return
                        handled_grouped_ids.append(event.message.grouped_id)

                    unique_id = f"{event_chat_id}_{msg_id}"

                    t_linked_check = int(str(found_target.get('linked_chat_id', 0)).replace('-100', ''))
                    t_main_check = int(str(found_target.get('chat_id', 0)).replace('-100', ''))

                    if not (t_linked_check and t_linked_check != t_main_check and event_chat_id == t_main_check):
                        if unique_id not in handled_posts_for_comments:
                            handled_posts_for_comments.append(unique_id)
                            asyncio.create_task(process_new_post(event, found_target))

                    for r_target in current_settings.get('reaction_targets', []):
                        if event_chat_id == int(str(r_target.get('linked_chat_id', 0)).replace('-100', '')):
                            if unique_id not in handled_posts_for_reactions:
                                handled_posts_for_reactions.append(unique_id)
                                asyncio.create_task(process_new_post_for_reaction(event.input_chat, msg_id, r_target))

            if not event.message.fwd_from and event.is_group and found_target:
                if found_target.get('ai_enabled', True) and found_target.get('reply_chance', 0) > 0:
                    asyncio.create_task(process_reply_to_comment(event, found_target))

            if event.is_channel and not event.message.fwd_from:
                for m_t in current_settings.get('monitor_targets', []):
                    if int(str(m_t.get('chat_id', 0)).replace('-100', '')) == event_chat_id:
                        if event.message.grouped_id and event.message.grouped_id in handled_grouped_ids: return
                        if event.message.grouped_id: handled_grouped_ids.append(event.message.grouped_id)

                        unique_mon_id = f"mon_{event_chat_id}_{msg_id}"
                        if unique_mon_id not in handled_posts_for_monitoring:
                            handled_posts_for_monitoring.append(unique_mon_id)
                            asyncio.create_task(process_post_for_monitoring(event, m_t))


async def ensure_account_joined(client_wrapper, target_config):
    global JOINED_CACHE

    targets_to_join = set()

    main_chat_id = target_config.get('chat_id')
    if main_chat_id:
        targets_to_join.add(str(main_chat_id))

    linked_chat_id = target_config.get('linked_chat_id')
    if linked_chat_id and str(linked_chat_id) != str(main_chat_id):
        targets_to_join.add(str(linked_chat_id))

    username = target_config.get('chat_username')
    invite_link = target_config.get('invite_link')

    all_success = True

    for target_id in targets_to_join:
        cache_key = (client_wrapper.session_name, target_id)

        if cache_key in JOINED_CACHE:
            continue

        joined = False

        if invite_link and not joined:
            try:
                if "t.me/+" in invite_link or "joinchat" in invite_link or "/" not in invite_link:
                    hash_arg = invite_link.split('/')[-1].replace('+', '')
                    await client_wrapper.client(ImportChatInviteRequest(hash_arg))
                    logger.info(f"[{client_wrapper.session_name}] Вступил по инвайту в {target_id}")
                    joined = True
            except UserAlreadyParticipantError:
                joined = True
            except Exception:
                pass

        if username and not joined:
            try:
                await client_wrapper.client(JoinChannelRequest(username))
                joined = True

                if linked_chat_id and target_id == str(linked_chat_id):
                    try:
                        entity = await client_wrapper.client.get_entity(username)
                        full = await client_wrapper.client(GetFullChannelRequest(entity))
                        if full.full_chat.linked_chat_id:
                            linked_entity = await client_wrapper.client.get_input_entity(full.full_chat.linked_chat_id)
                            await client_wrapper.client(JoinChannelRequest(linked_entity))
                            logger.info(f"[{client_wrapper.session_name}] Довступил в привязанный чат через канал")
                    except Exception as e:
                        pass

            except UserAlreadyParticipantError:
                joined = True
            except Exception:
                pass

        if not joined:
            try:
                real_id = int(str(target_id).replace("-100", ""))
                entity = await client_wrapper.client.get_input_entity(int(target_id))
                await client_wrapper.client(JoinChannelRequest(entity))
                logger.info(f"[{client_wrapper.session_name}] Вступил по ID в {target_id}")
                joined = True
            except UserAlreadyParticipantError:
                joined = True
            except Exception as e:
                pass

        if joined:
            JOINED_CACHE.add(cache_key)
        else:
            all_success = False

    return all_success


async def manage_clients(api_id, api_hash):
    global active_clients, current_settings, CLIENT_CATCH_UP_STATUS

    current_settings = load_json_data(SETTINGS_FILE)
    accounts_from_file = load_json_data(ACCOUNTS_FILE, [])

    file_session_names = {acc['session_name'] for acc in accounts_from_file if acc.get('status') != 'banned'}
    for session_name in list(active_clients.keys()):
        acc_data = next((a for a in accounts_from_file if a['session_name'] == session_name), None)
        if session_name not in file_session_names or not (acc_data and is_bot_awake(acc_data)):
            client_to_stop = active_clients.pop(session_name)
            await client_to_stop.stop()
            keys_to_remove = [k for k in CLIENT_CATCH_UP_STATUS if k.startswith(f"{session_name}_")]
            for k in keys_to_remove:
                CLIENT_CATCH_UP_STATUS.discard(k)
            logger.info(f"Клиент {session_name} остановлен (удален или время сна).")

    for account_data in accounts_from_file:
        session_name = account_data['session_name']

        if account_data.get('status') == 'banned':
            continue

        if is_bot_awake(account_data):
            if session_name not in active_clients:
                client_wrapper = CommentatorClient(account_data, api_id, api_hash)
                if await client_wrapper.start():
                    active_clients[session_name] = client_wrapper
                    logger.info(f"Клиент {session_name} запущен.")
                else:
                    continue

            client_wrapper = active_clients[session_name]

            for target in current_settings.get('targets', []):
                assigned = target.get('assigned_accounts', [])
                if not assigned or session_name in assigned:
                    await ensure_account_joined(client_wrapper, target)

                    catch_up_key = f"{session_name}_{target.get('chat_id')}"
                    if catch_up_key not in CLIENT_CATCH_UP_STATUS:
                        CLIENT_CATCH_UP_STATUS.add(catch_up_key)
                        asyncio.create_task(catch_up_missed_posts(client_wrapper, target))

            for target in current_settings.get('reaction_targets', []):
                assigned = target.get('assigned_accounts', [])
                if not assigned or session_name in assigned:
                    await ensure_account_joined(client_wrapper, target)

            for target in current_settings.get('monitor_targets', []):
                assigned = target.get('assigned_accounts', [])
                if not assigned or session_name in assigned:
                    await ensure_account_joined(client_wrapper, target)


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


def get_all_our_user_ids():
    accounts = load_json_data(ACCOUNTS_FILE, [])
    return {acc.get('user_id') for acc in accounts if acc.get('user_id')}


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


async def execute_reply_with_fallback(candidate_list, chat_id, target_chat, prompt_base, delay, reply_to_msg_id, reply_to_name=None, is_intervention=False):
    global PENDING_TASKS
    task = asyncio.current_task()
    PENDING_TASKS.add(task)
    try:
        await asyncio.sleep(delay)
        tag_chance = target_chat.get('tag_reply_chance', 50)
        actual_reply_id = reply_to_msg_id if random.randint(1, 100) <= tag_chance else None
        for client_wrapper in candidate_list:
            reply_text, prompt_info = await generate_comment(
                prompt_base,
                target_chat,
                client_wrapper.session_name,
                image_bytes=None,
                is_reply_mode=True,
                reply_to_name=reply_to_name
            )
            if reply_text:
                await human_type_and_send(client_wrapper.client, chat_id, reply_text, actual_reply_id)
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
                return
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Ошибка в цепочке ответов: {e}")
    finally:
        PENDING_TASKS.discard(task)


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


async def process_reply_to_comment(event, target_chat):
    global active_clients, REPLY_PROCESS_CACHE, PENDING_TASKS
    msg_id = event.message.id
    if msg_id in REPLY_PROCESS_CACHE:
        return
    REPLY_PROCESS_CACHE.add(msg_id)
    chat_id = event.chat_id
    max_history = target_chat.get('max_dialogue_depth', 6)
    if not await check_dialogue_depth(event.client, event.message, max_history):
        logger.info(f"⏭ Сообщение {msg_id} пропущено: превышена глубина диалога ({max_history})")
        return
    accounts_data = load_json_data(ACCOUNTS_FILE, [])
    sender_id = event.message.sender_id
    our_ids = get_all_our_user_ids()
    target_user_id, target_name = await get_thread_context(event.client, event, our_ids)
    is_reply_to_us = target_user_id in our_ids
    eligible_candidates = []
    for c in list(active_clients.values()):
        acc_conf = next((a for a in accounts_data if a['session_name'] == c.session_name), None)
        if acc_conf and is_bot_awake(acc_conf) and getattr(c, 'user_id', None) != sender_id:
            if not target_chat.get('assigned_accounts', []) or c.session_name in target_chat.get('assigned_accounts', []):
                eligible_candidates.append(c)
    if not eligible_candidates:
        logger.info(f"⏭ Нет доступных аккаунтов для ответа на {msg_id}")
        return
    intervention_chance = target_chat.get('intervention_chance', 30)
    roll = random.randint(1, 100)
    if roll > intervention_chance:
        logger.info(f"🎲 Шанс не сработал ({roll} > {intervention_chance}%) для {msg_id}. Никто не ответил")
        return
    triggered_client = random.choice(eligible_candidates)
    is_intervention = getattr(triggered_client, 'user_id', None) != target_user_id
    d_min, d_max = target_chat.get('reply_delay_min', 20), target_chat.get('reply_delay_max', 80)
    personal_delay = random.randint(min(d_min, d_max), max(d_min, d_max))
    action_type = "ВМЕШАЕТСЯ" if is_intervention else "ОТВЕТИТ"
    to_who = f"нашему боту ({target_name})" if is_reply_to_us else f"пользователю {target_name}"
    logger.info(f"🤖 [{triggered_client.session_name}] {action_type} {to_who} через {personal_delay}с (шанс {roll}%)")
    asyncio.create_task(execute_reply_with_fallback([triggered_client], chat_id, target_chat,
                                                    f"{event.message.text}", personal_delay,
                                                    msg_id, reply_to_name=target_name, is_intervention=is_intervention))


async def mark_account_as_banned(session_name):
    accounts = load_json_data(ACCOUNTS_FILE, [])
    updated = False
    for acc in accounts:
        if acc['session_name'] == session_name:
            acc['status'] = 'banned'
            acc['banned_at'] = datetime.now(timezone.utc).isoformat()
            updated = True
            break
    if updated:
        with open(ACCOUNTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(accounts, f, indent=2, ensure_ascii=False)
    logger.error(f"Аккаунт {session_name} помечен как ЗАБАНЕН в {ACCOUNTS_FILE}")


def post_process_text(text):
    if not text:
        return text
    global current_settings
    h_set = current_settings.get('humanization', {})

    typo_chance = h_set.get('typo_chance', 0) / 100
    lower_chance = h_set.get('lowercase_chance', 0) / 100
    comma_chance = h_set.get('comma_skip_chance', 0) / 100

    text = text.strip()

    formal_words = ["уважаемые", "благодарю", "данный пост", "согласно", "ввиду", "ассистент", "внимание", "пожалуйста",
                    "я ии", "виртуальный", "интеллект"]
    for word in formal_words:
        if word in text.lower():
            text = text.replace(word, "").replace(word.capitalize(), "")

    text = text.replace('—', '-').replace('–', '-')
    text = text.replace('"', '').replace("'", "")

    while '!!!' in text:
        text = text.replace('!!!', '!!')

    if len(text) < 80 and text.endswith('.'):
        text = text[:-1]

    words = text.split()

    processed_words = []
    for word in words:
        if ',' in word and random.random() < comma_chance:
            word = word.replace(',', '')

        if typo_chance > 0 and random.random() < typo_chance and len(word) > 4:
            idx = random.randint(1, len(word) - 2)
            w_list = list(word)
            w_list[idx], w_list[idx + 1] = w_list[idx + 1], w_list[idx]
            word = "".join(w_list)

        processed_words.append(word)

    res = " ".join(processed_words)

    if random.random() < lower_chance:
        res = res.lower()
    elif words and random.random() < lower_chance:
        words[0] = words[0].lower()
        res = " ".join(words)

    return res


async def human_type_and_send(client, chat_id, text, reply_to_msg_id=None, skip_processing=False):
    if not text:
        return
    global current_settings

    if skip_processing:
        processed_text = text
    else:
        processed_text = post_process_text(text)

    split_chance = current_settings.get('humanization', {}).get('split_chance', 0) / 100
    message_parts = []

    if not skip_processing and len(processed_text) > 50 and random.random() < split_chance:
        delimiters = [', ', '. ', '! ', '? ']
        split_done = False
        for d in delimiters:
            if d in processed_text:
                parts = processed_text.split(d, 1)
                message_parts = [parts[0], parts[1]]
                split_done = True
                break
        if not split_done: message_parts = [processed_text]
    else:
        message_parts = [processed_text]

    last_msg = None
    original_reply_id = reply_to_msg_id

    for part in message_parts:
        part = part.strip()
        if not part: continue

        await asyncio.sleep(random.uniform(2, 4))

        typing_time = min(len(part) * 0.06, 6)

        try:
            async with client.action(chat_id, 'typing'):
                await asyncio.sleep(typing_time)
        except (ChatAdminRequiredError, RPCError, Exception):
            await asyncio.sleep(typing_time)

        try:
            last_msg = await client.send_message(chat_id, part, reply_to=original_reply_id)
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке сообщения: {e}")

    return last_msg


async def generate_batch_identities(topic, count, provider, api_key):
    system_prompt = (
        f"Ты — генератор профилей для Telegram. Тематика: '{topic}'.\n"
        f"Сгенерируй список из {count} УНИКАЛЬНЫХ имен и фамилий.\n"
        f"Имена должны быть разными, креативными, реалистичными или сленговыми.\n"
        f"Верни ответ ТОЛЬКО в формате чистого списка Python, без лишних слов:\n"
        f"['Имя Фамилия', 'Имя Фамилия', ...]"
    )

    try:
        content = ""
        if provider == 'openai' or provider == 'deepseek':
            base_url = "https://api.deepseek.com" if provider == 'deepseek' else None
            client = openai.AsyncOpenAI(api_key=api_key, base_url=base_url)
            completion = await client.chat.completions.create(
                messages=[{"role": "user", "content": system_prompt}],
                model="gpt-4o" if provider == 'openai' else "deepseek-chat",
                temperature=1.4
            )
            content = completion.choices[0].message.content.strip()
        elif provider == 'gemini':
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel('gemini-1.5-flash')
            response = await model.generate_content_async(system_prompt)
            content = response.text.strip()

        content = content.replace("```python", "").replace("```json", "").replace("```", "")
        names_list = eval(content)
        if isinstance(names_list, list):
            return names_list
        return []
    except Exception as e:
        logger.error(f"Ошибка массовой генерации имен: {e}")
        return []


async def get_real_identities_from_channel(client, source_channel, limit=200):
    identities = []
    seen_ids = set()
    scan_depth = 4000

    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id FROM used_identities")
            rows = cursor.fetchall()
            for row in rows:
                seen_ids.add(row[0])

    except Exception as e:
        logger.error(f"Ошибка загрузки использованных ID из БД: {e}")

    try:
        if not client.is_connected():
            await client.connect()

        async for message in client.iter_messages(source_channel, limit=scan_depth):
            if not message.sender or not hasattr(message.sender, 'id'):
                continue

            user_id = message.sender.id
            if user_id in seen_ids:
                continue

            if hasattr(message.sender, 'bot') and message.sender.bot:
                continue

            if hasattr(message.sender, 'scam') and message.sender.scam:
                continue

            seen_ids.add(user_id)

            first_name = getattr(message.sender, 'first_name', '') or ''
            last_name = getattr(message.sender, 'last_name', '') or ''

            if not first_name.strip():
                continue

            identities.append({
                'user_id': user_id,
                'first_name': first_name,
                'last_name': last_name,
                'user_entity': message.sender,
                'has_photo': getattr(message.sender, 'photo', None) is not None
            })

            if len(identities) >= limit * 4:
                break

    except Exception as e:
        logger.error(f"Ошибка при сборе реальных профилей: {e}")

    random.shuffle(identities)
    return identities[:limit]


async def run_rebrand_logic(api_id, api_hash):
    global current_settings, active_clients
    task = current_settings.get('rebrand_task')
    if not task or task.get('status') != 'pending':
        return

    raw_source = task.get('source_value', task.get('source_channel'))
    is_channel = task.get('is_channel', True)

    if raw_source:
        source_val = raw_source.strip().replace('https://t.me/', '').replace('http://t.me/', '').replace('t.me/', '').replace('@', '')
        if '+' in source_val and not source_val.startswith('+'):
            pass
    else:
        source_val = "abstract"

    logger.info(f"🚀 Запуск процесса ребрендинга: {task['topic']} | Источник: {source_val}")

    provider = current_settings.get('ai_provider', 'gemini')
    api_key = current_settings.get('api_keys', {}).get(provider)
    openai_key = current_settings.get('api_keys', {}).get('openai')

    import httpx
    import urllib.parse
    from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest

    clients_list = list(active_clients.values())
    real_identities = []
    generated_names = []

    using_real_profiles = False
    successful_parser = None

    if is_channel and clients_list:
        logger.info(f"📥 Начинаем поиск донора для парсинга канала @{source_val}...")

        for parser_wrapper in clients_list:
            try:
                parser_client = parser_wrapper.client
                if not parser_client.is_connected():
                    await parser_client.connect()

                try:
                    entity = await parser_client.get_entity(source_val)
                except ValueError:
                    try:
                        await parser_client(JoinChannelRequest(source_val))
                        entity = await parser_client.get_entity(source_val)
                    except Exception:
                        raise Exception(f"Не удалось найти канал {source_val}")

                target_entity = entity
                try:
                    full_channel = await parser_client(GetFullChannelRequest(entity))
                    if full_channel.full_chat.linked_chat_id:
                        linked_id = full_channel.full_chat.linked_chat_id
                        target_entity = await parser_client.get_entity(linked_id)
                        logger.info(f"💬 У канала есть чат с обсуждениями: {target_entity.title}. Будем парсить его.")
                    else:
                        logger.warning("⚠️ У канала НЕТ привязанного чата. Попробуем парсить сам канал (если это группа).")
                except Exception as e:
                    logger.warning(f"Не удалось получить инфо о привязанном чате: {e}")

                logger.info(f"🔄 Пробуем парсить через аккаунт: {parser_wrapper.session_name}")

                needed_count = len(clients_list) + 10
                real_identities = await get_real_identities_from_channel(parser_client, target_entity, limit=needed_count)

                if real_identities:
                    logger.info(f"✅ Успешно собрано {len(real_identities)} профилей через {parser_wrapper.session_name}")
                    using_real_profiles = True
                    successful_parser = parser_client
                    break
                else:
                    logger.warning(f"⚠️ Аккаунт {parser_wrapper.session_name} зашел, но не нашел сообщений с людьми.")
            except Exception as e:
                logger.warning(f"⚠️ Аккаунт {parser_wrapper.session_name} не смог спарсить канал: {e}")
                continue

        if not using_real_profiles:
            logger.error("❌ Ни один аккаунт не смог получить доступ к каналу-донору. Переключаюсь на генерацию AI.")
            is_channel = False

    if not using_real_profiles:
        needed_count = len(clients_list) + 5
        generated_names = await generate_batch_identities(task['topic'], needed_count, provider, api_key)
        logger.info(f"✅ Сгенерировано {len(generated_names)} имен через AI.")

    identity_index = 0

    for client_wrapper in clients_list:
        try:
            photo_path = os.path.join(BASE_DIR, f"avatar_{client_wrapper.session_name}.jpg")
            got_photo = False
            first_name = ""
            last_name = ""
            current_identity_user_id = None

            if using_real_profiles and successful_parser:
                identity = real_identities[identity_index % len(real_identities)]
                first_name = identity['first_name']
                last_name = identity['last_name']
                current_identity_user_id = identity.get('user_id')

                if identity['has_photo']:
                    try:
                        await successful_parser.download_profile_photo(identity['user_entity'], file=photo_path)
                        if os.path.exists(photo_path):
                            got_photo = True
                    except Exception as e:
                        logger.error(f"Ошибка скачивания фото реального юзера: {e}")

                identity_index += 1
            else:
                if generated_names:
                    full_name = generated_names.pop(0)
                    parts = full_name.split(' ', 1)
                    first_name = parts[0].replace('"', '').replace("'", "")
                    last_name = (parts[1] if len(parts) > 1 else "").replace('"', '').replace("'", "")
                else:
                    first_name = f"User{random.randint(100, 999)}"
                    last_name = ""

                if not got_photo and openai_key:
                    try:
                        logger.info(f"🎨 Генерирую аватар через DALL-E 3 для {client_wrapper.session_name}...")
                        openai_client = openai.AsyncOpenAI(api_key=openai_key)
                        dalle_prompt = f"Avatar profile picture for social media, topic: {raw_source}, style: realistic, high quality, professional headshot"

                        response = await openai_client.images.generate(
                            model="dall-e-3", prompt=dalle_prompt, size="1024x1024", quality="standard", n=1
                        )
                        image_url = response.data[0].url

                        async with httpx.AsyncClient(timeout=30.0) as http_client:
                            resp = await http_client.get(image_url)
                            if resp.status_code == 200:
                                with open(photo_path, 'wb') as f:
                                    f.write(resp.content)
                                got_photo = True
                    except Exception:
                        pass

                if not got_photo:
                    encoded_query = urllib.parse.quote(source_val)
                    seed = random.randint(1, 9999999)
                    url = f"https://image.pollinations.ai/prompt/avatar%20{encoded_query}%20{seed}?width=500&height=500&nologo=true&model=flux"

                    async with httpx.AsyncClient(timeout=60.0) as http_client:
                        resp = await http_client.get(url)
                        if resp.status_code == 200:
                            with open(photo_path, 'wb') as f:
                                f.write(resp.content)
                            got_photo = True

                    await asyncio.sleep(3)

            success = await update_account_profile(client_wrapper, first_name, last_name, photo_path if got_photo else None)

            if success and current_identity_user_id:
                try:
                    with sqlite3.connect(DB_FILE) as conn:
                        conn.execute("INSERT OR IGNORE INTO used_identities (user_id, date_used) VALUES (?, ?)",
                                     (current_identity_user_id, datetime.now(timezone.utc).isoformat()))
                        conn.commit()
                except Exception as db_e:
                    logger.error(f"Ошибка сохранения использованного ID {current_identity_user_id}: {db_e}")

            if got_photo and os.path.exists(photo_path):
                os.remove(photo_path)

            logger.info(f"✅ Аккаунт {client_wrapper.session_name} обновлен: {first_name} {last_name}")

            await asyncio.sleep(random.randint(5, 12))

        except Exception as e:
            logger.error(f"Ошибка ребрендинга для {client_wrapper.session_name}: {repr(e)}")

    current_settings['rebrand_task']['status'] = 'completed'
    with open(SETTINGS_FILE, 'w', encoding='utf-8') as f:
        json.dump(current_settings, f, indent=2, ensure_ascii=False)
    logger.info("🏁 Задача по ребрендингу завершена.")


async def proxy_auto_checker(bot_token, owner_ids):
    while True:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, url FROM proxies")
            proxies = cursor.fetchall()
            for p_id, url in proxies:
                res = await check_proxy_health(url)
                cursor.execute("UPDATE proxies SET status = ?, last_check = ?, ip = ?, country = ? WHERE id = ?",
                               (res['status'], datetime.now().isoformat(), res['ip'], res['country'], p_id))
                if res['status'] == 'dead':
                    async with httpx.AsyncClient() as client:
                        for oid in owner_ids:
                            try:
                                text = f"⚠️ <b>Прокси не работает!</b>\nURL: <code>{url}</code>"
                                await client.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",
                                                  json={"chat_id": oid, "text": text, "parse_mode": "html"})
                            except: pass
            conn.commit()
        await asyncio.sleep(86400)


async def process_scenarios():
    global active_clients, current_settings, SCENARIO_CONTEXT

    if not hasattr(process_scenarios, "last_log_time"):
        process_scenarios.last_log_time = {}

    if not hasattr(process_scenarios, "msg_history"):
        process_scenarios.msg_history = {}

    tasks_to_process = []

    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
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

    accounts_data = load_json_data(ACCOUNTS_FILE, [])
    ordered_accounts = [acc for acc in accounts_data if acc.get('status') != 'banned']

    for task in tasks_to_process:
        row_id = task['id']
        chat_id_str = task['chat_id']
        post_id = task['post_id']
        idx = task['current_index']
        last_run = task['last_run_time']
        content = task['script_content']

        target_settings = None
        for t in current_settings.get('targets', []):
            t_id = str(t.get('chat_id'))
            if t_id == chat_id_str or t_id.replace('-100', '') == chat_id_str.replace('-100', ''):
                target_settings = t
                break

        if not target_settings:
            with sqlite3.connect(DB_FILE) as conn:
                conn.execute("DELETE FROM post_scenarios WHERE id = ?", (row_id,))
            continue

        destination_id_str = target_settings.get('linked_chat_id', target_settings.get('chat_id'))

        lines = [l.strip() for l in content.split('\n') if l.strip()]

        if idx >= len(lines):
            with sqlite3.connect(DB_FILE) as conn:
                conn.execute("DELETE FROM post_scenarios WHERE id = ?", (row_id,))

            hist_key = f"{chat_id_str}_{post_id}"
            if hist_key in process_scenarios.msg_history:
                del process_scenarios.msg_history[hist_key]

            logger.info(f"🏁 Сценарий для поста {post_id} завершен.")
            continue

        line = lines[idx]

        match = re.search(r'\[(\d+)\]\s*[\|¦]?\s*([\d\.,]+)\s*[-–—]\s*([\d\.,]+)[сcCcSsа-яА-Яa-zA-Z]*\s*[\|¦]?\s*(.+)',
                          line)

        if not match:
            logger.warning(f"⚠️ [SKIP] Неверный формат строки {idx + 1}: '{line}'")
            with sqlite3.connect(DB_FILE) as conn:
                conn.execute("UPDATE post_scenarios SET current_index = current_index + 1 WHERE id = ?", (row_id,))
            continue

        acc_idx_raw = int(match.group(1))
        min_delay = float(match.group(2).replace(',', '.'))
        max_delay = float(match.group(3).replace(',', '.'))
        text = match.group(4).strip()

        time_passed = time.time() - last_run
        log_key = f"{row_id}_{idx}"

        if time_passed < min_delay:
            if time.time() - process_scenarios.last_log_time.get(log_key, 0) > 10:
                logger.info(f"⏳ [WAIT] Пост {post_id}: Шаг {idx + 1}. Ждем еще {min_delay - time_passed:.1f}с")
                process_scenarios.last_log_time[log_key] = time.time()
            continue

        if log_key in process_scenarios.last_log_time:
            del process_scenarios.last_log_time[log_key]

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
                with sqlite3.connect(DB_FILE) as conn:
                    conn.execute("UPDATE post_scenarios SET current_index = current_index + 1 WHERE id = ?", (row_id,))
                continue

        try:
            hist_key = f"{chat_id_str}_{post_id}"
            if hist_key not in process_scenarios.msg_history:
                process_scenarios.msg_history[hist_key] = {}

            reply_to_id = post_id
            use_reply_mode = target_settings.get('scenario_reply_mode', False)

            tags = re.findall(r'\{(\d+)\}', text)
            for t_num in tags:
                ref_idx = int(t_num)

                if ref_idx in process_scenarios.msg_history[hist_key]:
                    reply_to_id = process_scenarios.msg_history[hist_key][ref_idx]

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
                with sqlite3.connect(DB_FILE) as conn:
                    conn.execute("UPDATE post_scenarios SET current_index = current_index + 1 WHERE id = ?", (row_id,))
                continue
            except Exception as e:
                try:
                    entity = await client_wrapper.client.get_entity(norm_dest_id)
                except:
                    logger.error(f"❌ [{session_name}] Чат не найден: {e}")
                    with sqlite3.connect(DB_FILE) as conn:
                        conn.execute("UPDATE post_scenarios SET current_index = current_index + 1 WHERE id = ?",
                                     (row_id,))
                    continue

            wait_real = random.uniform(0, max(0, max_delay - min_delay))
            if wait_real > 0:
                logger.info(f"⏱ [{session_name}] Пауза перед вводом {wait_real:.1f}с...")
                await asyncio.sleep(wait_real)

            logger.info(f"✍️ [{session_name}] Печатает сообщение...")

            sent_msg = await human_type_and_send(client_wrapper.client, entity, text, reply_to_msg_id=reply_to_id, skip_processing=True)

            if sent_msg:
                logger.info(f"✅ [{session_name}] УСПЕШНО отправил: {text[:20]}...")

                process_scenarios.msg_history[hist_key][acc_idx_raw] = sent_msg.id

                me = await client_wrapper.client.get_me()
                log_action_to_db({
                    'type': 'comment',
                    'post_id': post_id,
                    'comment': f"[SCENARIO STEP {idx + 1}] {text}",
                    'date': datetime.now(timezone.utc).isoformat(),
                    'account': {'session_name': session_name, 'first_name': me.first_name, 'username': me.username},
                    'target': {'chat_name': 'Scenario', 'destination_chat_id': destination_id_str}
                })

            with sqlite3.connect(DB_FILE) as conn:
                conn.execute(
                    "UPDATE post_scenarios SET current_index = current_index + 1, last_run_time = ? WHERE id = ?",
                    (time.time(), row_id))

        except Exception as e:
            logger.error(f"❌ Ошибка выполнения шага (Post {post_id}): {e}", exc_info=True)
            with sqlite3.connect(DB_FILE) as conn:
                conn.execute("UPDATE post_scenarios SET current_index = current_index + 1 WHERE id = ?", (row_id,))


async def process_outbound_queue():
    global active_clients
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM outbound_queue WHERE status = 'pending'")
            tasks = cursor.fetchall()

        if not tasks:
            return

        for task in tasks:
            t_id = task['id']
            session_name = task['session_name']
            chat_id_str = task['chat_id']
            reply_id = task['reply_to_msg_id']
            text = task['text']

            client_wrapper = active_clients.get(session_name)

            if not client_wrapper:
                with sqlite3.connect(DB_FILE) as conn:
                    conn.execute("UPDATE outbound_queue SET status = 'failed_no_client' WHERE id = ?", (t_id,))
                continue

            try:
                dest_chat = int(str(chat_id_str).replace('-100', ''))
                entity = await client_wrapper.client.get_input_entity(dest_chat)

                await client_wrapper.client.send_message(entity, text, reply_to=reply_id)
                logger.info(f"✅ Ручной ответ отправлен от {session_name} в {dest_chat}")

                with sqlite3.connect(DB_FILE) as conn:
                    conn.execute("UPDATE outbound_queue SET status = 'sent' WHERE id = ?", (t_id,))

            except Exception as e:
                logger.error(f"Ошибка отправки ручного ответа (ID {t_id}): {e}")
                with sqlite3.connect(DB_FILE) as conn:
                    conn.execute("UPDATE outbound_queue SET status = 'error' WHERE id = ?", (t_id,))

    except Exception as e:
        logger.error(f"Ошибка в outbound_queue: {e}")


async def process_manual_tasks():
    global current_settings, active_clients, POST_PROCESS_CACHE

    if 'manual_queue' not in current_settings or not current_settings['manual_queue']:
        return

    tasks = current_settings['manual_queue']
    if not tasks: return

    logger.info(f"🚀 [MANUAL] Найдено {len(tasks)} ручных задач на обработку...")

    tasks_to_remove = []

    for task in tasks:
        chat_id_raw = task['chat_id']
        post_id = task['post_id']

        target_chat = None
        for t in current_settings.get('targets', []):
            if t['chat_id'] == chat_id_raw:
                target_chat = t
                break

        if not target_chat:
            tasks_to_remove.append(task)
            continue

        eligible_clients = [c for c in list(active_clients.values())
                            if not target_chat.get('assigned_accounts', []) or c.session_name in target_chat.get(
                'assigned_accounts', [])]

        if not eligible_clients:
            logger.warning(f"⚠️ Нет активных клиентов для ручной задачи в {chat_id_raw}")
            continue

        client_wrapper = random.choice(eligible_clients)

        try:
            entity_id = int(str(chat_id_raw).replace('-100', ''))
            try:
                entity = await client_wrapper.client.get_input_entity(entity_id)
            except:
                await ensure_account_joined(client_wrapper, target_chat)
                entity = await client_wrapper.client.get_input_entity(entity_id)

            messages = await client_wrapper.client.get_messages(entity, ids=[post_id])
            if messages and messages[0]:
                msg = messages[0]

                final_chat_id = entity_id

                try:
                    discussion_res = await client_wrapper.client(
                        GetDiscussionMessageRequest(peer=entity, msg_id=post_id))
                    if discussion_res.messages:
                        found_msg = discussion_res.messages[0]
                        logger.info(f"🔄 [MANUAL] Переадресация: Пост Канала {post_id} -> Пост Группы {found_msg.id}")
                        msg = found_msg
                        final_chat_id = msg.chat_id
                except Exception as e:
                    logger.warning(f"⚠️ [MANUAL] Не удалось найти Linked-сообщение (возможно нет комментариев): {e}")

                event_mock = collections.namedtuple('EventMock', ['message', 'chat_id'])
                mock_event = event_mock(message=msg, chat_id=final_chat_id)

                logger.info(f"⚡ [MANUAL] Принудительный запуск обработки поста {msg.id} в {final_chat_id}")

                asyncio.create_task(process_new_post(mock_event, target_chat, from_catch_up=False, is_manual=True))

                tasks_to_remove.append(task)
            else:
                logger.warning(f"❌ [MANUAL] Не удалось найти сообщение {post_id} в {chat_id_raw}")
                tasks_to_remove.append(task)

        except Exception as e:
            logger.error(f"Ошибка ручной обработки поста: {e}")

    if tasks_to_remove:
        new_queue = [t for t in current_settings['manual_queue'] if t not in tasks_to_remove]
        current_settings['manual_queue'] = new_queue
        save_data(SETTINGS_FILE, current_settings)


def save_data(file_path, data):
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.error(f"Ошибка сохранения {file_path}: {e}")


async def main():
    global current_settings, active_clients, PENDING_TASKS
    init_database()
    logger.info(">>> AI-Комментатор запускается...")
    try:
        telethon_config = load_config('telethon_credentials')
        api_id, api_hash = int(telethon_config['api_id']), telethon_config['api_hash']
    except Exception as e:
        logger.critical(f"Критическая ошибка конфигурации: {e}")
        return
    while True:
        try:
            new_settings = load_json_data(SETTINGS_FILE)
            current_settings = new_settings
            status = current_settings.get('status')
            if status == 'running':
                await manage_clients(api_id, api_hash)

                await process_scenarios()
                await process_outbound_queue()
                await process_manual_tasks()

                if not active_clients:
                    logger.info("Статус: Работает, но нет активных аккаунтов (спят или не добавлены)")
                if 'rebrand_task' in current_settings and current_settings['rebrand_task'].get('status') == 'pending':
                    await run_rebrand_logic(api_id, api_hash)
            else:
                if PENDING_TASKS:
                    logger.info(f"⏳ Завершаю фоновые задачи ({len(PENDING_TASKS)})...")
                    for t in list(PENDING_TASKS):
                        t.cancel()
                    await asyncio.gather(*PENDING_TASKS, return_exceptions=True)
                if active_clients:
                    logger.info("Статус изменился на 'Остановлен'. Выключаю клиентов...")
                    for client in list(active_clients.values()):
                        await client.stop()
                    active_clients.clear()
            await asyncio.sleep(2)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.critical(f"Критическая ошибка в главном цикле: {e}", exc_info=True)
            await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(main())
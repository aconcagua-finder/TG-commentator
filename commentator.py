import ast
import configparser
import json
import logging
import asyncio
import random
import httpx
import os
import re
import difflib
import base64
import time
import collections
import sqlite3
from datetime import datetime, timezone

from google import genai
from google.genai import types as genai_types
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
from telethon import utils as tg_utils
from telethon.tl import types as tl_types
from telethon.tl.functions.account import UpdatePersonalChannelRequest, UpdateProfileRequest
from telethon.tl.types import ReactionEmoji

from app_paths import ACCOUNTS_FILE, CONFIG_FILE, DB_FILE, OLD_LOGS_FILE, PROXIES_FILE, SETTINGS_FILE, ensure_data_dir
from app_storage import load_json, save_json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Avoid leaking secrets (e.g., Telegram bot token is part of the URL).
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

SCRIPT_START_TIME = datetime.now(timezone.utc)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

current_settings = {}
active_clients = {}
handled_posts_for_comments = collections.deque(maxlen=500)
handled_posts_for_reactions = collections.deque(maxlen=500)
handled_posts_for_monitoring = collections.deque(maxlen=500)
handled_grouped_ids = collections.deque(maxlen=200)
CHANNEL_LAST_POST_TIME = {}
MONITOR_CHANNEL_LAST_POST_TIME = {}
COMMENTER_ROTATION = {}
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


DEFAULT_MODELS = {
    "openai_chat": "gpt-5.2-chat-latest",
    "openai_eval": "gpt-5.2",
    "openai_image": "gpt-image-1",
    "openrouter_chat": "x-ai/grok-4.1-fast",
    "openrouter_eval": "openai/gpt-4.1-mini",
    "deepseek_chat": "deepseek-chat",
    "deepseek_eval": "deepseek-chat",
    "gemini_chat": "gemini-3-flash-preview",
    "gemini_eval": "gemini-3-flash-preview",
    "gemini_names": "gemini-3-flash-preview",
}

DEFAULT_PROJECT_ID = "default"


def _active_project_id(settings=None):
    if isinstance(settings, dict):
        raw = settings.get("active_project_id")
    else:
        raw = current_settings.get("active_project_id") if isinstance(current_settings, dict) else None
    pid = str(raw or "").strip()
    return pid or DEFAULT_PROJECT_ID


def _project_id_for(item):
    if not isinstance(item, dict):
        return DEFAULT_PROJECT_ID
    pid = str(item.get("project_id") or "").strip()
    return pid or DEFAULT_PROJECT_ID


def _filter_project_items(items, project_id):
    if not isinstance(items, list):
        return []
    return [i for i in items if isinstance(i, dict) and _project_id_for(i) == project_id]


def get_project_targets(settings=None):
    s = settings if isinstance(settings, dict) else current_settings
    pid = _active_project_id(s)
    return _filter_project_items((s or {}).get("targets", []) or [], pid)


def get_project_reaction_targets(settings=None):
    s = settings if isinstance(settings, dict) else current_settings
    pid = _active_project_id(s)
    return _filter_project_items((s or {}).get("reaction_targets", []) or [], pid)


def get_project_monitor_targets(settings=None):
    s = settings if isinstance(settings, dict) else current_settings
    pid = _active_project_id(s)
    return _filter_project_items((s or {}).get("monitor_targets", []) or [], pid)


def get_project_manual_queue(settings=None):
    s = settings if isinstance(settings, dict) else current_settings
    pid = _active_project_id(s)
    return _filter_project_items((s or {}).get("manual_queue", []) or [], pid)


def load_project_accounts(settings=None):
    pid = _active_project_id(settings)
    accounts = load_json_data(ACCOUNTS_FILE, [])
    return _filter_project_items(accounts, pid)


def get_model_setting(settings, key, default_value=None):
    if default_value is None:
        default_value = DEFAULT_MODELS.get(key, "")
    models = settings.get("models", {}) if isinstance(settings, dict) else {}
    if isinstance(models, dict):
        value = models.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return default_value


def gemini_model_candidates(settings, key):
    primary = get_model_setting(settings, key)
    candidates = [primary]

    for fallback in ["gemini-2.5-flash", "gemini-1.5-flash"]:
        if fallback != primary:
            candidates.append(fallback)

    seen = set()
    unique = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            unique.append(candidate)

    return unique


def is_model_unavailable_error(exc):
    text = str(exc).lower()
    if "model" not in text:
        return False
    for phrase in [
        "does not exist",
        "not found",
        "no such model",
        "unknown model",
        "you do not have access",
        "not supported",
        "invalid model",
    ]:
        if phrase in text:
            return True
    return False


def openai_model_candidates(settings, key):
    primary = get_model_setting(settings, key)
    candidates = [primary]

    if key == "openai_chat":
        candidates.extend(["gpt-5.2-chat-latest", "gpt-5.2", "gpt-5-mini", "gpt-4.1", "gpt-4.1-mini", "gpt-4o"])
    elif key == "openai_eval":
        candidates.extend(["gpt-5.2", "gpt-5-mini", "gpt-4.1-mini", "gpt-4o-mini"])

    seen = set()
    unique = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            unique.append(candidate)

    return unique


def guess_image_mime_type(image_bytes):
    if not image_bytes:
        return "image/jpeg"
    if image_bytes.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if image_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if image_bytes[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if image_bytes.startswith(b"RIFF") and image_bytes[8:12] == b"WEBP":
        return "image/webp"
    return "image/jpeg"


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
            conn.execute('''
                CREATE TABLE IF NOT EXISTS inbox_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kind TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    session_name TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    msg_id INTEGER,
                    reply_to_msg_id INTEGER,
                    sender_id INTEGER,
                    sender_username TEXT,
                    sender_name TEXT,
                    chat_title TEXT,
                    chat_username TEXT,
                    text TEXT,
                    replied_to_text TEXT,
                    is_read INTEGER DEFAULT 0,
                    error TEXT
                )
            ''')
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_inbox_unique ON inbox_messages(session_name, chat_id, msg_id, direction)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_inbox_kind_unread ON inbox_messages(kind, is_read, id)")

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


def log_comment_skip_to_db(post_id, target_chat, destination_chat_id, reason):
    try:
        log_action_to_db(
            {
                "type": "comment_skip",
                "post_id": post_id,
                "comment": str(reason or "").strip(),
                "date": datetime.now(timezone.utc).isoformat(),
                "account": {"session_name": ""},
                "target": {
                    "chat_name": target_chat.get("chat_name") if isinstance(target_chat, dict) else None,
                    "chat_username": target_chat.get("chat_username") if isinstance(target_chat, dict) else None,
                    "channel_id": target_chat.get("chat_id") if isinstance(target_chat, dict) else None,
                    "destination_chat_id": destination_chat_id,
                },
            }
        )
    except Exception:
        pass


def log_inbox_message_to_db(
    *,
    kind: str,
    direction: str,
    status: str,
    session_name: str,
    chat_id: str,
    msg_id: int | None = None,
    reply_to_msg_id: int | None = None,
    sender_id: int | None = None,
    sender_username: str | None = None,
    sender_name: str | None = None,
    chat_title: str | None = None,
    chat_username: str | None = None,
    text: str | None = None,
    replied_to_text: str | None = None,
    is_read: int = 0,
    error: str | None = None,
) -> int | None:
    kind = (kind or "").strip() or "dm"
    direction = (direction or "").strip() or "in"
    status = (status or "").strip() or "received"
    session_name = (session_name or "").strip()
    chat_id = (chat_id or "").strip()
    if not session_name or not chat_id:
        return None

    created_at = datetime.now(timezone.utc).isoformat()
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR IGNORE INTO inbox_messages (
                    kind, direction, status, created_at,
                    session_name, chat_id, msg_id, reply_to_msg_id,
                    sender_id, sender_username, sender_name,
                    chat_title, chat_username,
                    text, replied_to_text,
                    is_read, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    kind,
                    direction,
                    status,
                    created_at,
                    session_name,
                    chat_id,
                    msg_id,
                    reply_to_msg_id,
                    sender_id,
                    (sender_username or "").strip() or None,
                    (sender_name or "").strip() or None,
                    (chat_title or "").strip() or None,
                    (chat_username or "").strip() or None,
                    (text or "").strip() or None,
                    (replied_to_text or "").strip() or None,
                    int(bool(is_read)),
                    (error or "").strip() or None,
                ),
            )
            conn.commit()
            return cursor.lastrowid or None
    except Exception:
        return None


def _queued_outgoing_exists(
    *,
    kind: str,
    session_name: str,
    chat_id: str,
    text: str | None,
    reply_to_msg_id: int | None,
) -> bool:
    if not session_name or not chat_id:
        return False
    try:
        with sqlite3.connect(DB_FILE) as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM inbox_messages
                WHERE kind=?
                  AND direction='out'
                  AND status='queued'
                  AND session_name=?
                  AND chat_id=?
                  AND COALESCE(text, '') = COALESCE(?, '')
                  AND COALESCE(reply_to_msg_id, -1) = COALESCE(?, -1)
                LIMIT 1
                """,
                (kind, session_name, str(chat_id), text or "", reply_to_msg_id),
            ).fetchone()
            return row is not None
    except Exception:
        return False


def load_config(section):
    parser = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Файл config.ini не найден: {CONFIG_FILE}")
    parser.read(CONFIG_FILE)
    if section not in parser:
        raise KeyError(f"В config.ini не найдена секция [{section}].")
    return parser[section]


def load_json_data(file_path, default_data=None):
    if default_data is None:
        default_data = {}
    return load_json(file_path, default_data)


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


async def generate_comment(
    post_text,
    target_chat,
    session_name,
    image_bytes=None,
    is_reply_mode=False,
    reply_to_name=None,
    extra_instructions=None,
):
    global current_settings, RECENT_GENERATED_MESSAGES

    provider = target_chat.get('ai_provider', 'default')
    if provider == 'default':
        provider = current_settings.get('ai_provider', 'gemini')

    accounts_data = load_project_accounts()
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
        user_persona = (
            "Коротенький небрежно написанный коммент, как в чатах телеграм на тему поста. "
            "Без избыточного количества эмодзи, рандомно, обращаясь к одному из ключевых тезисов поста. "
            "Не пиши как нейросеть, пиши как обычный человек с присущими ему опечатками или речевыми ошибками (не обязательно). "
            "Без всяких длинных тире и кавычек-елочек. "
            "Сообщение может быть как очень короткое, так и чуть длиннее, иногда звучать как вопрос или сомнение, "
            "мягкое отрицание написанного. "
            "От двух до 40 слов в ответе, как захочется, лучше длинных фраз и мыслей избегать."
        )

    global_blacklist = current_settings.get('blacklist', [])
    h_set = current_settings.get('humanization', {})
    custom_rules = h_set.get('custom_rules', "")
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

    system_prompt = (
        f"ТВОЯ РОЛЬ (ОТЫГРЫВАЙ ЕЕ ДОСЛОВНО):\n{user_persona}\n\n"
        f"ПРАВИЛА ОФОРМЛЕНИЯ ТЕКСТА:\n{custom_rules}\n"
    )
    if global_blacklist:
        system_prompt += f"\nНЕ ИСПОЛЬЗУЙ СЛОВА: {', '.join(global_blacklist)}"

    if is_reply_mode:
        context_prefix = f"ТЕБЕ ГОВОРИТ {reply_to_name}: " if reply_to_name else ""
        user_template = "КОНТЕКСТ ДИАЛОГА:\n{context}{post}\n\nОтветь согласно своей роли:"
    else:
        context_prefix = ""
        user_template = "ТЕКСТ ПОСТА:\n{post}\n\nНапиши комментарий от своей роли:"

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

            user_message_content = user_template.format(context=context_prefix, post=post_text_for_prompt)
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
                clean_gen = generated_text.replace('"', '').replace("'", "").lower().strip()
                if len(clean_gen) < 2:
                    last_failure = "empty_or_too_short(output)"
                    continue
                RECENT_GENERATED_MESSAGES.append(generated_text)
                return generated_text, prompt_info
            if last_failure == "unknown_error":
                last_failure = "empty_or_too_short(output)"
        except Exception as e:
            last_failure = _short_exc(e)
            continue

    model_part = f"{provider}:{last_model}" if last_model else provider
    return None, f"{prompt_info} · FAIL({model_part}): {last_failure}"


def normalize_id(chat_id):
    if not chat_id:
        return 0
    try:
        return int(str(chat_id).replace('-100', ''))
    except ValueError:
        return 0


def _select_accounts_with_rotation(chat_key: str, eligible_clients: list, count: int) -> list:
    if not eligible_clients:
        return []

    eligible_names = [c.session_name for c in eligible_clients]
    eligible_set = set(eligible_names)
    if count <= 0:
        count = len(eligible_names)
    if count >= len(eligible_names):
        count = len(eligible_names)

    state = COMMENTER_ROTATION.setdefault(chat_key, {"remaining": [], "used": set()})
    remaining = [n for n in state.get("remaining", []) if n in eligible_set]
    used = {n for n in state.get("used", set()) if n in eligible_set}

    for name in eligible_names:
        if name not in used and name not in remaining:
            remaining.append(name)

    if not remaining:
        remaining = eligible_names.copy()
        random.shuffle(remaining)
        used = set()

    selected_names = []
    selected_set = set()
    while len(selected_names) < count:
        if not remaining:
            remaining = eligible_names.copy()
            random.shuffle(remaining)
            used = set(selected_set)
            remaining = [n for n in remaining if n not in used]
            if not remaining:
                break
        take = min(count - len(selected_names), len(remaining))
        picked = remaining[:take]
        for name in picked:
            if name in selected_set:
                continue
            selected_names.append(name)
            selected_set.add(name)
            used.add(name)
        remaining = remaining[take:]

    state["remaining"] = remaining
    state["used"] = used

    by_name = {c.session_name: c for c in eligible_clients}
    return [by_name[name] for name in selected_names if name in by_name]


def make_fallback_comment_variant(base_text: str, session_name: str, msg_id: int) -> str:
    text = (base_text or "").strip()
    if not text:
        return ""

    # Deterministic per account + message, to avoid identical duplicates.
    try:
        import hashlib

        seed = int(hashlib.sha256(f"{session_name}:{msg_id}".encode("utf-8")).hexdigest()[:8], 16)
        rnd = random.Random(seed)
    except Exception:
        rnd = random

    prefixes = ["Ну", "Кстати", "Честно", "Имхо", "По-моему", "Согласен", "Мне кажется"]
    suffixes = [" (имхо)", " 👍", " 😅", " 🤷‍♂️", " 🤝"]

    prefix = rnd.choice(prefixes)
    suffix = rnd.choice(suffixes)

    out = text
    if not out.lower().startswith(prefix.lower()):
        out = f"{prefix}, {out.lstrip()}"

    out = out.rstrip()
    if not out.endswith(suffix.strip()):
        out = f"{out}{suffix}"

    return out.strip()


COMMENT_DIVERSITY_MODES = [
    "Короткий уточняющий вопрос (1 предложение).",
    "Лёгкий скепсис/контраргумент (без токсичности).",
    "Практичный совет/лайфхак по теме.",
    "Лёгкая ирония/юмор (без оскорблений).",
    "Согласие + один новый аргумент.",
    "Короткое резюме одной мысли + вывод.",
]


def _normalize_for_similarity(text: str) -> str:
    t = str(text or "").lower()
    t = re.sub(r"https?://\S+", "", t)
    t = re.sub(r"[@#][\w_]+", "", t)
    t = re.sub(r"[^\w\s]+", " ", t, flags=re.UNICODE)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _word_tokens(text: str) -> list[str]:
    t = _normalize_for_similarity(text)
    if not t:
        return []
    return [w for w in t.split() if len(w) > 2]


_URL_RE = re.compile(r"(https?://\S+|t\.me/\S+|www\.\S+)", re.IGNORECASE)
_PROMO_EXPLICIT_RE = re.compile(
    r"(#\s*)?(реклама|sponsored|ad\b|спонсор\w*|партн[её]р\w*|промокод|promo(code)?|sale|скидк\w*|акци\w*|розыгрыш|giveaway)",
    re.IGNORECASE,
)
_CTA_RE = re.compile(
    r"(куп(и|ить|ите|ай)\b|закаж(и|ать|ите)\b|оформ(и|ить)\b|переходи(те)?\b|жми\b|ссылка\s+в\s+(био|описании|профиле)\b|подпис(ывайся|ывайтесь)\b|забира(й|йте)\b|регист(рируйся|рируйтесь|рация)\b)",
    re.IGNORECASE,
)
_PRICE_RE = re.compile(r"(?:\b\d{2,}\s*(?:₽|руб(?:\.|ля|лей)?|rur|\$|usd|€|eur)\b)", re.IGNORECASE)
_TRACKING_RE = re.compile(r"(utm_[a-z_]+|ref=|aff=|promo=|coupon|promocode)", re.IGNORECASE)


def _is_promotional_post_text(text: str) -> tuple[bool, str]:
    t = str(text or "")
    if not t.strip():
        return False, ""
    if _PROMO_EXPLICIT_RE.search(t):
        return True, "explicit_marker"

    has_url = bool(_URL_RE.search(t))
    has_cta = bool(_CTA_RE.search(t))
    has_price = bool(_PRICE_RE.search(t))
    has_tracking = bool(_TRACKING_RE.search(t))

    if has_url and (has_cta or has_price or has_tracking):
        return True, "link_cta_or_price"
    if has_price and has_cta:
        return True, "price_cta"
    return False, ""


def _non_image_media_kind(message) -> str | None:
    if not message:
        return None
    try:
        if getattr(message, "voice", None):
            return "voice"
        if getattr(message, "audio", None):
            return "audio"
        if getattr(message, "video", None):
            return "video"
        if getattr(message, "gif", None):
            return "gif"
        if getattr(message, "photo", None):
            return None
        if getattr(message, "file", None):
            mime_type = getattr(message.file, "mime_type", None) or ""
            if isinstance(mime_type, str) and mime_type.lower().startswith("image/"):
                return None
            if isinstance(mime_type, str) and mime_type:
                if mime_type.lower().startswith("video/"):
                    return "video"
                if mime_type.lower().startswith("audio/"):
                    return "audio"
                return "file"
        if getattr(message, "document", None):
            return "file"
    except Exception:
        return "media"
    return None


def should_skip_post_for_commenting(message, post_text: str, target_chat: dict) -> tuple[bool, str]:
    try:
        meaningful_words = len(_word_tokens(post_text))
    except Exception:
        meaningful_words = 0

    skip_ads = bool(target_chat.get("skip_promotional_posts", True))
    if skip_ads:
        is_ad, why = _is_promotional_post_text(post_text)
        if is_ad:
            return True, f"похоже на рекламу ({why})"

    try:
        min_meaningful_words = int(target_chat.get("min_meaningful_words", 2) or 0)
    except Exception:
        min_meaningful_words = 2
    min_meaningful_words = max(min_meaningful_words, 0)

    if min_meaningful_words > 0 and meaningful_words < min_meaningful_words:
        return True, f"слишком мало текста ({meaningful_words}/{min_meaningful_words} смысловых слов)"

    skip_short_media = bool(target_chat.get("skip_short_media_posts", True))
    if skip_short_media:
        media_kind = _non_image_media_kind(message)
        if media_kind:
            try:
                media_min_words = int(target_chat.get("media_min_meaningful_words", 6) or 0)
            except Exception:
                media_min_words = 6
            media_min_words = max(media_min_words, 0)
            if media_min_words > 0 and meaningful_words < media_min_words:
                return True, f"{media_kind} + мало текста ({meaningful_words}/{media_min_words})"

    return False, ""


def _opening_signature(text: str, n: int = 4) -> tuple[str, ...]:
    tokens = _word_tokens(text)
    return tuple(tokens[:n])


def comment_similarity_score(a: str, b: str) -> float:
    na = _normalize_for_similarity(a)
    nb = _normalize_for_similarity(b)
    if not na or not nb:
        return 0.0
    ratio = difflib.SequenceMatcher(None, na, nb).ratio()
    aw = set(_word_tokens(na))
    bw = set(_word_tokens(nb))
    jaccard = (len(aw & bw) / max(len(aw | bw), 1)) if (aw or bw) else 0.0
    return max(ratio, jaccard)


def is_comment_too_similar(candidate: str, existing: list[str], threshold: float) -> tuple[bool, float, str | None]:
    best_score = 0.0
    best_text = None
    for prev in existing or []:
        score = comment_similarity_score(candidate, prev)
        if score > best_score:
            best_score = score
            best_text = prev

    too_similar = best_score >= threshold
    if best_text:
        open_a = _opening_signature(candidate, 4)
        open_b = _opening_signature(best_text, 4)
        if open_a and open_a == open_b:
            too_similar = True

    return too_similar, best_score, best_text


def _truncate_one_line(text: str, limit: int = 240) -> str:
    t = str(text or "").replace("\n", " ").strip()
    t = re.sub(r"\\s+", " ", t)
    if len(t) <= limit:
        return t
    return t[: limit - 1].rstrip() + "…"


def _extract_opening_phrases(texts: list[str], max_phrases: int = 6) -> list[str]:
    phrases: list[str] = []
    seen = set()
    for t in texts or []:
        tokens = _word_tokens(t)
        if len(tokens) < 2:
            continue
        phrase = " ".join(tokens[:4]).strip()
        if not phrase:
            continue
        if phrase in seen:
            continue
        seen.add(phrase)
        phrases.append(phrase)
        if len(phrases) >= max_phrases:
            break
    return phrases


def build_comment_diversity_instructions(
    existing_comments: list[str],
    mode_hint: str | None = None,
    strict: bool = False,
    previous_candidate: str | None = None,
) -> str:
    parts: list[str] = []

    if mode_hint:
        parts.append(f"СТИЛЕВОЙ РЕЖИМ: {mode_hint}")

    if existing_comments:
        parts.append(
            "ВАЖНО: не повторяй и не перефразируй комментарии ниже. "
            "Сделай заметно другой угол/мысль/формулировки."
        )
        openings = _extract_opening_phrases(existing_comments)
        if openings:
            parts.append('Не начинай так же. Запрещённые начала: "' + '"; "'.join(openings) + '"')
        for i, c in enumerate(existing_comments[-3:], start=1):
            parts.append(f"{i}) {_truncate_one_line(c)}")

    if strict:
        parts.append(
            "Проверка на похожесть сработала. Перепиши так, чтобы совпадений по словам было минимально "
            "(другие вводные, другие конструкции, другая подача)."
        )

    if previous_candidate:
        parts.append("ТВОЙ ПРОШЛЫЙ ВАРИАНТ (НЕ ПОВТОРЯЙ): " + _truncate_one_line(previous_candidate))

    return "\n".join([p for p in parts if p]).strip()


_RU_STOPWORDS = {
    "и",
    "а",
    "но",
    "да",
    "нет",
    "это",
    "как",
    "что",
    "в",
    "на",
    "по",
    "за",
    "к",
    "у",
    "из",
    "для",
    "с",
    "со",
    "же",
    "то",
    "тут",
    "там",
    "вот",
    "ну",
    "типа",
    "просто",
    "вообще",
    "всё",
    "все",
    "еще",
    "ещё",
    "если",
    "или",
    "когда",
    "где",
    "почему",
    "зачем",
    "потому",
    "кстати",
    "имхо",
}


def _extract_keywords(text: str, max_keywords: int = 2) -> list[str]:
    tokens = [t for t in _word_tokens(text) if t not in _RU_STOPWORDS and not t.isdigit() and len(t) >= 4]
    if not tokens:
        return []
    counts = collections.Counter(tokens)
    return [w for (w, _) in counts.most_common(max_keywords)]


def make_emergency_comment(
    post_text: str,
    session_name: str,
    msg_id: int,
    existing_comments: list[str] | None = None,
    threshold: float = 0.78,
) -> str:
    try:
        import hashlib

        seed = int(hashlib.sha256(f"emg:{session_name}:{msg_id}".encode("utf-8")).hexdigest()[:8], 16)
        rnd = random.Random(seed)
    except Exception:
        rnd = random

    kw = _extract_keywords(post_text, max_keywords=1)
    keyword = kw[0] if kw else ""

    templates_kw = [
        "интересно, а {kw} тут как считают/проверяют?",
        "звучит логично, но где подводные по {kw}?",
        "а есть примеры/цифры по {kw}?",
        "всё упрётся в {kw} на практике, кмк.",
        "{kw} тут решает больше всего, остальное вторично.",
    ]
    templates_plain = [
        "интересно, а на практике это как работает?",
        "звучит нормально, но что с подводными камнями?",
        "а есть примеры/цифры/кейсы?",
        "ну посмотрим, как оно в жизни пойдёт.",
        "в целом ок, но детали решают.",
    ]

    templates = templates_kw if keyword else templates_plain
    pool = templates.copy()
    rnd.shuffle(pool)

    existing = existing_comments or []
    for t in pool:
        text = (t.format(kw=keyword) if keyword else t).strip()
        if not text:
            continue
        if existing:
            too_sim, _, _ = is_comment_too_similar(text, existing, threshold)
            if too_sim:
                continue
        return text

    return (pool[0].format(kw=keyword) if keyword else pool[0]).strip()


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
        destination_chat_id_for_logs = event.chat_id
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
                return

        accounts_data = load_project_accounts()
        eligible_clients = [
            c
            for c in list(active_clients.values())
            if is_bot_awake(next((a for a in accounts_data if a["session_name"] == c.session_name), {}))
            and (
                not target_chat.get("assigned_accounts", [])
                or c.session_name in target_chat.get("assigned_accounts", [])
            )
        ]

        if not eligible_clients:
            log_comment_skip_to_db(
                msg_id,
                target_chat,
                destination_chat_id_for_logs,
                "нет подходящих аккаунтов (все спят / не назначены / не подключены)",
            )
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

            last_time = CHANNEL_LAST_POST_TIME.get(channel_key)
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
                    return

            CHANNEL_LAST_POST_TIME[channel_key] = msg_date

        try:
            range_min = int(target_chat.get("accounts_per_post_min", 0) or 0)
        except Exception:
            range_min = 0
        try:
            range_max = int(target_chat.get("accounts_per_post_max", 0) or 0)
        except Exception:
            range_max = 0
        range_min = max(range_min, 0)
        range_max = max(range_max, 0)

        if range_min == 0 and range_max == 0:
            selected_count = len(eligible_clients)
        else:
            if range_max < range_min:
                range_max = range_min
            if range_max == 0:
                range_max = range_min
            if range_min == 0:
                range_min = 1
            available = len(eligible_clients)
            if available > 0:
                range_min = min(range_min, available)
                range_max = min(range_max, available)
                if range_max < range_min:
                    range_max = range_min
            selected_count = random.randint(range_min, range_max)

        eligible_clients = _select_accounts_with_rotation(channel_key, eligible_clients, selected_count)

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

        sent_comments: list[str] = []
        use_modes = len(eligible_clients) > 1
        mode_pool = COMMENT_DIVERSITY_MODES.copy()
        random.shuffle(mode_pool)

        for idx, client_wrapper in enumerate(eligible_clients):
            try:
                if daily_limit > 0:
                    current_daily_count = get_daily_action_count_from_db(destination_chat_id_for_logs, "comment")
                    if current_daily_count >= daily_limit:
                        logger.info(
                            f"🧾 Лимит комментариев/сутки достигнут ({current_daily_count}/{daily_limit}) для {destination_chat_id_for_logs}. Останавливаюсь."
                        )
                        break

                mode_hint = None
                if use_modes and mode_pool:
                    mode_hint = mode_pool[idx % len(mode_pool)]

                extra = build_comment_diversity_instructions(
                    sent_comments,
                    mode_hint=mode_hint,
                )

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
                    if not too_similar:
                        break

                    failure_reason = f"too_similar(score={score:.2f})"
                    logger.info(
                        f"♻️ [{client_wrapper.session_name}] комментарий слишком похож (score={score:.2f}). Перегенерирую..."
                    )
                    extra = build_comment_diversity_instructions(
                        sent_comments,
                        mode_hint=mode_hint,
                        strict=True,
                        previous_candidate=generated_text,
                    )
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

                await human_type_and_send(
                    client_wrapper.client,
                    event.chat_id,
                    generated_text,
                    reply_to_msg_id=msg_id,
                )
                me = await client_wrapper.client.get_me()
                logger.info(f"✅ [{client_wrapper.session_name}] прокомментировал пост {msg_id} ({prompt_info})")
                sent_comments.append(generated_text)

                log_content = f"[{prompt_info}] {generated_text}"
                log_action_to_db(
                    {
                        "type": "comment",
                        "post_id": msg_id,
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

                if delay_between > 0 and idx != len(eligible_clients) - 1:
                    await asyncio.sleep(delay_between)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"❌ Ошибка комментирования ({client_wrapper.session_name}): {e}")
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


def _extract_existing_reaction_emojis(message):
    emojis = []
    if not message:
        return emojis
    reactions = getattr(message, "reactions", None)
    results = getattr(reactions, "results", None) if reactions else None
    if not results:
        return emojis
    for r in results:
        reaction_obj = getattr(r, "reaction", None)
        emoticon = getattr(reaction_obj, "emoticon", None) if reaction_obj else None
        if emoticon:
            emojis.append(emoticon)
    return emojis


def _select_reaction_emojis(desired, existing, count):
    desired = [str(x).strip() for x in (desired or []) if str(x).strip()]
    existing = [str(x).strip() for x in (existing or []) if str(x).strip()]

    pool = []
    if existing:
        intersect = [e for e in desired if e in existing]
        pool = intersect or existing
    else:
        pool = desired

    if not pool:
        return []
    count = max(int(count or 1), 1)
    if count == 1 or len(pool) == 1:
        return [random.choice(pool)]
    return random.sample(pool, min(count, len(pool)))


async def process_new_post_for_reaction(source_channel_peer, original_post_id, reaction_target, message=None):
    global active_clients, PENDING_TASKS
    task = asyncio.current_task()
    PENDING_TASKS.add(task)
    try:
        chance = reaction_target.get('reaction_chance', 80)
        if random.randint(1, 100) > chance:
            return
        destination_chat_id_for_logs = int(reaction_target.get('chat_id', reaction_target.get('linked_chat_id')))
        daily_limit = reaction_target.get('daily_reaction_limit', 999)
        current_daily_count = get_daily_action_count_from_db(destination_chat_id_for_logs, 'reaction')
        if current_daily_count >= daily_limit:
            return
        accounts_data = load_project_accounts()
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
        desired_reactions = reaction_target.get("reactions", []) or []
        existing_reactions = _extract_existing_reaction_emojis(message)
        for client_wrapper in eligible_clients:
            try:
                current_daily_count = get_daily_action_count_from_db(destination_chat_id_for_logs, 'reaction')
                if current_daily_count >= daily_limit:
                    break
                if not desired_reactions and not existing_reactions:
                    continue

                num_to_set = reaction_target.get("reaction_count", 1)
                reactions_to_set_str = _select_reaction_emojis(desired_reactions, existing_reactions, num_to_set)
                if not reactions_to_set_str:
                    continue

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
                try:
                    await client_wrapper.client(
                        SendReactionRequest(peer=actual_peer, msg_id=original_post_id, reaction=tl_reactions)
                    )
                except ReactionsTooManyError:
                    if not existing_reactions:
                        try:
                            msg = await client_wrapper.client.get_messages(actual_peer, ids=original_post_id)
                            msg = msg[0] if isinstance(msg, list) else msg
                            existing_reactions = _extract_existing_reaction_emojis(msg)
                        except Exception:
                            existing_reactions = []

                    fallback = _select_reaction_emojis(desired_reactions, existing_reactions, 1)
                    if not fallback:
                        raise
                    reactions_to_set_str = fallback
                    tl_reactions = [ReactionEmoji(emoticon=r) for r in reactions_to_set_str]
                    await client_wrapper.client(
                        SendReactionRequest(peer=actual_peer, msg_id=original_post_id, reaction=tl_reactions)
                    )

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

        timeout = httpx.Timeout(20.0, connect=10.0)
        async with httpx.AsyncClient(timeout=timeout) as client:
            for owner_id in owners:
                try:
                    payload = {
                        "chat_id": int(owner_id),
                        "text": safe_text,
                        "parse_mode": "HTML",
                        "link_preview_options": {"is_disabled": True},
                    }
                    if reply_markup:
                        payload["reply_markup"] = reply_markup

                    await client.post(f"https://api.telegram.org/bot{token}/sendMessage", json=payload)
                except Exception:
                    try:
                        payload.pop("parse_mode", None)
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

        accounts_data = load_project_accounts(current_settings)
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
            global current_settings, handled_posts_for_comments, handled_posts_for_reactions, handled_posts_for_monitoring, handled_grouped_ids, REPLY_PROCESS_CACHE

            try:
                event_chat_id = int(str(event.chat_id).replace('-100', ''))
            except:
                event_chat_id = event.chat_id

            msg_id = event.message.id
            sender_id = event.sender_id

            is_channel_post = event.message.post or (event.message.fwd_from and event.message.fwd_from.channel_post)

            # Inbox: private DMs + replies to our messages in groups/chats ("цитирование").
            try:
                our_ids = get_all_our_user_ids()
            except Exception:
                our_ids = set()

            if event.out:
                try:
                    text = event.message.text or ""
                    if not text:
                        try:
                            if getattr(event.message, "photo", None):
                                text = "[фото]"
                            elif getattr(event.message, "video", None) or getattr(event.message, "gif", None):
                                text = "[видео]"
                            elif getattr(event.message, "voice", None):
                                text = "[голосовое]"
                            elif getattr(event.message, "audio", None):
                                text = "[аудио]"
                            elif getattr(event.message, "document", None) or getattr(event.message, "file", None):
                                text = "[файл]"
                        except Exception:
                            pass

                    if event.is_private:
                        if _queued_outgoing_exists(
                            kind="dm",
                            session_name=self.session_name,
                            chat_id=str(event.chat_id),
                            text=text,
                            reply_to_msg_id=getattr(event.message, "reply_to_msg_id", None),
                        ):
                            return

                        chat = None
                        try:
                            chat = await event.get_chat()
                        except Exception:
                            chat = None
                        chat_username = getattr(chat, "username", "") if chat else ""
                        chat_title = ""
                        if chat:
                            fn = getattr(chat, "first_name", "") or ""
                            ln = getattr(chat, "last_name", "") or ""
                            chat_title = (f"{fn} {ln}").strip() or (chat_username or "")

                        log_inbox_message_to_db(
                            kind="dm",
                            direction="out",
                            status="sent",
                            session_name=self.session_name,
                            chat_id=str(event.chat_id),
                            msg_id=msg_id,
                            reply_to_msg_id=getattr(event.message, "reply_to_msg_id", None),
                            sender_id=getattr(self, "user_id", None),
                            chat_title=chat_title or None,
                            chat_username=chat_username or None,
                            text=text,
                            is_read=1,
                        )

                    elif event.is_reply:
                        reply_id = getattr(event.message, "reply_to_msg_id", None)
                        if reply_id:
                            with sqlite3.connect(DB_FILE) as conn:
                                found = conn.execute(
                                    """
                                    SELECT 1 FROM inbox_messages
                                    WHERE kind='quote' AND direction='in'
                                      AND session_name=? AND chat_id=? AND msg_id=?
                                    LIMIT 1
                                    """,
                                    (self.session_name, str(event.chat_id), reply_id),
                                ).fetchone()
                            if found:
                                if _queued_outgoing_exists(
                                    kind="quote",
                                    session_name=self.session_name,
                                    chat_id=str(event.chat_id),
                                    text=text,
                                    reply_to_msg_id=reply_id,
                                ):
                                    return

                                chat = None
                                try:
                                    chat = await event.get_chat()
                                except Exception:
                                    chat = None
                                chat_title = getattr(chat, "title", "") if chat else ""
                                chat_username = getattr(chat, "username", "") if chat else ""

                                reply_msg = None
                                try:
                                    reply_msg = await event.get_reply_message()
                                except Exception:
                                    reply_msg = None

                                replied_to_text = ""
                                if reply_msg:
                                    replied_to_text = (
                                        getattr(reply_msg, "text", None)
                                        or getattr(reply_msg, "message", None)
                                        or ""
                                    )

                                log_inbox_message_to_db(
                                    kind="quote",
                                    direction="out",
                                    status="sent",
                                    session_name=self.session_name,
                                    chat_id=str(event.chat_id),
                                    msg_id=msg_id,
                                    reply_to_msg_id=reply_id,
                                    sender_id=getattr(self, "user_id", None),
                                    chat_title=(chat_title or "").strip() or None,
                                    chat_username=(chat_username or "").strip() or None,
                                    text=text,
                                    replied_to_text=replied_to_text or None,
                                    is_read=1,
                                )
                except Exception:
                    pass
                return

            if sender_id and sender_id in our_ids:
                our_ids = our_ids  # keep for later logic
            try:
                if event.is_private and sender_id and sender_id not in our_ids:
                    sender = None
                    try:
                        sender = await event.get_sender()
                    except Exception:
                        sender = None
                    sender_username = getattr(sender, "username", "") if sender else ""
                    sender_name = ""
                    if sender:
                        fn = getattr(sender, "first_name", "") or ""
                        ln = getattr(sender, "last_name", "") or ""
                        sender_name = (f"{fn} {ln}").strip() or (sender_username or "")
                    if not sender_name and sender_id:
                        sender_name = str(sender_id)

                    text = event.message.text or ""
                    if not text:
                        try:
                            if getattr(event.message, "photo", None):
                                text = "[фото]"
                            elif getattr(event.message, "video", None) or getattr(event.message, "gif", None):
                                text = "[видео]"
                            elif getattr(event.message, "voice", None):
                                text = "[голосовое]"
                            elif getattr(event.message, "audio", None):
                                text = "[аудио]"
                            elif getattr(event.message, "document", None) or getattr(event.message, "file", None):
                                text = "[файл]"
                        except Exception:
                            pass

                    log_inbox_message_to_db(
                        kind="dm",
                        direction="in",
                        status="received",
                        session_name=self.session_name,
                        chat_id=str(event.chat_id),
                        msg_id=msg_id,
                        sender_id=sender_id,
                        sender_username=sender_username,
                        sender_name=sender_name,
                        chat_title=sender_name,
                        chat_username=sender_username,
                        text=text,
                        is_read=0,
                    )

                if event.is_reply and sender_id and sender_id not in our_ids and not event.is_private:
                    reply_msg = None
                    try:
                        reply_msg = await event.get_reply_message()
                    except Exception:
                        reply_msg = None

                    if reply_msg and getattr(reply_msg, "sender_id", None) == getattr(self, "user_id", None):
                        sender = None
                        try:
                            sender = await event.get_sender()
                        except Exception:
                            sender = None
                        sender_username = getattr(sender, "username", "") if sender else ""
                        sender_name = ""
                        if sender:
                            fn = getattr(sender, "first_name", "") or ""
                            ln = getattr(sender, "last_name", "") or ""
                            sender_name = (f"{fn} {ln}").strip() or (sender_username or "")
                        if not sender_name and sender_id:
                            sender_name = str(sender_id)

                        chat = None
                        try:
                            chat = await event.get_chat()
                        except Exception:
                            chat = None
                        chat_title = getattr(chat, "title", "") if chat else ""
                        chat_username = getattr(chat, "username", "") if chat else ""

                        text = event.message.text or ""
                        if not text:
                            try:
                                if getattr(event.message, "photo", None):
                                    text = "[фото]"
                                elif getattr(event.message, "video", None) or getattr(event.message, "gif", None):
                                    text = "[видео]"
                                elif getattr(event.message, "voice", None):
                                    text = "[голосовое]"
                                elif getattr(event.message, "audio", None):
                                    text = "[аудио]"
                                elif getattr(event.message, "document", None) or getattr(event.message, "file", None):
                                    text = "[файл]"
                            except Exception:
                                pass

                        replied_to_text = getattr(reply_msg, "text", None) or getattr(reply_msg, "message", None) or ""
                        log_inbox_message_to_db(
                            kind="quote",
                            direction="in",
                            status="received",
                            session_name=self.session_name,
                            chat_id=str(event.chat_id),
                            msg_id=msg_id,
                            reply_to_msg_id=getattr(event.message, "reply_to_msg_id", None),
                            sender_id=sender_id,
                            sender_username=sender_username,
                            sender_name=sender_name,
                            chat_title=chat_title,
                            chat_username=chat_username,
                            text=text,
                            replied_to_text=replied_to_text,
                            is_read=0,
                        )
            except Exception:
                pass

            found_target = None
            for t in get_project_targets(current_settings):
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


            if is_channel_post:
                unique_id = f"{event_chat_id}_{msg_id}"
                for r_target in get_project_reaction_targets(current_settings):
                    try:
                        if event_chat_id != int(str(r_target.get("chat_id", 0)).replace("-100", "")):
                            continue
                    except Exception:
                        continue
                    if unique_id not in handled_posts_for_reactions:
                        handled_posts_for_reactions.append(unique_id)
                        asyncio.create_task(
                            process_new_post_for_reaction(event.input_chat, msg_id, r_target, message=event.message)
                        )

            if not event.message.fwd_from and event.is_group and found_target:
                if found_target.get('ai_enabled', True) and found_target.get('reply_chance', 0) > 0:
                    asyncio.create_task(process_reply_to_comment(event, found_target))

            if event.is_channel and not event.message.fwd_from:
                for m_t in get_project_monitor_targets(current_settings):
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
    accounts_from_file = load_project_accounts(current_settings)

    file_session_names = {acc['session_name'] for acc in accounts_from_file if acc.get('status') != 'banned'}
    for session_name in list(active_clients.keys()):
        acc_data = next((a for a in accounts_from_file if a['session_name'] == session_name), None)
        if session_name not in file_session_names or not acc_data:
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

        if session_name not in active_clients:
            client_wrapper = CommentatorClient(account_data, api_id, api_hash)
            if await client_wrapper.start():
                active_clients[session_name] = client_wrapper
                logger.info(f"Клиент {session_name} запущен.")
            else:
                continue

        client_wrapper = active_clients[session_name]

        for target in get_project_targets(current_settings):
            assigned = target.get('assigned_accounts', [])
            if not assigned or session_name in assigned:
                await ensure_account_joined(client_wrapper, target)

                catch_up_key = f"{session_name}_{target.get('chat_id')}"
                if catch_up_key not in CLIENT_CATCH_UP_STATUS:
                    CLIENT_CATCH_UP_STATUS.add(catch_up_key)
                    asyncio.create_task(catch_up_missed_posts(client_wrapper, target))

            for target in get_project_reaction_targets(current_settings):
                assigned = target.get('assigned_accounts', [])
                if not assigned or session_name in assigned:
                    await ensure_account_joined(client_wrapper, target)

            for target in get_project_monitor_targets(current_settings):
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
    accounts = load_project_accounts()
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
    accounts_data = load_project_accounts(current_settings)
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
    try:
        max_words = int(h_set.get('max_words', 0) or 0)
    except Exception:
        max_words = 0

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

    if max_words > 0:
        processed_words = processed_words[:max_words]

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

            model_key = "deepseek_chat" if provider == "deepseek" else ("openrouter_chat" if provider == "openrouter" else "openai_chat")
            model_name = get_model_setting(current_settings, model_key)
            client = openai.AsyncOpenAI(api_key=api_key, base_url=base_url, default_headers=default_headers)
            create_kwargs = {
                "messages": [{"role": "user", "content": system_prompt}],
                "model": model_name,
                "temperature": 1.4,
            }
            if provider == "openai":
                create_kwargs["max_completion_tokens"] = 512
            else:
                create_kwargs["max_tokens"] = 512
            completion = await client.chat.completions.create(**create_kwargs)
            content = completion.choices[0].message.content.strip()
        elif provider == 'gemini':
            for model_name in gemini_model_candidates(current_settings, "gemini_names"):
                try:
                    async with genai.Client(api_key=api_key).aio as aclient:
                        response = await aclient.models.generate_content(
                            model=model_name,
                            contents=system_prompt,
                            config=genai_types.GenerateContentConfig(
                                temperature=1.4,
                                max_output_tokens=256,
                            ),
                        )
                    content = (response.text or "").strip()
                    break
                except Exception:
                    content = ""
                    continue

        content = content.replace("```python", "").replace("```json", "").replace("```", "")
        names_list = ast.literal_eval(content)
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
                        image_model = get_model_setting(current_settings, "openai_image")
                        logger.info(f"🎨 Генерирую аватар через OpenAI ({image_model}) для {client_wrapper.session_name}...")
                        openai_client = openai.AsyncOpenAI(api_key=openai_key)
                        dalle_prompt = f"Avatar profile picture for social media, topic: {raw_source}, style: realistic, high quality, professional headshot"

                        for model_to_try in [image_model, "dall-e-3"]:
                            if not model_to_try:
                                continue
                            try:
                                params = {"model": model_to_try, "prompt": dalle_prompt, "size": "1024x1024", "n": 1}
                                if model_to_try == "dall-e-3":
                                    params["quality"] = "standard"
                                response = await openai_client.images.generate(**params)

                                image_item = response.data[0]
                                b64_json = getattr(image_item, "b64_json", None)
                                image_url = getattr(image_item, "url", None)

                                if b64_json:
                                    image_bytes = base64.b64decode(b64_json)
                                    with open(photo_path, 'wb') as f:
                                        f.write(image_bytes)
                                    got_photo = True
                                    break

                                if image_url:
                                    async with httpx.AsyncClient(timeout=30.0) as http_client:
                                        resp = await http_client.get(image_url)
                                        if resp.status_code == 200:
                                            with open(photo_path, 'wb') as f:
                                                f.write(resp.content)
                                            got_photo = True
                                            break
                            except Exception:
                                continue
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
    save_json(SETTINGS_FILE, current_settings)
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
                    timeout = httpx.Timeout(20.0, connect=10.0)
                    async with httpx.AsyncClient(timeout=timeout) as client:
                        for oid in owner_ids:
                            try:
                                safe_url = str(url).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                                text = f"⚠️ <b>Прокси не работает!</b>\nURL: <code>{safe_url}</code>"
                                await client.post(f"https://api.telegram.org/bot{bot_token}/sendMessage",
                                                  json={
                                                      "chat_id": oid,
                                                      "text": text,
                                                      "parse_mode": "HTML",
                                                      "link_preview_options": {"is_disabled": True},
                                                  })
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

    accounts_data = load_project_accounts(current_settings)
    ordered_accounts = [acc for acc in accounts_data if acc.get('status') != 'banned']

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
        project_sessions = {
            a.get("session_name") for a in load_project_accounts(current_settings) if a.get("session_name")
        }
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
            if session_name not in project_sessions:
                continue

            client_wrapper = active_clients.get(session_name)
            temp_client = None
            client = client_wrapper.client if client_wrapper else None
            if client is None:
                try:
                    telethon_config = load_config('telethon_credentials')
                    api_id, api_hash = int(telethon_config['api_id']), telethon_config['api_hash']
                    accounts_data = load_project_accounts(current_settings)
                    account_data = next((a for a in accounts_data if a.get('session_name') == session_name), None)
                    if not account_data:
                        raise KeyError("account_not_found")
                    temp_client = await _connect_temp_client(account_data, api_id, api_hash)
                    client = temp_client
                except Exception as e:
                    with sqlite3.connect(DB_FILE) as conn:
                        conn.execute("UPDATE outbound_queue SET status = 'failed_no_client' WHERE id = ?", (t_id,))
                    kind = "quote" if reply_id else "dm"
                    with sqlite3.connect(DB_FILE) as conn:
                        cur = conn.cursor()
                        cur.execute(
                            """
                            UPDATE inbox_messages
                            SET status='error', error=?
                            WHERE id = (
                              SELECT id
                              FROM inbox_messages
                              WHERE kind=? AND direction='out' AND status='queued'
                                AND session_name=? AND chat_id=? AND text=?
                              ORDER BY id DESC
                              LIMIT 1
                            )
                            """,
                            (f"no_client:{e}", kind, session_name, str(chat_id_str), text),
                        )
                        if cur.rowcount == 0:
                            conn.execute(
                                """
                                INSERT INTO inbox_messages (
                                  kind, direction, status, created_at,
                                  session_name, chat_id, reply_to_msg_id,
                                  text, is_read, error
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    kind,
                                    "out",
                                    "error",
                                    datetime.now(timezone.utc).isoformat(),
                                    session_name,
                                    str(chat_id_str),
                                    reply_id,
                                    text,
                                    1,
                                    f"no_client:{e}",
                                ),
                            )
                        conn.commit()
                    continue

            try:
                dest_chat = int(str(chat_id_str).replace('-100', ''))
                entity = await client.get_input_entity(dest_chat)

                sent_msg = await client.send_message(entity, text, reply_to=reply_id)
                logger.info(f"✅ Ручной ответ отправлен от {session_name} в {dest_chat}")

                with sqlite3.connect(DB_FILE) as conn:
                    conn.execute("UPDATE outbound_queue SET status = 'sent' WHERE id = ?", (t_id,))

                # Mark the queued row (if any) as sent; otherwise insert a fresh row.
                now = datetime.now(timezone.utc).isoformat()
                kind = "quote" if reply_id else "dm"
                msg_id = getattr(sent_msg, "id", None) if sent_msg else None
                with sqlite3.connect(DB_FILE) as conn:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        UPDATE inbox_messages
                        SET status='sent', msg_id=?, reply_to_msg_id=?, error=NULL
                        WHERE id = (
                          SELECT id
                          FROM inbox_messages
                          WHERE kind=? AND direction='out' AND status='queued'
                            AND session_name=? AND chat_id=? AND text=?
                          ORDER BY id DESC
                          LIMIT 1
                        )
                        """,
                        (msg_id, reply_id, kind, session_name, str(chat_id_str), text),
                    )
                    if cur.rowcount == 0:
                        conn.execute(
                            """
                            INSERT INTO inbox_messages (
                              kind, direction, status, created_at,
                              session_name, chat_id, msg_id, reply_to_msg_id,
                              text, is_read
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (kind, "out", "sent", now, session_name, str(chat_id_str), msg_id, reply_id, text, 1),
                        )
                    conn.commit()

            except Exception as e:
                logger.error(f"Ошибка отправки ручного ответа (ID {t_id}): {e}")
                with sqlite3.connect(DB_FILE) as conn:
                    conn.execute("UPDATE outbound_queue SET status = 'error' WHERE id = ?", (t_id,))
                kind = "quote" if reply_id else "dm"
                with sqlite3.connect(DB_FILE) as conn:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        UPDATE inbox_messages
                        SET status='error', error=?
                        WHERE id = (
                          SELECT id
                          FROM inbox_messages
                          WHERE kind=? AND direction='out' AND status='queued'
                            AND session_name=? AND chat_id=? AND text=?
                          ORDER BY id DESC
                          LIMIT 1
                        )
                        """,
                        (str(e), kind, session_name, str(chat_id_str), text),
                    )
                    if cur.rowcount == 0:
                        conn.execute(
                            """
                            INSERT INTO inbox_messages (
                              kind, direction, status, created_at,
                              session_name, chat_id, msg_id, reply_to_msg_id,
                              text, is_read, error
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                kind,
                                "out",
                                "error",
                                datetime.now(timezone.utc).isoformat(),
                                session_name,
                                str(chat_id_str),
                                None,
                                reply_id,
                                text,
                                1,
                                str(e),
                            ),
                        )
                    conn.commit()
            finally:
                if temp_client is not None:
                    try:
                        if temp_client.is_connected():
                            await temp_client.disconnect()
                    except Exception:
                        pass

    except Exception as e:
        logger.error(f"Ошибка в outbound_queue: {e}")


async def process_manual_tasks():
    global current_settings, active_clients, POST_PROCESS_CACHE

    if 'manual_queue' not in current_settings or not current_settings['manual_queue']:
        return

    tasks = get_project_manual_queue(current_settings)
    if not tasks: return

    logger.info(f"🚀 [MANUAL] Найдено {len(tasks)} ручных задач на обработку...")

    tasks_to_remove = []

    for task in tasks:
        chat_id_raw = task['chat_id']
        post_id = task['post_id']

        target_chat = None
        for t in get_project_targets(current_settings):
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


def _parse_proxy_url(url: str | None):
    if not url:
        return None
    try:
        protocol, rest = url.split("://", 1)
        auth, addr = rest.split("@", 1)
        user, password = auth.split(":", 1)
        host, port = addr.split(":", 1)
        return (protocol, host, int(port), True, user, password)
    except Exception:
        return None


async def _connect_temp_client(account_data: dict, api_id: int, api_hash: str):
    proxy = _parse_proxy_url(account_data.get("proxy_url"))
    client = TelegramClient(StringSession(account_data["session_string"]), api_id, api_hash, proxy=proxy)
    await client.connect()
    if not await client.is_user_authorized():
        try:
            await client.disconnect()
        except Exception:
            pass
        raise RuntimeError("account_not_authorized")
    return client


async def _clear_profile_photo(client: TelegramClient) -> None:
    try:
        photos = await client.get_profile_photos("me", limit=1)
    except Exception:
        photos = []
    if not photos:
        return
    try:
        input_photos = [tg_utils.get_input_photo(p) for p in photos if p]
        input_photos = [p for p in input_photos if p]
        if input_photos:
            await client(DeletePhotosRequest(id=input_photos))
    except Exception:
        pass


async def update_account_profile(
    client: TelegramClient,
    *,
    first_name: str | None = None,
    last_name: str | None = None,
    bio: str | None = None,
    avatar_path: str | None = None,
    avatar_clear: bool = False,
    personal_channel: str | None = None,
    personal_channel_clear: bool = False,
) -> None:
    if first_name is not None:
        first_name = str(first_name).strip()
        if not first_name:
            raise ValueError("first_name_empty")
    if last_name is not None:
        last_name = str(last_name).strip()
    if bio is not None:
        bio = str(bio).strip()

    if first_name is not None or last_name is not None or bio is not None:
        await client(
            UpdateProfileRequest(
                first_name=first_name,
                last_name=last_name,
                about=bio,
            )
        )

    if avatar_clear:
        await _clear_profile_photo(client)

    if avatar_path:
        p = str(avatar_path).strip()
        if p and os.path.exists(p):
            uploaded = await client.upload_file(p)
            await client(UploadProfilePhotoRequest(file=uploaded))
        else:
            raise FileNotFoundError("avatar_file_not_found")

    if personal_channel_clear:
        await client(UpdatePersonalChannelRequest(channel=tl_types.InputChannelEmpty()))
    elif personal_channel:
        ref = str(personal_channel).strip().replace("@", "")
        if not ref:
            raise ValueError("personal_channel_empty")
        entity = await client.get_entity(ref)
        input_channel = tg_utils.get_input_channel(entity)
        await client(UpdatePersonalChannelRequest(channel=input_channel))


async def process_profile_tasks(api_id: int, api_hash: str) -> None:
    global current_settings, active_clients
    tasks = current_settings.get("profile_tasks")
    if not isinstance(tasks, dict) or not tasks:
        return

    accounts_data = load_project_accounts(current_settings)
    accounts_by_session = {
        a.get("session_name"): a for a in accounts_data if isinstance(a, dict) and a.get("session_name")
    }
    allowed_sessions = set(accounts_by_session.keys())

    for session_name, task in list(tasks.items()):
        if not isinstance(task, dict):
            continue
        if task.get("status") != "pending":
            continue
        if session_name not in allowed_sessions:
            continue

        task["status"] = "processing"
        task["started_at"] = datetime.now(timezone.utc).isoformat()
        task["error"] = ""
        save_json(SETTINGS_FILE, current_settings)

        temp_client = None
        try:
            account_data = accounts_by_session.get(session_name)
            if not account_data:
                raise KeyError("account_not_found")

            wrapper = active_clients.get(session_name)
            client = wrapper.client if wrapper else None
            if client is None:
                temp_client = await _connect_temp_client(account_data, api_id, api_hash)
                client = temp_client

            avatar_path = str(task.get("avatar_path") or "").strip() or None
            bio = task.get("bio")
            first_name = task.get("first_name")
            last_name = task.get("last_name")
            personal_channel = task.get("personal_channel")
            avatar_clear = bool(task.get("avatar_clear"))
            personal_channel_clear = bool(task.get("personal_channel_clear"))

            await update_account_profile(
                client,
                first_name=first_name,
                last_name=last_name,
                bio=bio,
                avatar_path=avatar_path,
                avatar_clear=avatar_clear,
                personal_channel=personal_channel,
                personal_channel_clear=personal_channel_clear,
            )

            try:
                me = await client.get_me()
                if me:
                    account_data["user_id"] = getattr(me, "id", account_data.get("user_id"))
                    account_data["first_name"] = getattr(me, "first_name", account_data.get("first_name"))
                    account_data["last_name"] = getattr(me, "last_name", "") or ""
                    account_data["username"] = getattr(me, "username", "") or ""
            except Exception:
                pass

            if bio is not None:
                account_data["profile_bio"] = str(bio)
            if personal_channel_clear:
                account_data.pop("profile_personal_channel", None)
            elif personal_channel:
                account_data["profile_personal_channel"] = str(personal_channel)

            save_json(ACCOUNTS_FILE, accounts_data)

            task["status"] = "done"
            task["finished_at"] = datetime.now(timezone.utc).isoformat()
            task["error"] = ""
            save_json(SETTINGS_FILE, current_settings)

            if avatar_path and os.path.exists(avatar_path):
                try:
                    os.remove(avatar_path)
                except Exception:
                    pass
            logger.info(f"🧩 [{session_name}] профиль обновлён.")
        except Exception as e:
            task["status"] = "failed"
            task["finished_at"] = datetime.now(timezone.utc).isoformat()
            task["error"] = str(e)
            save_json(SETTINGS_FILE, current_settings)
            logger.error(f"🧩 [{session_name}] ошибка обновления профиля: {e}")
        finally:
            if temp_client is not None:
                try:
                    if temp_client.is_connected():
                        await temp_client.disconnect()
                except Exception:
                    pass


def save_data(file_path, data):
    try:
        save_json(file_path, data)
    except Exception as e:
        logger.error(f"Ошибка сохранения {file_path}: {e}")


async def main():
    global current_settings, active_clients, PENDING_TASKS
    ensure_data_dir()
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

                await process_profile_tasks(api_id, api_hash)
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
                await process_profile_tasks(api_id, api_hash)
            await asyncio.sleep(2)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.critical(f"Критическая ошибка в главном цикле: {e}", exc_info=True)
            await asyncio.sleep(30)


if __name__ == "__main__":
    asyncio.run(main())

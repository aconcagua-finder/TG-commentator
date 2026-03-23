import asyncio
import collections
import configparser
import json
import logging
import os
from datetime import datetime, timedelta, timezone

from app_paths import CONFIG_FILE, OLD_LOGS_FILE, PROXIES_FILE, SETTINGS_FILE, ensure_data_dir
from app_storage import load_json, save_json
from role_engine import ensure_role_schema
from services.client import manage_clients
from services.commenting import process_new_post
from services.discussions import process_discussion_queue, process_discussion_start_queue
from services.manual_tasks import process_manual_tasks
from services.outbound import process_outbound_queue
from services.profile import process_profile_tasks
from services.project import (
    ensure_discussion_targets_schema,
    migrate_legacy_manual_queue_to_db as _migrate_legacy_manual_queue_to_db_impl,
)
from services.rebrand import run_rebrand_logic
from services.scenarios import process_scenarios

logger = logging.getLogger(__name__)
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
NO_ACTIVE_CLIENTS_LOG_INTERVAL = timedelta(minutes=5)


def configure_logging():
    """Configure root logging with a single stdout handler.

    Docker and repeated interpreter initialisation can leave multiple root
    handlers attached. Reset the root logger explicitly so each record is
    emitted once.
    """
    root_logger = logging.getLogger()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)

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
EVENT_HANDLER_LOCK = asyncio.Lock()
LATEST_CHANNEL_POSTS = {}
JOINED_CACHE = set()
PROCESSING_CACHE = set()
PROCESSED_BURST_IDS = set()
CHAT_REPLY_COOLDOWN = {}
REPLY_PROCESS_CACHE = set()
POST_PROCESS_CACHE = set()
POST_PROCESS_CACHE_ORDER = collections.deque()
POST_PROCESS_CACHE_MAX = 5000
DISCUSSION_START_CACHE = set()
DISCUSSION_START_CACHE_ORDER = collections.deque()
DISCUSSION_START_CACHE_MAX = 2000
DISCUSSION_ACTIVE_TASKS = {}
DISCUSSION_START_SUPPRESS_CHAT_IDS = set()
PENDING_TASKS = set()
SCENARIO_CONTEXT = {}
CLIENT_CATCH_UP_STATUS = set()
RECENT_GENERATED_MESSAGES = collections.deque(maxlen=100)


def _build_shared_state():
    """Build the shared_state dict consumed by CommentatorClient and manage_clients."""
    def _get_current_settings():
        return current_settings
    def _update_current_settings(new):
        global current_settings
        current_settings = new
    return {
        "event_handler_lock": EVENT_HANDLER_LOCK,
        "current_settings_ref": _get_current_settings,
        "current_settings_update": _update_current_settings,
        "active_clients": active_clients,
        "handled_posts_for_comments": handled_posts_for_comments,
        "handled_posts_for_reactions": handled_posts_for_reactions,
        "handled_posts_for_monitoring": handled_posts_for_monitoring,
        "handled_grouped_ids": handled_grouped_ids,
        "reply_process_cache": REPLY_PROCESS_CACHE,
        "discussion_start_suppress_chat_ids": DISCUSSION_START_SUPPRESS_CHAT_IDS,
        "discussion_active_tasks": DISCUSSION_ACTIVE_TASKS,
        "discussion_start_cache": DISCUSSION_START_CACHE,
        "discussion_start_cache_order": DISCUSSION_START_CACHE_ORDER,
        "discussion_start_cache_max": DISCUSSION_START_CACHE_MAX,
        "pending_tasks": PENDING_TASKS,
        "scenario_context": SCENARIO_CONTEXT,
        "processing_cache": PROCESSING_CACHE,
        "post_process_cache": POST_PROCESS_CACHE,
        "post_process_cache_order": POST_PROCESS_CACHE_ORDER,
        "post_process_cache_max": POST_PROCESS_CACHE_MAX,
        "channel_last_post_time": CHANNEL_LAST_POST_TIME,
        "monitor_channel_last_post_time": MONITOR_CHANNEL_LAST_POST_TIME,
        "recent_generated_messages": RECENT_GENERATED_MESSAGES,
        "client_catch_up_status": CLIENT_CATCH_UP_STATUS,
        "joined_cache": JOINED_CACHE,
    }


def _make_process_new_post_fn():
    """Return a wrapper around process_new_post that injects shared global state."""
    async def _wrapper(event, target_chat, from_catch_up=False, is_manual=False):
        return await process_new_post(
            event, target_chat,
            from_catch_up=from_catch_up,
            is_manual=is_manual,
            active_clients=active_clients,
            current_settings=current_settings,
            pending_tasks=PENDING_TASKS,
            scenario_context=SCENARIO_CONTEXT,
            processing_cache=PROCESSING_CACHE,
            post_process_cache=POST_PROCESS_CACHE,
            post_process_cache_order=POST_PROCESS_CACHE_ORDER,
            post_process_cache_max=POST_PROCESS_CACHE_MAX,
            channel_last_post_time=CHANNEL_LAST_POST_TIME,
            recent_generated_messages=RECENT_GENERATED_MESSAGES,
        )
    return _wrapper


SHARED_STATE = _build_shared_state()


def _db_connect():
    """Get a database connection (sync context manager).

    Uses PostgreSQL when DB_URL is set, otherwise falls back to SQLite.
    """
    from db.connection import get_connection
    return get_connection()


def init_database():
    try:
        from db.schema import init_database as _init_schema
        with _db_connect() as conn:
            _init_schema(conn)
    except Exception as e:
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


# All business logic extracted to services/. See MIGRATION_PLAN.md for details.

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
    last_no_active_clients_log_at = None
    try:
        telethon_config = load_config('telethon_credentials')
        api_id, api_hash = int(telethon_config['api_id']), telethon_config['api_hash']
    except Exception as e:
        logger.critical(f"Критическая ошибка конфигурации: {e}")
        return
    while True:
        try:
            new_settings = load_json_data(SETTINGS_FILE)
            current_settings = new_settings if isinstance(new_settings, dict) else {}
            ensure_role_schema(current_settings)
            if ensure_discussion_targets_schema(current_settings):
                try:
                    save_json(SETTINGS_FILE, current_settings)
                except Exception:
                    pass
            migrated_manual = _migrate_legacy_manual_queue_to_db_impl(current_settings, save_data, SETTINGS_FILE)
            if migrated_manual:
                logger.info(f"✅ migrated {migrated_manual} legacy manual_queue tasks to manual_tasks")
            status = current_settings.get('status')
            if status == 'running':
                await manage_clients(api_id, api_hash, shared_state=SHARED_STATE)

                await process_discussion_start_queue(
                    current_settings=current_settings,
                    active_clients=active_clients,
                    discussion_active_tasks=DISCUSSION_ACTIVE_TASKS,
                    discussion_start_cache=DISCUSSION_START_CACHE,
                    discussion_start_cache_order=DISCUSSION_START_CACHE_ORDER,
                    discussion_start_cache_max=DISCUSSION_START_CACHE_MAX,
                    discussion_start_suppress_chat_ids=DISCUSSION_START_SUPPRESS_CHAT_IDS,
                    reply_process_cache=REPLY_PROCESS_CACHE,
                    pending_tasks=PENDING_TASKS,
                    recent_generated_messages=RECENT_GENERATED_MESSAGES,
                    joined_cache=JOINED_CACHE,
                    save_settings_fn=lambda: save_data(SETTINGS_FILE, current_settings),
                )
                await process_discussion_queue(
                    current_settings=current_settings,
                    active_clients=active_clients,
                    discussion_active_tasks=DISCUSSION_ACTIVE_TASKS,
                    discussion_start_cache=DISCUSSION_START_CACHE,
                    discussion_start_cache_order=DISCUSSION_START_CACHE_ORDER,
                    discussion_start_cache_max=DISCUSSION_START_CACHE_MAX,
                    reply_process_cache=REPLY_PROCESS_CACHE,
                    pending_tasks=PENDING_TASKS,
                    discussion_start_suppress_chat_ids=DISCUSSION_START_SUPPRESS_CHAT_IDS,
                    recent_generated_messages=RECENT_GENERATED_MESSAGES,
                    save_settings_fn=lambda: save_data(SETTINGS_FILE, current_settings),
                )
                await process_profile_tasks(api_id, api_hash, current_settings=current_settings, active_clients=active_clients)
                await process_scenarios(active_clients=active_clients, current_settings=current_settings)
                await process_outbound_queue(active_clients=active_clients, current_settings=current_settings)
                await process_manual_tasks(
                    active_clients=active_clients,
                    current_settings=current_settings,
                    joined_cache=JOINED_CACHE,
                    process_new_post_fn=_make_process_new_post_fn(),
                )

                if not active_clients:
                    now = datetime.now(timezone.utc)
                    should_log_no_active_clients = (
                        last_no_active_clients_log_at is None
                        or now - last_no_active_clients_log_at >= NO_ACTIVE_CLIENTS_LOG_INTERVAL
                    )
                    if should_log_no_active_clients:
                        logger.info("Статус: Работает, но нет активных аккаунтов (спят или не добавлены)")
                        last_no_active_clients_log_at = now
                else:
                    last_no_active_clients_log_at = None

                if 'rebrand_task' in current_settings and current_settings['rebrand_task'].get('status') == 'pending':
                    await run_rebrand_logic(api_id, api_hash, current_settings=current_settings, active_clients=active_clients)
            else:
                last_no_active_clients_log_at = None
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
                await process_profile_tasks(api_id, api_hash, current_settings=current_settings, active_clients=active_clients)
            await asyncio.sleep(2)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.critical(f"Критическая ошибка в главном цикле: {e}", exc_info=True)
            await asyncio.sleep(30)


if __name__ == "__main__":
    configure_logging()
    asyncio.run(main())

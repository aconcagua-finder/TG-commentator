import json
import logging
import configparser
import os
import time
import asyncio
import httpx
import random
import locale
import html
import csv
import io
import sqlite3
import pandas as pd
import base64
from openpyxl.utils import get_column_letter
from datetime import datetime, timedelta, timezone

import telegram.error
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
try:
    from telegram import LinkPreviewOptions
except ImportError:  # pragma: no cover
    LinkPreviewOptions = None
from telegram.ext import (Application, CommandHandler, ConversationHandler,
                          MessageHandler, filters, ContextTypes, CallbackQueryHandler)
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, PhoneCodeExpiredError, \
    PasswordHashInvalidError, RPCError, UserDeactivatedBanError
from telethon.tl import types
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest, GetChannelRecommendationsRequest
from telethon.tl.functions.messages import CheckChatInviteRequest

from app_paths import ACCOUNTS_FILE, CONFIG_FILE, DB_FILE, LOGS_FILE, PROXIES_FILE, SETTINGS_FILE, ensure_data_dir
from app_storage import load_json, save_json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Avoid leaking secrets (e.g., Telegram bot token is part of the URL).
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

ITEMS_PER_PAGE = 5


def load_config(section):
    parser = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Файл config.ini не найден: {CONFIG_FILE}")
    parser.read(CONFIG_FILE)
    if section not in parser:
        raise KeyError(f"В config.ini не найдена секция [{section}].")
    return parser[section]


try:
    admin_config = load_config('admin_bot')
    telethon_config = load_config('telethon_credentials')
    if not admin_config.get('owner_id'):
        raise ValueError("Поле 'owner_id' в секции [admin_bot] пустое или отсутствует.")
    try:
        OWNER_ID = int(admin_config['owner_id'])
    except ValueError:
        raise ValueError(
            f"Некорректное значение 'owner_id' в config.ini: '{admin_config['owner_id']}' не является числом.")
    allowed_ids_str = admin_config.get('allowed_ids', str(OWNER_ID))
    if not allowed_ids_str.strip():
        raise ValueError("Поле 'allowed_ids' в секции [admin_bot] пустое.")
    ALLOWED_IDS = []
    for id_ in allowed_ids_str.split(','):
        id_ = id_.strip()
        if id_:
            try:
                ALLOWED_IDS.append(int(id_))
            except ValueError:
                raise ValueError(f"Некорректное значение в 'allowed_ids': '{id_}' не является числом.")
    if not ALLOWED_IDS:
        raise ValueError("Список 'allowed_ids' пуст после обработки (возможно, лишние запятые или пробелы).")
except (FileNotFoundError, KeyError, ValueError) as e:
    logger.critical(f"Критическая ошибка конфигурации: {e}")
    exit()

(
    MAIN_MENU, SETTINGS_MENU, AI_PROVIDER_MENU, GET_API_KEY,
    TARGETS_MENU, ADD_TARGET_CHAT_ID, ADD_TARGET_PROMPT,
    EDIT_TARGET_MENU, EDIT_TARGET_PROMPT, GET_TARGET_TO_DELETE,
    ACCOUNTS_MENU, ADD_ACCOUNT_SESSION, GET_ACCOUNT_TO_DELETE,
    ADD_TARGET_BETWEEN_DELAY, ADD_ACCOUNT_PHONE, GET_AUTH_CODE, GET_2FA_PASSWORD,
    ADD_TARGET_INITIAL_DELAY, EDIT_CHAT_ASSIGN_ACCOUNTS, EDIT_CHAT_AI,
    EDIT_DELAYS_INITIAL, EDIT_DELAYS_BETWEEN, ADD_TARGET_DAILY_LIMIT,
    ADD_TARGET_AI_PROVIDER, STATS_MENU, EDIT_TARGET_DAILY_LIMIT,
    PROMPTS_MENU, GET_ACCOUNT_PROMPT, MY_ACCOUNTS_MENU, MY_TARGETS_MENU,
    REACTION_TARGETS_MENU, ADD_REACTION_TARGET_CHAT_ID, ADD_REACTION_TARGET_REACTIONS,
    ADD_REACTION_TARGET_INITIAL_DELAY, ADD_REACTION_TARGET_BETWEEN_DELAY,
    ADD_REACTION_TARGET_DAILY_LIMIT, ADD_REACTION_TARGET_REACTION_COUNT,
    MY_REACTION_TARGETS_MENU, EDIT_REACTION_TARGET_MENU, GET_REACTION_TARGET_TO_DELETE,
    EDIT_REACTION_TARGET_ASSIGN_ACCOUNTS, EDIT_REACTION_TARGET_REACTIONS_MENU,
    EDIT_REACTION_TARGET_GET_REACTIONS, EDIT_REACTION_TARGET_REACTION_COUNT,
    EDIT_REACTION_DELAYS_INITIAL, EDIT_REACTION_DELAYS_BETWEEN, EDIT_REACTION_TARGET_DAILY_LIMIT,
    CONFIRM_DELETE_ALL_TARGETS, CONFIRM_DELETE_ALL_REACTION_TARGETS,
    ADD_TARGET_MIN_WORDS, ADD_TARGET_MIN_INTERVAL, EDIT_FILTERS_MENU,
    EDIT_FILTER_MIN_WORDS, EDIT_FILTER_MIN_INTERVAL,
    MONITOR_TARGETS_MENU, MY_MONITOR_TARGETS_MENU, ADD_MONITOR_TARGET_CHAT_ID, ADD_MONITOR_TARGET_PROMPT,
    ADD_MONITOR_TARGET_NOTIFICATION_CHAT, ADD_MONITOR_TARGET_DAILY_LIMIT,
    ADD_MONITOR_TARGET_MIN_WORDS, ADD_MONITOR_TARGET_MIN_INTERVAL,
    GET_MONITOR_TARGET_TO_DELETE, CONFIRM_DELETE_ALL_MONITOR_TARGETS,
    EDIT_MONITOR_TARGET_MENU, EDIT_MONITOR_FILTERS_MENU, EDIT_MONITOR_ASSIGN_ACCOUNTS,
    EDIT_MONITOR_NOTIFICATION_CHAT, EDIT_MONITOR_PROMPT, EDIT_MONITOR_LIMIT, EDIT_MONITOR_FILTER_MIN_WORDS,
    EDIT_MONITOR_FILTER_MIN_INTERVAL, EDIT_MONITOR_AI,
    SEARCH_CHANNELS_MENU, GET_SOURCE_CHANNEL, SHOW_FOUND_CHANNELS,
    EDIT_REPLY_SETTINGS_MENU, EDIT_REPLY_CHANCE, EDIT_REPLY_DELAY_MIN, EDIT_REPLY_DELAY_MAX,
    PERSONAS_MENU, ADD_PERSONA_NAME, ADD_PERSONA_PROMPT,
    SELECT_ACCOUNTS_FOR_PERSONA, REBRAND_MENU,
    REBRAND_GET_TOPIC,
    REBRAND_GET_SOURCE, EDIT_ACCOUNT_SLEEP_START, EDIT_ACCOUNT_SLEEP_END,
    EDIT_HUMAN_SETTINGS, EDIT_HUMAN_VALUE, GET_PERSONA_TO_DELETE,
    PROXIES_MENU, ADD_PROXIES_INPUT, SELECT_PROXY_FOR_ACCOUNT,
    GET_PROXY_TO_DELETE, ACCOUNT_PROXY_MENU, EDIT_REACTION_CHANCE, EDIT_REPLY_DEPTH, EDIT_REPLY_INTERVENTION,
    BLACKLIST_MENU, ADD_BLACKLIST_WORD, DELETE_BLACKLIST_WORD, EDIT_HUMAN_PROMPT, EDIT_TAG_REPLY_CHANCE, SCENARIO_MENU, UPLOAD_SCENARIO,
    TRIGGERS_MENU, ADD_TRIGGER_PHRASE, ADD_TRIGGER_RESPONSE, DELETE_TRIGGER, MANUAL_REPLY_SEND,
    START_MANUAL_LINK
) = range(113)



def owner_only(func):
    async def wrapped(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id if update.effective_user else None
        if user_id not in ALLOWED_IDS:
            text = "❌ Доступ запрещен. Только авторизованные пользователи могут использовать этот бот."
            if update.callback_query:
                try:
                    await update.callback_query.answer(text, show_alert=True)
                except Exception:
                    pass
            elif update.effective_message:
                await update.effective_message.reply_text(text)
            return None
        return await func(update, context, *args, **kwargs)

    return wrapped


async def check_proxy_health(proxy_url):
    test_url = "http://ip-api.com/json/"

    try:
        timeout = httpx.Timeout(15.0, connect=10.0)
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
        }

        async with httpx.AsyncClient(proxy=proxy_url, timeout=timeout, headers=headers) as client:
            response = await client.get(test_url, follow_redirects=True)

            if response.status_code == 200:
                data = response.json()
                return {
                    "status": "active",
                    "ip": data.get("query"),
                    "country": data.get("country")
                }
    except Exception as e:
        logging.getLogger(__name__).warning(f"Ошибка проверки прокси {proxy_url}: {e}")
        pass

    return {"status": "dead", "ip": None, "country": None}


def get_data(file_path, default_data=None):
    if default_data is None:
        default_data = {}
        return load_json(file_path, default_data)


def save_data(file_path, data):
    try:
        save_json(file_path, data)
    except Exception as e:
                print(f"Ошибка при сохранении {file_path}: {e}")


NO_LINK_PREVIEW = LinkPreviewOptions(is_disabled=True) if LinkPreviewOptions else None


def no_link_preview_kwargs() -> dict:
    if NO_LINK_PREVIEW is not None:
        return {"link_preview_options": NO_LINK_PREVIEW}
    return {"disable_web_page_preview": True}


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
            conn.commit()
    except sqlite3.Error as e:
        logger.critical(f"Критическая ошибка при инициализации БД: {e}")
        exit()


async def try_edit_message(query, text, reply_markup=None):
    try:
        try:
            await query.answer()
        except Exception:
            pass

        await query.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=reply_markup,
            **no_link_preview_kwargs(),
        )
    except Exception as e:
        if "Message is not modified" not in str(e):
            print(f"Ошибка при редактировании сообщения: {e}")


async def send_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    settings = get_data(SETTINGS_FILE, {})
    accounts = get_data(ACCOUNTS_FILE, [])
    try:
        with open(PROXIES_FILE, 'r') as f:
            proxies_count = len([line for line in f if line.strip()])
    except FileNotFoundError:
        proxies_count = 0

    is_running = settings.get('status') == 'running'
    status_icon = "🟢 Работает" if is_running else "🔴 Остановлен"
    toggle_button_text = "⏹️ Остановить" if is_running else "▶️ Запустить"
    toggle_button_callback = "stop_commentator" if is_running else "start_commentator"

    info_text = (f"👤 Аккаунтов: <b>{len(accounts)}</b>\n"
                 f"🎯 Коммент: <b>{len(settings.get('targets', []))}</b> | 👍 Реакции: <b>{len(settings.get('reaction_targets', []))}</b>\n"
                 f"🎭 Ролей: <b>{len(settings.get('personas', {}))}</b> | 🌐 Прокси: <b>{proxies_count}</b>")

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Запустить по ссылке", callback_data="manual_link_start")],
        [InlineKeyboardButton("🎯 Комментирование", callback_data="targets_menu"),
         InlineKeyboardButton("👍 Реакции", callback_data="reaction_targets_menu")],
        [InlineKeyboardButton("📡 Мониторинг", callback_data="monitor_targets_menu"),
         InlineKeyboardButton("🎭 Роли (Personas)", callback_data="personas_menu_0")],
        [InlineKeyboardButton("👤 Аккаунты", callback_data="accounts_menu"),
         InlineKeyboardButton("🌐 Прокси", callback_data="proxies_menu")],
        [InlineKeyboardButton("🎭 Ребрендинг", callback_data="rebrand_menu"),
         InlineKeyboardButton("⚙️ AI", callback_data="settings_menu")],
        [InlineKeyboardButton("📊 Статистика", callback_data="stats_menu")],
        [InlineKeyboardButton(toggle_button_text, callback_data=toggle_button_callback)],
    ])

    message_text = f"<b>--- AI-Центр ---</b>\n<b>Статус:</b> {status_icon}\n{info_text}\n\nВыберите действие:"

    if update.callback_query:
         await try_edit_message(update.callback_query, message_text, keyboard)
    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=message_text,
            reply_markup=keyboard,
            parse_mode='HTML'
        )
    return MAIN_MENU


@owner_only
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    await send_main_menu(update, context)
    return MAIN_MENU


@owner_only
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if 'telethon_client' in context.user_data:
        client = context.user_data['telethon_client']
        if client.is_connected():
            await client.disconnect()
    await update.message.reply_text('Действие отменено.')
    return await start(update, context)


async def start_commentator(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    settings = get_data(SETTINGS_FILE, {})
    settings['status'] = 'running'
    save_data(SETTINGS_FILE, settings)
    await query.answer("✅ Комментатор запущен. Рабочий скрипт подхватит команду в течение 5 секунд.", show_alert=True)
    await start(update, context)
    return MAIN_MENU


async def stop_commentator(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    settings = get_data(SETTINGS_FILE, {})
    settings['status'] = 'stopped'
    save_data(SETTINGS_FILE, settings)
    await query.answer("✅ Комментатор остановлен. Рабочий скрипт подхватит команду в течение 5 секунд.",
                       show_alert=True)
    await start(update, context)
    return MAIN_MENU


def get_paginated_items(items, page, items_per_page=ITEMS_PER_PAGE):
    start_idx = page * items_per_page
    end_idx = start_idx + items_per_page
    return items[start_idx:end_idx], len(items)


@owner_only
async def accounts_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    text = "<b>👤 Управление аккаунтами</b>\n\n<code>api_id</code> и <code>api_hash</code> берутся из <code>config.ini</code> для всех аккаунтов."
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Мои аккаунты", callback_data="my_accounts_list")],
        [InlineKeyboardButton("🔍 Проверить аккаунты", callback_data="check_accounts")],
        [InlineKeyboardButton("➕ Добавить аккаунт", callback_data="add_acc_start")],
        [InlineKeyboardButton("❌ Удалить аккаунт", callback_data="del_acc_start")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
    ])
    await try_edit_message(query, text, keyboard)
    return ACCOUNTS_MENU


@owner_only
async def check_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "<b>⏳ Начинаю проверку всех аккаунтов...</b>")
    accounts = get_data(ACCOUNTS_FILE, [])
    if not accounts:
        await try_edit_message(query, "Нет аккаунтов.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_accounts")]]))
        return ACCOUNTS_MENU
    api_id = int(telethon_config['api_id'])
    api_hash = telethon_config['api_hash']
    report_lines = ["<b>--- Отчет о проверке аккаунтов ---</b>"]
    needs_saving = False
    for i, acc in enumerate(accounts):
        session_name = acc.get('session_name', 'N/A')
        session_string = acc.get('session_string')
        if acc.get('status') == 'banned':
            report_lines.append(f"🚫 <b>{html.escape(session_name)}:</b> ЗАБАНЕН")
            continue
        client = TelegramClient(StringSession(session_string), api_id, api_hash)
        status = ""
        try:
            await client.connect()
            if not await client.is_user_authorized():
                status = "🔴 <b>НЕ АВТОРИЗОВАН</b>"
                accounts[i]['status'] = 'unauthorized'
                needs_saving = True
            else:
                me = await client.get_me()
                status = f"✅ <b>OK</b> (ID: <code>{me.id}</code>)"
                accounts[i].update({'user_id': me.id, 'first_name': me.first_name, 'last_name': me.last_name or "", 'username': me.username or "", 'status': 'active'})
                needs_saving = True
        except UserDeactivatedBanError:
            status = "🚫 <b>ЗАБЛОКИРОВАН</b>"
            accounts[i]['status'] = 'banned'
            needs_saving = True
        except Exception as e:
            status = f"❌ <b>ОШИБКА</b> ({type(e).__name__})"
        finally:
            if client.is_connected():
                await client.disconnect()
        report_lines.append(f"<b>{html.escape(session_name)}:</b> {status}")
    if needs_saving:
        save_data(ACCOUNTS_FILE, accounts)
    await try_edit_message(query, "\n".join(report_lines), reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_accounts")]]))
    return MY_ACCOUNTS_MENU


@owner_only
async def monitor_targets_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    text = "<b>📡 Управление каналами для мониторинга</b>\n\nБот будет отслеживать посты в этих каналах и уведомлять вас, если пост соответствует заданному промпту."
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Мои каналы", callback_data="my_monitor_targets_list_0")],
        [InlineKeyboardButton("➕ Добавить канал", callback_data="add_monitor_target_start")],
        [InlineKeyboardButton("❌ Удалить канал", callback_data="delete_monitor_target_start")],
        [InlineKeyboardButton("🗑️ Удалить все", callback_data="delete_all_monitor_targets_confirm")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
    ])

    await try_edit_message(query, text, keyboard)
    return MONITOR_TARGETS_MENU


@owner_only
async def show_my_monitor_targets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = 0
    if query.data and query.data.startswith("my_monitor_targets_list_"):
        page = int(query.data.split('_')[-1])
    settings = get_data(SETTINGS_FILE, {})
    targets = settings.get('monitor_targets', [])

    paginated_targets, total_items = get_paginated_items(targets, page)

    text = f"<b>📡 Список каналов для мониторинга (Стр. {page + 1})</b>"
    if not paginated_targets:
        text += "\n\n<i>Пока не добавлено ни одного канала.</i>"

    keyboard_buttons = []
    start_index = page * ITEMS_PER_PAGE
    for i, target in enumerate(paginated_targets, start=start_index):
        keyboard_buttons.append([InlineKeyboardButton(f"⚙️ {html.escape(target.get('chat_name', target['chat_id']))}",
                                                      callback_data=f"edit_monitor_target_{i}")])

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"my_monitor_targets_list_{page - 1}"))
    if (page + 1) * ITEMS_PER_PAGE < total_items:
        nav_buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"my_monitor_targets_list_{page + 1}"))
    if nav_buttons:
        keyboard_buttons.append(nav_buttons)

    keyboard_buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_monitor_targets_menu")])
    reply_markup = InlineKeyboardMarkup(keyboard_buttons)
    await try_edit_message(query, text, reply_markup)
    return MY_MONITOR_TARGETS_MENU

@owner_only
async def add_monitor_target_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['new_monitor_target'] = {}
    await try_edit_message(query, "Введите ID или @username канала для мониторинга:", None)
    return ADD_MONITOR_TARGET_CHAT_ID

@owner_only
async def add_monitor_target_get_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_input = update.message.text.strip()
    api_id, api_hash = int(telethon_config['api_id']), telethon_config['api_hash']
    accounts = get_data(ACCOUNTS_FILE, [])

    if not accounts:
        await update.message.reply_text("❌ Нет аккаунтов для проверки ID. Добавьте хотя бы один.")
        return await start(update, context)

    client = TelegramClient(StringSession(accounts[0]['session_string']), api_id, api_hash)
    await update.message.reply_text("⏳ Проверяю канал...")

    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise Exception("Первый аккаунт в списке не авторизован.")

        entity = await client.get_entity(chat_input)
        await client(JoinChannelRequest(entity))

        context.user_data['new_monitor_target'].update({
            'chat_id': f"-100{entity.id}",
            'chat_username': getattr(entity, 'username', None),
            'chat_name': entity.title
        })

        await update.message.reply_text("Теперь введите ID чата для получения уведомлений (например, ваш ID или ID группы).")
        return ADD_MONITOR_TARGET_NOTIFICATION_CHAT

    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return await start(update, context)
    finally:
        if client.is_connected(): await client.disconnect()

@owner_only
async def add_monitor_target_get_notification_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data['new_monitor_target']['notification_chat_id'] = int(update.message.text.strip())
        await update.message.reply_text("Введите промпт для AI. Бот будет искать посты, соответствующие этому описанию.\n\nНапример: 'ищем посты о продаже аккаунтов'")
        return ADD_MONITOR_TARGET_PROMPT
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести числовой ID чата.")
        return ADD_MONITOR_TARGET_NOTIFICATION_CHAT

@owner_only
async def add_monitor_target_get_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['new_monitor_target']['prompt'] = update.message.text.strip()
    await update.message.reply_text("Лимит найденных постов в сутки?\n(Введите 0 для неограниченного количества)")
    return ADD_MONITOR_TARGET_DAILY_LIMIT

@owner_only
async def add_monitor_target_get_daily_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data['new_monitor_target']['daily_limit'] = int(update.message.text.strip())
        await update.message.reply_text("Фильтр: минимальное кол-во слов в посте?\n(Введите 0, чтобы не использовать)")
        return ADD_MONITOR_TARGET_MIN_WORDS
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return ADD_MONITOR_TARGET_DAILY_LIMIT

@owner_only
async def add_monitor_target_get_min_words(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data['new_monitor_target']['min_word_count'] = int(update.message.text.strip())
        await update.message.reply_text("Фильтр: минимальная пауза между постами (в минутах)?\n(Введите 0, чтобы не использовать)")
        return ADD_MONITOR_TARGET_MIN_INTERVAL
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return ADD_MONITOR_TARGET_MIN_WORDS

@owner_only
async def add_monitor_target_save_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data['new_monitor_target']['min_post_interval_mins'] = int(update.message.text.strip())
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return ADD_MONITOR_TARGET_MIN_INTERVAL

    settings = get_data(SETTINGS_FILE, {})
    new_target = context.user_data['new_monitor_target']
    new_target['date_added'] = datetime.now(timezone.utc).isoformat()
    new_target['assigned_accounts'] = []
    new_target['ai_provider'] = 'default'

    targets = settings.setdefault('monitor_targets', [])
    targets.append(new_target)
    save_data(SETTINGS_FILE, settings)

    await update.message.reply_text(f"✅ Канал для мониторинга '{html.escape(new_target['chat_name'])}' добавлен.")

    context.user_data.clear()
    return await send_main_menu(update, context)


@owner_only
async def edit_monitor_target_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['edit_monitor_target_index'] = int(update.callback_query.data.split('_')[-1])
    return await edit_monitor_target_menu_logic(update, context)


@owner_only
async def edit_monitor_target_menu_logic(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                         from_callback: bool = True):
    if from_callback and update.callback_query:
        await update.callback_query.answer()
    target_index = context.user_data.get('edit_monitor_target_index')
    settings = get_data(SETTINGS_FILE, {})
    if target_index is None or target_index >= len(settings.get('monitor_targets', [])): return await start(update,
                                                                                                            context)

    target = settings.get('monitor_targets', [])[target_index]
    provider = target.get('ai_provider', 'По умолч.').upper()
    assigned_accounts = target.get('assigned_accounts', [])
    min_words = target.get('min_word_count', 0)
    min_interval = target.get('min_post_interval_mins', 0)
    prompt_text = target.get('prompt', 'Не задан')

    text = (f"<b>⚙️ Настройки мониторинга: {html.escape(target.get('chat_name'))}</b>\n\n"
            f"ID канала: <code>{target.get('chat_id')}</code>\n"
            f"Чат для уведомлений: <code>{target.get('notification_chat_id')}</code>\n\n"
            f"<b>Промпт:</b> <i>{html.escape(prompt_text)}</i>")

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"👤 Аккаунты: {len(assigned_accounts) if assigned_accounts else 'Все'}",
                              callback_data="edit_monitor_assign_accounts")],
        [InlineKeyboardButton(f"🤖 AI чата: {provider}", callback_data="edit_monitor_ai")],
        [InlineKeyboardButton("✏️ Промпт", callback_data="edit_monitor_prompt")],
        [InlineKeyboardButton(f"📊 Лимит в сутки: {target.get('daily_limit', 'N/A')}",
                              callback_data="edit_monitor_limit")],
        [InlineKeyboardButton(f"⚙️ Фильтры: {min_words} сл. / {min_interval} мин.",
                              callback_data="edit_monitor_filters")],
        [InlineKeyboardButton(f"🔔 Чат уведомлений", callback_data="edit_monitor_notification_chat")],
        [InlineKeyboardButton("⬅️ Назад к списку", callback_data="my_monitor_targets_list_0")]
    ])
    if from_callback and update.callback_query:
        await try_edit_message(update.callback_query, text, keyboard)
    else:
        await context.bot.send_message(update.effective_chat.id, text, reply_markup=keyboard, parse_mode='HTML')
    return EDIT_MONITOR_TARGET_MENU

@owner_only
async def assign_monitor_accounts_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    target_index = context.user_data['edit_monitor_target_index']
    settings = get_data(SETTINGS_FILE, {})
    accounts = get_data(ACCOUNTS_FILE, [])
    target = settings['monitor_targets'][target_index]
    assigned_accounts = set(target.get('assigned_accounts', []))
    if not accounts:
        await query.answer("Сначала добавьте аккаунты!", show_alert=True)
        return EDIT_MONITOR_TARGET_MENU
    keyboard = [
        [InlineKeyboardButton(
            f"{'✅' if acc['session_name'] in assigned_accounts else '➖'} {html.escape(acc['session_name'])}",
            callback_data=f"toggle_monitor_acc_{acc['session_name']}")] for acc in accounts]
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_edit_monitor_menu")])
    await try_edit_message(query, f"Отметьте аккаунты для <b>{html.escape(target['chat_name'])}</b>:", InlineKeyboardMarkup(keyboard))
    return EDIT_MONITOR_ASSIGN_ACCOUNTS

@owner_only
async def toggle_monitor_account_assignment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    session_name = query.data.split('_')[-1]
    target_index = context.user_data['edit_monitor_target_index']
    settings = get_data(SETTINGS_FILE, {})
    assigned_accounts = set(settings['monitor_targets'][target_index].get('assigned_accounts', []))
    if session_name in assigned_accounts:
        assigned_accounts.remove(session_name)
    else:
        assigned_accounts.add(session_name)
    settings['monitor_targets'][target_index]['assigned_accounts'] = list(assigned_accounts)
    save_data(SETTINGS_FILE, settings)
    return await assign_monitor_accounts_menu(update, context)

@owner_only
async def set_monitor_ai_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    target = get_data(SETTINGS_FILE, {})['monitor_targets'][context.user_data['edit_monitor_target_index']]
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🤖 Google Gemini", callback_data="save_monitor_ai_gemini")],
        [InlineKeyboardButton("🧠 OpenAI GPT", callback_data="save_monitor_ai_openai")],
        [InlineKeyboardButton("🧩 OpenRouter", callback_data="save_monitor_ai_openrouter")],
        [InlineKeyboardButton("🌐 Deepseek", callback_data="save_monitor_ai_deepseek")],
        [InlineKeyboardButton("🌐 Использовать глобальный", callback_data="save_monitor_ai_default")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_edit_monitor_menu")]
    ])
    await try_edit_message(query, f"Выберите AI для <b>{html.escape(target['chat_name'])}</b>:", keyboard)
    return EDIT_MONITOR_AI

@owner_only
async def save_monitor_ai(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    provider = query.data.split('_')[-1]
    target_index = context.user_data['edit_monitor_target_index']
    settings = get_data(SETTINGS_FILE, {})
    if provider == 'default':
        if 'ai_provider' in settings['monitor_targets'][target_index]:
            del settings['monitor_targets'][target_index]['ai_provider']
    else:
        settings['monitor_targets'][target_index]['ai_provider'] = provider
    save_data(SETTINGS_FILE, settings)
    await query.answer("AI для мониторинга изменен", show_alert=True)
    return await edit_monitor_target_menu_logic(update, context)

@owner_only
async def edit_monitor_prompt_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    target_index = context.user_data['edit_monitor_target_index']
    target = get_data(SETTINGS_FILE)['monitor_targets'][target_index]
    await try_edit_message(query,
                           f"<b>Текущий промпт:</b>\n<code>{html.escape(target.get('prompt', ''))}</code>\n\nВведите новый текст промпта.",
                           None)
    return EDIT_MONITOR_PROMPT


@owner_only
async def save_monitor_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target_index = context.user_data['edit_monitor_target_index']
    settings = get_data(SETTINGS_FILE)
    settings['monitor_targets'][target_index]['prompt'] = update.message.text.strip()
    save_data(SETTINGS_FILE, settings)
    await update.message.reply_text("✅ Промпт обновлен.")
    return await edit_monitor_target_menu_logic(update, context, from_callback=False)


@owner_only
async def edit_monitor_limit_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "Введите новый суточный лимит найденных постов:", None)
    return EDIT_MONITOR_LIMIT


@owner_only
async def save_monitor_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        limit = int(update.message.text)
        target_index = context.user_data['edit_monitor_target_index']
        settings = get_data(SETTINGS_FILE)
        settings['monitor_targets'][target_index]['daily_limit'] = limit
        save_data(SETTINGS_FILE, settings)
        await update.message.reply_text("✅ Лимит обновлен.")
        return await edit_monitor_target_menu_logic(update, context, from_callback=False)
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return EDIT_MONITOR_LIMIT


@owner_only
async def edit_monitor_notification_chat_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "Введите новый ID чата для уведомлений:", None)
    return EDIT_MONITOR_NOTIFICATION_CHAT


@owner_only
async def save_monitor_notification_chat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        chat_id = int(update.message.text)
        target_index = context.user_data['edit_monitor_target_index']
        settings = get_data(SETTINGS_FILE)
        settings['monitor_targets'][target_index]['notification_chat_id'] = chat_id
        save_data(SETTINGS_FILE, settings)
        await update.message.reply_text("✅ Чат для уведомлений обновлен.")
        return await edit_monitor_target_menu_logic(update, context, from_callback=False)
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести числовой ID.")
        return EDIT_MONITOR_NOTIFICATION_CHAT


@owner_only
async def edit_monitor_filters_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    target_index = context.user_data['edit_monitor_target_index']
    target = get_data(SETTINGS_FILE)['monitor_targets'][target_index]
    text = f"<b>⚙️ Настройка фильтров для '{html.escape(target['chat_name'])}'</b>\n\n" \
           f" - Мин. слов: <b>{target.get('min_word_count', 0)}</b>\n" \
           f" - Мин. интервал: <b>{target.get('min_post_interval_mins', 0)}</b> мин"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("💬 Изменить мин. кол-во слов", callback_data="edit_monitor_filter_words")],
        [InlineKeyboardButton("⏱ Изменить мин. интервал", callback_data="edit_monitor_filter_interval")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_edit_monitor_menu")]
    ])
    await try_edit_message(query, text, keyboard)
    return EDIT_MONITOR_FILTERS_MENU


@owner_only
async def edit_monitor_filter_min_words_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "Введите новое минимальное кол-во слов (0 - отключить):")
    return EDIT_MONITOR_FILTER_MIN_WORDS


@owner_only
async def save_monitor_filter_min_words(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        words = int(update.message.text)
        target_index = context.user_data['edit_monitor_target_index']
        settings = get_data(SETTINGS_FILE)
        settings['monitor_targets'][target_index]['min_word_count'] = words
        save_data(SETTINGS_FILE, settings)
        await update.message.reply_text(f"✅ Минимальное кол-во слов установлено: {words}")
        return await edit_monitor_target_menu_logic(update, context, from_callback=False)
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return EDIT_MONITOR_FILTER_MIN_WORDS


@owner_only
async def edit_monitor_filter_min_interval_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "Введите новый минимальный интервал (в минутах, 0 - отключить):")
    return EDIT_MONITOR_FILTER_MIN_INTERVAL


@owner_only
async def save_monitor_filter_min_interval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        interval = int(update.message.text)
        target_index = context.user_data['edit_monitor_target_index']
        settings = get_data(SETTINGS_FILE)
        settings['monitor_targets'][target_index]['min_post_interval_mins'] = interval
        save_data(SETTINGS_FILE, settings)
        await update.message.reply_text(f"✅ Минимальный интервал установлен: {interval} мин.")
        return await edit_monitor_target_menu_logic(update, context, from_callback=False)
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return EDIT_MONITOR_FILTER_MIN_INTERVAL


@owner_only
async def delete_monitor_target_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = 0
    if query.data and query.data.startswith("delete_monitor_target_start_"):
        page = int(query.data.split('_')[-1])
    settings = get_data(SETTINGS_FILE, {})
    targets = settings.get('monitor_targets', [])
    if not targets:
        await try_edit_message(query, "Нет каналов для удаления.", InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_monitor_targets_menu")]]))
        return MONITOR_TARGETS_MENU
    indexed_targets = list(enumerate(targets))
    paginated_targets, total_items = get_paginated_items(indexed_targets, page)
    text = f"<b>❌ Выберите канал для удаления (Стр. {page + 1})</b>"
    keyboard_buttons = []
    for original_index, target_data in paginated_targets:
        keyboard_buttons.append([InlineKeyboardButton(f"❌ {html.escape(target_data.get('chat_name', target_data['chat_id']))}",
                                                      callback_data=f"del_monitor_target_{original_index}")])
    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"delete_monitor_target_start_{page - 1}"))
    if (page + 1) * ITEMS_PER_PAGE < total_items:
        nav_buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"delete_monitor_target_start_{page + 1}"))
    if nav_buttons:
        keyboard_buttons.append(nav_buttons)
    keyboard_buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_monitor_targets_menu")])
    await try_edit_message(query, text, InlineKeyboardMarkup(keyboard_buttons))
    return GET_MONITOR_TARGET_TO_DELETE


@owner_only
async def delete_monitor_target_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    target_index = int(query.data.split('_')[-1])
    settings = get_data(SETTINGS_FILE, {})
    if 0 <= target_index < len(settings.get('monitor_targets', [])):
        removed = settings['monitor_targets'].pop(target_index)
        save_data(SETTINGS_FILE, settings)
        await query.answer(f"✅ Канал мониторинга '{removed.get('chat_name')}' удален.", show_alert=True)
    else:
        await query.answer("Ошибка: неверный индекс.", show_alert=True)
    return await start(update, context)


@owner_only
async def confirm_delete_all_monitor_targets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("ДА, УДАЛИТЬ ВСЕ", callback_data="delete_all_monitor_targets_confirmed")],
        [InlineKeyboardButton("⬅️ Нет, назад", callback_data="back_to_monitor_targets_menu")]
    ])
    await try_edit_message(query, "<b>Вы уверены, что хотите удалить ВСЕ каналы мониторинга?</b>", keyboard)
    return CONFIRM_DELETE_ALL_MONITOR_TARGETS


@owner_only
async def do_delete_all_monitor_targets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    settings = get_data(SETTINGS_FILE, {})
    settings['monitor_targets'] = []
    save_data(SETTINGS_FILE, settings)
    await query.answer("✅ Все каналы мониторинга удалены.", show_alert=True)
    return await monitor_targets_menu(update, context)


@owner_only
async def show_my_accounts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    accounts = get_data(ACCOUNTS_FILE, [])

    if not accounts:
        keyboard = [[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_accounts")]]
        await try_edit_message(query, "Нет добавленных аккаунтов.", InlineKeyboardMarkup(keyboard))
        return MY_ACCOUNTS_MENU

    text = "<b>📋 Выберите аккаунт для настройки:</b>"
    keyboard = []
    for i, acc in enumerate(accounts):
        session_name = acc.get('session_name', 'N/A')
        keyboard.append(
            [InlineKeyboardButton(f"👤 {html.escape(session_name)}", callback_data=f"edit_acc_settings_{i}")])

    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_accounts")])
    await try_edit_message(query, text, InlineKeyboardMarkup(keyboard))
    return MY_ACCOUNTS_MENU


@owner_only
async def edit_account_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
        if "edit_acc_settings_" in query.data:
            acc_index = int(query.data.split('_')[-1])
            context.user_data['edit_acc_index'] = acc_index

    acc_index = context.user_data.get('edit_acc_index')
    if acc_index is None:
        return await start(update, context)

    accounts = get_data(ACCOUNTS_FILE, [])
    if acc_index >= len(accounts):
        return await start(update, context)

    acc = accounts[acc_index]
    sleep_settings = acc.get('sleep_settings', {})
    start_h = sleep_settings.get('start_hour', 8)
    end_h = sleep_settings.get('end_hour', 23)
    current_proxy = acc.get('proxy_url', 'Не задан')

    text = (f"<b>⚙️ Настройки аккаунта: {html.escape(acc['session_name'])}</b>\n\n"
            f"Текущий режим работы: с <code>{start_h}:00</code> до <code>{end_h}:00</code>\n"
            f"Прокси: <code>{current_proxy}</code>")

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🌅 Время пробуждения", callback_data="set_acc_sleep_start"),
         InlineKeyboardButton("🌙 Время засыпания", callback_data="set_acc_sleep_end")],
        [InlineKeyboardButton("🌐 Изменить прокси", callback_data="set_acc_proxy_start")],
        [InlineKeyboardButton("⬅️ Назад к списку", callback_data="my_accounts_list")]
    ])

    if query:
        await try_edit_message(query, text, keyboard)
    else:
        await context.bot.send_message(update.effective_chat.id, text, reply_markup=keyboard, parse_mode='HTML')
    return MY_ACCOUNTS_MENU


@owner_only
async def set_account_sleep_hour_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    mode = query.data.split('_')[-1]
    context.user_data['edit_sleep_mode'] = mode

    text = "Введите час (число от 0 до 23) для "
    text += "<b>пробуждения</b> (когда бот начнет работу):" if mode == 'start' else "<b>засыпания</b> (когда бот уйдет в сон):"

    await try_edit_message(query, text, None)
    return EDIT_ACCOUNT_SLEEP_START if mode == 'start' else EDIT_ACCOUNT_SLEEP_END


@owner_only
async def save_account_sleep_hour(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        hour = int(update.message.text.strip())
        if not (0 <= hour <= 23):
            raise ValueError
    except (ValueError, TypeError):
        await update.message.reply_text("❌ Ошибка. Введите целое число от 0 до 23:")
        return None
    acc_index = context.user_data.get('edit_acc_index')
    mode = context.user_data.get('edit_sleep_mode')
    accounts = get_data(ACCOUNTS_FILE, [])
    if acc_index is None or acc_index >= len(accounts):
        return await start(update, context)
    if 'sleep_settings' not in accounts[acc_index]:
        accounts[acc_index]['sleep_settings'] = {"start_hour": 8, "end_hour": 23}
    key = "start_hour" if mode == "start" else "end_hour"
    accounts[acc_index]['sleep_settings'][key] = hour
    save_data(ACCOUNTS_FILE, accounts)
    await update.message.reply_text(f"✅ Сохранено: {hour}:00")
    return await edit_account_settings_menu(update, context)


@owner_only
async def add_account_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query,
                           "Придумайте имя для сессии (латиницей, без пробелов).\n\nДля отмены в любой момент отправьте /start или /cancel",
                           None)
    return ADD_ACCOUNT_SESSION


@owner_only
async def add_account_get_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    session_name = update.message.text.strip()
    accounts = get_data(ACCOUNTS_FILE, [])
    if any(acc['session_name'] == session_name for acc in accounts):
        await update.message.reply_text("Аккаунт с таким именем уже существует. Придумайте другое.")
        return ADD_ACCOUNT_SESSION
    context.user_data['new_account'] = {'session_name': session_name}
    await update.message.reply_text(
        "Введите номер телефона аккаунта в международном формате (например, +79123456789).")
    return ADD_ACCOUNT_PHONE


@owner_only
async def add_account_get_phone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    phone = update.message.text.strip()
    context.user_data['new_account']['phone'] = phone
    await update.message.reply_text("⏳ Пытаюсь подключиться к Telegram... Пожалуйста, подождите.")
    api_id = int(telethon_config['api_id'])
    api_hash = telethon_config['api_hash']
    proxy = None
    try:
        with open(PROXIES_FILE, 'r') as f:
            proxies = [line.strip() for line in f if line.strip()]
            if proxies:
                proxy_str = random.choice(proxies)
                logger.info(f"Попытка использовать прокси: {proxy_str}")
                try:
                    if '://' in proxy_str:
                        proxy_type_str, rest = proxy_str.split('://', 1)
                        user_pass, host_port = rest.rsplit('@', 1)
                        user, password = user_pass.split(':')
                        host, port = host_port.split(':')
                        proxy = (proxy_type_str, host, int(port), True, user, password)
                    else:
                        parts = proxy_str.split(':')
                        host, port, user, password = parts[0], parts[1], parts[2], parts[3]
                        proxy = ('socks5', host, int(port), True, user, password)
                except Exception as e:
                    logger.error(f"Неверный формат прокси: {proxy_str}, ошибка: {e}")
    except FileNotFoundError:
        logger.info("Файл proxies.txt не найден, продолжаем без прокси.")

    try:
        client = TelegramClient(StringSession(), api_id, api_hash, proxy=proxy)
        await client.connect()
        context.user_data['telethon_client'] = client
        sent_code = await client.send_code_request(phone)
        context.user_data['phone_code_hash'] = sent_code.phone_code_hash
        await update.message.reply_text("Я отправил код в ваш аккаунт Telegram. Введите его.")
        return GET_AUTH_CODE
    except RPCError as e:
        if 'client' in context.user_data and context.user_data['telethon_client'].is_connected():
            await context.user_data['telethon_client'].disconnect()
        logger.error(f"RPC ошибка при запросе кода: {e}")
        await update.message.reply_text(f"❌ Ошибка Telegram API: {e}\n\nВозврат в главное меню.")
        return await start(update, context)
    except Exception as e:
        if 'client' in context.user_data and context.user_data['telethon_client'].is_connected():
            await context.user_data['telethon_client'].disconnect()
        logger.error(f"Ошибка при подключении: {e}")
        await update.message.reply_text(f"❌ Произошла ошибка: {e}\n\nВозврат в главное меню.")
        return await start(update, context)


@owner_only
async def get_auth_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    code = update.message.text.strip()
    client = context.user_data['telethon_client']
    phone = context.user_data['new_account']['phone']
    phone_code_hash = context.user_data['phone_code_hash']
    try:
        await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
        return await finalize_account_addition(update, context)
    except (PhoneCodeInvalidError, PhoneCodeExpiredError):
        logger.warning(f"Неверный или истекший код для {phone}")
        await update.message.reply_text("❌ Неверный или истекший код. Попробуйте снова.")
        return GET_AUTH_CODE
    except SessionPasswordNeededError:
        await update.message.reply_text("Этот аккаунт защищен 2FA. Введите ваш пароль.")
        return GET_2FA_PASSWORD
    except Exception as e:
        if client.is_connected(): await client.disconnect()
        logger.error(f"Ошибка при входе: {e}")
        await update.message.reply_text(f"❌ Произошла ошибка: {e}\n\nВозврат в главное меню.")
        return await start(update, context)


@owner_only
async def get_2fa_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    password = update.message.text.strip()
    client = context.user_data['telethon_client']
    try:
        await client.sign_in(password=password)
        return await finalize_account_addition(update, context)
    except PasswordHashInvalidError:
        await update.message.reply_text("❌ Неверный пароль. Попробуйте снова.")
        return GET_2FA_PASSWORD
    except Exception as e:
        if client.is_connected(): await client.disconnect()
        logger.error(f"Ошибка при вводе 2FA пароля: {e}")
        await update.message.reply_text(f"❌ Произошла ошибка: {e}\n\nВозврат в главное меню.")
        return await start(update, context)


async def finalize_account_addition(update: Update, context: ContextTypes.DEFAULT_TYPE):
    client = context.user_data['telethon_client']
    me = await client.get_me()
    session_string = client.session.save()
    await client.disconnect()
    acc_data = context.user_data['new_account']
    final_account_data = {
        "session_name": acc_data['session_name'],
        "session_string": session_string,
        "user_id": me.id,
        "first_name": me.first_name,
        "last_name": me.last_name or "",
        "username": me.username or ""
    }
    accounts = get_data(ACCOUNTS_FILE, [])
    accounts.append(final_account_data)
    save_data(ACCOUNTS_FILE, accounts)
    try:
        await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=update.message.message_id - 1)
        await update.message.delete()
    except Exception as e:
        logger.warning(f"Не удалось удалить сообщения при добавлении аккаунта: {e}")
    await context.bot.send_message(update.effective_chat.id,
                                   f"✅ Аккаунт '{acc_data['session_name']}' ({me.first_name}) успешно добавлен!")
    context.user_data.clear()
    return await send_main_menu(update, context)


@owner_only
async def delete_account_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    accounts = get_data(ACCOUNTS_FILE, [])
    if not accounts:
        await try_edit_message(query, "Нет аккаунтов для удаления.", InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_accounts")]]))
        return ACCOUNTS_MENU
    keyboard = [[InlineKeyboardButton(f"❌ {acc['session_name']}", callback_data=f"del_acc_{i}")] for i, acc in
                enumerate(accounts)]
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_accounts")])
    await try_edit_message(query, "Выберите аккаунт для удаления:", InlineKeyboardMarkup(keyboard))
    return GET_ACCOUNT_TO_DELETE


@owner_only
async def delete_account_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    acc_index = int(query.data.split('_')[-1])
    accounts = get_data(ACCOUNTS_FILE, [])

    removed_name = "Неизвестный"
    if 0 <= acc_index < len(accounts):
        removed = accounts.pop(acc_index)
        removed_name = removed.get('session_name', 'N/A')
        save_data(ACCOUNTS_FILE, accounts)
        await query.answer(f"✅ Аккаунт '{removed_name}' удален.", show_alert=True)
    else:
        await query.answer("❌ Ошибка: неверный индекс аккаунта.", show_alert=True)

    return await start(update, context)


@owner_only
async def settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()

    settings = get_data(SETTINGS_FILE, {})
    provider = settings.get('ai_provider', 'Не задан')

    text = (f"<b>⚙️ Глобальные настройки AI</b>\n\n"
            f"🔄 Провайдер: <b>{provider.upper()}</b>\n"
            f"🎭 Очеловечивание: <b>Настроено</b>\n"
            f"🚫 Чёрный список: <b>Настроено</b>")

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Выбрать провайдера", callback_data="change_provider")],
        [InlineKeyboardButton("🔑 Указать API ключ", callback_data="update_api_key")],
        [InlineKeyboardButton("🎭 Очеловечивание", callback_data="human_settings_menu")],
        [InlineKeyboardButton("🚫 Чёрный список", callback_data="blacklist_menu")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
    ])

    if query:
        await try_edit_message(query, text, keyboard)
    else:
        await context.bot.send_message(update.effective_chat.id, text, reply_markup=keyboard, parse_mode='HTML')

    return SETTINGS_MENU


@owner_only
async def humanization_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()

    settings = get_data(SETTINGS_FILE, {}).get('humanization', {})

    typo = settings.get('typo_chance', 0)
    lower = settings.get('lowercase_chance', 80)
    split = settings.get('split_chance', 60)
    comma = settings.get('comma_skip_chance', 30)
    rep_penalty = settings.get('repetition_penalty', 0)

    temp_raw = settings.get('temperature')
    temp_display = f"{temp_raw}" if temp_raw is not None else "Auto"

    max_tokens = settings.get('max_tokens', 60)
    max_words = settings.get('max_words', 20)

    text = (f"<b>🎭 Настройки имитации человека</b>\n\n"
            f"🌡 Температура (Креатив): <b>{temp_display}</b>\n"
            f"🚫 Штраф за повторы: <b>{rep_penalty}</b> (0-100)\n"
            f"⌨️ Шанс опечатки: <b>{typo}%</b>\n"
            f"abc Буквы (lowercase): <b>{lower}%</b>\n"
            f"✂️ Разбив сообщения: <b>{split}%</b>\n"
            f"📉 Пропуск запятых: <b>{comma}%</b>\n"
            f"📏 Макс. слов (в ответах): <b>{max_words}</b>\n"
            f"🧱 Max Tokens (лимит AI): <b>{max_tokens}</b>")

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Промпт правил", callback_data="set_human_prompt")],
        [InlineKeyboardButton("🌡 Температура", callback_data="set_human_temp"),
         InlineKeyboardButton("🚫 Штраф повторов", callback_data="set_human_rep_penalty")],
        [InlineKeyboardButton("⌨️ Опечатки", callback_data="set_human_typo"),
         InlineKeyboardButton("abc Буквы", callback_data="set_human_lower")],
        [InlineKeyboardButton("✂️ Разбив", callback_data="set_human_split"),
         InlineKeyboardButton("📉 Запятые", callback_data="set_human_comma")],
        [InlineKeyboardButton("📏 Макс. слов", callback_data="set_human_words"),
         InlineKeyboardButton("🧱 Max Tokens", callback_data="set_human_tokens")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_settings")]
    ])

    if query:
        await try_edit_message(query, text, keyboard)
    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=text,
            reply_markup=keyboard,
            parse_mode='HTML'
        )

    return EDIT_HUMAN_SETTINGS


@owner_only
async def edit_human_prompt_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    settings = get_data(SETTINGS_FILE, {}).get('humanization', {})
    current_rules = settings.get('custom_rules', (
        "ПРАВИЛА ГЕНЕРАЦИИ:\n"
        "1. Пиши предельно кратко (1-2 предложения).\n"
        "2. ИЗБЕГАЙ ЗАУМНЫХ СЛОВ. Пиши так, как пишут реальные люди в чатах (с маленькой буквы, сленг).\n"
        "3. РАЗНООБРАЗИЕ: Используй синонимы. Не используй одни и те же вводные конструкции. Меняй структуру предложений."
    ))

    await try_edit_message(query,
                           f"Введите новый текст правил генерации.\n\n<b>Текущие правила:</b>\n<code>{html.escape(current_rules)}</code>",
                           None)
    return EDIT_HUMAN_PROMPT


@owner_only
async def save_human_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_rules = update.message.text.strip()

    settings = get_data(SETTINGS_FILE, {})
    if 'humanization' not in settings:
        settings['humanization'] = {}

    settings['humanization']['custom_rules'] = new_rules
    save_data(SETTINGS_FILE, settings)

    await update.message.reply_text("✅ Новые правила генерации сохранены.")
    return await humanization_settings_menu(update, context)


@owner_only
async def edit_human_setting_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    mode = query.data.split('_')[-1]
    context.user_data['edit_human_mode'] = mode

    prompts = {
        'typo': "Введите шанс опечатки (0-100):",
        'lower': "Введите шанс использования маленьких букв (0-100):",
        'split': "Введите шанс разбива сообщения на части (0-100):",
        'comma': "Введите шанс пропуска запятой (0-100):",
        'words': "Введите ограничение по количеству слов (для инструкции AI, например 15):",
        'tokens': "Введите лимит Max Tokens (техническое ограничение, например 60):",
        'penalty': "Введите силу штрафа за повторы (0-100).\nЧем выше число, тем сильнее нейросеть избегает повторения слов.\nРекомендую 20-50.",
        'temp': "Введите значение Температуры (от 0.1 до 2.0).\n\n📉 <b>0.1 - 0.7</b>: Строгий, логичный, 'сухой', меньше галлюцинаций.\n📈 <b>0.8 - 1.3</b>: Сбалансированный (стандарт).\n🔥 <b>1.4 - 2.0</b>: Креативный, непредсказуемый, может шутить или нести бред."
    }

    prompt_text = prompts.get(mode, "Введите значение:")
    if mode == 'rep':
        prompt_text = prompts['penalty']

    await try_edit_message(query, prompt_text, None)
    return EDIT_HUMAN_VALUE


@owner_only
async def save_human_setting(update: Update, context: ContextTypes.DEFAULT_TYPE):
    mode = context.user_data.get('edit_human_mode')

    try:
        val_text = update.message.text.strip().replace(',', '.')
        if mode == 'temp':
            val = float(val_text)
            if not (0.0 <= val <= 2.0):
                await update.message.reply_text("Температура должна быть от 0.0 до 2.0")
                return None
        else:
            val = int(float(val_text))
            if val < 0: raise ValueError
    except:
        await update.message.reply_text("Введите корректное число.")
        return None

    settings = get_data(SETTINGS_FILE, {})
    if 'humanization' not in settings: settings['humanization'] = {}

    key_map = {
        'typo': 'typo_chance',
        'lower': 'lowercase_chance',
        'split': 'split_chance',
        'comma': 'comma_skip_chance',
        'words': 'max_words',
        'tokens': 'max_tokens',
        'penalty': 'repetition_penalty',
        'rep': 'repetition_penalty',
        'temp': 'temperature'
    }

    actual_key = key_map.get(mode)
    if not actual_key:
        actual_key = 'repetition_penalty'

    if mode in ['typo', 'lower', 'split', 'comma', 'rep', 'penalty'] and val > 100:
        await update.message.reply_text("Введите число от 0 до 100")
        return None

    settings['humanization'][actual_key] = val
    save_data(SETTINGS_FILE, settings)

    await update.message.reply_text(f"✅ Сохранено: {val}")
    return await humanization_settings_menu(update, context)


@owner_only
async def change_provider_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🤖 Google Gemini", callback_data="set_provider_gemini")],
        [InlineKeyboardButton("🧠 OpenAI GPT", callback_data="set_provider_openai")],
        [InlineKeyboardButton("🧩 OpenRouter", callback_data="set_provider_openrouter")],
        [InlineKeyboardButton("🌐 Deepseek", callback_data="set_provider_deepseek")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_settings")]
    ])
    await try_edit_message(query, "Выберите AI провайдера по умолчанию:", keyboard)
    return AI_PROVIDER_MENU


@owner_only
async def set_provider(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    provider = query.data.split('_')[-1]
    settings = get_data(SETTINGS_FILE, {})
    settings['ai_provider'] = provider
    save_data(SETTINGS_FILE, settings)
    await query.answer(f"✅ Провайдер по умолчанию изменен на {provider.upper()}", show_alert=True)
    return await settings_menu(update, context)


@owner_only
async def update_api_key_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🤖 Google Gemini", callback_data="get_api_key_gemini")],
        [InlineKeyboardButton("🧠 OpenAI GPT", callback_data="get_api_key_openai")],
        [InlineKeyboardButton("🧩 OpenRouter", callback_data="get_api_key_openrouter")],
        [InlineKeyboardButton("🌐 Deepseek", callback_data="get_api_key_deepseek")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_settings")]
    ])
    await try_edit_message(query, "Для какого AI провайдера вы хотите указать ключ?", keyboard)
    return SETTINGS_MENU


@owner_only
async def get_api_key_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    provider = query.data.split('_')[-1]
    context.user_data['api_key_provider'] = provider
    await query.answer()

    settings = get_data(SETTINGS_FILE, {})
    current_key = settings.get('api_keys', {}).get(provider)

    display_key = "<code>не задан</code>"
    if current_key and len(current_key) > 8:
        display_key = f"<code>{current_key[:4]}...{current_key[-4:]}</code>"
    elif current_key:
        display_key = "<code>ключ задан (слишком короткий для маскировки)</code>"

    text = (f"Отправьте мне API ключ для <b>{provider.upper()}</b>.\n\n"
            f"Текущий ключ: {display_key}")

    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="update_api_key")]])

    await try_edit_message(query, text, keyboard)
    return GET_API_KEY


@owner_only
async def get_api_key(update: Update, context: ContextTypes.DEFAULT_TYPE):
    api_key = update.message.text.strip()
    provider = context.user_data.get('api_key_provider')
    if not provider:
        await update.message.reply_text("Ошибка: не удалось определить провайдера. Попробуйте снова.")
        return await settings_menu(update, context)

    settings = get_data(SETTINGS_FILE, {})
    if 'api_keys' not in settings:
        settings['api_keys'] = {}
    settings['api_keys'][provider] = api_key
    save_data(SETTINGS_FILE, settings)

    await update.message.delete()
    await context.bot.send_message(update.effective_chat.id, f"✅ API ключ для {provider.upper()} сохранен.")

    context.user_data.clear()
    return await start(update, context)


@owner_only
async def targets_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    text = "<b>🎯 Управление целевыми чатами для комментирования</b>"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Мои чаты", callback_data="my_targets_list_0")],
        [InlineKeyboardButton("➕ Добавить чат", callback_data="add_target_start"),
         InlineKeyboardButton("🔍 Найти похожие каналы", callback_data="search_similar_channels")],
        [InlineKeyboardButton("❌ Удалить чат", callback_data="delete_target_start")],
        [InlineKeyboardButton("🗑️ Удалить все чаты", callback_data="delete_all_targets_confirm")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
    ])
    await try_edit_message(query, text, keyboard)
    return TARGETS_MENU


@owner_only
async def search_similar_channels_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "Отправьте ID, @username или ссылку на канал, чтобы найти похожие на него.", None)
    return GET_SOURCE_CHANNEL


@owner_only
async def get_source_channel_and_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    source_channel_input = update.message.text.strip()
    accounts = get_data(ACCOUNTS_FILE, [])
    if not accounts:
        await update.message.reply_text("❌ Нет аккаунтов для выполнения поиска. Добавьте хотя бы один.")
        return await start(update, context)

    await update.message.reply_text("⏳ Ищу похожие каналы и проверяю комментарии... Это может занять до минуты.")

    api_id = int(telethon_config['api_id'])
    api_hash = telethon_config['api_hash']
    client = TelegramClient(StringSession(accounts[0]['session_string']), api_id, api_hash)

    found_channels_with_comments = []
    try:
        await client.connect()
        if not await client.is_user_authorized():
            await update.message.reply_text("❌ Первый аккаунт в списке не авторизован. Проверьте аккаунты.")
            return await start(update, context)

        source_entity = await client.get_entity(source_channel_input)
        result = await client(GetChannelRecommendationsRequest(channel=source_entity))

        for chat in result.chats:
            if not getattr(chat, 'megagroup', False):
                try:
                    full_channel = await client(GetFullChannelRequest(channel=chat))
                    if hasattr(full_channel.full_chat, 'linked_chat_id') and full_channel.full_chat.linked_chat_id:
                        linked_chat_id_bare = full_channel.full_chat.linked_chat_id
                        comment_chat_entity = await client.get_entity(types.PeerChannel(linked_chat_id_bare))

                        found_channels_with_comments.append({
                            'chat_id': f"-100{chat.id}",
                            'chat_username': getattr(chat, 'username', None),
                            'chat_name': chat.title,
                            'linked_chat_id': f"-100{comment_chat_entity.id}"
                        })
                except Exception:
                    continue

    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка при поиске: {e}")
        return await start(update, context)
    finally:
        if client.is_connected():
            await client.disconnect()

    context.user_data['found_channels'] = found_channels_with_comments
    return await show_found_channels(update, context)


@owner_only
async def show_found_channels(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    page = 0
    if query and query.data.startswith("found_channels_page_"):
        page = int(query.data.split('_')[-1])
        await query.answer()

    found_channels = context.user_data.get('found_channels', [])

    if not found_channels:
        text = "😕 Не найдено похожих каналов с открытыми комментариями."
        keyboard = [[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_targets_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
    else:
        paginated_channels, total_items = get_paginated_items(found_channels, page)
        text = f"<b>✅ Найдено каналов с комментариями: {total_items} (Стр. {page + 1})</b>\n\nНажмите на канал, чтобы добавить его в цели."

        keyboard_buttons = []
        for i, channel_data in enumerate(paginated_channels):
            original_index = (page * ITEMS_PER_PAGE) + i
            keyboard_buttons.append([InlineKeyboardButton(f"➕ {html.escape(channel_data['chat_name'])}",
                                                          callback_data=f"add_found_{original_index}")])

        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"found_channels_page_{page - 1}"))
        if (page + 1) * ITEMS_PER_PAGE < total_items:
            nav_buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"found_channels_page_{page + 1}"))

        if nav_buttons:
            keyboard_buttons.append(nav_buttons)

        keyboard_buttons.append([InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_targets_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard_buttons)

    if query:
        await try_edit_message(query, text, reply_markup)
    else:
        await context.bot.send_message(update.effective_chat.id, text, reply_markup=reply_markup, parse_mode='HTML')

    return SHOW_FOUND_CHANNELS


@owner_only
async def add_found_channel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    channel_index = int(query.data.split('_')[-1])
    found_channels = context.user_data.get('found_channels', [])

    if channel_index >= len(found_channels):
        await query.message.edit_text("Ошибка: неверный индекс канала. Попробуйте снова.")
        return await targets_menu(update, context)

    channel_to_add = found_channels[channel_index]
    context.user_data['new_target'] = channel_to_add

    await query.message.edit_text(
        f"✅ Канал '{html.escape(channel_to_add['chat_name'])}' выбран.\n\n"
        "Теперь введите <b>промпт по умолчанию</b> ('личность') для AI в этом чате.",
        parse_mode='HTML'
    )

    return ADD_TARGET_PROMPT


@owner_only
async def confirm_delete_all_targets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("ДА, УДАЛИТЬ ВСЕ", callback_data="delete_all_targets_confirmed")],
        [InlineKeyboardButton("⬅️ Нет, назад", callback_data="back_to_targets")]
    ])
    await try_edit_message(query, "<b>Вы уверены, что хотите удалить ВСЕ целевые чаты?</b>\n\nЭто действие необратимо!",
                           keyboard)
    return CONFIRM_DELETE_ALL_TARGETS


@owner_only
async def do_delete_all_targets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    settings = get_data(SETTINGS_FILE, {})
    settings['targets'] = []
    save_data(SETTINGS_FILE, settings)
    await query.answer("✅ Все целевые чаты были удалены.", show_alert=True)
    return await targets_menu(update, context)


@owner_only
async def show_my_targets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    page = 0
    if query.data and query.data.startswith("my_targets_list_"):
        page = int(query.data.split('_')[-1])

    settings = get_data(SETTINGS_FILE, {})
    targets = settings.get('targets', [])

    indexed_targets = list(enumerate(targets))
    sorted_indexed_targets = sorted(indexed_targets, key=lambda x: x[1].get('date_added', ''), reverse=True)

    paginated_targets, total_items = get_paginated_items(sorted_indexed_targets, page)

    text = f"<b>📋 Список ваших чатов для комментирования (Стр. {page + 1})</b>"
    if not paginated_targets:
        text += "\n\n<i>Пока не добавлено ни одного чата.</i>"

    keyboard_buttons = []
    for original_index, target_data in paginated_targets:
        keyboard_buttons.append(
            [InlineKeyboardButton(f"⚙️ {html.escape(target_data.get('chat_name', target_data['chat_id']))}",
                                  callback_data=f"edit_target_{original_index}")])

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"my_targets_list_{page - 1}"))
    if (page + 1) * ITEMS_PER_PAGE < total_items:
        nav_buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"my_targets_list_{page + 1}"))

    if nav_buttons:
        keyboard_buttons.append(nav_buttons)

    keyboard_buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_targets_menu")])
    reply_markup = InlineKeyboardMarkup(keyboard_buttons)

    await try_edit_message(query, text, reply_markup)
    return MY_TARGETS_MENU


@owner_only
async def add_target_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "Введите ID или @username целевого чата/канала (например, -100... или @channel):",
                           None)
    return ADD_TARGET_CHAT_ID


@owner_only
async def add_target_get_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_input = update.message.text.strip()
    api_id = int(telethon_config['api_id'])
    api_hash = telethon_config['api_hash']
    accounts = get_data(ACCOUNTS_FILE, [])
    if not accounts:
        await update.message.reply_text("❌ Нет аккаунтов. Добавьте хотя бы один.")
        return await start(update, context)
    client = TelegramClient(StringSession(accounts[0]['session_string']), api_id, api_hash)
    await update.message.reply_text("⏳ Проверяю канал и ссылку...")
    try:
        await client.connect()
        invite_link = None
        if "t.me/+" in chat_input or "t.me/joinchat/" in chat_input:
            invite_hash = chat_input.split('/')[-1].replace('+', '')
            invite_link = invite_hash
            try:
                invite_info = await client(CheckChatInviteRequest(invite_hash))
                entity = invite_info.chat
            except Exception as e:
                await update.message.reply_text(f"❌ Ошибка ссылки: {e}")
                return await start(update, context)
        else:
            entity = await client.get_entity(chat_input)

        chat_username = getattr(entity, 'username', None)
        try:
            await client(JoinChannelRequest(entity))
        except:
            pass

        channel_id_str = f"-100{entity.id}"
        chat_name_to_save = entity.title
        comment_chat_id_str = channel_id_str
        try:
            full_channel = await client(GetFullChannelRequest(channel=entity))
            if hasattr(full_channel.full_chat, 'linked_chat_id') and full_channel.full_chat.linked_chat_id:
                linked_chat_id_bare = full_channel.full_chat.linked_chat_id
                comment_chat_entity = await client.get_entity(types.PeerChannel(linked_chat_id_bare))
                await client(JoinChannelRequest(comment_chat_entity))
                comment_chat_id_str = f"-100{comment_chat_entity.id}"
        except:
            pass
        await client.disconnect()
        context.user_data['new_target'] = {
            'chat_id': channel_id_str,
            'chat_username': chat_username,
            'linked_chat_id': comment_chat_id_str,
            'chat_name': chat_name_to_save,
            'invite_link': invite_link
        }

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("⏩ Пропустить (назначу роль позже)", callback_data="skip_target_prompt")]
        ])
        await update.message.reply_text(
            f"✅ Канал '{html.escape(chat_name_to_save)}' добавлен.\n\n"
            f"Введите <b>промпт по умолчанию</b> или нажмите пропустить:",
            parse_mode='HTML',
            reply_markup=keyboard
        )
        return ADD_TARGET_PROMPT

    except Exception as e:
        if client.is_connected(): await client.disconnect()
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return await start(update, context)


@owner_only
async def add_target_get_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['new_target']['prompts'] = {'default': update.message.text}
    context.user_data['new_target'].pop('prompt', None)
    await update.message.reply_text("🕒 Пауза после поста (в секундах)?")
    return ADD_TARGET_INITIAL_DELAY


@owner_only
async def skip_target_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    default_text = "Ты обычный пользователь Telegram. Общайся кратко, по теме поста."
    context.user_data['new_target']['prompts'] = {'default': default_text}
    context.user_data['new_target'].pop('prompt', None)

    await try_edit_message(query,
                           f"✅ Установлен стандартный промпт.\n\n🕒 Пауза после поста (в секундах)?",
                           None)
    return ADD_TARGET_INITIAL_DELAY


@owner_only
async def add_target_get_initial_delay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data['new_target']['initial_comment_delay'] = int(update.message.text)
        await update.message.reply_text("🕒 Пауза между комментариями аккаунтов (в секундах)?")
        return ADD_TARGET_BETWEEN_DELAY
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return ADD_TARGET_INITIAL_DELAY


@owner_only
async def add_target_get_between_delay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data['new_target']['delay_between_accounts'] = int(update.message.text)
        await update.message.reply_text("📊 Лимит комментариев в сутки?")
        return ADD_TARGET_DAILY_LIMIT
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return ADD_TARGET_BETWEEN_DELAY


@owner_only
async def add_target_get_daily_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data['new_target']['daily_comment_limit'] = int(update.message.text)
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🤖 Google Gemini", callback_data="save_target_ai_gemini")],
            [InlineKeyboardButton("🧠 OpenAI GPT", callback_data="save_target_ai_openai")],
            [InlineKeyboardButton("🧩 OpenRouter", callback_data="save_target_ai_openrouter")],
            [InlineKeyboardButton("🌐 Deepseek", callback_data="save_target_ai_deepseek")],
            [InlineKeyboardButton("🌐 Использовать глобальный", callback_data="save_target_ai_default")]
        ])
        await update.message.reply_text(
            f"Выберите AI для чата <b>{html.escape(context.user_data['new_target']['chat_name'])}</b>:",
            reply_markup=keyboard, parse_mode='HTML')
        return ADD_TARGET_AI_PROVIDER
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return ADD_TARGET_DAILY_LIMIT


@owner_only
async def add_target_get_ai_provider(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    provider = query.data.split('_')[-1]

    if provider != 'default':
        context.user_data['new_target']['ai_provider'] = provider

    context.user_data['new_target']['date_added'] = datetime.now(timezone.utc).isoformat()

    await query.message.edit_text("💬 Фильтр: минимальное кол-во слов в посте?\n(Введите 0, чтобы не использовать)")
    return ADD_TARGET_MIN_WORDS


@owner_only
async def add_target_get_min_words(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data['new_target']['min_word_count'] = int(update.message.text)
        await update.message.reply_text(
            "⏱ Фильтр: минимальная пауза между постами (в минутах)?\n(Введите 0, чтобы не использовать)")
        return ADD_TARGET_MIN_INTERVAL
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return ADD_TARGET_MIN_WORDS


@owner_only
async def add_target_save_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data['new_target']['min_post_interval_mins'] = int(update.message.text)
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return ADD_TARGET_MIN_INTERVAL

    settings = get_data(SETTINGS_FILE, {})
    new_target = context.user_data['new_target']
    new_target['assigned_accounts'] = []
    targets = settings.setdefault('targets', [])
    targets.append(new_target)
    save_data(SETTINGS_FILE, settings)

    await update.message.delete()
    await context.bot.send_message(
        update.effective_chat.id,
        f"✅ Чат для комментирования '{html.escape(new_target['chat_name'])}' добавлен.",
        parse_mode='HTML'
    )

    context.user_data.clear()
    return await send_main_menu(update, context)


@owner_only
async def edit_target_menu_logic(update: Update, context: ContextTypes.DEFAULT_TYPE, from_callback: bool = True):
    if from_callback and update.callback_query:
        await update.callback_query.answer()
    target_index = context.user_data.get('edit_target_index')
    if target_index is None: return await start(update, context)
    settings = get_data(SETTINGS_FILE, {})
    if target_index >= len(settings.get('targets', [])): return await start(update, context)
    target = settings.get('targets', [])[target_index]

    provider = target.get('ai_provider', 'По умолч.').upper()
    assigned_accounts = target.get('assigned_accounts', [])
    min_words = target.get('min_word_count', 0)
    min_interval = target.get('min_post_interval_mins', 0)

    ai_status = target.get('ai_enabled', True)
    ai_icon = "🟢" if ai_status else "🔴"

    text = f"<b>⚙️ Настройки чата: {html.escape(target.get('chat_name'))}</b>\n\n"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🤖 AI Авто-ответы: {ai_icon}", callback_data="toggle_target_ai"),
         InlineKeyboardButton("⚡ Триггеры (Ответы)", callback_data="triggers_menu_entry")],
        [InlineKeyboardButton("📜 Сценарий (Script)", callback_data="scenario_menu_entry")],
        [InlineKeyboardButton(f"👤 Аккаунты: {len(assigned_accounts) if assigned_accounts else 'Все'}",
                              callback_data="assign_accounts")],
        [InlineKeyboardButton(f"🤖 AI чата: {provider}", callback_data="set_chat_ai")],
        [InlineKeyboardButton("✏️ Промпты", callback_data="edit_prompts_menu")],
        [InlineKeyboardButton(
            f"⏱ Паузы: {target.get('initial_comment_delay', 'N/A')}с / {target.get('delay_between_accounts', 'N/A')}с",
            callback_data="edit_delays")],
        [InlineKeyboardButton("💬 Настройки ответов (Диалог)", callback_data="edit_reply_settings")],
        [InlineKeyboardButton(f"📊 Лимит в сутки: {target.get('daily_comment_limit', 'N/A')}",
                              callback_data="edit_limit")],
        [InlineKeyboardButton(f"⚙️ Фильтры: {min_words} сл. / {min_interval} мин.", callback_data="edit_filters")],
        [InlineKeyboardButton("⬅️ Назад к списку", callback_data="my_targets_list_0")]
    ])
    if from_callback and update.callback_query:
        await try_edit_message(update.callback_query, text, keyboard)
    else:
        await context.bot.send_message(update.effective_chat.id, text, reply_markup=keyboard, parse_mode='HTML')
    return EDIT_TARGET_MENU


@owner_only
async def toggle_target_ai(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    target_index = context.user_data.get('edit_target_index')
    settings = get_data(SETTINGS_FILE, {})

    current_status = settings['targets'][target_index].get('ai_enabled', True)
    settings['targets'][target_index]['ai_enabled'] = not current_status
    save_data(SETTINGS_FILE, settings)

    await query.answer(f"AI режим {'включен' if not current_status else 'выключен'}")
    return await edit_target_menu_logic(update, context)


@owner_only
async def edit_reply_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()
    target_index = context.user_data.get('edit_target_index')
    settings = get_data(SETTINGS_FILE)
    if target_index is None or target_index >= len(settings.get('targets', [])):
        return await start(update, context)
    target = settings['targets'][target_index]

    chance = target.get('reply_chance', 0)
    d_min = target.get('reply_delay_min', 30)
    d_max = target.get('reply_delay_max', 120)
    depth = target.get('max_dialogue_depth', 10)
    intervention = target.get('intervention_chance', 30)
    tag_chance = target.get('tag_reply_chance', 50)

    text = (f"<b>💬 Настройки диалогов для '{html.escape(target['chat_name'])}'</b>\n\n"
            f"Боты будут отвечать на чужие комментарии с заданной вероятностью.\n\n"
            f"🎲 Вероятность ответа: <b>{chance}%</b>\n"
            f"⚡ Шанс вмешательства: <b>{intervention}%</b>\n"
            f"🔗 Визуальный тег (Reply): <b>{tag_chance}%</b>\n"
            f"📜 Глубина контекста: <b>{depth}</b> сообщ.\n"
            f"⏱ Задержка: от <b>{d_min}</b> до <b>{d_max}</b> сек.")

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎲 Шанс ответа", callback_data="edit_reply_chance"),
         InlineKeyboardButton("⚡ Шанс вмешательства", callback_data="edit_reply_intervention")],
        [InlineKeyboardButton("🔗 Шанс тега (Reply)", callback_data="edit_tag_reply_chance"),
         InlineKeyboardButton("📜 Глубина контекста", callback_data="edit_reply_depth")],
        [InlineKeyboardButton("⏱ Мин. задержка", callback_data="edit_reply_delay_min"),
         InlineKeyboardButton("⏱ Макс. задержка", callback_data="edit_reply_delay_max")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_edit_menu")]
    ])

    if query:
        await try_edit_message(query, text, keyboard)
    else:
        await context.bot.send_message(update.effective_chat.id, text, reply_markup=keyboard, parse_mode='HTML')
    return EDIT_REPLY_SETTINGS_MENU


@owner_only
async def edit_tag_reply_chance_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['current_setting_state'] = 'tag_reply'
    await query.message.reply_text("Введите вероятность визуального тега (Reply) от 0 до 100.\n0 — всегда без тега, 100 — всегда с тегом:")
    return EDIT_TAG_REPLY_CHANCE


@owner_only
async def save_reply_setting(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        val = int(update.message.text.strip())
        state = context.user_data.get('current_setting_state')
        target_index = context.user_data.get('edit_target_index')
        settings = get_data(SETTINGS_FILE)

        if target_index is None:
            return await start(update, context)

        if state == 'chance':
            val = max(0, min(100, val))
            settings['targets'][target_index]['reply_chance'] = val
        elif state == 'intervention':
            val = max(0, min(100, val))
            settings['targets'][target_index]['intervention_chance'] = val
        elif state == 'tag_reply':
            val = max(0, min(100, val))
            settings['targets'][target_index]['tag_reply_chance'] = val
        elif state == 'min':
            settings['targets'][target_index]['reply_delay_min'] = val
        elif state == 'max':
            settings['targets'][target_index]['reply_delay_max'] = val
        elif state == 'depth':
            val = max(1, min(50, val))
            settings['targets'][target_index]['max_dialogue_depth'] = val

        save_data(SETTINGS_FILE, settings)
        await update.message.reply_text(f"✅ Настройка сохранена: {val}")
        return await edit_reply_settings_menu(update, context)

    except (ValueError, TypeError):
        await update.message.reply_text("❌ Ошибка. Введите целое число.")
        return await edit_reply_settings_menu(update, context)


@owner_only
async def edit_target_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['edit_target_index'] = int(update.callback_query.data.split('_')[-1])
    return await edit_target_menu_logic(update, context)


@owner_only
async def edit_reply_intervention_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['current_setting_state'] = 'intervention'
    await query.message.reply_text(
        "⚡ Введите вероятность вмешательства третьего бота в чужой диалог (0-100%).\n"
        "По умолчанию 30%."
    )
    return EDIT_REPLY_INTERVENTION


@owner_only
async def assign_accounts_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    target_index = context.user_data['edit_target_index']
    settings = get_data(SETTINGS_FILE, {})
    accounts = get_data(ACCOUNTS_FILE, [])
    target = settings['targets'][target_index]
    assigned_accounts = set(target.get('assigned_accounts', []))
    if not accounts:
        await query.answer("Сначала добавьте аккаунты!", show_alert=True)
        return EDIT_TARGET_MENU
    keyboard = [
        [InlineKeyboardButton(
            f"{'✅' if acc['session_name'] in assigned_accounts else '➖'} {html.escape(acc['session_name'])}",
            callback_data=f"toggle_acc_{acc['session_name']}")] for acc in accounts]
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_edit_menu")])
    await try_edit_message(query, f"Отметьте аккаунты для чата <b>{html.escape(target['chat_name'])}</b>:",
                           InlineKeyboardMarkup(keyboard))
    return EDIT_CHAT_ASSIGN_ACCOUNTS


@owner_only
async def toggle_account_assignment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    session_name = query.data.split('_')[-1]
    target_index = context.user_data['edit_target_index']

    settings = get_data(SETTINGS_FILE, {})
    target = settings['targets'][target_index]

    assigned_accounts = set(target.get('assigned_accounts', []))

    if session_name in assigned_accounts:
        assigned_accounts.remove(session_name)
        await query.answer(f"➖ {session_name} отвязан.", show_alert=False)
    else:
        assigned_accounts.add(session_name)
        await query.answer(f"✅ {session_name} назначен.", show_alert=False)

    settings['targets'][target_index]['assigned_accounts'] = list(assigned_accounts)
    save_data(SETTINGS_FILE, settings)

    return await assign_accounts_menu(update, context)


@owner_only
async def set_chat_ai_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    target_index = context.user_data['edit_target_index']
    target = get_data(SETTINGS_FILE, {})['targets'][target_index]
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🤖 Google Gemini", callback_data="save_chat_ai_gemini")],
        [InlineKeyboardButton("🧠 OpenAI GPT", callback_data="save_chat_ai_openai")],
        [InlineKeyboardButton("🧩 OpenRouter", callback_data="save_chat_ai_openrouter")],
        [InlineKeyboardButton("🌐 Deepseek", callback_data="save_chat_ai_deepseek")],
        [InlineKeyboardButton("🌐 Использовать глобальный", callback_data="save_chat_ai_default")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_edit_menu")]
    ])
    await try_edit_message(query, f"Выберите AI для чата <b>{html.escape(target['chat_name'])}</b>:", keyboard)
    return EDIT_CHAT_AI


@owner_only
async def save_chat_ai(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    provider = query.data.split('_')[-1]
    target_index = context.user_data['edit_target_index']
    settings = get_data(SETTINGS_FILE, {})

    if provider == 'default':
        if 'ai_provider' in settings['targets'][target_index]:
            del settings['targets'][target_index]['ai_provider']
        provider_name = 'По умолчанию'
    else:
        settings['targets'][target_index]['ai_provider'] = provider
        provider_name = provider.upper()

    save_data(SETTINGS_FILE, settings)
    await query.answer(f"AI для чата изменен на {provider_name}", show_alert=True)
    return await edit_target_menu_logic(update, context)


@owner_only
async def edit_delays_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "🕒 Введите новую паузу после поста (в сек):", None)
    return EDIT_DELAYS_INITIAL


@owner_only
async def edit_delays_get_initial(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        delay = int(update.message.text)
        target_index = context.user_data['edit_target_index']
        settings = get_data(SETTINGS_FILE, {})
        settings['targets'][target_index]['initial_comment_delay'] = delay
        save_data(SETTINGS_FILE, settings)
        await update.message.reply_text(
            "✅ Пауза после поста обновлена.\n\n🕒 Введите новую паузу между комментариями от аккаунтов (в сек):")
        return EDIT_DELAYS_BETWEEN
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return EDIT_DELAYS_INITIAL


@owner_only
async def edit_delays_get_between(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        delay = int(update.message.text)
        target_index = context.user_data['edit_target_index']
        settings = get_data(SETTINGS_FILE, {})
        settings['targets'][target_index]['delay_between_accounts'] = delay
        save_data(SETTINGS_FILE, settings)
        await update.message.delete()
        await context.bot.send_message(update.effective_chat.id, "✅ Паузы обновлены.")
        return await edit_target_menu_logic(update, context, from_callback=False)
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return EDIT_DELAYS_BETWEEN


@owner_only
async def edit_limit_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "📊 Введите новый лимит комментариев в сутки:", None)
    return EDIT_TARGET_DAILY_LIMIT


@owner_only
async def edit_target_get_daily_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        limit = int(update.message.text)
        target_index = context.user_data['edit_target_index']
        settings = get_data(SETTINGS_FILE, {})
        settings['targets'][target_index]['daily_comment_limit'] = limit
        save_data(SETTINGS_FILE, settings)
        await update.message.delete()
        await context.bot.send_message(update.effective_chat.id, "✅ Лимит обновлен.")
        return await edit_target_menu_logic(update, context, from_callback=False)
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return EDIT_TARGET_DAILY_LIMIT


@owner_only
async def prompts_menu_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    if query:
        await query.answer()

    target_index = context.user_data.get('edit_target_index')
    settings = get_data(SETTINGS_FILE, {})

    if target_index is None or target_index >= len(settings.get('targets', [])):
        return await start(update, context)

    target = settings['targets'][target_index]

    if 'prompt' in target and 'prompts' not in target:
        target['prompts'] = {'default': target.pop('prompt')}
        save_data(SETTINGS_FILE, settings)

    prompts = target.get('prompts', {'default': 'Не задан'})
    default_prompt = prompts.get('default', 'Не задан')
    accounts = get_data(ACCOUNTS_FILE, [])
    assigned_accounts = target.get('assigned_accounts', [])

    keyboard_buttons = [[InlineKeyboardButton("✏️ Промпт по умолчанию", callback_data="edit_prompt_default")]]

    for acc in accounts:
        if acc['session_name'] in assigned_accounts:
            acc_prompt = prompts.get(acc['session_name'])
            btn_text = f"👤 {html.escape(acc['session_name'])}"
            if acc_prompt:
                btn_text += " (персональный)"
            keyboard_buttons.append(
                [InlineKeyboardButton(btn_text, callback_data=f"edit_prompt_{acc['session_name']}")])

    keyboard_buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_edit_menu")])
    keyboard = InlineKeyboardMarkup(keyboard_buttons)

    text = (f"<b>✏️ Управление промптами для \"{html.escape(target['chat_name'])}\"</b>\n\n"
            f"<b>Текущий промпт по умолчанию:</b>\n<code>{html.escape(default_prompt)}</code>\n\n"
            f"Выберите, какой промпт изменить, или назначьте персональные промпты для аккаунтов.")

    if query:
        await try_edit_message(query, text, keyboard)
    else:
        await context.bot.send_message(update.effective_chat.id, text, reply_markup=keyboard, parse_mode='HTML')

    return PROMPTS_MENU


@owner_only
async def edit_account_prompt_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    account_key = query.data.split('_', 2)[-1]
    context.user_data['editing_prompt_for'] = account_key

    target_index = context.user_data['edit_target_index']
    settings = get_data(SETTINGS_FILE, {})
    target = settings['targets'][target_index]
    current_prompt = target.get('prompts', {}).get(account_key, 'Не задан')

    prompt_name = "по умолчанию" if account_key == 'default' else f"для аккаунта {html.escape(account_key)}"
    text = (f"Введите новый текст промпта {prompt_name}.\n\n"
            f"<b>Текущий промпт:</b>\n<code>{html.escape(current_prompt)}</code>")

    await try_edit_message(query, text, None)
    return GET_ACCOUNT_PROMPT


@owner_only
async def save_account_prompt(update: Update, context: ContextTypes.DEFAULT_TYPE):
    new_prompt = update.message.text
    target_index = context.user_data['edit_target_index']
    account_key = context.user_data['editing_prompt_for']

    settings = get_data(SETTINGS_FILE, {})
    target = settings['targets'][target_index]

    if 'prompts' not in target:
        target['prompts'] = {}

    target['prompts'][account_key] = new_prompt
    save_data(SETTINGS_FILE, settings)

    await update.message.delete()
    await context.bot.send_message(update.effective_chat.id, "✅ Промпт успешно обновлен.")

    return await prompts_menu_start(update, context)


@owner_only
async def delete_target_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    page = 0
    if query.data and query.data.startswith("delete_target_start_"):
        page = int(query.data.split('_')[-1])

    settings = get_data(SETTINGS_FILE, {})
    targets = settings.get('targets', [])

    if not targets:
        await try_edit_message(query, "Нет чатов для удаления.", InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_targets")]]))
        return TARGETS_MENU

    indexed_targets = list(enumerate(targets))
    paginated_targets, total_items = get_paginated_items(indexed_targets, page)

    text = f"<b>❌ Выберите чат для удаления (Стр. {page + 1})</b>"

    keyboard_buttons = []
    for original_index, target_data in paginated_targets:
        keyboard_buttons.append(
            [InlineKeyboardButton(f"❌ {html.escape(target_data.get('chat_name', target_data['chat_id']))}",
                                  callback_data=f"del_target_{original_index}")])

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"delete_target_start_{page - 1}"))
    if (page + 1) * ITEMS_PER_PAGE < total_items:
        nav_buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"delete_target_start_{page + 1}"))

    if nav_buttons:
        keyboard_buttons.append(nav_buttons)

    keyboard_buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_targets")])
    await try_edit_message(query, text, InlineKeyboardMarkup(keyboard_buttons))

    return GET_TARGET_TO_DELETE


@owner_only
async def delete_target_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    target_index = int(query.data.split('_')[-1])
    settings = get_data(SETTINGS_FILE, {})

    if 0 <= target_index < len(settings.get('targets', [])):
        removed = settings['targets'].pop(target_index)
        save_data(SETTINGS_FILE, settings)
        await query.answer(f"✅ Чат '{removed.get('chat_name')}' удален.", show_alert=True)
    else:
        await query.answer("Ошибка: неверный индекс чата.", show_alert=True)

    context.user_data.clear()
    return await start(update, context)


@owner_only
async def reaction_targets_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    text = "<b>👍 Управление чатами для реакций</b>"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Мои чаты", callback_data="my_reaction_targets_list_0")],
        [InlineKeyboardButton("➕ Добавить чат", callback_data="add_reaction_target_start")],
        [InlineKeyboardButton("❌ Удалить чат", callback_data="delete_reaction_target_start")],
        [InlineKeyboardButton("🗑️ Удалить все чаты", callback_data="delete_all_reaction_targets_confirm")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
    ])
    await try_edit_message(query, text, keyboard)
    return REACTION_TARGETS_MENU


@owner_only
async def confirm_delete_all_reaction_targets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("ДА, УДАЛИТЬ ВСЕ", callback_data="delete_all_reaction_targets_confirmed")],
        [InlineKeyboardButton("⬅️ Нет, назад", callback_data="back_to_reaction_targets")]
    ])
    await try_edit_message(query,
                           "<b>Вы уверены, что хотите удалить ВСЕ чаты для реакций?</b>\n\nЭто действие необратимо!",
                           keyboard)
    return CONFIRM_DELETE_ALL_REACTION_TARGETS


@owner_only
async def do_delete_all_reaction_targets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    settings = get_data(SETTINGS_FILE, {})
    settings['reaction_targets'] = []
    save_data(SETTINGS_FILE, settings)
    await query.answer("✅ Все чаты для реакций были удалены.", show_alert=True)
    return await reaction_targets_menu(update, context)


@owner_only
async def show_my_reaction_targets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    page = 0
    if query.data and query.data.startswith("my_reaction_targets_list_"):
        page = int(query.data.split('_')[-1])

    settings = get_data(SETTINGS_FILE, {})
    targets = settings.get('reaction_targets', [])

    indexed_targets = list(enumerate(targets))
    sorted_indexed_targets = sorted(indexed_targets, key=lambda x: x[1].get('date_added', ''), reverse=True)

    paginated_targets, total_items = get_paginated_items(sorted_indexed_targets, page)

    text = f"<b>📋 Список ваших чатов для реакций (Стр. {page + 1})</b>"
    if not paginated_targets:
        text += "\n\n<i>Пока не добавлено ни одного чата.</i>"

    keyboard_buttons = []
    for original_index, target_data in paginated_targets:
        keyboard_buttons.append([InlineKeyboardButton(f"⚙️ {html.escape(target_data.get('chat_name', target_data['chat_id']))}",
                                                  callback_data=f"edit_reaction_target_{original_index}")])

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"my_reaction_targets_list_{page - 1}"))
    if (page + 1) * ITEMS_PER_PAGE < total_items:
        nav_buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"my_reaction_targets_list_{page + 1}"))

    if nav_buttons:
        keyboard_buttons.append(nav_buttons)

    keyboard_buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_reaction_targets_menu")])
    reply_markup = InlineKeyboardMarkup(keyboard_buttons)

    await try_edit_message(query, text, reply_markup)
    return MY_REACTION_TARGETS_MENU


@owner_only
async def add_reaction_target_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "Введите ID или @username целевого чата/канала:", None)
    return ADD_REACTION_TARGET_CHAT_ID


@owner_only
async def add_reaction_target_get_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_input = update.message.text.strip()
    api_id = int(telethon_config['api_id'])
    api_hash = telethon_config['api_hash']
    accounts = get_data(ACCOUNTS_FILE, [])

    if not accounts:
        await update.message.reply_text("❌ Нет аккаунтов для проверки ID канала. Добавьте хотя бы один аккаунт.")
        return await start(update, context)

    client = TelegramClient(StringSession(accounts[0]['session_string']), api_id, api_hash)
    await update.message.reply_text("⏳ Проверяю канал...")

    try:
        await client.connect()
        if not await client.is_user_authorized():
            await client.disconnect()
            await update.message.reply_text("❌ Первый аккаунт в списке не авторизован.")
            return await start(update, context)

        entity = await client.get_entity(chat_input)
        await client(JoinChannelRequest(entity))

        context.user_data['new_reaction_target'] = {
            'chat_id': f"-100{entity.id}",
            'chat_username': getattr(entity, 'username', None),
            'linked_chat_id': f"-100{entity.id}",
            'chat_name': entity.title
        }

        try:
            full_channel = await client(GetFullChannelRequest(channel=entity))
            if hasattr(full_channel.full_chat, 'linked_chat_id') and full_channel.full_chat.linked_chat_id:
                linked_chat_id = f"-100{full_channel.full_chat.linked_chat_id}"
                context.user_data['new_reaction_target']['linked_chat_id'] = linked_chat_id
        except Exception:
            pass

        await client.disconnect()

        await update.message.reply_text(
            f"✅ Канал '{html.escape(entity.title)}' найден.\n\n"
            f"Введите реакции через пробел. Первая будет основной, остальные случайными.\n"
            f"Пример: 👍 ❤️ 🔥 🎉")
        return ADD_REACTION_TARGET_REACTIONS

    except Exception as e:
        if client.is_connected(): await client.disconnect()
        await update.message.reply_text(f"❌ Ошибка: {e}")
        return await start(update, context)


@owner_only
async def add_reaction_target_get_reactions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reactions = update.message.text.strip().split()
    if not reactions:
        await update.message.reply_text("Вы не ввели реакции. Попробуйте снова.")
        return ADD_REACTION_TARGET_REACTIONS
    context.user_data['new_reaction_target']['reactions'] = reactions
    await update.message.reply_text("Пауза после поста (в секундах)?")
    return ADD_REACTION_TARGET_INITIAL_DELAY


@owner_only
async def add_reaction_target_get_initial_delay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data['new_reaction_target']['initial_reaction_delay'] = int(update.message.text)
        await update.message.reply_text("Пауза между реакциями аккаунтов (в секундах)?")
        return ADD_REACTION_TARGET_BETWEEN_DELAY
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return ADD_REACTION_TARGET_INITIAL_DELAY


@owner_only
async def add_reaction_target_get_between_delay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data['new_reaction_target']['delay_between_reactions'] = int(update.message.text)
        await update.message.reply_text("Лимит постов для реакций в сутки?")
        return ADD_REACTION_TARGET_DAILY_LIMIT
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return ADD_REACTION_TARGET_BETWEEN_DELAY


@owner_only
async def add_reaction_target_get_daily_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        context.user_data['new_reaction_target']['daily_reaction_limit'] = int(update.message.text)
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("1", callback_data="save_reaction_count_1"),
             InlineKeyboardButton("2", callback_data="save_reaction_count_2"),
             InlineKeyboardButton("3", callback_data="save_reaction_count_3")]
        ])
        await update.message.reply_text("Выберите кол-во реакций на 1 пост:", reply_markup=keyboard)
        return ADD_REACTION_TARGET_REACTION_COUNT
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return ADD_REACTION_TARGET_DAILY_LIMIT


@owner_only
async def add_reaction_target_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    count = int(query.data.split('_')[-1])
    context.user_data['new_reaction_target']['reaction_count'] = count
    context.user_data['new_reaction_target']['date_added'] = datetime.now(timezone.utc).isoformat()

    settings = get_data(SETTINGS_FILE, {})
    new_target = context.user_data['new_reaction_target']
    new_target['assigned_accounts'] = []
    targets = settings.setdefault('reaction_targets', [])
    targets.append(new_target)
    save_data(SETTINGS_FILE, settings)

    await try_edit_message(query, f"✅ Чат для реакций '{html.escape(new_target['chat_name'])}' добавлен.")

    context.user_data.clear()

    await asyncio.sleep(2)

    return await start(update, context)


@owner_only
async def edit_filters_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    target_index = context.user_data.get('edit_target_index')
    settings = get_data(SETTINGS_FILE, {})
    target = settings.get('targets', [])[target_index]

    min_words = target.get('min_word_count', 0)
    min_interval = target.get('min_post_interval_mins', 0)

    text = (f"<b>⚙️ Настройка фильтров для '{html.escape(target['chat_name'])}'</b>\n\n"
            f"Текущие значения (0 - отключено):\n"
            f" - Мин. слов в посте: <b>{min_words}</b>\n"
            f" - Мин. интервал между постами: <b>{min_interval}</b> минут")

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("💬 Изменить мин. кол-во слов", callback_data="edit_filter_words")],
        [InlineKeyboardButton("⏱ Изменить мин. интервал", callback_data="edit_filter_interval")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_edit_menu")]
    ])
    await try_edit_message(query, text, keyboard)
    return EDIT_FILTERS_MENU


@owner_only
async def edit_filter_min_words_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "Введите новое минимальное кол-во слов (0 - отключить):")
    return EDIT_FILTER_MIN_WORDS


@owner_only
async def save_filter_min_words(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        words = int(update.message.text)
        target_index = context.user_data['edit_target_index']
        settings = get_data(SETTINGS_FILE, {})
        settings['targets'][target_index]['min_word_count'] = words
        save_data(SETTINGS_FILE, settings)
        await update.message.delete()
        await context.bot.send_message(update.effective_chat.id, f"✅ Минимальное кол-во слов установлено на: {words}")
        return await edit_target_menu_logic(update, context, from_callback=False)
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return EDIT_FILTER_MIN_WORDS


@owner_only
async def edit_filter_min_interval_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "Введите новый минимальный интервал между постами в минутах (0 - отключить):")
    return EDIT_FILTER_MIN_INTERVAL


@owner_only
async def save_filter_min_interval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        interval = int(update.message.text)
        target_index = context.user_data['edit_target_index']
        settings = get_data(SETTINGS_FILE, {})
        settings['targets'][target_index]['min_post_interval_mins'] = interval
        save_data(SETTINGS_FILE, settings)
        await update.message.delete()
        await context.bot.send_message(update.effective_chat.id,
                                       f"✅ Минимальный интервал установлен на: {interval} мин.")
        return await edit_target_menu_logic(update, context, from_callback=False)
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return EDIT_FILTER_MIN_INTERVAL


@owner_only
async def edit_reaction_target_menu_logic(update: Update, context: ContextTypes.DEFAULT_TYPE,
                                          from_callback: bool = True):
    if from_callback and update.callback_query:
        await update.callback_query.answer()

    target_index = context.user_data.get('edit_reaction_target_index')
    settings = get_data(SETTINGS_FILE, {})
    if target_index is None or target_index >= len(settings.get('reaction_targets', [])): return await start(update,
                                                                                                             context)

    target = settings.get('reaction_targets', [])[target_index]
    assigned_accounts = target.get('assigned_accounts', [])
    text = f"<b>⚙️ Настройки чата реакций: {html.escape(target.get('chat_name'))}</b>\n\n"
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"👤 Аккаунты: {len(assigned_accounts) if assigned_accounts else 'Все'}",
                              callback_data="assign_reaction_accounts")],
        [InlineKeyboardButton(f"🔢 Кол-во реакций: {target.get('reaction_count', 1)}",
                              callback_data="edit_reaction_count"),
         InlineKeyboardButton(f"🎲 Шанс: {target.get('reaction_chance', 80)}%", callback_data="edit_reaction_chance")],
        [InlineKeyboardButton(f"✏️ Реакции: {' '.join(target.get('reactions', []))}",
                              callback_data="edit_reaction_list")],
        [InlineKeyboardButton(
            f"⏱ Паузы: {target.get('initial_reaction_delay', 'N/A')}с / {target.get('delay_between_reactions', 'N/A')}с",
            callback_data="edit_reaction_delays")],
        [InlineKeyboardButton(f"📊 Лимит в сутки: {target.get('daily_reaction_limit', 'N/A')}",
                              callback_data="edit_reaction_limit")],
        [InlineKeyboardButton("⬅️ Назад к списку", callback_data="my_reaction_targets_list_0")]
    ])
    if from_callback and update.callback_query:
        await try_edit_message(update.callback_query, text, keyboard)
    else:
        await context.bot.send_message(update.effective_chat.id, text, reply_markup=keyboard, parse_mode='HTML')
    return EDIT_REACTION_TARGET_MENU


@owner_only
async def edit_reaction_target_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['edit_reaction_target_index'] = int(update.callback_query.data.split('_')[-1])
    return await edit_reaction_target_menu_logic(update, context)


@owner_only
async def assign_reaction_accounts_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    target_index = context.user_data['edit_reaction_target_index']
    settings = get_data(SETTINGS_FILE, {})
    accounts = get_data(ACCOUNTS_FILE, [])
    target = settings['reaction_targets'][target_index]
    assigned_accounts = set(target.get('assigned_accounts', []))

    if not accounts:
        await query.answer("Сначала добавьте аккаунты!", show_alert=True)
        return EDIT_REACTION_TARGET_MENU

    keyboard = [[InlineKeyboardButton(
        f"{'✅' if acc['session_name'] in assigned_accounts else '➖'} {html.escape(acc['session_name'])}",
        callback_data=f"toggle_reaction_acc_{acc['session_name']}")] for acc in accounts]
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_edit_reaction_menu")])
    await try_edit_message(query, f"Отметьте аккаунты для чата <b>{html.escape(target['chat_name'])}</b>:",
                           InlineKeyboardMarkup(keyboard))
    return EDIT_REACTION_TARGET_ASSIGN_ACCOUNTS


@owner_only
async def toggle_reaction_account_assignment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    session_name = query.data.split('_')[-1]
    target_index = context.user_data['edit_reaction_target_index']
    settings = get_data(SETTINGS_FILE, {})
    target = settings['reaction_targets'][target_index]
    assigned_accounts = set(target.get('assigned_accounts', []))

    if session_name in assigned_accounts:
        assigned_accounts.remove(session_name)
    else:
        assigned_accounts.add(session_name)

    settings['reaction_targets'][target_index]['assigned_accounts'] = list(assigned_accounts)
    save_data(SETTINGS_FILE, settings)
    await query.answer()
    return await assign_reaction_accounts_menu(update, context)


@owner_only
async def edit_reaction_count_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("1", callback_data="set_reaction_count_1"),
         InlineKeyboardButton("2", callback_data="set_reaction_count_2"),
         InlineKeyboardButton("3", callback_data="set_reaction_count_3")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_edit_reaction_menu")]
    ])
    await try_edit_message(query, "Выберите новое кол-во реакций:", keyboard)
    return EDIT_REACTION_TARGET_REACTION_COUNT


@owner_only
async def save_reaction_count(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    count = int(query.data.split('_')[-1])
    target_index = context.user_data['edit_reaction_target_index']
    settings = get_data(SETTINGS_FILE, {})
    settings['reaction_targets'][target_index]['reaction_count'] = count
    save_data(SETTINGS_FILE, settings)
    await query.answer(f"Кол-во реакций изменено на {count}", show_alert=True)
    return await edit_reaction_target_menu_logic(update, context)


@owner_only
async def edit_reaction_list_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "Введите новый список реакций через пробел (👍 ❤️ 🔥):", None)
    return EDIT_REACTION_TARGET_GET_REACTIONS


@owner_only
async def save_reaction_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reactions = update.message.text.strip().split()
    if not reactions:
        await update.message.reply_text("Список реакций не может быть пустым.")
        return EDIT_REACTION_TARGET_GET_REACTIONS

    target_index = context.user_data['edit_reaction_target_index']
    settings = get_data(SETTINGS_FILE, {})
    settings['reaction_targets'][target_index]['reactions'] = reactions
    save_data(SETTINGS_FILE, settings)
    await update.message.delete()
    await context.bot.send_message(update.effective_chat.id, "✅ Список реакций обновлен.")
    return await edit_reaction_target_menu_logic(update, context, from_callback=False)


@owner_only
async def edit_reaction_delays_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "🕒 Введите новую паузу после поста (в сек):", None)
    return EDIT_REACTION_DELAYS_INITIAL


@owner_only
async def edit_reaction_delays_get_initial(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        delay = int(update.message.text)
        target_index = context.user_data['edit_reaction_target_index']
        settings = get_data(SETTINGS_FILE, {})
        settings['reaction_targets'][target_index]['initial_reaction_delay'] = delay
        save_data(SETTINGS_FILE, settings)
        await update.message.reply_text(
            "✅ Пауза после поста обновлена.\n\n🕒 Введите новую паузу между реакциями (в сек):")
        return EDIT_REACTION_DELAYS_BETWEEN
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return EDIT_REACTION_DELAYS_INITIAL


@owner_only
async def edit_reaction_delays_get_between(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        delay = int(update.message.text)
        target_index = context.user_data['edit_reaction_target_index']
        settings = get_data(SETTINGS_FILE, {})
        settings['reaction_targets'][target_index]['delay_between_reactions'] = delay
        save_data(SETTINGS_FILE, settings)
        await update.message.delete()
        await context.bot.send_message(update.effective_chat.id, "✅ Паузы обновлены.")
        return await edit_reaction_target_menu_logic(update, context, from_callback=False)
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return EDIT_REACTION_DELAYS_BETWEEN


@owner_only
async def edit_reaction_limit_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "📊 Введите новый суточный лимит постов для реакций:", None)
    return EDIT_REACTION_TARGET_DAILY_LIMIT


@owner_only
async def edit_reaction_target_get_daily_limit(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        limit = int(update.message.text)
        target_index = context.user_data['edit_reaction_target_index']
        settings = get_data(SETTINGS_FILE, {})
        settings['reaction_targets'][target_index]['daily_reaction_limit'] = limit
        save_data(SETTINGS_FILE, settings)
        await update.message.delete()
        await context.bot.send_message(update.effective_chat.id, "✅ Лимит обновлен.")
        return await edit_reaction_target_menu_logic(update, context, from_callback=False)
    except (ValueError, TypeError):
        await update.message.reply_text("Нужно ввести целое число.")
        return EDIT_REACTION_TARGET_DAILY_LIMIT


@owner_only
async def delete_reaction_target_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    page = 0
    if query.data and query.data.startswith("delete_reaction_target_start_"):
        page = int(query.data.split('_')[-1])

    settings = get_data(SETTINGS_FILE, {})
    targets = settings.get('reaction_targets', [])

    if not targets:
        await try_edit_message(query, "Нет чатов для удаления.", InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Назад", callback_data="back_to_reaction_targets")]]))
        return REACTION_TARGETS_MENU

    indexed_targets = list(enumerate(targets))
    paginated_targets, total_items = get_paginated_items(indexed_targets, page)

    text = f"<b>❌ Выберите чат для удаления (Стр. {page + 1})</b>"

    keyboard_buttons = []
    for original_index, target_data in paginated_targets:
        keyboard_buttons.append(
            [InlineKeyboardButton(f"❌ {html.escape(target_data.get('chat_name', target_data['chat_id']))}",
                                  callback_data=f"del_reaction_target_{original_index}")])

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"delete_reaction_target_start_{page - 1}"))
    if (page + 1) * ITEMS_PER_PAGE < total_items:
        nav_buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"delete_reaction_target_start_{page + 1}"))

    if nav_buttons:
        keyboard_buttons.append(nav_buttons)

    keyboard_buttons.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_reaction_targets")])
    await try_edit_message(query, text, InlineKeyboardMarkup(keyboard_buttons))

    return GET_REACTION_TARGET_TO_DELETE


@owner_only
async def delete_reaction_target_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    target_index = int(query.data.split('_')[-1])
    settings = get_data(SETTINGS_FILE, {})

    if 0 <= target_index < len(settings.get('reaction_targets', [])):
        removed = settings['reaction_targets'].pop(target_index)
        save_data(SETTINGS_FILE, settings)
        await query.answer(f"✅ Чат для реакций '{removed.get('chat_name')}' удален.", show_alert=True)
    else:
        await query.answer("Ошибка: неверный индекс чата.", show_alert=True)

    context.user_data.clear()
    return await start(update, context)


def get_logs_for_period_from_db(period: str, page: int = 0, items_per_page: int = 5) -> tuple:
    if period == 'day':
        period_filter = "timestamp >= datetime('now', '-1 day', 'localtime')"
    elif period == 'week':
        period_filter = "timestamp >= datetime('now', '-7 days', 'localtime')"
    else:
        period_filter = "timestamp >= datetime('now', '-30 days', 'localtime')"

    offset = page * items_per_page

    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            count_query = f"""
                SELECT COUNT(*) 
                FROM (
                    SELECT 1 
                    FROM logs 
                    WHERE {period_filter} 
                    GROUP BY post_id, account_session_name, content
                )
            """
            cursor.execute(count_query)
            total_items = cursor.fetchone()[0]

            data_query = f"""
                SELECT * FROM logs 
                WHERE {period_filter} 
                GROUP BY post_id, account_session_name, content
                ORDER BY timestamp DESC 
                LIMIT ? OFFSET ?
            """
            cursor.execute(data_query, (items_per_page, offset))
            rows = cursor.fetchall()

    except sqlite3.Error as e:
        logger.error(f"Ошибка при чтении логов из БД: {e}")
        return [], 0

    formatted_logs = []
    for row in rows:
        log_entry = {
            'type': row['log_type'], 'date': row['timestamp'], 'post_id': row['post_id'],
            'target': {'chat_name': row['channel_name'], 'chat_username': row['channel_username'],
                       'channel_id': row['source_channel_id'], 'destination_chat_id': row['destination_chat_id']},
            'account': {'session_name': row['account_session_name'], 'first_name': row['account_first_name'],
                        'username': row['account_username']}
        }
        if row['log_type'] == 'reaction':
            log_entry['reactions'] = row['content'].split(' ') if row['content'] else []
        else:
            log_entry['comment'] = row['content']
        formatted_logs.append(log_entry)

    return formatted_logs, total_items

@owner_only
async def stats_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 За день", callback_data="stats_show_day")],
        [InlineKeyboardButton("📊 За неделю", callback_data="stats_show_week")],
        [InlineKeyboardButton("📊 За месяц", callback_data="stats_show_month")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
    ])
    await try_edit_message(query, "Выберите период для просмотра статистики:", keyboard)
    return STATS_MENU


@owner_only
async def export_stats_to_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("⏳ Готовлю Excel-файл...")

    period = query.data.split('_')[-1]
    period_map = {'day': 'День', 'week': 'Неделя', 'month': 'Месяц'}
    period_text = period_map.get(period, '')

    all_logs, _ = await asyncio.to_thread(get_logs_for_period_from_db, period, 0, 100000)

    try:
        await query.message.delete()
    except:
        pass

    if not all_logs:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⚠️ Нет данных для экспорта за выбранный период."
        )
        return await send_main_menu(update, context)

    table_data = []
    for log in all_logs:
        log_type = "Реакция" if log.get('type') == 'reaction' else "Комментарий"

        date_str = ""
        if log.get('date'):
            try:
                dt = datetime.fromisoformat(log['date'])
                date_str = dt.astimezone(timezone(timedelta(hours=3))).strftime('%d.%m.%Y %H:%M:%S')
            except:
                date_str = str(log.get('date'))

        target_info = log.get('target', {})
        account_info = log.get('account', {})

        actor_name = f"{account_info.get('first_name', '')} {account_info.get('last_name', '')}".strip() or account_info.get(
            'session_name', '')

        content = ""
        if log_type == "Реакция":
            content = ' '.join(log.get('reactions', []))
        else:
            content = log.get('comment', '')

        post_id = log.get('post_id', 'N/A')

        channel_username = target_info.get('chat_username')
        if channel_username:
            post_link = f"https://t.me/{channel_username}/{post_id}"
        else:
            chat_id_clean = str(target_info.get('channel_id', '')).replace('-100', '')
            post_link = f"https://t.me/c/{chat_id_clean}/{post_id}"

        table_data.append([
            log_type, date_str, target_info.get('chat_name', ''), actor_name,
            account_info.get('username', ''), post_id, content, post_link
        ])

    header = ['Тип', 'Дата (МСК)', 'Канал', 'Исполнитель', 'Юзернейм', 'ID Поста', 'Текст', 'Ссылка на пост']
    df = pd.DataFrame(table_data, columns=header)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='Статистика')
        worksheet = writer.sheets['Статистика']
        for i, col in enumerate(df.columns):
            max_len = 0
            column = df[col]
            for val in column:
                length = len(str(val))
                if length > max_len: max_len = length

            final_width = min(max_len + 2, 50)
            worksheet.column_dimensions[get_column_letter(i + 1)].width = final_width

    output.seek(0)

    today_str = datetime.now().strftime('%Y-%m-%d')
    file_name = f"stats_{period}_{today_str}.xlsx"

    await context.bot.send_document(
        chat_id=update.effective_chat.id,
        document=output,
        filename=file_name,
        caption=f"📊 Ваш отчет по статистике за период «{period_text}» готов."
    )

    return await send_main_menu(update, context)


@owner_only
async def show_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("⏳ Собираю статистику...", show_alert=False)
    data = query.data.split('_')
    action = data[1]

    if action == "show":
        period, page = data[2], 0
    else:
        page, period = int(data[2]), data[3]

    period_map = {'day': 'последний день', 'week': 'последнюю неделю', 'month': 'последний месяц'}
    period_text = period_map.get(period, '')

    paginated_logs, total_items = await asyncio.to_thread(get_logs_for_period_from_db, period, page, ITEMS_PER_PAGE)

    stats_text = f"<b>📊 Статистика за {period_text} (Страница {page + 1})</b>\n\n"

    if not paginated_logs:
        stats_text += "⚠️ Нет данных за выбранный период.\n"
        reply_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ В меню статистики", callback_data="back_to_stats_menu")]])
    else:
        for log in paginated_logs:
            target_info, account_info = log.get('target', {}), log.get('account', {})
            post_id, log_type = log.get('post_id', 'N/A'), log.get('type', 'comment')

            channel_username = target_info.get('chat_username')
            if channel_username:
                post_link = f"https://t.me/{channel_username}/{post_id}"
            else:
                chat_id_clean = str(target_info.get('channel_id', '')).replace('-100', '')
                post_link = f"https://t.me/c/{chat_id_clean}/{post_id}"

            post_link_html = f'<a href="{post_link}">посту №{post_id}</a>' if post_link else f"посту №{post_id}"
            channel_name = html.escape(target_info.get('chat_name', 'Неизвестно'))
            channel_user_display = f"(@{channel_username})" if channel_username else ""

            actor_name = html.escape(
                f"{account_info.get('first_name', '')} {account_info.get('last_name', '')}".strip() or f"Аккаунт {account_info.get('session_name', '')}")
            actor_user = f"@{account_info.get('username')}" if account_info.get('username') else "нет"

            date_str = "Дата отсутствует"
            if log.get('date'):
                try:
                    dt = datetime.fromisoformat(log['date'])
                    date_str = dt.astimezone(timezone(timedelta(hours=3))).strftime('%d.%m.%Y, %H:%M')
                except:
                    date_str = log['date']

            if log_type == 'comment' or log_type == 'comment_reply':
                comment_text = log.get('comment', '')
                truncated_comment = (comment_text[:250] + '...') if len(comment_text) > 250 else comment_text
                icon = "💬" if log_type == 'comment' else "🗣"
                stats_text += (f"{icon} <b>Комментарий к {post_link_html}</b>\n"
                               f"   - <b>Канал:</b> {channel_name} {channel_user_display}\n"
                               f"   - <b>Комментатор:</b> {actor_name} ({actor_user})\n"
                               f"   - <b>Сообщение:</b> <i>{html.escape(truncated_comment)}</i>\n"
                               f"   - <b>Дата:</b> <code>{date_str}</code>\n"
                               "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
            elif log_type == 'reaction':
                actual_reactions = ' '.join(log.get('reactions', []))
                stats_text += (f"👍 <b>Реакция к {post_link_html}</b>\n"
                               f"   - <b>Канал:</b> {channel_name} {channel_user_display}\n"
                               f"   - <b>Исполнитель:</b> {actor_name} ({actor_user})\n"
                               f"   - <b>Реакция:</b> {html.escape(actual_reactions)}\n"
                               f"   - <b>Дата:</b> <code>{date_str}</code>\n"
                               "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")

        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"stats_page_{page - 1}_{period}"))

        total_pages = (total_items + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"stats_page_{page + 1}_{period}"))

        keyboard_buttons = [nav_buttons] if nav_buttons else []
        keyboard_buttons.extend([
            [InlineKeyboardButton("📤 Выгрузить в .xlsx", callback_data=f"export_csv_{period}")],
            [InlineKeyboardButton("⬅️ В меню статистики", callback_data="back_to_stats_menu")]
        ])
        reply_markup = InlineKeyboardMarkup(keyboard_buttons)

    await try_edit_message(query, stats_text, reply_markup)
    return STATS_MENU


@owner_only
async def edit_reply_chance_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['current_setting_state'] = 'chance'
    await query.message.reply_text("Введите вероятность ответа (число от 0 до 100):")
    return EDIT_REPLY_CHANCE


@owner_only
async def edit_reply_delay_min_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['current_setting_state'] = 'min'
    await query.message.reply_text("Введите минимальную задержку перед ответом (в секундах):")
    return EDIT_REPLY_DELAY_MIN


@owner_only
async def edit_reply_depth_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['current_setting_state'] = 'depth'
    await query.message.reply_text(
        "📜 Введите глубину контекста (сколько последних сообщений помнить).\n"
        "Рекомендуется от 5 до 15. Введите число:"
    )
    return EDIT_REPLY_DEPTH


@owner_only
async def edit_reply_delay_max_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['current_setting_state'] = 'max'
    await query.message.reply_text("Введите максимальную задержку перед ответом (в секундах):")
    return EDIT_REPLY_DELAY_MAX


@owner_only
async def personas_menu_logic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    page = 0
    if query:
        await query.answer()
        if query.data and query.data.startswith("personas_menu_"):
            page = int(query.data.split('_')[-1])

    context.user_data['selected_sessions'] = []
    settings = get_data(SETTINGS_FILE, {})
    personas = settings.get('personas', {})
    persona_list = list(personas.items())
    paginated_personas, total_items = get_paginated_items(persona_list, page)

    text = f"<b>🎭 Управление ролями (Personas) (Стр. {page + 1})</b>\n\nЗдесь вы можете создать шаблоны поведения и промпты, которые затем можно назначить сразу нескольким аккаунтам."

    keyboard = []
    for p_id, p_data in paginated_personas:
        keyboard.append([InlineKeyboardButton(f"⚙️ {p_data['name']}", callback_data=f"edit_persona_{p_id}_0")])

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"personas_menu_{page - 1}"))
    if (page + 1) * ITEMS_PER_PAGE < total_items:
        nav_buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"personas_menu_{page + 1}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton("➕ Создать новую роль", callback_data="add_persona_start")])
    keyboard.append([InlineKeyboardButton("❌ Удалить роль", callback_data="delete_persona_start_0")])
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")])

    reply_markup = InlineKeyboardMarkup(keyboard)

    if query:
        await try_edit_message(query, text, reply_markup)
    else:
        await context.bot.send_message(update.effective_chat.id, text, reply_markup=reply_markup, parse_mode='HTML')

    return PERSONAS_MENU


@owner_only
async def add_persona_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "Введите короткое название для роли (например: <code>Скептик</code>):", None)
    return ADD_PERSONA_NAME


@owner_only
async def add_persona_get_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['new_persona_name'] = update.message.text.strip()
    await update.message.reply_text(
        "Теперь введите системный промпт для этой роли. Опишите стиль общения, лексику и характер:")
    return ADD_PERSONA_PROMPT


@owner_only
async def add_persona_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    prompt = update.message.text.strip()
    name = context.user_data['new_persona_name']

    settings = get_data(SETTINGS_FILE, {})
    if 'personas' not in settings:
        settings['personas'] = {}

    persona_id = str(int(time.time()))
    settings['personas'][persona_id] = {
        'name': name,
        'prompt': prompt
    }

    save_data(SETTINGS_FILE, settings)
    await update.message.reply_text(f"✅ Роль «{name}» успешно создана.")
    return await personas_menu_logic(update, context)


@owner_only
async def assign_persona_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data_parts = query.data.split('_')

    if len(data_parts) >= 4 and data_parts[1] == 'persona':
        persona_id = data_parts[2]
        page = int(data_parts[3])
        context.user_data['active_persona_id'] = persona_id
        context.user_data['current_persona_page'] = page
    else:
        persona_id = context.user_data.get('active_persona_id')
        page = context.user_data.get('current_persona_page', 0)

    if not persona_id:
        return await personas_menu_logic(update, context)

    accounts = get_data(ACCOUNTS_FILE, [])
    settings = get_data(SETTINGS_FILE, {})

    if persona_id not in settings.get('personas', {}):
        return await personas_menu_logic(update, context)

    persona_name = settings['personas'][persona_id]['name']
    selected_sessions = context.user_data.get('selected_sessions', [])
    paginated_accs, total_items = get_paginated_items(accounts, page)

    text = f"<b>👥 Назначение роли: {persona_name} (Стр. {page + 1})</b>\n\nВыберите аккаунты для присвоения роли:"
    keyboard = []

    for acc in paginated_accs:
        session_name = acc['session_name']
        current_p_id = acc.get('persona_id')
        status_icon = "✅" if session_name in selected_sessions else "➖"
        display_p = "Нет"
        if current_p_id and current_p_id in settings.get('personas', {}):
            display_p = settings['personas'][current_p_id]['name']

        keyboard.append([InlineKeyboardButton(
            f"{status_icon} {session_name} (сейчас: {display_p})",
            callback_data=f"tg_ps_{session_name}_{page}"
        )])

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"edit_persona_{persona_id}_{page - 1}"))
    if (page + 1) * ITEMS_PER_PAGE < total_items:
        nav_buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"edit_persona_{persona_id}_{page + 1}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton("💾 Сохранить и применить", callback_data="save_persona_batch")])
    keyboard.append([InlineKeyboardButton("⬅️ Назад к ролям", callback_data="personas_menu_0")])

    await try_edit_message(query, text, InlineKeyboardMarkup(keyboard))
    return SELECT_ACCOUNTS_FOR_PERSONA


@owner_only
async def save_persona_batch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    persona_id = context.user_data.get('active_persona_id')
    selected_sessions = context.user_data.get('selected_sessions', [])

    if not selected_sessions:
        await query.answer("Вы не выбрали ни одного аккаунта!", show_alert=True)
        return SELECT_ACCOUNTS_FOR_PERSONA

    settings = get_data(SETTINGS_FILE, {})
    persona_prompt = settings['personas'][persona_id]['prompt']

    accounts = get_data(ACCOUNTS_FILE, [])
    for acc in accounts:
        if acc['session_name'] in selected_sessions:
            acc['persona_id'] = persona_id

    save_data(ACCOUNTS_FILE, accounts)

    for target in settings.get('targets', []):
        if 'prompts' not in target:
            target['prompts'] = {}
        for session in selected_sessions:
            target['prompts'][session] = persona_prompt

    save_data(SETTINGS_FILE, settings)
    await query.answer("✅ Роль успешно назначена выбранным аккаунтам!", show_alert=True)
    return await personas_menu_logic(update, context)


@owner_only
async def toggle_persona_account_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data_parts = query.data.split('_')
    session_name = data_parts[2]
    page = int(data_parts[3]) if len(data_parts) > 3 else 0

    selected = context.user_data.get('selected_sessions', [])
    if session_name in selected:
        selected.remove(session_name)
    else:
        selected.append(session_name)

    context.user_data['selected_sessions'] = selected
    context.user_data['current_persona_page'] = page

    await query.answer()
    return await assign_persona_menu(update, context)


@owner_only
async def delete_persona_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    page = 0
    if query.data and query.data.startswith("delete_persona_start_"):
        page = int(query.data.split('_')[-1])

    settings = get_data(SETTINGS_FILE, {})
    personas = settings.get('personas', {})

    if not personas:
        await try_edit_message(query, "Нет созданных ролей.", InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Назад", callback_data="personas_menu_0")]]))
        return PERSONAS_MENU

    persona_list = list(personas.items())
    paginated_personas, total_items = get_paginated_items(persona_list, page)

    text = f"<b>❌ Выберите роль для удаления (Стр. {page + 1})</b>"
    keyboard = []

    for p_id, p_data in paginated_personas:
        keyboard.append([InlineKeyboardButton(f"❌ {p_data['name']}", callback_data=f"confirm_del_persona_{p_id}")])

    nav_buttons = []
    if page > 0:
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"delete_persona_start_{page - 1}"))
    if (page + 1) * ITEMS_PER_PAGE < total_items:
        nav_buttons.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f"delete_persona_start_{page + 1}"))

    if nav_buttons:
        keyboard.append(nav_buttons)

    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="personas_menu_0")])
    await try_edit_message(query, text, InlineKeyboardMarkup(keyboard))
    return GET_PERSONA_TO_DELETE


@owner_only
async def delete_persona_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    persona_id = query.data.split('_')[-1]

    settings = get_data(SETTINGS_FILE, {})
    accounts = get_data(ACCOUNTS_FILE, [])

    if persona_id in settings.get('personas', {}):
        removed = settings['personas'].pop(persona_id)
        save_data(SETTINGS_FILE, settings)

        updated_accounts = False
        for acc in accounts:
            if acc.get('persona_id') == persona_id:
                del acc['persona_id']
                updated_accounts = True

        if updated_accounts:
            save_data(ACCOUNTS_FILE, accounts)

        await query.answer(f"✅ Роль '{removed['name']}' удалена.", show_alert=True)
    else:
        await query.answer("Ошибка: роль не найдена.", show_alert=True)

    return await personas_menu_logic(update, context)


@owner_only
async def edit_reply_chance_call(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['current_setting_state'] = 'chance'
    await query.message.reply_text("Введите вероятность (0-100):")
    return EDIT_REPLY_CHANCE


@owner_only
async def edit_reply_delay_min_call(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['current_setting_state'] = 'min'
    await query.message.reply_text("Введите мин. задержку (сек):")
    return EDIT_REPLY_DELAY_MIN


@owner_only
async def edit_reply_delay_max_call(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data['current_setting_state'] = 'max'
    await query.message.reply_text("Введите макс. задержку (сек):")
    return EDIT_REPLY_DELAY_MAX


@owner_only
async def rebrand_menu_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    text = "<b>🎭 Ребрендинг всей сетки</b>\n\nЭта функция автоматически изменит имена и аватарки всех активных аккаунтов под выбранную тематику."
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Запустить ребрендинг", callback_data="rebrand_run")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
    ])
    await try_edit_message(query, text, keyboard)
    return REBRAND_MENU


@owner_only
async def rebrand_get_topic_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "Введите тематику для новых профилей (например: <code>Крипта</code>, <code>Спорт</code>, <code>Девушки</code>):", None)
    return REBRAND_GET_TOPIC


@owner_only
async def rebrand_save_topic(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['rebrand_topic'] = update.message.text.strip()
    await update.message.reply_text(
        "<b>Откуда брать аватарки?</b>\n\n"
        "1. Отправьте <b>@username</b> канала (бот возьмет фото оттуда).\n"
        "2. ИЛИ отправьте <b>ключевое слово</b> (например: <code>Crypto</code>, <code>Girls</code>, <code>Cars</code>)  бот сгенерирует уникальные аватары через AI.\n\n"
        "Жду ввод:",
        parse_mode='HTML'
    )
    return REBRAND_GET_SOURCE


@owner_only
async def rebrand_run_task(update: Update, context: ContextTypes.DEFAULT_TYPE):
    source_input = update.message.text.strip()
    topic = context.user_data.get('rebrand_topic')

    is_channel = source_input.startswith('@') or "t.me/" in source_input

    final_source = source_input.replace('@', '') if is_channel else source_input

    settings = get_data(SETTINGS_FILE)
    settings['rebrand_task'] = {
        'topic': topic,
        'source_value': final_source,
        'is_channel': is_channel,
        'status': 'pending'
    }
    save_data(SETTINGS_FILE, settings)

    source_type_text = f"Канал @{final_source}" if is_channel else f"Генерация по теме '{final_source}'"

    await update.message.reply_text(
        f"✅ Задача на ребрендинг создана!\n"
        f"🎭 Тема имен: {topic}\n"
        f"🖼 Аватарки: {source_type_text}\n\n"
        f"Скрипт commentator.py начнет обновление аккаунтов в течение 10-15 секунд."
    )
    return await start(update, context)


@owner_only
async def proxies_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    if query:
        await query.answer()

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM proxies")
        total_proxies = cursor.fetchone()[0]

    text = (f"<b>🌐 Управление прокси</b>\n\n"
            f"Всего в базе: <b>{total_proxies}</b>\n\n"
            f"Формат для добавления:\n"
            f"<code>protocol://user:password@ip:port</code>\n\n"
            f"Поддерживаются: socks5, http, https")

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Список прокси", callback_data="my_proxies_list_0")],
        [InlineKeyboardButton("➕ Добавить прокси", callback_data="add_proxies_start")],
        [InlineKeyboardButton("🔍 Проверить все", callback_data="check_all_proxies")],
        [InlineKeyboardButton("🗑️ Удалить нерабочие", callback_data="delete_dead_proxies")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_main")]
    ])

    if query:
        await try_edit_message(query, text, keyboard)
    else:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=text,
            reply_markup=keyboard,
            parse_mode='HTML'
        )
    return PROXIES_MENU


@owner_only
async def add_proxies_get_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    input_text = update.message.text.strip().split('\n')
    added_count = 0
    duplicate_count = 0

    await update.message.reply_text(f"⏳ Начинаю импорт и проверку {len(input_text)} прокси...")

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        for line in input_text:
            url = line.strip()
            if not url: continue

            try:
                check_res = await check_proxy_health(url)
                cursor.execute(
                    "INSERT INTO proxies (url, ip, country, status, last_check) VALUES (?, ?, ?, ?, ?)",
                    (url, check_res['ip'], check_res['country'], check_res['status'], datetime.now().isoformat())
                )
                added_count += 1
            except sqlite3.IntegrityError:
                duplicate_count += 1
            except Exception as e:
                logger.error(f"Error adding proxy {url}: {e}")
        conn.commit()

    await update.message.reply_text(
        f"✅ Результат импорта:\n"
        f"Добавлено: {added_count}\n"
        f"Дубликаты: {duplicate_count}"
    )
    return await proxies_menu(update, context)


@owner_only
async def show_my_proxies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()

    page = 0
    if query.data and query.data.startswith("my_proxies_list_"):
        try:
            page = int(query.data.split('_')[-1])
        except ValueError:
            page = 0

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, url, ip, country, status FROM proxies LIMIT 10 OFFSET ?", (page * 10,))
        proxies = cursor.fetchall()
        cursor.execute("SELECT COUNT(*) FROM proxies")
        total = cursor.fetchone()[0]

    text = f"<b>📋 Список прокси (Стр. {page + 1})</b>\n\n"
    keyboard = []
    for p_id, url, ip, country, status in proxies:
        status_icon = "🟢" if status == "active" else "🔴"
        display_name = ip if ip else url.split('@')[-1] if '@' in url else url[:20]
        btn_text = f"{status_icon} {display_name} ({country if country else '??'})"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"proxy_actions_{p_id}")])

    nav_btns = []
    if page > 0:
        nav_btns.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"my_proxies_list_{page - 1}"))
    if (page + 1) * 10 < total:
        nav_btns.append(InlineKeyboardButton("Вперед ➡️", callback_data=f"my_proxies_list_{page + 1}"))
    if nav_btns:
        keyboard.append(nav_btns)

    keyboard.append([InlineKeyboardButton("⬅️ В меню прокси", callback_data="proxies_menu")])

    await try_edit_message(query, text, InlineKeyboardMarkup(keyboard))
    return PROXIES_MENU


@owner_only
async def check_all_proxies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await try_edit_message(query, "⏳ <b>Начинаю полную проверку прокси...</b>\nЭто может занять время.", None)

    active_count = 0
    dead_count = 0

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, url FROM proxies")
        rows = cursor.fetchall()

        total = len(rows)

        for i, (p_id, url) in enumerate(rows):
            if i % 5 == 0:
                await try_edit_message(query, f"⏳ Проверка: {i}/{total}...", None)

            res = await check_proxy_health(url)
            cursor.execute("UPDATE proxies SET status = ?, ip = ?, country = ?, last_check = ? WHERE id = ?",
                           (res['status'], res['ip'], res['country'], datetime.now().isoformat(), p_id))

            if res['status'] == 'active':
                active_count += 1
            else:
                dead_count += 1

        conn.commit()

    text = (f"<b>✅ Проверка завершена!</b>\n\n"
            f"🟢 Рабочих: <b>{active_count}</b>\n"
            f"🔴 Нерабочих: <b>{dead_count}</b>")

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ Вернуться в меню", callback_data="proxies_menu")]
    ])

    await try_edit_message(query, text, keyboard)
    return PROXIES_MENU


@owner_only
async def select_proxy_for_account(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    page = 0
    if "page_" in query.data:
        page = int(query.data.split('_')[-1])

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, url, ip, country FROM proxies WHERE status = 'active' LIMIT 10 OFFSET ?", (page * 10,))
        proxies = cursor.fetchall()
        cursor.execute("SELECT COUNT(*) FROM proxies WHERE status = 'active'")
        total = cursor.fetchone()[0]

    text = "<b>🌐 Выберите рабочий прокси для аккаунта:</b>"
    keyboard = []
    for p_id, url, ip, country in proxies:
        label = f"🔗 {ip} ({country})"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"set_proxy_id_{p_id}")])

    nav = []
    if page > 0: nav.append(InlineKeyboardButton("⬅️", callback_data=f"sel_proxy_page_{page - 1}"))
    if (page + 1) * 10 < total: nav.append(InlineKeyboardButton("➡️", callback_data=f"sel_proxy_page_{page + 1}"))
    if nav: keyboard.append(nav)

    keyboard.append([InlineKeyboardButton("🚫 Без прокси (отвязать)", callback_data="set_proxy_id_none")])
    keyboard.append([InlineKeyboardButton("⬅️ Отмена", callback_data="accounts_menu")])

    await try_edit_message(query, text, InlineKeyboardMarkup(keyboard))
    return SELECT_PROXY_FOR_ACCOUNT


@owner_only
async def add_proxies_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "Введите прокси (один или несколько с новой строки) в формате:\n\n<code>protocol://user:password@ip:port</code>", None)
    return ADD_PROXIES_INPUT


@owner_only
async def add_proxies_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lines = update.message.text.strip().split('\n')
    msg = await update.message.reply_text(f"⏳ Проверяю {len(lines)} прокси...")

    added, duplicates = 0, 0
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        for url in lines:
            url = url.strip()
            if not url: continue
            try:
                res = await check_proxy_health(url)
                cursor.execute(
                    "INSERT INTO proxies (url, ip, country, status, last_check) VALUES (?, ?, ?, ?, ?)",
                    (url, res['ip'], res['country'], res['status'], datetime.now().isoformat())
                )
                added += 1
            except sqlite3.IntegrityError:
                duplicates += 1
        conn.commit()

    await msg.edit_text(f"✅ Добавлено: {added}\n❌ Дубликатов: {duplicates}\n\nПроверка завершена.")
    return await proxies_menu(update, context)


@owner_only
async def delete_dead_proxies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT url FROM proxies WHERE status = 'dead'")
        dead_urls = [r[0] for r in cursor.fetchall()]
        cursor.execute("DELETE FROM proxies WHERE status = 'dead'")
        conn.commit()

    accounts = get_data(ACCOUNTS_FILE, [])
    updated = False
    for acc in accounts:
        if acc.get('proxy_url') in dead_urls:
            acc['proxy_url'] = None
            updated = True
    if updated:
        save_data(ACCOUNTS_FILE, accounts)

    await query.answer(f"Удалено нерабочих прокси: {len(dead_urls)}", show_alert=True)
    return await proxies_menu(update, context)


@owner_only
async def save_account_proxy_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data.split('_')[-1]
    proxy_url = None

    if data != "none":
        try:
            proxy_id = int(data)
            with sqlite3.connect(DB_FILE) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT url FROM proxies WHERE id = ?", (proxy_id,))
                row = cursor.fetchone()
                if row:
                    proxy_url = row[0]
                else:
                    await query.message.edit_text("❌ Ошибка: этот прокси больше не существует в базе.")
                    await asyncio.sleep(2)
                    return await show_my_accounts(update, context)
        except ValueError:
             pass

    acc_index = context.user_data.get('edit_acc_index')
    accounts = get_data(ACCOUNTS_FILE, [])

    if acc_index is not None and acc_index < len(accounts):
        accounts[acc_index]['proxy_url'] = proxy_url
        save_data(ACCOUNTS_FILE, accounts)

        status_text = f"привязан" if proxy_url else "отвязан"
        await query.message.edit_text(
            f"✅ Прокси для аккаунта <b>{html.escape(accounts[acc_index]['session_name'])}</b> успешно {status_text}.",
            parse_mode='HTML'
        )

    await asyncio.sleep(2)
    return await show_my_accounts(update, context)


@owner_only
async def proxy_action_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    proxy_id = int(query.data.split('_')[-1])

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT url, ip, country, status, last_check FROM proxies WHERE id = ?", (proxy_id,))
        proxy = cursor.fetchone()

    if not proxy:
        await try_edit_message(query, "❌ Прокси не найден (возможно, был удален).",
                               InlineKeyboardMarkup(
                                   [[InlineKeyboardButton("⬅️ Назад", callback_data="my_proxies_list_0")]]))
        return PROXIES_MENU

    url, ip, country, status, last_check = proxy
    status_icon = "🟢 Активен" if status == "active" else "🔴 Не работает"

    text = (f"<b>⚙️ Управление прокси</b>\n\n"
            f"🔗 <b>URL:</b> <code>{url}</code>\n"
            f"🌍 <b>IP:</b> {ip}\n"
            f"🏳️ <b>Страна:</b> {country}\n"
            f"📊 <b>Статус:</b> {status_icon}\n"
            f"🕒 <b>Последняя проверка:</b> {last_check}")

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Проверить снова", callback_data=f"pr_act_chk_{proxy_id}")],
        [InlineKeyboardButton("❌ Удалить", callback_data=f"pr_act_del_{proxy_id}")],
        [InlineKeyboardButton("⬅️ Назад к списку", callback_data="my_proxies_list_0")]
    ])

    await try_edit_message(query, text, keyboard)
    return PROXIES_MENU


@owner_only
async def process_proxy_specific_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    parts = data.split('_')
    action = parts[2]
    proxy_id = int(parts[-1])

    if action == "del":
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute("DELETE FROM proxies WHERE id = ?", (proxy_id,))
            conn.commit()
        await query.answer("✅ Прокси удален!", show_alert=True)

        return await show_my_proxies(update, context)

    elif action == "chk":
        await query.answer("⏳ Проверяю...")
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT url FROM proxies WHERE id = ?", (proxy_id,))
            row = cursor.fetchone()
            if row:
                url = row[0]
                res = await check_proxy_health(url)
                cursor.execute("UPDATE proxies SET status = ?, ip = ?, country = ?, last_check = ? WHERE id = ?",
                               (res['status'], res['ip'], res['country'], datetime.now().isoformat(), proxy_id))
                conn.commit()

                status_text = "✅ Рабочий" if res['status'] == 'active' else "🔴 Не работает"
                await query.answer(f"Результат: {status_text}\nIP: {res['ip']}", show_alert=True)

        return await proxy_action_menu(update, context)


@owner_only
async def edit_reaction_chance_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "🎲 Введите вероятность срабатывания реакции (число от 0 до 100):", None)
    return EDIT_REACTION_CHANCE


@owner_only
async def save_reaction_chance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        chance = int(update.message.text.strip())
        if not (0 <= chance <= 100):
            raise ValueError
    except (ValueError, TypeError):
        await update.message.reply_text("❌ Введите целое число от 0 до 100.")
        return EDIT_REACTION_CHANCE

    target_index = context.user_data['edit_reaction_target_index']
    settings = get_data(SETTINGS_FILE, {})
    settings['reaction_targets'][target_index]['reaction_chance'] = chance
    save_data(SETTINGS_FILE, settings)

    await update.message.reply_text(f"✅ Вероятность реакции установлена: {chance}%")
    return await edit_reaction_target_menu_logic(update, context, from_callback=False)


@owner_only
async def blacklist_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()

    settings = get_data(SETTINGS_FILE, {})
    blacklist = settings.get('blacklist', [])

    blacklist_text = ", ".join(blacklist) if blacklist else "Пусто"
    if len(blacklist_text) > 300:
        blacklist_text = blacklist_text[:300] + "..."

    text = (f"<b>🚫 Чёрный список слов</b>\n\n"
            f"Бот <b>не будет</b> использовать эти слова в генерации. Если они попадутся он перепишет ответ.\n\n"
            f"<b>Текущий список:</b>\n{html.escape(blacklist_text)}")

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ Добавить слова", callback_data="add_blacklist_start")],
        [InlineKeyboardButton("🗑️ Удалить слова", callback_data="delete_blacklist_start")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="settings_menu")]
    ])

    if query:
        await try_edit_message(query, text, keyboard)
    else:
        await context.bot.send_message(update.effective_chat.id, text, reply_markup=keyboard, parse_mode='HTML')
    return BLACKLIST_MENU


@owner_only
async def add_blacklist_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "Введите слова для блокировки (через запятую или с новой строки):", None)
    return ADD_BLACKLIST_WORD


@owner_only
async def save_blacklist_words(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    new_words = [w.strip() for w in text.replace('\n', ',').split(',') if w.strip()]

    settings = get_data(SETTINGS_FILE, {})
    if 'blacklist' not in settings:
        settings['blacklist'] = []

    added_count = 0
    for w in new_words:
        if w.lower() not in [existing.lower() for existing in settings['blacklist']]:
            settings['blacklist'].append(w)
            added_count += 1

    save_data(SETTINGS_FILE, settings)
    await update.message.reply_text(f"✅ Добавлено слов: {added_count}")
    return await blacklist_menu(update, context)


@owner_only
async def delete_blacklist_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    settings = get_data(SETTINGS_FILE, {})
    blacklist = settings.get('blacklist', [])

    if not blacklist:
        await query.answer("Список пуст!", show_alert=True)
        return await blacklist_menu(update, context)

    keyboard = []
    for i, word in enumerate(blacklist[:20]):
        keyboard.append([InlineKeyboardButton(f"❌ {word}", callback_data=f"del_bl_word_{i}")])

    keyboard.append([InlineKeyboardButton("🗑️ ОЧИСТИТЬ ВЕСЬ СПИСОК", callback_data="del_bl_all")])
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="blacklist_menu")])

    await try_edit_message(query, "Нажмите на слово, чтобы удалить его, или очистите весь список:",
                           InlineKeyboardMarkup(keyboard))
    return DELETE_BLACKLIST_WORD


@owner_only
async def delete_blacklist_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    settings = get_data(SETTINGS_FILE, {})

    if data == "del_bl_all":
        settings['blacklist'] = []
        save_data(SETTINGS_FILE, settings)
        await query.answer("✅ Чёрный список очищен.", show_alert=True)
        return await blacklist_menu(update, context)

    if data.startswith("del_bl_word_"):
        idx = int(data.split('_')[-1])
        if 'blacklist' in settings and 0 <= idx < len(settings['blacklist']):
            removed = settings['blacklist'].pop(idx)
            save_data(SETTINGS_FILE, settings)
            await query.answer(f"✅ Слово '{removed}' удалено.")
            return await delete_blacklist_start(update, context)

    return await blacklist_menu(update, context)


@owner_only
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "<b>📚 ПОЛНОЕ РУКОВОДСТВО А-Я</b>\n\n"
        "<b>1. АККАУНТЫ И ВХОД</b>\n"
        "• <b>Добавление:</b> Введите номер. Если код не приходит это защита TG на определенных пулах номеров или прокси. Смените страну номера или провайдера прокси.\n"
        "• <b>2FA:</b> Обязательно устанавливайте облачный пароль сразу после входа.\n\n"
        "<b>2. ПРОМПТЫ И РОЛИ (PERSONAS)</b>\n"
        "• <b>Иерархия промптов:</b> Бот выбирает промпт в следующем порядке: <b>Роль (Persona) > Персональный промпт аккаунта в чате > Промпт чата по умолчанию</b>.\n"
        "• <b>Системный промпт:</b> Это ядро личности. Здесь описывается характер, стиль речи, отношение к миру и запретные темы.\n"
        "• <b>Роли (Personas):</b> Шаблоны, которые создаются один раз и назначаются любым аккаунтам. При назначении роли аккаунту, его промпты в чатах будут автоматически заменены на промпт роли.\n"
        "• <b>Персональный промпт:</b> Позволяет сделать так, чтобы один и тот же бот в разных чатах вел себя по-разному.\n\n"
        "<b>3. РЕБРЕНДИНГ (СМЕНА ОБЛИКА)</b>\n"
        "• <b>Донор:</b> Бот вступает в указанный канал, находит привязанный чат и копирует профили реальных участников (имена и аватарки) на ваши аккаунты.\n"
        "• <b>AI-генерация:</b> Бот генерирует имена через AI, а аватарки создает индивидуально для каждого аккаунта через DALL-E 3 или нейросеть Flux (Pollinations).\n\n"
        "<b>4. ВЕРОЯТНОСТИ И ДИАЛОГИ</b>\n"
        "• <b>Шанс ответа:</b> Вероятность реакции бота на любое входящее сообщение в обсуждении.\n"
        "• <b>Шанс вмешательства:</b> Шанс того, что другой ваш бот вклинится в уже начатый диалог между вашим первым ботом и реальным пользователем.\n"
        "• <b>Шанс тега/реплая:</b> Вероятность использования функции 'Ответить'. Если не сработал бот напишет просто сообщением в чат.\n"
        "• <b>Глубина:</b> Сколько предыдущих сообщений AI будет видеть для понимания контекста переписки.\n\n"
        "<b>5. ЛОГИКА AI VISION (КОМБО)</b>\n"
        "• <b>Deepseek:</b> Не имеет встроенного зрения. Если в посте есть картинка, он её не увидит.\n"
        "• <b>Режим Комбо:</b> Если у вас выбран Deepseek, но добавлен ключ <b>OpenAI</b>, бот сначала отправит картинку в OpenAI для получения текстового описания, а затем передаст это описание в Deepseek. Вы получаете дешевый текст с пониманием визуала.\n\n"
        "<b>6. ОЧЕЛОВЕЧИВАНИЕ</b>\n"
        "• <b>Lowercase:</b> Набор текста без заглавных букв.\n"
        "• <b>Опечатки:</b> Случайная перестановка букв в словах.\n"
        "• <b>Разбив:</b> Отправка одного длинного сообщения двумя частями с паузой имитации печати.\n\n"
        "<b>7. СЦЕНАРИИ И AI</b>\n"
        "• <b>Совместимость:</b> Если для чата включен сценарий (Script) и одновременно разрешены AI-ответы, бот будет параллельно выполнять обе задачи: идти по пунктам сценария и реагировать на живых людей через нейросеть.\n"
        "• <b>Триггеры:</b> Приоритет над AI. Если найдено совпадение по фразе бот ответит заготовкой.\n\n"
        "<b>8. ПАУЗЫ И ЛИМИТЫ</b>\n"
        "• <b>Пауза после поста:</b> Ожидание перед самым первым комментарием под постом.\n"
        "• <b>Пауза между аккаунтами:</b> Интервал, чтобы боты не писали одновременно.\n"
        "• <b>Мониторинг:</b> Автопоиск постов в чужих каналах по вашему ТЗ с уведомлением в ЛС."
    )
    await update.message.reply_text(text, parse_mode="HTML", **no_link_preview_kwargs())


@owner_only
async def scenario_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()

    target_index = context.user_data.get('edit_target_index')
    settings = get_data(SETTINGS_FILE, {})

    if target_index is None or target_index >= len(settings.get('targets', [])):
        if query:
            await try_edit_message(query, "❌ Ошибка контекста. Начните сначала.", None)
        else:
            await context.bot.send_message(update.effective_chat.id, "❌ Ошибка контекста. Начните сначала.")
        return TARGETS_MENU

    target = settings.get('targets', [])[target_index]
    chat_id = target.get('chat_id')

    status = "stopped"

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM scenarios WHERE chat_id = ?", (chat_id,))
        row = cursor.fetchone()
        if row:
            status = row[0]

    reply_mode = target.get('scenario_reply_mode', False)
    reply_icon = "🔗 Вкл" if reply_mode else "❌ Выкл"

    status_icon = "🟢 Работает" if status == 'running' else "🔴 Остановлен"
    toggle_cb = "scen_stop" if status == 'running' else "scen_start"
    toggle_text = "⏹️ Остановить" if status == 'running' else "▶️ Запустить"

    text = (f"<b>📜 Управление сценарием для {html.escape(target['chat_name'])}</b>\n\n"
            f"Статус: <b>{status_icon}</b>\n\n"
            f"🔗 <b>Связный диалог (Reply):</b> Если включено, боты будут технически отвечать на сообщения друг друга, создавая ветку.")

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(toggle_text, callback_data=toggle_cb)],
        [InlineKeyboardButton(f"🔗 Связный диалог: {reply_icon}", callback_data="toggle_scen_reply")],
        [InlineKeyboardButton("📤 Загрузить новый скрипт", callback_data="scen_upload")],
        [InlineKeyboardButton("⏮ Сбросить прогресс в 0", callback_data="scen_reset")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="back_to_edit_menu")]
    ])

    if query:
        await try_edit_message(query, text, keyboard)
    else:
        await context.bot.send_message(update.effective_chat.id, text, reply_markup=keyboard, parse_mode='HTML')

    return SCENARIO_MENU


@owner_only
async def toggle_scenario_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    target_index = context.user_data.get('edit_target_index')
    settings = get_data(SETTINGS_FILE, {})

    current = settings['targets'][target_index].get('scenario_reply_mode', False)
    settings['targets'][target_index]['scenario_reply_mode'] = not current
    save_data(SETTINGS_FILE, settings)

    await query.answer(f"Режим ответов {'включен' if not current else 'выключен'}")
    return await scenario_menu(update, context)


@owner_only
async def scenario_upload_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "Отправьте текстовый файл или сообщение со сценарием.", None)
    return UPLOAD_SCENARIO


@owner_only
async def save_scenario_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    target_index = context.user_data.get('edit_target_index')
    settings = get_data(SETTINGS_FILE, {})
    chat_id = settings['targets'][target_index]['chat_id']

    lines = [l.strip() for l in text.split('\n') if l.strip()]
    if not lines:
        await update.message.reply_text("❌ Пустой скрипт.")
        return await scenario_menu(update, context)

    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("""
            INSERT INTO scenarios (chat_id, script_content, current_index, status) 
            VALUES (?, ?, 0, 'stopped')
            ON CONFLICT(chat_id) DO UPDATE SET 
            script_content=excluded.script_content, 
            current_index=0, 
            status='stopped'
        """, (chat_id, text))
        conn.commit()

    await update.message.reply_text(f"✅ Сценарий сохранен. Строк: {len(lines)}")
    return await scenario_menu(update, context)


@owner_only
async def scenario_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    action = query.data
    target_index = context.user_data.get('edit_target_index')
    settings = get_data(SETTINGS_FILE, {})
    chat_id = settings['targets'][target_index]['chat_id']

    new_status = 'running' if action == 'scen_start' else 'stopped'

    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("UPDATE scenarios SET status = ? WHERE chat_id = ?", (new_status, chat_id))
        conn.commit()

    await query.answer(f"Сценарий {'запущен' if new_status == 'running' else 'остановлен'}")
    return await scenario_menu(update, context)


@owner_only
async def scenario_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    target_index = context.user_data.get('edit_target_index')
    settings = get_data(SETTINGS_FILE, {})
    chat_id = settings['targets'][target_index]['chat_id']

    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("UPDATE scenarios SET current_index = 0 WHERE chat_id = ?", (chat_id,))
        conn.commit()

    await query.answer("Прогресс сброшен на начало.")
    return await scenario_menu(update, context)


@owner_only
async def triggers_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query:
        await query.answer()

    target_index = context.user_data.get('edit_target_index')
    settings = get_data(SETTINGS_FILE, {})

    if target_index is None or target_index >= len(settings.get('targets', [])):
        return await start(update, context)

    target = settings.get('targets', [])[target_index]
    chat_id = target.get('chat_id')

    triggers = []
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT id, trigger_phrase, answer_text FROM triggers WHERE chat_id = ?", (chat_id,))
        triggers = cursor.fetchall()

    text = f"<b>⚡ Триггер-слова для {html.escape(target['chat_name'])}</b>\n\nЕсли реальный человек напишет одно из этих слов, бот мгновенно ответит заготовленной фразой (вместо генерации ИИ)."

    keyboard = []
    for t_id, phrase, answer in triggers:
        display = f"❌ '{phrase}' -> '{answer[:15]}...'"
        keyboard.append([InlineKeyboardButton(display, callback_data=f"del_trigger_{t_id}")])

    keyboard.append([InlineKeyboardButton("➕ Добавить триггер", callback_data="add_trigger_start")])
    keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data="back_to_edit_menu")])

    if query:
        await try_edit_message(query, text, InlineKeyboardMarkup(keyboard))
    else:
        await context.bot.send_message(update.effective_chat.id, text, reply_markup=InlineKeyboardMarkup(keyboard),
                                       parse_mode='HTML')

    return TRIGGERS_MENU


@owner_only
async def add_trigger_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query, "Введите <b>слово или фразу-триггер</b> (например: 'цена' или 'сколько стоит'):",
                           None)
    return ADD_TRIGGER_PHRASE


@owner_only
async def add_trigger_get_phrase(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['new_trigger_phrase'] = update.message.text.strip().lower()
    await update.message.reply_text("Теперь введите <b>ответ бота</b> на эту фразу:", parse_mode='HTML')
    return ADD_TRIGGER_RESPONSE


@owner_only
async def add_trigger_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    answer = update.message.text.strip()
    phrase = context.user_data['new_trigger_phrase']

    target_index = context.user_data.get('edit_target_index')
    settings = get_data(SETTINGS_FILE, {})
    chat_id = settings['targets'][target_index]['chat_id']

    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("INSERT INTO triggers (chat_id, trigger_phrase, answer_text) VALUES (?, ?, ?)",
                     (chat_id, phrase, answer))
        conn.commit()

    await update.message.reply_text(f"✅ Триггер '{phrase}' добавлен.")
    return await triggers_menu(update, context)


@owner_only
async def delete_trigger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    t_id = int(query.data.split('_')[-1])

    with sqlite3.connect(DB_FILE) as conn:
        conn.execute("DELETE FROM triggers WHERE id = ?", (t_id,))
        conn.commit()

    await query.answer("Триггер удален")
    return await triggers_menu(update, context)


@owner_only
async def manual_reply_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    try:
        alert_id = int(query.data.split('_')[-1])
    except (ValueError, IndexError):
        await query.message.reply_text("❌ Некорректные данные кнопки.")
        return MAIN_MENU

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT chat_id, msg_id, session_name FROM alert_context WHERE id = ?", (alert_id,))
        row = cursor.fetchone()

    if not row:
        await query.message.reply_text("❌ Ошибка: этот алерт устарел или был удален.")
        return MAIN_MENU

    chat_id, msg_id, session = row
    context.user_data['manual_reply_context'] = {
        'chat_id': chat_id,
        'msg_id': msg_id,
        'session': session
    }

    text = (
        f"✍️ <b>Ручной ответ</b>\n\n"
        f"🤖 От лица: <b>{html.escape(session)}</b>\n"
        f"📍 Чат ID: <code>{chat_id}</code>\n\n"
        f"Введите текст ответа:"
    )

    await query.message.reply_text(text, parse_mode='HTML')
    return MANUAL_REPLY_SEND


@owner_only
async def manual_reply_save(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    ctx = context.user_data.get('manual_reply_context')

    if not ctx:
        await update.message.reply_text("❌ Ошибка контекста. Попробуйте снова через уведомление.")
        return MAIN_MENU

    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.execute('''
                INSERT INTO outbound_queue (chat_id, reply_to_msg_id, session_name, text, status)
                VALUES (?, ?, ?, ?, 'pending')
            ''', (ctx['chat_id'], ctx['msg_id'], ctx['session'], text))
            conn.commit()

        await update.message.reply_text("✅ Ответ отправлен в очередь.")
    except Exception as e:
        logger.error(f"Ошибка сохранения ручного ответа: {e}")
        await update.message.reply_text("❌ Ошибка сохранения в БД.")

    context.user_data.pop('manual_reply_context', None)
    return await send_main_menu(update, context)


@owner_only
async def ask_manual_link_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await try_edit_message(query,
                           "🔗 <b>Ручной запуск</b>\n\nОтправьте ссылку на пост или сообщение, которое нужно обработать.\n\nПример:\n<code>https://t.me/channelname/123</code>\n<code>https://t.me/c/123456789/555</code>\n\nБот определит канал, найдет настройки и запустит обработку (AI, Скрипт, Триггеры) для этого поста, даже если он старый.",
                           None)
    return START_MANUAL_LINK


@owner_only
async def process_manual_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    link = update.message.text.strip()

    chat_identifier = None
    post_id = None
    is_private_link = False

    try:
        if "t.me/c/" in link:
            parts = link.split("t.me/c/")[-1].split('/')
            chat_id_num = parts[0]
            post_id = int(parts[1])
            chat_identifier = int(f"-100{chat_id_num}")
            is_private_link = True
        elif "t.me/" in link:
            parts = link.split("t.me/")[-1].split('/')
            chat_identifier = parts[0]
            post_id = int(parts[1])
        else:
            raise ValueError
    except:
        await update.message.reply_text("❌ Неверный формат ссылки. Попробуйте снова или нажмите /start")
        return START_MANUAL_LINK

    settings = get_data(SETTINGS_FILE, {})
    targets = settings.get('targets', [])
    found_target = None

    def clean_username(u):
        return str(u).lower().replace('@', '').strip() if u else ''

    target_identifier_clean = clean_username(chat_identifier)

    for t in targets:
        t_id = str(t.get('chat_id', ''))
        t_link = str(t.get('linked_chat_id', ''))
        t_user = clean_username(t.get('chat_username', ''))

        check_val = str(chat_identifier)

        if check_val == t_id or check_val == t_link:
            found_target = t
            break

        if is_private_link:
            short_id = str(chat_identifier).replace('-100', '')
            if short_id in t_id or short_id in t_link:
                found_target = t
                break

        if t_user and t_user == target_identifier_clean:
            found_target = t
            break

    if not found_target and not is_private_link:
        accounts = get_data(ACCOUNTS_FILE, [])
        if accounts:
            msg_checking = await update.message.reply_text(
                "🔎 Канал не найден по юзернейму в базе. Пробую определить ID через Telegram...")

            api_id = int(telethon_config['api_id'])
            api_hash = telethon_config['api_hash']
            client = TelegramClient(StringSession(accounts[0]['session_string']), api_id, api_hash)

            try:
                await client.connect()
                entity = await client.get_entity(link)
                real_id = f"-100{entity.id}"

                for t in targets:
                    t_id = str(t.get('chat_id', ''))
                    t_link = str(t.get('linked_chat_id', ''))

                    if real_id == t_id or real_id == t_link:
                        found_target = t
                        if not t.get('chat_username') and getattr(entity, 'username', None):
                            t['chat_username'] = entity.username
                            save_data(SETTINGS_FILE, settings)
                        break
            except Exception as e:
                logger.error(f"Ошибка резолва ссылки: {e}")
            finally:
                if client.is_connected():
                    await client.disconnect()
                try:
                    await msg_checking.delete()
                except:
                    pass

    if not found_target:
        await update.message.reply_text(
            f"❌ Канал из ссылки не найден в ваших 'Целевых чатах'.\n\nБот искал по юзернейму и пытался пробить ID, но совпадений в базе нет.\nУбедитесь, что канал добавлен в разделе 'Целевые чаты'.")
        return await send_main_menu(update, context)

    manual_task = {
        "chat_id": found_target['chat_id'],
        "post_id": post_id,
        "added_at": time.time()
    }

    if 'manual_queue' not in settings:
        settings['manual_queue'] = []

    settings['manual_queue'].append(manual_task)
    save_data(SETTINGS_FILE, settings)

    await update.message.reply_text(
        f"✅ <b>Задание принято!</b>\n\nКанал: {html.escape(found_target.get('chat_name', ''))}\nПост ID: {post_id}\n\nСкрипт commentator.py подхватит этот пост в течение нескольких секунд.",
        parse_mode='HTML')
    return await send_main_menu(update, context)


def main():
    ensure_data_dir()
    init_database()
    if not os.path.exists(SETTINGS_FILE):
        save_data(
            SETTINGS_FILE,
            {
                "status": "stopped",
                "ai_provider": "deepseek",
                "api_keys": {},
                "models": {
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
                },
                "targets": [],
                "reaction_targets": [],
                "monitor_targets": [],
            },
        )
    if not os.path.exists(ACCOUNTS_FILE):
        save_data(ACCOUNTS_FILE, [])

    application = Application.builder().token(admin_config['token']).build()

    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('start', start),
            CommandHandler('help', help_command),
            CallbackQueryHandler(manual_reply_start, pattern=r'^reply_alert_')
        ],
        states={
            MAIN_MENU: [
                CallbackQueryHandler(start_commentator, pattern='^start_commentator$'),
                CallbackQueryHandler(stop_commentator, pattern='^stop_commentator$'),
                CallbackQueryHandler(settings_menu, pattern='^settings_menu$'),
                CallbackQueryHandler(accounts_menu, pattern='^accounts_menu$'),
                CallbackQueryHandler(targets_menu, pattern='^targets_menu$'),
                CallbackQueryHandler(reaction_targets_menu, pattern='^reaction_targets_menu$'),
                CallbackQueryHandler(monitor_targets_menu, pattern='^monitor_targets_menu$'),
                CallbackQueryHandler(personas_menu_logic, pattern=r'^personas_menu(_\d+)?$'),
                CallbackQueryHandler(rebrand_menu_start, pattern='^rebrand_menu$'),
                CallbackQueryHandler(stats_menu, pattern='^stats_menu$'),
                CallbackQueryHandler(manual_reply_start, pattern=r'^reply_alert_'),
                CallbackQueryHandler(proxies_menu, pattern='^proxies_menu$'),
                CallbackQueryHandler(ask_manual_link_start, pattern='^manual_link_start$')
            ],
            START_MANUAL_LINK: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, process_manual_link),
                CallbackQueryHandler(start, pattern='^back_to_main$')
            ],
            MANUAL_REPLY_SEND: [MessageHandler(filters.TEXT & ~filters.COMMAND, manual_reply_save)],
            PERSONAS_MENU: [
                CallbackQueryHandler(add_persona_start, pattern='^add_persona_start$'),
                CallbackQueryHandler(assign_persona_menu, pattern=r'^edit_persona_'),
                CallbackQueryHandler(personas_menu_logic, pattern=r'^personas_menu(_\d+)?$'),
                CallbackQueryHandler(delete_persona_start, pattern=r'^delete_persona_start_'),
                CallbackQueryHandler(start, pattern='^back_to_main$')
            ],
            ADD_PERSONA_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_persona_get_name)],
            ADD_PERSONA_PROMPT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_persona_save)],
            SELECT_ACCOUNTS_FOR_PERSONA: [
                CallbackQueryHandler(toggle_persona_account_selection, pattern=r'^tg_ps_'),
                CallbackQueryHandler(assign_persona_menu, pattern=r'^edit_persona_'),
                CallbackQueryHandler(save_persona_batch, pattern='^save_persona_batch$'),
                CallbackQueryHandler(personas_menu_logic, pattern=r'^personas_menu(_\d+)?$')
            ],
            REBRAND_MENU: [
                CallbackQueryHandler(rebrand_get_topic_start, pattern='^rebrand_run$'),
                CallbackQueryHandler(start, pattern='^back_to_main$')
            ],
            REBRAND_GET_TOPIC: [MessageHandler(filters.TEXT & ~filters.COMMAND, rebrand_save_topic)],
            REBRAND_GET_SOURCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, rebrand_run_task)],
            STATS_MENU: [
                CallbackQueryHandler(show_stats, pattern=r'^stats_show_'),
                CallbackQueryHandler(show_stats, pattern=r'^stats_page_'),
                CallbackQueryHandler(export_stats_to_excel, pattern=r'^export_csv_'),
                CallbackQueryHandler(stats_menu, pattern='^back_to_stats_menu$'),
                CallbackQueryHandler(start, pattern='^back_to_main$')
            ],
            ACCOUNTS_MENU: [
                CallbackQueryHandler(show_my_accounts, pattern='^my_accounts_list$'),
                CallbackQueryHandler(check_accounts, pattern='^check_accounts$'),
                CallbackQueryHandler(add_account_start, pattern='^add_acc_start$'),
                CallbackQueryHandler(delete_account_start, pattern='^del_acc_start$'),
                CallbackQueryHandler(start, pattern='^back_to_main$')
            ],
            MY_ACCOUNTS_MENU: [
                CallbackQueryHandler(accounts_menu, pattern='^back_to_accounts$'),
                CallbackQueryHandler(edit_account_settings_menu, pattern=r'^edit_acc_settings_\d+$'),
                CallbackQueryHandler(set_account_sleep_hour_prompt, pattern=r'^set_acc_sleep_(start|end)$'),
                CallbackQueryHandler(show_my_accounts, pattern='^my_accounts_list$'),
                CallbackQueryHandler(select_proxy_for_account, pattern='^set_acc_proxy_start$')
            ],
            SETTINGS_MENU: [
                CallbackQueryHandler(change_provider_menu, pattern='^change_provider$'),
                CallbackQueryHandler(update_api_key_start, pattern='^update_api_key$'),
                CallbackQueryHandler(get_api_key_prompt, pattern=r'^get_api_key_'),
                CallbackQueryHandler(humanization_settings_menu, pattern='^human_settings_menu$'),
                CallbackQueryHandler(blacklist_menu, pattern='^blacklist_menu$'),
                CallbackQueryHandler(settings_menu, pattern='^back_to_settings$'),
                CallbackQueryHandler(start, pattern='^back_to_main$')
            ],
            AI_PROVIDER_MENU: [CallbackQueryHandler(set_provider, pattern='^set_provider_'),
                               CallbackQueryHandler(settings_menu, pattern='^back_to_settings$')],
            TARGETS_MENU: [
                CallbackQueryHandler(show_my_targets, pattern=r'^my_targets_list_0$'),
                CallbackQueryHandler(add_target_start, pattern='^add_target_start$'),
                CallbackQueryHandler(search_similar_channels_start, pattern='^search_similar_channels$'),
                CallbackQueryHandler(delete_target_start, pattern=r'^delete_target_start(_\d+)?$'),
                CallbackQueryHandler(confirm_delete_all_targets, pattern='^delete_all_targets_confirm$'),
                CallbackQueryHandler(start, pattern='^back_to_main$'),
            ],
            GET_SOURCE_CHANNEL: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_source_channel_and_search)],
            SHOW_FOUND_CHANNELS: [
                CallbackQueryHandler(add_found_channel, pattern=r'^add_found_'),
                CallbackQueryHandler(show_found_channels, pattern=r'^found_channels_page_'),
                CallbackQueryHandler(targets_menu, pattern=r'^back_to_targets_menu$')
            ],
            MY_TARGETS_MENU: [
                CallbackQueryHandler(show_my_targets, pattern=r'^my_targets_list_\d+$'),
                CallbackQueryHandler(edit_target_entry, pattern=r'^edit_target_\d+$'),
                CallbackQueryHandler(targets_menu, pattern='^back_to_targets_menu$'),
            ],
            REACTION_TARGETS_MENU: [
                CallbackQueryHandler(show_my_reaction_targets, pattern=r'^my_reaction_targets_list_0$'),
                CallbackQueryHandler(add_reaction_target_start, pattern='^add_reaction_target_start$'),
                CallbackQueryHandler(delete_reaction_target_start, pattern=r'^delete_reaction_target_start(_\d+)?$'),
                CallbackQueryHandler(confirm_delete_all_reaction_targets,
                                     pattern='^delete_all_reaction_targets_confirm$'),
                CallbackQueryHandler(start, pattern='^back_to_main$'),
            ],
            MY_REACTION_TARGETS_MENU: [
                CallbackQueryHandler(show_my_reaction_targets, pattern=r'^my_reaction_targets_list_\d+$'),
                CallbackQueryHandler(edit_reaction_target_entry, pattern=r'^edit_reaction_target_\d+$'),
                CallbackQueryHandler(reaction_targets_menu, pattern='^back_to_reaction_targets_menu$'),
            ],
            MONITOR_TARGETS_MENU: [
                CallbackQueryHandler(show_my_monitor_targets, pattern=r'^my_monitor_targets_list_0$'),
                CallbackQueryHandler(add_monitor_target_start, pattern='^add_monitor_target_start$'),
                CallbackQueryHandler(delete_monitor_target_start, pattern=r'^delete_monitor_target_start(_\d+)?$'),
                CallbackQueryHandler(confirm_delete_all_monitor_targets,
                                     pattern='^delete_all_monitor_targets_confirmed$'),
                CallbackQueryHandler(start, pattern='^back_to_main$'),
            ],
            MY_MONITOR_TARGETS_MENU: [
                CallbackQueryHandler(show_my_monitor_targets, pattern=r'^my_monitor_targets_list_\d+$'),
                CallbackQueryHandler(edit_monitor_target_entry, pattern=r'^edit_monitor_target_\d+$'),
                CallbackQueryHandler(monitor_targets_menu, pattern='^back_to_monitor_targets_menu$'),
            ],
            EDIT_TARGET_MENU: [
                CallbackQueryHandler(toggle_target_ai, pattern='^toggle_target_ai$'),
                CallbackQueryHandler(scenario_menu, pattern='^scenario_menu_entry$'),
                CallbackQueryHandler(triggers_menu, pattern='^triggers_menu_entry$'),
                CallbackQueryHandler(assign_accounts_menu, pattern='^assign_accounts$'),
                CallbackQueryHandler(set_chat_ai_menu, pattern='^set_chat_ai$'),
                CallbackQueryHandler(edit_delays_start, pattern='^edit_delays$'),
                CallbackQueryHandler(edit_reply_settings_menu, pattern='^edit_reply_settings$'),
                CallbackQueryHandler(edit_limit_start, pattern='^edit_limit$'),
                CallbackQueryHandler(prompts_menu_start, pattern='^edit_prompts_menu$'),
                CallbackQueryHandler(edit_filters_menu, pattern='^edit_filters$'),
                CallbackQueryHandler(show_my_targets, pattern='^my_targets_list_0$')
            ],
            SCENARIO_MENU: [
                CallbackQueryHandler(scenario_upload_start, pattern='^scen_upload$'),
                CallbackQueryHandler(scenario_toggle, pattern=r'^scen_(start|stop)$'),
                CallbackQueryHandler(scenario_reset, pattern='^scen_reset$'),
                CallbackQueryHandler(toggle_scenario_reply, pattern='^toggle_scen_reply$'),
                CallbackQueryHandler(edit_target_menu_logic, pattern='^back_to_edit_menu$')
            ],
            TRIGGERS_MENU: [
                CallbackQueryHandler(add_trigger_start, pattern='^add_trigger_start$'),
                CallbackQueryHandler(delete_trigger, pattern=r'^del_trigger_'),
                CallbackQueryHandler(edit_target_menu_logic, pattern='^back_to_edit_menu$')
            ],
            ADD_TRIGGER_PHRASE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_trigger_get_phrase)],
            ADD_TRIGGER_RESPONSE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_trigger_save)],
            UPLOAD_SCENARIO: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_scenario_text)],
            GET_PERSONA_TO_DELETE: [
                CallbackQueryHandler(delete_persona_confirm, pattern=r'^confirm_del_persona_'),
                CallbackQueryHandler(delete_persona_start, pattern=r'^delete_persona_start_'),
                CallbackQueryHandler(personas_menu_logic, pattern=r'^personas_menu(_\d+)?$')
            ],
            EDIT_REPLY_SETTINGS_MENU: [
                CallbackQueryHandler(edit_reply_chance_call, pattern='^edit_reply_chance$'),
                CallbackQueryHandler(edit_reply_intervention_start, pattern='^edit_reply_intervention$'),
                CallbackQueryHandler(edit_tag_reply_chance_start, pattern='^edit_tag_reply_chance$'),
                CallbackQueryHandler(edit_reply_depth_start, pattern='^edit_reply_depth$'),
                CallbackQueryHandler(edit_reply_delay_min_call, pattern='^edit_reply_delay_min$'),
                CallbackQueryHandler(edit_reply_delay_max_call, pattern='^edit_reply_delay_max$'),
                CallbackQueryHandler(edit_target_menu_logic, pattern='^back_to_edit_menu$')
            ],
            EDIT_REPLY_CHANCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_reply_setting)],
            EDIT_REPLY_INTERVENTION: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_reply_setting)],
            EDIT_REPLY_DEPTH: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_reply_setting)],
            EDIT_REPLY_DELAY_MIN: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_reply_setting)],
            EDIT_REPLY_DELAY_MAX: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_reply_setting)],
            EDIT_TAG_REPLY_CHANCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_reply_setting)],
            EDIT_REACTION_TARGET_MENU: [
                CallbackQueryHandler(assign_reaction_accounts_menu, pattern='^assign_reaction_accounts$'),
                CallbackQueryHandler(edit_reaction_count_menu, pattern='^edit_reaction_count$'),
                CallbackQueryHandler(edit_reaction_chance_start, pattern='^edit_reaction_chance$'),
                CallbackQueryHandler(edit_reaction_list_start, pattern='^edit_reaction_list'),
                CallbackQueryHandler(edit_reaction_delays_start, pattern='^edit_reaction_delays$'),
                CallbackQueryHandler(edit_reaction_limit_start, pattern='^edit_reaction_limit$'),
                CallbackQueryHandler(show_my_reaction_targets, pattern='^my_reaction_targets_list_0$'),
            ],
            EDIT_MONITOR_TARGET_MENU: [
                CallbackQueryHandler(assign_monitor_accounts_menu, pattern='^edit_monitor_assign_accounts$'),
                CallbackQueryHandler(set_monitor_ai_menu, pattern='^edit_monitor_ai$'),
                CallbackQueryHandler(edit_monitor_prompt_start, pattern='^edit_monitor_prompt$'),
                CallbackQueryHandler(edit_monitor_limit_start, pattern='^edit_monitor_limit$'),
                CallbackQueryHandler(edit_monitor_notification_chat_start, pattern='^edit_monitor_notification_chat$'),
                CallbackQueryHandler(edit_monitor_filters_menu, pattern='^edit_monitor_filters$'),
                CallbackQueryHandler(show_my_monitor_targets, pattern='^my_monitor_targets_list_0$'),
            ],
            EDIT_HUMAN_SETTINGS: [
                CallbackQueryHandler(edit_human_prompt_start, pattern='^set_human_prompt$'),
                CallbackQueryHandler(edit_human_setting_start, pattern=r'^set_human_'),
                CallbackQueryHandler(settings_menu, pattern='^back_to_settings$')
            ],
            EDIT_HUMAN_VALUE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, save_human_setting)
            ],
            EDIT_HUMAN_PROMPT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, save_human_prompt)
            ],
            EDIT_MONITOR_ASSIGN_ACCOUNTS: [
                CallbackQueryHandler(toggle_monitor_account_assignment, pattern=r'^toggle_monitor_acc_'),
                CallbackQueryHandler(edit_monitor_target_menu_logic, pattern='^back_to_edit_monitor_menu$')
            ],
            EDIT_MONITOR_AI: [
                CallbackQueryHandler(save_monitor_ai, pattern=r'save_monitor_ai_'),
                CallbackQueryHandler(edit_monitor_target_menu_logic, pattern='^back_to_edit_monitor_menu$')
            ],
            ADD_ACCOUNT_SESSION: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_account_get_session)],
            ADD_ACCOUNT_PHONE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_account_get_phone)],
            GET_AUTH_CODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_auth_code)],
            GET_2FA_PASSWORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_2fa_password)],
            GET_ACCOUNT_TO_DELETE: [CallbackQueryHandler(delete_account_confirm, pattern=r'^del_acc_\d+$'),
                                    CallbackQueryHandler(accounts_menu, pattern='^back_to_accounts$')],
            GET_API_KEY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, get_api_key),
                CallbackQueryHandler(update_api_key_start, pattern='^update_api_key$')
            ],
            CONFIRM_DELETE_ALL_TARGETS: [
                CallbackQueryHandler(do_delete_all_targets, pattern='^delete_all_targets_confirmed$'),
                CallbackQueryHandler(targets_menu, pattern='^back_to_targets$')],
            ADD_TARGET_CHAT_ID: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_target_get_chat_id)],
            GET_TARGET_TO_DELETE: [
                CallbackQueryHandler(delete_target_start, pattern=r'^delete_target_start_\d+$'),
                CallbackQueryHandler(delete_target_confirm, pattern=r'^del_target_\d+$'),
                CallbackQueryHandler(targets_menu, pattern='^back_to_targets$')
            ],
            ADD_TARGET_PROMPT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_target_get_prompt),
                CallbackQueryHandler(skip_target_prompt, pattern='^skip_target_prompt$')
            ],
            ADD_TARGET_INITIAL_DELAY: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_target_get_initial_delay)],
            ADD_TARGET_BETWEEN_DELAY: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_target_get_between_delay)],
            ADD_TARGET_DAILY_LIMIT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_target_get_daily_limit)],
            ADD_TARGET_AI_PROVIDER: [CallbackQueryHandler(add_target_get_ai_provider, pattern=r'^save_target_ai_')],
            ADD_TARGET_MIN_WORDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_target_get_min_words)],
            ADD_TARGET_MIN_INTERVAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_target_save_all)],
            EDIT_FILTERS_MENU: [CallbackQueryHandler(edit_filter_min_words_start, pattern='^edit_filter_words$'),
                                CallbackQueryHandler(edit_filter_min_interval_start, pattern='^edit_filter_interval$'),
                                CallbackQueryHandler(edit_target_menu_logic, pattern='^back_to_edit_menu$')],
            EDIT_FILTER_MIN_WORDS: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_filter_min_words)],
            EDIT_FILTER_MIN_INTERVAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_filter_min_interval)],
            EDIT_CHAT_ASSIGN_ACCOUNTS: [CallbackQueryHandler(toggle_account_assignment, pattern=r'^toggle_acc_'),
                                        CallbackQueryHandler(edit_target_menu_logic, pattern='^back_to_edit_menu$')],
            EDIT_CHAT_AI: [CallbackQueryHandler(save_chat_ai, pattern=r'^save_chat_ai_'),
                           CallbackQueryHandler(edit_target_menu_logic, pattern='^back_to_edit_menu$')],
            EDIT_DELAYS_INITIAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_delays_get_initial)],
            EDIT_DELAYS_BETWEEN: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_delays_get_between)],
            EDIT_TARGET_DAILY_LIMIT: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_target_get_daily_limit)],
            PROMPTS_MENU: [CallbackQueryHandler(edit_account_prompt_start, pattern=r'^edit_prompt_'),
                           CallbackQueryHandler(edit_target_menu_logic, pattern='^back_to_edit_menu$')],
            GET_ACCOUNT_PROMPT: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_account_prompt)],
            CONFIRM_DELETE_ALL_REACTION_TARGETS: [
                CallbackQueryHandler(do_delete_all_reaction_targets, pattern='^delete_all_reaction_targets_confirmed$'),
                CallbackQueryHandler(reaction_targets_menu, pattern='^back_to_reaction_targets$')],
            ADD_REACTION_TARGET_CHAT_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_reaction_target_get_chat_id)],
            ADD_REACTION_TARGET_REACTIONS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_reaction_target_get_reactions)],
            ADD_REACTION_TARGET_INITIAL_DELAY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_reaction_target_get_initial_delay)],
            ADD_REACTION_TARGET_BETWEEN_DELAY: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_reaction_target_get_between_delay)],
            ADD_REACTION_TARGET_DAILY_LIMIT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_reaction_target_get_daily_limit)],
            ADD_REACTION_TARGET_REACTION_COUNT: [
                CallbackQueryHandler(add_reaction_target_save, pattern=r'^save_reaction_count_')],
            GET_REACTION_TARGET_TO_DELETE: [
                CallbackQueryHandler(delete_reaction_target_start, pattern=r'^delete_reaction_target_start_\d+$'),
                CallbackQueryHandler(delete_reaction_target_confirm, pattern=r'^del_reaction_target_\d+$'),
                CallbackQueryHandler(reaction_targets_menu, pattern='^back_to_reaction_targets$')],
            EDIT_REACTION_TARGET_ASSIGN_ACCOUNTS: [
                CallbackQueryHandler(toggle_reaction_account_assignment, pattern=r'^toggle_reaction_acc_'),
                CallbackQueryHandler(edit_reaction_target_menu_logic, pattern='^back_to_edit_reaction_menu$')],
            EDIT_REACTION_TARGET_REACTION_COUNT: [
                CallbackQueryHandler(save_reaction_count, pattern=r'^set_reaction_count_'),
                CallbackQueryHandler(edit_reaction_target_menu_logic, pattern='^back_to_edit_reaction_menu$')],
            EDIT_REACTION_TARGET_GET_REACTIONS: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_reaction_list)],
            EDIT_REACTION_DELAYS_INITIAL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_reaction_delays_get_initial)],
            EDIT_REACTION_DELAYS_BETWEEN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_reaction_delays_get_between)],
            EDIT_REACTION_TARGET_DAILY_LIMIT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, edit_reaction_target_get_daily_limit)],
            CONFIRM_DELETE_ALL_MONITOR_TARGETS: [
                CallbackQueryHandler(do_delete_all_monitor_targets, pattern='^delete_all_monitor_targets_confirmed$'),
                CallbackQueryHandler(monitor_targets_menu, pattern='^back_to_monitor_targets_menu$')],
            ADD_MONITOR_TARGET_CHAT_ID: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_monitor_target_get_chat_id)],
            ADD_MONITOR_TARGET_NOTIFICATION_CHAT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_monitor_target_get_notification_chat)],
            ADD_MONITOR_TARGET_PROMPT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_monitor_target_get_prompt)],
            ADD_MONITOR_TARGET_DAILY_LIMIT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_monitor_target_get_daily_limit)],
            ADD_MONITOR_TARGET_MIN_WORDS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_monitor_target_get_min_words)],
            ADD_MONITOR_TARGET_MIN_INTERVAL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, add_monitor_target_save_all)],
            GET_MONITOR_TARGET_TO_DELETE: [
                CallbackQueryHandler(delete_monitor_target_start, pattern=r'^delete_monitor_target_start_\d+$'),
                CallbackQueryHandler(delete_monitor_target_confirm, pattern=r'^del_monitor_target_\d+$'),
                CallbackQueryHandler(monitor_targets_menu, pattern='^back_to_monitor_targets_menu$')],
            EDIT_MONITOR_PROMPT: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_monitor_prompt)],
            EDIT_MONITOR_LIMIT: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_monitor_limit)],
            EDIT_MONITOR_NOTIFICATION_CHAT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, save_monitor_notification_chat)],
            EDIT_MONITOR_FILTERS_MENU: [
                CallbackQueryHandler(edit_monitor_filter_min_words_start, pattern='^edit_monitor_filter_words$'),
                CallbackQueryHandler(edit_monitor_filter_min_interval_start, pattern='^edit_monitor_filter_interval$'),
                CallbackQueryHandler(edit_monitor_target_menu_logic, pattern='^back_to_edit_monitor_menu$'),
            ],
            EDIT_ACCOUNT_SLEEP_START: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_account_sleep_hour)],
            EDIT_ACCOUNT_SLEEP_END: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_account_sleep_hour)],
            EDIT_MONITOR_FILTER_MIN_WORDS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, save_monitor_filter_min_words)],
            EDIT_MONITOR_FILTER_MIN_INTERVAL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, save_monitor_filter_min_interval)],
            PROXIES_MENU: [
                CallbackQueryHandler(show_my_proxies, pattern=r'^my_proxies_list_'),
                CallbackQueryHandler(add_proxies_start, pattern='^add_proxies_start$'),
                CallbackQueryHandler(check_all_proxies, pattern='^check_all_proxies$'),
                CallbackQueryHandler(delete_dead_proxies, pattern='^delete_dead_proxies$'),
                CallbackQueryHandler(proxies_menu, pattern='^proxies_menu$'),
                CallbackQueryHandler(proxy_action_menu, pattern=r'^proxy_actions_'),
                CallbackQueryHandler(process_proxy_specific_action, pattern=r'^pr_act_'),
                CallbackQueryHandler(start, pattern='^back_to_main$')
            ],
            BLACKLIST_MENU: [
                CallbackQueryHandler(add_blacklist_start, pattern='^add_blacklist_start$'),
                CallbackQueryHandler(delete_blacklist_start, pattern='^delete_blacklist_start$'),
                CallbackQueryHandler(settings_menu, pattern='^settings_menu$')
            ],
            ADD_BLACKLIST_WORD: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_blacklist_words)],
            DELETE_BLACKLIST_WORD: [
                CallbackQueryHandler(delete_blacklist_confirm, pattern=r'^del_bl_'),
                CallbackQueryHandler(blacklist_menu, pattern='^blacklist_menu$')
            ],
            ADD_PROXIES_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_proxies_save)],
            SELECT_PROXY_FOR_ACCOUNT: [
                CallbackQueryHandler(select_proxy_for_account, pattern=r'^sel_proxy_page_'),
                CallbackQueryHandler(save_account_proxy_callback, pattern=r'^set_proxy_id_'),
                CallbackQueryHandler(accounts_menu, pattern='^accounts_menu$')
            ],
            EDIT_REACTION_CHANCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_reaction_chance)],
        },
        fallbacks=[
            CommandHandler('start', start),
            CommandHandler('cancel', cancel),
        ],
        per_message=False,
        allow_reentry=True,
        conversation_timeout=None
    )
    application.add_handler(conv_handler)
    logger.info("Админ-бот запущен...")
    application.run_polling()


if __name__ == "__main__":
    main()

from __future__ import annotations

import asyncio
import base64
import configparser
import html
import io
import logging
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import httpx
import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from google import genai
import openai
from starlette.middleware.sessions import SessionMiddleware

from app_paths import ACCOUNTS_FILE, CONFIG_FILE, DATA_DIR, DB_FILE, SETTINGS_FILE, ensure_data_dir
from app_storage import load_json, load_json_with_error, save_json

from telethon import TelegramClient
from telethon.errors import (
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    RPCError,
    SessionPasswordNeededError,
    UserDeactivatedBanError,
)
from telethon.sessions import StringSession
from telethon.tl.functions.channels import (
    GetChannelRecommendationsRequest,
    GetFullChannelRequest,
    JoinChannelRequest,
)
from telethon.tl.functions.messages import CheckChatInviteRequest
from telethon.tl.types import PeerChannel

ROOT_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = ROOT_DIR / "templates"
STATIC_DIR = ROOT_DIR / "static"
try:
    STATIC_VERSION = str(int((STATIC_DIR / "app.css").stat().st_mtime))
except Exception:
    STATIC_VERSION = "1"

logger = logging.getLogger("admin_web")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


ADMIN_WEB_DISABLE_AUTH = _env_bool("ADMIN_WEB_DISABLE_AUTH", False)
ADMIN_WEB_USERNAME = os.getenv("ADMIN_WEB_USERNAME", "admin")
ADMIN_WEB_PASSWORD = os.getenv("ADMIN_WEB_PASSWORD", "admin")
ADMIN_WEB_SECRET = os.getenv("ADMIN_WEB_SECRET") or base64.urlsafe_b64encode(os.urandom(32)).decode("utf-8")

DEFAULT_MODELS: Dict[str, str] = {
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
DEFAULT_PROJECT_NAME = "Стандартный проект"


def _mask_secret(value: str | None) -> str:
    if not value:
        return "не задан"
    if len(value) <= 8:
        return "ключ задан"
    return f"{value[:4]}...{value[-4:]}"


def _parse_int(value: str | None, *, default: int | None = None) -> int | None:
    if value is None:
        return default
    value = value.strip()
    if value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _wants_html(request: Request) -> bool:
    accept = (request.headers.get("accept") or "").lower()
    return "text/html" in accept or "*/*" in accept or accept == ""


def _parse_int_field(
    request: Request,
    value: str | None,
    *,
    default: int,
    label: str,
    min_value: int | None = None,
    max_value: int | None = None,
) -> int:
    raw = (value or "").strip().replace(",", ".")
    if raw == "":
        return default
    try:
        n = int(float(raw))
    except ValueError:
        _flash(request, "warning", f"Поле «{label}»: некорректное число. Использую {default}.")
        return default
    if min_value is not None and n < min_value:
        _flash(request, "warning", f"Поле «{label}»: минимум {min_value}. Исправлено.")
        n = min_value
    if max_value is not None and n > max_value:
        _flash(request, "warning", f"Поле «{label}»: максимум {max_value}. Исправлено.")
        n = max_value
    return n


def _parse_float_field(
    request: Request,
    value: str | None,
    *,
    default: float | None,
    label: str,
    min_value: float | None = None,
    max_value: float | None = None,
) -> float | None:
    raw = (value or "").strip().replace(",", ".")
    if raw == "":
        return default
    try:
        n = float(raw)
    except ValueError:
        _flash(request, "warning", f"Поле «{label}»: некорректное число. Использую значение по умолчанию.")
        return default
    if min_value is not None and n < min_value:
        _flash(request, "warning", f"Поле «{label}»: минимум {min_value}. Исправлено.")
        n = min_value
    if max_value is not None and n > max_value:
        _flash(request, "warning", f"Поле «{label}»: максимум {max_value}. Исправлено.")
        n = max_value
    return n


def _clean_username(value: Any) -> str:
    return str(value).lower().replace("@", "").strip() if value is not None else ""


def _ensure_settings_schema(settings: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(settings, dict):
        settings = {}
    settings.setdefault("status", "stopped")
    settings.setdefault("ai_provider", "deepseek")
    settings.setdefault("api_keys", {})

    models = settings.get("models")
    if not isinstance(models, dict):
        models = {}
    for key, value in DEFAULT_MODELS.items():
        models.setdefault(key, value)
    settings["models"] = models

    settings.setdefault("targets", [])
    settings.setdefault("reaction_targets", [])
    settings.setdefault("monitor_targets", [])
    settings.setdefault("humanization", {})
    settings.setdefault("blacklist", [])
    settings.setdefault("personas", {})
    settings.setdefault("manual_queue", [])
    settings.setdefault("profile_tasks", {})
    _ensure_projects_schema(settings)
    _ensure_project_ids(settings)
    return settings


def _ensure_projects_schema(settings: Dict[str, Any]) -> None:
    projects = settings.get("projects")
    if not isinstance(projects, list):
        projects = []

    has_default = False
    normalized: list[dict[str, Any]] = []
    for p in projects:
        if not isinstance(p, dict):
            continue
        pid = str(p.get("id") or "").strip()
        name = str(p.get("name") or "").strip()
        if not pid or not name:
            continue
        if pid == DEFAULT_PROJECT_ID:
            has_default = True
        normalized.append(
            {
                "id": pid,
                "name": name,
                "created_at": p.get("created_at") or datetime.now(timezone.utc).isoformat(),
            }
        )

    if not has_default:
        normalized.insert(
            0,
            {
                "id": DEFAULT_PROJECT_ID,
                "name": DEFAULT_PROJECT_NAME,
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    settings["projects"] = normalized

    active_project_id = str(settings.get("active_project_id") or "").strip()
    project_ids = {p["id"] for p in normalized}
    if active_project_id not in project_ids:
        active_project_id = DEFAULT_PROJECT_ID
    settings["active_project_id"] = active_project_id


def _ensure_project_ids(settings: Dict[str, Any]) -> None:
    for key in ("targets", "reaction_targets", "monitor_targets"):
        items = settings.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict) and "project_id" not in item:
                item["project_id"] = DEFAULT_PROJECT_ID


def _active_project_id(settings: Dict[str, Any]) -> str:
    pid = str(settings.get("active_project_id") or "").strip()
    if not pid:
        pid = DEFAULT_PROJECT_ID
    return pid


def _active_project(settings: Dict[str, Any]) -> Dict[str, Any]:
    pid = _active_project_id(settings)
    for p in settings.get("projects", []) or []:
        if isinstance(p, dict) and p.get("id") == pid:
            return p
    return {"id": DEFAULT_PROJECT_ID, "name": DEFAULT_PROJECT_NAME}


def _project_id_for(item: Dict[str, Any]) -> str:
    pid = str(item.get("project_id") or "").strip()
    return pid or DEFAULT_PROJECT_ID


def _filter_by_project(items: List[Dict[str, Any]], project_id: str) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if _project_id_for(item) == project_id:
            result.append(item)
    return result


def _filter_accounts_by_project(accounts: List[Dict[str, Any]], project_id: str) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for acc in accounts:
        if not isinstance(acc, dict):
            continue
        if _project_id_for(acc) == project_id:
            result.append(acc)
    return result


def _find_account_index(accounts: List[Dict[str, Any]], session_name: str, project_id: str) -> Optional[int]:
    for idx, acc in enumerate(accounts):
        if acc.get("session_name") != session_name:
            continue
        if _project_id_for(acc) == project_id:
            return idx
    return None


def _load_settings() -> Tuple[Dict[str, Any], str | None]:
    settings, err = load_json_with_error(SETTINGS_FILE, {})
    return _ensure_settings_schema(settings or {}), err


def _save_settings(settings: Dict[str, Any]) -> None:
    save_json(SETTINGS_FILE, settings)


def _normalize_tg_ref(value: str) -> str:
    v = (value or "").strip()
    if not v:
        return ""
    v = v.replace("https://t.me/", "").replace("http://t.me/", "").replace("t.me/", "")
    v = v.lstrip("@").strip()
    return v


def _upsert_profile_task(settings: Dict[str, Any], session_name: str, patch: Dict[str, Any]) -> None:
    tasks = settings.get("profile_tasks")
    if not isinstance(tasks, dict):
        tasks = {}
        settings["profile_tasks"] = tasks

    existing = tasks.get(session_name)
    if not isinstance(existing, dict):
        existing = {}

    existing.update({k: v for k, v in patch.items() if v is not None})
    existing["status"] = "pending"
    existing["error"] = ""
    existing["updated_at"] = datetime.now(timezone.utc).isoformat()
    tasks[session_name] = existing


def _load_accounts() -> Tuple[List[Dict[str, Any]], str | None]:
    accounts, err = load_json_with_error(ACCOUNTS_FILE, [])
    if not isinstance(accounts, list):
        accounts = []
    return accounts, err


def _save_accounts(accounts: List[Dict[str, Any]]) -> None:
    save_json(ACCOUNTS_FILE, accounts)


def _db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def _init_database() -> None:
    with _db_connect() as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                log_type TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                destination_chat_id INTEGER NOT NULL,
                channel_name TEXT,
                channel_username TEXT,
                source_channel_id INTEGER,
                post_id INTEGER NOT NULL,
                account_session_name TEXT,
                account_first_name TEXT,
                account_username TEXT,
                content TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS proxies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                ip TEXT,
                country TEXT,
                status TEXT,
                last_check TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS scenarios (
                chat_id TEXT PRIMARY KEY,
                script_content TEXT,
                current_index INTEGER DEFAULT 0,
                status TEXT DEFAULT 'stopped',
                last_run_time REAL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS post_scenarios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT,
                post_id INTEGER,
                current_index INTEGER DEFAULT 0,
                last_run_time REAL DEFAULT 0,
                UNIQUE(chat_id, post_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS triggers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT NOT NULL,
                trigger_phrase TEXT NOT NULL,
                answer_text TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_context (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT,
                msg_id INTEGER,
                session_name TEXT,
                created_at REAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS outbound_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT,
                reply_to_msg_id INTEGER,
                session_name TEXT,
                text TEXT,
                status TEXT DEFAULT 'pending'
            )
            """
        )
        conn.execute(
            """
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
            """
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_inbox_unique ON inbox_messages(session_name, chat_id, msg_id, direction)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_inbox_kind_unread ON inbox_messages(kind, is_read, id)")
        conn.commit()


def _load_config(section: str) -> Dict[str, str]:
    parser = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Файл config.ini не найден: {CONFIG_FILE}")
    parser.read(CONFIG_FILE)
    if section not in parser:
        raise KeyError(f"В config.ini не найдена секция [{section}].")
    return dict(parser[section])


def _telethon_credentials() -> Tuple[int, str]:
    cfg = _load_config("telethon_credentials")
    return int(cfg["api_id"]), cfg["api_hash"]


def _parse_proxy_tuple(url: str) -> tuple | None:
    try:
        protocol, rest = url.split("://", 1)
        auth, addr = rest.split("@", 1)
        user, password = auth.split(":", 1)
        host, port_s = addr.split(":", 1)
        return (protocol, host, int(port_s), True, user, password)
    except Exception:
        return None


async def _check_proxy_health(proxy_url: str) -> Dict[str, Any]:
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
                return {"status": "active", "ip": data.get("query"), "country": data.get("country")}
    except Exception:
        pass
    return {"status": "dead", "ip": None, "country": None}


async def _get_any_authorized_client() -> TelegramClient:
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    if not accounts:
        raise HTTPException(status_code=400, detail="Нет аккаунтов. Сначала добавьте хотя бы один.")

    api_id, api_hash = _telethon_credentials()

    for acc in accounts:
        if acc.get("status") == "banned":
            continue
        session_string = acc.get("session_string")
        if not session_string:
            continue
        proxy_tuple = _parse_proxy_tuple(acc["proxy_url"]) if acc.get("proxy_url") else None
        client = TelegramClient(StringSession(session_string), api_id, api_hash, proxy=proxy_tuple)
        await client.connect()
        if await client.is_user_authorized():
            return client
        await client.disconnect()

    raise HTTPException(status_code=400, detail="Нет авторизованных аккаунтов. Проверьте аккаунты.")


async def _resolve_channel_entity(client: TelegramClient, chat_input: str) -> Tuple[Any, str | None]:
    invite_link: str | None = None
    if "t.me/+" in chat_input or "t.me/joinchat/" in chat_input:
        invite_hash = chat_input.split("/")[-1].replace("+", "")
        invite_link = invite_hash
        invite_info = await client(CheckChatInviteRequest(invite_hash))
        entity = invite_info.chat
        return entity, invite_link

    entity = await client.get_entity(chat_input)
    return entity, None


async def _derive_target_chat_info(chat_input: str) -> Dict[str, Any]:
    client = await _get_any_authorized_client()
    try:
        entity, invite_link = await _resolve_channel_entity(client, chat_input)

        try:
            await client(JoinChannelRequest(entity))
        except Exception:
            pass

        chat_username = getattr(entity, "username", None)
        channel_id_str = f"-100{entity.id}"
        chat_name_to_save = getattr(entity, "title", None) or str(entity.id)

        comment_chat_id_str = channel_id_str
        try:
            full_channel = await client(GetFullChannelRequest(channel=entity))
            linked_chat_id_bare = getattr(full_channel.full_chat, "linked_chat_id", None)
            if linked_chat_id_bare:
                comment_chat_entity = await client.get_entity(PeerChannel(linked_chat_id_bare))
                try:
                    await client(JoinChannelRequest(comment_chat_entity))
                except Exception:
                    pass
                comment_chat_id_str = f"-100{comment_chat_entity.id}"
        except Exception:
            pass

        return {
            "chat_id": channel_id_str,
            "chat_username": chat_username,
            "linked_chat_id": comment_chat_id_str,
            "chat_name": chat_name_to_save,
            "invite_link": invite_link,
        }
    finally:
        if client.is_connected():
            await client.disconnect()


def _find_target_by_chat_id(
    settings: Dict[str, Any], chat_id: str, project_id: Optional[str] = None
) -> Tuple[int, Dict[str, Any]]:
    targets = settings.get("targets", [])
    pid = project_id or _active_project_id(settings)
    for i, t in enumerate(targets):
        if str(t.get("chat_id")) == str(chat_id) and _project_id_for(t) == pid:
            return i, t
    raise HTTPException(status_code=404, detail="Цель не найдена в текущем проекте")


def _find_reaction_target_by_chat_id(
    settings: Dict[str, Any], chat_id: str, project_id: Optional[str] = None
) -> Tuple[int, Dict[str, Any]]:
    targets = settings.get("reaction_targets", [])
    pid = project_id or _active_project_id(settings)
    for i, t in enumerate(targets):
        if str(t.get("chat_id")) == str(chat_id) and _project_id_for(t) == pid:
            return i, t
    raise HTTPException(status_code=404, detail="Цель реакций не найдена в текущем проекте")


def _find_monitor_target_by_chat_id(
    settings: Dict[str, Any], chat_id: str, project_id: Optional[str] = None
) -> Tuple[int, Dict[str, Any]]:
    targets = settings.get("monitor_targets", [])
    pid = project_id or _active_project_id(settings)
    for i, t in enumerate(targets):
        if str(t.get("chat_id")) == str(chat_id) and _project_id_for(t) == pid:
            return i, t
    raise HTTPException(status_code=404, detail="Цель мониторинга не найдена в текущем проекте")


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=303)


def _flash(request: Request, category: str, message: str) -> None:
    flashes = request.session.get("flashes") or []
    flashes.append({"category": category, "message": message})
    request.session["flashes"] = flashes


def _pop_flashes(request: Request) -> List[Dict[str, str]]:
    flashes = request.session.get("flashes") or []
    request.session["flashes"] = []
    return flashes


def _human_dt(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    else:
        s = str(value).strip()
        if not s:
            return "—"
        dt = None
        try:
            if s.replace(".", "", 1).isdigit():
                dt = datetime.fromtimestamp(float(s), tz=timezone.utc)
            else:
                dt = datetime.fromisoformat(s)
        except Exception:
            return s

    if dt.tzinfo is not None:
        try:
            dt = dt.astimezone()
        except Exception:
            pass
    return dt.strftime("%d.%m.%Y %H:%M:%S")


app = FastAPI(title="TG-комментатор (Web Admin)")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.filters["human_dt"] = _human_dt


@app.exception_handler(HTTPException)
async def _http_exception_handler(request: Request, exc: HTTPException):
    if _wants_html(request):
        title = f"Ошибка {exc.status_code}"
        detail = str(exc.detail) if exc.detail is not None else ""
        return templates.TemplateResponse(
            "error.html",
            _template_context(request, title=title, detail=detail),
            status_code=exc.status_code,
        )
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.exception_handler(RequestValidationError)
async def _validation_exception_handler(request: Request, exc: RequestValidationError):
    if _wants_html(request):
        return templates.TemplateResponse(
            "error.html",
            _template_context(
                request,
                title="Некорректный ввод",
                detail="Проверьте поля формы и попробуйте снова.",
            ),
            status_code=422,
        )
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error in admin_web", exc_info=exc)
    if _wants_html(request):
        return templates.TemplateResponse(
            "error.html",
            _template_context(
                request,
                title="Ошибка 500",
                detail="Неожиданная ошибка. Посмотрите логи контейнера admin_web.",
            ),
            status_code=500,
        )
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})


@app.on_event("startup")
async def _startup() -> None:
    ensure_data_dir()
    _init_database()
    if not os.path.exists(SETTINGS_FILE):
        _save_settings(_ensure_settings_schema({}))
    if not os.path.exists(ACCOUNTS_FILE):
        _save_accounts([])


@app.middleware("http")
async def _auth_middleware(request: Request, call_next):
    if ADMIN_WEB_DISABLE_AUTH:
        return await call_next(request)
    if request.url.path.startswith("/static"):
        return await call_next(request)
    if request.url.path in {"/login"}:
        return await call_next(request)
    if not request.session.get("user"):
        next_path = quote(request.url.path)
        return RedirectResponse(url=f"/login?next={next_path}", status_code=303)
    return await call_next(request)


# SessionMiddleware должен выполняться ДО auth middleware (последний добавленный middleware выполняется первым)
app.add_middleware(SessionMiddleware, secret_key=ADMIN_WEB_SECRET, same_site="lax")


def _template_context(request: Request, **extra: Any) -> Dict[str, Any]:
    inbox_counts = {"dialogs": 0, "quotes": 0}
    if request.session.get("user"):
        try:
            with _db_connect() as conn:
                inbox_counts["dialogs"] = conn.execute(
                    "SELECT COUNT(*) AS c FROM inbox_messages WHERE kind='dm' AND direction='in' AND is_read=0"
                ).fetchone()["c"]
                inbox_counts["quotes"] = conn.execute(
                    "SELECT COUNT(*) AS c FROM inbox_messages WHERE kind='quote' AND direction='in' AND is_read=0"
                ).fetchone()["c"]
        except Exception:
            inbox_counts = {"dialogs": 0, "quotes": 0}
    settings, _ = _load_settings()
    active_project = _active_project(settings)
    return {
        "request": request,
        "user": request.session.get("user"),
        "flashes": _pop_flashes(request),
        "static_version": STATIC_VERSION,
        "inbox_counts": inbox_counts,
        "projects": settings.get("projects", []) or [],
        "active_project": active_project,
        "active_project_id": active_project.get("id", DEFAULT_PROJECT_ID),
        **extra,
    }


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, next: str = "/"):
    return templates.TemplateResponse("login.html", _template_context(request, next=next))


@app.post("/login")
async def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    next: str = Form("/"),
):
    if username != ADMIN_WEB_USERNAME or password != ADMIN_WEB_PASSWORD:
        _flash(request, "danger", "Неверный логин или пароль.")
        return _redirect(f"/login?next={quote(next)}")

    request.session["user"] = {"username": username}
    _flash(request, "success", "Вы вошли в админку.")
    return _redirect(next or "/")


@app.post("/logout")
async def logout(request: Request):
    request.session.clear()
    return _redirect("/login")


@app.get("/projects", response_class=HTMLResponse)
async def projects_page(request: Request):
    settings, _ = _load_settings()
    accounts, _ = _load_accounts()
    project_id = _active_project_id(settings)

    project_stats: Dict[str, Dict[str, int]] = {}
    accounts_by_project: Dict[str, List[Dict[str, Any]]] = {}
    targets_by_project: Dict[str, List[Dict[str, Any]]] = {}
    reaction_by_project: Dict[str, List[Dict[str, Any]]] = {}
    monitor_by_project: Dict[str, List[Dict[str, Any]]] = {}
    for p in settings.get("projects", []) or []:
        pid = p.get("id")
        if not pid:
            continue
        accounts_by_project[pid] = _filter_accounts_by_project(accounts, pid)
        targets_by_project[pid] = _filter_by_project(settings.get("targets", []) or [], pid)
        reaction_by_project[pid] = _filter_by_project(settings.get("reaction_targets", []) or [], pid)
        monitor_by_project[pid] = _filter_by_project(settings.get("monitor_targets", []) or [], pid)
        project_stats[pid] = {
            "accounts": len(_filter_accounts_by_project(accounts, pid)),
            "targets": len(_filter_by_project(settings.get("targets", []) or [], pid)),
            "reaction_targets": len(_filter_by_project(settings.get("reaction_targets", []) or [], pid)),
            "monitor_targets": len(_filter_by_project(settings.get("monitor_targets", []) or [], pid)),
        }

    return templates.TemplateResponse(
        "projects.html",
        _template_context(
            request,
            projects=settings.get("projects", []) or [],
            project_stats=project_stats,
            accounts_by_project=accounts_by_project,
            targets_by_project=targets_by_project,
            reaction_by_project=reaction_by_project,
            monitor_by_project=monitor_by_project,
            active_project_id=project_id,
            default_project_id=DEFAULT_PROJECT_ID,
        ),
    )


@app.post("/projects/new")
async def projects_new(request: Request, name: str = Form(...)):
    name = name.strip()
    if not name:
        _flash(request, "warning", "Название проекта не может быть пустым.")
        return _redirect("/projects")

    settings, _ = _load_settings()
    project_id = uuid.uuid4().hex[:8]
    while any(p.get("id") == project_id for p in settings.get("projects", []) or []):
        project_id = uuid.uuid4().hex[:8]

    settings.setdefault("projects", []).append(
        {"id": project_id, "name": name, "created_at": datetime.now(timezone.utc).isoformat()}
    )
    settings["active_project_id"] = project_id
    _save_settings(settings)
    _flash(request, "success", f"Проект «{name}» создан и выбран.")
    return _redirect("/projects")


@app.post("/projects/select")
async def projects_select(request: Request, project_id: str = Form(...)):
    settings, _ = _load_settings()
    project_id = (project_id or "").strip()
    if not project_id or not any(p.get("id") == project_id for p in settings.get("projects", []) or []):
        _flash(request, "warning", "Проект не найден.")
        return _redirect("/")
    settings["active_project_id"] = project_id
    _save_settings(settings)
    _flash(request, "success", "Проект переключён.")
    return _redirect(request.headers.get("referer") or "/")


@app.post("/projects/{project_id}/rename")
async def projects_rename(request: Request, project_id: str, name: str = Form(...)):
    name = name.strip()
    if not name:
        _flash(request, "warning", "Название проекта не может быть пустым.")
        return _redirect("/projects")
    settings, _ = _load_settings()
    for p in settings.get("projects", []) or []:
        if p.get("id") == project_id:
            p["name"] = name
            _save_settings(settings)
            _flash(request, "success", "Название проекта обновлено.")
            return _redirect("/projects")
    _flash(request, "warning", "Проект не найден.")
    return _redirect("/projects")


@app.post("/projects/{project_id}/delete")
async def projects_delete(request: Request, project_id: str):
    if project_id == DEFAULT_PROJECT_ID:
        _flash(request, "warning", "Стандартный проект удалить нельзя.")
        return _redirect("/projects")

    settings, _ = _load_settings()
    projects = [p for p in settings.get("projects", []) or [] if p.get("id") != project_id]
    if len(projects) == len(settings.get("projects", []) or []):
        _flash(request, "warning", "Проект не найден.")
        return _redirect("/projects")

    settings["projects"] = projects
    for key in ("targets", "reaction_targets", "monitor_targets"):
        items = settings.get(key, []) or []
        settings[key] = [t for t in items if _project_id_for(t) != project_id]
    settings["manual_queue"] = [
        t for t in (settings.get("manual_queue", []) or []) if t.get("project_id") != project_id
    ]

    if settings.get("active_project_id") == project_id:
        settings["active_project_id"] = DEFAULT_PROJECT_ID

    _save_settings(settings)

    accounts, _ = _load_accounts()
    accounts = [a for a in accounts if _project_id_for(a) != project_id]
    _save_accounts(accounts)

    _flash(request, "success", "Проект удалён.")
    return _redirect("/projects")


@app.post("/projects/move")
async def projects_move(
    request: Request,
    source_project_id: str = Form(...),
    dest_project_id: str = Form(...),
    move_accounts: Optional[str] = Form(None),
    move_targets: Optional[str] = Form(None),
    move_reaction_targets: Optional[str] = Form(None),
    move_monitor_targets: Optional[str] = Form(None),
    move_manual_queue: Optional[str] = Form(None),
    selected_accounts: Optional[List[str]] = Form(None),
    selected_targets: Optional[List[str]] = Form(None),
    selected_reaction_targets: Optional[List[str]] = Form(None),
    selected_monitor_targets: Optional[List[str]] = Form(None),
):
    source_project_id = (source_project_id or "").strip()
    dest_project_id = (dest_project_id or "").strip()
    if not source_project_id or not dest_project_id:
        _flash(request, "warning", "Нужно выбрать проекты.")
        return _redirect("/projects")
    if source_project_id == dest_project_id:
        _flash(request, "warning", "Проекты совпадают — перемещение не требуется.")
        return _redirect("/projects")

    settings, _ = _load_settings()
    project_ids = {p.get("id") for p in settings.get("projects", []) or [] if p.get("id")}
    if source_project_id not in project_ids or dest_project_id not in project_ids:
        _flash(request, "warning", "Проект не найден.")
        return _redirect("/projects")

    if not any([move_accounts, move_targets, move_reaction_targets, move_monitor_targets, move_manual_queue]):
        _flash(request, "warning", "Выберите, что переносить.")
        return _redirect("/projects")

    accounts, _ = _load_accounts()

    moved_accounts = 0
    moved_targets = 0
    moved_reactions = 0
    moved_monitor = 0
    moved_manual = 0

    if move_accounts:
        selected_set = {s for s in (selected_accounts or []) if s}
        for acc in accounts:
            if _project_id_for(acc) != source_project_id:
                continue
            if selected_set and acc.get("session_name") not in selected_set:
                continue
            acc["project_id"] = dest_project_id
            moved_accounts += 1

    moved_comment_targets: List[Dict[str, Any]] = []
    moved_reaction_targets: List[Dict[str, Any]] = []
    moved_monitor_targets: List[Dict[str, Any]] = []

    if move_targets:
        selected_set = {s for s in (selected_targets or []) if s}
        for t in settings.get("targets", []) or []:
            if _project_id_for(t) != source_project_id:
                continue
            if selected_set and str(t.get("chat_id")) not in selected_set:
                continue
            t["project_id"] = dest_project_id
            moved_targets += 1
            moved_comment_targets.append(t)

    if move_reaction_targets:
        selected_set = {s for s in (selected_reaction_targets or []) if s}
        for t in settings.get("reaction_targets", []) or []:
            if _project_id_for(t) != source_project_id:
                continue
            if selected_set and str(t.get("chat_id")) not in selected_set:
                continue
            t["project_id"] = dest_project_id
            moved_reactions += 1
            moved_reaction_targets.append(t)

    if move_monitor_targets:
        selected_set = {s for s in (selected_monitor_targets or []) if s}
        for t in settings.get("monitor_targets", []) or []:
            if _project_id_for(t) != source_project_id:
                continue
            if selected_set and str(t.get("chat_id")) not in selected_set:
                continue
            t["project_id"] = dest_project_id
            moved_monitor += 1
            moved_monitor_targets.append(t)

    if move_manual_queue:
        for task in settings.get("manual_queue", []) or []:
            if _project_id_for(task) == source_project_id:
                task["project_id"] = dest_project_id
                moved_manual += 1

    dest_sessions = {
        a.get("session_name")
        for a in accounts
        if a.get("session_name") and _project_id_for(a) == dest_project_id
    }

    for t in moved_comment_targets:
        assigned = [s for s in (t.get("assigned_accounts") or []) if s in dest_sessions]
        t["assigned_accounts"] = assigned
        prompts = t.get("prompts")
        if isinstance(prompts, dict):
            new_prompts: Dict[str, Any] = {}
            if "default" in prompts:
                new_prompts["default"] = prompts.get("default")
            for k, v in prompts.items():
                if k == "default":
                    continue
                if k in dest_sessions:
                    new_prompts[k] = v
            t["prompts"] = new_prompts

    for t in moved_reaction_targets:
        assigned = [s for s in (t.get("assigned_accounts") or []) if s in dest_sessions]
        t["assigned_accounts"] = assigned

    for t in moved_monitor_targets:
        assigned = [s for s in (t.get("assigned_accounts") or []) if s in dest_sessions]
        t["assigned_accounts"] = assigned

    _save_settings(settings)
    _save_accounts(accounts)

    _flash(
        request,
        "success",
        "Перемещено: "
        f"аккаунтов {moved_accounts}, "
        f"комментирование {moved_targets}, "
        f"реакции {moved_reactions}, "
        f"мониторинг {moved_monitor}, "
        f"ручные задачи {moved_manual}.",
    )
    return _redirect("/projects")


@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    settings, settings_err = _load_settings()
    accounts, accounts_err = _load_accounts()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)

    with _db_connect() as conn:
        proxies_total = conn.execute("SELECT COUNT(*) AS c FROM proxies").fetchone()["c"]
        proxies_active = conn.execute("SELECT COUNT(*) AS c FROM proxies WHERE status='active'").fetchone()["c"]
        triggers_total = conn.execute("SELECT COUNT(*) AS c FROM triggers").fetchone()["c"]
        scenarios_total = conn.execute("SELECT COUNT(*) AS c FROM scenarios").fetchone()["c"]

    return templates.TemplateResponse(
        "dashboard.html",
        _template_context(
            request,
            settings=settings,
            settings_err=settings_err,
            accounts=accounts,
            accounts_err=accounts_err,
            proxies_total=proxies_total,
            proxies_active=proxies_active,
            triggers_total=triggers_total,
            scenarios_total=scenarios_total,
        ),
    )


@app.get("/guide", response_class=HTMLResponse)
async def guide_page(request: Request):
    settings, _ = _load_settings()
    accounts, _ = _load_accounts()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)

    provider = settings.get("ai_provider", "deepseek")
    api_keys = settings.get("api_keys", {}) or {}
    ai_ready = bool(api_keys.get(provider))

    accounts_ready = len(accounts) > 0
    targets_ready = len(_filter_by_project(settings.get("targets", []) or [], project_id)) > 0
    running_ready = settings.get("status") == "running"

    with _db_connect() as conn:
        proxies_active = conn.execute("SELECT COUNT(*) AS c FROM proxies WHERE status='active'").fetchone()["c"]

    steps = [
        {
            "title": "1) Укажите AI провайдера и API ключ",
            "hint": f"Сейчас выбран {provider.upper()}. Нужен ключ для выбранного провайдера, иначе генерация не пойдёт.",
            "done": ai_ready,
            "optional": False,
            "href": "/settings/ai",
            "action": "Открыть AI",
        },
        {
            "title": "2) Добавьте аккаунт(ы) Telegram",
            "hint": "Через session string или вход по телефону. Проверьте статусы, настройте сон/прокси по необходимости.",
            "done": accounts_ready,
            "optional": False,
            "href": "/accounts",
            "action": "Открыть аккаунты",
        },
        {
            "title": "3) Добавьте 1–2 цели комментирования",
            "hint": "Укажите канал/чат, промпт по умолчанию, задержки и лимиты. Дальше можно настроить триггеры/сценарии.",
            "done": targets_ready,
            "optional": False,
            "href": "/targets",
            "action": "Открыть цели",
        },
        {
            "title": "4) (Опционально) Добавьте прокси и назначьте на аккаунты",
            "hint": "Полезно для распределения трафика и стабильности. Прокси проверяются и хранятся в базе.",
            "done": proxies_active > 0,
            "optional": True,
            "href": "/proxies",
            "action": "Открыть прокси",
        },
        {
            "title": "5) Запустите комментатор",
            "hint": "После старта скрипт подхватывает настройки и начинает обработку новых постов.",
            "done": running_ready,
            "optional": False,
            "href": "/",
            "action": "На дашборд",
        },
        {
            "title": "6) Проверьте работу и смотрите статистику",
            "hint": "Статистика показывает, кто и где оставлял комментарии/реакции. Можно выгрузить в Excel.",
            "done": False,
            "optional": True,
            "href": "/stats",
            "action": "Открыть статистику",
        },
    ]

    return templates.TemplateResponse("guide.html", _template_context(request, steps=steps))


@app.post("/status/start")
async def status_start(request: Request):
    settings, _ = _load_settings()
    settings["status"] = "running"
    _save_settings(settings)
    _flash(request, "success", "Комментатор запущен (commentator.py подхватит в течение нескольких секунд).")
    return _redirect("/")


@app.post("/status/stop")
async def status_stop(request: Request):
    settings, _ = _load_settings()
    settings["status"] = "stopped"
    _save_settings(settings)
    _flash(request, "success", "Комментатор остановлен.")
    return _redirect("/")


@app.get("/settings/ai", response_class=HTMLResponse)
async def ai_settings_page(request: Request):
    settings, settings_err = _load_settings()
    keys = settings.get("api_keys", {})
    return templates.TemplateResponse(
        "ai_settings.html",
        _template_context(
            request,
            settings=settings,
            settings_err=settings_err,
            api_keys_masked={k: _mask_secret(v) for k, v in keys.items()},
        ),
    )


@app.get("/settings/ai/models", response_class=HTMLResponse)
async def ai_models_page(request: Request, provider: str = "", q: str = ""):
    settings, settings_err = _load_settings()
    provider = provider.strip() or settings.get("ai_provider", "deepseek")
    if provider not in {"gemini", "openai", "openrouter", "deepseek"}:
        raise HTTPException(status_code=400, detail="Некорректный провайдер")

    api_key = (settings.get("api_keys", {}) or {}).get(provider)
    query = q.strip().lower()

    models: List[Dict[str, str]] = []
    models_err: str | None = None

    if provider != "openrouter" and not api_key:
        models_err = f"Для провайдера {provider.upper()} не задан API ключ."
    else:
        try:
            if provider == "gemini":
                async with genai.Client(api_key=api_key, http_options={"timeout": 10_000}).aio as aclient:
                    pager = await aclient.models.list(config={"page_size": 200})
                    items: List[Any] = []
                    if hasattr(pager, "__aiter__"):
                        async for item in pager:
                            items.append(item)
                            if len(items) >= 200:
                                break
                    else:
                        for item in pager:
                            items.append(item)
                            if len(items) >= 200:
                                break

                for item in items:
                    raw_name = getattr(item, "name", None) or str(item)
                    model_id = raw_name.split("/")[-1] if isinstance(raw_name, str) else str(raw_name)
                    if query and query not in model_id.lower() and query not in str(raw_name).lower():
                        continue
                    methods = getattr(item, "supported_generation_methods", None)
                    meta = ""
                    if isinstance(methods, (list, tuple)) and methods:
                        meta = ", ".join(str(m) for m in methods)
                    models.append({"id": model_id, "raw": str(raw_name), "meta": meta})
            elif provider == "openrouter":
                headers = {}
                if api_key:
                    headers["Authorization"] = f"Bearer {api_key}"
                async with httpx.AsyncClient(timeout=10.0) as http_client:
                    resp = await http_client.get("https://openrouter.ai/api/v1/models", headers=headers)
                    resp.raise_for_status()
                    payload = resp.json()

                items = payload.get("data", []) if isinstance(payload, dict) else []
                limit = 500 if query else 200
                count = 0
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    model_id = str(item.get("id") or "").strip()
                    if not model_id:
                        continue
                    name = str(item.get("name") or model_id)
                    haystack = f"{model_id} {name}".lower()
                    if query and query not in haystack:
                        continue

                    meta_parts: List[str] = []
                    ctx = item.get("context_length")
                    if isinstance(ctx, int) and ctx > 0:
                        meta_parts.append(f"ctx={ctx}")
                    top = item.get("top_provider") if isinstance(item.get("top_provider"), dict) else {}
                    max_out = top.get("max_completion_tokens") if isinstance(top, dict) else None
                    if isinstance(max_out, int) and max_out > 0:
                        meta_parts.append(f"max_out={max_out}")
                    params = item.get("supported_parameters")
                    if isinstance(params, list) and params:
                        short = ", ".join(str(p) for p in params[:10])
                        meta_parts.append(f"params={short}{'…' if len(params) > 10 else ''}")

                    models.append({"id": model_id, "raw": name, "meta": " · ".join(meta_parts)})
                    count += 1
                    if count >= limit:
                        break
            else:
                base_url = "https://api.deepseek.com" if provider == "deepseek" else None
                client = openai.AsyncOpenAI(api_key=api_key, base_url=base_url, timeout=10.0)
                resp = await client.models.list()
                for item in getattr(resp, "data", []) or []:
                    model_id = getattr(item, "id", None) or str(item)
                    if query and query not in str(model_id).lower():
                        continue
                    models.append({"id": str(model_id), "raw": str(model_id), "meta": ""})
        except Exception as e:
            models_err = str(e)

    return templates.TemplateResponse(
        "ai_models.html",
        _template_context(
            request,
            settings=settings,
            settings_err=settings_err,
            provider=provider,
            q=q,
            models=models,
            models_err=models_err,
        ),
    )


@app.post("/settings/ai/provider")
async def ai_settings_provider(request: Request, ai_provider: str = Form(...)):
    if ai_provider not in {"gemini", "openai", "openrouter", "deepseek"}:
        raise HTTPException(status_code=400, detail="Некорректный провайдер")
    settings, _ = _load_settings()
    settings["ai_provider"] = ai_provider
    _save_settings(settings)
    _flash(request, "success", f"Провайдер по умолчанию: {ai_provider.upper()}")
    return _redirect("/settings/ai")


@app.post("/settings/ai/api-keys")
async def ai_settings_api_keys(
    request: Request,
    gemini_key: str = Form(""),
    openai_key: str = Form(""),
    openrouter_key: str = Form(""),
    deepseek_key: str = Form(""),
    clear_gemini: Optional[str] = Form(None),
    clear_openai: Optional[str] = Form(None),
    clear_openrouter: Optional[str] = Form(None),
    clear_deepseek: Optional[str] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    settings.setdefault("api_keys", {})

    if clear_gemini:
        settings["api_keys"].pop("gemini", None)
    elif gemini_key.strip():
        settings["api_keys"]["gemini"] = gemini_key.strip()

    if clear_openai:
        settings["api_keys"].pop("openai", None)
    elif openai_key.strip():
        settings["api_keys"]["openai"] = openai_key.strip()

    if clear_openrouter:
        settings["api_keys"].pop("openrouter", None)
    elif openrouter_key.strip():
        settings["api_keys"]["openrouter"] = openrouter_key.strip()

    if clear_deepseek:
        settings["api_keys"].pop("deepseek", None)
    elif deepseek_key.strip():
        settings["api_keys"]["deepseek"] = deepseek_key.strip()

    _save_settings(settings)
    _flash(request, "success", "API ключи обновлены.")
    return _redirect("/settings/ai")


@app.post("/settings/ai/models")
async def ai_settings_models(
    request: Request,
    openai_chat: str = Form(""),
    openai_eval: str = Form(""),
    openai_image: str = Form(""),
    openrouter_chat: str = Form(""),
    openrouter_eval: str = Form(""),
    deepseek_chat: str = Form(""),
    deepseek_eval: str = Form(""),
    gemini_chat: str = Form(""),
    gemini_eval: str = Form(""),
    gemini_names: str = Form(""),
    reset_models: Optional[str] = Form(None),
):
    settings, _ = _load_settings()

    if reset_models:
        settings["models"] = dict(DEFAULT_MODELS)
        _save_settings(settings)
        _flash(request, "success", "Модели сброшены к рекомендуемым.")
        return _redirect("/settings/ai")

    models = settings.get("models")
    if not isinstance(models, dict):
        models = {}

    def set_model(value: str, key: str) -> None:
        v = value.strip()
        if v:
            models[key] = v

    set_model(openai_chat, "openai_chat")
    set_model(openai_eval, "openai_eval")
    set_model(openai_image, "openai_image")
    set_model(openrouter_chat, "openrouter_chat")
    set_model(openrouter_eval, "openrouter_eval")
    set_model(deepseek_chat, "deepseek_chat")
    set_model(deepseek_eval, "deepseek_eval")
    set_model(gemini_chat, "gemini_chat")
    set_model(gemini_eval, "gemini_eval")
    set_model(gemini_names, "gemini_names")

    settings["models"] = models
    _save_settings(settings)
    _flash(request, "success", "Модели обновлены.")
    return _redirect("/settings/ai")


@app.get("/settings/humanization", response_class=HTMLResponse)
async def humanization_page(request: Request):
    settings, settings_err = _load_settings()
    h = settings.get("humanization", {}) or {}
    return templates.TemplateResponse(
        "humanization.html",
        _template_context(request, settings_err=settings_err, h=h, settings=settings),
    )


@app.post("/settings/humanization")
async def humanization_save(
    request: Request,
    temperature: str = Form(""),
    repetition_penalty: str = Form("0"),
    typo_chance: str = Form("0"),
    lowercase_chance: str = Form("80"),
    split_chance: str = Form("60"),
    comma_skip_chance: str = Form("30"),
    max_words: str = Form("20"),
    max_tokens: str = Form("60"),
    similarity_threshold: str = Form("0.78"),
    similarity_max_retries: str = Form("1"),
    custom_rules: str = Form(""),
):
    settings, _ = _load_settings()
    settings.setdefault("humanization", {})

    settings["humanization"]["temperature"] = _parse_float_field(
        request,
        temperature,
        default=None,
        label="Температура",
        min_value=0.0,
        max_value=2.0,
    )
    settings["humanization"]["repetition_penalty"] = _parse_int_field(
        request, repetition_penalty, default=0, label="Штраф повторов", min_value=0, max_value=100
    )
    settings["humanization"]["typo_chance"] = _parse_int_field(
        request, typo_chance, default=0, label="Опечатки", min_value=0, max_value=100
    )
    settings["humanization"]["lowercase_chance"] = _parse_int_field(
        request, lowercase_chance, default=80, label="lowercase", min_value=0, max_value=100
    )
    settings["humanization"]["split_chance"] = _parse_int_field(
        request, split_chance, default=60, label="Разбив", min_value=0, max_value=100
    )
    settings["humanization"]["comma_skip_chance"] = _parse_int_field(
        request, comma_skip_chance, default=30, label="Пропуск запятых", min_value=0, max_value=100
    )
    settings["humanization"]["max_words"] = _parse_int_field(request, max_words, default=20, label="Макс. слов", min_value=0)
    settings["humanization"]["max_tokens"] = _parse_int_field(
        request, max_tokens, default=60, label="Лимит ответа (токены)", min_value=1
    )
    settings["humanization"]["similarity_threshold"] = _parse_float_field(
        request,
        similarity_threshold,
        default=0.78,
        label="Порог схожести",
        min_value=0.0,
        max_value=1.0,
    )
    settings["humanization"]["similarity_max_retries"] = _parse_int_field(
        request,
        similarity_max_retries,
        default=1,
        label="Перегенераций",
        min_value=0,
        max_value=3,
    )

    rules = custom_rules.strip()
    if rules:
        settings["humanization"]["custom_rules"] = rules
    else:
        settings["humanization"].pop("custom_rules", None)

    _save_settings(settings)
    _flash(request, "success", "Очеловечивание обновлено.")
    return _redirect("/settings/humanization")


@app.get("/settings/blacklist", response_class=HTMLResponse)
async def blacklist_page(request: Request):
    settings, settings_err = _load_settings()
    blacklist = settings.get("blacklist", []) or []
    return templates.TemplateResponse(
        "blacklist.html",
        _template_context(request, settings_err=settings_err, blacklist=blacklist),
    )


@app.post("/settings/blacklist/add")
async def blacklist_add(request: Request, words: str = Form(...)):
    settings, _ = _load_settings()
    settings.setdefault("blacklist", [])
    existing_lower = {w.lower() for w in settings["blacklist"] if isinstance(w, str)}
    added = 0
    for w in words.replace("\n", ",").split(","):
        w = w.strip()
        if not w:
            continue
        if w.lower() in existing_lower:
            continue
        settings["blacklist"].append(w)
        existing_lower.add(w.lower())
        added += 1
    _save_settings(settings)
    _flash(request, "success", f"Добавлено слов: {added}")
    return _redirect("/settings/blacklist")


@app.post("/settings/blacklist/delete")
async def blacklist_delete(request: Request, word: str = Form(...)):
    settings, _ = _load_settings()
    settings.setdefault("blacklist", [])
    settings["blacklist"] = [w for w in settings["blacklist"] if str(w) != word]
    _save_settings(settings)
    _flash(request, "success", f"Удалено: {word}")
    return _redirect("/settings/blacklist")


@app.post("/settings/blacklist/clear")
async def blacklist_clear(request: Request):
    settings, _ = _load_settings()
    settings["blacklist"] = []
    _save_settings(settings)
    _flash(request, "success", "Чёрный список очищен.")
    return _redirect("/settings/blacklist")


@app.get("/accounts", response_class=HTMLResponse)
async def accounts_page(request: Request):
    accounts, accounts_err = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    personas = settings.get("personas", {}) or {}
    persona_names = {pid: pdata.get("name") for pid, pdata in personas.items()}

    with _db_connect() as conn:
        proxies = conn.execute(
            "SELECT id, url, ip, country, status FROM proxies ORDER BY id DESC"
        ).fetchall()

    return templates.TemplateResponse(
        "accounts.html",
        _template_context(
            request,
            accounts=accounts,
            accounts_err=accounts_err,
            persona_names=persona_names,
            proxies=proxies,
        ),
    )


@app.post("/accounts/check")
async def accounts_check(request: Request):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    if not _filter_accounts_by_project(accounts, project_id):
        _flash(request, "warning", "Нет аккаунтов для проверки.")
        return _redirect("/accounts")

    api_id, api_hash = _telethon_credentials()

    ok = 0
    banned = 0
    unauthorized = 0
    errors = 0

    for acc in accounts:
        if _project_id_for(acc) != project_id:
            continue
        session_name = acc.get("session_name") or "N/A"
        session_string = acc.get("session_string")
        if not session_string:
            errors += 1
            continue
        if acc.get("status") == "banned":
            banned += 1
            continue

        proxy_tuple = _parse_proxy_tuple(acc["proxy_url"]) if acc.get("proxy_url") else None
        client = TelegramClient(StringSession(session_string), api_id, api_hash, proxy=proxy_tuple)

        try:
            await client.connect()
            if not await client.is_user_authorized():
                acc["status"] = "unauthorized"
                unauthorized += 1
                continue
            me = await client.get_me()
            acc.update(
                {
                    "user_id": me.id,
                    "first_name": me.first_name,
                    "last_name": me.last_name or "",
                    "username": me.username or "",
                    "status": "active",
                }
            )
            ok += 1
        except UserDeactivatedBanError:
            acc["status"] = "banned"
            banned += 1
        except Exception:
            errors += 1
        finally:
            if client.is_connected():
                await client.disconnect()

    _save_accounts(accounts)
    _flash(
        request,
        "success",
        f"Проверка завершена: OK={ok}, banned={banned}, unauthorized={unauthorized}, errors={errors}",
    )
    return _redirect("/accounts")


@app.get("/accounts/new", response_class=HTMLResponse)
async def account_new_page(request: Request):
    settings, _ = _load_settings()
    personas = settings.get("personas", {}) or {}
    with _db_connect() as conn:
        proxies = conn.execute(
            "SELECT id, ip, country, url FROM proxies WHERE status='active' ORDER BY id DESC"
        ).fetchall()
    return templates.TemplateResponse(
        "account_new.html",
        _template_context(request, personas=personas, proxies=proxies),
    )


@app.post("/accounts/new/session")
async def account_new_session(
    request: Request,
    session_name: str = Form(...),
    session_string: str = Form(...),
    proxy_id: str = Form(""),
    persona_id: str = Form(""),
):
    session_name = session_name.strip()
    session_string = session_string.strip()
    if not session_name:
        raise HTTPException(status_code=400, detail="session_name пустой")
    if not session_string:
        raise HTTPException(status_code=400, detail="session_string пустой")

    accounts, _ = _load_accounts()
    if any(a.get("session_name") == session_name for a in accounts):
        raise HTTPException(status_code=400, detail="Аккаунт с таким session_name уже существует")

    proxy_url: str | None = None
    if proxy_id.strip():
        try:
            proxy_id_int = int(proxy_id)
        except ValueError:
            proxy_id_int = None
            _flash(request, "warning", "Прокси: некорректный ID, будет добавлено без прокси.")
        if proxy_id_int is not None:
            with _db_connect() as conn:
                row = conn.execute("SELECT url FROM proxies WHERE id = ?", (proxy_id_int,)).fetchone()
                if row:
                    proxy_url = row["url"]

    api_id, api_hash = _telethon_credentials()
    proxy_tuple = _parse_proxy_tuple(proxy_url) if proxy_url else None
    client = TelegramClient(StringSession(session_string), api_id, api_hash, proxy=proxy_tuple)
    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise HTTPException(status_code=400, detail="Сессия не авторизована")
        me = await client.get_me()
    finally:
        if client.is_connected():
            await client.disconnect()

    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    new_acc: Dict[str, Any] = {
        "session_name": session_name,
        "session_string": session_string,
        "user_id": me.id,
        "first_name": me.first_name,
        "last_name": me.last_name or "",
        "username": me.username or "",
        "status": "active",
        "project_id": project_id,
    }
    if proxy_url:
        new_acc["proxy_url"] = proxy_url
    if persona_id.strip():
        new_acc["persona_id"] = persona_id.strip()

    accounts.append(new_acc)
    _save_accounts(accounts)

    _flash(request, "success", f"Аккаунт '{session_name}' добавлен.")
    return _redirect("/accounts")


@dataclass
class _PhoneLoginState:
    token: str
    created_at: float
    client: TelegramClient
    session_name: str
    phone: str
    phone_code_hash: str
    proxy_url: str | None


PHONE_LOGINS: Dict[str, _PhoneLoginState] = {}


def _phone_logins_gc(max_age_seconds: int = 10 * 60) -> None:
    now = time.time()
    for token, st in list(PHONE_LOGINS.items()):
        if now - st.created_at > max_age_seconds:
            try:
                if st.client.is_connected():
                    asyncio.create_task(st.client.disconnect())
            except Exception:
                pass
            PHONE_LOGINS.pop(token, None)


@app.post("/accounts/new/phone/start", response_class=HTMLResponse)
async def account_new_phone_start(
    request: Request,
    session_name: str = Form(...),
    phone: str = Form(...),
    proxy_id: str = Form(""),
):
    _phone_logins_gc()

    session_name = session_name.strip()
    phone = phone.strip()
    if not session_name or not phone:
        raise HTTPException(status_code=400, detail="Нужно указать session_name и phone")

    accounts, _ = _load_accounts()
    if any(a.get("session_name") == session_name for a in accounts):
        raise HTTPException(status_code=400, detail="Аккаунт с таким session_name уже существует")

    proxy_url: str | None = None
    if proxy_id.strip():
        try:
            proxy_id_int = int(proxy_id)
        except ValueError:
            proxy_id_int = None
            _flash(request, "warning", "Прокси: некорректный ID, вход будет без прокси.")
        if proxy_id_int is not None:
            with _db_connect() as conn:
                row = conn.execute("SELECT url FROM proxies WHERE id = ?", (proxy_id_int,)).fetchone()
                if row:
                    proxy_url = row["url"]

    api_id, api_hash = _telethon_credentials()
    proxy_tuple = _parse_proxy_tuple(proxy_url) if proxy_url else None

    client = TelegramClient(StringSession(), api_id, api_hash, proxy=proxy_tuple)
    await client.connect()

    try:
        sent_code = await client.send_code_request(phone)
    except RPCError as e:
        await client.disconnect()
        raise HTTPException(status_code=400, detail=f"Ошибка Telegram API: {e}") from e

    token = uuid.uuid4().hex
    PHONE_LOGINS[token] = _PhoneLoginState(
        token=token,
        created_at=time.time(),
        client=client,
        session_name=session_name,
        phone=phone,
        phone_code_hash=sent_code.phone_code_hash,
        proxy_url=proxy_url,
    )

    return templates.TemplateResponse(
        "account_phone_code.html",
        _template_context(request, token=token, session_name=session_name, phone=phone),
    )


@app.post("/accounts/new/phone/{token}/cancel")
async def account_new_phone_cancel(request: Request, token: str):
    st = PHONE_LOGINS.pop(token, None)
    if st:
        try:
            if st.client.is_connected():
                await st.client.disconnect()
        except Exception:
            pass
    _flash(request, "success", "Вход по телефону отменён.")
    return _redirect("/accounts/new")


@app.post("/accounts/new/phone/{token}/code", response_class=HTMLResponse)
async def account_new_phone_code(request: Request, token: str, code: str = Form(...)):
    st = PHONE_LOGINS.get(token)
    if not st:
        _flash(request, "danger", "Сессия входа устарела. Начните заново.")
        return _redirect("/accounts/new")

    code = code.strip()
    try:
        await st.client.sign_in(st.phone, code, phone_code_hash=st.phone_code_hash)
    except (PhoneCodeInvalidError, PhoneCodeExpiredError):
        _flash(request, "danger", "Неверный или истекший код. Попробуйте снова.")
        return templates.TemplateResponse(
            "account_phone_code.html",
            _template_context(request, token=token, session_name=st.session_name, phone=st.phone),
        )
    except SessionPasswordNeededError:
        return templates.TemplateResponse(
            "account_phone_password.html",
            _template_context(request, token=token, session_name=st.session_name, phone=st.phone),
        )
    except Exception as e:
        PHONE_LOGINS.pop(token, None)
        try:
            if st.client.is_connected():
                await st.client.disconnect()
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=f"Ошибка входа: {e}") from e

    return await _finalize_phone_login(request, token)


@app.post("/accounts/new/phone/{token}/password", response_class=HTMLResponse)
async def account_new_phone_password(request: Request, token: str, password: str = Form(...)):
    st = PHONE_LOGINS.get(token)
    if not st:
        _flash(request, "danger", "Сессия входа устарела. Начните заново.")
        return _redirect("/accounts/new")

    try:
        await st.client.sign_in(password=password.strip())
    except PasswordHashInvalidError:
        _flash(request, "danger", "Неверный пароль 2FA. Попробуйте снова.")
        return templates.TemplateResponse(
            "account_phone_password.html",
            _template_context(request, token=token, session_name=st.session_name, phone=st.phone),
        )
    except Exception as e:
        PHONE_LOGINS.pop(token, None)
        try:
            if st.client.is_connected():
                await st.client.disconnect()
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=f"Ошибка 2FA: {e}") from e

    return await _finalize_phone_login(request, token)


async def _finalize_phone_login(request: Request, token: str):
    st = PHONE_LOGINS.pop(token, None)
    if not st:
        _flash(request, "danger", "Сессия входа устарела. Начните заново.")
        return _redirect("/accounts/new")

    try:
        me = await st.client.get_me()
        session_string = st.client.session.save()
    finally:
        try:
            if st.client.is_connected():
                await st.client.disconnect()
        except Exception:
            pass

    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    accounts, _ = _load_accounts()
    accounts.append(
        {
            "session_name": st.session_name,
            "session_string": session_string,
            "user_id": me.id,
            "first_name": me.first_name,
            "last_name": me.last_name or "",
            "username": me.username or "",
            "status": "active",
            "project_id": project_id,
            **({"proxy_url": st.proxy_url} if st.proxy_url else {}),
        }
    )
    _save_accounts(accounts)

    _flash(request, "success", f"Аккаунт '{st.session_name}' добавлен.")
    return _redirect("/accounts")


@app.get("/accounts/{session_name}", response_class=HTMLResponse)
async def account_edit_page(request: Request, session_name: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    account = next(
        (a for a in accounts if a.get("session_name") == session_name and _project_id_for(a) == project_id),
        None,
    )
    if not account:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    def _is_account_awake(acc: Dict[str, Any], hour: int) -> bool:
        ss = acc.get("sleep_settings") or {}
        try:
            start_hour = int(ss.get("start_hour", 8) or 8)
        except (TypeError, ValueError):
            start_hour = 8
        try:
            end_hour = int(ss.get("end_hour", 23) or 23)
        except (TypeError, ValueError):
            end_hour = 23

        start_hour = max(0, min(23, start_hour))
        end_hour = max(0, min(23, end_hour))

        if start_hour == end_hour:
            return True
        if start_hour < end_hour:
            return start_hour <= hour < end_hour
        return hour >= start_hour or hour < end_hour

    personas = settings.get("personas", {}) or {}
    profile_task = None
    tasks = settings.get("profile_tasks")
    if isinstance(tasks, dict):
        profile_task = tasks.get(session_name)

    with _db_connect() as conn:
        proxies = conn.execute(
            "SELECT id, ip, country, url FROM proxies WHERE status='active' ORDER BY id DESC"
        ).fetchall()

    server_now = datetime.now(timezone.utc)
    server_now_label = server_now.strftime("%Y-%m-%d %H:%M UTC")
    server_hour = server_now.hour
    awake_now = _is_account_awake(account, server_hour)

    return templates.TemplateResponse(
        "account_edit.html",
        _template_context(
            request,
            account=account,
            proxies=proxies,
            personas=personas,
            profile_task=profile_task,
            server_now=server_now_label,
            server_hour=server_hour,
            awake_now=awake_now,
        ),
    )


@app.post("/accounts/{session_name}/sleep")
async def account_update_sleep(
    request: Request,
    session_name: str,
    start_hour: str = Form(...),
    end_hour: str = Form(...),
):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    a = accounts[idx]
    a.setdefault("sleep_settings", {"start_hour": 8, "end_hour": 23})
    start_val = _parse_int(start_hour, default=8)
    end_val = _parse_int(end_hour, default=23)
    if start_val is None:
        start_val = 8
    if end_val is None:
        end_val = 23
    a["sleep_settings"]["start_hour"] = max(0, min(23, int(start_val)))
    a["sleep_settings"]["end_hour"] = max(0, min(23, int(end_val)))
    _save_accounts(accounts)
    _flash(request, "success", "Время сна обновлено.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.post("/accounts/{session_name}/proxy")
async def account_update_proxy(
    request: Request,
    session_name: str,
    proxy_id: str = Form(""),
):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    proxy_url: str | None = None
    if proxy_id.strip():
        try:
            proxy_id_int = int(proxy_id)
        except ValueError:
            proxy_id_int = None
            _flash(request, "warning", "Прокси: некорректный ID, значение не изменено.")
        if proxy_id_int is not None:
            with _db_connect() as conn:
                row = conn.execute("SELECT url FROM proxies WHERE id = ?", (proxy_id_int,)).fetchone()
                if row:
                    proxy_url = row["url"]

    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    if proxy_url:
        accounts[idx]["proxy_url"] = proxy_url
    else:
        accounts[idx].pop("proxy_url", None)
    _save_accounts(accounts)
    _flash(request, "success", "Прокси обновлён.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.post("/accounts/{session_name}/persona")
async def account_update_persona(
    request: Request,
    session_name: str,
    persona_id: str = Form(""),
):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    personas = settings.get("personas", {}) or {}

    persona_id = persona_id.strip()
    persona_prompt = None
    if persona_id:
        persona = personas.get(persona_id)
        if not persona:
            raise HTTPException(status_code=400, detail="Persona не найдена")
        persona_prompt = persona.get("prompt")

    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    if persona_id:
        accounts[idx]["persona_id"] = persona_id
    else:
        accounts[idx].pop("persona_id", None)
    _save_accounts(accounts)

    if persona_id and persona_prompt:
        for t in _filter_by_project(settings.get("targets", []) or [], project_id):
            t.setdefault("prompts", {})
            t["prompts"][session_name] = persona_prompt
        _save_settings(settings)

    _flash(request, "success", "Роль обновлена.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.post("/accounts/{session_name}/profile")
async def account_update_profile(
    request: Request,
    session_name: str,
    first_name: str = Form(...),
    last_name: str = Form(""),
    bio: str = Form(""),
):
    first_name = first_name.strip()
    last_name = last_name.strip()
    bio = bio.strip()
    if not first_name:
        _flash(request, "warning", "Имя не может быть пустым.")
        return _redirect(f"/accounts/{quote(session_name)}")

    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    accounts[idx]["first_name"] = first_name
    accounts[idx]["last_name"] = last_name
    accounts[idx]["profile_bio"] = bio
    _save_accounts(accounts)

    _upsert_profile_task(
        settings,
        session_name,
        {"first_name": first_name, "last_name": last_name, "bio": bio},
    )
    _save_settings(settings)
    _flash(request, "success", "Задача на обновление профиля создана.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.post("/accounts/{session_name}/avatar")
async def account_update_avatar(
    request: Request,
    session_name: str,
    avatar: UploadFile = File(...),
):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    if _find_account_index(accounts, session_name, project_id) is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    data = await avatar.read()
    if not data:
        _flash(request, "warning", "Файл пустой.")
        return _redirect(f"/accounts/{quote(session_name)}")
    if len(data) > 5 * 1024 * 1024:
        _flash(request, "warning", "Файл слишком большой (макс 5MB).")
        return _redirect(f"/accounts/{quote(session_name)}")

    ext = (Path(avatar.filename or "").suffix or "").lower()
    if ext not in {".jpg", ".jpeg", ".png", ".webp"}:
        ext = ".jpg"

    uploads_dir = DATA_DIR / "uploads" / "avatars"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"{session_name}_{int(time.time())}_{uuid.uuid4().hex[:8]}{ext}"
    file_path = uploads_dir / file_name
    file_path.write_bytes(data)

    _upsert_profile_task(
        settings,
        session_name,
        {"avatar_path": str(file_path), "avatar_clear": False},
    )
    _save_settings(settings)
    _flash(request, "success", "Задача на обновление аватара создана.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.post("/accounts/{session_name}/avatar/clear")
async def account_clear_avatar(request: Request, session_name: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    if _find_account_index(accounts, session_name, project_id) is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    _upsert_profile_task(
        settings,
        session_name,
        {"avatar_clear": True, "avatar_path": ""},
    )
    _save_settings(settings)
    _flash(request, "success", "Задача на удаление аватара создана.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.post("/accounts/{session_name}/personal-channel")
async def account_set_personal_channel(
    request: Request,
    session_name: str,
    personal_channel: str = Form(""),
):
    ref = _normalize_tg_ref(personal_channel)
    if not ref:
        _flash(request, "warning", "Укажите @username или ссылку на канал (t.me/...).")
        return _redirect(f"/accounts/{quote(session_name)}")

    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    accounts[idx]["profile_personal_channel"] = ref
    _save_accounts(accounts)

    _upsert_profile_task(
        settings,
        session_name,
        {"personal_channel": ref, "personal_channel_clear": False},
    )
    _save_settings(settings)
    _flash(request, "success", "Задача на установку персонального канала создана.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.post("/accounts/{session_name}/personal-channel/clear")
async def account_clear_personal_channel(request: Request, session_name: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    accounts[idx].pop("profile_personal_channel", None)
    _save_accounts(accounts)

    _upsert_profile_task(
        settings,
        session_name,
        {"personal_channel_clear": True, "personal_channel": ""},
    )
    _save_settings(settings)
    _flash(request, "success", "Задача на очистку персонального канала создана.")
    return _redirect(f"/accounts/{quote(session_name)}")


@app.post("/accounts/{session_name}/delete")
async def account_delete(request: Request, session_name: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    new_accounts = [
        a
        for a in accounts
        if not (a.get("session_name") == session_name and _project_id_for(a) == project_id)
    ]
    if len(new_accounts) == len(accounts):
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    _save_accounts(new_accounts)
    _flash(request, "success", f"Аккаунт '{session_name}' удалён.")
    return _redirect("/accounts")


@app.get("/targets", response_class=HTMLResponse)
async def targets_page(request: Request):
    settings, settings_err = _load_settings()
    project_id = _active_project_id(settings)
    targets = _filter_by_project(settings.get("targets", []) or [], project_id)
    targets_sorted = sorted(targets, key=lambda x: x.get("date_added", ""), reverse=True)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    accounts_by_session = {a.get("session_name"): a for a in accounts}
    return templates.TemplateResponse(
        "targets.html",
        _template_context(
            request,
            settings_err=settings_err,
            targets=targets_sorted,
            accounts_by_session=accounts_by_session,
        ),
    )


@app.get("/targets/new", response_class=HTMLResponse)
async def targets_new_page(request: Request, chat_input: str = ""):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse(
        "target_new.html",
        _template_context(request, accounts=accounts, chat_input_prefill=chat_input),
    )


@app.get("/targets/search", response_class=HTMLResponse)
async def targets_search_page(request: Request, source: str = ""):
    results: List[Dict[str, Any]] = []
    error: str | None = None

    source = source.strip()
    if source:
        try:
            client = await _get_any_authorized_client()
            try:
                source_entity = await client.get_entity(source)
                rec = await client(GetChannelRecommendationsRequest(channel=source_entity))

                for chat in getattr(rec, "chats", []) or []:
                    if getattr(chat, "megagroup", False):
                        continue
                    try:
                        full_channel = await client(GetFullChannelRequest(channel=chat))
                        linked_chat_id_bare = getattr(full_channel.full_chat, "linked_chat_id", None)
                        if not linked_chat_id_bare:
                            continue
                        comment_chat_entity = await client.get_entity(PeerChannel(linked_chat_id_bare))
                        results.append(
                            {
                                "chat_id": f"-100{chat.id}",
                                "chat_username": getattr(chat, "username", None),
                                "chat_name": getattr(chat, "title", str(chat.id)),
                                "linked_chat_id": f"-100{comment_chat_entity.id}",
                            }
                        )
                    except Exception:
                        continue
            finally:
                if client.is_connected():
                    await client.disconnect()
        except Exception as e:
            error = str(e)

    return templates.TemplateResponse(
        "target_search.html",
        _template_context(request, source=source, results=results, error=error),
    )


@app.post("/targets/new")
async def targets_new_submit(
    request: Request,
    chat_input: str = Form(...),
    prompt_default: str = Form(""),
    initial_comment_delay: str = Form("10"),
    delay_between_accounts: str = Form("10"),
    comment_chance: str = Form("100"),
    accounts_per_post_min: str = Form("0"),
    accounts_per_post_max: str = Form("0"),
    daily_comment_limit: str = Form("50"),
    ai_provider: str = Form("default"),
    min_word_count: str = Form("0"),
    min_post_interval_mins: str = Form("0"),
    min_meaningful_words: str = Form("2"),
    media_min_meaningful_words: str = Form("6"),
    skip_promotional_posts: Optional[str] = Form(None),
    skip_short_media_posts: Optional[str] = Form(None),
):
    settings, _ = _load_settings()

    chat_input = chat_input.strip()
    try:
        chat_info = await _derive_target_chat_info(chat_input)
    except HTTPException as e:
        _flash(request, "danger", str(e.detail))
        return _redirect(f"/targets/new?chat_input={quote(chat_input)}")

    chat_id = chat_info["chat_id"]
    if any(str(t.get("chat_id")) == str(chat_id) for t in settings.get("targets", [])):
        _flash(request, "warning", "Этот чат уже добавлен в цели комментирования.")
        return _redirect(f"/targets/{quote(chat_id)}")

    prompt_default = prompt_default.strip() or (
        "Коротенький небрежно написанный коммент, как в чатах телеграм на тему поста. "
        "Без избыточного количества эмодзи, рандомно, обращаясь к одному из ключевых тезисов поста. "
        "Не пиши как нейросеть, пиши как обычный человек с присущими ему опечатками или речевыми ошибками (не обязательно). "
        "Без всяких длинных тире и кавычек-елочек. "
        "Сообщение может быть как очень короткое, так и чуть длиннее, иногда звучать как вопрос или сомнение, "
        "мягкое отрицание написанного. "
        "От двух до 40 слов в ответе, как захочется, лучше длинных фраз и мыслей избегать."
    )

    new_target: Dict[str, Any] = {
        **chat_info,
        "prompts": {"default": prompt_default},
        "initial_comment_delay": _parse_int_field(
            request, initial_comment_delay, default=180, label="Пауза после поста", min_value=0
        ),
        "delay_between_accounts": _parse_int_field(
            request, delay_between_accounts, default=240, label="Пауза между аккаунтами", min_value=0
        ),
        "comment_chance": _parse_int_field(
            request, comment_chance, default=100, label="Комментировать посты (%)", min_value=0, max_value=100
        ),
        "accounts_per_post_min": _parse_int_field(
            request, accounts_per_post_min, default=1, label="Аккаунтов на пост (мин)", min_value=0
        ),
        "accounts_per_post_max": _parse_int_field(
            request, accounts_per_post_max, default=50, label="Аккаунтов на пост (макс)", min_value=0
        ),
        "daily_comment_limit": _parse_int_field(
            request, daily_comment_limit, default=50, label="Лимит комментариев/сутки", min_value=0
        ),
        "ai_provider": ai_provider,
        "date_added": datetime.now(timezone.utc).isoformat(),
        "min_word_count": _parse_int_field(request, min_word_count, default=0, label="Мин. слов", min_value=0),
        "min_post_interval_mins": _parse_int_field(
            request, min_post_interval_mins, default=0, label="Мин. интервал (мин)", min_value=0
        ),
        "min_meaningful_words": _parse_int_field(
            request, min_meaningful_words, default=6, label="Мин. смысловых слов", min_value=0
        ),
        "media_min_meaningful_words": _parse_int_field(
            request, media_min_meaningful_words, default=12, label="Мин. слов для медиа", min_value=0
        ),
        "skip_promotional_posts": bool(skip_promotional_posts),
        "skip_short_media_posts": bool(skip_short_media_posts),
        "assigned_accounts": [],
        "project_id": project_id,
    }

    min_acc = int(new_target.get("accounts_per_post_min", 0) or 0)
    max_acc = int(new_target.get("accounts_per_post_max", 0) or 0)
    if max_acc < min_acc:
        _flash(request, "warning", "Диапазон аккаунтов на пост: максимум меньше минимума, исправлено.")
        new_target["accounts_per_post_max"] = min_acc

    settings.setdefault("targets", []).append(new_target)
    _save_settings(settings)
    _flash(request, "success", f"Цель комментирования добавлена: {chat_info.get('chat_name')}")
    return _redirect(f"/targets/{quote(chat_id)}")


@app.get("/targets/{chat_id}", response_class=HTMLResponse)
async def target_edit_page(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_target_by_chat_id(settings, chat_id, project_id)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse(
        "target_edit.html",
        _template_context(request, target=target, accounts=accounts),
    )


@app.post("/targets/{chat_id}")
async def target_edit_save(
    request: Request,
    chat_id: str,
    ai_enabled: Optional[str] = Form(None),
    ai_provider: str = Form("default"),
    initial_comment_delay: str = Form(""),
    delay_between_accounts: str = Form(""),
    comment_chance: str = Form(""),
    accounts_per_post_min: str = Form(""),
    accounts_per_post_max: str = Form(""),
    daily_comment_limit: str = Form(""),
    min_word_count: str = Form(""),
    min_post_interval_mins: str = Form(""),
    min_meaningful_words: str = Form(""),
    media_min_meaningful_words: str = Form(""),
    skip_promotional_posts: Optional[str] = Form(None),
    skip_short_media_posts: Optional[str] = Form(None),
    reply_chance: str = Form(""),
    intervention_chance: str = Form(""),
    tag_reply_chance: str = Form(""),
    reply_delay_min: str = Form(""),
    reply_delay_max: str = Form(""),
    max_dialogue_depth: str = Form(""),
    assigned_accounts: Optional[List[str]] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, target = _find_target_by_chat_id(settings, chat_id, project_id)

    target["ai_enabled"] = bool(ai_enabled)
    target["ai_provider"] = ai_provider

    if initial_comment_delay.strip():
        target["initial_comment_delay"] = _parse_int_field(
            request,
            initial_comment_delay,
            default=int(target.get("initial_comment_delay", 10)),
            label="Пауза после поста",
            min_value=0,
        )
    if delay_between_accounts.strip():
        target["delay_between_accounts"] = _parse_int_field(
            request,
            delay_between_accounts,
            default=int(target.get("delay_between_accounts", 10)),
            label="Пауза между аккаунтами",
            min_value=0,
        )
    if comment_chance.strip():
        target["comment_chance"] = _parse_int_field(
            request,
            comment_chance,
            default=int(target.get("comment_chance", 100)),
            label="Комментировать посты (%)",
            min_value=0,
            max_value=100,
        )
    if accounts_per_post_min.strip():
        target["accounts_per_post_min"] = _parse_int_field(
            request,
            accounts_per_post_min,
            default=int(target.get("accounts_per_post_min", 0)),
            label="Аккаунтов на пост (мин)",
            min_value=0,
        )
    if accounts_per_post_max.strip():
        target["accounts_per_post_max"] = _parse_int_field(
            request,
            accounts_per_post_max,
            default=int(target.get("accounts_per_post_max", 0)),
            label="Аккаунтов на пост (макс)",
            min_value=0,
        )
    if accounts_per_post_min.strip() or accounts_per_post_max.strip():
        min_acc = int(target.get("accounts_per_post_min", 0) or 0)
        max_acc = int(target.get("accounts_per_post_max", 0) or 0)
        if max_acc < min_acc:
            _flash(request, "warning", "Диапазон аккаунтов на пост: максимум меньше минимума, исправлено.")
            target["accounts_per_post_max"] = min_acc
    if "accounts_per_post" in target:
        target.pop("accounts_per_post", None)
    if daily_comment_limit.strip():
        target["daily_comment_limit"] = _parse_int_field(
            request,
            daily_comment_limit,
            default=int(target.get("daily_comment_limit", 50)),
            label="Лимит комментариев/сутки",
            min_value=0,
        )
    if min_word_count.strip():
        target["min_word_count"] = _parse_int_field(
            request, min_word_count, default=int(target.get("min_word_count", 0)), label="Мин. слов", min_value=0
        )
    if min_post_interval_mins.strip():
        target["min_post_interval_mins"] = _parse_int_field(
            request,
            min_post_interval_mins,
            default=int(target.get("min_post_interval_mins", 0)),
            label="Мин. интервал (мин)",
            min_value=0,
        )
    if min_meaningful_words.strip():
        target["min_meaningful_words"] = _parse_int_field(
            request,
            min_meaningful_words,
            default=int(target.get("min_meaningful_words", 2)),
            label="Мин. смысловых слов",
            min_value=0,
        )
    if media_min_meaningful_words.strip():
        target["media_min_meaningful_words"] = _parse_int_field(
            request,
            media_min_meaningful_words,
            default=int(target.get("media_min_meaningful_words", 6)),
            label="Мин. слов для медиа",
            min_value=0,
        )

    target["skip_promotional_posts"] = bool(skip_promotional_posts) if skip_promotional_posts is not None else False
    target["skip_short_media_posts"] = bool(skip_short_media_posts) if skip_short_media_posts is not None else False

    if reply_chance.strip():
        target["reply_chance"] = _parse_int_field(
            request, reply_chance, default=int(target.get("reply_chance", 0)), label="Шанс ответа", min_value=0, max_value=100
        )
    if intervention_chance.strip():
        target["intervention_chance"] = _parse_int_field(
            request,
            intervention_chance,
            default=int(target.get("intervention_chance", 30)),
            label="Шанс вмешательства",
            min_value=0,
            max_value=100,
        )
    if tag_reply_chance.strip():
        target["tag_reply_chance"] = _parse_int_field(
            request,
            tag_reply_chance,
            default=int(target.get("tag_reply_chance", 50)),
            label="Шанс Reply-тега",
            min_value=0,
            max_value=100,
        )
    if reply_delay_min.strip():
        target["reply_delay_min"] = _parse_int_field(
            request,
            reply_delay_min,
            default=int(target.get("reply_delay_min", 30)),
            label="Мин. задержка (сек)",
            min_value=0,
        )
    if reply_delay_max.strip():
        target["reply_delay_max"] = _parse_int_field(
            request,
            reply_delay_max,
            default=int(target.get("reply_delay_max", 120)),
            label="Макс. задержка (сек)",
            min_value=0,
        )
    if max_dialogue_depth.strip():
        target["max_dialogue_depth"] = _parse_int_field(
            request,
            max_dialogue_depth,
            default=int(target.get("max_dialogue_depth", 10)),
            label="Глубина контекста",
            min_value=1,
        )

    if assigned_accounts is None:
        target["assigned_accounts"] = []
    else:
        accounts, _ = _load_accounts()
        allowed_sessions = {
            a.get("session_name")
            for a in _filter_accounts_by_project(accounts, project_id)
            if a.get("session_name")
        }
        target["assigned_accounts"] = [s for s in list(assigned_accounts) if s in allowed_sessions]

    settings["targets"][idx] = target
    _save_settings(settings)
    _flash(request, "success", "Настройки чата обновлены.")
    return _redirect(f"/targets/{quote(chat_id)}")


@app.post("/targets/{chat_id}/delete")
async def target_delete(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    settings["targets"].pop(idx)
    _save_settings(settings)
    _flash(request, "success", "Цель удалена.")
    return _redirect("/targets")


@app.get("/targets/{chat_id}/prompts", response_class=HTMLResponse)
async def target_prompts_page(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_target_by_chat_id(settings, chat_id, project_id)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    target.setdefault("prompts", {})
    return templates.TemplateResponse(
        "target_prompts.html",
        _template_context(request, target=target, accounts=accounts),
    )


@app.post("/targets/{chat_id}/prompts")
async def target_prompts_save(request: Request, chat_id: str):
    form = await request.form()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, target = _find_target_by_chat_id(settings, chat_id, project_id)
    prompts = target.get("prompts") if isinstance(target.get("prompts"), dict) else {}

    default_prompt = str(form.get("default_prompt") or "").strip()
    if default_prompt:
        prompts["default"] = default_prompt

    for k, v in form.items():
        if not str(k).startswith("prompt_"):
            continue
        session = str(k).split("prompt_", 1)[-1]
        val = str(v).strip()
        if val:
            prompts[session] = val
        else:
            prompts.pop(session, None)

    target["prompts"] = prompts
    settings["targets"][idx] = target
    _save_settings(settings)
    _flash(request, "success", "Промпты обновлены.")
    return _redirect(f"/targets/{quote(chat_id)}/prompts")


@app.get("/targets/{chat_id}/scenario", response_class=HTMLResponse)
async def target_scenario_page(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_target_by_chat_id(settings, chat_id, project_id)

    with _db_connect() as conn:
        row = conn.execute(
            "SELECT script_content, current_index, status FROM scenarios WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()

    scenario = dict(row) if row else {"script_content": "", "current_index": 0, "status": "stopped"}
    return templates.TemplateResponse(
        "target_scenario.html",
        _template_context(request, target=target, scenario=scenario),
    )


@app.post("/targets/{chat_id}/scenario/save")
async def target_scenario_save(request: Request, chat_id: str, script_content: str = Form("")):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)

    script_content = script_content.strip()
    if not script_content:
        _flash(request, "warning", "Сценарий пустой — нечего сохранять.")
        return _redirect(f"/targets/{quote(chat_id)}/scenario")

    with _db_connect() as conn:
        conn.execute(
            """
            INSERT INTO scenarios (chat_id, script_content, current_index, status)
            VALUES (?, ?, 0, 'stopped')
            ON CONFLICT(chat_id) DO UPDATE SET
                script_content=excluded.script_content,
                current_index=0,
                status='stopped'
            """,
            (chat_id, script_content),
        )
        conn.commit()

    _flash(request, "success", "Сценарий сохранён (статус: остановлен, прогресс сброшен).")
    return _redirect(f"/targets/{quote(chat_id)}/scenario")


@app.post("/targets/{chat_id}/scenario/toggle")
async def target_scenario_toggle(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    with _db_connect() as conn:
        row = conn.execute("SELECT status FROM scenarios WHERE chat_id = ?", (chat_id,)).fetchone()
        current = row["status"] if row else "stopped"
        new_status = "stopped" if current == "running" else "running"
        conn.execute("UPDATE scenarios SET status = ? WHERE chat_id = ?", (new_status, chat_id))
        conn.commit()
    _flash(request, "success", f"Сценарий: {new_status}")
    return _redirect(f"/targets/{quote(chat_id)}/scenario")


@app.post("/targets/{chat_id}/scenario/reset")
async def target_scenario_reset(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    with _db_connect() as conn:
        conn.execute("UPDATE scenarios SET current_index = 0 WHERE chat_id = ?", (chat_id,))
        conn.commit()
    _flash(request, "success", "Прогресс сценария сброшен в 0.")
    return _redirect(f"/targets/{quote(chat_id)}/scenario")


@app.post("/targets/{chat_id}/scenario/toggle-reply")
async def target_scenario_toggle_reply(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, target = _find_target_by_chat_id(settings, chat_id, project_id)
    target["scenario_reply_mode"] = not bool(target.get("scenario_reply_mode", False))
    settings["targets"][idx] = target
    _save_settings(settings)
    _flash(request, "success", f"Связный Reply: {'вкл' if target['scenario_reply_mode'] else 'выкл'}")
    return _redirect(f"/targets/{quote(chat_id)}/scenario")


@app.get("/targets/{chat_id}/triggers", response_class=HTMLResponse)
async def target_triggers_page(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_target_by_chat_id(settings, chat_id, project_id)
    with _db_connect() as conn:
        triggers = conn.execute(
            "SELECT id, trigger_phrase, answer_text FROM triggers WHERE chat_id = ? ORDER BY id DESC",
            (chat_id,),
        ).fetchall()
    return templates.TemplateResponse(
        "target_triggers.html",
        _template_context(request, target=target, triggers=triggers),
    )


@app.post("/targets/{chat_id}/triggers/add")
async def target_triggers_add(
    request: Request,
    chat_id: str,
    trigger_phrase: str = Form(...),
    answer_text: str = Form(...),
):
    trigger_phrase = trigger_phrase.strip().lower()
    answer_text = answer_text.strip()
    if not trigger_phrase or not answer_text:
        raise HTTPException(status_code=400, detail="Нужно указать фразу и ответ")

    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    with _db_connect() as conn:
        conn.execute(
            "INSERT INTO triggers (chat_id, trigger_phrase, answer_text) VALUES (?, ?, ?)",
            (chat_id, trigger_phrase, answer_text),
        )
        conn.commit()
    _flash(request, "success", f"Триггер добавлен: {trigger_phrase}")
    return _redirect(f"/targets/{quote(chat_id)}/triggers")


@app.post("/targets/{chat_id}/triggers/{trigger_id}/delete")
async def target_triggers_delete(request: Request, chat_id: str, trigger_id: int):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    with _db_connect() as conn:
        conn.execute("DELETE FROM triggers WHERE id = ?", (trigger_id,))
        conn.commit()
    _flash(request, "success", "Триггер удалён.")
    return _redirect(f"/targets/{quote(chat_id)}/triggers")


@app.get("/reaction-targets", response_class=HTMLResponse)
async def reaction_targets_page(request: Request):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    targets = _filter_by_project(settings.get("reaction_targets", []) or [], project_id)
    targets_sorted = sorted(targets, key=lambda x: x.get("date_added", ""), reverse=True)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse(
        "reaction_targets.html",
        _template_context(request, targets=targets_sorted, accounts=accounts),
    )


@app.get("/reaction-targets/new", response_class=HTMLResponse)
async def reaction_targets_new_page(request: Request):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse("reaction_target_new.html", _template_context(request, accounts=accounts))


@app.post("/reaction-targets/new")
async def reaction_targets_new_submit(
    request: Request,
    chat_input: str = Form(...),
    reactions: str = Form("👍"),
    reaction_count: str = Form("1"),
    reaction_chance: str = Form("80"),
    initial_reaction_delay: str = Form("10"),
    delay_between_reactions: str = Form("5"),
    daily_reaction_limit: str = Form("999"),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)

    chat_input = chat_input.strip()
    try:
        base = await _derive_target_chat_info(chat_input)
    except HTTPException as e:
        _flash(request, "danger", str(e.detail))
        return _redirect("/reaction-targets/new")

    chat_id = base["chat_id"]
    if any(str(t.get("chat_id")) == str(chat_id) for t in settings.get("reaction_targets", [])):
        _flash(request, "warning", "Этот чат уже добавлен в цели реакций.")
        return _redirect(f"/reaction-targets/{quote(chat_id)}")

    reaction_list = [r for r in reactions.split() if r.strip()]
    if not reaction_list:
        _flash(request, "warning", "Список реакций пустой.")
        return _redirect("/reaction-targets/new")

    new_target: Dict[str, Any] = {
        **base,
        "reactions": reaction_list,
        "reaction_count": _parse_int_field(request, reaction_count, default=1, label="Кол-во реакций", min_value=1),
        "reaction_chance": _parse_int_field(
            request, reaction_chance, default=80, label="Шанс", min_value=0, max_value=100
        ),
        "initial_reaction_delay": _parse_int_field(
            request, initial_reaction_delay, default=10, label="Пауза после поста", min_value=0
        ),
        "delay_between_reactions": _parse_int_field(
            request, delay_between_reactions, default=5, label="Пауза между реакциями", min_value=0
        ),
        "daily_reaction_limit": _parse_int_field(
            request, daily_reaction_limit, default=999, label="Лимит/сутки", min_value=0
        ),
        "date_added": datetime.now(timezone.utc).isoformat(),
        "assigned_accounts": [],
        "project_id": project_id,
    }
    settings.setdefault("reaction_targets", []).append(new_target)
    _save_settings(settings)
    _flash(request, "success", f"Цель реакций добавлена: {base.get('chat_name')}")
    return _redirect(f"/reaction-targets/{quote(chat_id)}")


@app.get("/reaction-targets/{chat_id}", response_class=HTMLResponse)
async def reaction_target_edit_page(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_reaction_target_by_chat_id(settings, chat_id, project_id)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse(
        "reaction_target_edit.html",
        _template_context(request, target=target, accounts=accounts),
    )


@app.post("/reaction-targets/{chat_id}")
async def reaction_target_edit_save(
    request: Request,
    chat_id: str,
    reactions: str = Form(""),
    reaction_count: str = Form(""),
    reaction_chance: str = Form(""),
    initial_reaction_delay: str = Form(""),
    delay_between_reactions: str = Form(""),
    daily_reaction_limit: str = Form(""),
    assigned_accounts: Optional[List[str]] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, target = _find_reaction_target_by_chat_id(settings, chat_id, project_id)

    if reactions.strip():
        target["reactions"] = [r for r in reactions.split() if r.strip()]
    if reaction_count.strip():
        target["reaction_count"] = _parse_int_field(
            request, reaction_count, default=int(target.get("reaction_count", 1)), label="Кол-во реакций", min_value=1
        )
    if reaction_chance.strip():
        target["reaction_chance"] = _parse_int_field(
            request,
            reaction_chance,
            default=int(target.get("reaction_chance", 80)),
            label="Шанс",
            min_value=0,
            max_value=100,
        )
    if initial_reaction_delay.strip():
        target["initial_reaction_delay"] = _parse_int_field(
            request,
            initial_reaction_delay,
            default=int(target.get("initial_reaction_delay", 10)),
            label="Пауза после поста",
            min_value=0,
        )
    if delay_between_reactions.strip():
        target["delay_between_reactions"] = _parse_int_field(
            request,
            delay_between_reactions,
            default=int(target.get("delay_between_reactions", 5)),
            label="Пауза между реакциями",
            min_value=0,
        )
    if daily_reaction_limit.strip():
        target["daily_reaction_limit"] = _parse_int_field(
            request, daily_reaction_limit, default=int(target.get("daily_reaction_limit", 999)), label="Лимит/сутки", min_value=0
        )

    accounts, _ = _load_accounts()
    allowed_sessions = {
        a.get("session_name")
        for a in _filter_accounts_by_project(accounts, project_id)
        if a.get("session_name")
    }
    target["assigned_accounts"] = [s for s in list(assigned_accounts or []) if s in allowed_sessions]
    settings["reaction_targets"][idx] = target
    _save_settings(settings)
    _flash(request, "success", "Настройки реакций обновлены.")
    return _redirect(f"/reaction-targets/{quote(chat_id)}")


@app.post("/reaction-targets/{chat_id}/delete")
async def reaction_target_delete(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, _ = _find_reaction_target_by_chat_id(settings, chat_id, project_id)
    settings["reaction_targets"].pop(idx)
    _save_settings(settings)
    _flash(request, "success", "Цель реакций удалена.")
    return _redirect("/reaction-targets")


@app.get("/monitor-targets", response_class=HTMLResponse)
async def monitor_targets_page(request: Request):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    targets = _filter_by_project(settings.get("monitor_targets", []) or [], project_id)
    targets_sorted = sorted(targets, key=lambda x: x.get("date_added", ""), reverse=True)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse(
        "monitor_targets.html",
        _template_context(request, targets=targets_sorted, accounts=accounts),
    )


@app.get("/monitor-targets/new", response_class=HTMLResponse)
async def monitor_targets_new_page(request: Request):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse("monitor_target_new.html", _template_context(request, accounts=accounts))


@app.post("/monitor-targets/new")
async def monitor_targets_new_submit(
    request: Request,
    chat_input: str = Form(...),
    notification_chat_id: str = Form(...),
    prompt: str = Form(...),
    daily_limit: str = Form("0"),
    min_word_count: str = Form("0"),
    min_post_interval_mins: str = Form("0"),
    ai_provider: str = Form("default"),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    chat_input = chat_input.strip()
    try:
        chat_info = await _derive_target_chat_info(chat_input)
    except HTTPException as e:
        _flash(request, "danger", str(e.detail))
        return _redirect("/monitor-targets/new")

    chat_id = chat_info["chat_id"]
    if any(str(t.get("chat_id")) == str(chat_id) for t in settings.get("monitor_targets", [])):
        _flash(request, "warning", "Этот канал уже добавлен в мониторинг.")
        return _redirect(f"/monitor-targets/{quote(chat_id)}")

    try:
        notification_id = int(notification_chat_id.strip())
    except ValueError:
        _flash(request, "danger", "Чат уведомлений: укажите числовой chat_id (например: -100...).")
        return _redirect("/monitor-targets/new")

    new_target: Dict[str, Any] = {
        "chat_id": chat_info["chat_id"],
        "chat_username": chat_info.get("chat_username"),
        "chat_name": chat_info.get("chat_name"),
        "notification_chat_id": notification_id,
        "prompt": prompt.strip(),
        "daily_limit": _parse_int_field(request, daily_limit, default=0, label="Лимит/сутки", min_value=0),
        "min_word_count": _parse_int_field(request, min_word_count, default=0, label="Мин. слов", min_value=0),
        "min_post_interval_mins": _parse_int_field(
            request, min_post_interval_mins, default=0, label="Мин. интервал (мин)", min_value=0
        ),
        "ai_provider": ai_provider,
        "assigned_accounts": [],
        "date_added": datetime.now(timezone.utc).isoformat(),
        "project_id": project_id,
    }
    settings.setdefault("monitor_targets", []).append(new_target)
    _save_settings(settings)
    _flash(request, "success", "Канал мониторинга добавлен.")
    return _redirect(f"/monitor-targets/{quote(chat_id)}")


@app.get("/monitor-targets/{chat_id}", response_class=HTMLResponse)
async def monitor_target_edit_page(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_monitor_target_by_chat_id(settings, chat_id, project_id)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse(
        "monitor_target_edit.html",
        _template_context(request, target=target, accounts=accounts),
    )


@app.post("/monitor-targets/{chat_id}")
async def monitor_target_edit_save(
    request: Request,
    chat_id: str,
    notification_chat_id: str = Form(""),
    prompt: str = Form(""),
    daily_limit: str = Form(""),
    min_word_count: str = Form(""),
    min_post_interval_mins: str = Form(""),
    ai_provider: str = Form("default"),
    assigned_accounts: Optional[List[str]] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, target = _find_monitor_target_by_chat_id(settings, chat_id, project_id)

    if notification_chat_id.strip():
        try:
            target["notification_chat_id"] = int(notification_chat_id.strip())
        except ValueError:
            _flash(request, "warning", "Чат уведомлений: некорректное число, значение не изменено.")
    if prompt.strip():
        target["prompt"] = prompt.strip()
    if daily_limit.strip():
        target["daily_limit"] = _parse_int_field(
            request, daily_limit, default=int(target.get("daily_limit", 0)), label="Лимит/сутки", min_value=0
        )
    if min_word_count.strip():
        target["min_word_count"] = _parse_int_field(
            request, min_word_count, default=int(target.get("min_word_count", 0)), label="Мин. слов", min_value=0
        )
    if min_post_interval_mins.strip():
        target["min_post_interval_mins"] = _parse_int_field(
            request,
            min_post_interval_mins,
            default=int(target.get("min_post_interval_mins", 0)),
            label="Мин. интервал (мин)",
            min_value=0,
        )
    target["ai_provider"] = ai_provider
    accounts, _ = _load_accounts()
    allowed_sessions = {
        a.get("session_name")
        for a in _filter_accounts_by_project(accounts, project_id)
        if a.get("session_name")
    }
    target["assigned_accounts"] = [s for s in list(assigned_accounts or []) if s in allowed_sessions]

    settings["monitor_targets"][idx] = target
    _save_settings(settings)
    _flash(request, "success", "Настройки мониторинга обновлены.")
    return _redirect(f"/monitor-targets/{quote(chat_id)}")


@app.post("/monitor-targets/{chat_id}/delete")
async def monitor_target_delete(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, _ = _find_monitor_target_by_chat_id(settings, chat_id, project_id)
    settings["monitor_targets"].pop(idx)
    _save_settings(settings)
    _flash(request, "success", "Канал мониторинга удалён.")
    return _redirect("/monitor-targets")


@app.get("/personas", response_class=HTMLResponse)
async def personas_page(request: Request):
    settings, settings_err = _load_settings()
    personas = settings.get("personas", {}) or {}
    accounts, _ = _load_accounts()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse(
        "personas.html",
        _template_context(request, settings_err=settings_err, personas=personas, accounts=accounts),
    )


@app.post("/personas/new")
async def persona_new(request: Request, name: str = Form(...), prompt: str = Form(...)):
    name = name.strip()
    prompt = prompt.strip()
    if not name or not prompt:
        raise HTTPException(status_code=400, detail="Нужно указать имя и промпт")
    settings, _ = _load_settings()
    pid = str(int(time.time()))
    settings.setdefault("personas", {})[pid] = {"name": name, "prompt": prompt}
    _save_settings(settings)
    _flash(request, "success", f"Persona создана: {name}")
    return _redirect(f"/personas/{pid}")


@app.get("/personas/{persona_id}", response_class=HTMLResponse)
async def persona_edit_page(request: Request, persona_id: str):
    settings, _ = _load_settings()
    personas = settings.get("personas", {}) or {}
    persona = personas.get(persona_id)
    if not persona:
        raise HTTPException(status_code=404, detail="Persona не найдена")
    accounts, _ = _load_accounts()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse(
        "persona_edit.html",
        _template_context(request, persona_id=persona_id, persona=persona, accounts=accounts),
    )


@app.post("/personas/{persona_id}/assign")
async def persona_assign(
    request: Request,
    persona_id: str,
    sessions: Optional[List[str]] = Form(None),
):
    settings, _ = _load_settings()
    persona = (settings.get("personas", {}) or {}).get(persona_id)
    if not persona:
        raise HTTPException(status_code=404, detail="Persona не найдена")
    persona_prompt = persona.get("prompt", "")
    project_id = _active_project_id(settings)

    wanted = set(sessions or [])
    accounts, _ = _load_accounts()
    for acc in accounts:
        s = acc.get("session_name")
        if not s:
            continue
        if _project_id_for(acc) != project_id:
            continue
        if s in wanted:
            acc["persona_id"] = persona_id
        else:
            if acc.get("persona_id") == persona_id:
                acc.pop("persona_id", None)
    _save_accounts(accounts)

    for t in _filter_by_project(settings.get("targets", []) or [], project_id):
        t.setdefault("prompts", {})
        for s in wanted:
            t["prompts"][s] = persona_prompt
        for s in list(t["prompts"].keys()):
            if s != "default" and s not in wanted and t["prompts"].get(s) == persona_prompt:
                t["prompts"].pop(s, None)

    _save_settings(settings)
    _flash(request, "success", "Назначения сохранены и применены.")
    return _redirect(f"/personas/{quote(persona_id)}")


@app.post("/personas/{persona_id}/update")
async def persona_update(
    request: Request,
    persona_id: str,
    name: str = Form(...),
    prompt: str = Form(...),
    apply_to_targets: Optional[str] = Form(None),
):
    name = name.strip()
    prompt = prompt.strip()
    if not name or not prompt:
        _flash(request, "warning", "Нужно указать название и промпт.")
        return _redirect(f"/personas/{quote(persona_id)}")

    settings, _ = _load_settings()
    personas = settings.get("personas", {}) or {}
    persona = personas.get(persona_id)
    if not persona:
        raise HTTPException(status_code=404, detail="Persona не найдена")

    personas[persona_id] = {"name": name, "prompt": prompt}
    settings["personas"] = personas

    if apply_to_targets:
        accounts, _ = _load_accounts()
        project_id = _active_project_id(settings)
        sessions = [
            a.get("session_name")
            for a in accounts
            if a.get("session_name")
            and a.get("persona_id") == persona_id
            and _project_id_for(a) == project_id
        ]
        if sessions:
            for t in _filter_by_project(settings.get("targets", []) or [], project_id):
                t.setdefault("prompts", {})
                for s in sessions:
                    t["prompts"][s] = prompt

    _save_settings(settings)
    _flash(request, "success", "Роль обновлена.")
    return _redirect(f"/personas/{quote(persona_id)}")


@app.post("/personas/{persona_id}/delete")
async def persona_delete(request: Request, persona_id: str):
    settings, _ = _load_settings()
    personas = settings.get("personas", {}) or {}
    removed = personas.pop(persona_id, None)
    if not removed:
        raise HTTPException(status_code=404, detail="Persona не найдена")
    settings["personas"] = personas
    _save_settings(settings)

    accounts, _ = _load_accounts()
    updated = False
    for acc in accounts:
        if acc.get("persona_id") == persona_id:
            acc.pop("persona_id", None)
            updated = True
    if updated:
        _save_accounts(accounts)

    _flash(request, "success", f"Persona удалена: {removed.get('name')}")
    return _redirect("/personas")


@app.get("/proxies", response_class=HTMLResponse)
async def proxies_page(request: Request):
    with _db_connect() as conn:
        proxies = conn.execute(
            "SELECT id, url, ip, country, status, last_check FROM proxies ORDER BY id DESC LIMIT 200"
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) AS c FROM proxies").fetchone()["c"]
        active = conn.execute("SELECT COUNT(*) AS c FROM proxies WHERE status='active'").fetchone()["c"]
        dead = conn.execute("SELECT COUNT(*) AS c FROM proxies WHERE status='dead'").fetchone()["c"]

    return templates.TemplateResponse(
        "proxies.html",
        _template_context(request, proxies=proxies, total=total, active=active, dead=dead),
    )


@app.post("/proxies/add")
async def proxies_add(request: Request, proxies_text: str = Form(...)):
    lines = [l.strip() for l in proxies_text.splitlines() if l.strip()]
    if not lines:
        _flash(request, "warning", "Список пустой.")
        return _redirect("/proxies")

    added = 0
    dup = 0
    for url in lines:
        res = await _check_proxy_health(url)
        with _db_connect() as conn:
            try:
                conn.execute(
                    "INSERT INTO proxies (url, ip, country, status, last_check) VALUES (?, ?, ?, ?, ?)",
                    (url, res["ip"], res["country"], res["status"], datetime.now().isoformat()),
                )
                conn.commit()
                added += 1
            except sqlite3.IntegrityError:
                dup += 1

    _flash(request, "success", f"Импорт завершён: добавлено={added}, дубликаты={dup}")
    return _redirect("/proxies")


@app.post("/proxies/check-all")
async def proxies_check_all(request: Request):
    with _db_connect() as conn:
        rows = conn.execute("SELECT id, url FROM proxies").fetchall()

    active = 0
    dead = 0
    for r in rows:
        res = await _check_proxy_health(r["url"])
        with _db_connect() as conn:
            conn.execute(
                "UPDATE proxies SET status=?, ip=?, country=?, last_check=? WHERE id=?",
                (res["status"], res["ip"], res["country"], datetime.now().isoformat(), r["id"]),
            )
            conn.commit()
        if res["status"] == "active":
            active += 1
        else:
            dead += 1

    _flash(request, "success", f"Проверка завершена: active={active}, dead={dead}")
    return _redirect("/proxies")


@app.post("/proxies/{proxy_id}/check")
async def proxies_check_one(request: Request, proxy_id: int):
    with _db_connect() as conn:
        row = conn.execute("SELECT url FROM proxies WHERE id=?", (proxy_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Прокси не найден")
        url = row["url"]

    res = await _check_proxy_health(url)
    with _db_connect() as conn:
        conn.execute(
            "UPDATE proxies SET status=?, ip=?, country=?, last_check=? WHERE id=?",
            (res["status"], res["ip"], res["country"], datetime.now().isoformat(), proxy_id),
        )
        conn.commit()

    _flash(request, "success", f"Прокси обновлён: {res['status']}, IP={res['ip']}")
    return _redirect("/proxies")


@app.post("/proxies/{proxy_id}/delete")
async def proxies_delete_one(request: Request, proxy_id: int):
    with _db_connect() as conn:
        conn.execute("DELETE FROM proxies WHERE id=?", (proxy_id,))
        conn.commit()
    _flash(request, "success", "Прокси удалён.")
    return _redirect("/proxies")


@app.post("/proxies/delete-dead")
async def proxies_delete_dead(request: Request):
    with _db_connect() as conn:
        dead_urls = [r["url"] for r in conn.execute("SELECT url FROM proxies WHERE status='dead'").fetchall()]
        conn.execute("DELETE FROM proxies WHERE status='dead'")
        conn.commit()

    accounts, _ = _load_accounts()
    updated = False
    for acc in accounts:
        if acc.get("proxy_url") in dead_urls:
            acc.pop("proxy_url", None)
            updated = True
    if updated:
        _save_accounts(accounts)

    _flash(request, "success", f"Удалено нерабочих прокси: {len(dead_urls)}")
    return _redirect("/proxies")


def _get_logs_for_period(period: str, page: int, items_per_page: int = 20) -> Tuple[List[sqlite3.Row], int]:
    if period == "day":
        period_filter = "timestamp >= datetime('now', '-1 day', 'localtime')"
    elif period == "week":
        period_filter = "timestamp >= datetime('now', '-7 days', 'localtime')"
    else:
        period_filter = "timestamp >= datetime('now', '-30 days', 'localtime')"

    offset = page * items_per_page

    with _db_connect() as conn:
        total_items = conn.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM (
                SELECT 1 FROM logs
                WHERE {period_filter}
                GROUP BY post_id, account_session_name, content
            )
            """
        ).fetchone()["c"]

        rows = conn.execute(
            f"""
            SELECT * FROM logs
            WHERE {period_filter}
            GROUP BY post_id, account_session_name, content
            ORDER BY timestamp DESC
            LIMIT ? OFFSET ?
            """,
            (items_per_page, offset),
        ).fetchall()

    return rows, total_items


@app.get("/stats", response_class=HTMLResponse)
async def stats_page(request: Request, period: str = "day", page: int = 0):
    if period not in {"day", "week", "month"}:
        period = "day"
    rows, total = _get_logs_for_period(period, page)

    total_pages = max((total + 19) // 20, 1)
    page = max(min(page, total_pages - 1), 0)

    return templates.TemplateResponse(
        "stats.html",
        _template_context(request, period=period, page=page, total=total, total_pages=total_pages, rows=rows),
    )


@app.get("/stats/export")
async def stats_export(period: str = "day"):
    if period not in {"day", "week", "month"}:
        period = "day"
    rows, _ = _get_logs_for_period(period, 0, items_per_page=100000)

    table_data: List[List[Any]] = []
    for row in rows:
        log_type_raw = row["log_type"] or ""
        log_type_map = {
            "reaction": "Реакция",
            "comment": "Комментарий",
            "comment_reply": "Ответ",
            "comment_failed": "Ошибка",
            "comment_skip": "Пропуск",
            "monitoring": "Мониторинг",
            "forbidden": "Запрещено",
        }
        log_type = log_type_map.get(log_type_raw, log_type_raw or "—")

        date_str = str(row["timestamp"] or "")
        try:
            dt = datetime.fromisoformat(date_str)
            date_str = dt.astimezone(timezone(timedelta(hours=3))).strftime("%d.%m.%Y %H:%M:%S")
        except Exception:
            pass

        post_id = row["post_id"]
        channel_username = row["channel_username"]
        if channel_username:
            post_link = f"https://t.me/{channel_username}/{post_id}"
        else:
            chat_id_clean = str(row["source_channel_id"] or "").replace("-100", "")
            post_link = f"https://t.me/c/{chat_id_clean}/{post_id}"

        content = row["content"] or ""
        table_data.append(
            [
                log_type,
                date_str,
                row["channel_name"] or "",
                row["account_session_name"] or "",
                row["account_username"] or "",
                post_id,
                content,
                post_link,
            ]
        )

    header = ["Тип", "Дата (МСК)", "Канал", "Исполнитель", "Юзернейм", "ID Поста", "Текст", "Ссылка на пост"]
    df = pd.DataFrame(table_data, columns=header)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Статистика")
        worksheet = writer.sheets["Статистика"]
        for i, col in enumerate(df.columns):
            max_len = max((len(str(v)) for v in df[col]), default=0)
            worksheet.column_dimensions[chr(ord("A") + i)].width = min(max_len + 2, 50)

    output.seek(0)
    today_str = datetime.now().strftime("%Y-%m-%d")
    file_name = f"stats_{period}_{today_str}.xlsx"

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={file_name}"},
    )


@app.get("/rebrand", response_class=HTMLResponse)
async def rebrand_page(request: Request):
    return templates.TemplateResponse("rebrand.html", _template_context(request))


@app.post("/rebrand")
async def rebrand_submit(
    request: Request,
    topic: str = Form(...),
    source: str = Form(...),
):
    topic = topic.strip()
    source = source.strip()
    if not topic or not source:
        raise HTTPException(status_code=400, detail="Нужно указать тему и источник")

    is_channel = source.startswith("@") or "t.me/" in source
    final_source = source.replace("@", "") if is_channel else source

    settings, _ = _load_settings()
    settings["rebrand_task"] = {
        "topic": topic,
        "source_value": final_source,
        "is_channel": is_channel,
        "status": "pending",
    }
    _save_settings(settings)

    _flash(request, "success", "Задача на ребрендинг создана (commentator.py подхватит в течение ~10 секунд).")
    return _redirect("/")


@app.get("/manual", response_class=HTMLResponse)
async def manual_page(request: Request):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    q = [t for t in (settings.get("manual_queue", []) or []) if _project_id_for(t) == project_id]
    return templates.TemplateResponse("manual.html", _template_context(request, queue=q))


@app.post("/manual/run")
async def manual_run(request: Request, link: str = Form(...)):
    link = link.strip()
    chat_identifier: Any = None
    post_id: int | None = None
    is_private_link = False

    try:
        if "t.me/c/" in link:
            parts = link.split("t.me/c/")[-1].split("/")
            chat_id_num = parts[0]
            post_id = int(parts[1])
            chat_identifier = int(f"-100{chat_id_num}")
            is_private_link = True
        elif "t.me/" in link:
            parts = link.split("t.me/")[-1].split("/")
            chat_identifier = parts[0]
            post_id = int(parts[1])
        else:
            raise ValueError
    except Exception:
        _flash(request, "danger", "Неверный формат ссылки. Пример: https://t.me/channel/123 или https://t.me/c/123/456")
        return _redirect("/manual")

    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    targets = _filter_by_project(settings.get("targets", []) or [], project_id)
    found_target: Dict[str, Any] | None = None

    target_identifier_clean = _clean_username(chat_identifier)
    for t in targets:
        t_id = str(t.get("chat_id", ""))
        t_link = str(t.get("linked_chat_id", ""))
        t_user = _clean_username(t.get("chat_username", ""))
        check_val = str(chat_identifier)

        if check_val == t_id or check_val == t_link:
            found_target = t
            break

        if is_private_link:
            short_id = str(chat_identifier).replace("-100", "")
            if short_id in t_id or short_id in t_link:
                found_target = t
                break

        if t_user and t_user == target_identifier_clean:
            found_target = t
            break

    if not found_target and not is_private_link:
        try:
            client = await _get_any_authorized_client()
        except HTTPException:
            client = None
        if client:
            try:
                entity = await client.get_entity(link)
                real_id = f"-100{entity.id}"
                for t in targets:
                    t_id = str(t.get("chat_id", ""))
                    t_link = str(t.get("linked_chat_id", ""))
                    if real_id == t_id or real_id == t_link:
                        found_target = t
                        if not t.get("chat_username") and getattr(entity, "username", None):
                            t["chat_username"] = entity.username
                            _save_settings(settings)
                        break
            except Exception:
                pass
            finally:
                try:
                    if client.is_connected():
                        await client.disconnect()
                except Exception:
                    pass

    if not found_target:
        _flash(request, "warning", "Канал из ссылки не найден в ваших целях комментирования.")
        return _redirect("/manual")

    manual_task = {
        "chat_id": found_target["chat_id"],
        "post_id": post_id,
        "added_at": time.time(),
        "project_id": project_id,
    }
    settings.setdefault("manual_queue", []).append(manual_task)
    _save_settings(settings)
    _flash(request, "success", f"Задание добавлено: {found_target.get('chat_name')} / post_id={post_id}")
    return _redirect("/manual")


@app.post("/manual/clear")
async def manual_clear(request: Request):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    settings["manual_queue"] = [
        t for t in (settings.get("manual_queue", []) or []) if _project_id_for(t) != project_id
    ]
    _save_settings(settings)
    _flash(request, "success", "Очередь ручных заданий очищена.")
    return _redirect("/manual")


def _telegram_message_link(chat_username: str | None, chat_id: str | None, msg_id: int | None) -> str | None:
    if not msg_id:
        return None
    if chat_username:
        return f"https://t.me/{chat_username}/{msg_id}"
    if not chat_id:
        return None
    raw = str(chat_id)
    if raw.startswith("-100"):
        return f"https://t.me/c/{raw.replace('-100', '')}/{msg_id}"
    if raw.startswith("-"):
        return f"https://t.me/c/{raw.replace('-', '')}/{msg_id}"
    return None


@app.get("/dialogs", response_class=HTMLResponse)
async def dialogs_page(request: Request):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    accounts, _ = _load_accounts()
    sessions = [a.get("session_name") for a in _filter_accounts_by_project(accounts, project_id) if a.get("session_name")]
    rows = []
    if sessions:
        placeholders = ", ".join(["?"] * len(sessions))
        params = tuple(sessions) + tuple(sessions)
        with _db_connect() as conn:
            rows = conn.execute(
                f"""
                WITH last AS (
                  SELECT session_name, chat_id, MAX(id) AS last_id
                  FROM inbox_messages
                  WHERE kind='dm' AND session_name IN ({placeholders})
                  GROUP BY session_name, chat_id
                ),
                unread AS (
                  SELECT session_name, chat_id,
                         SUM(CASE WHEN direction='in' AND is_read=0 THEN 1 ELSE 0 END) AS unread
                  FROM inbox_messages
                  WHERE kind='dm' AND session_name IN ({placeholders})
                  GROUP BY session_name, chat_id
                )
                SELECT m.*,
                       COALESCE(u.unread, 0) AS unread
                FROM inbox_messages m
                JOIN last l ON l.last_id = m.id
                LEFT JOIN unread u ON u.session_name = m.session_name AND u.chat_id = m.chat_id
                ORDER BY m.id DESC
                LIMIT 200
                """,
                params,
            ).fetchall()

    threads: List[Dict[str, Any]] = []
    for r in rows:
        title = r["chat_title"] or r["sender_name"] or r["chat_username"] or r["sender_username"] or r["chat_id"]
        threads.append(
            {
                "session_name": r["session_name"],
                "chat_id": r["chat_id"],
                "title": title,
                "last_text": r["text"] or "",
                "last_at": r["created_at"],
                "unread": int(r["unread"] or 0),
                "url": f"/dialogs/{quote(str(r['session_name']))}/{quote(str(r['chat_id']))}",
            }
        )

    return templates.TemplateResponse("dialogs.html", _template_context(request, threads=threads))


@app.get("/dialogs/{session_name}/{chat_id}", response_class=HTMLResponse)
async def dialog_thread_page(request: Request, session_name: str, chat_id: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    if not any(a.get("session_name") == session_name and _project_id_for(a) == project_id for a in accounts):
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    with _db_connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM inbox_messages
            WHERE kind='dm' AND session_name=? AND chat_id=?
            ORDER BY id DESC
            LIMIT 400
            """,
            (session_name, chat_id),
        ).fetchall()
        conn.execute(
            "UPDATE inbox_messages SET is_read=1 WHERE kind='dm' AND session_name=? AND chat_id=? AND direction='in'",
            (session_name, chat_id),
        )
        conn.commit()

    messages = list(reversed([dict(r) for r in rows]))
    title = None
    for m in reversed(messages):
        title = m.get("chat_title") or m.get("sender_name") or m.get("chat_username") or m.get("chat_id")
        if title:
            break
    title = title or chat_id

    return templates.TemplateResponse(
        "dialog_thread.html",
        _template_context(
            request,
            session_name=session_name,
            chat_id=chat_id,
            title=title,
            messages=messages,
        ),
    )


@app.post("/dialogs/{session_name}/{chat_id}/send")
async def dialog_send_message(request: Request, session_name: str, chat_id: str, text: str = Form(...)):
    text = text.strip()
    if not text:
        _flash(request, "warning", "Сообщение пустое.")
        return _redirect(f"/dialogs/{quote(session_name)}/{quote(chat_id)}")

    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    if not any(a.get("session_name") == session_name and _project_id_for(a) == project_id for a in accounts):
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    now = datetime.now(timezone.utc).isoformat()
    with _db_connect() as conn:
        conn.execute(
            """
            INSERT INTO inbox_messages (
                kind, direction, status, created_at,
                session_name, chat_id,
                text, is_read
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("dm", "out", "queued", now, session_name, chat_id, text, 1),
        )
        conn.execute(
            """
            INSERT INTO outbound_queue (chat_id, reply_to_msg_id, session_name, text, status)
            VALUES (?, ?, ?, ?, 'pending')
            """,
            (chat_id, None, session_name, text),
        )
        conn.commit()

    _flash(request, "success", "Сообщение поставлено в очередь на отправку.")
    return _redirect(f"/dialogs/{quote(session_name)}/{quote(chat_id)}")


@app.get("/quotes", response_class=HTMLResponse)
async def quotes_page(request: Request):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    accounts, _ = _load_accounts()
    sessions = [a.get("session_name") for a in _filter_accounts_by_project(accounts, project_id) if a.get("session_name")]
    rows = []
    if sessions:
        placeholders = ", ".join(["?"] * len(sessions))
        with _db_connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM inbox_messages
                WHERE kind='quote' AND session_name IN ({placeholders})
                ORDER BY id DESC
                LIMIT 300
                """,
                tuple(sessions),
            ).fetchall()

    items: List[Dict[str, Any]] = []
    for r in rows:
        link = _telegram_message_link(r["chat_username"], r["chat_id"], r["msg_id"])
        items.append(
            {
                **dict(r),
                "title": r["chat_title"] or r["chat_username"] or r["chat_id"],
                "sender": r["sender_name"] or r["sender_username"] or (str(r["sender_id"]) if r["sender_id"] else ""),
                "is_unread": bool(r["is_read"] == 0),
                "link": link,
                "url": f"/quotes/{r['id']}",
            }
        )

    return templates.TemplateResponse("quotes.html", _template_context(request, items=items))


@app.get("/quotes/{inbox_id}", response_class=HTMLResponse)
async def quote_detail_page(request: Request, inbox_id: int):
    with _db_connect() as conn:
        row = conn.execute("SELECT * FROM inbox_messages WHERE id = ? AND kind='quote'", (inbox_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Сообщение не найдено")
        settings, _ = _load_settings()
        project_id = _active_project_id(settings)
        accounts, _ = _load_accounts()
        if not any(
            a.get("session_name") == row["session_name"] and _project_id_for(a) == project_id for a in accounts
        ):
            raise HTTPException(status_code=404, detail="Сообщение не найдено в текущем проекте")
        conn.execute("UPDATE inbox_messages SET is_read=1 WHERE id = ?", (inbox_id,))
        conn.commit()

    link = _telegram_message_link(row["chat_username"], row["chat_id"], row["msg_id"])
    item = {
        **dict(row),
        "title": row["chat_title"] or row["chat_username"] or row["chat_id"],
        "sender": row["sender_name"] or row["sender_username"] or (str(row["sender_id"]) if row["sender_id"] else ""),
        "link": link,
    }
    return templates.TemplateResponse("quote_detail.html", _template_context(request, item=item))


@app.post("/quotes/{inbox_id}/reply")
async def quote_reply(request: Request, inbox_id: int, text: str = Form(...)):
    text = text.strip()
    if not text:
        _flash(request, "warning", "Сообщение пустое.")
        return _redirect(f"/quotes/{inbox_id}")

    with _db_connect() as conn:
        row = conn.execute("SELECT * FROM inbox_messages WHERE id = ? AND kind='quote'", (inbox_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Сообщение не найдено")
        settings, _ = _load_settings()
        project_id = _active_project_id(settings)
        accounts, _ = _load_accounts()
        if not any(
            a.get("session_name") == row["session_name"] and _project_id_for(a) == project_id for a in accounts
        ):
            raise HTTPException(status_code=404, detail="Сообщение не найдено в текущем проекте")

        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            """
            INSERT INTO inbox_messages (
                kind, direction, status, created_at,
                session_name, chat_id,
                reply_to_msg_id,
                text, is_read
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("quote", "out", "queued", now, row["session_name"], row["chat_id"], row["msg_id"], text, 1),
        )
        conn.execute(
            """
            INSERT INTO outbound_queue (chat_id, reply_to_msg_id, session_name, text, status)
            VALUES (?, ?, ?, ?, 'pending')
            """,
            (row["chat_id"], row["msg_id"], row["session_name"], text),
        )
        conn.execute("UPDATE inbox_messages SET is_read=1 WHERE id = ?", (inbox_id,))
        conn.commit()

    _flash(request, "success", "Ответ поставлен в очередь на отправку.")
    return _redirect(f"/quotes/{inbox_id}")

"""Account resolution, model settings, and utility functions.

Extracted from commentator.py — account loading, session/proxy resolution,
model candidate selection, image type detection, sleep schedule.
"""

import os
from datetime import datetime

from telethon.sessions import StringSession

from app_paths import ACCOUNTS_FILE
from app_storage import load_json, save_json
from tg_device import ensure_device_profile
from role_engine import (
    ensure_accounts_have_roles,
    ensure_role_schema,
    role_for_account,
)
from services.project import _active_project_id, _filter_project_items


ACCOUNTS_DIR = os.getenv("APP_ACCOUNTS_DIR", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "accounts"))

DEFAULT_MODELS = {
    "openai_chat": "gpt-5.4-chat-latest",
    "openai_eval": "gpt-5.4-mini",
    "openai_image": "gpt-image-1",
    "openrouter_chat": "x-ai/grok-4.1-fast",
    "openrouter_eval": "openai/gpt-5.4-mini",
    "deepseek_chat": "deepseek-chat",
    "deepseek_eval": "deepseek-chat",
    "gemini_chat": "gemini-3.1-flash-lite-preview",
    "gemini_eval": "gemini-3.1-flash-lite-preview",
    "gemini_names": "gemini-3.1-flash-lite-preview",
}


def _load_json_data(file_path, default_data=None):
    if default_data is None:
        default_data = {}
    return load_json(file_path, default_data)


# ---------------------------------------------------------------------------
# Account loading
# ---------------------------------------------------------------------------

def load_project_accounts(settings=None, *, current_settings=None):
    """Load accounts for the active project.

    Args:
        settings: Explicit settings dict. Falls back to current_settings.
        current_settings: Global settings dict (passed from caller).
    """
    settings_obj = settings if isinstance(settings, dict) else (current_settings if isinstance(current_settings, dict) else {})
    if not isinstance(settings_obj, dict):
        settings_obj = {}
    ensure_role_schema(settings_obj)

    pid = _active_project_id(settings_obj)
    accounts = _load_json_data(ACCOUNTS_FILE, [])
    changed = False
    if isinstance(accounts, list):
        for acc in accounts:
            if isinstance(acc, dict) and ensure_device_profile(acc):
                changed = True
        if ensure_accounts_have_roles(accounts, settings_obj):
            changed = True
    if changed:
        try:
            save_json(ACCOUNTS_FILE, accounts)
        except Exception:
            pass
    accounts = _filter_project_items(accounts, pid)
    dir_accounts = _load_accounts_from_dir(pid, settings_obj)
    return _merge_accounts_by_session_name(accounts, dir_accounts)


def _merge_accounts_by_session_name(primary: list, secondary: list) -> list:
    merged: dict[str, dict] = {}
    for acc in secondary or []:
        if isinstance(acc, dict):
            name = str(acc.get("session_name") or "").strip()
            if name:
                merged[name] = acc
    for acc in primary or []:
        if isinstance(acc, dict):
            name = str(acc.get("session_name") or "").strip()
            if not name:
                continue
            if name in merged:
                merged[name] = {**merged[name], **acc}
            else:
                merged[name] = acc
    return list(merged.values())


def _load_accounts_from_dir(project_id: str, settings: dict | None = None) -> list[dict]:
    accounts_dir = ACCOUNTS_DIR
    if not accounts_dir or not os.path.isdir(accounts_dir):
        return []

    settings_obj = settings if isinstance(settings, dict) else {}
    if settings_obj:
        ensure_role_schema(settings_obj)

    accounts: list[dict] = []
    try:
        entries = sorted(os.listdir(accounts_dir))
    except Exception:
        return []

    for filename in entries:
        if not filename.endswith(".json"):
            continue
        path = os.path.join(accounts_dir, filename)
        data = _load_json_data(path, None)
        if not isinstance(data, dict):
            continue

        if ensure_device_profile(data):
            try:
                save_json(path, data)
            except Exception:
                pass

        session_file = str(data.get("session_file") or data.get("phone") or os.path.splitext(filename)[0] or "").strip()
        if not session_file:
            continue

        session_name = str(data.get("session_name") or session_file).strip()
        session_path = _find_session_file_path(session_file, accounts_dir)

        sleep_settings = data.get("sleep_settings")
        if not isinstance(sleep_settings, dict):
            sleep_settings = {"start_hour": 0, "end_hour": 23}

        account = {
            "session_name": session_name,
            "session_file": session_file,
            "session_path": session_path,
            "app_id": data.get("app_id"),
            "app_hash": data.get("app_hash"),
            "proxy": data.get("proxy"),
            "user_id": data.get("user_id"),
            "first_name": data.get("first_name") or "",
            "last_name": data.get("last_name") or "",
            "username": data.get("username") or "",
            "status": data.get("status") or "active",
            "sleep_settings": sleep_settings,
            "project_id": data.get("project_id") or project_id,
            "device_type": data.get("device_type"),
            "device_model": data.get("device_model"),
            "system_version": data.get("system_version"),
            "app_version": data.get("app_version"),
            "lang_code": data.get("lang_code"),
            "system_lang_code": data.get("system_lang_code"),
            "role_id": data.get("role_id"),
            "persona_id": data.get("persona_id"),
        }
        if settings_obj:
            resolved_role_id, _ = role_for_account(account, settings_obj)
            if resolved_role_id:
                account["role_id"] = resolved_role_id
        accounts.append(account)

    return accounts


def _find_session_file_path(session_file: str, accounts_dir: str) -> str | None:
    if not session_file:
        return None
    candidates = []
    raw = str(session_file)
    if os.path.isabs(raw):
        candidates.append(raw)
    if raw.endswith(".session"):
        candidates.append(raw)
    if not os.path.isabs(raw):
        candidates.append(os.path.join(accounts_dir, raw))
        candidates.append(os.path.join(accounts_dir, f"{raw}.session"))
    for path in candidates:
        if path and os.path.exists(path):
            return path
    return None


def _resolve_account_credentials(account_data: dict, fallback_api_id: int, fallback_api_hash: str) -> tuple[int, str | None]:
    api_id = account_data.get("app_id") or account_data.get("api_id") or fallback_api_id
    api_hash = account_data.get("app_hash") or account_data.get("api_hash") or fallback_api_hash
    try:
        api_id = int(api_id)
    except Exception:
        api_id = fallback_api_id
    return api_id, api_hash


def _resolve_account_session(account_data: dict) -> StringSession | str | None:
    session_string = (account_data.get("session_string") or "").strip()
    if session_string:
        return StringSession(session_string)

    session_path = account_data.get("session_path")
    if isinstance(session_path, str) and session_path and os.path.exists(session_path):
        return session_path

    session_file = account_data.get("session_file") or account_data.get("session_name")
    if not session_file:
        return None
    return _find_session_file_path(str(session_file), ACCOUNTS_DIR)


def _resolve_account_proxy(account_data: dict):
    proxy_url = account_data.get("proxy_url")
    if proxy_url:
        return _parse_proxy_url(proxy_url)

    proxy_tuple = account_data.get("proxy")
    if not isinstance(proxy_tuple, (list, tuple)) or len(proxy_tuple) < 3:
        return None

    proxy_type_raw = proxy_tuple[0]
    host = proxy_tuple[1]
    port = proxy_tuple[2]
    user = proxy_tuple[3] if len(proxy_tuple) > 3 else None
    password = proxy_tuple[4] if len(proxy_tuple) > 4 else None

    if not host or not port:
        return None

    proxy_type = "socks5"
    if isinstance(proxy_type_raw, str) and proxy_type_raw:
        proxy_type = proxy_type_raw.lower()
    elif isinstance(proxy_type_raw, int):
        if proxy_type_raw == 1:
            proxy_type = "socks4"
        elif proxy_type_raw in (2, 3):
            proxy_type = "http" if proxy_type_raw == 3 else "socks5"
        else:
            proxy_type = "socks5"

    try:
        port = int(port)
    except Exception:
        return None

    return (proxy_type, host, port, True, user, password)


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


# ---------------------------------------------------------------------------
# Model settings
# ---------------------------------------------------------------------------

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

    # Fallbacks: current 3.x lite preview → stable 2.5 flash (still available until
    # 2026-06-17 deprecation). We no longer fall back to gemini-1.5-* (EOL).
    for fallback in ["gemini-3.1-flash-lite-preview", "gemini-2.5-flash"]:
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
        candidates.extend(["gpt-5.4-chat-latest", "gpt-5.4", "gpt-5.4-mini", "gpt-5-mini", "gpt-4.1", "gpt-4.1-mini"])
    elif key == "openai_eval":
        candidates.extend(["gpt-5.4-mini", "gpt-5-mini", "gpt-4.1-mini"])

    seen = set()
    unique = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            unique.append(candidate)

    return unique


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------

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

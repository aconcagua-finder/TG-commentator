"""Account profile management — update name/bio/avatar/username, connect
temp clients, mark accounts as banned.

Extracted from commentator.py.
"""

import json
import logging
import os
from datetime import datetime, timezone

from telethon import TelegramClient
from telethon import utils as tg_utils
from telethon.tl import types as tl_types
from telethon.tl.functions.account import (
    UpdatePersonalChannelRequest,
    UpdateProfileRequest,
    UpdateUsernameRequest,
)
from telethon.tl.functions.photos import (
    DeletePhotosRequest,
    UploadProfilePhotoRequest,
)

from app_paths import ACCOUNTS_FILE, SETTINGS_FILE
from app_storage import load_json, save_json
from services.account_utils import (
    _resolve_account_credentials,
    _resolve_account_proxy,
    _resolve_account_session,
    load_project_accounts,
)
from tg_device import device_kwargs

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Mark account as banned
# ---------------------------------------------------------------------------

async def mark_account_as_banned(session_name):
    accounts = load_json(ACCOUNTS_FILE, [])
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


# ---------------------------------------------------------------------------
# Temporary client connection
# ---------------------------------------------------------------------------

async def _connect_temp_client(account_data: dict, api_id: int, api_hash: str):
    api_id, api_hash = _resolve_account_credentials(account_data, api_id, api_hash)
    if not api_id or not api_hash:
        raise RuntimeError("missing_api_credentials")
    proxy = _resolve_account_proxy(account_data)
    session = _resolve_account_session(account_data)
    if not session:
        raise RuntimeError("missing_session")
    client = TelegramClient(
        session,
        api_id,
        api_hash,
        proxy=proxy,
        **device_kwargs(account_data),
    )
    await client.connect()
    if not await client.is_user_authorized():
        try:
            await client.disconnect()
        except Exception:
            pass
        raise RuntimeError("account_not_authorized")
    return client


# ---------------------------------------------------------------------------
# Profile photo helpers
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Update account profile
# ---------------------------------------------------------------------------

async def update_account_profile(
    client: TelegramClient,
    *,
    first_name: str | None = None,
    last_name: str | None = None,
    username: str | None = None,
    username_clear: bool = False,
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
    if username is not None:
        username = str(username).strip()
        username = username.replace("https://t.me/", "").replace("http://t.me/", "").replace("t.me/", "")
        username = username.lstrip("@").strip().lower()
        if not username:
            username_clear = True

    if first_name is not None or last_name is not None or bio is not None:
        await client(
            UpdateProfileRequest(
                first_name=first_name,
                last_name=last_name,
                about=bio,
            )
        )

    if username_clear:
        await client(UpdateUsernameRequest(username=""))
    elif username is not None:
        await client(UpdateUsernameRequest(username=username))

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


# ---------------------------------------------------------------------------
# Process profile tasks from settings
# ---------------------------------------------------------------------------

async def process_profile_tasks(
    api_id: int,
    api_hash: str,
    *,
    current_settings: dict,
    active_clients: dict,
) -> None:
    """Process pending profile update tasks.

    Parameters
    ----------
    current_settings : dict
        The global settings dict (mutated in-place to update task status).
    active_clients : dict
        session_name -> CommentatorClient mapping.
    """
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
            username = task.get("username")
            username_clear = bool(task.get("username_clear"))
            personal_channel = task.get("personal_channel")
            avatar_clear = bool(task.get("avatar_clear"))
            personal_channel_clear = bool(task.get("personal_channel_clear"))

            await update_account_profile(
                client,
                first_name=first_name,
                last_name=last_name,
                username=username,
                username_clear=username_clear,
                bio=bio,
                avatar_path=avatar_path,
                avatar_clear=avatar_clear,
                personal_channel=personal_channel,
                personal_channel_clear=personal_channel_clear,
            )

            me_username = None
            try:
                me = await client.get_me()
                if me:
                    account_data["user_id"] = getattr(me, "id", account_data.get("user_id"))
                    account_data["first_name"] = getattr(me, "first_name", account_data.get("first_name"))
                    account_data["last_name"] = getattr(me, "last_name", "") or ""
                    me_username = getattr(me, "username", None)
                    account_data["username"] = me_username or ""
            except Exception:
                pass

            if bio is not None:
                account_data["profile_bio"] = str(bio)
            if username is not None or username_clear:
                if username_clear:
                    account_data["profile_username"] = ""
                elif me_username is not None:
                    account_data["profile_username"] = me_username or ""
                else:
                    u = str(username or "").strip()
                    u = u.replace("https://t.me/", "").replace("http://t.me/", "").replace("t.me/", "")
                    u = u.lstrip("@").strip().lower()
                    account_data["profile_username"] = u
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

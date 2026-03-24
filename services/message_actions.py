"""Telegram message edit/delete helpers."""

from __future__ import annotations

import configparser
import logging
import os

from telethon.errors import (
    MessageAuthorRequiredError,
    MessageDeleteForbiddenError,
    MessageEditTimeExpiredError,
    MessageIdInvalidError,
    MessageNotModifiedError,
    RPCError,
)

from app_paths import CONFIG_FILE
from services.account_utils import load_project_accounts
from services.profile import _connect_temp_client

logger = logging.getLogger(__name__)


def _db_connect():
    from db.connection import get_connection
    return get_connection()


def _load_config_section(section: str):
    parser = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Файл config.ini не найден: {CONFIG_FILE}")
    parser.read(CONFIG_FILE)
    if section not in parser:
        raise KeyError(f"В config.ini не найдена секция [{section}].")
    return parser[section]


def _entity_ref(chat_id):
    raw = str(chat_id or "").strip()
    if not raw:
        raise ValueError("empty_chat_id")
    if raw.startswith("-100") and raw[4:].isdigit():
        return int(raw[4:])
    if raw.lstrip("-").isdigit():
        return int(raw)
    return raw


def _chat_id_variants(chat_id) -> list[int]:
    raw = str(chat_id or "").strip()
    if not raw or not raw.lstrip("-").isdigit():
        return []
    variants: list[int] = []
    try:
        variants.append(int(raw))
    except Exception:
        return []
    if raw.startswith("-100") and raw[4:].isdigit():
        variants.append(int(raw[4:]))
    elif raw.isdigit():
        variants.append(int(f"-100{raw}"))
    deduped: list[int] = []
    for value in variants:
        if value not in deduped:
            deduped.append(value)
    return deduped


async def _resolve_client(session_name: str, *, active_clients: dict, current_settings: dict):
    client_wrapper = active_clients.get(session_name)
    client = getattr(client_wrapper, "client", None) if client_wrapper is not None else None
    if client is not None:
        return client, None

    telethon_config = _load_config_section("telethon_credentials")
    api_id = int(telethon_config["api_id"])
    api_hash = telethon_config["api_hash"]
    accounts_data = load_project_accounts(current_settings=current_settings)
    account_data = next((a for a in accounts_data if a.get("session_name") == session_name), None)
    if not account_data:
        raise RuntimeError("Аккаунт не найден в текущем проекте.")
    temp_client = await _connect_temp_client(account_data, api_id, api_hash)
    return temp_client, temp_client


def _update_related_rows(
    *,
    source: str | None,
    record_id: int | None,
    session_name: str,
    chat_id,
    msg_id: int,
    new_text: str | None = None,
    deleted: bool = False,
) -> None:
    chat_variants = _chat_id_variants(chat_id)
    with _db_connect() as conn:
        if deleted:
            if source == "logs" and record_id is not None:
                conn.execute("DELETE FROM logs WHERE id = ?", (int(record_id),))
            elif chat_variants:
                placeholders = ", ".join(["?"] * len(chat_variants))
                conn.execute(
                    f"""
                    DELETE FROM logs
                    WHERE account_session_name = ? AND msg_id = ?
                      AND destination_chat_id IN ({placeholders})
                    """,
                    (session_name, int(msg_id), *chat_variants),
                )

            if source == "inbox" and record_id is not None:
                conn.execute(
                    "UPDATE inbox_messages SET status='deleted', error=NULL WHERE id = ?",
                    (int(record_id),),
                )
            else:
                conn.execute(
                    """
                    UPDATE inbox_messages
                    SET status='deleted', error=NULL
                    WHERE direction='out' AND session_name = ? AND chat_id = ? AND msg_id = ?
                    """,
                    (session_name, str(chat_id), int(msg_id)),
                )
        else:
            if source == "logs" and record_id is not None:
                conn.execute("UPDATE logs SET content = ? WHERE id = ?", (new_text, int(record_id)))
            elif chat_variants:
                placeholders = ", ".join(["?"] * len(chat_variants))
                conn.execute(
                    f"""
                    UPDATE logs
                    SET content = ?
                    WHERE account_session_name = ? AND msg_id = ?
                      AND destination_chat_id IN ({placeholders})
                    """,
                    (new_text, session_name, int(msg_id), *chat_variants),
                )

            if source == "inbox" and record_id is not None:
                conn.execute(
                    "UPDATE inbox_messages SET text = ?, error=NULL WHERE id = ?",
                    (new_text, int(record_id)),
                )
            else:
                conn.execute(
                    """
                    UPDATE inbox_messages
                    SET text = ?, error=NULL
                    WHERE direction='out' AND session_name = ? AND chat_id = ? AND msg_id = ?
                    """,
                    (new_text, session_name, str(chat_id), int(msg_id)),
                )
        conn.commit()


def _friendly_edit_error(exc: Exception) -> str:
    if isinstance(exc, MessageNotModifiedError):
        return "Текст не изменился."
    if isinstance(exc, MessageEditTimeExpiredError):
        return "Telegram больше не позволяет редактировать это сообщение (обычно после ~48 часов)."
    if isinstance(exc, MessageAuthorRequiredError):
        return "Сообщение нельзя редактировать не от того аккаунта, который его отправил."
    if isinstance(exc, MessageIdInvalidError):
        return "Сообщение не найдено или msg_id устарел."
    return f"Не удалось отредактировать сообщение: {exc}"


def _friendly_delete_error(exc: Exception) -> str:
    if isinstance(exc, MessageDeleteForbiddenError):
        return "Telegram не разрешает удалить это сообщение."
    if isinstance(exc, MessageAuthorRequiredError):
        return "Сообщение нельзя удалить не от того аккаунта, который его отправил."
    if isinstance(exc, MessageIdInvalidError):
        return "Сообщение не найдено или msg_id устарел."
    return f"Не удалось удалить сообщение: {exc}"


async def edit_message(
    session_name,
    chat_id,
    msg_id,
    new_text,
    *,
    active_clients,
    current_settings,
    source: str | None = None,
    record_id: int | None = None,
) -> bool:
    text = str(new_text or "").strip()
    if not session_name or msg_id is None or not text:
        return False

    client = None
    temp_client = None
    try:
        client, temp_client = await _resolve_client(
            str(session_name),
            active_clients=active_clients,
            current_settings=current_settings,
        )
        entity = await client.get_input_entity(_entity_ref(chat_id))
        try:
            await client.edit_message(entity, int(msg_id), text)
        except MessageNotModifiedError:
            pass
        except (MessageEditTimeExpiredError, MessageAuthorRequiredError, MessageIdInvalidError, RPCError) as exc:
            raise RuntimeError(_friendly_edit_error(exc)) from exc

        _update_related_rows(
            source=source,
            record_id=record_id,
            session_name=str(session_name),
            chat_id=chat_id,
            msg_id=int(msg_id),
            new_text=text,
        )
        return True
    except RuntimeError:
        raise
    except Exception:
        logger.exception("Не удалось отредактировать сообщение %s/%s/%s", session_name, chat_id, msg_id)
        return False
    finally:
        if temp_client is not None:
            try:
                if temp_client.is_connected():
                    await temp_client.disconnect()
            except Exception:
                pass


async def delete_message(
    session_name,
    chat_id,
    msg_id,
    *,
    active_clients,
    current_settings,
    source: str | None = None,
    record_id: int | None = None,
) -> bool:
    if not session_name or msg_id is None:
        return False

    client = None
    temp_client = None
    try:
        client, temp_client = await _resolve_client(
            str(session_name),
            active_clients=active_clients,
            current_settings=current_settings,
        )
        entity = await client.get_input_entity(_entity_ref(chat_id))
        try:
            await client.delete_messages(entity, [int(msg_id)])
        except (MessageDeleteForbiddenError, MessageAuthorRequiredError, MessageIdInvalidError, RPCError) as exc:
            raise RuntimeError(_friendly_delete_error(exc)) from exc

        _update_related_rows(
            source=source,
            record_id=record_id,
            session_name=str(session_name),
            chat_id=chat_id,
            msg_id=int(msg_id),
            deleted=True,
        )
        return True
    except RuntimeError:
        raise
    except Exception:
        logger.exception("Не удалось удалить сообщение %s/%s/%s", session_name, chat_id, msg_id)
        return False
    finally:
        if temp_client is not None:
            try:
                if temp_client.is_connected():
                    await temp_client.disconnect()
            except Exception:
                pass

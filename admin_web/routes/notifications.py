"""Telegram bot notification settings routes."""

from __future__ import annotations

import re
from typing import Optional

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from telethon.utils import get_peer_id

from admin_web.helpers import (
    _active_project_id,
    _auto_pause_commentator,
    _clean_username,
    _extract_invite_hash,
    _flash,
    _load_settings,
    _mask_secret,
    _redirect,
    _save_settings,
)
from admin_web.telethon_utils import _get_any_authorized_client, _resolve_channel_entity
from admin_web.templating import templates, _template_context
from services.telegram_bot import (
    TELEGRAM_BOT_EVENT_OPTIONS,
    get_project_telegram_bot_settings,
    normalize_telegram_bot_settings,
    send_test_message,
    set_project_telegram_bot_settings,
)

router = APIRouter()

_CHAT_ID_RE = re.compile(r"^-?\d+$")
_CHAT_USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{3,63}$")
_PUBLIC_T_ME_RE = re.compile(r"^(?:https?://)?t\.me/(?!\+|joinchat/|c/)([A-Za-z0-9_]{4,64})(?:/.*)?/?$", re.IGNORECASE)
_PRIVATE_T_ME_C_RE = re.compile(r"^(?:https?://)?t\.me/c/(\d+)(?:/\d+)?/?$", re.IGNORECASE)


def _valid_chat_id(chat_id: str) -> bool:
    return bool(_CHAT_ID_RE.fullmatch(str(chat_id or "").strip()))


def _valid_chat_target(chat_target: str) -> bool:
    raw = str(chat_target or "").strip()
    if _valid_chat_id(raw):
        return True
    if not raw.startswith("@"):
        return False
    return bool(_CHAT_USERNAME_RE.fullmatch(_clean_username(raw)))


def _short_exc(exc: Exception) -> str:
    try:
        msg = str(exc).replace("\n", " ").strip()
    except Exception:
        msg = ""
    if msg:
        msg = re.sub(r"\s+", " ", msg)
        if len(msg) > 220:
            msg = msg[:219].rstrip() + "…"
    name = exc.__class__.__name__
    return f"{name}: {msg}" if msg else name


def _direct_notification_chat_target(raw_chat_target: str) -> dict[str, str] | None:
    raw = str(raw_chat_target or "").strip()
    if not raw:
        return {
            "chat_id": "",
            "chat_title": "",
            "chat_username": "",
        }
    if _valid_chat_id(raw):
        return {
            "chat_id": raw,
            "chat_title": "",
            "chat_username": "",
        }

    if raw.startswith("@"):
        username = _clean_username(raw)
        if _CHAT_USERNAME_RE.fullmatch(username):
            return {
                "chat_id": f"@{username}",
                "chat_title": "",
                "chat_username": username,
            }

    public_match = _PUBLIC_T_ME_RE.fullmatch(raw)
    if public_match:
        username = _clean_username(public_match.group(1))
        if _CHAT_USERNAME_RE.fullmatch(username):
            return {
                "chat_id": f"@{username}",
                "chat_title": "",
                "chat_username": username,
            }

    private_match = _PRIVATE_T_ME_C_RE.fullmatch(raw)
    if private_match:
        return {
            "chat_id": f"-100{private_match.group(1)}",
            "chat_title": "",
            "chat_username": "",
        }

    return None


async def _resolve_notification_chat_target(request: Request, raw_chat_target: str) -> dict[str, str]:
    chat_target = str(raw_chat_target or "").strip()
    direct_target = _direct_notification_chat_target(chat_target)
    if direct_target is not None:
        return direct_target

    invite_hash = _extract_invite_hash(chat_target)
    if not invite_hash:
        raise HTTPException(
            status_code=400,
            detail="Укажите chat_id, @username, ссылку https://t.me/... или invite-ссылку вида https://t.me/+....",
        )

    async with _auto_pause_commentator(
        request,
        auto_pause=True,
        reason="Определение чата уведомлений",
    ):
        client = await _get_any_authorized_client()
        try:
            entity, _ = await _resolve_channel_entity(client, chat_target)
            resolved_chat_id = str(get_peer_id(entity))
            if not _valid_chat_target(resolved_chat_id):
                raise HTTPException(status_code=400, detail="Не удалось определить chat_id для чата уведомлений.")
            return {
                "chat_id": resolved_chat_id,
                "chat_title": str(
                    getattr(entity, "title", None)
                    or getattr(entity, "first_name", None)
                    or getattr(entity, "username", None)
                    or ""
                ).strip(),
                "chat_username": str(getattr(entity, "username", None) or "").strip().lstrip("@"),
            }
        except HTTPException as exc:
            detail = str(exc.detail)
            if "Нет авторизованных аккаунтов" in detail:
                raise HTTPException(
                    status_code=400,
                    detail="Для invite-ссылки нужен хотя бы один авторизованный аккаунт: Bot API не умеет сам определять chat_id по invite-ссылке.",
                ) from exc
            raise
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Не удалось определить чат уведомлений: {_short_exc(exc)}",
            ) from exc
        finally:
            try:
                if client.is_connected():
                    await client.disconnect()
            except Exception:
                pass


@router.get("/notifications", response_class=HTMLResponse)
async def notifications_page(request: Request):
    settings, settings_err = _load_settings()
    project_id = _active_project_id(settings)
    telegram_bot = get_project_telegram_bot_settings(settings, project_id)
    return templates.TemplateResponse(
        "notifications.html",
        _template_context(
            request,
            settings_err=settings_err,
            telegram_bot=telegram_bot,
            bot_token_masked=_mask_secret(telegram_bot.get("bot_token")),
            event_options=TELEGRAM_BOT_EVENT_OPTIONS,
        ),
    )


@router.post("/notifications")
async def notifications_save(
    request: Request,
    enabled: Optional[str] = Form(None),
    bot_token: str = Form(""),
    chat_id: str = Form(""),
    clear_bot_token: Optional[str] = Form(None),
    warnings: Optional[str] = Form(None),
    inbox_dm: Optional[str] = Form(None),
    inbox_replies: Optional[str] = Form(None),
    inbox_reactions: Optional[str] = Form(None),
    monitoring: Optional[str] = Form(None),
    spam_deleted: Optional[str] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    telegram_bot = get_project_telegram_bot_settings(settings, project_id)

    token_value = str(bot_token or "").strip()
    if token_value:
        telegram_bot["bot_token"] = token_value
    elif clear_bot_token:
        telegram_bot["bot_token"] = ""

    try:
        chat_target = await _resolve_notification_chat_target(request, chat_id)
    except HTTPException as exc:
        _flash(request, "danger", str(exc.detail))
        return _redirect("/notifications")

    telegram_bot["enabled"] = enabled is not None
    telegram_bot["chat_id"] = chat_target["chat_id"]
    telegram_bot["chat_title"] = chat_target["chat_title"]
    telegram_bot["chat_username"] = chat_target["chat_username"]
    telegram_bot["events"] = normalize_telegram_bot_settings(
        {
            "events": {
                "warnings": warnings is not None,
                "inbox_dm": inbox_dm is not None,
                "inbox_replies": inbox_replies is not None,
                "inbox_reactions": inbox_reactions is not None,
                "monitoring": monitoring is not None,
                "spam_deleted": spam_deleted is not None,
            }
        }
    )["events"]

    try:
        set_project_telegram_bot_settings(settings, project_id, telegram_bot)
    except KeyError:
        _flash(request, "danger", "Текущий проект не найден.")
        return _redirect("/notifications")

    _save_settings(settings)
    if telegram_bot["enabled"] and (not telegram_bot.get("bot_token") or not telegram_bot.get("chat_id")):
        _flash(request, "warning", "Уведомления включены, но bot_token или chat_id ещё не заполнены.")
    else:
        _flash(request, "success", "Настройки уведомлений сохранены.")
    return _redirect("/notifications")


@router.post("/notifications/test")
async def notifications_test(request: Request):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    telegram_bot = get_project_telegram_bot_settings(settings, project_id)

    form = await request.form()
    raw_token = str(form.get("bot_token") or "").strip()
    clear_bot_token = form.get("clear_bot_token") is not None
    raw_chat_target = str(form.get("chat_id") or "").strip()

    saved_bot_token = str(telegram_bot.get("bot_token") or "").strip()
    saved_chat_id = str(telegram_bot.get("chat_id") or "").strip()

    bot_token = raw_token or saved_bot_token
    if clear_bot_token and not raw_token:
        bot_token = ""

    try:
        chat_target = await _resolve_notification_chat_target(request, raw_chat_target or saved_chat_id)
    except HTTPException as exc:
        _flash(request, "danger", str(exc.detail))
        return _redirect("/notifications")

    chat_id = str(chat_target.get("chat_id") or "").strip()
    if not bot_token or not chat_id:
        _flash(request, "warning", "Укажите bot_token и чат уведомлений или сохраните их, чтобы выполнить проверку.")
        return _redirect("/notifications")
    if not _valid_chat_target(chat_id):
        _flash(request, "danger", "Текущий чат уведомлений некорректен.")
        return _redirect("/notifications")

    if raw_token:
        telegram_bot["bot_token"] = raw_token
    elif clear_bot_token:
        telegram_bot["bot_token"] = ""

    if raw_chat_target:
        telegram_bot["chat_id"] = chat_target["chat_id"]
        telegram_bot["chat_title"] = chat_target["chat_title"]
        telegram_bot["chat_username"] = chat_target["chat_username"]

    telegram_bot["enabled"] = form.get("enabled") is not None
    telegram_bot["events"] = normalize_telegram_bot_settings(
        {
            "events": {
                "warnings": form.get("warnings") is not None,
                "inbox_dm": form.get("inbox_dm") is not None,
                "inbox_replies": form.get("inbox_replies") is not None,
                "inbox_reactions": form.get("inbox_reactions") is not None,
                "monitoring": form.get("monitoring") is not None,
            }
        }
    )["events"]

    try:
        set_project_telegram_bot_settings(settings, project_id, telegram_bot)
    except KeyError:
        _flash(request, "danger", "Текущий проект не найден.")
        return _redirect("/notifications")
    _save_settings(settings)

    result = await send_test_message(bot_token, chat_id)
    if result.get("ok"):
        _flash(request, "success", "Настройки сохранены. Тестовое сообщение отправлено.")
    else:
        detail = str(result.get("description") or result.get("error") or "неизвестная ошибка").strip()
        _flash(request, "danger", f"Не удалось отправить тестовое сообщение: {detail}")
    return _redirect("/notifications")

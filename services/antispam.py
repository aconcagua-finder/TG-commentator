"""Anti-spam module for discussion groups (linked chats).

Detects spam via keywords and optional AI verification, deletes messages,
logs actions, and blocks further bot replies in the affected thread.
"""

from __future__ import annotations

import json
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Any

import openai

from services.dialogue import get_all_our_user_ids
from services.project import get_project_targets

logger = logging.getLogger(__name__)

# Cache: chat_id -> session_name that successfully deleted last time
_delete_success_cache: dict[int, str] = {}


def _db_connect():
    from db.connection import get_connection
    return get_connection()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_text(message) -> str:
    try:
        text = str(getattr(message, "message", None) or getattr(message, "text", None) or "").strip()
    except Exception:
        text = ""
    if text:
        return text
    # Fallback for non-text spam (rare but possible)
    try:
        if getattr(message, "photo", None):
            return "[фото]"
        if getattr(message, "video", None) or getattr(message, "gif", None):
            return "[видео]"
        if getattr(message, "voice", None):
            return "[голосовое]"
        if getattr(message, "audio", None):
            return "[аудио]"
        if getattr(message, "document", None) or getattr(message, "file", None):
            return "[файл]"
    except Exception:
        pass
    return ""


def _parse_keywords(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        items = raw
    else:
        s = str(raw or "").strip()
        if not s:
            return []
        try:
            parsed = json.loads(s)
        except Exception:
            return []
        items = parsed if isinstance(parsed, list) else []
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        kw = str(item or "").strip()
        if not kw:
            continue
        key = kw.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(kw)
    return out


def _keyword_match(text: str, keywords: list[str]) -> str | None:
    if not text or not keywords:
        return None
    hay = text.lower()
    for kw in keywords:
        needle = str(kw or "").strip()
        if not needle:
            continue
        if needle.lower() in hay:
            return needle
    return None


def _load_spam_rule(chat_id: str) -> dict[str, Any] | None:
    chat_id = str(chat_id or "").strip()
    if not chat_id:
        return None
    try:
        with _db_connect() as conn:
            row = conn.execute(
                """
                SELECT enabled, keywords, ai_enabled, ai_prompt, ai_model, notify_telegram
                FROM spam_rules
                WHERE chat_id = ?
                LIMIT 1
                """,
                (chat_id,),
            ).fetchone()
        if not row:
            return None
        return {
            "chat_id": chat_id,
            "enabled": int(row["enabled"] or 0),
            "keywords": _parse_keywords(row["keywords"]),
            "ai_enabled": int(row["ai_enabled"] or 0),
            "ai_prompt": str(row["ai_prompt"] or "").strip(),
            "ai_model": str(row["ai_model"] or "gpt-4.1-nano").strip() or "gpt-4.1-nano",
            "notify_telegram": int(row["notify_telegram"] or 0),
        }
    except Exception as exc:
        logger.warning("antispam: failed to load spam rule: chat_id=%s err=%s", chat_id, exc)
        return None


def _openai_api_key(current_settings: dict) -> str:
    try:
        api_keys = current_settings.get("api_keys", {}) if isinstance(current_settings, dict) else {}
    except Exception:
        api_keys = {}
    key = ""
    if isinstance(api_keys, dict):
        key = str(api_keys.get("openai") or "").strip()
    if not key:
        key = str((current_settings or {}).get("openai_api_key") or "").strip()
    return key


def _strip_code_fences(text: str) -> str:
    s = str(text or "").strip()
    if s.startswith("```"):
        s = s.strip("`").strip()
        # If it's ```json ...```, best-effort strip the first line.
        if "\n" in s:
            first, rest = s.split("\n", 1)
            if first.strip().lower() in {"json", "javascript"}:
                s = rest.strip()
    return s


async def _ai_check_spam(
    text: str,
    *,
    ai_prompt: str,
    model: str,
    api_key: str,
) -> tuple[bool, str]:
    if not api_key:
        return False, "missing_openai_api_key"
    if not text.strip():
        return False, "empty_text"

    system_prompt = (
        "Ты антиспам-фильтр для Telegram-комментариев. "
        "Определи, является ли сообщение спамом/рекламой/скамом.\n"
        "Спам: крипта/инвестиции/сигналы, казино/ставки, промокоды/скидки, "
        "реферальные ссылки, набор в команды, 'пиши в лс', предложения заработка, "
        "продажа услуг, накрутка, фишинг.\n"
        "Верни СТРОГО JSON без пояснений вокруг: "
        '{"spam": true|false, "reason": "коротко почему"}.\n'
        "Если сомневаешься — spam=false."
    )
    extra = str(ai_prompt or "").strip()
    if extra:
        system_prompt += f"\n\nДоп.описание спама для этого канала:\n{extra}"

    client = openai.AsyncOpenAI(api_key=api_key)
    try:
        completion = await client.chat.completions.create(
            model=str(model or "gpt-4.1-nano"),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Сообщение:\n{text}"},
            ],
            temperature=0,
            max_completion_tokens=120,
        )
    except Exception as exc:
        logger.warning("antispam: OpenAI request failed: %s", exc)
        return False, f"openai_error:{type(exc).__name__}"

    content = ""
    try:
        content = (completion.choices[0].message.content or "").strip()
    except Exception:
        content = ""
    if not content:
        return False, "empty_openai_response"

    raw = _strip_code_fences(content)
    try:
        payload = json.loads(raw)
    except Exception:
        return False, "invalid_json"

    spam = bool(payload.get("spam")) if isinstance(payload, dict) else False
    reason = ""
    if isinstance(payload, dict):
        reason = str(payload.get("reason") or "").strip()
    return spam, (reason or "ai")


async def _try_delete_with_client(client, chat_id: int, msg_id: int) -> bool:
    try:
        await client.delete_messages(chat_id, [msg_id])
        return True
    except Exception:
        return False


async def _delete_message_any(
    chat_id: int,
    msg_id: int,
    *,
    active_clients: dict,
) -> tuple[bool, str | None]:
    """Try deleting a message using any active client.

    Returns:
        (deleted, operator_session_name)
    """
    cached_session = None
    try:
        cached_session = _delete_success_cache.get(int(chat_id))
    except Exception:
        cached_session = None

    async def _attempt(wrapper) -> bool:
        client = getattr(wrapper, "client", None)
        if client is None:
            return False
        try:
            if not client.is_connected():
                return False
        except Exception:
            return False
        ok_local = await _try_delete_with_client(client, chat_id, msg_id)
        if ok_local:
            return True
        try:
            bare_local = int(str(chat_id).replace("-100", ""))
        except Exception:
            bare_local = None
        if bare_local is not None and bare_local != chat_id:
            return await _try_delete_with_client(client, bare_local, msg_id)
        return False

    if cached_session and isinstance(active_clients, dict) and cached_session in active_clients:
        wrapper = active_clients.get(cached_session)
        if wrapper is not None:
            ok = await _attempt(wrapper)
            if ok:
                return True, str(cached_session)
        _delete_success_cache.pop(int(chat_id), None)

    wrappers = list(active_clients.values()) if isinstance(active_clients, dict) else []
    for wrapper in wrappers:
        session_name = str(getattr(wrapper, "session_name", "") or "").strip()
        if cached_session and session_name == cached_session:
            continue

        client = getattr(wrapper, "client", None)
        if client is None:
            continue
        try:
            if not client.is_connected():
                continue
        except Exception:
            continue
        ok = await _attempt(wrapper)
        if ok:
            try:
                _delete_success_cache[int(chat_id)] = session_name
            except Exception:
                pass
            return True, session_name or None
    return False, None


def _insert_spam_log(entry: dict[str, Any]) -> int | None:
    try:
        with _db_connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO spam_log(
                    chat_id, msg_id, sender_id, sender_name, sender_username,
                    message_text, detection_method, matched_keyword, ai_reason,
                    action, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entry.get("chat_id"),
                    entry.get("msg_id"),
                    entry.get("sender_id"),
                    entry.get("sender_name"),
                    entry.get("sender_username"),
                    entry.get("message_text"),
                    entry.get("detection_method"),
                    entry.get("matched_keyword"),
                    entry.get("ai_reason"),
                    entry.get("action") or "deleted",
                    entry.get("created_at") or _now_iso(),
                ),
            )
            try:
                return int(getattr(cur, "lastrowid", None) or 0) or None
            except Exception:
                return None
    except Exception as exc:
        logger.warning("antispam: failed to insert spam_log: %s", exc)
        return None


async def _notify_spam_deleted(
    *,
    log_entry: dict[str, Any],
    target: dict[str, Any] | None,
    operator_session_name: str | None,
    current_settings: dict,
) -> None:
    if not target:
        return
    try:
        from services.telegram_bot import build_spam_notification, notify_event
    except Exception:
        return

    project_id = str(target.get("project_id") or "default").strip() or "default"
    try:
        message_text = build_spam_notification(log_entry, target)
    except Exception:
        return

    try:
        await notify_event("spam_deleted", project_id, message_text, settings=current_settings)
    except Exception as exc:
        logger.warning("antispam: telegram notify failed: %s", exc)


async def check_and_handle_spam(
    event,
    *,
    active_clients: dict,
    current_settings: dict,
    spam_blocked_msgs: set | None,
    spam_blocked_msgs_order: deque | None = None,
    spam_blocked_msgs_max: int = 10000,
) -> bool:
    """Check whether the incoming event is spam and handle it.

    Returns True if spam was detected (even if deletion failed), False otherwise.
    """
    msg = getattr(event, "message", None)
    if msg is None:
        return False

    msg_id = getattr(msg, "id", None)
    try:
        msg_id_int = int(msg_id)
    except Exception:
        return False

    chat_id = getattr(event, "chat_id", None)
    try:
        chat_id_int = int(chat_id)
    except Exception:
        return False
    chat_id_str = str(chat_id_int)

    sender_id = getattr(event, "sender_id", None) or getattr(msg, "sender_id", None)
    try:
        sender_id_int = int(sender_id) if sender_id is not None else 0
    except Exception:
        sender_id_int = 0

    # Guardrails: never handle our own messages.
    try:
        our_ids = get_all_our_user_ids(active_clients=active_clients, current_settings=current_settings)
    except Exception:
        return False
    if sender_id_int and sender_id_int in our_ids:
        return False

    rule = _load_spam_rule(chat_id_str)
    if not rule or int(rule.get("enabled") or 0) != 1:
        return False

    text = _safe_text(msg)
    matched_keyword = _keyword_match(text, rule.get("keywords") or [])
    spam = False
    detection_method = ""
    ai_reason = ""

    if matched_keyword:
        spam = True
        detection_method = "keyword"
    elif int(rule.get("ai_enabled") or 0) == 1:
        api_key = _openai_api_key(current_settings)
        spam, ai_reason = await _ai_check_spam(
            text,
            ai_prompt=str(rule.get("ai_prompt") or ""),
            model=str(rule.get("ai_model") or "gpt-4.1-nano"),
            api_key=api_key,
        )
        if spam:
            detection_method = "ai"

    if not spam:
        return False

    # Mark blocked messages (LRU eviction to avoid unbounded growth).
    if isinstance(spam_blocked_msgs, set):
        spam_blocked_msgs.add(msg_id_int)
        if isinstance(spam_blocked_msgs_order, deque):
            spam_blocked_msgs_order.append(msg_id_int)
            try:
                limit = int(spam_blocked_msgs_max or 0)
            except Exception:
                limit = 10000
            limit = max(limit, 0)
            if limit > 0:
                while len(spam_blocked_msgs_order) > limit:
                    old = spam_blocked_msgs_order.popleft()
                    spam_blocked_msgs.discard(old)

    # Delete using any admin-capable account.
    deleted, operator_session_name = await _delete_message_any(
        chat_id_int,
        msg_id_int,
        active_clients=active_clients,
    )
    action = "deleted" if deleted else "failed_to_delete"

    sender_name = ""
    sender_username = ""
    try:
        sender = await event.get_sender()
        sender_username = str(getattr(sender, "username", None) or "").strip()
        fn = str(getattr(sender, "first_name", None) or "").strip()
        ln = str(getattr(sender, "last_name", None) or "").strip()
        sender_name = (f"{fn} {ln}").strip()
    except Exception:
        sender = None
    if not sender_name and sender_id_int:
        sender_name = str(sender_id_int)

    log_entry: dict[str, Any] = {
        "chat_id": chat_id_str,
        "msg_id": msg_id_int,
        "sender_id": sender_id_int or None,
        "sender_name": sender_name or None,
        "sender_username": sender_username or None,
        "message_text": text or None,
        "detection_method": detection_method or None,
        "matched_keyword": matched_keyword if detection_method == "keyword" else None,
        "ai_reason": ai_reason if detection_method == "ai" else None,
        "action": action,
        "created_at": _now_iso(),
    }
    _insert_spam_log(log_entry)

    # Also mirror into generic action logs so it appears in /stats.
    try:
        from services.db_queries import log_action_to_db

        target = None
        for t in get_project_targets(current_settings):
            if str(t.get("linked_chat_id") or "").strip() == chat_id_str:
                target = t
                break

        chat_title = ""
        chat_username = ""
        try:
            chat = await event.get_chat()
            chat_title = str(getattr(chat, "title", None) or "").strip()
            chat_username = str(getattr(chat, "username", None) or "").strip()
        except Exception:
            chat = None

        log_action_to_db(
            {
                "type": "spam_deleted",
                "post_id": msg_id_int,
                "msg_id": msg_id_int,
                "comment": (
                    f"[{detection_method}] "
                    + (f"kw={matched_keyword} " if matched_keyword else "")
                    + (f"reason={ai_reason} " if ai_reason else "")
                    + (text or "")
                ).strip(),
                "date": _now_iso(),
                "account": {
                    "session_name": operator_session_name or "",
                    "first_name": "",
                    "username": "",
                },
                "target": {
                    "chat_name": str((target or {}).get("chat_name") or chat_title or chat_id_str),
                    "chat_username": str((target or {}).get("chat_username") or chat_username or ""),
                    "channel_id": chat_id_str,
                    "destination_chat_id": chat_id_str,
                },
            }
        )
    except Exception:
        target = None

    if int(rule.get("notify_telegram") or 0) == 1:
        try:
            await _notify_spam_deleted(
                log_entry=log_entry,
                target=target,
                operator_session_name=operator_session_name,
                current_settings=current_settings,
            )
        except Exception:
            pass

    if deleted:
        logger.info(
            "🧹 [antispam] deleted chat=%s msg=%s method=%s",
            chat_id_str,
            msg_id_int,
            detection_method,
        )
    else:
        logger.warning(
            "⚠️ [antispam] detected but failed to delete chat=%s msg=%s method=%s",
            chat_id_str,
            msg_id_int,
            detection_method,
        )

    return True

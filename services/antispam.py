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

import httpx
import openai

from services.dialogue import get_all_our_user_ids
from services.project import get_project_targets

logger = logging.getLogger(__name__)

# Cache: chat_id -> session_name that successfully deleted last time
_delete_success_cache: dict[int, str] = {}


async def _delete_message_via_bot(
    bot_token: str,
    chat_id: int,
    msg_id: int,
) -> bool:
    """Delete a message using Telegram Bot API (deleteMessage)."""
    token = str(bot_token or "").strip()
    if not token:
        return False
    url = f"https://api.telegram.org/bot{token}/deleteMessage"
    payload = {"chat_id": chat_id, "message_id": msg_id}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload)
        data = resp.json() if resp.is_success else {}
        if isinstance(data, dict) and data.get("ok") is True:
            return True
        logger.warning(
            "antispam: Bot API deleteMessage failed: chat=%s msg=%s status=%s resp=%s",
            chat_id, msg_id, resp.status_code, resp.text[:200],
        )
    except Exception as exc:
        logger.warning("antispam: Bot API deleteMessage exception: %s", exc)
    return False


async def _ban_user_via_bot(bot_token: str, chat_id: int, user_id: int) -> bool:
    """Ban a user using Telegram Bot API (banChatMember)."""
    token = str(bot_token or "").strip()
    if not token or not user_id:
        return False
    url = f"https://api.telegram.org/bot{token}/banChatMember"
    payload = {"chat_id": chat_id, "user_id": user_id}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload)
        data = resp.json() if resp.is_success else {}
        if isinstance(data, dict) and data.get("ok") is True:
            return True
        logger.warning(
            "antispam: Bot API banChatMember failed: chat=%s user=%s resp=%s",
            chat_id, user_id, resp.text[:200],
        )
    except Exception as exc:
        logger.warning("antispam: Bot API banChatMember exception: %s", exc)
    return False


async def _unban_user_via_bot(bot_token: str, chat_id: int, user_id: int) -> bool:
    """Unban a user using Telegram Bot API (unbanChatMember)."""
    token = str(bot_token or "").strip()
    if not token or not user_id:
        return False
    url = f"https://api.telegram.org/bot{token}/unbanChatMember"
    payload = {"chat_id": chat_id, "user_id": user_id, "only_if_banned": True}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload)
        data = resp.json() if resp.is_success else {}
        if isinstance(data, dict) and data.get("ok") is True:
            return True
        logger.warning(
            "antispam: Bot API unbanChatMember failed: chat=%s user=%s resp=%s",
            chat_id, user_id, resp.text[:200],
        )
    except Exception as exc:
        logger.warning("antispam: Bot API unbanChatMember exception: %s", exc)
    return False


async def _ban_user_via_client(
    chat_id: int,
    user_id: int,
    *,
    active_clients: dict,
    allowed_sessions: list[str] | None = None,
) -> bool:
    """Ban a user using any suitable Telethon client."""
    if not user_id:
        return False
    from telethon.tl.functions.channels import EditBannedRequest
    from telethon.tl.types import ChatBannedRights

    rights = ChatBannedRights(until_date=0, view_messages=True, send_messages=True)
    wrappers = list(active_clients.values()) if isinstance(active_clients, dict) else []
    if allowed_sessions:
        allowed_set = set(allowed_sessions)
        wrappers = [w for w in wrappers if str(getattr(w, "session_name", "") or "").strip() in allowed_set]

    for wrapper in wrappers:
        client = getattr(wrapper, "client", None)
        if client is None:
            continue
        try:
            if not client.is_connected():
                continue
            await client(EditBannedRequest(channel=chat_id, participant=user_id, banned_rights=rights))
            return True
        except Exception as exc:
            logger.debug("antispam: Telethon ban failed via %s: %s", getattr(wrapper, "session_name", "?"), exc)
            # Try bare ID as fallback.
            try:
                bare = int(str(chat_id).replace("-100", ""))
                if bare != chat_id:
                    await client(EditBannedRequest(channel=bare, participant=user_id, banned_rights=rights))
                    return True
            except Exception:
                pass
    return False


async def _unban_user_via_client(
    chat_id: int,
    user_id: int,
    *,
    active_clients: dict,
    allowed_sessions: list[str] | None = None,
) -> bool:
    """Unban a user using any suitable Telethon client."""
    if not user_id:
        return False
    from telethon.tl.functions.channels import EditBannedRequest
    from telethon.tl.types import ChatBannedRights

    rights = ChatBannedRights(until_date=0)  # All False = no restrictions.
    wrappers = list(active_clients.values()) if isinstance(active_clients, dict) else []
    if allowed_sessions:
        allowed_set = set(allowed_sessions)
        wrappers = [w for w in wrappers if str(getattr(w, "session_name", "") or "").strip() in allowed_set]

    for wrapper in wrappers:
        client = getattr(wrapper, "client", None)
        if client is None:
            continue
        try:
            if not client.is_connected():
                continue
            await client(EditBannedRequest(channel=chat_id, participant=user_id, banned_rights=rights))
            return True
        except Exception as exc:
            logger.debug("antispam: Telethon unban failed via %s: %s", getattr(wrapper, "session_name", "?"), exc)
            try:
                bare = int(str(chat_id).replace("-100", ""))
                if bare != chat_id:
                    await client(EditBannedRequest(channel=bare, participant=user_id, banned_rights=rights))
                    return True
            except Exception:
                pass
    return False


def _insert_spam_ban(entry: dict[str, Any]) -> None:
    """Insert or update a ban record in spam_bans."""
    try:
        with _db_connect() as conn:
            conn.execute(
                """
                INSERT INTO spam_bans(chat_id, user_id, username, display_name, reason, detection_method, banned_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(chat_id, user_id) DO UPDATE SET
                    username = excluded.username,
                    display_name = excluded.display_name,
                    reason = excluded.reason,
                    detection_method = excluded.detection_method,
                    banned_at = excluded.banned_at,
                    unbanned_at = NULL
                """,
                (
                    entry.get("chat_id"),
                    entry.get("user_id"),
                    entry.get("username"),
                    entry.get("display_name"),
                    entry.get("reason"),
                    entry.get("detection_method"),
                    entry.get("banned_at") or _now_iso(),
                ),
            )
    except Exception as exc:
        logger.warning("antispam: failed to insert spam_ban: %s", exc)


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
    """Match keywords in text. Supports a trailing '*' as a prefix marker.

    - "крипта"   → requires exact substring "крипта" (strict, as before)
    - "крипт*"   → matches any word that starts with "крипт": крипта/крипте/
                   крипту/крипторубль/…

    The prefix marker is used instead of always stripping endings because
    short keywords (3-4 letters) would otherwise cause false positives.
    """
    if not text or not keywords:
        return None
    import re as _re
    hay = text.lower()
    for kw in keywords:
        needle_raw = str(kw or "").strip()
        if not needle_raw:
            continue
        needle = needle_raw.lower()
        if needle.endswith("*"):
            stem = needle[:-1]
            if not stem:
                continue
            # Word starts with stem: either at the very beginning, or after
            # a non-letter character (space, punctuation, etc.).
            pattern = r"(?:^|[^\w])" + _re.escape(stem) + r"\w*"
            if _re.search(pattern, hay, flags=_re.UNICODE):
                return needle_raw
        else:
            if needle in hay:
                return needle_raw
    return None


def _load_spam_rule(chat_id: str) -> dict[str, Any] | None:
    chat_id = str(chat_id or "").strip()
    if not chat_id:
        return None
    try:
        with _db_connect() as conn:
            row = conn.execute(
                """
                SELECT enabled, keywords, name_keywords, ai_enabled, ai_check_name,
                       ai_prompt, ai_model, notify_telegram
                FROM spam_rules
                WHERE chat_id = %s
                LIMIT 1
                """,
                (chat_id,),
            ).fetchone()
        if not row:
            return None
        # Tolerate older rows where new columns may be NULL.
        try:
            name_keywords_raw = row["name_keywords"]
        except (KeyError, IndexError):
            name_keywords_raw = None
        try:
            ai_check_name_raw = row["ai_check_name"]
        except (KeyError, IndexError):
            ai_check_name_raw = 0
        return {
            "chat_id": chat_id,
            "enabled": int(row["enabled"] or 0),
            "keywords": _parse_keywords(row["keywords"]),
            "name_keywords": _parse_keywords(name_keywords_raw),
            "ai_enabled": int(row["ai_enabled"] or 0),
            "ai_check_name": int(ai_check_name_raw or 0),
            "ai_prompt": str(row["ai_prompt"] or "").strip(),
            "ai_model": str(row["ai_model"] or "gpt-5-mini").strip() or "gpt-5-mini",
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
    sender_display: str = "",
    check_name: bool = False,
) -> tuple[bool, str, str]:
    """Check if a message is spam via OpenAI.

    Returns:
        (is_spam, reason, source) where source is "text" or "name".
    """
    if not api_key:
        return False, "missing_openai_api_key", ""
    if not text.strip() and not (check_name and sender_display.strip()):
        return False, "empty_input", ""

    extra = str(ai_prompt or "").strip()

    system_prompt = "Ты антиспам-фильтр для Telegram-комментариев. "
    if extra:
        # Put the channel-specific definition FIRST and make it the primary
        # rule. Previously the custom prompt was appended after the default
        # categories and a "when in doubt — not spam" hedge, so short prompts
        # like "упоминания крипты" were ignored by the model.
        system_prompt += (
            "Главное правило (задано администратором канала): "
            f"{extra}\n"
            "Любое сообщение, подпадающее под это правило, — СПАМ (spam=true), "
            "даже если текст выглядит вежливым или осмысленным, а автор не призывает напрямую. "
            "Признаки из этого правила важнее признаков 'нейтральности' сообщения.\n"
        )
    system_prompt += (
        "Дополнительно относи к спаму стандартные категории: "
        "крипта/инвестиции/сигналы, казино/ставки, промокоды/скидки, "
        "реферальные ссылки, набор в команды, 'пиши в лс', предложения заработка, "
        "продажа услуг, накрутка, фишинг.\n"
    )
    if check_name and sender_display.strip():
        system_prompt += (
            "Также учитывай имя отправителя: если в имени или username "
            "явно присутствует реклама/продвижение/услуги (например, "
            "'Имя | Реклама', 'Продвижение в TG', '@promo_user') — "
            "это спам, даже если текст комментария осмысленный.\n"
            "В поле 'source' укажи 'text' если основная причина в тексте, "
            "или 'name' если в имени отправителя.\n"
            'Верни СТРОГО JSON: {"spam": true|false, "reason": "...", "source": "text|name"}.\n'
        )
    else:
        system_prompt += (
            "Верни СТРОГО JSON без пояснений вокруг: "
            '{"spam": true|false, "reason": "коротко почему"}.\n'
        )
    # Milder hedge: only bail on truly ambiguous cases, not on everything.
    # With a custom prompt provided, the admin's intent already disambiguates.
    system_prompt += "Если сообщение не подпадает ни под одно из правил выше — spam=false."

    user_parts = []
    if check_name and sender_display.strip():
        user_parts.append(f"Имя отправителя: {sender_display.strip()}")
    user_parts.append(f"Сообщение:\n{text}")
    user_content = "\n\n".join(user_parts)

    def _is_temperature_error(exc: Exception) -> bool:
        msg = str(exc or "").lower()
        return "temperature" in msg and (
            "unsupported" in msg or "does not support" in msg or "only the default" in msg
        )

    model_name = str(model or "gpt-5-mini")
    # GPT-5 series rejects temperature != 1; skip the parameter for those models
    # up-front to save an API round-trip. For older models we still send temperature=0
    # so the classification stays deterministic.
    model_lower = model_name.lower()
    include_temperature = not (model_lower.startswith("gpt-5") or model_lower.startswith("o1") or model_lower.startswith("o3"))

    def _build_kwargs(with_temperature: bool) -> dict:
        kw: dict[str, Any] = {
            "model": model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            "max_completion_tokens": 160,
        }
        if with_temperature:
            kw["temperature"] = 0
        return kw

    client = openai.AsyncOpenAI(api_key=api_key)
    try:
        completion = await client.chat.completions.create(**_build_kwargs(include_temperature))
    except Exception as exc:
        if include_temperature and _is_temperature_error(exc):
            logger.info("antispam: model %s rejects temperature — retrying without it", model_name)
            try:
                completion = await client.chat.completions.create(**_build_kwargs(False))
            except Exception as exc2:
                logger.warning("antispam: OpenAI request failed after temperature retry: %s", exc2)
                return False, f"openai_error:{type(exc2).__name__}", ""
        else:
            logger.warning("antispam: OpenAI request failed: %s", exc)
            return False, f"openai_error:{type(exc).__name__}", ""

    content = ""
    try:
        content = (completion.choices[0].message.content or "").strip()
    except Exception:
        content = ""
    if not content:
        return False, "empty_openai_response", ""

    raw = _strip_code_fences(content)
    try:
        payload = json.loads(raw)
    except Exception:
        return False, "invalid_json", ""

    if not isinstance(payload, dict):
        return False, "invalid_json", ""

    spam = bool(payload.get("spam"))
    reason = str(payload.get("reason") or "").strip()
    source = str(payload.get("source") or "").strip().lower()
    if source not in ("text", "name"):
        source = "text"
    return spam, (reason or "ai"), source


async def _try_delete_with_client(client, chat_id: int, msg_id: int) -> bool:
    """Delete a single message via a Telethon client and verify it actually went through.

    Telethon returns AffectedMessages with pts_count == 0 when the account has no
    rights to delete (or the message doesn't exist) without raising an exception.
    We must check pts_count, otherwise we'd report success on a silent no-op.
    """
    try:
        result = await client.delete_messages(chat_id, [msg_id])
    except Exception as exc:
        logger.debug("antispam: delete_messages raised for chat=%s msg=%s: %s", chat_id, msg_id, exc)
        return False

    # result is typically a list[AffectedMessages] (one per DC/peer). Any entry
    # with pts_count > 0 means the deletion applied to at least one message.
    try:
        if isinstance(result, (list, tuple)):
            for item in result:
                if int(getattr(item, "pts_count", 0) or 0) > 0:
                    return True
            return False
        # Single AffectedMessages returned
        return int(getattr(result, "pts_count", 0) or 0) > 0
    except Exception:
        # Unknown shape — treat as success to avoid regressions on future
        # Telethon versions that may change the return type.
        return True


async def _delete_message_any(
    chat_id: int,
    msg_id: int,
    *,
    active_clients: dict,
    allowed_sessions: list[str] | None = None,
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

    allowed_set = set(allowed_sessions) if allowed_sessions else None

    if cached_session and isinstance(active_clients, dict) and cached_session in active_clients:
        if allowed_set is None or cached_session in allowed_set:
            wrapper = active_clients.get(cached_session)
            if wrapper is not None:
                ok = await _attempt(wrapper)
                if ok:
                    return True, str(cached_session)
        _delete_success_cache.pop(int(chat_id), None)

    wrappers = list(active_clients.values()) if isinstance(active_clients, dict) else []
    if allowed_set:
        wrappers = [
            w for w in wrappers
            if str(getattr(w, "session_name", "") or "").strip() in allowed_set
        ]
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
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
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
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else None
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


async def _classify_spam(
    text: str,
    *,
    sender_name: str,
    sender_username: str,
    rule: dict[str, Any],
    current_settings: dict,
) -> tuple[bool, str, str, str]:
    """Classify a message as spam using keywords and optional AI.

    Returns:
        (is_spam, detection_method, matched_keyword, ai_reason)
        detection_method ∈ {"keyword", "name_keyword", "ai", "ai_name"}
    """
    # 1. Text keywords.
    matched = _keyword_match(text, rule.get("keywords") or [])
    if matched:
        return True, "keyword", matched, ""

    # 2. Name keywords (sender display name + username).
    name_haystack = " ".join([sender_name or "", sender_username or ""]).strip()
    name_matched = _keyword_match(name_haystack, rule.get("name_keywords") or [])
    if name_matched:
        return True, "name_keyword", name_matched, ""

    # 3. AI verification.
    if int(rule.get("ai_enabled") or 0) == 1:
        api_key = _openai_api_key(current_settings)
        check_name = int(rule.get("ai_check_name") or 0) == 1
        sender_display = ""
        if check_name:
            parts = []
            if sender_name:
                parts.append(sender_name)
            if sender_username:
                parts.append(f"@{sender_username}")
            sender_display = " ".join(parts).strip()

        is_spam, reason, source = await _ai_check_spam(
            text,
            ai_prompt=str(rule.get("ai_prompt") or ""),
            model=str(rule.get("ai_model") or "gpt-5-mini"),
            api_key=api_key,
            sender_display=sender_display,
            check_name=check_name,
        )
        if is_spam:
            method = "ai_name" if (check_name and source == "name") else "ai"
            return True, method, "", reason

    return False, "", "", ""


async def _handle_detected_spam(
    *,
    chat_id_int: int,
    msg_id_int: int,
    text: str,
    sender_id_int: int,
    sender_name: str,
    sender_username: str,
    detection_method: str,
    matched_keyword: str,
    ai_reason: str,
    rule: dict[str, Any],
    antispam_target: dict[str, Any] | None,
    active_clients: dict,
    current_settings: dict,
    spam_blocked_msgs: set | None,
    spam_blocked_msgs_order: deque | None,
    spam_blocked_msgs_max: int,
    chat_title: str = "",
    chat_username: str = "",
) -> dict[str, Any]:
    """Delete + ban + log a detected spam message.

    Returns a result dict with deleted/banned booleans and operator info.
    """
    chat_id_str = str(chat_id_int)

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

    # Delete: prefer bot_token → Bot API; fall back to user-accounts.
    # Bot API has a 48-hour limit on deleting incoming messages ("message can't be
    # deleted" on older posts), so when the bot fails we retry via a user-client —
    # admin user-accounts aren't bound by that limit.
    deleted = False
    operator_session_name: str | None = None
    bot_token = str((antispam_target or {}).get("bot_token") or "").strip()
    allowed_sessions = (antispam_target or {}).get("assigned_accounts") or None

    if bot_token:
        deleted = await _delete_message_via_bot(bot_token, chat_id_int, msg_id_int)
        if deleted:
            operator_session_name = "bot"

    if not deleted:
        client_deleted, client_op = await _delete_message_any(
            chat_id_int,
            msg_id_int,
            active_clients=active_clients,
            allowed_sessions=allowed_sessions,
        )
        if client_deleted:
            deleted = True
            operator_session_name = client_op
            if bot_token:
                logger.info(
                    "antispam: bot deleteMessage failed for chat=%s msg=%s — fallback succeeded via user client %s",
                    chat_id_int, msg_id_int, client_op,
                )

    action = "deleted" if deleted else "failed_to_delete"

    log_entry: dict[str, Any] = {
        "chat_id": chat_id_str,
        "msg_id": msg_id_int,
        "sender_id": sender_id_int or None,
        "sender_name": sender_name or None,
        "sender_username": sender_username or None,
        "message_text": text or None,
        "detection_method": detection_method or None,
        "matched_keyword": matched_keyword if detection_method in ("keyword", "name_keyword") else None,
        "ai_reason": ai_reason if detection_method in ("ai", "ai_name") else None,
        "action": action,
        "created_at": _now_iso(),
    }
    _insert_spam_log(log_entry)

    # Ban spammer if enabled and deletion succeeded. Bot API first, user-client
    # as fallback (same reasoning as deletion: bot may lack an up-to-date
    # admin role or hit some restriction, user-admin usually can).
    banned = False
    ban_enabled = bool((antispam_target or {}).get("ban_spammers"))
    if deleted and ban_enabled and sender_id_int:
        if bot_token:
            banned = await _ban_user_via_bot(bot_token, chat_id_int, sender_id_int)
        if not banned:
            banned = await _ban_user_via_client(
                chat_id_int, sender_id_int,
                active_clients=active_clients, allowed_sessions=allowed_sessions,
            )
            if banned and bot_token:
                logger.info(
                    "antispam: bot banChatMember failed for chat=%s user=%s — fallback succeeded via user client",
                    chat_id_int, sender_id_int,
                )
        if banned:
            reason_for_ban = matched_keyword or ai_reason or detection_method
            _insert_spam_ban({
                "chat_id": chat_id_str,
                "user_id": sender_id_int,
                "username": sender_username or None,
                "display_name": sender_name or None,
                "reason": reason_for_ban,
                "detection_method": detection_method,
            })
            logger.info("antispam: banned user=%s in chat=%s", sender_id_int, chat_id_str)

    # Mirror into generic action logs so it appears in /stats.
    target = antispam_target
    try:
        from services.db_queries import log_action_to_db

        if target is None:
            for t in get_project_targets(current_settings):
                if str(t.get("linked_chat_id") or "").strip() == chat_id_str:
                    target = t
                    break

        log_action_to_db(
            {
                "type": "spam_deleted" if deleted else "spam_failed",
                "post_id": msg_id_int,
                "msg_id": msg_id_int,
                "comment": (
                    ("" if deleted else "НЕ УДАЛЕНО · ")
                    + f"[{detection_method}] "
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
        pass

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
            chat_id_str, msg_id_int, detection_method,
        )
    else:
        logger.warning(
            "⚠️ [antispam] detected but failed to delete chat=%s msg=%s method=%s",
            chat_id_str, msg_id_int, detection_method,
        )

    return {
        "deleted": deleted,
        "banned": banned,
        "operator_session_name": operator_session_name,
        "log_entry": log_entry,
    }


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

    # Extract sender info upfront so we can also classify by name.
    sender_name = ""
    sender_username = ""
    try:
        sender = await event.get_sender()
        sender_username = str(getattr(sender, "username", None) or "").strip()
        fn = str(getattr(sender, "first_name", None) or "").strip()
        ln = str(getattr(sender, "last_name", None) or "").strip()
        sender_name = (f"{fn} {ln}").strip()
    except Exception:
        pass
    if not sender_name and sender_id_int:
        sender_name = str(sender_id_int)

    text = _safe_text(msg)
    is_spam, detection_method, matched_keyword, ai_reason = await _classify_spam(
        text,
        sender_name=sender_name,
        sender_username=sender_username,
        rule=rule,
        current_settings=current_settings,
    )
    if not is_spam:
        return False

    # Resolve antispam target to get bot_token / assigned_accounts / ban_spammers.
    antispam_target = None
    try:
        for at in (current_settings.get("antispam_targets") or []):
            if str(at.get("chat_id") or "").strip() == chat_id_str:
                antispam_target = at
                break
    except Exception:
        pass

    chat_title = ""
    chat_username = ""
    try:
        chat = await event.get_chat()
        chat_title = str(getattr(chat, "title", None) or "").strip()
        chat_username = str(getattr(chat, "username", None) or "").strip()
    except Exception:
        pass

    await _handle_detected_spam(
        chat_id_int=chat_id_int,
        msg_id_int=msg_id_int,
        text=text,
        sender_id_int=sender_id_int,
        sender_name=sender_name,
        sender_username=sender_username,
        detection_method=detection_method,
        matched_keyword=matched_keyword,
        ai_reason=ai_reason,
        rule=rule,
        antispam_target=antispam_target,
        active_clients=active_clients,
        current_settings=current_settings,
        spam_blocked_msgs=spam_blocked_msgs,
        spam_blocked_msgs_order=spam_blocked_msgs_order,
        spam_blocked_msgs_max=spam_blocked_msgs_max,
        chat_title=chat_title,
        chat_username=chat_username,
    )

    return True


# ---------------------------------------------------------------------------
# Manual scan: re-check existing post comments
# ---------------------------------------------------------------------------


def _parse_telegram_post_url(url: str) -> tuple[str, int] | None:
    """Parse a Telegram message link.

    Returns (chat_ref, msg_id) where chat_ref is either:
      - a "@username" / "username" string for public channels, or
      - a "-100..." string for private channels (/c/ links).

    Supported formats:
      - https://t.me/CHANNEL/123
      - https://t.me/CHANNEL/123/456 (forum or comment link → uses second id)
      - https://t.me/c/1234567890/123
      - https://t.me/c/1234567890/123/456
      - t.me/... without scheme
    """
    import re
    s = str(url or "").strip()
    if not s:
        return None
    s = re.sub(r"^https?://", "", s, flags=re.IGNORECASE)
    s = s.lstrip("/")
    if s.lower().startswith("t.me/"):
        s = s[5:]
    elif s.lower().startswith("telegram.me/"):
        s = s[12:]
    parts = [p for p in s.split("/") if p]
    if len(parts) < 2:
        return None

    if parts[0] == "c":
        # Private: c/<bare>/<id>[/<sub_id>]
        if len(parts) < 3:
            return None
        try:
            bare = int(parts[1])
        except ValueError:
            return None
        chat_ref = f"-100{bare}"
        # Use the deepest message id we have.
        try:
            msg_id = int(parts[3]) if len(parts) >= 4 else int(parts[2])
        except ValueError:
            return None
        return chat_ref, msg_id

    # Public: <username>/<id>[/<sub_id>]
    username = parts[0].lstrip("@")
    try:
        msg_id = int(parts[2]) if len(parts) >= 3 else int(parts[1])
    except ValueError:
        return None
    return username, msg_id


async def scan_post_for_spam(
    *,
    post_url: str,
    antispam_target: dict[str, Any],
    client,
    active_clients: dict | None = None,
    current_settings: dict,
    spam_blocked_msgs: set | None = None,
    spam_blocked_msgs_order: deque | None = None,
    spam_blocked_msgs_max: int = 10000,
    limit: int = 200,
) -> dict[str, Any]:
    """Manually scan all comments under a Telegram post for spam.

    Args:
        client: A connected and authorized Telethon TelegramClient used to
            read the channel and discussion messages. The caller is responsible
            for connecting/disconnecting it.
        active_clients: Optional dict of active client wrappers. Used only by
            _handle_detected_spam → _delete_message_any / _ban_user_via_client
            when the antispam target uses user-account moderation. Pass {} when
            the target uses bot_token-based moderation (or runs from admin_web).

    Resolves the post URL → channel → discussion thread, iterates the
    last `limit` comments, and runs the same spam classifier+handler.

    Returns: {ok, error, checked, spam, deleted, banned}
    """
    parsed = _parse_telegram_post_url(post_url)
    if parsed is None:
        return {"ok": False, "error": "invalid_url", "checked": 0, "spam": 0, "deleted": 0, "banned": 0}
    chat_ref, post_id = parsed

    target_chat_id = str(antispam_target.get("chat_id") or "").strip()
    if not target_chat_id:
        return {"ok": False, "error": "no_target_chat_id", "checked": 0, "spam": 0, "deleted": 0, "banned": 0}

    rule = _load_spam_rule(target_chat_id)
    if not rule:
        return {"ok": False, "error": "no_spam_rule", "checked": 0, "spam": 0, "deleted": 0, "banned": 0}

    if client is None:
        return {"ok": False, "error": "no_client", "checked": 0, "spam": 0, "deleted": 0, "banned": 0}

    if active_clients is None:
        active_clients = {}

    # Resolve the source channel entity.
    try:
        if isinstance(chat_ref, str) and chat_ref.startswith("-100"):
            entity = await client.get_input_entity(int(chat_ref))
        else:
            entity = await client.get_entity(chat_ref)
    except Exception as exc:
        logger.warning("antispam scan: failed to resolve %s: %s", chat_ref, exc)
        return {"ok": False, "error": f"resolve_failed:{type(exc).__name__}", "checked": 0, "spam": 0, "deleted": 0, "banned": 0}

    # Resolve to discussion thread (linked group + thread root msg id).
    discussion_chat_id = None
    discussion_root_id = None
    try:
        from telethon.tl.functions.messages import GetDiscussionMessageRequest
        discussion_res = await client(GetDiscussionMessageRequest(peer=entity, msg_id=post_id))
        if discussion_res and discussion_res.messages:
            root_msg = discussion_res.messages[0]
            try:
                discussion_chat_id = int(getattr(root_msg, "chat_id", None) or 0) or None
            except Exception:
                discussion_chat_id = None
            if not discussion_chat_id:
                peer = getattr(root_msg, "peer_id", None)
                channel_id = getattr(peer, "channel_id", None)
                if channel_id:
                    discussion_chat_id = int(f"-100{channel_id}")
            discussion_root_id = int(getattr(root_msg, "id", None) or post_id)
    except Exception as exc:
        logger.warning("antispam scan: GetDiscussionMessageRequest failed: %s", exc)

    if not discussion_chat_id or not discussion_root_id:
        return {"ok": False, "error": "no_discussion_thread", "checked": 0, "spam": 0, "deleted": 0, "banned": 0}

    # Make sure the discussion chat matches the antispam target chat_id.
    if str(discussion_chat_id) != target_chat_id:
        logger.warning(
            "antispam scan: discussion chat %s != target %s",
            discussion_chat_id, target_chat_id,
        )

    # Iterate comments in the thread.
    try:
        discussion_entity = await client.get_input_entity(discussion_chat_id)
    except Exception as exc:
        logger.warning("antispam scan: failed to get discussion entity: %s", exc)
        return {"ok": False, "error": "discussion_entity_failed", "checked": 0, "spam": 0, "deleted": 0, "banned": 0}

    try:
        our_ids = get_all_our_user_ids(active_clients=active_clients, current_settings=current_settings)
    except Exception:
        our_ids = set()

    checked = 0
    spam_count = 0
    deleted_count = 0
    failed_count = 0
    banned_count = 0

    try:
        async for message in client.iter_messages(
            discussion_entity, reply_to=discussion_root_id, limit=limit,
        ):
            if message is None:
                continue
            try:
                msg_id_int = int(getattr(message, "id", 0) or 0)
            except Exception:
                continue
            if not msg_id_int:
                continue

            # Skip the root message itself.
            if msg_id_int == discussion_root_id:
                continue

            sender_id_raw = getattr(message, "sender_id", None)
            try:
                sender_id_int = int(sender_id_raw) if sender_id_raw is not None else 0
            except Exception:
                sender_id_int = 0

            if sender_id_int and sender_id_int in our_ids:
                continue

            # Extract sender info.
            sender_name = ""
            sender_username = ""
            try:
                sender = await message.get_sender()
                sender_username = str(getattr(sender, "username", None) or "").strip()
                fn = str(getattr(sender, "first_name", None) or "").strip()
                ln = str(getattr(sender, "last_name", None) or "").strip()
                sender_name = (f"{fn} {ln}").strip()
            except Exception:
                pass
            if not sender_name and sender_id_int:
                sender_name = str(sender_id_int)

            text = _safe_text(message)
            checked += 1

            is_spam, detection_method, matched_keyword, ai_reason = await _classify_spam(
                text,
                sender_name=sender_name,
                sender_username=sender_username,
                rule=rule,
                current_settings=current_settings,
            )
            if not is_spam:
                continue

            spam_count += 1
            result = await _handle_detected_spam(
                chat_id_int=discussion_chat_id,
                msg_id_int=msg_id_int,
                text=text,
                sender_id_int=sender_id_int,
                sender_name=sender_name,
                sender_username=sender_username,
                detection_method=detection_method,
                matched_keyword=matched_keyword,
                ai_reason=ai_reason,
                rule=rule,
                antispam_target=antispam_target,
                active_clients=active_clients,
                current_settings=current_settings,
                spam_blocked_msgs=spam_blocked_msgs,
                spam_blocked_msgs_order=spam_blocked_msgs_order,
                spam_blocked_msgs_max=spam_blocked_msgs_max,
            )
            if result.get("deleted"):
                deleted_count += 1
            else:
                failed_count += 1
            if result.get("banned"):
                banned_count += 1
    except Exception as exc:
        logger.warning("antispam scan: iteration failed: %s", exc)
        return {
            "ok": False,
            "error": f"iter_failed:{type(exc).__name__}",
            "checked": checked,
            "spam": spam_count,
            "deleted": deleted_count,
            "failed_to_delete": failed_count,
            "banned": banned_count,
            "discussion_chat_id": discussion_chat_id,
            "via_bot": bool(str((antispam_target or {}).get("bot_token") or "").strip()),
        }

    return {
        "ok": True,
        "error": "",
        "checked": checked,
        "spam": spam_count,
        "deleted": deleted_count,
        "failed_to_delete": failed_count,
        "banned": banned_count,
        "discussion_chat_id": discussion_chat_id,
        "via_bot": bool(str((antispam_target or {}).get("bot_token") or "").strip()),
    }

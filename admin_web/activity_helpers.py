"""Helpers for enriching action-log rows for Dashboard / Stats UI.

Raw log rows from `logs` table carry numeric IDs and embedded metadata
(role/mood prefix, target username, destination chat, etc.). Templates
shouldn't untangle all of that — this module converts each row into a
presentation-ready dict with human labels, Telegram links, and a short
"result" phrase describing what actually happened.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote


# ---------------------------------------------------------------------------
# Log type → UI metadata
# ---------------------------------------------------------------------------

# label     — human-readable Russian name
# icon      — bootstrap-icons class
# color     — bootstrap contextual color (primary/success/…)
# task_verb — verb describing what the bot was doing (используется в «задача»)
LOG_TYPE_META: Dict[str, Dict[str, str]] = {
    "comment": {
        "label": "Комментарий",
        "icon": "bi-chat-left-text",
        "color": "primary",
        "task": "Комментирование поста",
    },
    "comment_reply": {
        "label": "Ответ",
        "icon": "bi-reply",
        "color": "info",
        "task": "Ответ в обсуждении",
    },
    "comment_failed": {
        "label": "Ошибка комментария",
        "icon": "bi-exclamation-triangle",
        "color": "danger",
        "task": "Комментирование поста",
    },
    "comment_skip": {
        "label": "Пропуск",
        "icon": "bi-dash-circle",
        "color": "secondary",
        "task": "Решение по посту",
    },
    "reaction": {
        "label": "Реакция",
        "icon": "bi-hand-thumbs-up",
        "color": "warning",
        "task": "Реакция на пост",
    },
    "monitoring": {
        "label": "Мониторинг",
        "icon": "bi-broadcast",
        "color": "success",
        "task": "Мониторинг постов",
    },
    "discussion": {
        "label": "Обсуждение",
        "icon": "bi-chat-dots",
        "color": "info",
        "task": "Ведение обсуждения",
    },
    "spam_deleted": {
        "label": "Антиспам",
        "icon": "bi-shield-check",
        "color": "success",
        "task": "Удаление спама",
    },
    "forbidden": {
        "label": "Запрещено",
        "icon": "bi-slash-circle",
        "color": "dark",
        "task": "Запрещённое действие",
    },
}


def log_type_meta(log_type: Optional[str]) -> Dict[str, str]:
    """Return UI metadata for a log type, with a safe fallback."""
    key = str(log_type or "").strip().lower()
    meta = LOG_TYPE_META.get(key)
    if meta:
        return meta
    return {
        "label": key.replace("_", " ").capitalize() or "—",
        "icon": "bi-activity",
        "color": "secondary",
        "task": "Действие",
    }


# ---------------------------------------------------------------------------
# Content parsing (role / mood / tag / payload)
# ---------------------------------------------------------------------------

_ROLE_MOOD_RE = re.compile(
    r"^\s*\[\s*Роль:\s*([^·\]]+?)\s*·\s*настроение:\s*([^\]]+?)\s*\]\s*",
    re.IGNORECASE,
)
# Same pattern but without surrounding brackets — comment_failed stores it bare.
_ROLE_MOOD_BARE_RE = re.compile(
    r"^\s*Роль:\s*([^·]+?)\s*·\s*настроение:\s*(\S[^·]*?)(?:\s*·\s*|\s*$)",
    re.IGNORECASE,
)
_TAG_RE = re.compile(r"^\s*\[\s*([^\]]+?)\s*\]\s*")


def parse_content(log_type: str, raw: Optional[str]) -> Dict[str, Any]:
    """Split raw `content` into structured pieces.

    Returns a dict with keys:
      * role      — optional role name ("Эмпатичный собеседник" / "Кастомная"…)
      * mood      — optional mood name
      * tag       — any extra tag in square brackets (ВМЕШАТЕЛЬСТВО / ОТВЕТ / SCENARIO…)
      * body      — the rest of the content (cleaned of prefixes)
      * summary   — short one-line description for list view
    """
    text = (raw or "").strip()
    result: Dict[str, Any] = {"role": None, "mood": None, "tag": None, "body": "", "summary": ""}

    lt = (log_type or "").strip().lower()

    # Reactions store emojis in content
    if lt == "reaction":
        emojis = [e for e in text.split() if e]
        result["body"] = " ".join(emojis)
        result["summary"] = "Поставил(и) реакцию: " + (result["body"] or "—")
        return result

    # Monitoring stores free-form "Found post, notified <id>"
    if lt == "monitoring":
        result["body"] = text
        result["summary"] = text or "Обнаружен релевантный пост"
        return result

    # Spam deletion stores the deleted message body
    if lt == "spam_deleted":
        result["body"] = text
        result["summary"] = ("Удалил спам: " + text) if text else "Удалил спам-сообщение"
        return result

    # Comment skip stores a short reason
    if lt == "comment_skip":
        result["body"] = text
        result["summary"] = ("Пропустил: " + text) if text else "Пропустил пост"
        return result

    # Failed comment — stores "<role info> · FAIL(<details>)" (no brackets)
    if lt == "comment_failed":
        m = _ROLE_MOOD_RE.match(text) or _ROLE_MOOD_BARE_RE.match(text)
        if m:
            result["role"] = m.group(1).strip()
            result["mood"] = m.group(2).strip()
            text = text[m.end():]
        result["body"] = text.strip()
        if result["body"]:
            result["summary"] = "Не удалось сгенерировать комментарий: " + result["body"]
        else:
            result["summary"] = "Не удалось сгенерировать комментарий"
        return result

    # comment / comment_reply — [Роль: ... · настроение: ...] [ТЭГ?] <текст>
    m = _ROLE_MOOD_RE.match(text)
    if m:
        result["role"] = m.group(1).strip()
        result["mood"] = m.group(2).strip()
        text = text[m.end():]

    t = _TAG_RE.match(text)
    if t:
        result["tag"] = t.group(1).strip()
        text = text[t.end():]

    result["body"] = text.strip()
    if lt == "comment_reply":
        head = "Ответил" if not result["tag"] else f"{result['tag'].capitalize()}"
        result["summary"] = f"{head}: {result['body']}" if result["body"] else head
    else:
        result["summary"] = f"Написал: {result['body']}" if result["body"] else "Отправил комментарий"
    return result


# ---------------------------------------------------------------------------
# Target lookup + links
# ---------------------------------------------------------------------------


def _str_id(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def build_target_index(settings: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Map chat_id (as string) → { chat_name, href, username } for quick lookup.

    Covers comment targets, reaction targets, monitor targets, discussion
    targets. Both main chat_id and linked_chat_id are indexed so discussion
    group IDs resolve back to the parent target.
    """
    index: Dict[str, Dict[str, Any]] = {}

    def _add(chat_id: Any, name: Any, href: str, username: Any = None) -> None:
        key = _str_id(chat_id)
        if not key:
            return
        entry = index.setdefault(key, {})
        if name and not entry.get("chat_name"):
            entry["chat_name"] = str(name)
        if username and not entry.get("chat_username"):
            entry["chat_username"] = str(username).lstrip("@")
        if href and not entry.get("href"):
            entry["href"] = href

    for t in settings.get("targets") or []:
        main_id = t.get("chat_id")
        if not main_id:
            continue
        href = f"/targets/{quote(str(main_id))}"
        name = t.get("chat_name")
        uname = t.get("chat_username")
        _add(main_id, name, href, uname)
        _add(t.get("linked_chat_id"), name, href, t.get("linked_chat_username") or uname)

    for t in settings.get("reaction_targets") or []:
        main_id = t.get("chat_id")
        if not main_id:
            continue
        href = f"/reaction-targets/{quote(str(main_id))}"
        _add(main_id, t.get("chat_name"), href, t.get("chat_username"))
        _add(t.get("linked_chat_id"), t.get("chat_name"), href, t.get("linked_chat_username"))

    for t in settings.get("monitor_targets") or []:
        main_id = t.get("chat_id")
        if not main_id:
            continue
        href = f"/monitor-targets/{quote(str(main_id))}"
        _add(main_id, t.get("chat_name"), href, t.get("chat_username"))

    for t in settings.get("discussion_targets") or []:
        main_id = t.get("chat_id")
        if not main_id:
            continue
        href = f"/discussions/{quote(str(main_id))}"
        _add(main_id, t.get("chat_name"), href, t.get("chat_username"))
        _add(t.get("linked_chat_id"), t.get("chat_name"), href, t.get("linked_chat_username"))

    return index


def _telegram_post_link(channel_username: Any, chat_id: Any, post_id: Any) -> str:
    """Build a t.me link to the original post using username (preferred) or c/<id>."""
    pid = _str_id(post_id)
    if not pid:
        return ""
    uname = str(channel_username or "").lstrip("@").strip()
    if uname:
        return f"https://t.me/{uname}/{pid}"
    cid = _str_id(chat_id).replace("-100", "")
    if cid:
        return f"https://t.me/c/{cid}/{pid}"
    return ""


def _telegram_message_link(destination_chat_id: Any, msg_id: Any) -> str:
    """Build a t.me/c link to a specific message in a discussion group."""
    mid = _str_id(msg_id)
    cid = _str_id(destination_chat_id).replace("-100", "")
    if not mid or not cid:
        return ""
    return f"https://t.me/c/{cid}/{mid}"


# ---------------------------------------------------------------------------
# Row enrichment
# ---------------------------------------------------------------------------


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    """Access a sqlite3.Row or plain dict field, returning default when missing."""
    try:
        value = row[key]
    except (KeyError, IndexError):
        return default
    return default if value is None else value


def enrich_log_row(row: Any, *, target_index: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Convert a raw log row into a UI-ready dict."""
    log_type = _row_get(row, "log_type", "") or ""
    meta = log_type_meta(log_type)

    destination_chat_id = _row_get(row, "destination_chat_id", "")
    source_channel_id = _row_get(row, "source_channel_id", "")
    channel_username = _row_get(row, "channel_username", "") or ""
    channel_name = _row_get(row, "channel_name", "") or ""
    post_id = _row_get(row, "post_id", None)
    msg_id = _row_get(row, "msg_id", None)

    # Resolve target (prefer source channel for comments/reactions; fall back to destination)
    target_entry: Dict[str, Any] = {}
    for key in (source_channel_id, destination_chat_id):
        key_str = _str_id(key)
        if not key_str:
            continue
        entry = target_index.get(key_str)
        if entry:
            target_entry = entry
            break

    resolved_name = channel_name or target_entry.get("chat_name") or ""
    resolved_username = channel_username or target_entry.get("chat_username") or ""
    target_href = target_entry.get("href") or ""

    # Links
    post_link = _telegram_post_link(resolved_username, source_channel_id or destination_chat_id, post_id)
    message_link = _telegram_message_link(destination_chat_id, msg_id)

    # Content
    parsed = parse_content(log_type, _row_get(row, "content", ""))

    # Display label for the channel
    if resolved_name:
        channel_display = resolved_name
    elif resolved_username:
        channel_display = f"@{resolved_username}"
    else:
        channel_display = f"chat {destination_chat_id}" if destination_chat_id else "—"

    return {
        "id": _row_get(row, "id", None),
        "log_type": log_type,
        "timestamp": _row_get(row, "timestamp", ""),
        "type_label": meta["label"],
        "type_icon": meta["icon"],
        "type_color": meta["color"],
        "task_label": meta["task"],
        "account_session_name": _row_get(row, "account_session_name", "") or "",
        "account_username": _row_get(row, "account_username", "") or "",
        "account_first_name": _row_get(row, "account_first_name", "") or "",
        "post_id": post_id,
        "msg_id": msg_id,
        "destination_chat_id": destination_chat_id,
        "channel_display": channel_display,
        "channel_name": resolved_name,
        "channel_username": resolved_username,
        "target_href": target_href,
        "post_link": post_link,
        "message_link": message_link,
        "role": parsed["role"],
        "mood": parsed["mood"],
        "tag": parsed["tag"],
        "body": parsed["body"],
        "summary": parsed["summary"],
        "raw_content": _row_get(row, "content", "") or "",
    }


def enrich_log_rows(rows: Iterable[Any], settings: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Enrich a batch of log rows against the given settings snapshot."""
    index = build_target_index(settings or {})
    return [enrich_log_row(row, target_index=index) for row in rows]


# ---------------------------------------------------------------------------
# Aggregated counts (for stats dashboard summary)
# ---------------------------------------------------------------------------


def summarize_log_counts(rows: Iterable[Any]) -> Dict[str, int]:
    """Count rows grouped by log_type, plus a ``total`` key.

    Useful for rendering summary cards on the stats page.
    """
    counts: Dict[str, int] = {"total": 0}
    for row in rows:
        lt = _row_get(row, "log_type", "") or "other"
        counts[lt] = counts.get(lt, 0) + 1
        counts["total"] += 1
    return counts

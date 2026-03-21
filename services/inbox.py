"""Inbox message logging, reaction tracking, and outgoing message helpers.

Extracted from commentator.py — DB-backed inbox operations for DMs, quotes,
and reaction events.
"""

import collections
from datetime import datetime, timezone

from telethon.tl import types as tl_types

from db.connection import get_connection as _get_db_connection


def _db_connect():
    return _get_db_connection()


# ---------------------------------------------------------------------------
# Inbox message logging
# ---------------------------------------------------------------------------

def log_inbox_message_to_db(
    *,
    kind: str,
    direction: str,
    status: str,
    session_name: str,
    chat_id: str,
    msg_id: int | None = None,
    reply_to_msg_id: int | None = None,
    sender_id: int | None = None,
    sender_username: str | None = None,
    sender_name: str | None = None,
    chat_title: str | None = None,
    chat_username: str | None = None,
    text: str | None = None,
    replied_to_text: str | None = None,
    reactions_summary: str | None = None,
    reactions_updated_at: str | None = None,
    is_read: int = 0,
    error: str | None = None,
) -> int | None:
    kind = (kind or "").strip() or "dm"
    direction = (direction or "").strip() or "in"
    status = (status or "").strip() or "received"
    session_name = (session_name or "").strip()
    chat_id = (chat_id or "").strip()
    if not session_name or not chat_id:
        return None

    created_at = datetime.now(timezone.utc).isoformat()
    try:
        with _db_connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO inbox_messages (
                    kind, direction, status, created_at,
                    session_name, chat_id, msg_id, reply_to_msg_id,
                    sender_id, sender_username, sender_name,
                    chat_title, chat_username,
                    text, replied_to_text,
                    reactions_summary, reactions_updated_at,
                    is_read, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                (
                    kind,
                    direction,
                    status,
                    created_at,
                    session_name,
                    chat_id,
                    msg_id,
                    reply_to_msg_id,
                    sender_id,
                    (sender_username or "").strip() or None,
                    (sender_name or "").strip() or None,
                    (chat_title or "").strip() or None,
                    (chat_username or "").strip() or None,
                    (text or "").strip() or None,
                    (replied_to_text or "").strip() or None,
                    (reactions_summary or "").strip() or None,
                    (reactions_updated_at or "").strip() or None,
                    int(bool(is_read)),
                    (error or "").strip() or None,
                ),
            )
            conn.commit()
            return cursor.lastrowid or None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Message text preview
# ---------------------------------------------------------------------------

def _message_text_preview(message) -> str:
    text = getattr(message, "text", None) or getattr(message, "message", None) or ""
    if text:
        return text
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


# ---------------------------------------------------------------------------
# Peer / reaction helpers
# ---------------------------------------------------------------------------

def _peer_chat_id(peer) -> str | None:
    if isinstance(peer, tl_types.PeerChannel):
        return f"-100{peer.channel_id}"
    if isinstance(peer, tl_types.PeerChat):
        return f"-{peer.chat_id}"
    if isinstance(peer, tl_types.PeerUser):
        return str(peer.user_id)
    return None


def _reaction_label(reaction_obj) -> str:
    if not reaction_obj:
        return ""
    emoticon = getattr(reaction_obj, "emoticon", None)
    if emoticon:
        return str(emoticon)
    if hasattr(reaction_obj, "document_id"):
        return "кастом"
    if reaction_obj.__class__.__name__ == "ReactionPaid":
        return "paid"
    return ""


def _reaction_summary_from_update(update) -> str:
    grouped: collections.OrderedDict[str, int] = collections.OrderedDict()

    def add(label: str, count: int) -> None:
        if not label:
            return
        grouped[label] = grouped.get(label, 0) + max(int(count or 0), 0)

    if isinstance(update, tl_types.UpdateMessageReactions):
        for item in (getattr(getattr(update, "reactions", None), "results", None) or []):
            add(_reaction_label(getattr(item, "reaction", None)), int(getattr(item, "count", 0) or 0))
    elif isinstance(update, tl_types.UpdateBotMessageReactions):
        for item in (getattr(update, "reactions", None) or []):
            add(_reaction_label(getattr(item, "reaction", None)), int(getattr(item, "count", 0) or 0))
    elif isinstance(update, tl_types.UpdateBotMessageReaction):
        for reaction_obj in (getattr(update, "new_reactions", None) or []):
            add(_reaction_label(reaction_obj), 1)

    parts: list[str] = []
    for label, count in grouped.items():
        if count <= 0:
            continue
        parts.append(f"{label}×{count}" if count > 1 else label)
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Reaction event storage
# ---------------------------------------------------------------------------

def _store_message_reaction_event(
    *,
    session_name: str,
    chat_id: str,
    msg_id: int,
    kind: str,
    text: str | None,
    chat_title: str | None,
    chat_username: str | None,
    reactions_summary: str | None,
) -> None:
    if not session_name or not chat_id or not msg_id:
        return
    now = datetime.now(timezone.utc).isoformat()
    summary = (reactions_summary or "").strip()

    with _db_connect() as conn:
        conn.execute(
            """
            INSERT INTO inbox_messages (
                kind, direction, status, created_at,
                session_name, chat_id, msg_id,
                chat_title, chat_username, text,
                reactions_summary, reactions_updated_at,
                is_read
            )
            VALUES (?, 'out', 'sent', ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(session_name, chat_id, msg_id, direction) DO UPDATE SET
                kind=excluded.kind,
                status='sent',
                chat_title=COALESCE(excluded.chat_title, inbox_messages.chat_title),
                chat_username=COALESCE(excluded.chat_username, inbox_messages.chat_username),
                text=COALESCE(excluded.text, inbox_messages.text),
                reactions_summary=CASE
                    WHEN excluded.reactions_summary IS NULL OR excluded.reactions_summary = ''
                    THEN NULL
                    ELSE excluded.reactions_summary
                END,
                reactions_updated_at=CASE
                    WHEN excluded.reactions_summary IS NULL OR excluded.reactions_summary = ''
                    THEN NULL
                    ELSE excluded.reactions_updated_at
                END
            """,
            (
                kind,
                now,
                session_name,
                chat_id,
                int(msg_id),
                (chat_title or "").strip() or None,
                (chat_username or "").strip() or None,
                (text or "").strip() or None,
                summary or None,
                now if summary else None,
            ),
        )

        if summary:
            event_text = f"Реакция на сообщение бота: {summary}"
            conn.execute(
                """
                INSERT INTO inbox_messages (
                    kind, direction, status, created_at,
                    session_name, chat_id, msg_id, reply_to_msg_id,
                    chat_title, chat_username, text, replied_to_text,
                    reactions_summary, reactions_updated_at,
                    is_read
                )
                VALUES (?, 'in', 'reaction', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(session_name, chat_id, msg_id, direction) DO UPDATE SET
                    kind=excluded.kind,
                    status='reaction',
                    created_at=excluded.created_at,
                    reply_to_msg_id=excluded.reply_to_msg_id,
                    chat_title=COALESCE(excluded.chat_title, inbox_messages.chat_title),
                    chat_username=COALESCE(excluded.chat_username, inbox_messages.chat_username),
                    text=excluded.text,
                    replied_to_text=COALESCE(excluded.replied_to_text, inbox_messages.replied_to_text),
                    reactions_summary=excluded.reactions_summary,
                    reactions_updated_at=excluded.reactions_updated_at,
                    is_read=0,
                    error=NULL
                """,
                (
                    kind,
                    now,
                    session_name,
                    chat_id,
                    int(msg_id),
                    int(msg_id),
                    (chat_title or "").strip() or None,
                    (chat_username or "").strip() or None,
                    event_text,
                    (text or "").strip() or None,
                    summary,
                    now,
                ),
            )
        else:
            conn.execute(
                """
                DELETE FROM inbox_messages
                WHERE session_name=? AND chat_id=? AND msg_id=? AND direction='in' AND status='reaction'
                """,
                (session_name, chat_id, int(msg_id)),
            )

        conn.commit()


# ---------------------------------------------------------------------------
# Outgoing queue check
# ---------------------------------------------------------------------------

def _queued_outgoing_exists(
    *,
    kind: str,
    session_name: str,
    chat_id: str,
    text: str | None,
    reply_to_msg_id: int | None,
) -> bool:
    if not session_name or not chat_id:
        return False
    try:
        with _db_connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM inbox_messages
                WHERE kind=?
                  AND direction='out'
                  AND status='queued'
                  AND session_name=?
                  AND chat_id=?
                  AND COALESCE(text, '') = COALESCE(?, '')
                  AND COALESCE(reply_to_msg_id, -1) = COALESCE(?, -1)
                LIMIT 1
                """,
                (kind, session_name, str(chat_id), text or "", reply_to_msg_id),
            ).fetchone()
            return row is not None
    except Exception:
        return False

"""Dialog (DM) and quote routes."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List
from urllib.parse import quote

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse

from admin_web.helpers import (
    _load_settings,
    _load_accounts,
    _active_project_id,
    _filter_accounts_by_project,
    _project_id_for,
    _db_connect,
    _cleanup_inbox_for_removed_accounts,
    _flash,
    _redirect,
    _telegram_message_link,
    _safe_local_redirect_path,
)
from admin_web.templating import templates, _template_context

router = APIRouter()


# ---------------------------------------------------------------------------
# Dialogs (DM)
# ---------------------------------------------------------------------------


@router.get("/dialogs", response_class=HTMLResponse)
async def dialogs_page(request: Request, session_name: str = ""):
    settings, _ = _load_settings()
    try:
        _cleanup_inbox_for_removed_accounts(settings)
    except Exception:
        pass
    project_id = _active_project_id(settings)
    accounts, _ = _load_accounts()
    sessions = sorted(
        [str(a.get("session_name")) for a in _filter_accounts_by_project(accounts, project_id) if a.get("session_name")]
    )
    selected_session = (session_name or "").strip()
    if selected_session not in sessions:
        selected_session = ""
    active_sessions = [selected_session] if selected_session else sessions
    rows = []
    if active_sessions:
        placeholders = ", ".join(["?"] * len(active_sessions))
        params = tuple(active_sessions) + tuple(active_sessions)
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
            conn.execute(
                f"UPDATE inbox_messages SET is_read=1 WHERE kind='dm' AND direction='in' AND is_read=0 AND session_name IN ({placeholders})",
                tuple(active_sessions),
            )
            conn.commit()

    return_suffix = f"?session_name={quote(selected_session)}" if selected_session else ""
    return_to = f"/dialogs{return_suffix}"
    threads: List[Dict[str, Any]] = []
    for r in rows:
        title = r["chat_title"] or r["sender_name"] or r["chat_username"] or r["sender_username"] or r["chat_id"]
        threads.append(
            {
                "session_name": r["session_name"],
                "chat_id": r["chat_id"],
                "title": title,
                "last_text": r["text"] or "",
                "reactions_summary": r["reactions_summary"] or "",
                "last_at": r["created_at"],
                "unread": int(r["unread"] or 0),
                "url": f"/dialogs/{quote(str(r['session_name']))}/{quote(str(r['chat_id']))}",
                "delete_url": f"/dialogs/{quote(str(r['session_name']))}/{quote(str(r['chat_id']))}/delete",
            }
        )

    return templates.TemplateResponse(
        "dialogs.html",
        _template_context(
            request,
            threads=threads,
            sessions=sessions,
            selected_session=selected_session,
            return_to=return_to,
        ),
    )


@router.get("/dialogs/{session_name}/{chat_id}", response_class=HTMLResponse)
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

    back_url = f"/dialogs?session_name={quote(session_name)}"
    return templates.TemplateResponse(
        "dialog_thread.html",
        _template_context(
            request,
            session_name=session_name,
            chat_id=chat_id,
            title=title,
            messages=messages,
            back_url=back_url,
        ),
    )


@router.post("/dialogs/{session_name}/{chat_id}/send")
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


@router.post("/dialogs/{session_name}/{chat_id}/delete")
async def dialog_delete_thread(
    request: Request,
    session_name: str,
    chat_id: str,
    return_to: str = Form("/dialogs"),
):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    if not any(a.get("session_name") == session_name and _project_id_for(a) == project_id for a in accounts):
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    with _db_connect() as conn:
        conn.execute("DELETE FROM inbox_messages WHERE kind='dm' AND session_name=? AND chat_id=?", (session_name, chat_id))
        conn.execute(
            "DELETE FROM outbound_queue WHERE session_name=? AND chat_id=? AND reply_to_msg_id IS NULL",
            (session_name, chat_id),
        )
        conn.commit()

    _flash(request, "success", "Переписка удалена.")
    return _redirect(_safe_local_redirect_path(return_to, "/dialogs"))


# ---------------------------------------------------------------------------
# Quotes
# ---------------------------------------------------------------------------


@router.get("/quotes", response_class=HTMLResponse)
async def quotes_page(request: Request, session_name: str = ""):
    settings, _ = _load_settings()
    try:
        _cleanup_inbox_for_removed_accounts(settings)
    except Exception:
        pass
    project_id = _active_project_id(settings)
    accounts, _ = _load_accounts()
    sessions = sorted(
        [str(a.get("session_name")) for a in _filter_accounts_by_project(accounts, project_id) if a.get("session_name")]
    )
    selected_session = (session_name or "").strip()
    if selected_session not in sessions:
        selected_session = ""
    active_sessions = [selected_session] if selected_session else sessions
    rows = []
    if active_sessions:
        placeholders = ", ".join(["?"] * len(active_sessions))
        with _db_connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM inbox_messages
                WHERE kind='quote' AND session_name IN ({placeholders})
                ORDER BY id DESC
                LIMIT 300
                """,
                tuple(active_sessions),
            ).fetchall()
            conn.execute(
                f"UPDATE inbox_messages SET is_read=1 WHERE kind='quote' AND direction='in' AND is_read=0 AND session_name IN ({placeholders})",
                tuple(active_sessions),
            )
            conn.commit()

    suffix = f"?session_name={quote(selected_session)}" if selected_session else ""
    return_to = f"/quotes{suffix}"
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
                "url": f"/quotes/{r['id']}{suffix}",
                "delete_url": f"/quotes/{r['id']}/delete",
            }
        )

    return templates.TemplateResponse(
        "quotes.html",
        _template_context(
            request,
            items=items,
            sessions=sessions,
            selected_session=selected_session,
            return_to=return_to,
        ),
    )


@router.get("/quotes/{inbox_id}", response_class=HTMLResponse)
async def quote_detail_page(request: Request, inbox_id: int, session_name: str = ""):
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
    selected_session = (session_name or "").strip()
    back_url = "/quotes"
    if selected_session:
        back_url = f"/quotes?session_name={quote(selected_session)}"
    return templates.TemplateResponse("quote_detail.html", _template_context(request, item=item, back_url=back_url))


@router.post("/quotes/{inbox_id}/reply")
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


@router.post("/quotes/{inbox_id}/delete")
async def quote_delete(request: Request, inbox_id: int, return_to: str = Form("/quotes")):
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
        conn.execute("DELETE FROM inbox_messages WHERE id = ?", (inbox_id,))
        conn.commit()

    _flash(request, "success", "Запись удалена.")
    return _redirect(_safe_local_redirect_path(return_to, "/quotes"))

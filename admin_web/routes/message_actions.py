"""Routes for editing and deleting sent Telegram messages."""

from __future__ import annotations

from fastapi import APIRouter, Form, HTTPException, Request

from admin_web.helpers import (
    _active_project_id,
    _db_connect,
    _flash,
    _load_accounts,
    _load_settings,
    _project_id_for,
    _redirect,
    _safe_local_redirect_path,
)
from services.message_actions import delete_message, edit_message

router = APIRouter()


def _redirect_back(request: Request, return_to: str | None, default: str = "/"):
    referer = request.headers.get("referer")
    return _redirect(_safe_local_redirect_path(return_to or referer, default))


def _load_message_record(source: str, record_id: int):
    with _db_connect() as conn:
        if source == "logs":
            return conn.execute(
                """
                SELECT
                    id,
                    account_session_name AS session_name,
                    destination_chat_id AS chat_id,
                    msg_id,
                    content AS text
                FROM logs
                WHERE id = ?
                """,
                (record_id,),
            ).fetchone()
        if source == "inbox":
            return conn.execute(
                """
                SELECT id, session_name, chat_id, msg_id, text
                FROM inbox_messages
                WHERE id = ? AND direction = 'out'
                """,
                (record_id,),
            ).fetchone()
    raise HTTPException(status_code=400, detail="Неизвестный источник сообщения.")


def _ensure_project_account(session_name: str, *, settings: dict, accounts: list[dict]) -> None:
    project_id = _active_project_id(settings)
    if not any(a.get("session_name") == session_name and _project_id_for(a) == project_id for a in accounts):
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")


@router.post("/messages/edit")
async def message_edit(
    request: Request,
    source: str = Form(...),
    record_id: str = Form(...),
    new_text: str = Form(...),
    return_to: str | None = Form(None),
):
    redirect_response = _redirect_back(request, return_to)
    text = str(new_text or "").strip()
    if not text:
        _flash(request, "warning", "Сообщение пустое.")
        return redirect_response

    try:
        record_id_int = int(str(record_id).strip())
    except ValueError:
        _flash(request, "danger", "Некорректный record_id.")
        return redirect_response

    row = _load_message_record(source, record_id_int)
    if not row:
        _flash(request, "danger", "Сообщение не найдено.")
        return redirect_response

    session_name = str(row["session_name"] or "").strip()
    msg_id = row["msg_id"]
    if msg_id is None:
        _flash(request, "warning", "Для этой записи нет msg_id, редактирование невозможно.")
        return redirect_response

    settings, _ = _load_settings()
    accounts, _ = _load_accounts()
    _ensure_project_account(session_name, settings=settings, accounts=accounts)

    active_clients = getattr(request.app.state, "active_clients", {}) or {}
    try:
        ok = await edit_message(
            session_name,
            row["chat_id"],
            msg_id,
            text,
            active_clients=active_clients,
            current_settings=settings,
            source=source,
            record_id=record_id_int,
        )
    except RuntimeError as exc:
        _flash(request, "danger", str(exc))
        return redirect_response

    if ok:
        _flash(request, "success", "Сообщение отредактировано.")
    else:
        _flash(request, "danger", "Не удалось отредактировать сообщение.")
    return redirect_response


@router.post("/messages/delete")
async def message_delete(
    request: Request,
    source: str = Form(...),
    record_id: str = Form(...),
    return_to: str | None = Form(None),
):
    redirect_response = _redirect_back(request, return_to)
    try:
        record_id_int = int(str(record_id).strip())
    except ValueError:
        _flash(request, "danger", "Некорректный record_id.")
        return redirect_response

    row = _load_message_record(source, record_id_int)
    if not row:
        _flash(request, "danger", "Сообщение не найдено.")
        return redirect_response

    session_name = str(row["session_name"] or "").strip()
    msg_id = row["msg_id"]
    if msg_id is None:
        _flash(request, "warning", "Для этой записи нет msg_id, удаление невозможно.")
        return redirect_response

    settings, _ = _load_settings()
    accounts, _ = _load_accounts()
    _ensure_project_account(session_name, settings=settings, accounts=accounts)

    active_clients = getattr(request.app.state, "active_clients", {}) or {}
    try:
        ok = await delete_message(
            session_name,
            row["chat_id"],
            msg_id,
            active_clients=active_clients,
            current_settings=settings,
            source=source,
            record_id=record_id_int,
        )
    except RuntimeError as exc:
        _flash(request, "danger", str(exc))
        return redirect_response

    if ok:
        _flash(request, "success", "Сообщение удалено.")
    else:
        _flash(request, "danger", "Не удалось удалить сообщение.")
    return redirect_response

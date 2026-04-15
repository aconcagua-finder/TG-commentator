import asyncio
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse
from telethon import TelegramClient
from telethon.errors import (
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    RPCError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession

from app_paths import DATA_DIR
from tg_device import device_kwargs, ensure_device_profile
from role_engine import (
    ACCOUNT_CUSTOM_ROLE_KEY,
    CUSTOM_ROLE_ID,
    CUSTOM_ROLE_NAME,
    EMOJI_LEVELS,
    GENDER_OPTIONS,
    ROLE_PRESET_CATEGORIES,
    legacy_role_id,
    role_for_account,
    role_presets_for_category,
)

from admin_web.helpers import (
    _load_settings,
    _save_settings,
    _load_accounts,
    _save_accounts,
    _active_project_id,
    _filter_accounts_by_project,
    _filter_by_project,
    _project_id_for,
    _find_account_index,
    _db_connect,
    _roles_dict,
    _default_role_id,
    _resolve_role_id,
    _role_name_map,
    _sorted_role_items,
    _ensure_accounts_date_added,
    _ensure_accounts_roles_saved,
    _parse_int,
    _parse_int_field,
    _flash,
    _redirect,
    _normalize_tg_ref,
    _clean_username,
    _upsert_profile_task,
    _load_join_status,
    _update_join_status,
    _record_account_failure,
    _clear_account_failure,
    _cleanup_inbox_for_removed_accounts,
    _auto_pause_commentator,
    ACCOUNT_CHECKS_ENABLED,
)
from admin_web.sort_helpers import apply_sort, resolve_key, template_options
from admin_web.telethon_utils import (
    _telethon_credentials,
    _parse_proxy_tuple,
    _resolve_account_session,
    _resolve_account_credentials,
    _resolve_account_proxy,
    _check_account_entry,
)
from admin_web.templating import templates, _template_context

router = APIRouter()


@router.get("/accounts", response_class=HTMLResponse)
async def accounts_page(request: Request, sort: str = ""):
    accounts, accounts_err = _load_accounts()
    if _ensure_accounts_date_added(accounts):
        _save_accounts(accounts)
    settings, _ = _load_settings()
    _ensure_accounts_roles_saved(accounts, settings)
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    role_names = _role_name_map(settings)

    sort_key = resolve_key(sort, "accounts")
    accounts = apply_sort(accounts, sort_key, "accounts")

    with _db_connect() as conn:
        proxies = conn.execute(
            "SELECT id, url, name, ip, country, status FROM proxies ORDER BY id DESC"
        ).fetchall()
    proxy_names = {p["url"]: p["name"] for p in proxies if p["name"]}

    return templates.TemplateResponse(
        "accounts.html",
        _template_context(
            request,
            accounts=accounts,
            accounts_err=accounts_err,
            role_names=role_names,
            proxies=proxies,
            proxy_names=proxy_names,
            sort_options=template_options("accounts"),
            current_sort=sort_key,
        ),
    )


@router.post("/accounts/check")
async def accounts_check(request: Request):
    if not ACCOUNT_CHECKS_ENABLED:
        _flash(request, "warning", "Проверки аккаунтов отключены.")
        return _redirect("/accounts")
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    if not _filter_accounts_by_project(accounts, project_id):
        _flash(request, "warning", "Нет аккаунтов для проверки.")
        return _redirect("/accounts")

    api_id_default, api_hash_default = _telethon_credentials()
    status_counts: Dict[str, int] = {}
    errors = 0

    async with _auto_pause_commentator(request, reason="Проверка аккаунтов"):
        for acc in accounts:
            if _project_id_for(acc) != project_id:
                continue
            status, is_error = await _check_account_entry(
                acc,
                api_id_default,
                api_hash_default,
            )
            status_counts[status] = status_counts.get(status, 0) + 1
            if is_error:
                errors += 1

    _save_accounts(accounts)
    parts = [f"{k}={v}" for k, v in status_counts.items() if v > 0]
    if errors:
        parts.append(f"errors={errors}")
    _flash(request, "success", "Проверка завершена: " + ", ".join(parts))
    return _redirect("/accounts")


@router.post("/accounts/{session_name}/check")
async def account_check_single(request: Request, session_name: str):
    """Check availability of a single account."""
    if not ACCOUNT_CHECKS_ENABLED:
        _flash(request, "warning", "Проверки аккаунтов отключены.")
        return _redirect(f"/accounts/{quote(session_name)}")

    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    acc = next(
        (a for a in accounts if a.get("session_name") == session_name and _project_id_for(a) == project_id),
        None,
    )
    if not acc:
        _flash(request, "danger", "Аккаунт не найден.")
        return _redirect("/accounts")

    api_id_default, api_hash_default = _telethon_credentials()
    status, is_error = await _check_account_entry(acc, api_id_default, api_hash_default)
    _save_accounts(accounts)

    if is_error:
        error_msg = acc.get("last_error", "")
        _flash(request, "danger", f"Аккаунт {session_name}: {status}. {error_msg}")
    else:
        _flash(request, "success", f"Аккаунт {session_name}: {status}")
    return _redirect(f"/accounts/{quote(session_name)}")


@router.get("/accounts/{session_name}/errors", response_class=HTMLResponse)
async def account_error_log(request: Request, session_name: str):
    """Show full error history for a single account."""
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    acc = next(
        (a for a in accounts if a.get("session_name") == session_name and _project_id_for(a) == project_id),
        None,
    )
    if not acc:
        raise HTTPException(status_code=404, detail="Аккаунт не найден")

    with _db_connect() as conn:
        rows = conn.execute(
            "SELECT kind, error, target, created_at FROM account_failure_log "
            "WHERE session_name = %s ORDER BY created_at DESC LIMIT 200",
            (session_name,),
        ).fetchall()

    log_entries = []
    for row in rows:
        ts = row[3]
        try:
            dt_str = datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%d.%m.%Y %H:%M:%S")
        except Exception:
            dt_str = str(ts)
        log_entries.append(
            {"kind": row[0], "error": row[1] or "", "target": row[2] or "", "created_at": dt_str}
        )

    return templates.TemplateResponse(
        "account_errors.html",
        _template_context(request, account=acc, log_entries=log_entries),
    )


@router.get("/accounts/new", response_class=HTMLResponse)
async def account_new_page(request: Request):
    settings, _ = _load_settings()
    roles = _sorted_role_items(settings)
    default_role_id = _default_role_id(settings)
    with _db_connect() as conn:
        proxies = conn.execute(
            "SELECT id, ip, country, url, name FROM proxies WHERE status='active' ORDER BY id DESC"
        ).fetchall()
    return templates.TemplateResponse(
        "account_new.html",
        _template_context(request, roles=roles, default_role_id=default_role_id, proxies=proxies),
    )


@router.post("/accounts/new/session")
async def account_new_session(
    request: Request,
    session_name: str = Form(...),
    session_string: str = Form(...),
    proxy_id: str = Form(""),
    role_id: str = Form(""),
):
    session_name = session_name.strip()
    session_string = session_string.strip()
    if not session_name:
        raise HTTPException(status_code=400, detail="session_name пустой")
    if not session_string:
        raise HTTPException(status_code=400, detail="session_string пустой")

    accounts, _ = _load_accounts()
    if any(a.get("session_name") == session_name for a in accounts):
        raise HTTPException(status_code=400, detail="Аккаунт с таким session_name уже существует")

    settings, _ = _load_settings()
    selected_role_id = _resolve_role_id(settings, role_id)
    if not selected_role_id:
        raise HTTPException(status_code=400, detail="Не найдена роль по умолчанию")

    proxy_url: str | None = None
    if proxy_id.strip():
        try:
            proxy_id_int = int(proxy_id)
        except ValueError:
            proxy_id_int = None
            _flash(request, "warning", "Прокси: некорректный ID, будет добавлено без прокси.")
        if proxy_id_int is not None:
            with _db_connect() as conn:
                row = conn.execute("SELECT url FROM proxies WHERE id = %s", (proxy_id_int,)).fetchone()
                if row:
                    proxy_url = row["url"]

    api_id, api_hash = _telethon_credentials()
    proxy_tuple = _parse_proxy_tuple(proxy_url) if proxy_url else None
    tmp_acc: Dict[str, Any] = {"session_name": session_name, "session_string": session_string}
    ensure_device_profile(tmp_acc)
    client = TelegramClient(
        StringSession(session_string),
        api_id,
        api_hash,
        proxy=proxy_tuple,
        **device_kwargs(tmp_acc),
    )
    try:
        await client.connect()
        if not await client.is_user_authorized():
            raise HTTPException(status_code=400, detail="Сессия не авторизована")
        me = await client.get_me()
    finally:
        if client.is_connected():
            await client.disconnect()

    settings, _ = _load_settings()
    selected_role_id = _resolve_role_id(settings, role_id)
    if not selected_role_id:
        raise HTTPException(status_code=400, detail="Не найдена роль по умолчанию")
    project_id = _active_project_id(settings)
    new_acc: Dict[str, Any] = {
        "session_name": session_name,
        "session_string": session_string,
        "user_id": me.id,
        "first_name": me.first_name,
        "last_name": me.last_name or "",
        "username": me.username or "",
        "status": "active",
        "project_id": project_id,
        "date_added": datetime.now(timezone.utc).isoformat(),
        "role_id": selected_role_id,
    }
    for key in ("device_type", "device_model", "system_version", "app_version", "lang_code", "system_lang_code"):
        if tmp_acc.get(key):
            new_acc[key] = tmp_acc[key]
    if proxy_url:
        new_acc["proxy_url"] = proxy_url

    accounts.append(new_acc)
    _save_accounts(accounts)

    _flash(request, "success", f"Аккаунт '{session_name}' добавлен.")
    return _redirect("/accounts")


@dataclass
class _PhoneLoginState:
    token: str
    created_at: float
    client: TelegramClient
    session_name: str
    phone: str
    phone_code_hash: str
    proxy_url: str | None
    device_profile: Dict[str, Any]
    role_id: str


PHONE_LOGINS: Dict[str, _PhoneLoginState] = {}


def _phone_logins_gc(max_age_seconds: int = 10 * 60) -> None:
    now = time.time()
    for token, st in list(PHONE_LOGINS.items()):
        if now - st.created_at > max_age_seconds:
            try:
                if st.client.is_connected():
                    asyncio.create_task(st.client.disconnect())
            except Exception:
                pass
            PHONE_LOGINS.pop(token, None)


@router.post("/accounts/new/phone/start", response_class=HTMLResponse)
async def account_new_phone_start(
    request: Request,
    session_name: str = Form(...),
    phone: str = Form(...),
    proxy_id: str = Form(""),
    role_id: str = Form(""),
):
    _phone_logins_gc()

    session_name = session_name.strip()
    phone = phone.strip()
    if not session_name or not phone:
        raise HTTPException(status_code=400, detail="Нужно указать session_name и phone")

    accounts, _ = _load_accounts()
    if any(a.get("session_name") == session_name for a in accounts):
        raise HTTPException(status_code=400, detail="Аккаунт с таким session_name уже существует")

    settings, _ = _load_settings()
    selected_role_id = _resolve_role_id(settings, role_id)
    if not selected_role_id:
        raise HTTPException(status_code=400, detail="Не найдена роль по умолчанию")

    proxy_url: str | None = None
    if proxy_id.strip():
        try:
            proxy_id_int = int(proxy_id)
        except ValueError:
            proxy_id_int = None
            _flash(request, "warning", "Прокси: некорректный ID, вход будет без прокси.")
        if proxy_id_int is not None:
            with _db_connect() as conn:
                row = conn.execute("SELECT url FROM proxies WHERE id = %s", (proxy_id_int,)).fetchone()
                if row:
                    proxy_url = row["url"]

    api_id, api_hash = _telethon_credentials()
    proxy_tuple = _parse_proxy_tuple(proxy_url) if proxy_url else None
    tmp_acc: Dict[str, Any] = {"session_name": session_name, "phone": phone}
    ensure_device_profile(tmp_acc)
    device_profile = {k: tmp_acc.get(k) for k in ("device_type", "device_model", "system_version", "app_version", "lang_code", "system_lang_code") if tmp_acc.get(k)}

    client = TelegramClient(
        StringSession(),
        api_id,
        api_hash,
        proxy=proxy_tuple,
        **device_kwargs(tmp_acc),
    )
    await client.connect()

    try:
        sent_code = await client.send_code_request(phone)
    except RPCError as e:
        await client.disconnect()
        raise HTTPException(status_code=400, detail=f"Ошибка Telegram API: {e}") from e

    token = uuid.uuid4().hex
    PHONE_LOGINS[token] = _PhoneLoginState(
        token=token,
        created_at=time.time(),
        client=client,
        session_name=session_name,
        phone=phone,
        phone_code_hash=sent_code.phone_code_hash,
        proxy_url=proxy_url,
        device_profile=device_profile,
        role_id=selected_role_id,
    )

    return templates.TemplateResponse(
        "account_phone_code.html",
        _template_context(request, token=token, session_name=session_name, phone=phone),
    )


@router.post("/accounts/new/phone/{token}/cancel")
async def account_new_phone_cancel(request: Request, token: str):
    st = PHONE_LOGINS.pop(token, None)
    if st:
        try:
            if st.client.is_connected():
                await st.client.disconnect()
        except Exception:
            pass
    _flash(request, "success", "Вход по телефону отменён.")
    return _redirect("/accounts/new")


@router.post("/accounts/new/phone/{token}/code", response_class=HTMLResponse)
async def account_new_phone_code(request: Request, token: str, code: str = Form(...)):
    st = PHONE_LOGINS.get(token)
    if not st:
        _flash(request, "danger", "Сессия входа устарела. Начните заново.")
        return _redirect("/accounts/new")

    code = code.strip()
    try:
        await st.client.sign_in(st.phone, code, phone_code_hash=st.phone_code_hash)
    except (PhoneCodeInvalidError, PhoneCodeExpiredError):
        _flash(request, "danger", "Неверный или истекший код. Попробуйте снова.")
        return templates.TemplateResponse(
            "account_phone_code.html",
            _template_context(request, token=token, session_name=st.session_name, phone=st.phone),
        )
    except SessionPasswordNeededError:
        return templates.TemplateResponse(
            "account_phone_password.html",
            _template_context(request, token=token, session_name=st.session_name, phone=st.phone),
        )
    except Exception as e:
        PHONE_LOGINS.pop(token, None)
        try:
            if st.client.is_connected():
                await st.client.disconnect()
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=f"Ошибка входа: {e}") from e

    return await _finalize_phone_login(request, token)


@router.post("/accounts/new/phone/{token}/password", response_class=HTMLResponse)
async def account_new_phone_password(request: Request, token: str, tfa_password: str = Form(...)):
    st = PHONE_LOGINS.get(token)
    if not st:
        _flash(request, "danger", "Сессия входа устарела. Начните заново.")
        return _redirect("/accounts/new")

    try:
        await st.client.sign_in(password=tfa_password.strip())
    except PasswordHashInvalidError:
        _flash(request, "danger", "Неверный пароль 2FA. Попробуйте снова.")
        return templates.TemplateResponse(
            "account_phone_password.html",
            _template_context(request, token=token, session_name=st.session_name, phone=st.phone),
        )
    except Exception as e:
        PHONE_LOGINS.pop(token, None)
        try:
            if st.client.is_connected():
                await st.client.disconnect()
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=f"Ошибка 2FA: {e}") from e

    return await _finalize_phone_login(request, token)


async def _finalize_phone_login(request: Request, token: str):
    st = PHONE_LOGINS.pop(token, None)
    if not st:
        _flash(request, "danger", "Сессия входа устарела. Начните заново.")
        return _redirect("/accounts/new")

    try:
        me = await st.client.get_me()
        session_string = st.client.session.save()
    finally:
        try:
            if st.client.is_connected():
                await st.client.disconnect()
        except Exception:
            pass

    settings, _ = _load_settings()
    selected_role_id = _resolve_role_id(settings, st.role_id)
    if not selected_role_id:
        raise HTTPException(status_code=400, detail="Не найдена роль по умолчанию")
    project_id = _active_project_id(settings)
    accounts, _ = _load_accounts()
    _ensure_accounts_roles_saved(accounts, settings)
    new_acc: Dict[str, Any] = {
        "session_name": st.session_name,
        "session_string": session_string,
        "user_id": me.id,
        "first_name": me.first_name,
        "last_name": me.last_name or "",
        "username": me.username or "",
        "phone": st.phone,
        "status": "active",
        "project_id": project_id,
        "date_added": datetime.now(timezone.utc).isoformat(),
        "role_id": selected_role_id,
        **({"proxy_url": st.proxy_url} if st.proxy_url else {}),
        **(st.device_profile or {}),
    }
    accounts.append(new_acc)
    _save_accounts(accounts)

    _flash(request, "success", f"Аккаунт '{st.session_name}' добавлен.")
    return _redirect("/accounts")


@router.get("/accounts/{session_name}", response_class=HTMLResponse)
async def account_edit_page(request: Request, session_name: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    _ensure_accounts_roles_saved(accounts, settings)
    project_id = _active_project_id(settings)
    account = next(
        (a for a in accounts if a.get("session_name") == session_name and _project_id_for(a) == project_id),
        None,
    )
    if not account:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    def _is_account_awake(acc: Dict[str, Any], hour: int) -> bool:
        ss = acc.get("sleep_settings") or {}
        try:
            start_hour = int(ss.get("start_hour", 8))
        except (TypeError, ValueError):
            start_hour = 8
        try:
            end_hour = int(ss.get("end_hour", 23))
        except (TypeError, ValueError):
            end_hour = 23

        start_hour = max(0, min(23, start_hour))
        end_hour = max(0, min(23, end_hour))

        if start_hour == end_hour:
            return True
        if start_hour < end_hour:
            return start_hour <= hour < end_hour
        return hour >= start_hour or hour < end_hour

    roles = _sorted_role_items(settings)
    default_role_id = _default_role_id(settings)
    profile_task = None
    tasks = settings.get("profile_tasks")
    if isinstance(tasks, dict):
        profile_task = tasks.get(session_name)

    with _db_connect() as conn:
        proxies = conn.execute(
            "SELECT id, ip, country, url, name FROM proxies WHERE status='active' ORDER BY id DESC"
        ).fetchall()

    server_now = datetime.now(timezone.utc)
    server_now_label = server_now.strftime("%Y-%m-%d %H:%M UTC")
    server_hour = server_now.hour
    awake_now = _is_account_awake(account, server_hour)
    has_custom_role = isinstance(account.get(ACCOUNT_CUSTOM_ROLE_KEY), dict) and bool(account.get(ACCOUNT_CUSTOM_ROLE_KEY))

    comment_targets = _filter_by_project(settings.get("targets", []) or [], project_id)
    reaction_targets = _filter_by_project(settings.get("reaction_targets", []) or [], project_id)
    monitor_targets = _filter_by_project(settings.get("monitor_targets", []) or [], project_id)

    def _sort_targets(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(
            items,
            key=lambda t: (
                str(t.get("chat_name") or "").lower(),
                str(t.get("chat_username") or "").lower(),
                str(t.get("chat_id") or ""),
            ),
        )

    comment_targets = _sort_targets(comment_targets)
    reaction_targets = _sort_targets(reaction_targets)
    monitor_targets = _sort_targets(monitor_targets)

    join_target_ids: List[str] = []
    for t in comment_targets:
        if not isinstance(t, dict):
            continue
        if t.get("chat_id"):
            join_target_ids.append(str(t.get("chat_id")))
        if t.get("linked_chat_id"):
            join_target_ids.append(str(t.get("linked_chat_id")))
    join_status = _load_join_status(join_target_ids)

    return templates.TemplateResponse(
        "account_edit.html",
        _template_context(
            request,
            account=account,
            account_checks_enabled=ACCOUNT_CHECKS_ENABLED,
            proxies=proxies,
            roles=roles,
            default_role_id=default_role_id,
            custom_role_id=CUSTOM_ROLE_ID,
            custom_role_name=CUSTOM_ROLE_NAME,
            has_custom_role=has_custom_role,
            profile_task=profile_task,
            server_now=server_now_label,
            server_hour=server_hour,
            awake_now=awake_now,
            comment_targets=comment_targets,
            reaction_targets=reaction_targets,
            monitor_targets=monitor_targets,
            join_status=join_status,
        ),
    )


@router.post("/accounts/{session_name}/sleep")
async def account_update_sleep(
    request: Request,
    session_name: str,
    start_hour: str = Form(...),
    end_hour: str = Form(...),
):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    a = accounts[idx]
    a.setdefault("sleep_settings", {"start_hour": 8, "end_hour": 23})
    start_val = _parse_int(start_hour, default=8)
    end_val = _parse_int(end_hour, default=23)
    if start_val is None:
        start_val = 8
    if end_val is None:
        end_val = 23
    a["sleep_settings"]["start_hour"] = max(0, min(23, int(start_val)))
    a["sleep_settings"]["end_hour"] = max(0, min(23, int(end_val)))
    _save_accounts(accounts)
    _flash(request, "success", "Время сна обновлено.")
    return _redirect(f"/accounts/{quote(session_name)}")


@router.post("/accounts/{session_name}/proxy")
async def account_update_proxy(
    request: Request,
    session_name: str,
    proxy_id: str = Form(""),
):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    proxy_url: str | None = None
    if proxy_id.strip():
        try:
            proxy_id_int = int(proxy_id)
        except ValueError:
            proxy_id_int = None
            _flash(request, "warning", "Прокси: некорректный ID, значение не изменено.")
        if proxy_id_int is not None:
            with _db_connect() as conn:
                row = conn.execute("SELECT url FROM proxies WHERE id = %s", (proxy_id_int,)).fetchone()
                if row:
                    proxy_url = row["url"]

    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    if proxy_url:
        accounts[idx]["proxy_url"] = proxy_url
    else:
        accounts[idx].pop("proxy_url", None)
    _save_accounts(accounts)
    _flash(request, "success", "Прокси обновлён.")
    return _redirect(f"/accounts/{quote(session_name)}")


@router.post("/accounts/{session_name}/targets")
async def account_update_targets(
    request: Request,
    session_name: str,
    comment_target_ids: Optional[List[str]] = Form(None),
    reaction_target_ids: Optional[List[str]] = Form(None),
    monitor_target_ids: Optional[List[str]] = Form(None),
):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    if _find_account_index(accounts, session_name, project_id) is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    selected_comment = {str(x).strip() for x in (comment_target_ids or []) if str(x).strip()}
    selected_reaction = {str(x).strip() for x in (reaction_target_ids or []) if str(x).strip()}
    selected_monitor = {str(x).strip() for x in (monitor_target_ids or []) if str(x).strip()}

    def _normalize_assigned_accounts(value: Any) -> List[str]:
        if not isinstance(value, list):
            return []
        result: List[str] = []
        seen: set[str] = set()
        for item in value:
            s = str(item or "").strip()
            if not s or s in seen:
                continue
            seen.add(s)
            result.append(s)
        return result

    def _apply(key: str, selected_ids: set[str]) -> int:
        items = settings.get(key)
        if not isinstance(items, list):
            return 0
        changed = 0
        for item in items:
            if not isinstance(item, dict):
                continue
            if _project_id_for(item) != project_id:
                continue
            chat_id = str(item.get("chat_id") or "").strip()
            if not chat_id:
                continue
            assigned = _normalize_assigned_accounts(item.get("assigned_accounts"))
            should_assign = chat_id in selected_ids
            has_assign = session_name in assigned
            if should_assign and not has_assign:
                assigned.append(session_name)
                item["assigned_accounts"] = assigned
                changed += 1
            elif not should_assign and has_assign:
                item["assigned_accounts"] = [s for s in assigned if s != session_name]
                changed += 1
        return changed

    changed = 0
    changed += _apply("targets", selected_comment)
    changed += _apply("reaction_targets", selected_reaction)
    changed += _apply("monitor_targets", selected_monitor)

    _save_settings(settings)
    _flash(request, "success", "Подключения обновлены." if changed else "Изменений нет.")
    return _redirect(f"/accounts/{quote(session_name)}")


@router.post("/accounts/{session_name}/persona")
@router.post("/accounts/{session_name}/role")
async def account_update_role(
    request: Request,
    session_name: str,
    role_id: str = Form(""),
    persona_id: str = Form(""),
):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    _ensure_accounts_roles_saved(accounts, settings)
    project_id = _active_project_id(settings)
    wanted_role_id = role_id.strip()
    legacy_persona_id = persona_id.strip()
    if not wanted_role_id and legacy_persona_id:
        wanted_role_id = legacy_role_id(legacy_persona_id)

    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    if wanted_role_id == CUSTOM_ROLE_ID:
        has_custom = isinstance(accounts[idx].get(ACCOUNT_CUSTOM_ROLE_KEY), dict) and bool(
            accounts[idx].get(ACCOUNT_CUSTOM_ROLE_KEY)
        )
        if not has_custom:
            _flash(request, "warning", "У аккаунта нет кастомной роли. Сначала настройте её.")
            return _redirect(f"/accounts/{quote(session_name)}")
        accounts[idx]["role_id"] = CUSTOM_ROLE_ID
        _save_accounts(accounts)
        _flash(request, "success", "Роль обновлена.")
        return _redirect(f"/accounts/{quote(session_name)}")

    selected_role_id = _resolve_role_id(settings, wanted_role_id)
    if not selected_role_id:
        raise HTTPException(status_code=400, detail="Роль не найдена")
    accounts[idx]["role_id"] = selected_role_id
    _save_accounts(accounts)

    _flash(request, "success", "Роль обновлена.")
    return _redirect(f"/accounts/{quote(session_name)}")


@router.get("/accounts/{session_name}/role/custom", response_class=HTMLResponse)
async def account_custom_role_page(request: Request, session_name: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    _ensure_accounts_roles_saved(accounts, settings)
    project_id = _active_project_id(settings)

    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    account = accounts[idx]

    role_presets = {
        category: sorted(
            list(role_presets_for_category(settings, category).items()),
            key=lambda item: str(item[1].get("name") or item[0]).lower(),
        )
        for category in ROLE_PRESET_CATEGORIES
    }

    has_custom_role = isinstance(account.get(ACCOUNT_CUSTOM_ROLE_KEY), dict) and bool(account.get(ACCOUNT_CUSTOM_ROLE_KEY))
    role_src_account = {**account, "role_id": CUSTOM_ROLE_ID} if has_custom_role else account
    _, role = role_for_account(role_src_account, settings)
    role = {**(role or {}), "name": CUSTOM_ROLE_NAME}

    role_is_active = str(account.get("role_id") or "").strip() == CUSTOM_ROLE_ID

    return templates.TemplateResponse(
        "account_role_custom.html",
        _template_context(
            request,
            account=account,
            role=role,
            role_presets=role_presets,
            emoji_levels=EMOJI_LEVELS,
            gender_options=GENDER_OPTIONS,
            has_custom_role=has_custom_role,
            role_is_active=role_is_active,
        ),
    )


@router.post("/accounts/{session_name}/role/custom")
async def account_custom_role_update(
    request: Request,
    session_name: str,
    character_preset_id: str = Form(""),
    behavior_preset_id: str = Form(""),
    mood_preset_ids: Optional[List[str]] = Form(None),
    humanization_preset_id: str = Form(""),
    character_prompt_override: str = Form(""),
    behavior_prompt_override: str = Form(""),
    humanization_prompt_override: str = Form(""),
    emoji_level: str = Form("minimal"),
    gender: str = Form("neutral"),
    custom_prompt: str = Form(""),
):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    _ensure_accounts_roles_saved(accounts, settings)
    project_id = _active_project_id(settings)

    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    account = accounts[idx]
    presets = {category: role_presets_for_category(settings, category) for category in ROLE_PRESET_CATEGORIES}

    has_custom = isinstance(account.get(ACCOUNT_CUSTOM_ROLE_KEY), dict) and bool(account.get(ACCOUNT_CUSTOM_ROLE_KEY))
    fallback_src = {**account, "role_id": CUSTOM_ROLE_ID} if has_custom else account
    _, fallback = role_for_account(fallback_src, settings)

    if character_preset_id not in presets["character"]:
        character_preset_id = str(fallback.get("character_preset_id") or "character_balanced")
    if behavior_preset_id not in presets["behavior"]:
        behavior_preset_id = str(fallback.get("behavior_preset_id") or "behavior_balanced")
    if humanization_preset_id not in presets["humanization"]:
        humanization_preset_id = str(fallback.get("humanization_preset_id") or "human_natural")

    mood_ids = [m for m in (mood_preset_ids or []) if m in presets["mood"]]
    if not mood_ids:
        prev_moods = fallback.get("mood_preset_ids") if isinstance(fallback.get("mood_preset_ids"), list) else []
        mood_ids = [m for m in prev_moods if m in presets["mood"]]
    if not mood_ids:
        mood_ids = ["mood_neutral"] if "mood_neutral" in presets["mood"] else list(presets["mood"].keys())[:1]

    emoji_level = str(emoji_level or fallback.get("emoji_level") or "minimal").strip().lower()
    if emoji_level not in EMOJI_LEVELS:
        emoji_level = "minimal"

    gender = str(gender or fallback.get("gender") or "neutral").strip().lower()
    if gender not in GENDER_OPTIONS:
        gender = "neutral"

    existing = account.get(ACCOUNT_CUSTOM_ROLE_KEY) if isinstance(account.get(ACCOUNT_CUSTOM_ROLE_KEY), dict) else {}
    now = datetime.now(timezone.utc).isoformat()
    role_payload: Dict[str, Any] = {
        "name": CUSTOM_ROLE_NAME,
        "character_preset_id": character_preset_id,
        "behavior_preset_id": behavior_preset_id,
        "mood_preset_ids": mood_ids,
        "humanization_preset_id": humanization_preset_id,
        "character_prompt_override": character_prompt_override.strip(),
        "behavior_prompt_override": behavior_prompt_override.strip(),
        "humanization_prompt_override": humanization_prompt_override.strip(),
        "emoji_level": emoji_level,
        "gender": gender,
        "custom_prompt": custom_prompt.strip(),
        "created_at": existing.get("created_at") or now,
        "updated_at": now,
        "builtin": False,
    }

    account[ACCOUNT_CUSTOM_ROLE_KEY] = role_payload
    account["role_id"] = CUSTOM_ROLE_ID
    accounts[idx] = account
    _save_accounts(accounts)

    _flash(request, "success", "Кастомная роль сохранена и назначена аккаунту.")
    return _redirect(f"/accounts/{quote(session_name)}")


@router.post("/accounts/{session_name}/role/custom/delete")
async def account_custom_role_delete(request: Request, session_name: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    _ensure_accounts_roles_saved(accounts, settings)
    project_id = _active_project_id(settings)

    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    account = accounts[idx]
    removed = account.pop(ACCOUNT_CUSTOM_ROLE_KEY, None)
    if str(account.get("role_id") or "").strip() == CUSTOM_ROLE_ID:
        account["role_id"] = _default_role_id(settings)

    accounts[idx] = account
    _save_accounts(accounts)

    if removed:
        _flash(request, "success", "Кастомная роль удалена.")
    else:
        _flash(request, "warning", "У аккаунта нет кастомной роли.")
    return _redirect(f"/accounts/{quote(session_name)}")


@router.post("/accounts/{session_name}/profile")
async def account_update_profile(
    request: Request,
    session_name: str,
    first_name: str = Form(...),
    last_name: str = Form(""),
    username: str = Form(""),
    bio: str = Form(""),
):
    first_name = first_name.strip()
    last_name = last_name.strip()
    bio = bio.strip()
    if not first_name:
        _flash(request, "warning", "Имя не может быть пустым.")
        return _redirect(f"/accounts/{quote(session_name)}")

    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    username_clean = _clean_username(_normalize_tg_ref(username))
    if "profile_username" in accounts[idx]:
        current_username = _clean_username(accounts[idx].get("profile_username"))
    else:
        current_username = _clean_username(accounts[idx].get("username"))
    username_patch: str | None = None
    if username_clean != current_username:
        username_patch = username_clean
    accounts[idx]["first_name"] = first_name
    accounts[idx]["last_name"] = last_name
    accounts[idx]["profile_bio"] = bio
    if username_patch is not None:
        accounts[idx]["profile_username"] = username_patch
    _save_accounts(accounts)

    task_patch: Dict[str, Any] = {"first_name": first_name, "last_name": last_name, "bio": bio}
    if username_patch is not None:
        task_patch["username"] = username_patch
        task_patch["username_clear"] = username_patch == ""
    _upsert_profile_task(
        settings,
        session_name,
        task_patch,
    )
    _save_settings(settings)
    _flash(request, "success", "Задача на обновление профиля создана.")
    return _redirect(f"/accounts/{quote(session_name)}")


@router.post("/accounts/{session_name}/avatar")
async def account_update_avatar(
    request: Request,
    session_name: str,
    avatar: UploadFile = File(...),
):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    if _find_account_index(accounts, session_name, project_id) is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    data = await avatar.read()
    if not data:
        _flash(request, "warning", "Файл пустой.")
        return _redirect(f"/accounts/{quote(session_name)}")
    if len(data) > 5 * 1024 * 1024:
        _flash(request, "warning", "Файл слишком большой (макс 5MB).")
        return _redirect(f"/accounts/{quote(session_name)}")

    ext = (Path(avatar.filename or "").suffix or "").lower()
    if ext not in {".jpg", ".jpeg", ".png", ".webp"}:
        ext = ".jpg"

    uploads_dir = DATA_DIR / "uploads" / "avatars"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    file_name = f"{session_name}_{int(time.time())}_{uuid.uuid4().hex[:8]}{ext}"
    file_path = uploads_dir / file_name
    file_path.write_bytes(data)

    _upsert_profile_task(
        settings,
        session_name,
        {"avatar_path": str(file_path), "avatar_clear": False},
    )
    _save_settings(settings)
    _flash(request, "success", "Задача на обновление аватара создана.")
    return _redirect(f"/accounts/{quote(session_name)}")


@router.post("/accounts/{session_name}/avatar/clear")
async def account_clear_avatar(request: Request, session_name: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    if _find_account_index(accounts, session_name, project_id) is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    _upsert_profile_task(
        settings,
        session_name,
        {"avatar_clear": True, "avatar_path": ""},
    )
    _save_settings(settings)
    _flash(request, "success", "Задача на удаление аватара создана.")
    return _redirect(f"/accounts/{quote(session_name)}")


@router.post("/accounts/{session_name}/personal-channel")
async def account_set_personal_channel(
    request: Request,
    session_name: str,
    personal_channel: str = Form(""),
):
    ref = _normalize_tg_ref(personal_channel)
    if not ref:
        _flash(request, "warning", "Укажите @username или ссылку на канал (t.me/...).")
        return _redirect(f"/accounts/{quote(session_name)}")

    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    accounts[idx]["profile_personal_channel"] = ref
    _save_accounts(accounts)

    _upsert_profile_task(
        settings,
        session_name,
        {"personal_channel": ref, "personal_channel_clear": False},
    )
    _save_settings(settings)
    _flash(request, "success", "Задача на установку персонального канала создана.")
    return _redirect(f"/accounts/{quote(session_name)}")


@router.post("/accounts/{session_name}/personal-channel/clear")
async def account_clear_personal_channel(request: Request, session_name: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    accounts[idx].pop("profile_personal_channel", None)
    _save_accounts(accounts)

    _upsert_profile_task(
        settings,
        session_name,
        {"personal_channel_clear": True, "personal_channel": ""},
    )
    _save_settings(settings)
    _flash(request, "success", "Задача на очистку персонального канала создана.")
    return _redirect(f"/accounts/{quote(session_name)}")


@router.post("/accounts/{session_name}/delete")
async def account_delete(request: Request, session_name: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    new_accounts = [
        a
        for a in accounts
        if not (a.get("session_name") == session_name and _project_id_for(a) == project_id)
    ]
    if len(new_accounts) == len(accounts):
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")
    _save_accounts(new_accounts)

    # Keep inbox unread indicators in sync after removing accounts.
    try:
        _cleanup_inbox_for_removed_accounts(settings)
    except Exception:
        pass

    _flash(request, "success", f"Аккаунт '{session_name}' удалён.")
    return _redirect("/accounts")


# ---------------------------------------------------------------------------
# Re-authorization (reauth) flow for accounts that lost their session
# ---------------------------------------------------------------------------


@dataclass
class _PhoneReauthState:
    token: str
    created_at: float
    client: TelegramClient
    session_name: str
    phone: str
    phone_code_hash: str
    proxy_url: str | None
    device_profile: Dict[str, Any]
    expected_user_id: int | None


PHONE_REAUTHS: Dict[str, _PhoneReauthState] = {}


def _phone_reauths_gc(max_age_seconds: int = 10 * 60) -> None:
    now = time.time()
    for token, st in list(PHONE_REAUTHS.items()):
        if now - st.created_at > max_age_seconds:
            try:
                if st.client.is_connected():
                    asyncio.create_task(st.client.disconnect())
            except Exception:
                pass
            PHONE_REAUTHS.pop(token, None)


def _rejoin_target_lookup(settings: Dict[str, Any], project_id: str, session_name: str) -> Dict[str, Dict[str, Any]]:
    """Return target lookup by chat_id/linked_chat_id for the given session_name."""
    targets = _filter_by_project(settings.get("targets") or [], project_id)
    lookup: Dict[str, Dict[str, Any]] = {}
    for t in targets:
        if not isinstance(t, dict):
            continue
        assigned = t.get("assigned_accounts") or []
        if not isinstance(assigned, list):
            continue
        if session_name not in [str(x).strip() for x in assigned if str(x).strip()]:
            continue
        for key in ("chat_id", "linked_chat_id"):
            tid = str(t.get(key) or "").strip()
            if tid:
                lookup[tid] = t
    return lookup


async def _rejoin_all_assigned(acc: Dict[str, Any], settings: Dict[str, Any], project_id: str) -> tuple[int, int]:
    """Re-join the account into every target it's assigned to. Returns (ok, fail)."""
    from admin_web.telethon_utils import _attempt_join_target
    from services.connection import _record_account_failure as _record_failure_svc

    session_name = acc.get("session_name", "")
    lookup = _rejoin_target_lookup(settings, project_id, session_name)
    if not lookup:
        return 0, 0

    api_id_default, api_hash_default = _telethon_credentials()
    session = _resolve_account_session(acc)
    if not session:
        return 0, len(lookup)
    api_id, api_hash = _resolve_account_credentials(acc, api_id_default, api_hash_default)
    proxy_tuple = _resolve_account_proxy(acc)
    client = TelegramClient(session, api_id, api_hash, proxy=proxy_tuple, **device_kwargs(acc))

    ok = 0
    fail = 0
    try:
        await asyncio.wait_for(client.connect(), timeout=15.0)
        if not await asyncio.wait_for(client.is_user_authorized(), timeout=15.0):
            for tid in lookup.keys():
                _update_join_status(session_name, tid, "failed", last_error="unauthorized")
            return 0, len(lookup)
        for tid, target in lookup.items():
            try:
                joined, last_error, last_method = await _attempt_join_target(
                    client, session_name, target, tid
                )
                if joined:
                    _update_join_status(session_name, tid, "joined")
                    ok += 1
                else:
                    _update_join_status(
                        session_name, tid, "failed",
                        last_error=last_error, last_method=last_method,
                    )
                    try:
                        _record_failure_svc(
                            session_name, "join",
                            last_error=str(last_error) if last_error else None,
                            last_target=str(tid),
                        )
                    except Exception:
                        pass
                    fail += 1
            except Exception as e:
                _update_join_status(session_name, tid, "failed", last_error=str(e)[:500])
                fail += 1
    except Exception as e:
        for tid in lookup.keys():
            _update_join_status(session_name, tid, "failed", last_error=str(e)[:500])
        fail += len(lookup) - ok
    finally:
        try:
            if client.is_connected():
                await client.disconnect()
        except Exception:
            pass
    return ok, fail


@router.get("/accounts/{session_name}/reauth", response_class=HTMLResponse)
async def account_reauth_page(request: Request, session_name: str):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    account = next(
        (a for a in accounts if a.get("session_name") == session_name and _project_id_for(a) == project_id),
        None,
    )
    if not account:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    with _db_connect() as conn:
        proxies = conn.execute(
            "SELECT id, ip, country, url, name FROM proxies WHERE status='active' ORDER BY id DESC"
        ).fetchall()

    current_proxy_url = account.get("proxy_url") or ""
    current_proxy_id: Optional[int] = None
    if current_proxy_url:
        with _db_connect() as conn:
            row = conn.execute(
                "SELECT id FROM proxies WHERE url = %s LIMIT 1", (current_proxy_url,)
            ).fetchone()
            if row:
                current_proxy_id = int(row["id"])

    return templates.TemplateResponse(
        "account_reauth.html",
        _template_context(
            request,
            account=account,
            proxies=proxies,
            current_proxy_url=current_proxy_url,
            current_proxy_id=current_proxy_id,
        ),
    )


@router.post("/accounts/{session_name}/reauth/start", response_class=HTMLResponse)
async def account_reauth_start(
    request: Request,
    session_name: str,
    phone: str = Form(...),
    proxy_id: str = Form(""),
):
    _phone_reauths_gc()

    phone = phone.strip()
    if not phone:
        raise HTTPException(status_code=400, detail="Нужно указать номер телефона")

    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    account = next(
        (a for a in accounts if a.get("session_name") == session_name and _project_id_for(a) == project_id),
        None,
    )
    if not account:
        raise HTTPException(status_code=404, detail="Аккаунт не найден в текущем проекте")

    # Pick proxy: use explicitly selected one, else fall back to account's current proxy.
    proxy_url: str | None = account.get("proxy_url") or None
    if proxy_id.strip():
        try:
            proxy_id_int = int(proxy_id)
        except ValueError:
            proxy_id_int = None
            _flash(request, "warning", "Прокси: некорректный ID, используется текущий прокси аккаунта.")
        if proxy_id_int is not None:
            with _db_connect() as conn:
                row = conn.execute("SELECT url FROM proxies WHERE id = %s", (proxy_id_int,)).fetchone()
                proxy_url = row["url"] if row else None
    elif proxy_id == "":
        # Empty explicit selection means "keep current account proxy". If the user wants
        # to drop the proxy entirely, they can set it via the proxy form before reauth.
        pass

    api_id, api_hash = _telethon_credentials()
    proxy_tuple = _parse_proxy_tuple(proxy_url) if proxy_url else None

    # Reuse the account's existing device profile so Telegram sees the same fingerprint.
    ensure_device_profile(account)
    device_profile = {
        k: account.get(k)
        for k in ("device_type", "device_model", "system_version", "app_version", "lang_code", "system_lang_code")
        if account.get(k)
    }

    client = TelegramClient(
        StringSession(),
        api_id,
        api_hash,
        proxy=proxy_tuple,
        **device_kwargs(account),
    )
    await client.connect()

    try:
        sent_code = await client.send_code_request(phone)
    except RPCError as e:
        try:
            if client.is_connected():
                await client.disconnect()
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=f"Ошибка Telegram API: {e}") from e

    try:
        expected_user_id = int(account.get("user_id") or 0) or None
    except (TypeError, ValueError):
        expected_user_id = None

    token = uuid.uuid4().hex
    PHONE_REAUTHS[token] = _PhoneReauthState(
        token=token,
        created_at=time.time(),
        client=client,
        session_name=session_name,
        phone=phone,
        phone_code_hash=sent_code.phone_code_hash,
        proxy_url=proxy_url,
        device_profile=device_profile,
        expected_user_id=expected_user_id,
    )

    return templates.TemplateResponse(
        "account_phone_code.html",
        _template_context(
            request,
            token=token,
            session_name=session_name,
            phone=phone,
            action_base=f"/accounts/{quote(session_name)}/reauth",
        ),
    )


@router.post("/accounts/{session_name}/reauth/{token}/cancel")
async def account_reauth_cancel(request: Request, session_name: str, token: str):
    st = PHONE_REAUTHS.pop(token, None)
    if st:
        try:
            if st.client.is_connected():
                await st.client.disconnect()
        except Exception:
            pass
    _flash(request, "success", "Переавторизация отменена.")
    return _redirect(f"/accounts/{quote(session_name)}")


@router.post("/accounts/{session_name}/reauth/{token}/code", response_class=HTMLResponse)
async def account_reauth_code(
    request: Request,
    session_name: str,
    token: str,
    code: str = Form(...),
):
    st = PHONE_REAUTHS.get(token)
    if not st or st.session_name != session_name:
        _flash(request, "danger", "Сессия входа устарела. Начните заново.")
        return _redirect(f"/accounts/{quote(session_name)}/reauth")

    code = code.strip()
    try:
        await st.client.sign_in(st.phone, code, phone_code_hash=st.phone_code_hash)
    except (PhoneCodeInvalidError, PhoneCodeExpiredError):
        _flash(request, "danger", "Неверный или истекший код. Попробуйте снова.")
        return templates.TemplateResponse(
            "account_phone_code.html",
            _template_context(
                request,
                token=token,
                session_name=st.session_name,
                phone=st.phone,
                action_base=f"/accounts/{quote(session_name)}/reauth",
            ),
        )
    except SessionPasswordNeededError:
        return templates.TemplateResponse(
            "account_phone_password.html",
            _template_context(
                request,
                token=token,
                session_name=st.session_name,
                phone=st.phone,
                action_base=f"/accounts/{quote(session_name)}/reauth",
            ),
        )
    except Exception as e:
        PHONE_REAUTHS.pop(token, None)
        try:
            if st.client.is_connected():
                await st.client.disconnect()
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=f"Ошибка входа: {e}") from e

    return await _finalize_phone_reauth(request, session_name, token)


@router.post("/accounts/{session_name}/reauth/{token}/password", response_class=HTMLResponse)
async def account_reauth_password(
    request: Request,
    session_name: str,
    token: str,
    tfa_password: str = Form(...),
):
    st = PHONE_REAUTHS.get(token)
    if not st or st.session_name != session_name:
        _flash(request, "danger", "Сессия входа устарела. Начните заново.")
        return _redirect(f"/accounts/{quote(session_name)}/reauth")

    try:
        await st.client.sign_in(password=tfa_password.strip())
    except PasswordHashInvalidError:
        _flash(request, "danger", "Неверный пароль 2FA. Попробуйте снова.")
        return templates.TemplateResponse(
            "account_phone_password.html",
            _template_context(
                request,
                token=token,
                session_name=st.session_name,
                phone=st.phone,
                action_base=f"/accounts/{quote(session_name)}/reauth",
            ),
        )
    except Exception as e:
        PHONE_REAUTHS.pop(token, None)
        try:
            if st.client.is_connected():
                await st.client.disconnect()
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=f"Ошибка 2FA: {e}") from e

    return await _finalize_phone_reauth(request, session_name, token)


async def _finalize_phone_reauth(request: Request, session_name: str, token: str):
    st = PHONE_REAUTHS.pop(token, None)
    if not st:
        _flash(request, "danger", "Сессия входа устарела. Начните заново.")
        return _redirect(f"/accounts/{quote(session_name)}/reauth")

    try:
        me = await st.client.get_me()
        session_string = st.client.session.save()
    finally:
        try:
            if st.client.is_connected():
                await st.client.disconnect()
        except Exception:
            pass

    if not me:
        _flash(request, "danger", "Не удалось получить данные пользователя Telegram.")
        return _redirect(f"/accounts/{quote(session_name)}/reauth")

    # Guard: reject if a different Telegram user logged in — preserves project links.
    if st.expected_user_id and int(me.id) != st.expected_user_id:
        _flash(
            request,
            "danger",
            (
                "В Telegram вошли под другим пользователем "
                f"(ID {me.id}, ожидался {st.expected_user_id}). "
                "Переавторизация отклонена, чтобы не сломать привязки аккаунта к чатам и настройкам."
            ),
        )
        return _redirect(f"/accounts/{quote(session_name)}/reauth")

    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx = _find_account_index(accounts, session_name, project_id)
    if idx is None:
        _flash(request, "danger", "Аккаунт не найден в текущем проекте.")
        return _redirect("/accounts")

    acc = accounts[idx]
    acc["session_string"] = session_string
    acc["user_id"] = me.id
    acc["first_name"] = me.first_name or acc.get("first_name", "")
    acc["last_name"] = me.last_name or ""
    acc["username"] = me.username or ""
    acc["phone"] = st.phone
    acc["status"] = "active"
    acc.pop("last_error", None)
    acc["last_checked"] = datetime.now(timezone.utc).isoformat()
    if st.proxy_url:
        acc["proxy_url"] = st.proxy_url
    for k, v in (st.device_profile or {}).items():
        if v and not acc.get(k):
            acc[k] = v
    accounts[idx] = acc
    _save_accounts(accounts)

    for _kind in ("connect", "join", "send"):
        try:
            _clear_account_failure(session_name, _kind)
        except Exception:
            pass

    # Re-join assigned chats.
    ok, fail = 0, 0
    try:
        ok, fail = await _rejoin_all_assigned(acc, settings, project_id)
    except Exception:
        pass

    if ok and not fail:
        _flash(request, "success", f"Переавторизация успешна. Перевступили во все {ok} чат(ов).")
    elif ok or fail:
        _flash(
            request,
            "warning" if fail else "success",
            f"Переавторизация успешна. Перевступили: {ok}, не удалось: {fail}.",
        )
    else:
        _flash(request, "success", "Переавторизация успешна.")
    return _redirect(f"/accounts/{quote(session_name)}")

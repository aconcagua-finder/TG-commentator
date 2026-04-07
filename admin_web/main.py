"""TG-комментатор — Web Admin Panel.

Main application module: app creation, lifespan, middleware, exception handlers.
All routes are in admin_web.routes/ modules.
Helper functions in admin_web.helpers, Telegram utilities in admin_web.telethon_utils,
template setup in admin_web.templating.
"""

from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Dict
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from app_paths import ACCOUNTS_FILE, SETTINGS_FILE, ensure_data_dir

from admin_web.helpers import (
    ADMIN_WEB_PASSWORD,
    ADMIN_WEB_USERNAME,
    DEFAULT_PROJECT_ID,
    STATIC_DIR,
    logger,
    ADMIN_WEB_DISABLE_AUTH,
    ADMIN_WEB_SECRET,
    ACCOUNT_CHECK_HOUR,
    WARNING_FAILURE_THRESHOLD,
    _channel_bare_id,
    _clear_account_failure,
    _db_connect,
    _wants_html,
    _ensure_settings_schema,
    _filter_by_project,
    _load_settings,
    _save_settings,
    _load_accounts,
    _save_accounts,
    _init_database,
    _migrate_legacy_manual_queue,
    _parse_bool,
    _project_id_for,
    _record_account_failure,
    _safe_local_redirect_path,
    _telegram_message_link,
)

from admin_web.telethon_utils import (
    _telethon_credentials,
    _check_account_entry,
)

from admin_web.templating import templates, _template_context

from admin_web.routes import register_routes


# ---------------------------------------------------------------------------
# Lifespan & Background tasks
# ---------------------------------------------------------------------------

async def _scheduled_account_check_loop() -> None:
    """Background loop that checks all accounts once a day at ACCOUNT_CHECK_HOUR (UTC)."""
    while True:
        try:
            now = datetime.now(timezone.utc)
            target = now.replace(hour=ACCOUNT_CHECK_HOUR, minute=0, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            wait_seconds = (target - now).total_seconds()
            logger.info(
                "[account_check_scheduler] Next scheduled check at %s UTC (in %.0f s)",
                target.isoformat(),
                wait_seconds,
            )
            await asyncio.sleep(wait_seconds)

            logger.info("[account_check_scheduler] Starting scheduled account availability check")
            accounts, _ = _load_accounts()
            if not accounts:
                logger.info("[account_check_scheduler] No accounts to check")
                continue

            api_id_default, api_hash_default = _telethon_credentials()
            results: Dict[str, int] = {}
            for acc in accounts:
                status, _ = await _check_account_entry(acc, api_id_default, api_hash_default)
                results[status] = results.get(status, 0) + 1

            _save_accounts(accounts)
            logger.info("[account_check_scheduler] Check complete: %s", results)
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("[account_check_scheduler] Error during scheduled check")
            await asyncio.sleep(60)


@asynccontextmanager
async def _app_lifespan(_: FastAPI):
    ensure_data_dir()
    _init_database()
    if not os.path.exists(SETTINGS_FILE):
        _save_settings(_ensure_settings_schema({}))
    settings, _ = _load_settings()
    moved_legacy_tasks = _migrate_legacy_manual_queue(settings)
    if moved_legacy_tasks:
        logger.info("Migrated %s legacy manual_queue tasks into manual_tasks table", moved_legacy_tasks)
    if not os.path.exists(ACCOUNTS_FILE):
        _save_accounts([])

    check_task = asyncio.create_task(_scheduled_account_check_loop())
    try:
        yield
    finally:
        check_task.cancel()
        try:
            await check_task
        except asyncio.CancelledError:
            pass


# ---------------------------------------------------------------------------
# FastAPI app creation
# ---------------------------------------------------------------------------

app = FastAPI(title="TG-комментатор (Web Admin)", lifespan=_app_lifespan)
app.state.active_clients = {}
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Register all route modules
register_routes(app)


# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------

@app.exception_handler(HTTPException)
async def _http_exception_handler(request: Request, exc: HTTPException):
    if _wants_html(request):
        title = f"Ошибка {exc.status_code}"
        detail = str(exc.detail) if exc.detail is not None else ""
        return templates.TemplateResponse(
            "error.html",
            _template_context(request, title=title, detail=detail),
            status_code=exc.status_code,
        )
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


@app.exception_handler(RequestValidationError)
async def _validation_exception_handler(request: Request, exc: RequestValidationError):
    if _wants_html(request):
        return templates.TemplateResponse(
            "error.html",
            _template_context(
                request,
                title="Некорректный ввод",
                detail="Проверьте поля формы и попробуйте снова.",
            ),
            status_code=422,
        )
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error in admin_web", exc_info=exc)
    if _wants_html(request):
        return templates.TemplateResponse(
            "error.html",
            _template_context(
                request,
                title="Ошибка 500",
                detail="Неожиданная ошибка. Посмотрите логи контейнера admin_web.",
            ),
            status_code=500,
        )
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

@app.middleware("http")
async def _auth_middleware(request: Request, call_next):
    if ADMIN_WEB_DISABLE_AUTH:
        return await call_next(request)
    if request.url.path.startswith("/static"):
        return await call_next(request)
    if request.url.path in {"/login"}:
        return await call_next(request)
    if not request.session.get("user"):
        next_path = quote(request.url.path)
        return RedirectResponse(url=f"/login?next={next_path}", status_code=303)
    return await call_next(request)


# SessionMiddleware должен выполняться ДО auth middleware (последний добавленный middleware выполняется первым)
app.add_middleware(SessionMiddleware, secret_key=ADMIN_WEB_SECRET, same_site="lax")

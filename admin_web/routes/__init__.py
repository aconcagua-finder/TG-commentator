"""Route registration for admin_web.

Import and include all sub-routers here.
"""

from fastapi import FastAPI

from admin_web.routes.accounts import router as accounts_router
from admin_web.routes.auth import router as auth_router
from admin_web.routes.dashboard import router as dashboard_router
from admin_web.routes.dialogs import router as dialogs_router
from admin_web.routes.discussions import router as discussions_router
from admin_web.routes.monitors import router as monitors_router
from admin_web.routes.personas import router as personas_router
from admin_web.routes.proxies import router as proxies_router
from admin_web.routes.reactions import router as reactions_router
from admin_web.routes.settings import router as settings_router
from admin_web.routes.stats import router as stats_router
from admin_web.routes.targets import router as targets_router


def register_routes(app: FastAPI) -> None:
    """Include all route modules into the FastAPI application."""
    app.include_router(auth_router)
    app.include_router(dashboard_router)
    app.include_router(settings_router)
    app.include_router(accounts_router)
    app.include_router(targets_router)
    app.include_router(discussions_router)
    app.include_router(reactions_router)
    app.include_router(monitors_router)
    app.include_router(personas_router)
    app.include_router(proxies_router)
    app.include_router(stats_router)
    app.include_router(dialogs_router)

"""Jinja2 templates instance and shared template context builder.

Separated from main.py so that route modules can import `templates` and
`_template_context` without circular dependencies.
"""

from __future__ import annotations

from typing import Any, Dict

from fastapi import Request
from fastapi.templating import Jinja2Templates

from admin_web.helpers import (
    ACCOUNT_CHECKS_ENABLED,
    DEFAULT_PROJECT_ID,
    STATIC_VERSION,
    TEMPLATES_DIR,
    _active_project,
    _active_project_id,
    _db_connect,
    _filter_accounts_by_project,
    _human_dt,
    _load_accounts,
    _load_settings,
    _pop_flashes,
    _warnings_count,
)

# ---------------------------------------------------------------------------
# Jinja2 templates
# ---------------------------------------------------------------------------

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
templates.env.filters["human_dt"] = _human_dt

_raw_template_response = templates.TemplateResponse


def _template_response_compat(name_or_request: Any, context: Any = None, *args: Any, **kwargs: Any):
    """Keep existing call-sites compatible while using Starlette's newer signature."""
    if isinstance(name_or_request, Request):
        return _raw_template_response(name_or_request, context, *args, **kwargs)
    if not isinstance(context, dict):
        raise TypeError("Template context must be a dict containing request")
    request_obj = context.get("request")
    if request_obj is None:
        raise ValueError("Template context must include request")
    return _raw_template_response(request_obj, name_or_request, context, *args, **kwargs)


templates.TemplateResponse = _template_response_compat  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# Template context builder
# ---------------------------------------------------------------------------

def _template_context(request: Request, **extra: Any) -> Dict[str, Any]:
    settings, _ = _load_settings()
    active_project = _active_project(settings)
    warnings_count = 0
    inbox_counts = {"dialogs": 0, "quotes": 0}
    if request.session.get("user"):
        try:
            accounts, _ = _load_accounts()
            warnings_count = _warnings_count(accounts, settings)
            project_id = _active_project_id(settings)
            sessions = [
                str(a.get("session_name")).strip()
                for a in _filter_accounts_by_project(accounts, project_id)
                if a.get("session_name")
            ]
            sessions = [s for s in sessions if s]

            if sessions:
                placeholders = ", ".join(["?"] * len(sessions))
                with _db_connect() as conn:
                    inbox_counts["dialogs"] = conn.execute(
                        f"SELECT COUNT(*) AS c FROM inbox_messages WHERE kind='dm' AND direction='in' AND is_read=0 AND session_name IN ({placeholders})",
                        tuple(sessions),
                    ).fetchone()["c"]
                    inbox_counts["quotes"] = conn.execute(
                        f"SELECT COUNT(*) AS c FROM inbox_messages WHERE kind='quote' AND direction='in' AND is_read=0 AND session_name IN ({placeholders})",
                        tuple(sessions),
                    ).fetchone()["c"]
            else:
                inbox_counts = {"dialogs": 0, "quotes": 0}
        except Exception:
            warnings_count = 0
            inbox_counts = {"dialogs": 0, "quotes": 0}
    return {
        "request": request,
        "user": request.session.get("user"),
        "flashes": _pop_flashes(request),
        "static_version": STATIC_VERSION,
        "inbox_counts": inbox_counts,
        "warnings_count": warnings_count,
        "account_checks_enabled": ACCOUNT_CHECKS_ENABLED,
        "projects": settings.get("projects", []) or [],
        "active_project": active_project,
        "active_project_id": active_project.get("id", DEFAULT_PROJECT_ID),
        **extra,
    }

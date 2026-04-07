"""Shared sorting helpers for admin_web list pages.

Each list page (accounts, targets, reaction targets, monitor targets,
discussion targets, antispam targets, proxies, personas) needs the same
sort dropdown UX: newest/oldest, A→Я / Я→A, status, last activity, etc.

This module centralises:

* the per-list-type catalogue of allowed sort keys (label + sorter)
* a single ``apply_sort`` entrypoint for in-memory Python lists
* a SQL builder for the proxies page (the only list pulled from the DB)
* helpers to normalise / fall back to the default key

Templates render the dropdown via ``ui.sort_dropdown(...)`` (see
``macros.html``); routes pass the resolved key to ``apply_sort`` and
forward it back into the template context as ``current_sort``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------


def _str_lower(value: Any) -> str:
    return str(value or "").strip().lower()


def _parse_dt(value: Any) -> float:
    """Best-effort timestamp extraction for sort comparison.

    Returns 0.0 for missing/unparseable values, so they end up at the
    bottom of "newest first" (and top of "oldest first").
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return 0.0
    try:
        if s.replace(".", "", 1).isdigit():
            return float(s)
        # Strip trailing 'Z' if present, fromisoformat handles offsets in 3.11+ but not Z.
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return 0.0


def _account_full_name(acc: Mapping[str, Any]) -> str:
    first = str(acc.get("first_name") or "").strip()
    last = str(acc.get("last_name") or "").strip()
    full = f"{first} {last}".strip()
    if full:
        return full.lower()
    return _str_lower(acc.get("username") or acc.get("session_name"))


# Account status weight: active first, problems last.
_ACCOUNT_STATUS_WEIGHT: Dict[str, int] = {
    "active": 0,
    "sleeping": 1,
    "limited": 2,
    "frozen": 3,
    "human_check": 4,
    "unauthorized": 5,
    "unavailable": 6,
    "banned": 7,
    "": 8,
}


def _account_status_weight(acc: Mapping[str, Any]) -> Tuple[int, str]:
    status = _str_lower(acc.get("status"))
    return (_ACCOUNT_STATUS_WEIGHT.get(status, 99), status)


# ---------------------------------------------------------------------------
# Sort catalogue
# ---------------------------------------------------------------------------


# Each option entry: ``key`` is what goes in the URL / localStorage,
# ``label`` is shown in the dropdown, ``sorter`` extracts a sort key from
# an item, ``reverse`` flips the comparison direction. Items are returned
# in the order they appear here, so the first option is also the default.

SortOption = Dict[str, Any]


def _opt(key: str, label: str, sorter: Callable[[Any], Any], reverse: bool = False) -> SortOption:
    return {"key": key, "label": label, "sorter": sorter, "reverse": reverse}


LIST_SORT_OPTIONS: Dict[str, List[SortOption]] = {
    # ── Accounts (`accounts.html`) ─────────────────────────────────────
    "accounts": [
        _opt("date_added_desc", "Сначала новые", lambda a: _parse_dt(a.get("date_added")), reverse=True),
        _opt("date_added_asc", "Сначала старые", lambda a: _parse_dt(a.get("date_added"))),
        _opt("name_asc", "Имя А→Я", _account_full_name),
        _opt("name_desc", "Имя Я→A", _account_full_name, reverse=True),
        _opt("session_asc", "Session A→Z", lambda a: _str_lower(a.get("session_name"))),
        _opt("session_desc", "Session Z→A", lambda a: _str_lower(a.get("session_name")), reverse=True),
        _opt("status", "По статусу (рабочие сверху)", _account_status_weight),
        _opt(
            "last_check_desc",
            "Недавно проверенные",
            lambda a: _parse_dt(a.get("last_checked")),
            reverse=True,
        ),
        _opt(
            "last_check_asc",
            "Давно не проверявшиеся",
            lambda a: _parse_dt(a.get("last_checked")),
        ),
    ],
    # ── Targets (comment / reaction / monitor / discussion / antispam) ─
    # All five lists share the same shape, so they share one catalogue.
    "chat_target": [
        _opt("date_added_desc", "Сначала новые", lambda t: _parse_dt(t.get("date_added")), reverse=True),
        _opt("date_added_asc", "Сначала старые", lambda t: _parse_dt(t.get("date_added"))),
        _opt("name_asc", "Канал А→Я", lambda t: _str_lower(t.get("chat_name"))),
        _opt("name_desc", "Канал Я→A", lambda t: _str_lower(t.get("chat_name")), reverse=True),
        _opt("username_asc", "Юзернейм A→Z", lambda t: _str_lower(t.get("chat_username"))),
        _opt("username_desc", "Юзернейм Z→A", lambda t: _str_lower(t.get("chat_username")), reverse=True),
    ],
    # ── Personas / roles ───────────────────────────────────────────────
    "personas": [
        _opt("name_asc", "Имя А→Я", lambda r: _str_lower(r[1].get("name") or r[0])),
        _opt("name_desc", "Имя Я→A", lambda r: _str_lower(r[1].get("name") or r[0]), reverse=True),
        _opt(
            "created_desc",
            "Сначала новые",
            lambda r: _parse_dt(r[1].get("created_at") or r[1].get("date_added")),
            reverse=True,
        ),
        _opt(
            "created_asc",
            "Сначала старые",
            lambda r: _parse_dt(r[1].get("created_at") or r[1].get("date_added")),
        ),
    ],
}


# ---------------------------------------------------------------------------
# Public API for routes
# ---------------------------------------------------------------------------


def options_for(list_type: str) -> List[SortOption]:
    """Return the catalogue for a given list type, or [] if unknown."""
    return LIST_SORT_OPTIONS.get(list_type, [])


def default_key(list_type: str) -> str:
    """Return the first (default) sort key for a list type."""
    opts = options_for(list_type)
    return opts[0]["key"] if opts else ""


def resolve_key(value: Optional[str], list_type: str) -> str:
    """Normalise a query-string sort value, falling back to the default."""
    raw = str(value or "").strip()
    valid = {opt["key"] for opt in options_for(list_type)}
    return raw if raw in valid else default_key(list_type)


def apply_sort(items: Iterable[Any], sort_key: str, list_type: str) -> List[Any]:
    """Sort an iterable using a configured option (or no-op if unknown)."""
    materialised = list(items)
    opts = options_for(list_type)
    if not opts:
        return materialised
    target = next((o for o in opts if o["key"] == sort_key), None)
    if target is None:
        target = opts[0]
    sorter: Callable[[Any], Any] = target["sorter"]
    reverse: bool = bool(target["reverse"])
    try:
        materialised.sort(key=sorter, reverse=reverse)
    except Exception:
        # Defensive fallback: never break the page if a row has unexpected types.
        pass
    return materialised


def template_options(list_type: str) -> List[Dict[str, str]]:
    """Strip sorter callables, leaving only ``key`` + ``label`` for templates."""
    return [{"key": o["key"], "label": o["label"]} for o in options_for(list_type)]


# ---------------------------------------------------------------------------
# SQL builder for the proxies page
# ---------------------------------------------------------------------------


# Proxy rows live in the DB; sorting in SQL is cheaper than re-sorting the
# whole result set in Python. Each entry maps a sort key to a deterministic
# ORDER BY clause (with id as a stable tiebreaker).
PROXY_SORT_OPTIONS: List[Dict[str, str]] = [
    {"key": "id_desc", "label": "Сначала новые", "sql": "id DESC"},
    {"key": "id_asc", "label": "Сначала старые", "sql": "id ASC"},
    {"key": "name_asc", "label": "Название А→Я", "sql": "LOWER(COALESCE(name, '')) ASC, id DESC"},
    {"key": "name_desc", "label": "Название Я→A", "sql": "LOWER(COALESCE(name, '')) DESC, id DESC"},
    {"key": "country_asc", "label": "Страна A→Z", "sql": "LOWER(COALESCE(country, '')) ASC, id DESC"},
    {"key": "country_desc", "label": "Страна Z→A", "sql": "LOWER(COALESCE(country, '')) DESC, id DESC"},
    {"key": "status", "label": "Сначала рабочие", "sql": "CASE status WHEN 'active' THEN 0 WHEN 'checking' THEN 1 WHEN 'dead' THEN 2 ELSE 3 END, id DESC"},
    {"key": "last_check_desc", "label": "Недавно проверенные", "sql": "COALESCE(last_check, '') DESC, id DESC"},
    {"key": "last_check_asc", "label": "Давно не проверявшиеся", "sql": "COALESCE(last_check, '') ASC, id DESC"},
]


def proxy_sort_options() -> List[Dict[str, str]]:
    return [{"key": o["key"], "label": o["label"]} for o in PROXY_SORT_OPTIONS]


def proxy_default_key() -> str:
    return PROXY_SORT_OPTIONS[0]["key"]


def proxy_resolve_key(value: Optional[str]) -> str:
    raw = str(value or "").strip()
    valid = {o["key"] for o in PROXY_SORT_OPTIONS}
    return raw if raw in valid else proxy_default_key()


def proxy_order_by_sql(sort_key: str) -> str:
    """Return a safe ORDER BY clause (without the keyword) for a proxy sort key.

    The mapping is closed (no f-string interpolation of user input), so
    SQL injection through the ?sort= query parameter is impossible.
    """
    key = proxy_resolve_key(sort_key)
    for option in PROXY_SORT_OPTIONS:
        if option["key"] == key:
            return option["sql"]
    return PROXY_SORT_OPTIONS[0]["sql"]

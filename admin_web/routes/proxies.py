"""Proxy management routes."""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse

from admin_web.helpers import (
    _db_connect,
    _flash,
    _load_accounts,
    _parse_int_field,
    _redirect,
    _save_accounts,
)
from admin_web.sort_helpers import (
    proxy_order_by_sql,
    proxy_resolve_key,
    proxy_sort_options,
)
from admin_web.telethon_utils import (
    _check_proxy_health,
    _normalize_proxy_url,
    _split_proxy_line,
)
from admin_web.templating import templates, _template_context

router = APIRouter()


@router.get("/proxies", response_class=HTMLResponse)
async def proxies_page(request: Request, sort: str = ""):
    sort_key = proxy_resolve_key(sort)
    order_by = proxy_order_by_sql(sort_key)
    with _db_connect() as conn:
        proxies = conn.execute(
            f"SELECT id, url, name, ip, country, status, last_check FROM proxies ORDER BY {order_by} LIMIT 200"
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) AS c FROM proxies").fetchone()["c"]
        active = conn.execute("SELECT COUNT(*) AS c FROM proxies WHERE status='active'").fetchone()["c"]
        dead = conn.execute("SELECT COUNT(*) AS c FROM proxies WHERE status='dead'").fetchone()["c"]

    return templates.TemplateResponse(
        "proxies.html",
        _template_context(
            request,
            proxies=proxies,
            total=total,
            active=active,
            dead=dead,
            sort_options=proxy_sort_options(),
            current_sort=sort_key,
        ),
    )


@router.post("/proxies/add")
async def proxies_add(
    request: Request,
    proxies_text: str = Form(...),
    proxy_name: str = Form(""),
):
    lines = [l.strip() for l in proxies_text.splitlines() if l.strip()]
    if not lines:
        _flash(request, "warning", "Список пустой.")
        return _redirect("/proxies")

    added = 0
    dup = 0
    invalid = 0
    base_name = proxy_name.strip() or None
    for line in lines:
        raw_url, line_name = _split_proxy_line(line)
        url = _normalize_proxy_url(raw_url)
        if not url:
            invalid += 1
            continue
        name = line_name or base_name
        res = await _check_proxy_health(url)
        with _db_connect() as conn:
            try:
                conn.execute(
                    "INSERT INTO proxies (url, name, ip, country, status, last_check) VALUES (%s, %s, %s, %s, %s, %s)",
                    (url, name, res["ip"], res["country"], res["status"], datetime.now().isoformat()),
                )
                conn.commit()
                added += 1
            except Exception:
                dup += 1

    msg = f"Импорт завершён: добавлено={added}, дубликаты={dup}"
    if invalid:
        msg += f", пропущено={invalid}"
    _flash(request, "success", msg)
    return _redirect("/proxies")


@router.post("/proxies/check-all")
async def proxies_check_all(request: Request):
    with _db_connect() as conn:
        rows = conn.execute("SELECT id, url FROM proxies").fetchall()

    active = 0
    dead = 0
    for r in rows:
        res = await _check_proxy_health(r["url"])
        with _db_connect() as conn:
            conn.execute(
                "UPDATE proxies SET status=%s, ip=%s, country=%s, last_check=%s WHERE id=%s",
                (res["status"], res["ip"], res["country"], datetime.now().isoformat(), r["id"]),
            )
            conn.commit()
        if res["status"] == "active":
            active += 1
        else:
            dead += 1

    _flash(request, "success", f"Проверка завершена: active={active}, dead={dead}")
    return _redirect("/proxies")


@router.post("/proxies/{proxy_id}/check")
async def proxies_check_one(request: Request, proxy_id: int):
    with _db_connect() as conn:
        row = conn.execute("SELECT url FROM proxies WHERE id=%s", (proxy_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Прокси не найден")
        url = row["url"]

    res = await _check_proxy_health(url)
    with _db_connect() as conn:
        conn.execute(
            "UPDATE proxies SET status=%s, ip=%s, country=%s, last_check=%s WHERE id=%s",
            (res["status"], res["ip"], res["country"], datetime.now().isoformat(), proxy_id),
        )
        conn.commit()

    _flash(request, "success", f"Прокси обновлён: {res['status']}, IP={res['ip']}")
    return _redirect("/proxies")


@router.post("/proxies/{proxy_id}/name")
async def proxies_update_name(request: Request, proxy_id: int, name: str = Form("")):
    name = name.strip()
    with _db_connect() as conn:
        row = conn.execute("SELECT id FROM proxies WHERE id=%s", (proxy_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Прокси не найден")
        conn.execute("UPDATE proxies SET name=%s WHERE id=%s", (name or None, proxy_id))
        conn.commit()
    _flash(request, "success", "Название прокси обновлено.")
    return _redirect("/proxies")


@router.post("/proxies/{proxy_id}/delete")
async def proxies_delete_one(request: Request, proxy_id: int):
    with _db_connect() as conn:
        conn.execute("DELETE FROM proxies WHERE id=%s", (proxy_id,))
        conn.commit()
    _flash(request, "success", "Прокси удалён.")
    return _redirect("/proxies")


@router.post("/proxies/delete-dead")
async def proxies_delete_dead(request: Request):
    with _db_connect() as conn:
        dead_urls = [r["url"] for r in conn.execute("SELECT url FROM proxies WHERE status='dead'").fetchall()]
        conn.execute("DELETE FROM proxies WHERE status='dead'")
        conn.commit()

    accounts, _ = _load_accounts()
    updated = False
    for acc in accounts:
        if acc.get("proxy_url") in dead_urls:
            acc.pop("proxy_url", None)
            updated = True
    if updated:
        _save_accounts(accounts)

    _flash(request, "success", f"Удалено нерабочих прокси: {len(dead_urls)}")
    return _redirect("/proxies")

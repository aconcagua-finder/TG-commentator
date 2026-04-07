"""Persona (role) management routes."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse

from role_engine import (
    EMOJI_LEVELS,
    GENDER_OPTIONS,
    ROLE_PRESET_CATEGORIES,
    random_role_profile,
    role_presets_for_category,
)

from admin_web.helpers import (
    _load_settings,
    _save_settings,
    _load_accounts,
    _save_accounts,
    _active_project_id,
    _filter_accounts_by_project,
    _project_id_for,
    _roles_dict,
    _default_role_id,
    _sorted_role_items,
    _ensure_accounts_roles_saved,
    _flash,
    _redirect,
)
from admin_web.sort_helpers import apply_sort, resolve_key, template_options
from admin_web.templating import templates, _template_context

router = APIRouter()


@router.get("/personas", response_class=HTMLResponse)
async def personas_page(request: Request, sort: str = ""):
    settings, settings_err = _load_settings()
    accounts, _ = _load_accounts()
    _ensure_accounts_roles_saved(accounts, settings)
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    role_presets = {
        category: sorted(
            list(role_presets_for_category(settings, category).items()),
            key=lambda item: str(item[1].get("name") or item[0]).lower(),
        )
        for category in ROLE_PRESET_CATEGORIES
    }
    roles = _sorted_role_items(settings)
    sort_key = resolve_key(sort, "personas")
    roles = apply_sort(roles, sort_key, "personas")
    default_role_id = _default_role_id(settings)
    return templates.TemplateResponse(
        "personas.html",
        _template_context(
            request,
            settings_err=settings_err,
            accounts=accounts,
            roles=roles,
            default_role_id=default_role_id,
            role_presets=role_presets,
            emoji_levels=EMOJI_LEVELS,
            gender_options=GENDER_OPTIONS,
            sort_options=template_options("personas"),
            current_sort=sort_key,
        ),
    )


@router.post("/personas/new")
async def persona_new(
    request: Request,
    name: str = Form(""),
    character_preset_id: str = Form(""),
    behavior_preset_id: str = Form(""),
    mood_preset_ids: Optional[List[str]] = Form(None),
    humanization_preset_id: str = Form(""),
    emoji_level: str = Form("minimal"),
    gender: str = Form("neutral"),
    custom_prompt: str = Form(""),
    randomize: Optional[str] = Form(None),
):
    settings, _ = _load_settings()
    presets = {category: role_presets_for_category(settings, category) for category in ROLE_PRESET_CATEGORIES}

    use_random = randomize is not None
    if use_random:
        rand = random_role_profile(settings)
        character_preset_id = rand.get("character_preset_id", character_preset_id)
        behavior_preset_id = rand.get("behavior_preset_id", behavior_preset_id)
        mood_preset_ids = rand.get("mood_preset_ids", mood_preset_ids or [])
        humanization_preset_id = rand.get("humanization_preset_id", humanization_preset_id)
        emoji_level = rand.get("emoji_level", emoji_level)
        gender = rand.get("gender", gender)

    name = name.strip()
    if not name:
        name = f"Роль {datetime.now().strftime('%H:%M:%S')}"

    if character_preset_id not in presets["character"]:
        character_preset_id = "character_balanced"
    if behavior_preset_id not in presets["behavior"]:
        behavior_preset_id = "behavior_balanced"
    if humanization_preset_id not in presets["humanization"]:
        humanization_preset_id = "human_natural"

    mood_ids = [m for m in (mood_preset_ids or []) if m in presets["mood"]]
    if not mood_ids:
        mood_ids = ["mood_neutral"] if "mood_neutral" in presets["mood"] else list(presets["mood"].keys())[:1]

    emoji_level = str(emoji_level or "minimal").strip().lower()
    if emoji_level not in EMOJI_LEVELS:
        emoji_level = "minimal"

    gender = str(gender or "neutral").strip().lower()
    if gender not in GENDER_OPTIONS:
        gender = "neutral"

    role_id = str(int(time.time() * 1000))
    role_payload: Dict[str, Any] = {
        "name": name,
        "character_preset_id": character_preset_id,
        "behavior_preset_id": behavior_preset_id,
        "mood_preset_ids": mood_ids,
        "humanization_preset_id": humanization_preset_id,
        "emoji_level": emoji_level,
        "gender": gender,
        "custom_prompt": custom_prompt.strip(),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "builtin": False,
    }
    settings.setdefault("roles", {})[role_id] = role_payload
    _save_settings(settings)
    _flash(request, "success", f"Роль создана: {name}")
    return _redirect(f"/personas/{role_id}")


@router.get("/personas/{persona_id}", response_class=HTMLResponse)
async def persona_edit_page(request: Request, persona_id: str):
    settings, _ = _load_settings()
    roles = _roles_dict(settings)
    role = roles.get(persona_id)
    if not role:
        raise HTTPException(status_code=404, detail="Роль не найдена")
    accounts, _ = _load_accounts()
    _ensure_accounts_roles_saved(accounts, settings)
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    role_presets = {
        category: sorted(
            list(role_presets_for_category(settings, category).items()),
            key=lambda item: str(item[1].get("name") or item[0]).lower(),
        )
        for category in ROLE_PRESET_CATEGORIES
    }
    return templates.TemplateResponse(
        "persona_edit.html",
        _template_context(
            request,
            role_id=persona_id,
            role=role,
            accounts=accounts,
            default_role_id=_default_role_id(settings),
            role_presets=role_presets,
            emoji_levels=EMOJI_LEVELS,
            gender_options=GENDER_OPTIONS,
        ),
    )


@router.post("/personas/{persona_id}/assign")
async def persona_assign(
    request: Request,
    persona_id: str,
    sessions: Optional[List[str]] = Form(None),
):
    settings, _ = _load_settings()
    if persona_id not in _roles_dict(settings):
        raise HTTPException(status_code=404, detail="Роль не найдена")
    project_id = _active_project_id(settings)
    default_role_id = _default_role_id(settings)

    wanted = set(sessions or [])
    accounts, _ = _load_accounts()
    _ensure_accounts_roles_saved(accounts, settings)
    for acc in accounts:
        s = acc.get("session_name")
        if not s:
            continue
        if _project_id_for(acc) != project_id:
            continue
        if s in wanted:
            acc["role_id"] = persona_id
        else:
            if acc.get("role_id") == persona_id and default_role_id:
                acc["role_id"] = default_role_id
    _save_accounts(accounts)

    _flash(request, "success", "Назначения ролей сохранены.")
    return _redirect(f"/personas/{quote(persona_id)}")


@router.post("/personas/{persona_id}/update")
async def persona_update(
    request: Request,
    persona_id: str,
    name: str = Form(""),
    character_preset_id: str = Form(""),
    behavior_preset_id: str = Form(""),
    mood_preset_ids: Optional[List[str]] = Form(None),
    humanization_preset_id: str = Form(""),
    emoji_level: str = Form("minimal"),
    gender: str = Form("neutral"),
    custom_prompt: str = Form(""),
    set_default: Optional[str] = Form(None),
    randomize: Optional[str] = Form(None),
):
    settings, _ = _load_settings()
    roles = _roles_dict(settings)
    existing = roles.get(persona_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Роль не найдена")

    presets = {category: role_presets_for_category(settings, category) for category in ROLE_PRESET_CATEGORIES}
    use_random = randomize is not None
    if use_random:
        rand = random_role_profile(settings)
        character_preset_id = rand.get("character_preset_id", character_preset_id)
        behavior_preset_id = rand.get("behavior_preset_id", behavior_preset_id)
        mood_preset_ids = rand.get("mood_preset_ids", mood_preset_ids or [])
        humanization_preset_id = rand.get("humanization_preset_id", humanization_preset_id)
        emoji_level = rand.get("emoji_level", emoji_level)
        gender = rand.get("gender", gender)

    name = name.strip()
    if not name:
        _flash(request, "warning", "Название роли не может быть пустым.")
        return _redirect(f"/personas/{quote(persona_id)}")

    if character_preset_id not in presets["character"]:
        character_preset_id = str(existing.get("character_preset_id") or "character_balanced")
    if behavior_preset_id not in presets["behavior"]:
        behavior_preset_id = str(existing.get("behavior_preset_id") or "behavior_balanced")
    if humanization_preset_id not in presets["humanization"]:
        humanization_preset_id = str(existing.get("humanization_preset_id") or "human_natural")

    mood_ids = [m for m in (mood_preset_ids or []) if m in presets["mood"]]
    if not mood_ids:
        prev_moods = existing.get("mood_preset_ids") if isinstance(existing.get("mood_preset_ids"), list) else []
        mood_ids = [m for m in prev_moods if m in presets["mood"]]
    if not mood_ids:
        mood_ids = ["mood_neutral"] if "mood_neutral" in presets["mood"] else list(presets["mood"].keys())[:1]

    emoji_level = str(emoji_level or existing.get("emoji_level") or "minimal").strip().lower()
    if emoji_level not in EMOJI_LEVELS:
        emoji_level = "minimal"

    gender = str(gender or existing.get("gender") or "neutral").strip().lower()
    if gender not in GENDER_OPTIONS:
        gender = "neutral"

    roles[persona_id] = {
        "name": name,
        "character_preset_id": character_preset_id,
        "behavior_preset_id": behavior_preset_id,
        "mood_preset_ids": mood_ids,
        "humanization_preset_id": humanization_preset_id,
        "emoji_level": emoji_level,
        "gender": gender,
        "custom_prompt": custom_prompt.strip(),
        "created_at": existing.get("created_at") or datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "builtin": bool(existing.get("builtin", False)),
    }
    settings["roles"] = roles
    if set_default is not None:
        settings["default_role_id"] = persona_id
    _save_settings(settings)
    _flash(request, "success", "Роль обновлена.")
    return _redirect(f"/personas/{quote(persona_id)}")


@router.post("/personas/{persona_id}/duplicate")
async def persona_duplicate(request: Request, persona_id: str):
    settings, _ = _load_settings()
    roles = _roles_dict(settings)
    role = roles.get(persona_id)
    if not role:
        raise HTTPException(status_code=404, detail="Роль не найдена")

    new_id = str(int(time.time() * 1000))
    base_name = str(role.get("name") or "Роль").strip() or "Роль"
    duplicated = {
        **role,
        "name": f"{base_name} (копия)",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "builtin": False,
    }
    roles[new_id] = duplicated
    settings["roles"] = roles
    _save_settings(settings)
    _flash(request, "success", "Роль продублирована.")
    return _redirect(f"/personas/{quote(new_id)}")


@router.post("/personas/{persona_id}/delete")
async def persona_delete(request: Request, persona_id: str):
    settings, _ = _load_settings()
    roles = _roles_dict(settings)
    if persona_id == _default_role_id(settings):
        _flash(request, "warning", "Нельзя удалить роль по умолчанию. Сначала выберите другую роль по умолчанию.")
        return _redirect(f"/personas/{quote(persona_id)}")
    removed = roles.pop(persona_id, None)
    if not removed:
        raise HTTPException(status_code=404, detail="Роль не найдена")
    settings["roles"] = roles
    _save_settings(settings)

    accounts, _ = _load_accounts()
    _ensure_accounts_roles_saved(accounts, settings)
    fallback_role_id = _default_role_id(settings)
    updated = False
    for acc in accounts:
        if acc.get("role_id") == persona_id and fallback_role_id:
            acc["role_id"] = fallback_role_id
            updated = True
    if updated:
        _save_accounts(accounts)

    _flash(request, "success", f"Роль удалена: {removed.get('name')}")
    return _redirect("/personas")


@router.post("/personas/{persona_id}/default")
async def persona_set_default(request: Request, persona_id: str):
    settings, _ = _load_settings()
    if persona_id not in _roles_dict(settings):
        raise HTTPException(status_code=404, detail="Роль не найдена")
    settings["default_role_id"] = persona_id
    _save_settings(settings)
    _flash(request, "success", "Роль по умолчанию обновлена.")
    return _redirect(f"/personas/{quote(persona_id)}")


@router.post("/personas/presets/{category}/new")
async def persona_preset_new(
    request: Request,
    category: str,
    name: str = Form(""),
    prompt: str = Form(""),
):
    category = str(category or "").strip()
    if category not in ROLE_PRESET_CATEGORIES:
        raise HTTPException(status_code=400, detail="Неизвестная категория пресета")

    name = name.strip()
    prompt = prompt.strip()
    if not name or not prompt:
        _flash(request, "warning", "Для пресета нужны название и текст.")
        return _redirect("/personas")

    settings, _ = _load_settings()
    settings.setdefault("role_presets", {})
    category_store = settings["role_presets"].setdefault(category, {})
    preset_id = f"custom_{category}_{int(time.time() * 1000)}"
    category_store[preset_id] = {"name": name, "prompt": prompt, "builtin": False}
    _save_settings(settings)
    _flash(request, "success", f"Пресет добавлен: {name}")
    return _redirect("/personas")


@router.post("/personas/presets/{category}/{preset_id}/delete")
async def persona_preset_delete(request: Request, category: str, preset_id: str):
    category = str(category or "").strip()
    if category not in ROLE_PRESET_CATEGORIES:
        raise HTTPException(status_code=400, detail="Неизвестная категория пресета")

    settings, _ = _load_settings()
    category_store = role_presets_for_category(settings, category)
    preset = category_store.get(preset_id)
    if not preset:
        raise HTTPException(status_code=404, detail="Пресет не найден")
    if bool(preset.get("builtin")):
        _flash(request, "warning", "Системные пресеты удалять нельзя.")
        return _redirect("/personas")

    roles = _roles_dict(settings)
    for rid, role in roles.items():
        if not isinstance(role, dict):
            continue
        if category == "mood":
            moods = role.get("mood_preset_ids") if isinstance(role.get("mood_preset_ids"), list) else []
            if preset_id in moods:
                _flash(request, "warning", f"Пресет используется в роли «{role.get('name', rid)}».")
                return _redirect("/personas")
            continue
        role_key = f"{category}_preset_id"
        if str(role.get(role_key) or "") == preset_id:
            _flash(request, "warning", f"Пресет используется в роли «{role.get('name', rid)}».")
            return _redirect("/personas")

    category_store.pop(preset_id, None)
    _save_settings(settings)
    _flash(request, "success", "Пресет удалён.")
    return _redirect("/personas")

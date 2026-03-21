"""AI settings, humanization, and blacklist routes."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import httpx
import openai
from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from google import genai

from admin_web.helpers import (
    DEFAULT_MODELS,
    _active_project_id,
    _load_settings,
    _mask_secret,
    _parse_bool,
    _parse_float_field,
    _parse_int_field,
    _save_settings,
    _flash,
    _redirect,
)
from admin_web.templating import templates, _template_context

router = APIRouter()


@router.get("/settings/ai", response_class=HTMLResponse)
async def ai_settings_page(request: Request):
    settings, settings_err = _load_settings()
    keys = settings.get("api_keys", {})
    return templates.TemplateResponse(
        "ai_settings.html",
        _template_context(
            request,
            settings=settings,
            settings_err=settings_err,
            api_keys_masked={k: _mask_secret(v) for k, v in keys.items()},
        ),
    )


@router.get("/settings/ai/models", response_class=HTMLResponse)
async def ai_models_page(request: Request, provider: str = "", q: str = ""):
    settings, settings_err = _load_settings()
    provider = provider.strip() or settings.get("ai_provider", "deepseek")
    if provider not in {"gemini", "openai", "openrouter", "deepseek"}:
        raise HTTPException(status_code=400, detail="Некорректный провайдер")

    api_key = (settings.get("api_keys", {}) or {}).get(provider)
    query = q.strip().lower()

    models: List[Dict[str, str]] = []
    models_err: str | None = None

    if provider != "openrouter" and not api_key:
        models_err = f"Для провайдера {provider.upper()} не задан API ключ."
    else:
        try:
            if provider == "gemini":
                async with genai.Client(api_key=api_key, http_options={"timeout": 10_000}).aio as aclient:
                    pager = await aclient.models.list(config={"page_size": 200})
                    items: List[Any] = []
                    if hasattr(pager, "__aiter__"):
                        async for item in pager:
                            items.append(item)
                            if len(items) >= 200:
                                break
                    else:
                        for item in pager:
                            items.append(item)
                            if len(items) >= 200:
                                break

                for item in items:
                    raw_name = getattr(item, "name", None) or str(item)
                    model_id = raw_name.split("/")[-1] if isinstance(raw_name, str) else str(raw_name)
                    if query and query not in model_id.lower() and query not in str(raw_name).lower():
                        continue
                    methods = getattr(item, "supported_generation_methods", None)
                    meta = ""
                    if isinstance(methods, (list, tuple)) and methods:
                        meta = ", ".join(str(m) for m in methods)
                    models.append({"id": model_id, "raw": str(raw_name), "meta": meta})
            elif provider == "openrouter":
                headers = {}
                if api_key:
                    headers["Authorization"] = f"Bearer {api_key}"
                async with httpx.AsyncClient(timeout=10.0) as http_client:
                    resp = await http_client.get("https://openrouter.ai/api/v1/models", headers=headers)
                    resp.raise_for_status()
                    payload = resp.json()

                items = payload.get("data", []) if isinstance(payload, dict) else []
                limit = 500 if query else 200
                count = 0
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    model_id = str(item.get("id") or "").strip()
                    if not model_id:
                        continue
                    name = str(item.get("name") or model_id)
                    haystack = f"{model_id} {name}".lower()
                    if query and query not in haystack:
                        continue

                    meta_parts: List[str] = []
                    ctx = item.get("context_length")
                    if isinstance(ctx, int) and ctx > 0:
                        meta_parts.append(f"ctx={ctx}")
                    top = item.get("top_provider") if isinstance(item.get("top_provider"), dict) else {}
                    max_out = top.get("max_completion_tokens") if isinstance(top, dict) else None
                    if isinstance(max_out, int) and max_out > 0:
                        meta_parts.append(f"max_out={max_out}")
                    params = item.get("supported_parameters")
                    if isinstance(params, list) and params:
                        short = ", ".join(str(p) for p in params[:10])
                        meta_parts.append(f"params={short}{'…' if len(params) > 10 else ''}")

                    models.append({"id": model_id, "raw": name, "meta": " · ".join(meta_parts)})
                    count += 1
                    if count >= limit:
                        break
            else:
                base_url = "https://api.deepseek.com" if provider == "deepseek" else None
                client = openai.AsyncOpenAI(api_key=api_key, base_url=base_url, timeout=10.0)
                resp = await client.models.list()
                for item in getattr(resp, "data", []) or []:
                    model_id = getattr(item, "id", None) or str(item)
                    if query and query not in str(model_id).lower():
                        continue
                    models.append({"id": str(model_id), "raw": str(model_id), "meta": ""})
        except Exception as e:
            models_err = str(e)

    return templates.TemplateResponse(
        "ai_models.html",
        _template_context(
            request,
            settings=settings,
            settings_err=settings_err,
            provider=provider,
            q=q,
            models=models,
            models_err=models_err,
        ),
    )


@router.post("/settings/ai/provider")
async def ai_settings_provider(request: Request, ai_provider: str = Form(...)):
    if ai_provider not in {"gemini", "openai", "openrouter", "deepseek"}:
        raise HTTPException(status_code=400, detail="Некорректный провайдер")
    settings, _ = _load_settings()
    settings["ai_provider"] = ai_provider
    _save_settings(settings)
    _flash(request, "success", f"Провайдер по умолчанию: {ai_provider.upper()}")
    return _redirect("/settings/ai")


@router.post("/settings/ai/api-keys")
async def ai_settings_api_keys(
    request: Request,
    gemini_key: str = Form(""),
    openai_key: str = Form(""),
    openrouter_key: str = Form(""),
    deepseek_key: str = Form(""),
    clear_gemini: Optional[str] = Form(None),
    clear_openai: Optional[str] = Form(None),
    clear_openrouter: Optional[str] = Form(None),
    clear_deepseek: Optional[str] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    settings.setdefault("api_keys", {})

    if clear_gemini:
        settings["api_keys"].pop("gemini", None)
    elif gemini_key.strip():
        settings["api_keys"]["gemini"] = gemini_key.strip()

    if clear_openai:
        settings["api_keys"].pop("openai", None)
    elif openai_key.strip():
        settings["api_keys"]["openai"] = openai_key.strip()

    if clear_openrouter:
        settings["api_keys"].pop("openrouter", None)
    elif openrouter_key.strip():
        settings["api_keys"]["openrouter"] = openrouter_key.strip()

    if clear_deepseek:
        settings["api_keys"].pop("deepseek", None)
    elif deepseek_key.strip():
        settings["api_keys"]["deepseek"] = deepseek_key.strip()

    _save_settings(settings)
    _flash(request, "success", "API ключи обновлены.")
    return _redirect("/settings/ai")


@router.post("/settings/ai/models")
async def ai_settings_models(
    request: Request,
    openai_chat: str = Form(""),
    openai_eval: str = Form(""),
    openai_image: str = Form(""),
    openrouter_chat: str = Form(""),
    openrouter_eval: str = Form(""),
    deepseek_chat: str = Form(""),
    deepseek_eval: str = Form(""),
    gemini_chat: str = Form(""),
    gemini_eval: str = Form(""),
    gemini_names: str = Form(""),
    reset_models: Optional[str] = Form(None),
):
    settings, _ = _load_settings()

    if reset_models:
        settings["models"] = dict(DEFAULT_MODELS)
        _save_settings(settings)
        _flash(request, "success", "Модели сброшены к рекомендуемым.")
        return _redirect("/settings/ai")

    models = settings.get("models")
    if not isinstance(models, dict):
        models = {}

    def set_model(value: str, key: str) -> None:
        v = value.strip()
        if v:
            models[key] = v

    set_model(openai_chat, "openai_chat")
    set_model(openai_eval, "openai_eval")
    set_model(openai_image, "openai_image")
    set_model(openrouter_chat, "openrouter_chat")
    set_model(openrouter_eval, "openrouter_eval")
    set_model(deepseek_chat, "deepseek_chat")
    set_model(deepseek_eval, "deepseek_eval")
    set_model(gemini_chat, "gemini_chat")
    set_model(gemini_eval, "gemini_eval")
    set_model(gemini_names, "gemini_names")

    settings["models"] = models
    _save_settings(settings)
    _flash(request, "success", "Модели обновлены.")
    return _redirect("/settings/ai")


# ---------------------------------------------------------------------------
# Humanization
# ---------------------------------------------------------------------------


@router.get("/settings/humanization", response_class=HTMLResponse)
async def humanization_page(request: Request):
    settings, settings_err = _load_settings()
    h = settings.get("humanization", {}) or {}
    pk = settings.get("product_knowledge", {}) or {}
    return templates.TemplateResponse(
        "humanization.html",
        _template_context(request, settings_err=settings_err, h=h, pk=pk, settings=settings),
    )


@router.post("/settings/humanization")
async def humanization_save(
    request: Request,
    temperature: str = Form(""),
    repetition_penalty: str = Form("0"),
    typo_chance: str = Form("0"),
    lowercase_chance: str = Form("80"),
    split_chance: str = Form("60"),
    comma_skip_chance: str = Form("30"),
    max_words: str = Form("20"),
    max_tokens: str = Form("60"),
    similarity_threshold: str = Form("0.78"),
    similarity_max_retries: str = Form("1"),
    short_post_diversify: str | None = Form(None),
    short_post_diversity_words: str = Form("10"),
    short_post_min_new_tokens: str = Form("2"),
    custom_rules: str = Form(""),
    product_knowledge_prompt: str = Form(""),
):
    settings, _ = _load_settings()
    settings.setdefault("humanization", {})
    settings.setdefault("product_knowledge", {})

    settings["humanization"]["temperature"] = _parse_float_field(
        request,
        temperature,
        default=None,
        label="Температура",
        min_value=0.0,
        max_value=2.0,
    )
    settings["humanization"]["repetition_penalty"] = _parse_int_field(
        request, repetition_penalty, default=0, label="Штраф повторов", min_value=0, max_value=100
    )
    settings["humanization"]["typo_chance"] = _parse_int_field(
        request, typo_chance, default=0, label="Опечатки", min_value=0, max_value=100
    )
    settings["humanization"]["lowercase_chance"] = _parse_int_field(
        request, lowercase_chance, default=80, label="lowercase", min_value=0, max_value=100
    )
    settings["humanization"]["split_chance"] = _parse_int_field(
        request, split_chance, default=60, label="Разбив", min_value=0, max_value=100
    )
    settings["humanization"]["comma_skip_chance"] = _parse_int_field(
        request, comma_skip_chance, default=30, label="Пропуск запятых", min_value=0, max_value=100
    )
    settings["humanization"]["max_words"] = _parse_int_field(request, max_words, default=20, label="Макс. слов", min_value=0)
    settings["humanization"]["max_tokens"] = _parse_int_field(
        request, max_tokens, default=60, label="Лимит ответа (токены)", min_value=1
    )
    settings["humanization"]["similarity_threshold"] = _parse_float_field(
        request,
        similarity_threshold,
        default=0.78,
        label="Порог схожести",
        min_value=0.0,
        max_value=1.0,
    )
    settings["humanization"]["similarity_max_retries"] = _parse_int_field(
        request,
        similarity_max_retries,
        default=1,
        label="Перегенераций",
        min_value=0,
        max_value=3,
    )

    settings["humanization"]["short_post_diversify"] = _parse_bool(short_post_diversify, default=False)
    settings["humanization"]["short_post_diversity_words"] = _parse_int_field(
        request,
        short_post_diversity_words,
        default=10,
        label="Короткий пост (смысловых слов)",
        min_value=0,
        max_value=50,
    )
    settings["humanization"]["short_post_min_new_tokens"] = _parse_int_field(
        request,
        short_post_min_new_tokens,
        default=2,
        label="Мин. новых смысловых слов",
        min_value=0,
        max_value=6,
    )

    rules = custom_rules.strip()
    if rules:
        settings["humanization"]["custom_rules"] = rules
    else:
        settings["humanization"].pop("custom_rules", None)

    pk_prompt = str(product_knowledge_prompt or "").strip()
    if pk_prompt:
        settings["product_knowledge"]["prompt"] = pk_prompt
    else:
        try:
            settings["product_knowledge"].pop("prompt", None)
        except Exception:
            pass

    _save_settings(settings)
    _flash(request, "success", "Общие параметры обновлены.")
    return _redirect("/settings/humanization")


# ---------------------------------------------------------------------------
# Blacklist
# ---------------------------------------------------------------------------


@router.get("/settings/blacklist", response_class=HTMLResponse)
async def blacklist_page(request: Request):
    settings, settings_err = _load_settings()
    blacklist = settings.get("blacklist", []) or []
    return templates.TemplateResponse(
        "blacklist.html",
        _template_context(request, settings_err=settings_err, blacklist=blacklist),
    )


@router.post("/settings/blacklist/add")
async def blacklist_add(request: Request, words: str = Form(...)):
    settings, _ = _load_settings()
    settings.setdefault("blacklist", [])
    existing_lower = {w.lower() for w in settings["blacklist"] if isinstance(w, str)}
    added = 0
    for w in words.replace("\n", ",").split(","):
        w = w.strip()
        if not w:
            continue
        if w.lower() in existing_lower:
            continue
        settings["blacklist"].append(w)
        existing_lower.add(w.lower())
        added += 1
    _save_settings(settings)
    _flash(request, "success", f"Добавлено слов: {added}")
    return _redirect("/settings/blacklist")


@router.post("/settings/blacklist/delete")
async def blacklist_delete(request: Request, word: str = Form(...)):
    settings, _ = _load_settings()
    settings.setdefault("blacklist", [])
    settings["blacklist"] = [w for w in settings["blacklist"] if str(w) != word]
    _save_settings(settings)
    _flash(request, "success", f"Удалено: {word}")
    return _redirect("/settings/blacklist")


@router.post("/settings/blacklist/clear")
async def blacklist_clear(request: Request):
    settings, _ = _load_settings()
    settings["blacklist"] = []
    _save_settings(settings)
    _flash(request, "success", "Чёрный список очищен.")
    return _redirect("/settings/blacklist")

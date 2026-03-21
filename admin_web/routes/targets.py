from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from telethon import TelegramClient
from telethon.tl.functions.channels import GetChannelRecommendationsRequest, GetFullChannelRequest
from telethon.tl.types import PeerChannel
from tg_device import device_kwargs

from admin_web.helpers import (
    _active_project_id,
    _auto_pause_commentator,
    _channel_bare_id,
    _clean_username,
    _clear_account_failure,
    _db_connect,
    _extract_invite_hash,
    _filter_accounts_by_project,
    _filter_by_project,
    _find_target_by_chat_id,
    _flash,
    _load_accounts,
    _load_join_status,
    _load_settings,
    _parse_bool,
    _parse_int_field,
    _project_id_for,
    _record_account_failure,
    _redirect,
    _save_settings,
    _update_join_status,
)
from admin_web.telethon_utils import (
    _attempt_join_target,
    _derive_target_chat_info,
    _get_any_authorized_client,
    _refresh_target_access_hashes,
    _resolve_account_credentials,
    _resolve_account_proxy,
    _resolve_account_session,
    _telethon_credentials,
)
from admin_web.templating import _template_context, templates

router = APIRouter()


@router.get("/targets", response_class=HTMLResponse)
async def targets_page(request: Request):
    settings, settings_err = _load_settings()
    project_id = _active_project_id(settings)
    targets = _filter_by_project(settings.get("targets", []) or [], project_id)
    targets_sorted = sorted(targets, key=lambda x: x.get("date_added", ""), reverse=True)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    accounts_by_session = {a.get("session_name"): a for a in accounts}
    return templates.TemplateResponse(
        "targets.html",
        _template_context(
            request,
            settings_err=settings_err,
            targets=targets_sorted,
            accounts_by_session=accounts_by_session,
        ),
    )


@router.get("/targets/new", response_class=HTMLResponse)
async def targets_new_page(request: Request, chat_input: str = ""):
    accounts, _ = _load_accounts()
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    accounts = _filter_accounts_by_project(accounts, project_id)
    return templates.TemplateResponse(
        "target_new.html",
        _template_context(request, accounts=accounts, chat_input_prefill=chat_input),
    )


@router.get("/targets/search", response_class=HTMLResponse)
async def targets_search_page(request: Request, source: str = "", auto_pause: str = "1"):
    results: List[Dict[str, Any]] = []
    error: str | None = None

    source = source.strip()
    auto_pause_flag = _parse_bool(auto_pause, default=True)
    if source:
        async with _auto_pause_commentator(request, auto_pause=auto_pause_flag, reason="Поиск каналов"):
            try:
                client = await _get_any_authorized_client()
                try:
                    source_entity = await client.get_entity(source)
                    rec = await client(GetChannelRecommendationsRequest(channel=source_entity))

                    for chat in getattr(rec, "chats", []) or []:
                        if getattr(chat, "megagroup", False):
                            continue
                        try:
                            full_channel = await client(GetFullChannelRequest(channel=chat))
                            linked_chat_id_bare = getattr(full_channel.full_chat, "linked_chat_id", None)
                            if not linked_chat_id_bare:
                                continue
                            comment_chat_entity = await client.get_entity(PeerChannel(linked_chat_id_bare))
                            results.append(
                                {
                                    "chat_id": f"-100{chat.id}",
                                    "chat_username": getattr(chat, "username", None),
                                    "chat_name": getattr(chat, "title", str(chat.id)),
                                    "linked_chat_id": f"-100{comment_chat_entity.id}",
                                }
                            )
                        except Exception:
                            continue
                finally:
                    if client.is_connected():
                        await client.disconnect()
            except Exception as e:
                error = str(e)

    return templates.TemplateResponse(
        "target_search.html",
        _template_context(request, source=source, results=results, error=error),
    )


@router.post("/targets/new")
async def targets_new_submit(
    request: Request,
    chat_input: str = Form(...),
    slow_join_interval_mins: str = Form("0"),
    initial_comment_delay: str = Form("10"),
    delay_between_accounts: str = Form("10"),
    comment_chance: str = Form("100"),
    tag_comment_chance: str = Form("50"),
    accounts_per_post_min: str = Form("0"),
    accounts_per_post_max: str = Form("0"),
    daily_comment_limit: str = Form("50"),
    ai_provider: str = Form("default"),
    min_word_count: str = Form("0"),
    min_post_interval_mins: str = Form("0"),
    min_meaningful_words: str = Form("2"),
    media_min_meaningful_words: str = Form("6"),
    skip_promotional_posts: Optional[str] = Form(None),
    skip_short_media_posts: Optional[str] = Form(None),
    auto_pause: Optional[str] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)

    chat_input = chat_input.strip()
    auto_pause_flag = _parse_bool(auto_pause, default=True)
    async with _auto_pause_commentator(request, auto_pause=auto_pause_flag, reason="Проверка/вступление в чат"):
        try:
            chat_info = await _derive_target_chat_info(chat_input)
        except HTTPException as e:
            _flash(request, "danger", str(e.detail))
            return _redirect(f"/targets/new?chat_input={quote(chat_input)}")

    chat_id = chat_info["chat_id"]
    existing_targets = _filter_by_project(settings.get("targets", []) or [], project_id)
    if any(str(t.get("chat_id")) == str(chat_id) for t in existing_targets):
        _flash(request, "warning", "Этот чат уже добавлен в цели комментирования.")
        return _redirect(f"/targets/{quote(chat_id)}")

    new_target: Dict[str, Any] = {
        **chat_info,
        "slow_join_interval_mins": _parse_int_field(
            request, slow_join_interval_mins, default=0, label="Медленное вступление (мин)", min_value=0
        ),
        "initial_comment_delay": _parse_int_field(
            request, initial_comment_delay, default=180, label="Пауза после поста", min_value=0
        ),
        "delay_between_accounts": _parse_int_field(
            request, delay_between_accounts, default=240, label="Пауза между аккаунтами", min_value=0
        ),
        "comment_chance": _parse_int_field(
            request, comment_chance, default=100, label="Комментировать посты (%)", min_value=0, max_value=100
        ),
        "tag_comment_chance": _parse_int_field(
            request, tag_comment_chance, default=50, label="Цитировать пост (%)", min_value=0, max_value=100
        ),
        "accounts_per_post_min": _parse_int_field(
            request, accounts_per_post_min, default=1, label="Аккаунтов на пост (мин)", min_value=0
        ),
        "accounts_per_post_max": _parse_int_field(
            request, accounts_per_post_max, default=50, label="Аккаунтов на пост (макс)", min_value=0
        ),
        "daily_comment_limit": _parse_int_field(
            request, daily_comment_limit, default=50, label="Лимит комментариев/сутки", min_value=0
        ),
        "ai_provider": ai_provider,
        "date_added": datetime.now(timezone.utc).isoformat(),
        "min_word_count": _parse_int_field(request, min_word_count, default=0, label="Мин. слов", min_value=0),
        "min_post_interval_mins": _parse_int_field(
            request, min_post_interval_mins, default=0, label="Мин. интервал (мин)", min_value=0
        ),
        "min_meaningful_words": _parse_int_field(
            request, min_meaningful_words, default=6, label="Мин. смысловых слов", min_value=0
        ),
        "media_min_meaningful_words": _parse_int_field(
            request, media_min_meaningful_words, default=12, label="Мин. слов для медиа", min_value=0
        ),
        "skip_promotional_posts": bool(skip_promotional_posts),
        "skip_short_media_posts": bool(skip_short_media_posts),
        "assigned_accounts": [],
        "project_id": project_id,
    }

    min_acc = int(new_target.get("accounts_per_post_min", 0) or 0)
    max_acc = int(new_target.get("accounts_per_post_max", 0) or 0)
    if max_acc < min_acc:
        _flash(request, "warning", "Диапазон аккаунтов на пост: максимум меньше минимума, исправлено.")
        new_target["accounts_per_post_max"] = min_acc

    settings.setdefault("targets", []).append(new_target)
    _save_settings(settings)
    _flash(request, "success", f"Цель комментирования добавлена: {chat_info.get('chat_name')}")
    return _redirect(f"/targets/{quote(chat_id)}")


@router.get("/targets/{chat_id}", response_class=HTMLResponse)
async def target_edit_page(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_target_by_chat_id(settings, chat_id, project_id)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)
    join_status = _load_join_status([target.get("chat_id"), target.get("linked_chat_id")])
    return templates.TemplateResponse(
        "target_edit.html",
        _template_context(request, target=target, accounts=accounts, join_status=join_status),
    )


@router.post("/targets/{chat_id}")
async def target_edit_save(
    request: Request,
    chat_id: str,
    ai_enabled: Optional[str] = Form(None),
    ai_provider: str = Form("default"),
    slow_join_interval_mins: str = Form(""),
    initial_comment_delay: str = Form(""),
    delay_between_accounts: str = Form(""),
    comment_chance: str = Form(""),
    tag_comment_chance: str = Form(""),
    accounts_per_post_min: str = Form(""),
    accounts_per_post_max: str = Form(""),
    daily_comment_limit: str = Form(""),
    min_word_count: str = Form(""),
    min_post_interval_mins: str = Form(""),
    min_meaningful_words: str = Form(""),
    media_min_meaningful_words: str = Form(""),
    skip_promotional_posts: Optional[str] = Form(None),
    skip_short_media_posts: Optional[str] = Form(None),
    reply_chance: str = Form(""),
    intervention_chance: str = Form(""),
    tag_reply_chance: str = Form(""),
    reply_delay_min: str = Form(""),
    reply_delay_max: str = Form(""),
    max_dialogue_depth: str = Form(""),
    max_dialogue_ai_replies: str = Form(""),
    select_all: Optional[str] = Form(None),
    assigned_accounts: Optional[List[str]] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, target = _find_target_by_chat_id(settings, chat_id, project_id)

    target["ai_enabled"] = bool(ai_enabled)
    target["ai_provider"] = ai_provider

    if slow_join_interval_mins.strip():
        target["slow_join_interval_mins"] = _parse_int_field(
            request,
            slow_join_interval_mins,
            default=int(target.get("slow_join_interval_mins", 0)),
            label="Медленное вступление (мин)",
            min_value=0,
        )

    if initial_comment_delay.strip():
        target["initial_comment_delay"] = _parse_int_field(
            request,
            initial_comment_delay,
            default=int(target.get("initial_comment_delay", 10)),
            label="Пауза после поста",
            min_value=0,
        )
    if delay_between_accounts.strip():
        target["delay_between_accounts"] = _parse_int_field(
            request,
            delay_between_accounts,
            default=int(target.get("delay_between_accounts", 10)),
            label="Пауза между аккаунтами",
            min_value=0,
        )
    if comment_chance.strip():
        target["comment_chance"] = _parse_int_field(
            request,
            comment_chance,
            default=int(target.get("comment_chance", 100)),
            label="Комментировать посты (%)",
            min_value=0,
            max_value=100,
        )
    if tag_comment_chance.strip():
        target["tag_comment_chance"] = _parse_int_field(
            request,
            tag_comment_chance,
            default=int(target.get("tag_comment_chance", 50)),
            label="Цитировать пост (%)",
            min_value=0,
            max_value=100,
        )
    if accounts_per_post_min.strip():
        target["accounts_per_post_min"] = _parse_int_field(
            request,
            accounts_per_post_min,
            default=int(target.get("accounts_per_post_min", 0)),
            label="Аккаунтов на пост (мин)",
            min_value=0,
        )
    if accounts_per_post_max.strip():
        target["accounts_per_post_max"] = _parse_int_field(
            request,
            accounts_per_post_max,
            default=int(target.get("accounts_per_post_max", 0)),
            label="Аккаунтов на пост (макс)",
            min_value=0,
        )
    if accounts_per_post_min.strip() or accounts_per_post_max.strip():
        min_acc = int(target.get("accounts_per_post_min", 0) or 0)
        max_acc = int(target.get("accounts_per_post_max", 0) or 0)
        if max_acc < min_acc:
            _flash(request, "warning", "Диапазон аккаунтов на пост: максимум меньше минимума, исправлено.")
            target["accounts_per_post_max"] = min_acc
    if "accounts_per_post" in target:
        target.pop("accounts_per_post", None)
    if daily_comment_limit.strip():
        target["daily_comment_limit"] = _parse_int_field(
            request,
            daily_comment_limit,
            default=int(target.get("daily_comment_limit", 50)),
            label="Лимит комментариев/сутки",
            min_value=0,
        )
    if min_word_count.strip():
        target["min_word_count"] = _parse_int_field(
            request, min_word_count, default=int(target.get("min_word_count", 0)), label="Мин. слов", min_value=0
        )
    if min_post_interval_mins.strip():
        target["min_post_interval_mins"] = _parse_int_field(
            request,
            min_post_interval_mins,
            default=int(target.get("min_post_interval_mins", 0)),
            label="Мин. интервал (мин)",
            min_value=0,
        )
    if min_meaningful_words.strip():
        target["min_meaningful_words"] = _parse_int_field(
            request,
            min_meaningful_words,
            default=int(target.get("min_meaningful_words", 2)),
            label="Мин. смысловых слов",
            min_value=0,
        )
    if media_min_meaningful_words.strip():
        target["media_min_meaningful_words"] = _parse_int_field(
            request,
            media_min_meaningful_words,
            default=int(target.get("media_min_meaningful_words", 6)),
            label="Мин. слов для медиа",
            min_value=0,
        )

    target["skip_promotional_posts"] = bool(skip_promotional_posts) if skip_promotional_posts is not None else False
    target["skip_short_media_posts"] = bool(skip_short_media_posts) if skip_short_media_posts is not None else False

    if reply_chance.strip():
        target["reply_chance"] = _parse_int_field(
            request, reply_chance, default=int(target.get("reply_chance", 0)), label="Шанс ответа", min_value=0, max_value=100
        )
    if intervention_chance.strip():
        target["intervention_chance"] = _parse_int_field(
            request,
            intervention_chance,
            default=int(target.get("intervention_chance", 30)),
            label="Шанс вмешательства",
            min_value=0,
            max_value=100,
        )
    if tag_reply_chance.strip():
        target["tag_reply_chance"] = _parse_int_field(
            request,
            tag_reply_chance,
            default=int(target.get("tag_reply_chance", 50)),
            label="Шанс Reply-тега",
            min_value=0,
            max_value=100,
        )
    if reply_delay_min.strip():
        target["reply_delay_min"] = _parse_int_field(
            request,
            reply_delay_min,
            default=int(target.get("reply_delay_min", 30)),
            label="Мин. задержка (сек)",
            min_value=0,
        )
    if reply_delay_max.strip():
        target["reply_delay_max"] = _parse_int_field(
            request,
            reply_delay_max,
            default=int(target.get("reply_delay_max", 120)),
            label="Макс. задержка (сек)",
            min_value=0,
        )
    if max_dialogue_depth.strip():
        target["max_dialogue_depth"] = _parse_int_field(
            request,
            max_dialogue_depth,
            default=int(target.get("max_dialogue_depth", 10)),
            label="Глубина контекста",
            min_value=1,
        )
    if max_dialogue_ai_replies.strip():
        target["max_dialogue_ai_replies"] = _parse_int_field(
            request,
            max_dialogue_ai_replies,
            default=int(target.get("max_dialogue_ai_replies", 2)),
            label="Лимит ответов ИИ",
            min_value=0,
        )

    accounts, _ = _load_accounts()
    allowed_sessions = [
        a.get("session_name")
        for a in _filter_accounts_by_project(accounts, project_id)
        if a.get("session_name")
    ]
    allowed_set = set(allowed_sessions)
    if select_all is not None:
        target["assigned_accounts"] = allowed_sessions
    elif assigned_accounts is None:
        target["assigned_accounts"] = []
    else:
        target["assigned_accounts"] = [s for s in list(assigned_accounts) if s in allowed_set]

    settings["targets"][idx] = target
    _save_settings(settings)
    _flash(request, "success", "Настройки чата обновлены.")
    return _redirect(f"/targets/{quote(chat_id)}")


@router.post("/targets/{chat_id}/join")
async def target_join_attempt(
    request: Request,
    chat_id: str,
    session_name: str = Form(""),
    target_id: str = Form(""),
    auto_pause: Optional[str] = Form(None),
):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_target_by_chat_id(settings, chat_id, project_id)
    accounts, _ = _load_accounts()
    accounts = _filter_accounts_by_project(accounts, project_id)

    if session_name:
        accounts = [a for a in accounts if a.get("session_name") == session_name]
    else:
        assigned = target.get("assigned_accounts") or []
        if assigned:
            accounts = [a for a in accounts if a.get("session_name") in assigned]
        else:
            accounts = []

    if not accounts:
        _flash(request, "warning", "Нет выбранных аккаунтов для вступления.")
        return _redirect(f"/targets/{quote(chat_id)}")

    auto_pause_flag = _parse_bool(auto_pause, default=True)
    api_id_default, api_hash_default = _telethon_credentials()
    target_ids: List[str] = []
    if target_id:
        target_ids = [str(target_id)]
    else:
        if target.get("chat_id"):
            target_ids.append(str(target.get("chat_id")))
        if target.get("linked_chat_id"):
            target_ids.append(str(target.get("linked_chat_id")))

    total_joined = 0
    total_failed = 0
    had_lock = False

    async with _auto_pause_commentator(request, auto_pause=auto_pause_flag, reason="Вступление в чат"):
        await _refresh_target_access_hashes(target, settings)
        for acc in accounts:
            account_success = True
            session = _resolve_account_session(acc)
            if not session:
                continue
            api_id, api_hash = _resolve_account_credentials(acc, api_id_default, api_hash_default)
            proxy_tuple = _resolve_account_proxy(acc)
            client = TelegramClient(
                session,
                api_id,
                api_hash,
                proxy=proxy_tuple,
                **device_kwargs(acc),
            )
            try:
                try:
                    await client.connect()
                except Exception as exc:
                    if "locked" in str(exc).lower():
                        had_lock = True
                        for t_id in target_ids:
                            _update_join_status(
                                acc.get("session_name", ""),
                                t_id,
                                "failed",
                                last_error="session_db_locked",
                                last_method="connect",
                            )
                            total_failed += 1
                        continue
                    raise
                if not await client.is_user_authorized():
                    _update_join_status(
                        acc.get("session_name", ""),
                        str(target.get("chat_id")),
                        "failed",
                        last_error="unauthorized",
                        last_method="auth",
                    )
                    _record_account_failure(
                        acc.get("session_name", ""),
                        "join",
                        last_error="unauthorized",
                        last_target=str(target.get("chat_id")),
                    )
                    total_failed += 1
                    account_success = False
                    continue
                for t_id in target_ids:
                    joined, last_error, last_method = await _attempt_join_target(
                        client, acc.get("session_name", ""), target, t_id
                    )
                    if joined:
                        _update_join_status(acc.get("session_name", ""), t_id, "joined")
                        total_joined += 1
                    else:
                        _update_join_status(
                            acc.get("session_name", ""), t_id, "failed", last_error=last_error, last_method=last_method
                        )
                        _record_account_failure(
                            acc.get("session_name", ""),
                            "join",
                            last_error=str(last_error) if last_error else None,
                            last_target=str(t_id),
                        )
                        account_success = False
                        total_failed += 1
            except Exception as exc:
                if "locked" in str(exc).lower():
                    had_lock = True
                    for t_id in target_ids:
                        _update_join_status(
                            acc.get("session_name", ""),
                            t_id,
                            "failed",
                            last_error="session_db_locked",
                            last_method="connect",
                        )
                        _record_account_failure(
                            acc.get("session_name", ""),
                            "join",
                            last_error="session_db_locked",
                            last_target=str(t_id),
                        )
                        total_failed += 1
                        account_success = False
                else:
                    for t_id in target_ids:
                        _update_join_status(
                            acc.get("session_name", ""),
                            t_id,
                            "failed",
                            last_error=str(exc),
                            last_method="connect",
                        )
                        _record_account_failure(
                            acc.get("session_name", ""),
                            "join",
                            last_error=str(exc),
                            last_target=str(t_id),
                        )
                        total_failed += 1
                        account_success = False
            finally:
                try:
                    if client.is_connected():
                        await client.disconnect()
                except Exception:
                    had_lock = True
                except Exception:
                    pass
            if account_success:
                _clear_account_failure(acc.get("session_name", ""), "join")

    if had_lock:
        _flash(
            request,
            "warning",
            "Сессия занята другим процессом (database is locked). "
            "Остановите commentator и повторите попытку.",
        )
    else:
        _flash(request, "success", f"Вступление: OK={total_joined}, failed={total_failed}")
    return _redirect(f"/targets/{quote(chat_id)}")


@router.post("/targets/{chat_id}/delete")
async def target_delete(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    settings["targets"].pop(idx)
    _save_settings(settings)
    _flash(request, "success", "Цель удалена.")
    return _redirect("/targets")


# ---------------------------------------------------------------------------
# Prompts, Scenarios, Triggers
# ---------------------------------------------------------------------------


@router.get("/targets/{chat_id}/prompts", response_class=HTMLResponse)
async def target_prompts_page(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    _flash(request, "info", "Промпты чата отключены. Используйте раздел «Роли».")
    return _redirect(f"/targets/{quote(chat_id)}")


@router.post("/targets/{chat_id}/prompts")
async def target_prompts_save(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    _flash(request, "info", "Промпты чата отключены. Роль задаётся на уровне аккаунта.")
    return _redirect(f"/targets/{quote(chat_id)}")


@router.get("/targets/{chat_id}/scenario", response_class=HTMLResponse)
async def target_scenario_page(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_target_by_chat_id(settings, chat_id, project_id)

    with _db_connect() as conn:
        row = conn.execute(
            "SELECT script_content, current_index, status FROM scenarios WHERE chat_id = ?",
            (chat_id,),
        ).fetchone()

    scenario = dict(row) if row else {"script_content": "", "current_index": 0, "status": "stopped"}
    return templates.TemplateResponse(
        "target_scenario.html",
        _template_context(request, target=target, scenario=scenario),
    )


@router.post("/targets/{chat_id}/scenario/save")
async def target_scenario_save(request: Request, chat_id: str, script_content: str = Form("")):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)

    script_content = script_content.strip()
    if not script_content:
        _flash(request, "warning", "Сценарий пустой — нечего сохранять.")
        return _redirect(f"/targets/{quote(chat_id)}/scenario")

    with _db_connect() as conn:
        conn.execute(
            """
            INSERT INTO scenarios (chat_id, script_content, current_index, status)
            VALUES (?, ?, 0, 'stopped')
            ON CONFLICT(chat_id) DO UPDATE SET
                script_content=excluded.script_content,
                current_index=0,
                status='stopped'
            """,
            (chat_id, script_content),
        )
        conn.commit()

    _flash(request, "success", "Сценарий сохранён (статус: остановлен, прогресс сброшен).")
    return _redirect(f"/targets/{quote(chat_id)}/scenario")


@router.post("/targets/{chat_id}/scenario/toggle")
async def target_scenario_toggle(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    with _db_connect() as conn:
        row = conn.execute("SELECT status FROM scenarios WHERE chat_id = ?", (chat_id,)).fetchone()
        current = row["status"] if row else "stopped"
        new_status = "stopped" if current == "running" else "running"
        conn.execute("UPDATE scenarios SET status = ? WHERE chat_id = ?", (new_status, chat_id))
        conn.commit()
    _flash(request, "success", f"Сценарий: {new_status}")
    return _redirect(f"/targets/{quote(chat_id)}/scenario")


@router.post("/targets/{chat_id}/scenario/reset")
async def target_scenario_reset(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    with _db_connect() as conn:
        conn.execute("UPDATE scenarios SET current_index = 0 WHERE chat_id = ?", (chat_id,))
        conn.commit()
    _flash(request, "success", "Прогресс сценария сброшен в 0.")
    return _redirect(f"/targets/{quote(chat_id)}/scenario")


@router.post("/targets/{chat_id}/scenario/toggle-reply")
async def target_scenario_toggle_reply(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    idx, target = _find_target_by_chat_id(settings, chat_id, project_id)
    target["scenario_reply_mode"] = not bool(target.get("scenario_reply_mode", False))
    settings["targets"][idx] = target
    _save_settings(settings)
    _flash(request, "success", f"Связный Reply: {'вкл' if target['scenario_reply_mode'] else 'выкл'}")
    return _redirect(f"/targets/{quote(chat_id)}/scenario")


@router.get("/targets/{chat_id}/triggers", response_class=HTMLResponse)
async def target_triggers_page(request: Request, chat_id: str):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, target = _find_target_by_chat_id(settings, chat_id, project_id)
    with _db_connect() as conn:
        triggers = conn.execute(
            "SELECT id, trigger_phrase, answer_text FROM triggers WHERE chat_id = ? ORDER BY id DESC",
            (chat_id,),
        ).fetchall()
    return templates.TemplateResponse(
        "target_triggers.html",
        _template_context(request, target=target, triggers=triggers),
    )


@router.post("/targets/{chat_id}/triggers/add")
async def target_triggers_add(
    request: Request,
    chat_id: str,
    trigger_phrase: str = Form(...),
    answer_text: str = Form(...),
):
    trigger_phrase = trigger_phrase.strip().lower()
    answer_text = answer_text.strip()
    if not trigger_phrase or not answer_text:
        raise HTTPException(status_code=400, detail="Нужно указать фразу и ответ")

    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    with _db_connect() as conn:
        conn.execute(
            "INSERT INTO triggers (chat_id, trigger_phrase, answer_text) VALUES (?, ?, ?)",
            (chat_id, trigger_phrase, answer_text),
        )
        conn.commit()
    _flash(request, "success", f"Триггер добавлен: {trigger_phrase}")
    return _redirect(f"/targets/{quote(chat_id)}/triggers")


@router.post("/targets/{chat_id}/triggers/{trigger_id}/delete")
async def target_triggers_delete(request: Request, chat_id: str, trigger_id: int):
    settings, _ = _load_settings()
    project_id = _active_project_id(settings)
    _, _ = _find_target_by_chat_id(settings, chat_id, project_id)
    with _db_connect() as conn:
        conn.execute("DELETE FROM triggers WHERE id = ?", (trigger_id,))
        conn.commit()
    _flash(request, "success", "Триггер удалён.")
    return _redirect(f"/targets/{quote(chat_id)}/triggers")

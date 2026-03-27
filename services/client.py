"""Telegram client wrapper and client lifecycle management.

Extracted from commentator.py.
"""

import asyncio
import logging

from telethon import TelegramClient, events
from telethon.tl import types as tl_types

from services.account_utils import (
    load_project_accounts,
    _resolve_account_credentials,
    _resolve_account_session,
    _resolve_account_proxy,
    is_bot_awake,
)
from services.antispam import check_and_handle_spam
from services.commenting import process_new_post
from services.connection import (
    _connect_backoff_ready,
    _schedule_connect_backoff,
    ensure_client_connected,
    _mark_account_unavailable,
    _is_account_active,
    _is_account_assigned,
    _extract_discussion_seed,
    _extract_discussion_seed_optional_prefix,
    CLIENT_CONNECT_STATE,
)
from services.dialogue import get_all_our_user_ids
from services.discussions import schedule_discussion_run
from services.inbox import (
    log_inbox_message_to_db,
    _message_text_preview,
    _peer_chat_id,
    _reaction_summary_from_update,
    _store_message_reaction_event,
    _queued_outgoing_exists,
)
from services.joining import ensure_account_joined
from services.monitoring import process_post_for_monitoring
from services.post_utils import catch_up_missed_posts
from services.project import (
    get_project_targets,
    get_project_discussion_targets,
    get_project_reaction_targets,
    get_project_monitor_targets,
    ensure_discussion_targets_schema,
)
from services.reactions import process_new_post_for_reaction
from services.replies import process_reply_to_comment
from services.telegram_bot import (
    build_inbox_dm_notification,
    build_inbox_reply_notification,
    build_reaction_notification,
    notify_event,
    resolve_project_id_for_session,
)
from services.triggers import process_trigger
from tg_device import device_kwargs

from role_engine import ensure_role_schema

logger = logging.getLogger(__name__)


def _db_connect():
    from db.connection import get_connection
    return get_connection()


async def _safe_notify_event(event_type: str, project_id: str, message_text: str, settings: dict) -> None:
    try:
        await notify_event(event_type, project_id, message_text, settings=settings)
    except Exception as exc:
        logger.warning("Telegram bot notify_event failed: event=%s project_id=%s error=%s", event_type, project_id, exc)


def _schedule_notify_event(event_type: str, project_id: str, message_text: str, settings: dict) -> None:
    try:
        asyncio.create_task(_safe_notify_event(event_type, project_id, message_text, settings))
    except Exception as exc:
        logger.warning("Telegram bot notification scheduling failed: event=%s project_id=%s error=%s", event_type, project_id, exc)


class CommentatorClient:
    """Wrapper around TelegramClient with event handling for commenting bot."""

    def __init__(self, account_data, api_id, api_hash, *, shared_state: dict):
        """Initialize client.

        Parameters
        ----------
        shared_state : dict
            Dictionary of shared mutable state objects. Keys:
            - event_handler_lock, current_settings_ref, active_clients,
            - handled_posts_for_comments, handled_posts_for_reactions,
            - handled_posts_for_monitoring, handled_grouped_ids,
            - reply_process_cache, discussion_start_suppress_chat_ids,
            - discussion_active_tasks, discussion_start_cache,
            - discussion_start_cache_order, discussion_start_cache_max,
            - pending_tasks, scenario_context, processing_cache,
            - post_process_cache, post_process_cache_order, post_process_cache_max,
            - channel_last_post_time, monitor_channel_last_post_time,
            - recent_generated_messages, joined_cache
        """
        self.session_name = account_data['session_name']
        self.session_string = account_data.get('session_string')
        self.session_file = account_data.get("session_file") or account_data.get("session_path")
        self.api_id, self.api_hash = _resolve_account_credentials(account_data, api_id, api_hash)
        self.proxy = _resolve_account_proxy(account_data)
        self.client = None
        self._init_error = None
        self._shared = shared_state

        if not self.api_id or not self.api_hash:
            self._init_error = "missing_api_credentials"
            return

        session = _resolve_account_session(account_data)
        if not session:
            self._init_error = "missing_session"
            return

        try:
            self.client = TelegramClient(
                session,
                self.api_id,
                self.api_hash,
                proxy=self.proxy,
                **device_kwargs(account_data),
            )
        except Exception as e:
            self._init_error = f"init_error:{e}"

    def _parse_proxy(self, url):
        try:
            protocol, rest = url.split('://')
            auth, addr = rest.split('@')
            user, password = auth.split(':')
            host, port = addr.split(':')
            return (protocol, host, int(port), True, user, password)
        except Exception:
            return None

    async def start(self):
        try:
            if not self.client:
                if self._init_error:
                    logger.error(f"Ошибка инициализации {self.session_name}: {self._init_error}")
                return False
            await self.client.connect()
            if not await self.client.is_user_authorized():
                self._init_error = "unauthorized"
                return False
            me = await self.client.get_me()
            self.user_id = me.id
            self.client.add_event_handler(self.event_handler, events.NewMessage)
            self.client.add_event_handler(
                self.reaction_event_handler,
                events.Raw(types=(tl_types.UpdateMessageReactions, tl_types.UpdateBotMessageReaction, tl_types.UpdateBotMessageReactions)),
            )
            return True
        except Exception as e:
            logger.error(f"Ошибка подключения {self.session_name}: {e}")
            return False

    async def stop(self):
        if self.client.is_connected():
            await self.client.disconnect()

    async def reaction_event_handler(self, update):
        try:
            current_settings = self._shared["current_settings_ref"]()
            peer = getattr(update, "peer", None)
            msg_id = int(getattr(update, "msg_id", 0) or 0)
            chat_id = _peer_chat_id(peer)
            if not chat_id or msg_id <= 0:
                return

            summary = _reaction_summary_from_update(update)
            entity = await self.client.get_entity(peer)
            msg = await self.client.get_messages(entity, ids=msg_id)
            if isinstance(msg, list):
                msg = msg[0] if msg else None
            if not msg:
                return

            if getattr(msg, "sender_id", None) != getattr(self, "user_id", None):
                return

            kind = "dm" if isinstance(peer, tl_types.PeerUser) else "quote"
            chat_username = getattr(entity, "username", None)
            if isinstance(peer, tl_types.PeerUser):
                first_name = getattr(entity, "first_name", "") or ""
                last_name = getattr(entity, "last_name", "") or ""
                chat_title = (f"{first_name} {last_name}").strip() or chat_username or chat_id
            else:
                chat_title = getattr(entity, "title", None) or chat_username or chat_id

            _store_message_reaction_event(
                session_name=self.session_name,
                chat_id=str(chat_id),
                msg_id=msg_id,
                kind=kind,
                text=_message_text_preview(msg),
                chat_title=chat_title,
                chat_username=chat_username,
                reactions_summary=summary,
            )

            project_id = resolve_project_id_for_session(self.session_name, current_settings)
            _schedule_notify_event(
                "inbox_reactions",
                project_id,
                build_reaction_notification(
                    session_name=self.session_name,
                    chat_title=chat_title,
                    summary=summary,
                ),
                current_settings,
            )
        except Exception:
            return

    async def event_handler(self, event):
        s = self._shared
        current_settings = s["current_settings_ref"]()
        active_clients = s["active_clients"]

        async with s["event_handler_lock"]:
            try:
                event_chat_id = int(str(event.chat_id).replace('-100', ''))
            except:
                event_chat_id = event.chat_id

            msg_id = event.message.id
            sender_id = event.sender_id

            is_channel_post = event.message.post or (event.message.fwd_from and event.message.fwd_from.channel_post)

            try:
                our_ids = get_all_our_user_ids(active_clients=active_clients, current_settings=current_settings)
            except Exception:
                our_ids = set()

            # -----------------------------------------------------------------
            # Anti-spam: handle external user messages in linked discussion groups
            # before any other processing (inbox logging / triggers / replies).
            # -----------------------------------------------------------------
            try:
                if (
                    sender_id
                    and not event.out
                    and not event.is_private
                    and event.is_group
                    and not is_channel_post
                ):
                    spam_blocked = s.get("spam_blocked_msgs")
                    spam_blocked_order = s.get("spam_blocked_msgs_order")
                    spam_blocked_max = s.get("spam_blocked_msgs_max")
                    is_spam = await check_and_handle_spam(
                        event,
                        active_clients=active_clients,
                        current_settings=current_settings,
                        spam_blocked_msgs=spam_blocked if isinstance(spam_blocked, set) else None,
                        spam_blocked_msgs_order=spam_blocked_order,
                        spam_blocked_msgs_max=int(spam_blocked_max or 0) if spam_blocked_max is not None else 0,
                    )
                    if is_spam:
                        return
            except Exception:
                pass

            if event.out:
                try:
                    text = event.message.text or ""
                    if not text:
                        try:
                            if getattr(event.message, "photo", None):
                                text = "[фото]"
                            elif getattr(event.message, "video", None) or getattr(event.message, "gif", None):
                                text = "[видео]"
                            elif getattr(event.message, "voice", None):
                                text = "[голосовое]"
                            elif getattr(event.message, "audio", None):
                                text = "[аудио]"
                            elif getattr(event.message, "document", None) or getattr(event.message, "file", None):
                                text = "[файл]"
                        except Exception:
                            pass

                    if event.is_private:
                        if _queued_outgoing_exists(
                            kind="dm",
                            session_name=self.session_name,
                            chat_id=str(event.chat_id),
                            text=text,
                            reply_to_msg_id=getattr(event.message, "reply_to_msg_id", None),
                        ):
                            return

                        chat = None
                        try:
                            chat = await event.get_chat()
                        except Exception:
                            chat = None
                        chat_username = getattr(chat, "username", "") if chat else ""
                        chat_title = ""
                        if chat:
                            fn = getattr(chat, "first_name", "") or ""
                            ln = getattr(chat, "last_name", "") or ""
                            chat_title = (f"{fn} {ln}").strip() or (chat_username or "")

                        log_inbox_message_to_db(
                            kind="dm",
                            direction="out",
                            status="sent",
                            session_name=self.session_name,
                            chat_id=str(event.chat_id),
                            msg_id=msg_id,
                            reply_to_msg_id=getattr(event.message, "reply_to_msg_id", None),
                            sender_id=getattr(self, "user_id", None),
                            chat_title=chat_title or None,
                            chat_username=chat_username or None,
                            text=text,
                            is_read=1,
                        )

                    elif event.is_reply:
                        reply_id = getattr(event.message, "reply_to_msg_id", None)
                        if reply_id:
                            with _db_connect() as conn:
                                found = conn.execute(
                                    """
                                    SELECT 1 FROM inbox_messages
                                    WHERE kind='quote' AND direction='in'
                                      AND session_name=? AND chat_id=? AND msg_id=?
                                    LIMIT 1
                                    """,
                                    (self.session_name, str(event.chat_id), reply_id),
                                ).fetchone()
                            if found:
                                if _queued_outgoing_exists(
                                    kind="quote",
                                    session_name=self.session_name,
                                    chat_id=str(event.chat_id),
                                    text=text,
                                    reply_to_msg_id=reply_id,
                                ):
                                    return

                                chat = None
                                try:
                                    chat = await event.get_chat()
                                except Exception:
                                    chat = None
                                chat_title = getattr(chat, "title", "") if chat else ""
                                chat_username = getattr(chat, "username", "") if chat else ""

                                reply_msg = None
                                try:
                                    reply_msg = await event.get_reply_message()
                                except Exception:
                                    reply_msg = None

                                replied_to_text = ""
                                if reply_msg:
                                    replied_to_text = (
                                        getattr(reply_msg, "text", None)
                                        or getattr(reply_msg, "message", None)
                                        or ""
                                    )

                                log_inbox_message_to_db(
                                    kind="quote",
                                    direction="out",
                                    status="sent",
                                    session_name=self.session_name,
                                    chat_id=str(event.chat_id),
                                    msg_id=msg_id,
                                    reply_to_msg_id=reply_id,
                                    sender_id=getattr(self, "user_id", None),
                                    chat_title=(chat_title or "").strip() or None,
                                    chat_username=(chat_username or "").strip() or None,
                                    text=text,
                                    replied_to_text=replied_to_text or None,
                                    is_read=1,
                                )
                except Exception:
                    pass
                return

            if sender_id and sender_id in our_ids:
                our_ids = our_ids  # keep for later logic
            try:
                if event.is_private and sender_id and sender_id not in our_ids:
                    sender = None
                    try:
                        sender = await event.get_sender()
                    except Exception:
                        sender = None
                    sender_username = getattr(sender, "username", "") if sender else ""
                    sender_name = ""
                    if sender:
                        fn = getattr(sender, "first_name", "") or ""
                        ln = getattr(sender, "last_name", "") or ""
                        sender_name = (f"{fn} {ln}").strip() or (sender_username or "")
                    if not sender_name and sender_id:
                        sender_name = str(sender_id)

                    text = event.message.text or ""
                    if not text:
                        try:
                            if getattr(event.message, "photo", None):
                                text = "[фото]"
                            elif getattr(event.message, "video", None) or getattr(event.message, "gif", None):
                                text = "[видео]"
                            elif getattr(event.message, "voice", None):
                                text = "[голосовое]"
                            elif getattr(event.message, "audio", None):
                                text = "[аудио]"
                            elif getattr(event.message, "document", None) or getattr(event.message, "file", None):
                                text = "[файл]"
                        except Exception:
                            pass

                    saved_dm_id = log_inbox_message_to_db(
                        kind="dm",
                        direction="in",
                        status="received",
                        session_name=self.session_name,
                        chat_id=str(event.chat_id),
                        msg_id=msg_id,
                        sender_id=sender_id,
                        sender_username=sender_username,
                        sender_name=sender_name,
                        chat_title=sender_name,
                        chat_username=sender_username,
                        text=text,
                        is_read=0,
                    )
                    if saved_dm_id is not None:
                        project_id = resolve_project_id_for_session(self.session_name, current_settings)
                        _schedule_notify_event(
                            "inbox_dm",
                            project_id,
                            build_inbox_dm_notification(
                                session_name=self.session_name,
                                sender_name=sender_name,
                                sender_username=sender_username,
                                text=text,
                            ),
                            current_settings,
                        )

                if event.is_reply and sender_id and sender_id not in our_ids and not event.is_private:
                    reply_msg = None
                    try:
                        reply_msg = await event.get_reply_message()
                    except Exception:
                        reply_msg = None

                    if reply_msg and getattr(reply_msg, "sender_id", None) == getattr(self, "user_id", None):
                        sender = None
                        try:
                            sender = await event.get_sender()
                        except Exception:
                            sender = None
                        sender_username = getattr(sender, "username", "") if sender else ""
                        sender_name = ""
                        if sender:
                            fn = getattr(sender, "first_name", "") or ""
                            ln = getattr(sender, "last_name", "") or ""
                            sender_name = (f"{fn} {ln}").strip() or (sender_username or "")
                        if not sender_name and sender_id:
                            sender_name = str(sender_id)

                        chat = None
                        try:
                            chat = await event.get_chat()
                        except Exception:
                            chat = None
                        chat_title = getattr(chat, "title", "") if chat else ""
                        chat_username = getattr(chat, "username", "") if chat else ""

                        text = event.message.text or ""
                        if not text:
                            try:
                                if getattr(event.message, "photo", None):
                                    text = "[фото]"
                                elif getattr(event.message, "video", None) or getattr(event.message, "gif", None):
                                    text = "[видео]"
                                elif getattr(event.message, "voice", None):
                                    text = "[голосовое]"
                                elif getattr(event.message, "audio", None):
                                    text = "[аудио]"
                                elif getattr(event.message, "document", None) or getattr(event.message, "file", None):
                                    text = "[файл]"
                            except Exception:
                                pass

                        replied_to_text = getattr(reply_msg, "text", None) or getattr(reply_msg, "message", None) or ""
                        saved_quote_id = log_inbox_message_to_db(
                            kind="quote",
                            direction="in",
                            status="received",
                            session_name=self.session_name,
                            chat_id=str(event.chat_id),
                            msg_id=msg_id,
                            reply_to_msg_id=getattr(event.message, "reply_to_msg_id", None),
                            sender_id=sender_id,
                            sender_username=sender_username,
                            sender_name=sender_name,
                            chat_title=chat_title,
                            chat_username=chat_username,
                            text=text,
                            replied_to_text=replied_to_text,
                            is_read=0,
                        )
                        if saved_quote_id is not None:
                            project_id = resolve_project_id_for_session(self.session_name, current_settings)
                            _schedule_notify_event(
                                "inbox_replies",
                                project_id,
                                build_inbox_reply_notification(
                                    session_name=self.session_name,
                                    chat_title=chat_title,
                                    sender_name=sender_name,
                                    text=text,
                                ),
                                current_settings,
                            )
            except Exception:
                pass

            found_target = None
            for t in get_project_targets(current_settings):
                t_linked = int(str(t.get('linked_chat_id', 0)).replace('-100', ''))
                t_main = int(str(t.get('chat_id', 0)).replace('-100', ''))
                if event_chat_id == t_linked or event_chat_id == t_main:
                    found_target = t
                    break

            discussion_targets = []
            for t in get_project_discussion_targets(current_settings):
                try:
                    t_linked = int(str(t.get("linked_chat_id", 0)).replace("-100", ""))
                    t_main = int(str(t.get("chat_id", 0)).replace("-100", ""))
                except Exception:
                    continue
                if event_chat_id == t_linked or event_chat_id == t_main:
                    discussion_targets.append(t)

            if discussion_targets and (not event.message.fwd_from) and event.is_group:
                if event.out and event_chat_id in s["discussion_start_suppress_chat_ids"]:
                    discussion_targets = []

                if discussion_targets and event.out:
                    msg_text = getattr(event.message, "text", None) or ""
                    raw = str(msg_text or "").strip()
                    matches: list[dict] = []
                    for t in discussion_targets:
                        if not bool(t.get("enabled", True)):
                            continue
                        operator_session = str(t.get("operator_session_name") or "").strip()
                        if not operator_session or operator_session != self.session_name:
                            continue

                        start_prefix = str(t.get("start_prefix") or "")
                        start_on_operator_message = bool(t.get("start_on_operator_message", False))

                        seed: str | None = None
                        if start_on_operator_message:
                            if event.is_reply:
                                seed = _extract_discussion_seed(raw, start_prefix) if start_prefix else None
                            else:
                                seed = _extract_discussion_seed_optional_prefix(raw, start_prefix)
                        else:
                            seed = _extract_discussion_seed(raw, start_prefix)
                        if not seed:
                            continue

                        explicit_prefix = bool(start_prefix and raw.startswith(start_prefix))
                        matches.append(
                            {
                                "target": t,
                                "seed": seed,
                                "start_prefix": start_prefix,
                                "explicit_prefix": explicit_prefix,
                            }
                        )

                    chosen = None
                    if matches:
                        explicit = [m for m in matches if m.get("explicit_prefix")]
                        if explicit:
                            explicit.sort(key=lambda m: len(str(m.get("start_prefix") or "")), reverse=True)
                            best_len = len(str(explicit[0].get("start_prefix") or ""))
                            tied = [m for m in explicit if len(str(m.get("start_prefix") or "")) == best_len]
                            if len(tied) == 1:
                                chosen = tied[0]
                            else:
                                ids = [
                                    str(m.get("target", {}).get("id") or m.get("target", {}).get("chat_id"))
                                    for m in tied
                                ]
                                logger.warning(
                                    f"⚠️ [discussion] неоднозначный старт по префиксу в чате {event_chat_id}: {ids}"
                                )
                        else:
                            if len(matches) == 1:
                                chosen = matches[0]
                            else:
                                ids = [
                                    str(m.get("target", {}).get("id") or m.get("target", {}).get("chat_id"))
                                    for m in matches
                                ]
                                logger.warning(
                                    f"⚠️ [discussion] неоднозначный старт без префикса в чате {event_chat_id}: {ids}"
                                )

                    if chosen:
                        schedule_discussion_run(
                            chat_bare_id=event_chat_id,
                            chat_id=event.chat_id,
                            seed_msg_id=msg_id,
                            seed_text=str(chosen.get("seed") or "").strip(),
                            target=chosen.get("target") or {},
                            active_clients=active_clients,
                            current_settings=current_settings,
                            discussion_active_tasks=s["discussion_active_tasks"],
                            discussion_start_cache=s["discussion_start_cache"],
                            discussion_start_cache_order=s["discussion_start_cache_order"],
                            discussion_start_cache_max=s["discussion_start_cache_max"],
                            reply_process_cache=s["reply_process_cache"],
                            pending_tasks=s["pending_tasks"],
                            discussion_start_suppress_chat_ids=s["discussion_start_suppress_chat_ids"],
                            recent_generated_messages=s["recent_generated_messages"],
                            spam_blocked_msgs=s.get("spam_blocked_msgs"),
                        )

            if found_target:
                our_ids = get_all_our_user_ids(active_clients=active_clients, current_settings=current_settings)

                if sender_id and sender_id not in our_ids:
                    asyncio.create_task(process_trigger(
                        event, found_target, our_ids,
                        active_clients=active_clients,
                        current_settings=current_settings,
                        reply_process_cache=s["reply_process_cache"],
                        spam_blocked_msgs=s.get("spam_blocked_msgs"),
                    ))

                    if (event.is_reply or event.is_private) and msg_id not in s["reply_process_cache"]:
                        is_reply_to_us = False
                        if event.is_reply:
                            reply_msg = await event.get_reply_message()
                            if reply_msg and reply_msg.sender_id == self.user_id:
                                is_reply_to_us = True
                        elif event.is_private:
                            is_reply_to_us = True

                        if is_reply_to_us:
                            s["reply_process_cache"].add(msg_id)

                if is_channel_post:
                    if event.message.grouped_id:
                        if event.message.grouped_id in s["handled_grouped_ids"]: return
                        s["handled_grouped_ids"].append(event.message.grouped_id)

                    unique_id = f"{event_chat_id}_{msg_id}"

                    t_linked_check = int(str(found_target.get('linked_chat_id', 0)).replace('-100', ''))
                    t_main_check = int(str(found_target.get('chat_id', 0)).replace('-100', ''))

                    if not (t_linked_check and t_linked_check != t_main_check and event_chat_id == t_main_check):
                        if unique_id not in s["handled_posts_for_comments"]:
                            s["handled_posts_for_comments"].append(unique_id)
                            asyncio.create_task(process_new_post(
                                event, found_target,
                                active_clients=active_clients,
                                current_settings=current_settings,
                                pending_tasks=s["pending_tasks"],
                                scenario_context=s["scenario_context"],
                                processing_cache=s["processing_cache"],
                                post_process_cache=s["post_process_cache"],
                                post_process_cache_order=s["post_process_cache_order"],
                                post_process_cache_max=s["post_process_cache_max"],
                                spam_blocked_msgs=s.get("spam_blocked_msgs"),
                                channel_last_post_time=s["channel_last_post_time"],
                                recent_generated_messages=s["recent_generated_messages"],
                            ))


            if is_channel_post:
                unique_id = f"{event_chat_id}_{msg_id}"
                for r_target in get_project_reaction_targets(current_settings):
                    try:
                        if event_chat_id != int(str(r_target.get("chat_id", 0)).replace("-100", "")):
                            continue
                    except Exception:
                        continue
                    if unique_id not in s["handled_posts_for_reactions"]:
                        s["handled_posts_for_reactions"].append(unique_id)
                        asyncio.create_task(
                            process_new_post_for_reaction(event.input_chat, msg_id, r_target, message=event.message, active_clients=active_clients, pending_tasks=s["pending_tasks"], current_settings=current_settings)
                        )

            if not event.message.fwd_from and event.is_group and found_target:
                if found_target.get('ai_enabled', True) and found_target.get('reply_chance', 0) > 0:
                    asyncio.create_task(process_reply_to_comment(
                        event, found_target,
                        active_clients=active_clients,
                        current_settings=current_settings,
                        reply_process_cache=s["reply_process_cache"],
                        pending_tasks=s["pending_tasks"],
                        recent_generated_messages=s["recent_generated_messages"],
                        spam_blocked_msgs=s.get("spam_blocked_msgs"),
                    ))

            if event.is_channel and not event.message.fwd_from:
                for m_t in get_project_monitor_targets(current_settings):
                    if int(str(m_t.get('chat_id', 0)).replace('-100', '')) == event_chat_id:
                        if event.message.grouped_id and event.message.grouped_id in s["handled_grouped_ids"]: return
                        if event.message.grouped_id: s["handled_grouped_ids"].append(event.message.grouped_id)

                        unique_mon_id = f"mon_{event_chat_id}_{msg_id}"
                        if unique_mon_id not in s["handled_posts_for_monitoring"]:
                            s["handled_posts_for_monitoring"].append(unique_mon_id)
                            asyncio.create_task(process_post_for_monitoring(event, m_t, active_clients=active_clients, monitor_channel_last_post_time=s["monitor_channel_last_post_time"], pending_tasks=s["pending_tasks"], current_settings=current_settings))


async def manage_clients(api_id, api_hash, *, shared_state: dict):
    """Manage client lifecycle: start, reconnect, join channels, catch-up.

    Parameters
    ----------
    shared_state : dict
        Same shared_state dict as CommentatorClient.__init__.
    """
    s = shared_state
    current_settings = s["current_settings_ref"]()
    active_clients = s["active_clients"]
    client_catch_up_status = s["client_catch_up_status"]

    from app_storage import load_json
    from app_paths import SETTINGS_FILE

    current_settings_loaded = load_json(SETTINGS_FILE, {})
    if not isinstance(current_settings_loaded, dict):
        current_settings_loaded = {}
    # Update the reference so caller sees changes
    s["current_settings_update"](current_settings_loaded)
    current_settings = current_settings_loaded

    ensure_role_schema(current_settings)
    accounts_from_file = load_project_accounts(current_settings)

    file_session_names = {acc['session_name'] for acc in accounts_from_file if _is_account_active(acc)}
    for session_name in list(active_clients.keys()):
        acc_data = next((a for a in accounts_from_file if a['session_name'] == session_name), None)
        if session_name not in file_session_names or not acc_data:
            client_to_stop = active_clients.pop(session_name)
            await client_to_stop.stop()
            keys_to_remove = [k for k in client_catch_up_status if k.startswith(f"{session_name}_")]
            for k in keys_to_remove:
                client_catch_up_status.discard(k)
            CLIENT_CONNECT_STATE.pop(session_name, None)
            logger.info(f"Клиент {session_name} остановлен (удален или время сна).")

    for account_data in accounts_from_file:
        session_name = account_data['session_name']

        if not _is_account_active(account_data):
            continue

        client_wrapper = active_clients.get(session_name)
        just_reconnected = False

        if client_wrapper is None:
            if not _connect_backoff_ready(session_name):
                continue
            client_wrapper = CommentatorClient(account_data, api_id, api_hash, shared_state=s)
            try:
                if await client_wrapper.start():
                    active_clients[session_name] = client_wrapper
                    just_reconnected = True
                    CLIENT_CONNECT_STATE.pop(session_name, None)
                    logger.info(f"Клиент {session_name} запущен.")
                else:
                    err = getattr(client_wrapper, "_init_error", None) or "start_failed"
                    exceeded = _schedule_connect_backoff(session_name, error=str(err), reason="start")
                    await client_wrapper.stop()
                    if exceeded:
                        _mark_account_unavailable(session_name, error=str(err))
                        CLIENT_CONNECT_STATE.pop(session_name, None)
                    continue
            except Exception as e:
                exceeded = _schedule_connect_backoff(session_name, error=str(e), reason="start")
                try:
                    await client_wrapper.stop()
                except Exception:
                    pass
                if exceeded:
                    _mark_account_unavailable(session_name, error=str(e))
                    CLIENT_CONNECT_STATE.pop(session_name, None)
                continue
        else:
            was_connected = bool(getattr(client_wrapper, "client", None) and client_wrapper.client.is_connected())
            if not await ensure_client_connected(client_wrapper, reason="manage_clients"):
                continue
            if not was_connected and client_wrapper.client.is_connected():
                just_reconnected = True

        if just_reconnected:
            keys_to_remove = [k for k in client_catch_up_status if k.startswith(f"{session_name}_")]
            for k in keys_to_remove:
                client_catch_up_status.discard(k)

        for target in get_project_targets(current_settings):
            if _is_account_assigned(target, session_name):
                joined_ok = await ensure_account_joined(client_wrapper, target, joined_cache=s["joined_cache"])

                catch_up_key = f"{session_name}_{target.get('chat_id')}"
                if joined_ok and catch_up_key not in client_catch_up_status:
                    client_catch_up_status.add(catch_up_key)

                    async def _process_new_post_wrapper(evt, tgt, from_catch_up=False, is_manual=False):
                        cs = s["current_settings_ref"]()
                        return await process_new_post(
                            evt, tgt,
                            from_catch_up=from_catch_up,
                            is_manual=is_manual,
                            active_clients=s["active_clients"],
                            current_settings=cs,
                            pending_tasks=s["pending_tasks"],
                            scenario_context=s["scenario_context"],
                            processing_cache=s["processing_cache"],
                            post_process_cache=s["post_process_cache"],
                            post_process_cache_order=s["post_process_cache_order"],
                            post_process_cache_max=s["post_process_cache_max"],
                            spam_blocked_msgs=s.get("spam_blocked_msgs"),
                            channel_last_post_time=s["channel_last_post_time"],
                            recent_generated_messages=s["recent_generated_messages"],
                        )

                    asyncio.create_task(catch_up_missed_posts(
                        client_wrapper, target,
                        post_process_cache=s["post_process_cache"],
                        handled_grouped_ids=s["handled_grouped_ids"],
                        pending_tasks=s["pending_tasks"],
                        processing_cache=s["processing_cache"],
                        process_new_post_fn=_process_new_post_wrapper,
                    ))

        for r_target in get_project_reaction_targets(current_settings):
            if _is_account_assigned(r_target, session_name):
                await ensure_account_joined(client_wrapper, r_target, joined_cache=s["joined_cache"])

        for d_target in get_project_discussion_targets(current_settings):
            operator_session = str(d_target.get("operator_session_name") or "").strip()
            if session_name == operator_session or _is_account_assigned(d_target, session_name):
                await ensure_account_joined(client_wrapper, d_target, joined_cache=s["joined_cache"])

        for m_target in get_project_monitor_targets(current_settings):
            if _is_account_assigned(m_target, session_name):
                await ensure_account_joined(client_wrapper, m_target, joined_cache=s["joined_cache"])

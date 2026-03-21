"""Manual task processing — execute user-submitted manual comment tasks.

Extracted from commentator.py.
"""

import asyncio
import collections
import logging
import random
import time

from services.connection import (
    ensure_client_connected,
    _is_account_assigned,
)
from services.joining import ensure_account_joined
from services.project import (
    _active_project_id,
    get_project_targets,
    _claim_project_manual_tasks,
    _set_manual_task_status,
)

logger = logging.getLogger(__name__)

# Module-level state replacing function attributes
_last_summary_log_at: float = 0.0
_last_summary_count: int | None = None
_last_no_clients_warn: dict = {}


async def process_manual_tasks(
    *,
    active_clients: dict,
    current_settings: dict,
    joined_cache: set,
    process_new_post_fn,
):
    """Process pending manual comment tasks from the queue.

    Parameters
    ----------
    active_clients : dict
        session_name -> CommentatorClient mapping.
    current_settings : dict
        Global settings dict.
    joined_cache : set
        Shared mutable set for join caching.
    process_new_post_fn : callable
        Reference to process_new_post function (async).
    """
    global _last_summary_log_at, _last_summary_count, _last_no_clients_warn

    tasks = _claim_project_manual_tasks(_active_project_id(current_settings), limit=100)
    if not tasks:
        return

    now_ts = time.time()
    if _last_summary_count != len(tasks) or (now_ts - _last_summary_log_at) >= 60.0:
        logger.info(f"🚀 [MANUAL] Найдено {len(tasks)} ручных задач на обработку...")
        _last_summary_log_at = now_ts
        _last_summary_count = len(tasks)

    for task in tasks:
        task_id = int(task.get("id") or 0)
        if not isinstance(task, dict):
            _set_manual_task_status(task_id, "failed", "manual_task_invalid_payload")
            continue

        target_chat_id_raw = str(task.get("chat_id") or "").strip()
        message_chat_id_raw = str(task.get("message_chat_id") or "").strip() or target_chat_id_raw
        post_id = task.get("post_id")
        if not target_chat_id_raw or not post_id:
            _set_manual_task_status(task_id, "failed", "manual_task_missing_chat_or_post")
            continue

        target_chat = None
        for t in get_project_targets(current_settings):
            if str(t.get("chat_id") or "").strip() == target_chat_id_raw:
                target_chat = t
                break
        if not target_chat:
            # Backward compatibility: some tasks might store linked_chat_id in chat_id.
            for t in get_project_targets(current_settings):
                if str(t.get("linked_chat_id") or "").strip() == target_chat_id_raw:
                    target_chat = t
                    break

        if not target_chat:
            _set_manual_task_status(task_id, "failed", f"target_not_found:{target_chat_id_raw}")
            continue

        effective_target_chat = target_chat
        overrides = task.get("overrides") if isinstance(task, dict) else None
        if isinstance(overrides, dict) and overrides:
            effective_target_chat = dict(target_chat)
            for key in [
                "vector_prompt",
                "accounts_per_post_min",
                "accounts_per_post_max",
                "delay_between_accounts",
                "daily_comment_limit",
            ]:
                if key in overrides and overrides.get(key) is not None:
                    effective_target_chat[key] = overrides.get(key)
            try:
                logger.info(
                    f"⚙️ [MANUAL] Overrides применены (keys={list(overrides.keys())}) для post_id={post_id} target={target_chat_id_raw}"
                )
            except Exception:
                pass

        eligible_clients = [
            c
            for c in list(active_clients.values())
            if _is_account_assigned(effective_target_chat, c.session_name)
            and getattr(c, "client", None) is not None
            and c.client.is_connected()
        ]

        if not eligible_clients:
            # Try to reconnect assigned accounts once (backoff applies).
            assigned = [
                c
                for c in list(active_clients.values())
                if _is_account_assigned(effective_target_chat, c.session_name)
                and getattr(c, "client", None) is not None
            ]
            for c in assigned:
                await ensure_client_connected(c, reason="manual")

            eligible_clients = [
                c
                for c in assigned
                if getattr(c, "client", None) is not None and c.client.is_connected()
            ]

        if not eligible_clients:
            last_warn_at = float(_last_no_clients_warn.get(str(target_chat_id_raw)) or 0.0)
            if (now_ts - last_warn_at) >= 60.0:
                logger.warning(f"⚠️ Нет подключенных клиентов для ручной задачи в {target_chat_id_raw}")
                _last_no_clients_warn[str(target_chat_id_raw)] = now_ts
            _set_manual_task_status(task_id, "pending", "no_connected_clients")
            continue

        client_wrapper = random.choice(eligible_clients)

        try:
            destination_chat_id = int(str(message_chat_id_raw))
            try:
                entity = await client_wrapper.client.get_input_entity(destination_chat_id)
            except Exception:
                await ensure_account_joined(client_wrapper, effective_target_chat, force=True, joined_cache=joined_cache)
                entity = await client_wrapper.client.get_input_entity(destination_chat_id)

            messages = await client_wrapper.client.get_messages(entity, ids=[post_id])
            if messages and messages[0]:
                msg = messages[0]

                final_chat_id = destination_chat_id

                # If we fetched the message from the main channel but the target has a linked discussion chat,
                # re-map to the linked chat message so comments go to the correct place.
                try:
                    from telethon.tl.functions.messages import GetDiscussionMessageRequest

                    should_map = False
                    linked_chat_id_cfg = str(effective_target_chat.get("linked_chat_id") or "").strip()
                    target_chat_id_cfg = str(effective_target_chat.get("chat_id") or "").strip()
                    if (
                        linked_chat_id_cfg
                        and target_chat_id_cfg
                        and str(destination_chat_id) == str(target_chat_id_cfg)
                        and str(linked_chat_id_cfg) != str(target_chat_id_cfg)
                    ):
                        should_map = True

                    if should_map:
                        discussion_res = await client_wrapper.client(
                            GetDiscussionMessageRequest(peer=entity, msg_id=post_id)
                        )
                        if discussion_res.messages:
                            found_msg = None
                            for m in discussion_res.messages:
                                try:
                                    if getattr(m, "chat_id", None) and int(getattr(m, "chat_id")) != int(
                                        destination_chat_id
                                    ):
                                        found_msg = m
                                        break
                                except Exception:
                                    continue
                            if not found_msg:
                                found_msg = discussion_res.messages[0]

                            linked_chat_id = getattr(found_msg, "chat_id", None) or destination_chat_id
                            linked_msg_id = getattr(found_msg, "id", None) or post_id

                            # Re-fetch message to ensure it's bound to the client (text/media access is more reliable).
                            refetched_msg = None
                            try:
                                linked_entity = await client_wrapper.client.get_input_entity(int(linked_chat_id))
                                refetched = await client_wrapper.client.get_messages(
                                    linked_entity, ids=[int(linked_msg_id)]
                                )
                                if refetched and refetched[0]:
                                    refetched_msg = refetched[0]
                            except Exception:
                                try:
                                    await ensure_account_joined(client_wrapper, effective_target_chat, force=True, joined_cache=joined_cache)
                                    linked_entity = await client_wrapper.client.get_input_entity(int(linked_chat_id))
                                    refetched = await client_wrapper.client.get_messages(
                                        linked_entity, ids=[int(linked_msg_id)]
                                    )
                                    if refetched and refetched[0]:
                                        refetched_msg = refetched[0]
                                except Exception:
                                    refetched_msg = None

                            msg = refetched_msg or found_msg
                            final_chat_id = getattr(msg, "chat_id", None) or linked_chat_id
                            logger.info(
                                f"🔄 [MANUAL] Переадресация: Пост Канала {post_id} -> Пост Группы {linked_msg_id}"
                            )
                except Exception as e:
                    logger.warning(
                        f"⚠️ [MANUAL] Не удалось найти Linked-сообщение (возможно нет комментариев): {e}"
                    )

                event_mock = collections.namedtuple("EventMock", ["message", "chat_id"])
                mock_event = event_mock(message=msg, chat_id=final_chat_id)

                logger.info(f"⚡ [MANUAL] Принудительный запуск обработки поста {msg.id} в {final_chat_id}")

                asyncio.create_task(
                    process_new_post_fn(
                        mock_event,
                        effective_target_chat,
                        from_catch_up=False,
                        is_manual=True,
                    )
                )
                _set_manual_task_status(task_id, "done")
            else:
                logger.warning(f"❌ [MANUAL] Не удалось найти сообщение {post_id} в {message_chat_id_raw}")
                _set_manual_task_status(task_id, "failed", f"message_not_found:{post_id}@{message_chat_id_raw}")

        except Exception as e:
            logger.error(f"Ошибка ручной обработки поста: {e}")
            _set_manual_task_status(task_id, "pending", f"processing_error:{type(e).__name__}:{e}")

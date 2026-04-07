"""Outbound message queue processing.

Extracted from commentator.py.
"""

import logging
from datetime import datetime, timezone

from services.account_utils import load_project_accounts
from services.profile import _connect_temp_client

logger = logging.getLogger(__name__)


def _db_connect():
    """Lazy import to avoid circular dependency."""
    from db.connection import get_connection
    return get_connection()


def _load_config_section(section: str):
    """Load a section from config.ini."""
    import configparser
    import os
    from app_paths import CONFIG_FILE
    parser = configparser.ConfigParser()
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"Файл config.ini не найден: {CONFIG_FILE}")
    parser.read(CONFIG_FILE)
    if section not in parser:
        raise KeyError(f"В config.ini не найдена секция [{section}].")
    return parser[section]


async def process_outbound_queue(
    *,
    active_clients: dict,
    current_settings: dict,
):
    """Process pending outbound messages (manual replies/DMs).

    Parameters
    ----------
    active_clients : dict
        session_name -> CommentatorClient mapping.
    current_settings : dict
        Global settings dict.
    """
    try:
        project_sessions = {
            a.get("session_name") for a in load_project_accounts(current_settings) if a.get("session_name")
        }
        with _db_connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM outbound_queue WHERE status = 'pending'")
            tasks = cursor.fetchall()

        if not tasks:
            return

        for task in tasks:
            t_id = task['id']
            session_name = task['session_name']
            chat_id_str = task['chat_id']
            reply_id = task['reply_to_msg_id']
            text = task['text']
            if session_name not in project_sessions:
                continue

            client_wrapper = active_clients.get(session_name)
            temp_client = None
            client = client_wrapper.client if client_wrapper else None
            if client is None:
                try:
                    telethon_config = _load_config_section('telethon_credentials')
                    api_id, api_hash = int(telethon_config['api_id']), telethon_config['api_hash']
                    accounts_data = load_project_accounts(current_settings)
                    account_data = next((a for a in accounts_data if a.get('session_name') == session_name), None)
                    if not account_data:
                        raise KeyError("account_not_found")
                    temp_client = await _connect_temp_client(account_data, api_id, api_hash)
                    client = temp_client
                except Exception as e:
                    with _db_connect() as conn:
                        conn.execute("UPDATE outbound_queue SET status = 'failed_no_client' WHERE id = %s", (t_id,))
                    kind = "quote" if reply_id else "dm"
                    with _db_connect() as conn:
                        cur = conn.cursor()
                        cur.execute(
                            """
                            UPDATE inbox_messages
                            SET status='error', error=%s
                            WHERE id = (
                              SELECT id
                              FROM inbox_messages
                              WHERE kind=%s AND direction='out' AND status='queued'
                                AND session_name=%s AND chat_id=%s AND text=%s
                              ORDER BY id DESC
                              LIMIT 1
                            )
                            """,
                            (f"no_client:{e}", kind, session_name, str(chat_id_str), text),
                        )
                        if cur.rowcount == 0:
                            conn.execute(
                                """
                                INSERT INTO inbox_messages (
                                  kind, direction, status, created_at,
                                  session_name, chat_id, reply_to_msg_id,
                                  text, is_read, error
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """,
                                (
                                    kind,
                                    "out",
                                    "error",
                                    datetime.now(timezone.utc).isoformat(),
                                    session_name,
                                    str(chat_id_str),
                                    reply_id,
                                    text,
                                    1,
                                    f"no_client:{e}",
                                ),
                            )
                        conn.commit()
                    continue

            try:
                dest_chat = int(str(chat_id_str).replace('-100', ''))
                entity = await client.get_input_entity(dest_chat)

                sent_msg = await client.send_message(entity, text, reply_to=reply_id)
                logger.info(f"✅ Ручной ответ отправлен от {session_name} в {dest_chat}")

                with _db_connect() as conn:
                    conn.execute("UPDATE outbound_queue SET status = 'sent' WHERE id = %s", (t_id,))

                # Mark the queued row (if any) as sent; otherwise insert a fresh row.
                now = datetime.now(timezone.utc).isoformat()
                kind = "quote" if reply_id else "dm"
                msg_id = getattr(sent_msg, "id", None) if sent_msg else None
                with _db_connect() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        UPDATE inbox_messages
                        SET status='sent', msg_id=%s, reply_to_msg_id=%s, error=NULL
                        WHERE id = (
                          SELECT id
                          FROM inbox_messages
                          WHERE kind=%s AND direction='out' AND status='queued'
                            AND session_name=%s AND chat_id=%s AND text=%s
                          ORDER BY id DESC
                          LIMIT 1
                        )
                        """,
                        (msg_id, reply_id, kind, session_name, str(chat_id_str), text),
                    )
                    if cur.rowcount == 0:
                        conn.execute(
                            """
                            INSERT INTO inbox_messages (
                              kind, direction, status, created_at,
                              session_name, chat_id, msg_id, reply_to_msg_id,
                              text, is_read
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (kind, "out", "sent", now, session_name, str(chat_id_str), msg_id, reply_id, text, 1),
                        )
                    conn.commit()

            except Exception as e:
                logger.error(f"Ошибка отправки ручного ответа (ID {t_id}): {e}")
                with _db_connect() as conn:
                    conn.execute("UPDATE outbound_queue SET status = 'error' WHERE id = %s", (t_id,))
                kind = "quote" if reply_id else "dm"
                with _db_connect() as conn:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        UPDATE inbox_messages
                        SET status='error', error=%s
                        WHERE id = (
                          SELECT id
                          FROM inbox_messages
                          WHERE kind=%s AND direction='out' AND status='queued'
                            AND session_name=%s AND chat_id=%s AND text=%s
                          ORDER BY id DESC
                          LIMIT 1
                        )
                        """,
                        (str(e), kind, session_name, str(chat_id_str), text),
                    )
                    if cur.rowcount == 0:
                        conn.execute(
                            """
                            INSERT INTO inbox_messages (
                              kind, direction, status, created_at,
                              session_name, chat_id, msg_id, reply_to_msg_id,
                              text, is_read, error
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                kind,
                                "out",
                                "error",
                                datetime.now(timezone.utc).isoformat(),
                                session_name,
                                str(chat_id_str),
                                None,
                                reply_id,
                                text,
                                1,
                                str(e),
                            ),
                        )
                    conn.commit()
            finally:
                if temp_client is not None:
                    try:
                        if temp_client.is_connected():
                            await temp_client.disconnect()
                    except Exception:
                        pass

    except Exception as e:
        logger.error(f"Ошибка в outbound_queue: {e}")

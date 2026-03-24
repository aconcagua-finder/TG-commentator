import os
import sqlite3
import tempfile
import unittest
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch


class _ClientStub:
    def __init__(self) -> None:
        self.entity_refs = []
        self.edits = []
        self.deletes = []

    async def get_input_entity(self, entity_ref):
        self.entity_refs.append(entity_ref)
        return entity_ref

    async def edit_message(self, entity, msg_id, new_text):
        self.edits.append((entity, msg_id, new_text))
        return SimpleNamespace(id=msg_id, message=new_text)

    async def delete_messages(self, entity, msg_ids):
        self.deletes.append((entity, list(msg_ids)))
        return True


class TestMessageActions(unittest.TestCase):
    def test_logs_migration_adds_msg_id_column(self) -> None:
        import db.schema as db_schema

        init_database = db_schema.init_database

        fd, db_path = tempfile.mkstemp(prefix="commentator-migrate-", suffix=".sqlite")
        os.close(fd)
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                """
                CREATE TABLE logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    log_type TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    destination_chat_id BIGINT NOT NULL,
                    channel_name TEXT,
                    channel_username TEXT,
                    source_channel_id BIGINT,
                    post_id BIGINT NOT NULL,
                    account_session_name TEXT,
                    account_first_name TEXT,
                    account_username TEXT,
                    content TEXT
                )
                """
            )
            conn.commit()

            old_db_url = db_schema.DB_URL
            db_schema.DB_URL = None
            try:
                init_database(conn)
            finally:
                db_schema.DB_URL = old_db_url

            cols = [row[1] for row in conn.execute("PRAGMA table_info(logs)").fetchall()]
            self.assertIn("msg_id", cols)
        finally:
            conn.close()
            try:
                os.remove(db_path)
            except Exception:
                pass

    def test_log_action_to_db_saves_msg_id(self) -> None:
        import db.connection as db_connection
        import db.schema as db_schema
        from services.db_queries import log_action_to_db

        init_database = db_schema.init_database

        fd, db_path = tempfile.mkstemp(prefix="commentator-log-msgid-", suffix=".sqlite")
        os.close(fd)
        try:
            old_sqlite_db_file = db_connection._sqlite_db_file
            old_db_url = db_connection.DB_URL
            old_schema_db_url = db_schema.DB_URL
            db_connection._sqlite_db_file = db_path
            db_connection.DB_URL = None
            db_schema.DB_URL = None
            try:
                with db_connection.get_connection() as conn:
                    init_database(conn)

                log_action_to_db(
                    {
                        "type": "comment",
                        "post_id": 1001,
                        "msg_id": 777001,
                        "comment": "hello",
                        "date": "2026-03-23T12:00:00+00:00",
                        "account": {"session_name": "Telegram17", "first_name": "A", "username": "u"},
                        "target": {"destination_chat_id": -100123, "channel_id": -100321},
                    }
                )

                with db_connection.get_connection() as conn:
                    row = conn.execute("SELECT msg_id, content FROM logs ORDER BY id DESC LIMIT 1").fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row["msg_id"], 777001)
                self.assertEqual(row["content"], "hello")
            finally:
                db_connection._sqlite_db_file = old_sqlite_db_file
                db_connection.DB_URL = old_db_url
                db_schema.DB_URL = old_schema_db_url
        finally:
            try:
                os.remove(db_path)
            except Exception:
                pass

    @contextmanager
    def _client_context(self):
        import app_paths
        import db.connection as db_connection
        import db.schema as db_schema
        import admin_web.helpers as helpers
        import admin_web.main as m
        import admin_web.routes.auth as auth_routes
        import admin_web.routes.dialogs as dialogs_routes
        import admin_web.routes.message_actions as message_actions_routes
        import admin_web.templating as templating
        from fastapi.testclient import TestClient

        fd, db_path = tempfile.mkstemp(prefix="commentator-message-actions-", suffix=".sqlite")
        os.close(fd)

        settings = {"active_project_id": helpers.DEFAULT_PROJECT_ID}
        accounts = [{"session_name": "Telegram17", "status": "active", "project_id": helpers.DEFAULT_PROJECT_ID}]

        old_active_clients = getattr(m.app.state, "active_clients", {})
        stub_client = _ClientStub()
        m.app.state.active_clients = {
            "Telegram17": SimpleNamespace(session_name="Telegram17", client=stub_client),
        }

        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(app_paths, "DB_FILE", db_path))
                stack.enter_context(patch.object(db_connection, "_sqlite_db_file", db_path))
                stack.enter_context(patch.object(db_connection, "DB_URL", None))
                stack.enter_context(patch.object(db_schema, "DB_URL", None))
                stack.enter_context(patch.object(m, "DB_FILE", db_path))
                stack.enter_context(patch.object(dialogs_routes, "_load_settings", lambda: (settings, None)))
                stack.enter_context(patch.object(dialogs_routes, "_load_accounts", lambda: (accounts, None)))
                stack.enter_context(patch.object(message_actions_routes, "_load_settings", lambda: (settings, None)))
                stack.enter_context(patch.object(message_actions_routes, "_load_accounts", lambda: (accounts, None)))
                stack.enter_context(patch.object(templating, "_load_settings", lambda: (settings, None)))
                stack.enter_context(patch.object(templating, "_load_accounts", lambda: (accounts, None)))

                with TestClient(m.app) as client:
                    login = client.post(
                        "/login",
                        data={
                            "username": auth_routes.ADMIN_WEB_USERNAME,
                            "password": auth_routes.ADMIN_WEB_PASSWORD,
                        },
                        follow_redirects=False,
                    )
                    self.assertEqual(login.status_code, 303)
                    yield client, helpers, stub_client
        finally:
            m.app.state.active_clients = old_active_clients
            try:
                os.remove(db_path)
            except Exception:
                pass

    def test_edit_route_updates_logs_and_calls_telegram(self) -> None:
        with self._client_context() as (client, helpers, stub_client):
            created_at = datetime.now(timezone.utc).isoformat()
            with helpers._db_connect() as conn:
                conn.execute(
                    """
                    INSERT INTO logs (
                        log_type, timestamp, destination_chat_id, channel_name, channel_username,
                        source_channel_id, post_id, msg_id, account_session_name, account_first_name,
                        account_username, content
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "comment",
                        created_at,
                        -100123,
                        "Chat",
                        "chat",
                        -100123,
                        321,
                        555,
                        "Telegram17",
                        "A",
                        "u",
                        "old text",
                    ),
                )
                conn.commit()

            response = client.post(
                "/messages/edit",
                data={"source": "logs", "record_id": "1", "new_text": "new text", "return_to": "/"},
                follow_redirects=False,
            )
            self.assertEqual(response.status_code, 303)
            self.assertEqual(stub_client.entity_refs, [123])
            self.assertEqual(stub_client.edits, [(123, 555, "new text")])

            with helpers._db_connect() as conn:
                row = conn.execute("SELECT content FROM logs WHERE id = 1").fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["content"], "new text")

    def test_delete_route_marks_inbox_deleted_and_calls_telegram(self) -> None:
        with self._client_context() as (client, helpers, stub_client):
            created_at = datetime.now(timezone.utc).isoformat()
            with helpers._db_connect() as conn:
                conn.execute(
                    """
                    INSERT INTO inbox_messages (
                        kind, direction, status, created_at,
                        session_name, chat_id, msg_id,
                        text, is_read
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("dm", "out", "sent", created_at, "Telegram17", "42", 700, "bye", 1),
                )
                conn.commit()

            response = client.post(
                "/messages/delete",
                data={"source": "inbox", "record_id": "1", "return_to": "/"},
                follow_redirects=False,
            )
            self.assertEqual(response.status_code, 303)
            self.assertEqual(stub_client.entity_refs, [42])
            self.assertEqual(stub_client.deletes, [(42, [700])])

            with helpers._db_connect() as conn:
                row = conn.execute("SELECT status FROM inbox_messages WHERE id = 1").fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["status"], "deleted")

    def test_stats_page_shows_actions_only_for_logs_with_msg_id(self) -> None:
        with self._client_context() as (client, helpers, _stub_client):
            created_at = datetime.now(timezone.utc).isoformat()
            with helpers._db_connect() as conn:
                conn.execute(
                    """
                    INSERT INTO logs (
                        log_type, timestamp, destination_chat_id, channel_name, channel_username,
                        source_channel_id, post_id, msg_id, account_session_name, account_first_name,
                        account_username, content
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "comment",
                        created_at,
                        -100123,
                        "Chat A",
                        "chat_a",
                        -100123,
                        11,
                        501,
                        "Telegram17",
                        "A",
                        "u",
                        "editable text",
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO logs (
                        log_type, timestamp, destination_chat_id, channel_name, channel_username,
                        source_channel_id, post_id, msg_id, account_session_name, account_first_name,
                        account_username, content
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "comment",
                        created_at,
                        -100124,
                        "Chat B",
                        "chat_b",
                        -100124,
                        12,
                        None,
                        "Telegram17",
                        "A",
                        "u",
                        "legacy text",
                    ),
                )
                conn.commit()

            response = client.get("/stats?period=day&page=0")
            self.assertEqual(response.status_code, 200)
            self.assertIn("<th class=\"text-end\">Действия</th>", response.text)
            self.assertIn('action="/messages/edit"', response.text)
            self.assertIn('action="/messages/delete"', response.text)
            self.assertIn('name="record_id" value="1"', response.text)
            self.assertNotIn('name="record_id" value="2"', response.text)
            self.assertIn('/static/message_actions.js', response.text)

    def test_dialog_thread_shows_inline_actions_for_sent_outgoing_and_deleted_placeholder(self) -> None:
        with self._client_context() as (client, helpers, _stub_client):
            created_at = datetime.now(timezone.utc).isoformat()
            with helpers._db_connect() as conn:
                conn.execute(
                    """
                    INSERT INTO inbox_messages (
                        kind, direction, status, created_at,
                        session_name, chat_id, msg_id,
                        text, is_read
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("dm", "out", "sent", created_at, "Telegram17", "chat-1", 900, "hello there", 1),
                )
                conn.execute(
                    """
                    INSERT INTO inbox_messages (
                        kind, direction, status, created_at,
                        session_name, chat_id, msg_id,
                        text, is_read
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("dm", "out", "deleted", created_at, "Telegram17", "chat-1", 901, "should be hidden", 1),
                )
                conn.commit()

            response = client.get("/dialogs/Telegram17/chat-1")
            self.assertEqual(response.status_code, 200)
            self.assertIn('action="/messages/edit"', response.text)
            self.assertIn('action="/messages/delete"', response.text)
            self.assertIn('name="record_id" value="1"', response.text)
            self.assertNotIn('name="record_id" value="2"', response.text)
            self.assertIn("Сообщение удалено", response.text)
            self.assertNotIn("should be hidden", response.text)


if __name__ == "__main__":
    unittest.main()

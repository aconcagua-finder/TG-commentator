import os
import tempfile
import unittest
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from unittest.mock import patch


class TestInboxBadgeReset(unittest.TestCase):
    @contextmanager
    def _client_context(self):
        import app_paths
        import db.connection as db_connection
        import admin_web.helpers as helpers
        import admin_web.main as m
        import admin_web.routes.auth as auth_routes
        import admin_web.routes.dialogs as dialogs_routes
        import admin_web.templating as templating
        from fastapi.testclient import TestClient

        fd, db_path = tempfile.mkstemp(prefix="commentator-inbox-", suffix=".sqlite")
        os.close(fd)

        settings = {"active_project_id": helpers.DEFAULT_PROJECT_ID}
        accounts = [{"session_name": "Telegram17", "status": "active"}]

        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(app_paths, "DB_FILE", db_path))
                stack.enter_context(patch.object(db_connection, "_sqlite_db_file", db_path))
                stack.enter_context(patch.object(m, "DB_FILE", db_path))
                stack.enter_context(patch.object(dialogs_routes, "_load_settings", lambda: (settings, None)))
                stack.enter_context(patch.object(dialogs_routes, "_load_accounts", lambda: (accounts, None)))
                stack.enter_context(patch.object(dialogs_routes, "_cleanup_inbox_for_removed_accounts", lambda _settings: None))
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
                    yield client, helpers
        finally:
            try:
                os.remove(db_path)
            except Exception:
                pass

    def test_dialogs_page_does_not_mark_incoming_messages_as_read_on_render(self) -> None:
        with self._client_context() as (client, helpers):
            created_at = datetime.now(timezone.utc).isoformat()
            with helpers._db_connect() as conn:
                conn.execute(
                    """
                    INSERT INTO inbox_messages (
                        kind, direction, status, created_at,
                        session_name, chat_id, msg_id,
                        sender_name, chat_title, text, is_read
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("dm", "in", "received", created_at, "Telegram17", "chat-1", 101, "Alice", "Alice", "hello", 0),
                )
                conn.commit()

            response = client.get("/dialogs")
            self.assertEqual(response.status_code, 200)
            self.assertIn('badge text-bg-danger rounded-pill', response.text)

            with helpers._db_connect() as conn:
                row = conn.execute(
                    """
                    SELECT is_read
                    FROM inbox_messages
                    WHERE kind='dm' AND direction='in' AND session_name=? AND chat_id=?
                    """,
                    ("Telegram17", "chat-1"),
                ).fetchone()
            self.assertIsNotNone(row)
            # Rendering /dialogs MUST NOT auto-mark messages as read.
            self.assertEqual(row["is_read"], 0)

    def test_dialogs_mark_all_read_endpoint_marks_messages_as_read(self) -> None:
        with self._client_context() as (client, helpers):
            created_at = datetime.now(timezone.utc).isoformat()
            with helpers._db_connect() as conn:
                conn.execute(
                    """
                    INSERT INTO inbox_messages (
                        kind, direction, status, created_at,
                        session_name, chat_id, msg_id,
                        sender_name, chat_title, text, is_read
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("dm", "in", "received", created_at, "Telegram17", "chat-1", 101, "Alice", "Alice", "hello", 0),
                )
                conn.commit()

            response = client.post("/dialogs/mark-all-read", data={"session_name": ""}, follow_redirects=False)
            self.assertEqual(response.status_code, 303)

            with helpers._db_connect() as conn:
                row = conn.execute(
                    """
                    SELECT is_read
                    FROM inbox_messages
                    WHERE kind='dm' AND direction='in' AND session_name=? AND chat_id=?
                    """,
                    ("Telegram17", "chat-1"),
                ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["is_read"], 1)

    def test_dialogs_bulk_delete_endpoint_removes_selected_threads(self) -> None:
        with self._client_context() as (client, helpers):
            created_at = datetime.now(timezone.utc).isoformat()
            with helpers._db_connect() as conn:
                conn.execute(
                    """
                    INSERT INTO inbox_messages (
                        kind, direction, status, created_at,
                        session_name, chat_id, msg_id,
                        sender_name, chat_title, text, is_read
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("dm", "in", "received", created_at, "Telegram17", "chat-1", 101, "Alice", "Alice", "hi", 0),
                )
                conn.execute(
                    """
                    INSERT INTO inbox_messages (
                        kind, direction, status, created_at,
                        session_name, chat_id, msg_id,
                        sender_name, chat_title, text, is_read
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("dm", "in", "received", created_at, "Telegram17", "chat-2", 102, "Bob", "Bob", "yo", 0),
                )
                conn.commit()

            response = client.post(
                "/dialogs/bulk-delete",
                data={"thread_keys": ["Telegram17|chat-1"], "session_name": ""},
                follow_redirects=False,
            )
            self.assertEqual(response.status_code, 303)

            with helpers._db_connect() as conn:
                rows = conn.execute(
                    "SELECT chat_id FROM inbox_messages WHERE kind='dm' AND session_name=?",
                    ("Telegram17",),
                ).fetchall()
            remaining_chats = {r["chat_id"] for r in rows}
            self.assertEqual(remaining_chats, {"chat-2"})

    def test_quotes_page_does_not_mark_incoming_quotes_as_read_on_render(self) -> None:
        with self._client_context() as (client, helpers):
            created_at = datetime.now(timezone.utc).isoformat()
            with helpers._db_connect() as conn:
                conn.execute(
                    """
                    INSERT INTO inbox_messages (
                        kind, direction, status, created_at,
                        session_name, chat_id, msg_id,
                        sender_name, chat_title, text, replied_to_text, is_read
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "quote",
                        "in",
                        "received",
                        created_at,
                        "Telegram17",
                        "-100123",
                        202,
                        "Bob",
                        "Group",
                        "reply text",
                        "original message",
                        0,
                    ),
                )
                conn.commit()

            response = client.get("/quotes")
            self.assertEqual(response.status_code, 200)
            self.assertIn('badge text-bg-danger">new</span>', response.text)

            with helpers._db_connect() as conn:
                row = conn.execute(
                    """
                    SELECT is_read
                    FROM inbox_messages
                    WHERE kind='quote' AND direction='in' AND session_name=? AND chat_id=?
                    """,
                    ("Telegram17", "-100123"),
                ).fetchone()
            self.assertIsNotNone(row)
            # Rendering /quotes MUST NOT auto-mark quotes as read.
            self.assertEqual(row["is_read"], 0)

    def test_quotes_mark_all_read_endpoint_marks_quotes_as_read(self) -> None:
        with self._client_context() as (client, helpers):
            created_at = datetime.now(timezone.utc).isoformat()
            with helpers._db_connect() as conn:
                conn.execute(
                    """
                    INSERT INTO inbox_messages (
                        kind, direction, status, created_at,
                        session_name, chat_id, msg_id,
                        sender_name, chat_title, text, replied_to_text, is_read
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "quote",
                        "in",
                        "received",
                        created_at,
                        "Telegram17",
                        "-100123",
                        202,
                        "Bob",
                        "Group",
                        "reply text",
                        "original message",
                        0,
                    ),
                )
                conn.commit()

            response = client.post("/quotes/mark-all-read", data={"session_name": ""}, follow_redirects=False)
            self.assertEqual(response.status_code, 303)

            with helpers._db_connect() as conn:
                row = conn.execute(
                    """
                    SELECT is_read
                    FROM inbox_messages
                    WHERE kind='quote' AND direction='in' AND session_name=? AND chat_id=?
                    """,
                    ("Telegram17", "-100123"),
                ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["is_read"], 1)

    def test_quotes_bulk_delete_endpoint_removes_selected_quotes(self) -> None:
        with self._client_context() as (client, helpers):
            created_at = datetime.now(timezone.utc).isoformat()
            with helpers._db_connect() as conn:
                cursor = conn.execute(
                    """
                    INSERT INTO inbox_messages (
                        kind, direction, status, created_at,
                        session_name, chat_id, msg_id,
                        sender_name, chat_title, text, is_read
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("quote", "in", "received", created_at, "Telegram17", "-100123", 301, "Bob", "Group", "a", 0),
                )
                quote_id_to_delete = cursor.lastrowid
                conn.execute(
                    """
                    INSERT INTO inbox_messages (
                        kind, direction, status, created_at,
                        session_name, chat_id, msg_id,
                        sender_name, chat_title, text, is_read
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("quote", "in", "received", created_at, "Telegram17", "-100123", 302, "Eve", "Group", "b", 0),
                )
                conn.commit()

            response = client.post(
                "/quotes/bulk-delete",
                data={"inbox_ids": [str(quote_id_to_delete)], "session_name": ""},
                follow_redirects=False,
            )
            self.assertEqual(response.status_code, 303)

            with helpers._db_connect() as conn:
                rows = conn.execute(
                    "SELECT msg_id FROM inbox_messages WHERE kind='quote' AND session_name=?",
                    ("Telegram17",),
                ).fetchall()
            remaining = {r["msg_id"] for r in rows}
            self.assertEqual(remaining, {302})


if __name__ == "__main__":
    unittest.main()

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

    def test_dialogs_page_marks_incoming_messages_as_read_after_render_data_loaded(self) -> None:
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
            self.assertEqual(row["is_read"], 1)

    def test_quotes_page_marks_incoming_quotes_as_read_after_render_data_loaded(self) -> None:
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
            self.assertEqual(row["is_read"], 1)


if __name__ == "__main__":
    unittest.main()

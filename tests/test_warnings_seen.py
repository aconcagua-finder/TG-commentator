import os
import tempfile
import unittest
from contextlib import ExitStack
from unittest.mock import patch


class TestWarningsSeen(unittest.TestCase):
    def test_warnings_badge_clears_after_opening_warnings_page(self) -> None:
        import app_paths
        import db.connection as db_connection
        import admin_web.helpers as helpers
        import admin_web.main as m
        import admin_web.routes.auth as auth_routes
        import admin_web.routes.dashboard as dashboard_routes
        import admin_web.templating as templating
        from fastapi.testclient import TestClient

        fd, db_path = tempfile.mkstemp(prefix="commentator-warnings-", suffix=".sqlite")
        os.close(fd)

        settings = {"active_project_id": helpers.DEFAULT_PROJECT_ID}
        accounts = [{"session_name": "Telegram17", "status": "active"}]
        warning_key = helpers._warning_key_failure("Telegram17", "connect")

        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(app_paths, "DB_FILE", db_path))
                stack.enter_context(patch.object(db_connection, "_sqlite_db_file", db_path))
                stack.enter_context(patch.object(m, "DB_FILE", db_path))
                stack.enter_context(patch.object(dashboard_routes, "_load_settings", lambda: (settings, None)))
                stack.enter_context(patch.object(dashboard_routes, "_load_accounts", lambda: (accounts, None)))
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

                    for _ in range(helpers.WARNING_FAILURE_THRESHOLD):
                        helpers._record_account_failure(
                            "Telegram17",
                            "connect",
                            last_error="unauthorized",
                            last_target="start",
                        )

                    home = client.get("/")
                    self.assertEqual(home.status_code, 200)
                    self.assertIn('href="/warnings" title="Предупреждения"', home.text)
                    self.assertIn('class="badge text-bg-danger app-nav-item-badge">1</span>', home.text)

                    warnings = client.get("/warnings")
                    self.assertEqual(warnings.status_code, 200)
                    self.assertNotIn('class="badge text-bg-danger app-nav-item-badge">', warnings.text)

                    home2 = client.get("/")
                    self.assertEqual(home2.status_code, 200)
                    self.assertNotIn('class="badge text-bg-danger app-nav-item-badge">', home2.text)

                    with helpers._db_connect() as conn:
                        history_row = conn.execute(
                            "SELECT key, resolved_at FROM warning_history WHERE key = ? ORDER BY id DESC LIMIT 1",
                            (warning_key,),
                        ).fetchone()
                    self.assertIsNotNone(history_row)
                    self.assertEqual(history_row["key"], warning_key)
                    self.assertIsNone(history_row["resolved_at"])

                    helpers._clear_account_failure("Telegram17", "connect")

                    warnings_resolved = client.get("/warnings")
                    self.assertEqual(warnings_resolved.status_code, 200)
                    self.assertIn("Решённые предупреждения", warnings_resolved.text)
                    self.assertIn("Telegram17: повторные ошибки", warnings_resolved.text)

                    with helpers._db_connect() as conn:
                        resolved_row = conn.execute(
                            "SELECT resolved_at FROM warning_history WHERE key = ? ORDER BY id DESC LIMIT 1",
                            (warning_key,),
                        ).fetchone()
                    self.assertIsNotNone(resolved_row)
                    self.assertIsNotNone(resolved_row["resolved_at"])
        finally:
            try:
                os.remove(db_path)
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main()

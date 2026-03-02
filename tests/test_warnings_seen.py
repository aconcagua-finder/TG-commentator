import os
import tempfile
import unittest


class TestWarningsSeen(unittest.TestCase):
    def test_warnings_badge_clears_after_opening_warnings_page(self) -> None:
        import admin_web.main as m
        from fastapi.testclient import TestClient

        fd, db_path = tempfile.mkstemp(prefix="commentator-warnings-", suffix=".sqlite")
        os.close(fd)

        old_db = m.DB_FILE
        old_load_settings = m._load_settings
        old_load_accounts = m._load_accounts
        old_auth_flag = m.ADMIN_WEB_DISABLE_AUTH

        settings = {"active_project_id": m.DEFAULT_PROJECT_ID}
        accounts = [{"session_name": "Telegram17", "status": "active"}]

        try:
            m.DB_FILE = db_path
            m._load_settings = lambda: (settings, None)
            m._load_accounts = lambda: (accounts, None)
            m.ADMIN_WEB_DISABLE_AUTH = False

            with TestClient(m.app) as client:
                login = client.post(
                    "/login",
                    data={"username": m.ADMIN_WEB_USERNAME, "password": m.ADMIN_WEB_PASSWORD},
                    follow_redirects=False,
                )
                self.assertEqual(login.status_code, 303)

                # Create a warning (>= threshold).
                for _ in range(m.WARNING_FAILURE_THRESHOLD):
                    m._record_account_failure("Telegram17", "connect", last_error="unauthorized", last_target="start")

                home = client.get("/")
                self.assertEqual(home.status_code, 200)
                self.assertIn('href="/warnings" title="Предупреждения"', home.text)
                self.assertIn('class="badge text-bg-danger app-nav-item-badge">1</span>', home.text)

                warnings = client.get("/warnings")
                self.assertEqual(warnings.status_code, 200)
                # Visiting the warnings page marks all as seen, so sidebar badge should be gone.
                self.assertNotIn('class="badge text-bg-danger app-nav-item-badge">', warnings.text)

                home2 = client.get("/")
                self.assertEqual(home2.status_code, 200)
                self.assertNotIn('class="badge text-bg-danger app-nav-item-badge">', home2.text)
        finally:
            m.DB_FILE = old_db
            m._load_settings = old_load_settings
            m._load_accounts = old_load_accounts
            m.ADMIN_WEB_DISABLE_AUTH = old_auth_flag
            try:
                os.remove(db_path)
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main()


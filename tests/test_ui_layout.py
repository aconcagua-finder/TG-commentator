from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parent.parent
BASE_TEMPLATE = ROOT / "admin_web" / "templates" / "base.html"
APP_CSS = ROOT / "admin_web" / "static" / "app.css"
MANUAL_TEMPLATE = ROOT / "admin_web" / "templates" / "manual.html"
DASHBOARD_TEMPLATE = ROOT / "admin_web" / "templates" / "dashboard.html"
STATS_TEMPLATE = ROOT / "admin_web" / "templates" / "stats.html"
ACCOUNTS_TEMPLATE = ROOT / "admin_web" / "templates" / "accounts.html"


class TestUiLayout(unittest.TestCase):
    def test_base_template_has_original_top_right_actions_and_aligned_sidebar_items(self) -> None:
        content = BASE_TEMPLATE.read_text(encoding="utf-8")

        self.assertIn('href="/dialogs" title="Диалоги"', content)
        self.assertIn('href="/quotes" title="Цитирование"', content)
        self.assertIn('href="/warnings" title="Предупреждения"', content)
        self.assertIn('href="/stats" title="Статистика"', content)
        self.assertNotIn('id="quickAccessMenuButton"', content)

        self.assertIn("app-nav-item-main", content)
        self.assertIn("app-nav-item-icon", content)
        self.assertIn("app-nav-item-label", content)

        self.assertIn("col-md-3 col-xl-2 d-none d-md-block sidebar", content)
        self.assertIn("col-12 col-md-9 col-xl-10", content)

    def test_app_css_has_rules_for_sidebar_alignment_and_toolbar(self) -> None:
        content = APP_CSS.read_text(encoding="utf-8")

        self.assertIn(".app-page-head", content)
        self.assertIn(".app-inline-form", content)

        self.assertIn(".app-nav-item-main", content)
        self.assertIn(".app-nav-item-icon", content)
        self.assertIn(".app-nav-item-label", content)
        self.assertIn("text-overflow: ellipsis;", content)

    def test_templates_use_responsive_layout_helpers(self) -> None:
        dashboard = DASHBOARD_TEMPLATE.read_text(encoding="utf-8")
        manual = MANUAL_TEMPLATE.read_text(encoding="utf-8")

        self.assertIn("app-page-head", dashboard)
        self.assertIn("app-actions", dashboard)
        self.assertIn("app-inline-form", dashboard)
        self.assertIn("app-page-head", manual)
        self.assertIn("app-inline-form", manual)

    def test_table_cells_do_not_apply_clamp_class_directly(self) -> None:
        stats = STATS_TEMPLATE.read_text(encoding="utf-8")
        accounts = ACCOUNTS_TEMPLATE.read_text(encoding="utf-8")
        self.assertNotIn("<td class=\"app-clamp", stats)
        self.assertNotIn("<td class=\"small app-clamp", stats)
        self.assertNotIn("<td class=\"mono app-clamp", accounts)

    def test_dashboard_renders_with_new_nav_markup(self) -> None:
        import admin_web.main as m
        from fastapi.testclient import TestClient

        original_auth_flag = m.ADMIN_WEB_DISABLE_AUTH
        m.ADMIN_WEB_DISABLE_AUTH = False
        try:
            with TestClient(m.app) as client:
                login = client.post(
                    "/login",
                    data={"username": m.ADMIN_WEB_USERNAME, "password": m.ADMIN_WEB_PASSWORD},
                    follow_redirects=False,
                )
                self.assertEqual(login.status_code, 303)
                response = client.get("/")
            self.assertEqual(response.status_code, 200)
            self.assertIn('href="/dialogs" title="Диалоги"', response.text)
            self.assertNotIn('id="quickAccessMenuButton"', response.text)
            self.assertIn("app-nav-item-label", response.text)
        finally:
            m.ADMIN_WEB_DISABLE_AUTH = original_auth_flag


if __name__ == "__main__":
    unittest.main()

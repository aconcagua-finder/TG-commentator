import unittest


class TestAdminWebUtils(unittest.TestCase):
    def setUp(self) -> None:
        # Import inside tests so these can be executed inside the Docker image
        # where dependencies (telethon/fastapi/etc.) are available.
        import admin_web.main as m

        self.m = m

    def test_parse_bool_defaults(self) -> None:
        m = self.m
        self.assertFalse(m._parse_bool(None))
        self.assertTrue(m._parse_bool(None, default=True))
        self.assertFalse(m._parse_bool(""))
        self.assertTrue(m._parse_bool("", default=True))

    def test_parse_bool_truthy(self) -> None:
        m = self.m
        for val in ("1", "true", "TRUE", " yes ", "Y", "on", True):
            with self.subTest(val=val):
                self.assertTrue(m._parse_bool(val))

    def test_parse_bool_falsy(self) -> None:
        m = self.m
        for val in ("0", "false", "no", "off", "random", False):
            with self.subTest(val=val):
                self.assertFalse(m._parse_bool(val))

    def test_project_id_for_and_filter_by_project(self) -> None:
        m = self.m
        items = [
            {"chat_id": "1", "project_id": "a"},
            {"chat_id": "2", "project_id": "b"},
            {"chat_id": "3"},  # default project
            "not-a-dict",
            None,
        ]
        self.assertEqual(m._project_id_for(items[0]), "a")
        self.assertEqual(m._project_id_for(items[2]), m.DEFAULT_PROJECT_ID)

        only_a = m._filter_by_project(items, "a")
        self.assertEqual([x.get("chat_id") for x in only_a], ["1"])

        only_default = m._filter_by_project(items, m.DEFAULT_PROJECT_ID)
        self.assertEqual([x.get("chat_id") for x in only_default], ["3"])

    def test_channel_bare_id(self) -> None:
        m = self.m
        self.assertEqual(m._channel_bare_id("-100123"), 123)
        self.assertEqual(m._channel_bare_id("-123"), 123)
        self.assertEqual(m._channel_bare_id("123"), 123)
        self.assertIsNone(m._channel_bare_id(""))
        self.assertIsNone(m._channel_bare_id("not-a-number"))

    def test_telegram_message_link(self) -> None:
        m = self.m
        self.assertEqual(m._telegram_message_link("channel", None, 10), "https://t.me/channel/10")
        self.assertEqual(m._telegram_message_link(None, "-100123", 10), "https://t.me/c/123/10")
        self.assertEqual(m._telegram_message_link(None, "-123", 10), "https://t.me/c/123/10")
        self.assertIsNone(m._telegram_message_link(None, None, 10))
        self.assertIsNone(m._telegram_message_link("channel", None, None))

    def test_safe_local_redirect_path(self) -> None:
        m = self.m
        self.assertEqual(m._safe_local_redirect_path("/dialogs?session_name=a", "/dialogs"), "/dialogs?session_name=a")
        self.assertEqual(m._safe_local_redirect_path("https://example.com", "/dialogs"), "/dialogs")
        self.assertEqual(m._safe_local_redirect_path("//evil.example", "/dialogs"), "/dialogs")
        self.assertEqual(m._safe_local_redirect_path("", "/dialogs"), "/dialogs")

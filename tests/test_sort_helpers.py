"""Unit tests for admin_web.sort_helpers."""

import unittest

from admin_web.sort_helpers import (
    PROXY_SORT_OPTIONS,
    apply_sort,
    default_key,
    options_for,
    proxy_default_key,
    proxy_order_by_sql,
    proxy_resolve_key,
    resolve_key,
    template_options,
)


class TestResolveKey(unittest.TestCase):
    def test_unknown_key_falls_back_to_default(self):
        self.assertEqual(resolve_key("garbage", "accounts"), default_key("accounts"))

    def test_known_key_passes_through(self):
        self.assertEqual(resolve_key("name_asc", "accounts"), "name_asc")

    def test_unknown_list_type_returns_empty(self):
        self.assertEqual(resolve_key("anything", "no_such_list"), "")

    def test_blank_or_none(self):
        self.assertEqual(resolve_key("", "accounts"), default_key("accounts"))
        self.assertEqual(resolve_key(None, "accounts"), default_key("accounts"))


class TestTemplateOptions(unittest.TestCase):
    def test_template_options_strip_callables(self):
        opts = template_options("accounts")
        self.assertGreater(len(opts), 0)
        for opt in opts:
            self.assertEqual(set(opt.keys()), {"key", "label"})


class TestApplySortAccounts(unittest.TestCase):
    def setUp(self):
        self.accounts = [
            {
                "session_name": "alpha",
                "first_name": "Boris",
                "last_name": "",
                "username": "boris",
                "status": "limited",
                "date_added": "2026-01-10T00:00:00",
                "last_checked": "2026-04-01T00:00:00",
            },
            {
                "session_name": "bravo",
                "first_name": "Anna",
                "last_name": "",
                "username": "anna",
                "status": "active",
                "date_added": "2026-03-20T00:00:00",
                "last_checked": "2026-04-05T00:00:00",
            },
            {
                "session_name": "charlie",
                "first_name": "",
                "last_name": "",
                "username": "",
                "status": "banned",
                "date_added": "2025-12-01T00:00:00",
                "last_checked": "",
            },
        ]

    def test_date_added_desc_default(self):
        out = apply_sort(self.accounts, "date_added_desc", "accounts")
        self.assertEqual([a["session_name"] for a in out], ["bravo", "alpha", "charlie"])

    def test_date_added_asc(self):
        out = apply_sort(self.accounts, "date_added_asc", "accounts")
        self.assertEqual([a["session_name"] for a in out], ["charlie", "alpha", "bravo"])

    def test_name_asc(self):
        out = apply_sort(self.accounts, "name_asc", "accounts")
        # Anna < Boris < charlie (charlie has no name → fallback to session_name "charlie")
        self.assertEqual([a["session_name"] for a in out], ["bravo", "alpha", "charlie"])

    def test_name_desc(self):
        out = apply_sort(self.accounts, "name_desc", "accounts")
        self.assertEqual([a["session_name"] for a in out], ["charlie", "alpha", "bravo"])

    def test_session_asc(self):
        out = apply_sort(self.accounts, "session_asc", "accounts")
        self.assertEqual([a["session_name"] for a in out], ["alpha", "bravo", "charlie"])

    def test_status_active_first(self):
        out = apply_sort(self.accounts, "status", "accounts")
        # active(0) < limited(2) < banned(7)
        self.assertEqual([a["session_name"] for a in out], ["bravo", "alpha", "charlie"])

    def test_last_check_desc(self):
        out = apply_sort(self.accounts, "last_check_desc", "accounts")
        self.assertEqual([a["session_name"] for a in out], ["bravo", "alpha", "charlie"])

    def test_invalid_key_uses_default(self):
        out = apply_sort(self.accounts, "ohmygod_unknown", "accounts")
        # Falls back to date_added_desc
        self.assertEqual([a["session_name"] for a in out], ["bravo", "alpha", "charlie"])


class TestApplySortChatTarget(unittest.TestCase):
    def setUp(self):
        self.targets = [
            {"chat_id": -1, "chat_name": "Бета", "chat_username": "beta", "date_added": "2026-02-01"},
            {"chat_id": -2, "chat_name": "Альфа", "chat_username": "alpha", "date_added": "2026-03-01"},
            {"chat_id": -3, "chat_name": "Гамма", "chat_username": None, "date_added": "2026-01-01"},
        ]

    def test_name_asc_uses_locale_lower(self):
        out = apply_sort(self.targets, "name_asc", "chat_target")
        self.assertEqual([t["chat_id"] for t in out], [-2, -1, -3])

    def test_date_desc_default(self):
        out = apply_sort(self.targets, "date_added_desc", "chat_target")
        self.assertEqual([t["chat_id"] for t in out], [-2, -1, -3])

    def test_username_asc_handles_missing_values(self):
        out = apply_sort(self.targets, "username_asc", "chat_target")
        # Empty username sorts first lexicographically
        self.assertEqual(out[0]["chat_id"], -3)


class TestApplySortPersonas(unittest.TestCase):
    def test_name_asc_and_desc(self):
        # Cyrillic letter order: А(а) < З(з) < М(м)
        roles = [
            ("rid_1", {"name": "Зет"}),
            ("rid_2", {"name": "Альфа"}),
            ("rid_3", {"name": "м"}),
        ]
        asc = apply_sort(roles, "name_asc", "personas")
        self.assertEqual([r[0] for r in asc], ["rid_2", "rid_1", "rid_3"])

        desc = apply_sort(roles, "name_desc", "personas")
        self.assertEqual([r[0] for r in desc], ["rid_3", "rid_1", "rid_2"])

    def test_falls_back_to_id_when_name_missing(self):
        roles = [
            ("rid_zzz", {}),
            ("rid_aaa", {"name": ""}),
        ]
        asc = apply_sort(roles, "name_asc", "personas")
        self.assertEqual([r[0] for r in asc], ["rid_aaa", "rid_zzz"])


class TestProxySort(unittest.TestCase):
    def test_default_sort_is_id_desc(self):
        self.assertEqual(proxy_default_key(), "id_desc")
        self.assertEqual(proxy_order_by_sql("id_desc"), "id DESC")

    def test_resolve_invalid_falls_back(self):
        self.assertEqual(proxy_resolve_key("nope"), "id_desc")
        self.assertEqual(proxy_resolve_key(""), "id_desc")
        self.assertEqual(proxy_resolve_key(None), "id_desc")

    def test_known_keys_each_have_safe_clause(self):
        for opt in PROXY_SORT_OPTIONS:
            sql = proxy_order_by_sql(opt["key"])
            self.assertNotIn(";", sql)
            self.assertNotIn("--", sql)

    def test_injection_attempt_returns_default(self):
        sql = proxy_order_by_sql("id_desc; DROP TABLE proxies; --")
        self.assertEqual(sql, "id DESC")


if __name__ == "__main__":
    unittest.main()

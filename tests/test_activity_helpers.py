"""Unit tests for admin_web.activity_helpers (no heavy deps required)."""

import unittest

from admin_web.activity_helpers import (
    build_target_index,
    enrich_log_row,
    log_type_meta,
    parse_content,
    summarize_log_counts,
)


class TestParseContent(unittest.TestCase):
    def test_comment_with_role_and_mood(self):
        p = parse_content(
            "comment",
            "[Роль: Эмпатичный собеседник · настроение: Оптимистичное] Забавно, как всё это складывается",
        )
        self.assertEqual(p["role"], "Эмпатичный собеседник")
        self.assertEqual(p["mood"], "Оптимистичное")
        self.assertIsNone(p["tag"])
        self.assertIn("Забавно", p["body"])
        self.assertTrue(p["summary"].startswith("Написал:"))

    def test_comment_reply_with_tag(self):
        p = parse_content(
            "comment_reply",
            "[Роль: Кастомная · настроение: Игривое] [ВМЕШАТЕЛЬСТВО] Да, а то уже на ровном месте завелись.",
        )
        self.assertEqual(p["tag"], "ВМЕШАТЕЛЬСТВО")
        self.assertEqual(p["role"], "Кастомная")
        self.assertEqual(p["mood"], "Игривое")
        self.assertIn("ровном", p["body"])

    def test_reaction(self):
        p = parse_content("reaction", "👍 🔥")
        self.assertEqual(p["body"], "👍 🔥")
        self.assertIn("реакцию", p["summary"])

    def test_comment_failed_bare_prefix(self):
        p = parse_content(
            "comment_failed",
            "Роль: Эмпатичный собеседник · настроение: Нейтральное · FAIL(openai:gpt-4o-mini)",
        )
        self.assertEqual(p["role"], "Эмпатичный собеседник")
        self.assertEqual(p["mood"], "Нейтральное")
        self.assertIn("FAIL", p["body"])

    def test_comment_skip(self):
        p = parse_content("comment_skip", "шанс коммента 25%")
        self.assertIn("Пропустил", p["summary"])


class TestLogTypeMeta(unittest.TestCase):
    def test_known_types(self):
        for lt in ("comment", "reaction", "comment_reply", "monitoring", "spam_deleted"):
            m = log_type_meta(lt)
            self.assertTrue(m["label"])
            self.assertTrue(m["icon"].startswith("bi-"))
            self.assertTrue(m["color"])

    def test_unknown_falls_back(self):
        m = log_type_meta("something_weird")
        self.assertEqual(m["color"], "secondary")


class TestBuildTargetIndex(unittest.TestCase):
    def test_indexes_main_and_linked(self):
        settings = {
            "targets": [
                {
                    "chat_id": -1002901278931,
                    "linked_chat_id": -1001692333845,
                    "chat_name": "ЦФУ ГРУПП",
                    "chat_username": "cfugrupp",
                }
            ],
            "reaction_targets": [],
            "monitor_targets": [],
            "discussion_targets": [],
        }
        index = build_target_index(settings)
        self.assertIn("-1002901278931", index)
        self.assertIn("-1001692333845", index)
        self.assertEqual(index["-1001692333845"]["chat_name"], "ЦФУ ГРУПП")
        self.assertTrue(index["-1002901278931"]["href"].startswith("/targets/"))


class TestEnrichLogRow(unittest.TestCase):
    def _row(self, **kwargs):
        base = {
            "id": 1,
            "log_type": "comment",
            "timestamp": "2026-04-01T12:00:00",
            "destination_chat_id": -1001692333845,
            "channel_name": "ЦФУ ГРУПП",
            "channel_username": "cfugrupp",
            "source_channel_id": -1002901278931,
            "post_id": 290,
            "msg_id": 1234,
            "account_session_name": "Telegram13",
            "account_first_name": "Teo",
            "account_username": "teobot",
            "content": "[Роль: Эмпатичный · настроение: Оптимистичное] Test body",
        }
        base.update(kwargs)
        return base

    def test_enriches_comment_with_links(self):
        row = self._row()
        enriched = enrich_log_row(row, target_index={})
        self.assertEqual(enriched["type_label"], "Комментарий")
        self.assertEqual(enriched["role"], "Эмпатичный")
        self.assertEqual(enriched["post_link"], "https://t.me/cfugrupp/290")
        # message_link uses destination group + msg_id
        self.assertEqual(enriched["message_link"], "https://t.me/c/1692333845/1234")

    def test_private_channel_link_fallback(self):
        row = self._row(channel_username="", source_channel_id=-1001234567890)
        enriched = enrich_log_row(row, target_index={})
        self.assertEqual(enriched["post_link"], "https://t.me/c/1234567890/290")

    def test_target_href_from_index(self):
        row = self._row()
        index = {"-1002901278931": {"href": "/targets/-1002901278931", "chat_name": "ЦФУ ГРУПП"}}
        enriched = enrich_log_row(row, target_index=index)
        self.assertEqual(enriched["target_href"], "/targets/-1002901278931")


class TestSummarizeCounts(unittest.TestCase):
    def test_counts(self):
        rows = [
            {"log_type": "comment"},
            {"log_type": "comment"},
            {"log_type": "reaction"},
        ]
        counts = summarize_log_counts(rows)
        self.assertEqual(counts["comment"], 2)
        self.assertEqual(counts["reaction"], 1)
        self.assertEqual(counts["total"], 3)


if __name__ == "__main__":
    unittest.main()

"""Standalone tests for build_spam_notification (no admin_web/telethon deps)."""

import unittest


class TestBuildSpamNotification(unittest.TestCase):
    def setUp(self) -> None:
        from services.telegram_bot import build_spam_notification

        self.build = build_spam_notification

    def test_deleted_uses_success_title_and_status(self) -> None:
        text = self.build(
            {
                "sender_name": "Sasha",
                "sender_username": "sasha",
                "detection_method": "keyword",
                "matched_keyword": "ставки",
                "message_text": "ставки и казино",
                "action": "deleted",
            },
            {"chat_name": "ЦФУ", "chat_username": "cfugrupp"},
        )
        self.assertIn("спам удалён", text)
        self.assertIn("✅", text)
        self.assertNotIn("НЕ удалён", text)
        self.assertIn("ставки", text)

    def test_failed_uses_warning_title_and_failure_status(self) -> None:
        text = self.build(
            {
                "sender_name": "Spammer",
                "sender_username": None,
                "detection_method": "name_keyword",
                "matched_keyword": "casino",
                "message_text": "spam payload",
                "action": "failed_to_delete",
            },
            {"chat_name": "ИИ для юриста"},
        )
        self.assertIn("НЕ удалён", text)
        self.assertIn("❌", text)
        self.assertIn("права", text.lower())
        self.assertIn("spam payload", text)

    def test_missing_action_defaults_to_deleted(self) -> None:
        # Backward compatibility: existing callers without "action".
        text = self.build(
            {"detection_method": "keyword", "message_text": "x"},
            {"chat_name": "Канал"},
        )
        self.assertIn("удалён", text)
        self.assertNotIn("НЕ удалён", text)


if __name__ == "__main__":
    unittest.main()

import os
import tempfile
import unittest


class TestShortPostDiversity(unittest.TestCase):
    def setUp(self) -> None:
        import commentator

        self.commentator = commentator
        self._old_db_file = commentator.DB_FILE

        fd, path = tempfile.mkstemp(prefix="commentator-test-", suffix=".sqlite")
        os.close(fd)
        self._tmp_db = path
        commentator.DB_FILE = self._tmp_db
        commentator.init_database()

    def tearDown(self) -> None:
        self.commentator.DB_FILE = self._old_db_file
        try:
            os.remove(self._tmp_db)
        except Exception:
            pass

    def test_build_short_post_semantic_instructions_includes_angle(self) -> None:
        c = self.commentator
        text = c.build_semantic_diversity_instructions(
            "Скоро релиз.",
            angle_hint="Уточни детали: задай один конкретный вопрос по теме.",
        )
        self.assertIn("ВАЖНО", text)
        self.assertIn("СМЫСЛОВОЙ УГОЛ", text)

    def test_short_post_novelty_check_flags_pure_paraphrase(self) -> None:
        c = self.commentator
        post_text = "Скоро релиз."
        existing = ["Да, скоро релиз."]
        needs, new_count = c.comment_needs_more_novelty(
            "Ну да, релиз скоро.",
            post_text=post_text,
            existing_comments=existing,
            min_new_tokens=1,
        )
        self.assertTrue(needs)
        self.assertEqual(new_count, 0)

    def test_short_post_novelty_check_allows_new_aspect(self) -> None:
        c = self.commentator
        post_text = "Скоро релиз."
        existing = ["Да, скоро релиз."]
        needs, new_count = c.comment_needs_more_novelty(
            "Круто, а будет ли changelog и дата точная?",
            post_text=post_text,
            existing_comments=existing,
            min_new_tokens=1,
        )
        self.assertFalse(needs)
        self.assertGreaterEqual(new_count, 1)

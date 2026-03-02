import os
import tempfile
import unittest
from datetime import datetime, timezone


class TestPersistenceMisc(unittest.TestCase):
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

    def test_last_post_time_roundtrip(self) -> None:
        c = self.commentator
        t = datetime(2026, 2, 19, 12, 0, 0, tzinfo=timezone.utc)
        c._db_set_last_post_time("comment", "123", t)
        got = c._db_get_last_post_time("comment", "123")
        self.assertIsNotNone(got)
        self.assertEqual(int(got.timestamp()), int(t.timestamp()))

    def test_scenario_msg_history_roundtrip_and_clear(self) -> None:
        c = self.commentator
        c._scenario_history_set("-100999", 42, 1, 777)
        c._scenario_history_set("-100999", 42, 2, 888)
        hist = c._scenario_history_load("-100999", 42)
        self.assertEqual(hist.get(1), 777)
        self.assertEqual(hist.get(2), 888)
        c._scenario_history_clear("-100999", 42)
        hist2 = c._scenario_history_load("-100999", 42)
        self.assertEqual(hist2, {})


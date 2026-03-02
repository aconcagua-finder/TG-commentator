import os
import tempfile
import unittest


class _ClientStub:
    def __init__(self, session_name: str) -> None:
        self.session_name = session_name


class TestPostCommentPlans(unittest.TestCase):
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

    def test_plan_persists_and_skips_already_commented_accounts(self) -> None:
        c = self.commentator
        eligible = [_ClientStub(n) for n in ["a", "b", "c", "d"]]
        target = {"accounts_per_post_min": 2, "accounts_per_post_max": 3}

        selected, planned_count, already_count, already_accounts = c._select_accounts_for_post(
            chat_key="123",
            post_id=1001,
            destination_chat_id=-100777,
            target_chat=target,
            eligible_clients=eligible,
        )
        self.assertTrue(2 <= planned_count <= 3)
        self.assertEqual(already_count, 0)
        self.assertEqual(already_accounts, set())
        self.assertEqual(len(selected), planned_count)

        # Simulate one account already commented (e.g. restart mid-run).
        commented = selected[0].session_name
        c.log_action_to_db(
            {
                "type": "comment",
                "post_id": 1001,
                "comment": "ok",
                "date": "2026-02-19T00:00:00+00:00",
                "account": {"session_name": commented, "first_name": "x", "username": "x"},
                "target": {"destination_chat_id": -100777, "channel_id": "-100123"},
            }
        )

        selected2, planned_count2, already_count2, already_accounts2 = c._select_accounts_for_post(
            chat_key="123",
            post_id=1001,
            destination_chat_id=-100777,
            target_chat=target,
            eligible_clients=eligible,
        )
        self.assertEqual(planned_count2, planned_count)
        self.assertGreaterEqual(already_count2, 1)
        self.assertIn(commented, already_accounts2)
        self.assertNotIn(commented, [x.session_name for x in selected2])
        self.assertEqual(len(selected2), max(planned_count - 1, 0))

    def test_all_range_uses_all_accounts(self) -> None:
        c = self.commentator
        eligible = [_ClientStub(n) for n in ["a", "b", "c"]]
        target = {"accounts_per_post_min": 0, "accounts_per_post_max": 0}

        selected, planned_count, already_count, already_accounts = c._select_accounts_for_post(
            chat_key="123",
            post_id=2002,
            destination_chat_id=-100888,
            target_chat=target,
            eligible_clients=eligible,
        )
        self.assertEqual(planned_count, 3)
        self.assertEqual(already_count, 0)
        self.assertEqual(already_accounts, set())
        self.assertEqual(sorted([x.session_name for x in selected]), ["a", "b", "c"])


import unittest


class _MeStub:
    first_name = "Test"
    username = "test"


class _ClientStub:
    def __init__(self, name: str) -> None:
        self.name = name

    async def get_me(self):
        return _MeStub()


class _WrapperStub:
    def __init__(self, session_name: str) -> None:
        self.session_name = session_name
        self.client = _ClientStub(session_name)


class _MsgStub:
    def __init__(self, msg_id: int) -> None:
        self.id = msg_id


class TestDiscussionAutomation(unittest.IsolatedAsyncioTestCase):
    async def test_run_discussion_session_replies_in_chain_and_excludes_operator(self) -> None:
        import commentator

        c = commentator
        old_active = c.active_clients
        old_generate = c.generate_comment
        old_send = c.human_type_and_send
        old_ensure = c.ensure_client_connected
        old_log = c.log_action_to_db
        old_record = c._record_account_failure
        old_clear = c._clear_account_failure
        old_load_accounts = c.load_project_accounts
        old_shuffle = c.random.shuffle
        old_choice = c.random.choice

        sent = []
        msg_seq = {"next": 200}

        async def _fake_generate_comment(
            post_text,
            target_chat,
            session_name,
            image_bytes=None,
            is_reply_mode=False,
            reply_to_name=None,
            extra_instructions=None,
        ):
            return f"reply:{session_name}", "test"

        async def _fake_send(client, chat_id, text, reply_to_msg_id=None, **kwargs):
            sent.append((client.name, text, reply_to_msg_id))
            mid = msg_seq["next"]
            msg_seq["next"] = mid + 1
            return _MsgStub(mid)

        async def _ensure_ok(*args, **kwargs):  # noqa: ANN001
            return True

        try:
            # Deterministic participant selection.
            c.random.shuffle = lambda _x: None
            c.random.choice = lambda seq: seq[0]

            c.active_clients = {
                "op": _WrapperStub("op"),
                "b": _WrapperStub("b"),
                "c": _WrapperStub("c"),
            }
            c.generate_comment = _fake_generate_comment
            c.human_type_and_send = _fake_send
            c.ensure_client_connected = _ensure_ok
            c.log_action_to_db = lambda *args, **kwargs: None
            c._record_account_failure = lambda *args, **kwargs: None
            c._clear_account_failure = lambda *args, **kwargs: None
            c.load_project_accounts = lambda *_args, **_kwargs: [
                {"session_name": "op", "sleep_settings": {"start_hour": 0, "end_hour": 0}},
                {"session_name": "b", "sleep_settings": {"start_hour": 0, "end_hour": 0}},
                {"session_name": "c", "sleep_settings": {"start_hour": 0, "end_hour": 0}},
            ]

            target = {
                "operator_session_name": "op",
                "assigned_accounts": ["op", "b", "c"],
                "turns_min": 2,
                "turns_max": 2,
                "initial_delay_min": 0,
                "initial_delay_max": 0,
                "delay_between_min": 0,
                "delay_between_max": 0,
            }

            await c.run_discussion_session(
                chat_id=123,
                chat_bare_id=123,
                seed_msg_id=100,
                seed_text=">> hello",
                target=target,
            )

            # Operator is excluded; b then c are used.
            self.assertEqual([s[0] for s in sent], ["b", "c"])
            self.assertEqual([s[1] for s in sent], ["reply:b", "reply:c"])
            # Reply chain: first replies to seed, second replies to first sent message.
            self.assertEqual([s[2] for s in sent], [100, 200])
        finally:
            c.active_clients = old_active
            c.generate_comment = old_generate
            c.human_type_and_send = old_send
            c.ensure_client_connected = old_ensure
            c.log_action_to_db = old_log
            c._record_account_failure = old_record
            c._clear_account_failure = old_clear
            c.load_project_accounts = old_load_accounts
            c.random.shuffle = old_shuffle
            c.random.choice = old_choice

    async def test_run_discussion_session_skips_connect_failure_and_tries_other_participant(self) -> None:
        import commentator

        c = commentator
        old_active = c.active_clients
        old_generate = c.generate_comment
        old_send = c.human_type_and_send
        old_ensure = c.ensure_client_connected
        old_log = c.log_action_to_db
        old_record = c._record_account_failure
        old_clear = c._clear_account_failure
        old_load_accounts = c.load_project_accounts
        old_shuffle = c.random.shuffle

        sent = []
        msg_seq = {"next": 200}

        async def _fake_generate_comment(
            post_text,
            target_chat,
            session_name,
            image_bytes=None,
            is_reply_mode=False,
            reply_to_name=None,
            extra_instructions=None,
        ):
            return f"reply:{session_name}", "test"

        async def _fake_send(client, chat_id, text, reply_to_msg_id=None, **kwargs):
            sent.append((client.name, text, reply_to_msg_id))
            mid = msg_seq["next"]
            msg_seq["next"] = mid + 1
            return _MsgStub(mid)

        async def _ensure_first_fails(wrapper, *args, **kwargs):  # noqa: ANN001
            return wrapper.session_name != "b"

        try:
            c.random.shuffle = lambda _x: None
            c.active_clients = {
                "op": _WrapperStub("op"),
                "b": _WrapperStub("b"),
                "c": _WrapperStub("c"),
            }
            c.generate_comment = _fake_generate_comment
            c.human_type_and_send = _fake_send
            c.ensure_client_connected = _ensure_first_fails
            c.log_action_to_db = lambda *args, **kwargs: None
            c._record_account_failure = lambda *args, **kwargs: None
            c._clear_account_failure = lambda *args, **kwargs: None
            c.load_project_accounts = lambda *_args, **_kwargs: [
                {"session_name": "op", "sleep_settings": {"start_hour": 0, "end_hour": 0}},
                {"session_name": "b", "sleep_settings": {"start_hour": 0, "end_hour": 0}},
                {"session_name": "c", "sleep_settings": {"start_hour": 0, "end_hour": 0}},
            ]

            target = {
                "operator_session_name": "op",
                "assigned_accounts": ["op", "b", "c"],
                "turns_min": 1,
                "turns_max": 1,
                "initial_delay_min": 0,
                "initial_delay_max": 0,
                "delay_between_min": 0,
                "delay_between_max": 0,
            }

            await c.run_discussion_session(
                chat_id=123,
                chat_bare_id=123,
                seed_msg_id=100,
                seed_text=">> hello",
                target=target,
            )

            # b can't connect => c is used instead.
            self.assertEqual([s[0] for s in sent], ["c"])
            self.assertEqual([s[1] for s in sent], ["reply:c"])
            self.assertEqual([s[2] for s in sent], [100])
        finally:
            c.active_clients = old_active
            c.generate_comment = old_generate
            c.human_type_and_send = old_send
            c.ensure_client_connected = old_ensure
            c.log_action_to_db = old_log
            c._record_account_failure = old_record
            c._clear_account_failure = old_clear
            c.load_project_accounts = old_load_accounts
            c.random.shuffle = old_shuffle

    async def test_run_discussion_session_runs_scenes_with_operator_phrase(self) -> None:
        import commentator

        c = commentator
        old_active = c.active_clients
        old_generate = c.generate_comment
        old_send = c.human_type_and_send
        old_ensure = c.ensure_client_connected
        old_log = c.log_action_to_db
        old_record = c._record_account_failure
        old_clear = c._clear_account_failure
        old_load_accounts = c.load_project_accounts
        old_shuffle = c.random.shuffle
        old_choice = c.random.choice

        sent = []
        msg_seq = {"next": 200}

        async def _fake_generate_comment(
            post_text,
            target_chat,
            session_name,
            image_bytes=None,
            is_reply_mode=False,
            reply_to_name=None,
            extra_instructions=None,
        ):
            return f"reply:{session_name}", "test"

        async def _fake_send(client, chat_id, text, reply_to_msg_id=None, **kwargs):
            sent.append((client.name, text, reply_to_msg_id))
            mid = msg_seq["next"]
            msg_seq["next"] = mid + 1
            return _MsgStub(mid)

        async def _ensure_ok(*args, **kwargs):  # noqa: ANN001
            return True

        try:
            # Deterministic participant selection.
            c.random.shuffle = lambda _x: None
            c.random.choice = lambda seq: seq[0]

            c.active_clients = {
                "op": _WrapperStub("op"),
                "b": _WrapperStub("b"),
                "c": _WrapperStub("c"),
            }
            c.generate_comment = _fake_generate_comment
            c.human_type_and_send = _fake_send
            c.ensure_client_connected = _ensure_ok
            c.log_action_to_db = lambda *args, **kwargs: None
            c._record_account_failure = lambda *args, **kwargs: None
            c._clear_account_failure = lambda *args, **kwargs: None
            c.load_project_accounts = lambda *_args, **_kwargs: [
                {"session_name": "op", "sleep_settings": {"start_hour": 0, "end_hour": 0}},
                {"session_name": "b", "sleep_settings": {"start_hour": 0, "end_hour": 0}},
                {"session_name": "c", "sleep_settings": {"start_hour": 0, "end_hour": 0}},
            ]

            target = {
                "operator_session_name": "op",
                "assigned_accounts": ["op", "b", "c"],
                "turns_min": 1,
                "turns_max": 1,
                "initial_delay_min": 0,
                "initial_delay_max": 0,
                "delay_between_min": 0,
                "delay_between_max": 0,
                "scenes": [
                    {
                        "operator_text": "next",
                        "turns_min": 1,
                        "turns_max": 1,
                        "initial_delay_min": 0,
                        "initial_delay_max": 0,
                        "delay_between_min": 0,
                        "delay_between_max": 0,
                    }
                ],
            }

            await c.run_discussion_session(
                chat_id=123,
                chat_bare_id=123,
                seed_msg_id=100,
                seed_text=">> hello",
                target=target,
            )

            self.assertEqual([s[0] for s in sent], ["b", "op", "c"])
            self.assertEqual([s[1] for s in sent], ["reply:b", "next", "reply:c"])
            # Reply chain: seed -> b -> operator(scene) -> c
            self.assertEqual([s[2] for s in sent], [100, 200, 201])
        finally:
            c.active_clients = old_active
            c.generate_comment = old_generate
            c.human_type_and_send = old_send
            c.ensure_client_connected = old_ensure
            c.log_action_to_db = old_log
            c._record_account_failure = old_record
            c._clear_account_failure = old_clear
            c.load_project_accounts = old_load_accounts
            c.random.shuffle = old_shuffle
            c.random.choice = old_choice


class TestDiscussionSeedExtraction(unittest.TestCase):
    def test_extract_discussion_seed_prefix(self) -> None:
        import commentator

        self.assertEqual(commentator._extract_discussion_seed(">> hi", ">>"), "hi")
        self.assertEqual(commentator._extract_discussion_seed(">>   hi", ">>"), "hi")
        self.assertIsNone(commentator._extract_discussion_seed("hi", ">>"))
        self.assertIsNone(commentator._extract_discussion_seed(">>", ">>"))

    def test_extract_discussion_seed_empty_prefix(self) -> None:
        import commentator

        self.assertEqual(commentator._extract_discussion_seed("hi", ""), "hi")
        self.assertIsNone(commentator._extract_discussion_seed("", ""))

    def test_extract_discussion_seed_optional_prefix(self) -> None:
        import commentator

        self.assertEqual(commentator._extract_discussion_seed_optional_prefix(">> hi", ">>"), "hi")
        self.assertEqual(commentator._extract_discussion_seed_optional_prefix("hi", ">>"), "hi")
        self.assertIsNone(commentator._extract_discussion_seed_optional_prefix(">>", ">>"))


class TestDiscussionQueue(unittest.IsolatedAsyncioTestCase):
    async def test_process_discussion_queue_schedules_and_clears(self) -> None:
        import commentator

        c = commentator
        old_settings = c.current_settings
        old_schedule = c._schedule_discussion_run
        old_save = c.save_data

        called = []
        saved = []

        def _fake_schedule_discussion_run(**kwargs):  # noqa: ANN003
            called.append(kwargs)

        def _fake_save(_path, data):  # noqa: ANN001
            saved.append(data)

        try:
            c.current_settings = {
                "active_project_id": "default",
                "discussion_targets": [
                    {
                        "chat_id": "-1001",
                        "linked_chat_id": "-1002",
                        "enabled": True,
                        "operator_session_name": "op",
                        "assigned_accounts": ["b", "c"],
                    }
                ],
                "discussion_queue": [
                    {
                        "project_id": "default",
                        "discussion_target_chat_id": "-1001",
                        "chat_id": "-1002",
                        "seed_msg_id": 123,
                        "seed_text": "hello",
                        "created_at": 0.0,
                    }
                ],
            }
            c._schedule_discussion_run = _fake_schedule_discussion_run
            c.save_data = _fake_save

            await c.process_discussion_queue()

            self.assertEqual(len(called), 1)
            self.assertEqual(c.current_settings.get("discussion_queue"), [])
            self.assertTrue(saved, "settings should be saved after clearing queue")
        finally:
            c.current_settings = old_settings
            c._schedule_discussion_run = old_schedule
            c.save_data = old_save


if __name__ == "__main__":
    unittest.main()

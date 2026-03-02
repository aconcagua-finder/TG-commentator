import unittest


class _ReplyStub:
    def __init__(self, reply_to_msg_id: int | None) -> None:
        self.reply_to_msg_id = reply_to_msg_id


class _MsgStub:
    def __init__(
        self,
        msg_id: int,
        sender_id: int,
        chat_id: int = 1,
        reply_to_msg_id: int | None = None,
    ) -> None:
        self.id = msg_id
        self.sender_id = sender_id
        self.chat_id = chat_id
        self.reply_to = _ReplyStub(reply_to_msg_id) if reply_to_msg_id else None


class _ClientStub:
    def __init__(self, messages_by_id: dict[int, _MsgStub]) -> None:
        self._messages = messages_by_id

    async def get_messages(self, chat_id, ids):  # noqa: ANN001
        return self._messages.get(int(ids))


class _EventStub:
    def __init__(self, client: _ClientStub, chat_id: int, message: _MsgStub) -> None:
        self.client = client
        self.chat_id = chat_id
        self.message = message


class TestDialogueAiReplyLimit(unittest.IsolatedAsyncioTestCase):
    async def test_counts_ai_replies_in_reply_chain(self) -> None:
        import commentator

        our_ids = {10, 11}
        # user(1) -> ai(10) -> user(1) -> ai(11) -> user(1)
        m100 = _MsgStub(100, sender_id=1)
        m101 = _MsgStub(101, sender_id=10, reply_to_msg_id=100)
        m102 = _MsgStub(102, sender_id=1, reply_to_msg_id=101)
        m103 = _MsgStub(103, sender_id=11, reply_to_msg_id=102)
        m104 = _MsgStub(104, sender_id=1, reply_to_msg_id=103)

        client = _ClientStub({100: m100, 101: m101, 102: m102, 103: m103, 104: m104})

        count_104 = await commentator.count_dialogue_ai_replies(
            client,
            m104,
            our_ids=our_ids,
            max_depth=10,
            include_current=True,
        )
        self.assertEqual(count_104, 2)

        count_102 = await commentator.count_dialogue_ai_replies(
            client,
            m102,
            our_ids=our_ids,
            max_depth=10,
            include_current=True,
        )
        self.assertEqual(count_102, 1)

        count_103 = await commentator.count_dialogue_ai_replies(
            client,
            m103,
            our_ids=our_ids,
            max_depth=10,
            include_current=True,
        )
        # Includes current AI message (m103) + previous AI message (m101).
        self.assertEqual(count_103, 2)

    async def test_errors_are_treated_as_limit_reached_when_early_stop_is_set(self) -> None:
        import commentator

        class _ErrorClient:
            async def get_messages(self, chat_id, ids):  # noqa: ANN001
                raise RuntimeError("boom")

        m = _MsgStub(200, sender_id=1, reply_to_msg_id=199)
        count = await commentator.count_dialogue_ai_replies(
            _ErrorClient(),
            m,
            our_ids={10},
            max_depth=10,
            include_current=True,
            early_stop=2,
        )
        self.assertEqual(count, 2)

    async def test_process_reply_to_comment_skips_when_limit_reached(self) -> None:
        import commentator

        c = commentator
        our_ids = {10, 11}
        accounts_data = [
            {"session_name": "a", "sleep_settings": {"start_hour": 0, "end_hour": 0}},
        ]
        m100 = _MsgStub(1000, sender_id=1)
        m101 = _MsgStub(1001, sender_id=10, reply_to_msg_id=1000)
        m102 = _MsgStub(1002, sender_id=1, reply_to_msg_id=1001)
        m103 = _MsgStub(1003, sender_id=11, reply_to_msg_id=1002)
        m104 = _MsgStub(1004, sender_id=1, reply_to_msg_id=1003)
        m104.text = "hi"

        client = _ClientStub({m.id: m for m in [m100, m101, m102, m103, m104]})
        event = _EventStub(client=client, chat_id=1, message=m104)

        async def _depth_ok(*args, **kwargs):  # noqa: ANN001
            return True

        old_cache = c.REPLY_PROCESS_CACHE
        old_active = c.active_clients
        old_load = c.load_project_accounts
        old_get_ids = c.get_all_our_user_ids
        old_depth = c.check_dialogue_depth
        old_create_task = c.asyncio.create_task

        try:
            c.REPLY_PROCESS_CACHE = set()
            c.active_clients = {"a": type("W", (), {"session_name": "a", "user_id": 999})()}
            c.load_project_accounts = lambda *args, **kwargs: accounts_data
            c.get_all_our_user_ids = lambda: our_ids
            c.check_dialogue_depth = _depth_ok

            def _fail_create_task(*args, **kwargs):  # noqa: ANN001
                raise AssertionError("should not schedule execute_reply_with_fallback when limit reached")

            c.asyncio.create_task = _fail_create_task

            await c.process_reply_to_comment(
                event,
                {
                    "assigned_accounts": ["a"],
                    "intervention_chance": 100,
                    "max_dialogue_depth": 10,
                    "max_dialogue_ai_replies": 2,
                },
            )
        finally:
            c.REPLY_PROCESS_CACHE = old_cache
            c.active_clients = old_active
            c.load_project_accounts = old_load
            c.get_all_our_user_ids = old_get_ids
            c.check_dialogue_depth = old_depth
            c.asyncio.create_task = old_create_task


if __name__ == "__main__":
    unittest.main()

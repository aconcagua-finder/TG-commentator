import unittest


class _MeStub:
    first_name = "Test"
    username = "test"


class _ClientStub:
    async def get_me(self):
        return _MeStub()


class _ClientWrapperStub:
    def __init__(self, session_name: str = "stub") -> None:
        self.session_name = session_name
        self.client = _ClientStub()


class TestChatRepliesAlwaysQuote(unittest.IsolatedAsyncioTestCase):
    async def test_execute_reply_with_fallback_always_quotes(self) -> None:
        import commentator

        c = commentator
        wrapper = _ClientWrapperStub("a")
        captured = {}

        async def _fake_generate_comment(
            prompt_base,
            target_chat,
            session_name,
            image_bytes=None,
            is_reply_mode=False,
            reply_to_name=None,
        ):
            return "ok", "test"

        async def _fake_human_type_and_send(client, chat_id, text, reply_to_msg_id=None, **kwargs):
            captured["reply_to_msg_id"] = reply_to_msg_id
            return None

        old_generate_comment = c.generate_comment
        old_send = c.human_type_and_send
        old_log = c.log_action_to_db
        old_clear_fail = c._clear_account_failure

        try:
            c.generate_comment = _fake_generate_comment
            c.human_type_and_send = _fake_human_type_and_send
            c.log_action_to_db = lambda *args, **kwargs: None
            c._clear_account_failure = lambda *args, **kwargs: None

            await c.execute_reply_with_fallback(
                candidate_list=[wrapper],
                chat_id=123,
                target_chat={"tag_reply_chance": 0},
                prompt_base="hi",
                delay=0,
                reply_to_msg_id=777,
                reply_to_name="Bob",
                is_intervention=False,
            )

            self.assertEqual(captured.get("reply_to_msg_id"), 777)
        finally:
            c.generate_comment = old_generate_comment
            c.human_type_and_send = old_send
            c.log_action_to_db = old_log
            c._clear_account_failure = old_clear_fail


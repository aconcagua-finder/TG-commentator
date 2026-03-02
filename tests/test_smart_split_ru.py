import unittest


class TestSmartSplitRuNoComma(unittest.TestCase):
    def test_does_not_split_interjection_comma(self) -> None:
        import commentator

        text = "о, я как раз об этом думал"
        parts = commentator.split_text_smart_ru_no_comma(text)
        self.assertEqual(parts, [text])

    def test_does_not_split_fixed_to_do_comma(self) -> None:
        import commentator

        text = "для того, чтобы это не повторялось"
        parts = commentator.split_text_smart_ru_no_comma(text)
        self.assertEqual(parts, [text])

    def test_splits_on_sentence_boundary(self) -> None:
        import commentator

        text = "Да, согласен. Но есть нюанс"
        parts = commentator.split_text_smart_ru_no_comma(text)
        self.assertEqual(parts, ["Да, согласен.", "Но есть нюанс"])

    def test_splits_on_colon(self) -> None:
        import commentator

        text = "Слушай: это правда важно"
        parts = commentator.split_text_smart_ru_no_comma(text)
        self.assertEqual(parts, ["Слушай:", "это правда важно"])

    def test_splits_on_dash_and_keeps_dash_in_second_part(self) -> None:
        import commentator

        text = "Я так думаю - потому что это логично"
        parts = commentator.split_text_smart_ru_no_comma(text)
        self.assertEqual(parts, ["Я так думаю", "- потому что это логично"])


class _NoopAction:
    async def __aenter__(self):
        return None

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _ClientStub:
    def __init__(self) -> None:
        self.sent = []

    def action(self, chat_id, action):
        return _NoopAction()

    async def send_message(self, chat_id, message, reply_to=None):
        self.sent.append((message, reply_to))
        return None


class TestHumanTypeAndSendSmartSplit(unittest.IsolatedAsyncioTestCase):
    async def test_human_type_and_send_uses_smart_split_mode(self) -> None:
        import commentator

        c = commentator
        old_settings = c.current_settings
        old_sleep = c.asyncio.sleep
        old_random = c.random.random

        async def _sleep_stub(*args, **kwargs):
            return None

        try:
            c.current_settings = {
                "humanization": {
                    "split_chance": 100,
                    "typo_chance": 0,
                    "lowercase_chance": 0,
                    "comma_skip_chance": 0,
                    "max_words": 0,
                }
            }
            c.asyncio.sleep = _sleep_stub
            c.random.random = lambda: 0.0

            client = _ClientStub()
            text = "Да, согласен. Но есть нюанс и он довольно важный чтобы не забыть"
            await c.human_type_and_send(
                client,
                chat_id=123,
                text=text,
                reply_to_msg_id=777,
                split_mode="smart_ru_no_comma",
            )

            self.assertEqual(
                [m for (m, _r) in client.sent],
                ["Да, согласен.", "Но есть нюанс и он довольно важный чтобы не забыть"],
            )
            self.assertEqual([r for (_m, r) in client.sent], [777, 777])
        finally:
            c.current_settings = old_settings
            c.asyncio.sleep = old_sleep
            c.random.random = old_random


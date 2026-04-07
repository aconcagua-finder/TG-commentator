import json
import os
import tempfile
import unittest
from contextlib import ExitStack, contextmanager
from unittest.mock import ANY, AsyncMock, patch

import httpx


def _project_entry(project_id: str, name: str, *, telegram_bot=None) -> dict:
    project = {
        "id": project_id,
        "name": name,
        "created_at": "2026-03-23T00:00:00+00:00",
    }
    if telegram_bot is not None:
        project["telegram_bot"] = telegram_bot
    return project


class TestTelegramBotProjectSchema(unittest.TestCase):
    def test_project_schema_preserves_extra_fields(self) -> None:
        import admin_web.helpers as helpers

        settings = {
            "active_project_id": helpers.DEFAULT_PROJECT_ID,
            "projects": [
                {
                    "id": helpers.DEFAULT_PROJECT_ID,
                    "name": "Стандартный проект",
                    "created_at": "2026-03-23T00:00:00+00:00",
                    "telegram_bot": {"enabled": True, "bot_token": "12345678TOKEN", "chat_id": "-1001"},
                    "custom_flag": "keep-me",
                }
            ],
        }

        normalized = helpers._ensure_settings_schema(settings)
        project = normalized["projects"][0]
        self.assertEqual(project["custom_flag"], "keep-me")
        self.assertIn("telegram_bot", project)
        self.assertEqual(project["telegram_bot"]["chat_id"], "-1001")


class TestNotificationsRoutes(unittest.TestCase):
    @staticmethod
    def _build_settings():
        import admin_web.helpers as helpers

        settings = {
            "active_project_id": helpers.DEFAULT_PROJECT_ID,
            "projects": [
                _project_entry(
                    helpers.DEFAULT_PROJECT_ID,
                    "Стандартный проект",
                    telegram_bot={
                        "enabled": True,
                        "bot_token": "1234567890ABCDEF",
                        "chat_id": "-100111",
                        "events": {
                            "warnings": True,
                            "inbox_dm": True,
                            "inbox_replies": False,
                            "inbox_reactions": False,
                            "monitoring": True,
                        },
                    },
                ),
                _project_entry(
                    "p2",
                    "Второй проект",
                    telegram_bot={
                        "enabled": False,
                        "bot_token": "SECOND_TOKEN_1234",
                        "chat_id": "-100222",
                        "events": {
                            "warnings": False,
                            "inbox_dm": False,
                            "inbox_replies": False,
                            "inbox_reactions": False,
                            "monitoring": False,
                        },
                    },
                ),
            ],
        }
        return helpers._ensure_settings_schema(settings)

    @contextmanager
    def _client_context(self, settings=None):
        import app_paths
        import db.connection as db_connection
        import db.schema as db_schema
        import admin_web.helpers as helpers
        import admin_web.main as m
        import admin_web.routes.auth as auth_routes
        import admin_web.routes.notifications as notifications_routes
        import admin_web.templating as templating
        from fastapi.testclient import TestClient

        fd, db_path = tempfile.mkstemp(prefix="commentator-notifications-", suffix=".sqlite")
        os.close(fd)

        settings_fd, settings_path = tempfile.mkstemp(prefix="commentator-settings-", suffix=".json")
        os.close(settings_fd)

        accounts_fd, accounts_path = tempfile.mkstemp(prefix="commentator-accounts-", suffix=".json")
        os.close(accounts_fd)

        settings = settings or self._build_settings()
        with open(settings_path, "w", encoding="utf-8") as f:
            json.dump(settings, f, ensure_ascii=False, indent=2)
        with open(accounts_path, "w", encoding="utf-8") as f:
            json.dump([], f)

        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(app_paths, "DB_FILE", db_path))
                stack.enter_context(patch.object(app_paths, "SETTINGS_FILE", settings_path))
                stack.enter_context(patch.object(app_paths, "ACCOUNTS_FILE", accounts_path))
                stack.enter_context(patch.object(db_connection, "_sqlite_db_file", db_path))
                stack.enter_context(patch.object(db_connection, "DB_URL", None))
                stack.enter_context(patch.object(db_schema, "DB_URL", None))
                stack.enter_context(patch.object(m, "DB_FILE", db_path))
                stack.enter_context(patch.object(m, "SETTINGS_FILE", settings_path))
                stack.enter_context(patch.object(m, "ACCOUNTS_FILE", accounts_path))
                stack.enter_context(patch.object(helpers, "SETTINGS_FILE", settings_path))
                stack.enter_context(patch.object(helpers, "ACCOUNTS_FILE", accounts_path))
                stack.enter_context(patch.object(notifications_routes, "_load_settings", lambda: (settings, None)))
                stack.enter_context(patch.object(notifications_routes, "_save_settings", lambda _settings: None))
                stack.enter_context(patch.object(templating, "_load_settings", lambda: (settings, None)))
                stack.enter_context(patch.object(templating, "_load_accounts", lambda: ([], None)))

                with TestClient(m.app) as client:
                    login = client.post(
                        "/login",
                        data={
                            "username": auth_routes.ADMIN_WEB_USERNAME,
                            "password": auth_routes.ADMIN_WEB_PASSWORD,
                        },
                        follow_redirects=False,
                    )
                    self.assertEqual(login.status_code, 303)
                    yield client, settings, notifications_routes
        finally:
            for path in (db_path, settings_path, accounts_path):
                try:
                    os.remove(path)
                except Exception:
                    pass

    def test_notifications_page_renders_masked_saved_settings(self) -> None:
        with self._client_context() as (client, _settings, _notifications_routes):
            response = client.get("/notifications")
            self.assertEqual(response.status_code, 200)
            self.assertIn("Уведомления Telegram", response.text)
            self.assertIn("1234...CDEF", response.text)
            self.assertIn("Сохранённый bot token", response.text)
            self.assertIn("Новый bot token", response.text)
            self.assertIn("Кнопка «Показать» раскрывает только то, что ты вводишь сейчас в это поле.", response.text)
            self.assertIn("Адрес: -100111", response.text)
            self.assertIn("https://t.me/test23032206", response.text)
            self.assertIn("Сохранить и протестировать", response.text)

    def test_notifications_save_updates_only_active_project(self) -> None:
        with self._client_context() as (client, settings, _notifications_routes):
            response = client.post(
                "/notifications",
                data={
                    "enabled": "1",
                    "chat_id": "-100999",
                    "warnings": "1",
                    "inbox_replies": "1",
                },
                follow_redirects=False,
            )
            self.assertEqual(response.status_code, 303)

            default_project = next(project for project in settings["projects"] if project["id"] == "default")
            second_project = next(project for project in settings["projects"] if project["id"] == "p2")

            self.assertEqual(default_project["telegram_bot"]["bot_token"], "1234567890ABCDEF")
            self.assertEqual(default_project["telegram_bot"]["chat_id"], "-100999")
            self.assertTrue(default_project["telegram_bot"]["enabled"])
            self.assertEqual(
                default_project["telegram_bot"]["events"],
                {
                    "warnings": True,
                    "inbox_dm": False,
                    "inbox_replies": True,
                    "inbox_reactions": False,
                    "monitoring": False,
                    "spam_deleted": False,
                },
            )
            self.assertEqual(second_project["telegram_bot"]["chat_id"], "-100222")
            self.assertEqual(second_project["telegram_bot"]["bot_token"], "SECOND_TOKEN_1234")

    def test_notifications_test_route_uses_saved_credentials(self) -> None:
        with self._client_context() as (client, _settings, notifications_routes):
            send_test_message = AsyncMock(return_value={"ok": True})
            with patch.object(notifications_routes, "send_test_message", send_test_message):
                response = client.post("/notifications/test", follow_redirects=False)

            self.assertEqual(response.status_code, 303)
            send_test_message.assert_awaited_once_with("1234567890ABCDEF", "-100111")

    def test_notifications_save_resolves_chat_link_and_stores_metadata(self) -> None:
        with self._client_context() as (client, settings, notifications_routes):
            resolve_target = AsyncMock(
                return_value={
                    "chat_id": "-100777",
                    "chat_title": "Alerts Room",
                    "chat_username": "test23032206",
                }
            )
            with patch.object(notifications_routes, "_resolve_notification_chat_target", resolve_target):
                response = client.post(
                    "/notifications",
                    data={
                        "enabled": "1",
                        "chat_id": "https://t.me/test23032206",
                        "warnings": "1",
                    },
                    follow_redirects=False,
                )

            self.assertEqual(response.status_code, 303)
            resolve_target.assert_awaited_once_with(ANY, "https://t.me/test23032206")

            default_project = next(project for project in settings["projects"] if project["id"] == "default")
            self.assertEqual(default_project["telegram_bot"]["chat_id"], "-100777")
            self.assertEqual(default_project["telegram_bot"]["chat_title"], "Alerts Room")
            self.assertEqual(default_project["telegram_bot"]["chat_username"], "test23032206")

    def test_notifications_test_route_uses_public_link_without_authorized_accounts(self) -> None:
        with self._client_context() as (client, _settings, notifications_routes):
            send_test_message = AsyncMock(return_value={"ok": True})
            get_any_authorized_client = AsyncMock(side_effect=AssertionError("should not be called"))
            with patch.object(notifications_routes, "send_test_message", send_test_message):
                with patch.object(notifications_routes, "_get_any_authorized_client", get_any_authorized_client):
                    response = client.post(
                        "/notifications/test",
                        data={"chat_id": "https://t.me/test23032206"},
                        follow_redirects=False,
                    )

            self.assertEqual(response.status_code, 303)
            send_test_message.assert_awaited_once_with("1234567890ABCDEF", "@test23032206")
            get_any_authorized_client.assert_not_awaited()

    def test_notifications_test_route_saves_current_form_before_send(self) -> None:
        with self._client_context() as (client, settings, notifications_routes):
            resolve_target = AsyncMock(
                return_value={
                    "chat_id": "-100888",
                    "chat_title": "Alerts Room",
                    "chat_username": "test23032206",
                }
            )
            send_test_message = AsyncMock(return_value={"ok": True})
            with patch.object(notifications_routes, "_resolve_notification_chat_target", resolve_target):
                with patch.object(notifications_routes, "send_test_message", send_test_message):
                    response = client.post(
                        "/notifications/test",
                        data={
                            "enabled": "1",
                            "bot_token": "NEW_TOKEN_999",
                            "chat_id": "https://t.me/+AbCdEf123456",
                            "warnings": "1",
                            "inbox_replies": "1",
                        },
                        follow_redirects=False,
                    )

            self.assertEqual(response.status_code, 303)
            resolve_target.assert_awaited_once_with(ANY, "https://t.me/+AbCdEf123456")
            send_test_message.assert_awaited_once_with("NEW_TOKEN_999", "-100888")

            default_project = next(project for project in settings["projects"] if project["id"] == "default")
            self.assertEqual(default_project["telegram_bot"]["bot_token"], "NEW_TOKEN_999")
            self.assertEqual(default_project["telegram_bot"]["chat_id"], "-100888")
            self.assertEqual(default_project["telegram_bot"]["chat_title"], "Alerts Room")
            self.assertEqual(default_project["telegram_bot"]["chat_username"], "test23032206")
            self.assertTrue(default_project["telegram_bot"]["enabled"])
            self.assertEqual(
                default_project["telegram_bot"]["events"],
                {
                    "warnings": True,
                    "inbox_dm": False,
                    "inbox_replies": True,
                    "inbox_reactions": False,
                    "monitoring": False,
                    "spam_deleted": False,
                },
            )


class TestTelegramBotService(unittest.IsolatedAsyncioTestCase):
    def test_resolve_project_id_for_session_reads_accounts_file(self) -> None:
        import services.telegram_bot as telegram_bot

        fd, accounts_path = tempfile.mkstemp(prefix="commentator-telegram-accounts-", suffix=".json")
        os.close(fd)
        try:
            with open(accounts_path, "w", encoding="utf-8") as f:
                json.dump(
                    [
                        {"session_name": "Telegram17", "project_id": "project-17"},
                        {"session_name": "Telegram18"},
                    ],
                    f,
                    ensure_ascii=False,
                    indent=2,
                )

            with patch.object(telegram_bot, "ACCOUNTS_FILE", accounts_path):
                self.assertEqual(
                    telegram_bot.resolve_project_id_for_session("Telegram17", {}),
                    "project-17",
                )
                self.assertEqual(
                    telegram_bot.resolve_project_id_for_session("Telegram18", {}),
                    telegram_bot.DEFAULT_PROJECT_ID,
                )
                self.assertEqual(
                    telegram_bot.resolve_project_id_for_session("Unknown", {}),
                    telegram_bot.DEFAULT_PROJECT_ID,
                )
        finally:
            try:
                os.remove(accounts_path)
            except Exception:
                pass

    def test_escape_html_escapes_special_chars(self) -> None:
        from services.telegram_bot import escape_html

        self.assertEqual(escape_html("<tag> & done"), "&lt;tag&gt; &amp; done")

    async def test_notify_event_skips_disabled_or_unconfigured_settings(self) -> None:
        from services.telegram_bot import notify_event

        settings = {
            "active_project_id": "default",
            "projects": [
                _project_entry(
                    "default",
                    "Default",
                    telegram_bot={
                        "enabled": False,
                        "bot_token": "token",
                        "chat_id": "-1001",
                        "events": {"warnings": True},
                    },
                )
            ],
        }

        result = await notify_event("warnings", "default", "hello", settings=settings)
        self.assertEqual(result["reason"], "disabled")

    async def test_notify_event_sends_when_enabled(self) -> None:
        import services.telegram_bot as telegram_bot

        settings = {
            "active_project_id": "default",
            "projects": [
                _project_entry(
                    "default",
                    "Default",
                    telegram_bot={
                        "enabled": True,
                        "bot_token": "token",
                        "chat_id": "-1001",
                        "events": {
                            "warnings": True,
                            "inbox_dm": False,
                            "inbox_replies": False,
                            "inbox_reactions": False,
                            "monitoring": False,
                        },
                    },
                )
            ],
        }

        send_notification = AsyncMock(return_value={"ok": True})
        with patch.object(telegram_bot, "send_notification", send_notification):
            result = await telegram_bot.notify_event("warnings", "default", "hello", settings=settings)

        self.assertEqual(result, {"ok": True})
        send_notification.assert_awaited_once_with("token", "-1001", "hello")

    async def test_send_notification_returns_error_payload_from_bot_api(self) -> None:
        from services.telegram_bot import send_notification

        request = httpx.Request("POST", "https://api.telegram.org/botTOKEN/sendMessage")
        response = httpx.Response(
            400,
            json={"ok": False, "description": "Bad Request: chat not found"},
            request=request,
        )

        async_post = AsyncMock(return_value=response)
        with patch("services.telegram_bot.httpx.AsyncClient.post", async_post):
            result = await send_notification("TOKEN", "-1001", "hello")

        self.assertFalse(result["ok"])
        self.assertEqual(result["status_code"], 400)
        self.assertEqual(result["description"], "Bad Request: chat not found")


class TestWarningNotifier(unittest.IsolatedAsyncioTestCase):
    async def test_warning_notifier_sends_only_for_new_warning(self) -> None:
        import app_paths
        import db.connection as db_connection
        import db.schema as db_schema
        import admin_web.helpers as helpers
        import services.warning_notifier as warning_notifier

        fd, db_path = tempfile.mkstemp(prefix="commentator-warning-notifier-", suffix=".sqlite")
        os.close(fd)

        settings = helpers._ensure_settings_schema(
            {
                "active_project_id": "project-1",
                "projects": [
                    _project_entry(
                        "project-1",
                        "Project 1",
                        telegram_bot={
                            "enabled": True,
                            "bot_token": "TOKEN",
                            "chat_id": "-100100",
                            "events": {
                                "warnings": True,
                                "inbox_dm": False,
                                "inbox_replies": False,
                                "inbox_reactions": False,
                                "monitoring": False,
                            },
                        },
                    )
                ],
            }
        )
        accounts = [
            {
                "session_name": "Telegram17",
                "status": "limited",
                "project_id": "project-1",
            }
        ]

        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(app_paths, "DB_FILE", db_path))
                stack.enter_context(patch.object(db_connection, "_sqlite_db_file", db_path))
                stack.enter_context(patch.object(db_connection, "DB_URL", None))
                stack.enter_context(patch.object(db_schema, "DB_URL", None))
                stack.enter_context(patch.object(warning_notifier, "_load_accounts", lambda: (accounts, None)))

                with db_connection.get_connection() as conn:
                    db_schema.init_database(conn)

                notify_event = AsyncMock(return_value={"ok": True})
                with patch.object(warning_notifier, "notify_event", notify_event):
                    sent_first = await warning_notifier.check_warning_notifications(current_settings=settings)
                    sent_second = await warning_notifier.check_warning_notifications(current_settings=settings)

                self.assertEqual(sent_first, 1)
                self.assertEqual(sent_second, 0)
                notify_event.assert_awaited_once()

                with db_connection.get_connection() as conn:
                    row = conn.execute(
                        "SELECT key, resolved_at FROM warning_history ORDER BY id DESC LIMIT 1"
                    ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual(row["key"], helpers._warning_key_status("Telegram17", "limited"))
                self.assertIsNone(row["resolved_at"])
        finally:
            try:
                os.remove(db_path)
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main()

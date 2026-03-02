import unittest

import role_engine as re_role


class TestRoleEngine(unittest.TestCase):
    def test_ensure_role_schema_populates_defaults(self) -> None:
        settings = {}
        changed = re_role.ensure_role_schema(settings)
        self.assertTrue(changed)
        roles = settings.get("roles") or {}
        self.assertGreaterEqual(len(roles), 8)
        self.assertIn(re_role.DEFAULT_ROLE_ID, roles)
        self.assertEqual(settings.get("default_role_id"), re_role.DEFAULT_ROLE_ID)

    def test_legacy_persona_is_mapped_to_role(self) -> None:
        settings = {
            "personas": {
                "123": {"name": "Legacy", "prompt": "legacy prompt"},
            }
        }
        re_role.ensure_role_schema(settings)
        accounts = [{"session_name": "acc", "persona_id": "123"}]
        changed = re_role.ensure_accounts_have_roles(accounts, settings)
        self.assertTrue(changed)
        self.assertEqual(accounts[0].get("role_id"), re_role.legacy_role_id("123"))

    def test_build_role_prompt_uses_selected_moods(self) -> None:
        settings = {}
        re_role.ensure_role_schema(settings)
        role = {
            "name": "Test",
            "character_preset_id": "character_balanced",
            "behavior_preset_id": "behavior_balanced",
            "mood_preset_ids": ["mood_thoughtful"],
            "humanization_preset_id": "human_natural",
            "character_prompt_override": "",
            "behavior_prompt_override": "",
            "humanization_prompt_override": "",
            "emoji_level": "minimal",
            "gender": "neutral",
            "custom_prompt": "",
        }
        prompt, info = re_role.build_role_prompt(role, settings)
        self.assertIn("Характер:", prompt)
        self.assertIn("2-40 слов", prompt)
        self.assertEqual(info.get("mood_id"), "mood_thoughtful")

    def test_build_role_prompt_uses_character_override(self) -> None:
        settings = {}
        re_role.ensure_role_schema(settings)
        role = {
            "name": "Test",
            "character_preset_id": "character_balanced",
            "behavior_preset_id": "behavior_balanced",
            "mood_preset_ids": ["mood_neutral"],
            "humanization_preset_id": "human_natural",
            "character_prompt_override": "МОЙ ХАРАКТЕР",
            "behavior_prompt_override": "",
            "humanization_prompt_override": "",
            "emoji_level": "minimal",
            "gender": "neutral",
            "custom_prompt": "",
        }
        prompt, _ = re_role.build_role_prompt(role, settings)
        self.assertIn("Характер: МОЙ ХАРАКТЕР", prompt)

    def test_ensure_role_schema_refreshes_builtin_presets(self) -> None:
        settings = {
            "role_presets": {
                "character": {
                    "character_balanced": {
                        "name": "Старое название",
                        "prompt": "старый кривой промпт",
                        "builtin": False,
                    }
                }
            }
        }
        re_role.ensure_role_schema(settings)
        character = settings["role_presets"]["character"]["character_balanced"]
        self.assertTrue(character.get("builtin"))
        self.assertNotEqual(character.get("prompt"), "старый кривой промпт")

    def test_enforce_emoji_level_none_removes_emoji(self) -> None:
        text = "Нормально 😄 всё ок 👍"
        self.assertNotIn("😄", re_role.enforce_emoji_level(text, "none"))
        self.assertNotIn("👍", re_role.enforce_emoji_level(text, "none"))

    def test_custom_role_is_used_for_account(self) -> None:
        settings: dict = {}
        re_role.ensure_role_schema(settings)
        account = {
            "session_name": "acc",
            "role_id": re_role.CUSTOM_ROLE_ID,
            re_role.ACCOUNT_CUSTOM_ROLE_KEY: {
                "character_preset_id": "character_cheerful",
                "behavior_preset_id": "behavior_supportive",
                "mood_preset_ids": ["mood_optimistic"],
                "humanization_preset_id": "human_expressive",
                "emoji_level": "active",
                "gender": "female",
                "custom_prompt": "Тест",
            },
        }
        role_id, role = re_role.role_for_account(account, settings)
        self.assertEqual(role_id, re_role.CUSTOM_ROLE_ID)
        self.assertEqual(role.get("name"), re_role.CUSTOM_ROLE_NAME)
        self.assertEqual(role.get("emoji_level"), "active")

    def test_ensure_accounts_have_roles_keeps_custom_role(self) -> None:
        settings: dict = {}
        re_role.ensure_role_schema(settings)
        accounts = [
            {
                "session_name": "acc",
                "role_id": re_role.CUSTOM_ROLE_ID,
                re_role.ACCOUNT_CUSTOM_ROLE_KEY: {
                    "character_preset_id": "character_balanced",
                    "behavior_preset_id": "behavior_balanced",
                    "mood_preset_ids": ["mood_neutral"],
                    "humanization_preset_id": "human_natural",
                    "emoji_level": "minimal",
                    "gender": "neutral",
                    "custom_prompt": "",
                },
            }
        ]
        changed = re_role.ensure_accounts_have_roles(accounts, settings)
        self.assertFalse(changed)
        self.assertEqual(accounts[0].get("role_id"), re_role.CUSTOM_ROLE_ID)

    def test_ensure_accounts_have_roles_fixes_invalid_custom_role(self) -> None:
        settings: dict = {}
        re_role.ensure_role_schema(settings)
        accounts = [{"session_name": "acc", "role_id": re_role.CUSTOM_ROLE_ID}]
        changed = re_role.ensure_accounts_have_roles(accounts, settings)
        self.assertTrue(changed)
        self.assertEqual(accounts[0].get("role_id"), re_role.DEFAULT_ROLE_ID)


if __name__ == "__main__":
    unittest.main()

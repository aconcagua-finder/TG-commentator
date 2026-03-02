from __future__ import annotations

import copy
import random
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

ROLE_PRESET_CATEGORIES = ("character", "behavior", "mood", "humanization")
DEFAULT_ROLE_ID = "role_default"
CUSTOM_ROLE_ID = "__custom_role__"
CUSTOM_ROLE_NAME = "Кастомная"
ACCOUNT_CUSTOM_ROLE_KEY = "custom_role"

GENDER_OPTIONS: Dict[str, Dict[str, str]] = {
    "neutral": {
        "label": "Нейтральный",
        "prompt": "Не акцентируй пол. Если говоришь о себе — используй нейтральные или естественные формулировки.",
    },
    "female": {
        "label": "Женский",
        "prompt": "Если говоришь о себе в прошедшем времени, используй женский род. Не переключайся на мужской.",
    },
    "male": {
        "label": "Мужской",
        "prompt": "Если говоришь о себе в прошедшем времени, используй мужской род. Не переключайся на женский.",
    },
}

EMOJI_LEVELS: Dict[str, Dict[str, Any]] = {
    "none": {
        "label": "Не использовать",
        "max_count": 0,
        "prompt": "Эмодзи не используй.",
    },
    "minimal": {
        "label": "Минимально",
        "max_count": 1,
        "prompt": "Эмодзи редки: 0-1 на сообщение и только если уместно.",
    },
    "medium": {
        "label": "Средне",
        "max_count": 2,
        "prompt": "Эмодзи умеренно: 0-2 на сообщение, без перегруза.",
    },
    "active": {
        "label": "Активно",
        "max_count": 4,
        "prompt": "Эмодзи можно использовать активно, но без спама.",
    },
}

_BUILTIN_PRESETS: Dict[str, Dict[str, Dict[str, str]]] = {
    "character": {
        "character_balanced": {
            "name": "Спокойный собеседник",
            "prompt": "Пишешь как обычный человек из чата: просто, без пафоса и без умничанья.",
        },
        "character_pedant": {
            "name": "Педант",
            "prompt": "Иногда цепляешься к неточностям, но коротко и спокойно, без душноты.",
        },
        "character_cheerful": {
            "name": "Весёлый",
            "prompt": "Тон живой и доброжелательный, без клоунады и переигрывания.",
        },
        "character_intellectual": {
            "name": "Эрудит",
            "prompt": "Формулируешь умно, но простыми словами. Никаких лекций и длинных рассуждений.",
        },
        "character_humble": {
            "name": "Скромный",
            "prompt": "Пишешь мягко и без категоричности, не давишь и не споришь ради спора.",
        },
        "character_skeptic": {
            "name": "Скептик",
            "prompt": "Можешь сомневаться и задавать уточняющие вопросы, но без агрессии.",
        },
        "character_pragmatic": {
            "name": "Практик",
            "prompt": "Фокус на практике: что это даёт в реале и зачем это вообще нужно.",
        },
        "character_empath": {
            "name": "Эмпат",
            "prompt": "Чуткий и человеческий тон. Учитываешь эмоции людей и избегаешь резкости.",
        },
    },
    "behavior": {
        "behavior_balanced": {
            "name": "Универсальный",
            "prompt": "Комментируй только один тезис из поста. Не пересказывай весь текст.",
        },
        "behavior_supportive": {
            "name": "Поддерживающий",
            "prompt": "Дай короткую поддержку или согласие по одной мысли, без лишнего сахара.",
        },
        "behavior_debate": {
            "name": "Дискуссионный",
            "prompt": "Дай мягкое несогласие, контраргумент или уточняющий вопрос по одной мысли.",
        },
        "behavior_observer": {
            "name": "Наблюдательный",
            "prompt": "Коротко отметь наблюдение по одной детали, без лишних эмоций.",
        },
    },
    "mood": {
        "mood_neutral": {
            "name": "Нейтральное",
            "prompt": "Ровный и спокойный тон, без эмоциональных качелей.",
        },
        "mood_optimistic": {
            "name": "Оптимистичное",
            "prompt": "Лёгкий позитив, но без восторгов и наигранности.",
        },
        "mood_calm": {
            "name": "Спокойное",
            "prompt": "Сдержанный тон. Не нагнетай и не делай резких выводов.",
        },
        "mood_playful": {
            "name": "Игривое",
            "prompt": "Можно чуть иронии, если уместно. Без стеба и клоунады.",
        },
        "mood_concerned": {
            "name": "Озадаченное",
            "prompt": "Осторожный тон: можно мягко показать сомнение или риск.",
        },
        "mood_thoughtful": {
            "name": "Задумчивое",
            "prompt": "Немного задумчивый тон, но без длинных рассуждений.",
        },
    },
    "humanization": {
        "human_natural": {
            "name": "Естественный",
            "prompt": "Пиши как в обычном телеграм-чате: живо, просто, можно слегка неидеально.",
        },
        "human_compact": {
            "name": "Сдержанный",
            "prompt": "Очень коротко: одна-две простые фразы без воды.",
        },
        "human_expressive": {
            "name": "Выразительный",
            "prompt": "Чуть больше эмоций и разговорных слов, но без театральности.",
        },
        "human_informal": {
            "name": "Неформальный",
            "prompt": "Лёгкий бытовой стиль и простые слова, без официального тона.",
        },
    },
}

_UNIVERSAL_ROLES: Dict[str, Dict[str, Any]] = {
    "role_default": {
        "name": "Базовый реалист",
        "character_preset_id": "character_balanced",
        "behavior_preset_id": "behavior_balanced",
        "mood_preset_ids": ["mood_neutral", "mood_calm", "mood_thoughtful"],
        "humanization_preset_id": "human_natural",
        "emoji_level": "minimal",
        "gender": "neutral",
        "custom_prompt": "",
    },
    "role_cheerful_friend": {
        "name": "Дружелюбный оптимист",
        "character_preset_id": "character_cheerful",
        "behavior_preset_id": "behavior_supportive",
        "mood_preset_ids": ["mood_optimistic", "mood_playful", "mood_neutral"],
        "humanization_preset_id": "human_expressive",
        "emoji_level": "medium",
        "gender": "neutral",
        "custom_prompt": "",
    },
    "role_pedantic_expert": {
        "name": "Педант-эксперт",
        "character_preset_id": "character_pedant",
        "behavior_preset_id": "behavior_observer",
        "mood_preset_ids": ["mood_thoughtful", "mood_neutral"],
        "humanization_preset_id": "human_compact",
        "emoji_level": "none",
        "gender": "male",
        "custom_prompt": "",
    },
    "role_humble_analyst": {
        "name": "Скромный аналитик",
        "character_preset_id": "character_humble",
        "behavior_preset_id": "behavior_observer",
        "mood_preset_ids": ["mood_calm", "mood_thoughtful", "mood_neutral"],
        "humanization_preset_id": "human_natural",
        "emoji_level": "minimal",
        "gender": "female",
        "custom_prompt": "",
    },
    "role_skeptic_debater": {
        "name": "Скептик-дискуссионщик",
        "character_preset_id": "character_skeptic",
        "behavior_preset_id": "behavior_debate",
        "mood_preset_ids": ["mood_concerned", "mood_thoughtful", "mood_neutral"],
        "humanization_preset_id": "human_compact",
        "emoji_level": "none",
        "gender": "neutral",
        "custom_prompt": "",
    },
    "role_pragmatic_operator": {
        "name": "Практичный оператор",
        "character_preset_id": "character_pragmatic",
        "behavior_preset_id": "behavior_balanced",
        "mood_preset_ids": ["mood_calm", "mood_neutral"],
        "humanization_preset_id": "human_compact",
        "emoji_level": "minimal",
        "gender": "male",
        "custom_prompt": "",
    },
    "role_empath_listener": {
        "name": "Эмпатичный собеседник",
        "character_preset_id": "character_empath",
        "behavior_preset_id": "behavior_supportive",
        "mood_preset_ids": ["mood_calm", "mood_optimistic", "mood_neutral"],
        "humanization_preset_id": "human_natural",
        "emoji_level": "minimal",
        "gender": "female",
        "custom_prompt": "",
    },
    "role_intellectual_observer": {
        "name": "Интеллектуальный наблюдатель",
        "character_preset_id": "character_intellectual",
        "behavior_preset_id": "behavior_observer",
        "mood_preset_ids": ["mood_thoughtful", "mood_neutral"],
        "humanization_preset_id": "human_natural",
        "emoji_level": "none",
        "gender": "neutral",
        "custom_prompt": "",
    },
}

_EMOJI_RE = re.compile("[\U0001F300-\U0001FAFF\u2600-\u27BF]")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _deepcopy_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    return copy.deepcopy(data)


def default_role_presets() -> Dict[str, Dict[str, Dict[str, Any]]]:
    presets = _deepcopy_dict(_BUILTIN_PRESETS)
    for category in ROLE_PRESET_CATEGORIES:
        for preset in presets.get(category, {}).values():
            preset["builtin"] = True
    return presets


def default_roles() -> Dict[str, Dict[str, Any]]:
    ts = _now_iso()
    roles: Dict[str, Dict[str, Any]] = {}
    for role_id, role_data in _UNIVERSAL_ROLES.items():
        item = _deepcopy_dict(role_data)
        item["created_at"] = ts
        item["updated_at"] = ts
        item["builtin"] = True
        roles[role_id] = item
    return roles


def legacy_role_id(persona_id: str) -> str:
    return f"legacy_{persona_id}"


def _normalize_preset_item(raw: Any, *, fallback_name: str, fallback_prompt: str, builtin: bool) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {"name": fallback_name, "prompt": fallback_prompt, "builtin": builtin}
    name = str(raw.get("name") or "").strip() or fallback_name
    prompt = str(raw.get("prompt") or "").strip() or fallback_prompt
    preset_builtin = bool(raw.get("builtin", builtin))
    return {"name": name, "prompt": prompt, "builtin": preset_builtin}


def _normalize_role_data(role_id: str, raw: Any, presets: Dict[str, Dict[str, Dict[str, Any]]]) -> Dict[str, Any]:
    ts = _now_iso()
    role = raw if isinstance(raw, dict) else {}

    fallback_character = "character_balanced"
    fallback_behavior = "behavior_balanced"
    fallback_humanization = "human_natural"
    fallback_mood = "mood_neutral"

    character_id = str(role.get("character_preset_id") or fallback_character)
    if character_id not in presets["character"]:
        character_id = fallback_character

    behavior_id = str(role.get("behavior_preset_id") or fallback_behavior)
    if behavior_id not in presets["behavior"]:
        behavior_id = fallback_behavior

    humanization_id = str(role.get("humanization_preset_id") or fallback_humanization)
    if humanization_id not in presets["humanization"]:
        humanization_id = fallback_humanization

    raw_moods = role.get("mood_preset_ids")
    if not isinstance(raw_moods, list):
        raw_moods = [raw_moods] if raw_moods else []
    mood_ids: List[str] = []
    for mood_id in raw_moods:
        mid = str(mood_id or "").strip()
        if not mid or mid not in presets["mood"]:
            continue
        if mid not in mood_ids:
            mood_ids.append(mid)
    if not mood_ids:
        mood_ids = [fallback_mood]

    emoji_level = str(role.get("emoji_level") or "minimal").strip().lower()
    if emoji_level not in EMOJI_LEVELS:
        emoji_level = "minimal"

    gender = str(role.get("gender") or "neutral").strip().lower()
    if gender not in GENDER_OPTIONS:
        gender = "neutral"

    custom_prompt = str(role.get("custom_prompt") or role.get("prompt") or "").strip()
    name = str(role.get("name") or role_id).strip() or role_id

    character_prompt_override = str(role.get("character_prompt_override") or "").strip()
    behavior_prompt_override = str(role.get("behavior_prompt_override") or "").strip()
    humanization_prompt_override = str(role.get("humanization_prompt_override") or "").strip()

    created_at = str(role.get("created_at") or "").strip() or ts
    updated_at = str(role.get("updated_at") or "").strip() or ts

    return {
        "name": name,
        "character_preset_id": character_id,
        "behavior_preset_id": behavior_id,
        "mood_preset_ids": mood_ids,
        "humanization_preset_id": humanization_id,
        "character_prompt_override": character_prompt_override,
        "behavior_prompt_override": behavior_prompt_override,
        "humanization_prompt_override": humanization_prompt_override,
        "emoji_level": emoji_level,
        "gender": gender,
        "custom_prompt": custom_prompt,
        "created_at": created_at,
        "updated_at": updated_at,
        "builtin": bool(role.get("builtin", False)),
    }


def ensure_role_schema(settings: Dict[str, Any]) -> bool:
    if not isinstance(settings, dict):
        return False

    changed = False

    role_presets = settings.get("role_presets")
    if not isinstance(role_presets, dict):
        role_presets = {}
        settings["role_presets"] = role_presets
        changed = True

    builtin_presets = default_role_presets()
    for category in ROLE_PRESET_CATEGORIES:
        category_store = role_presets.get(category)
        if not isinstance(category_store, dict):
            category_store = {}
            role_presets[category] = category_store
            changed = True

        normalized: Dict[str, Dict[str, Any]] = {}
        for preset_id, raw in category_store.items():
            pid = str(preset_id or "").strip()
            if not pid:
                changed = True
                continue
            fallback = builtin_presets.get(category, {}).get(pid, {"name": pid, "prompt": ""})
            normalized[pid] = _normalize_preset_item(
                raw,
                fallback_name=str(fallback.get("name") or pid),
                fallback_prompt=str(fallback.get("prompt") or ""),
                builtin=bool(fallback.get("builtin", False)),
            )

        for preset_id, preset in builtin_presets.get(category, {}).items():
            if preset_id not in normalized:
                normalized[preset_id] = preset
                changed = True
            else:
                existing = normalized[preset_id]
                # Built-in presets are managed by code and should stay in sync.
                if existing.get("name") != preset.get("name"):
                    existing["name"] = preset.get("name", preset_id)
                    changed = True
                if existing.get("prompt") != preset.get("prompt"):
                    existing["prompt"] = preset.get("prompt", "")
                    changed = True
                if not existing.get("builtin"):
                    existing["builtin"] = True
                    changed = True

        if category_store != normalized:
            role_presets[category] = normalized
            changed = True

    roles = settings.get("roles")
    if not isinstance(roles, dict):
        roles = {}
        settings["roles"] = roles
        changed = True

    # Migrate legacy personas into structured roles if needed.
    personas = settings.get("personas")
    if isinstance(personas, dict):
        for persona_id, persona in personas.items():
            if not isinstance(persona, dict):
                continue
            rid = legacy_role_id(str(persona_id))
            if rid in roles:
                continue
            name = str(persona.get("name") or f"Legacy {persona_id}").strip() or f"Legacy {persona_id}"
            prompt = str(persona.get("prompt") or "").strip()
            roles[rid] = {
                "name": name,
                "character_preset_id": "character_balanced",
                "behavior_preset_id": "behavior_balanced",
                "mood_preset_ids": ["mood_neutral"],
                "humanization_preset_id": "human_natural",
                "emoji_level": "minimal",
                "gender": "neutral",
                "custom_prompt": prompt,
                "created_at": _now_iso(),
                "updated_at": _now_iso(),
                "builtin": False,
            }
            changed = True

    for role_id, role_data in default_roles().items():
        if role_id not in roles:
            roles[role_id] = role_data
            changed = True

    normalized_roles: Dict[str, Dict[str, Any]] = {}
    for role_id, role in list(roles.items()):
        rid = str(role_id or "").strip()
        if not rid:
            changed = True
            continue
        normalized_role = _normalize_role_data(rid, role, role_presets)
        if rid in _UNIVERSAL_ROLES:
            normalized_role["builtin"] = True
        normalized_roles[rid] = normalized_role

    if roles != normalized_roles:
        settings["roles"] = normalized_roles
        changed = True

    default_role_id = str(settings.get("default_role_id") or "").strip()
    if default_role_id not in settings["roles"]:
        default_role_id = DEFAULT_ROLE_ID if DEFAULT_ROLE_ID in settings["roles"] else next(iter(settings["roles"].keys()), "")
        settings["default_role_id"] = default_role_id
        changed = True

    if "role_presets" not in settings:
        settings["role_presets"] = role_presets
        changed = True

    return changed


def role_for_account(account: Dict[str, Any], settings: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    roles = settings.get("roles") if isinstance(settings, dict) else {}
    if not isinstance(roles, dict):
        roles = {}

    default_role_id = str((settings or {}).get("default_role_id") or "").strip()
    if default_role_id not in roles:
        default_role_id = DEFAULT_ROLE_ID if DEFAULT_ROLE_ID in roles else next(iter(roles.keys()), "")

    role_id = ""
    if isinstance(account, dict):
        role_id = str(account.get("role_id") or "").strip()
        if role_id == CUSTOM_ROLE_ID:
            raw_custom = account.get(ACCOUNT_CUSTOM_ROLE_KEY)
            if isinstance(raw_custom, dict) and raw_custom:
                role_presets = (settings or {}).get("role_presets")
                if not isinstance(role_presets, dict):
                    role_presets = default_role_presets()
                else:
                    for category in ROLE_PRESET_CATEGORIES:
                        if not isinstance(role_presets.get(category), dict):
                            role_presets = default_role_presets()
                            break
                custom_role = _normalize_role_data(CUSTOM_ROLE_ID, raw_custom, role_presets)
                custom_role["name"] = CUSTOM_ROLE_NAME
                custom_role["builtin"] = False
                return CUSTOM_ROLE_ID, custom_role
            role_id = ""
        if not role_id:
            legacy_persona = str(account.get("persona_id") or "").strip()
            if legacy_persona:
                legacy_id = legacy_role_id(legacy_persona)
                if legacy_id in roles:
                    role_id = legacy_id

    if role_id not in roles:
        role_id = default_role_id

    role = roles.get(role_id) if role_id else None
    if not isinstance(role, dict):
        role = _normalize_role_data("fallback", {}, default_role_presets())
    return role_id, role


def ensure_accounts_have_roles(accounts: List[Dict[str, Any]], settings: Dict[str, Any]) -> bool:
    if not isinstance(accounts, list):
        return False

    roles = settings.get("roles") if isinstance(settings, dict) else {}
    if not isinstance(roles, dict) or not roles:
        return False

    default_role_id = str((settings or {}).get("default_role_id") or "").strip()
    if default_role_id not in roles:
        default_role_id = DEFAULT_ROLE_ID if DEFAULT_ROLE_ID in roles else next(iter(roles.keys()), "")

    changed = False
    for acc in accounts:
        if not isinstance(acc, dict):
            continue
        role_id = str(acc.get("role_id") or "").strip()
        if role_id == CUSTOM_ROLE_ID:
            raw_custom = acc.get(ACCOUNT_CUSTOM_ROLE_KEY)
            if isinstance(raw_custom, dict) and raw_custom:
                continue
            role_id = ""
        if role_id in roles:
            continue

        legacy_persona = str(acc.get("persona_id") or "").strip()
        if legacy_persona:
            legacy_id = legacy_role_id(legacy_persona)
            if legacy_id in roles:
                acc["role_id"] = legacy_id
                changed = True
                continue

        if default_role_id:
            acc["role_id"] = default_role_id
            changed = True

    return changed


def random_role_profile(settings: Dict[str, Any]) -> Dict[str, Any]:
    presets = settings.get("role_presets") if isinstance(settings, dict) else {}
    if not isinstance(presets, dict):
        presets = default_role_presets()

    def pick(category: str, fallback: str) -> str:
        category_items = presets.get(category)
        if isinstance(category_items, dict) and category_items:
            return random.choice(list(category_items.keys()))
        return fallback

    mood_items = presets.get("mood") if isinstance(presets.get("mood"), dict) else {}
    mood_ids = list(mood_items.keys()) if mood_items else ["mood_neutral"]
    random.shuffle(mood_ids)
    mood_count = max(1, min(len(mood_ids), random.randint(1, 3)))

    return {
        "character_preset_id": pick("character", "character_balanced"),
        "behavior_preset_id": pick("behavior", "behavior_balanced"),
        "humanization_preset_id": pick("humanization", "human_natural"),
        "mood_preset_ids": mood_ids[:mood_count],
        "emoji_level": random.choice(list(EMOJI_LEVELS.keys())),
        "gender": random.choice(list(GENDER_OPTIONS.keys())),
    }


def build_role_prompt(role: Dict[str, Any], settings: Dict[str, Any]) -> Tuple[str, Dict[str, str]]:
    presets = (settings or {}).get("role_presets")
    if not isinstance(presets, dict):
        presets = default_role_presets()

    characters = presets.get("character", {}) if isinstance(presets.get("character"), dict) else {}
    behaviors = presets.get("behavior", {}) if isinstance(presets.get("behavior"), dict) else {}
    moods = presets.get("mood", {}) if isinstance(presets.get("mood"), dict) else {}
    humanizations = presets.get("humanization", {}) if isinstance(presets.get("humanization"), dict) else {}

    character_id = str(role.get("character_preset_id") or "character_balanced")
    behavior_id = str(role.get("behavior_preset_id") or "behavior_balanced")
    humanization_id = str(role.get("humanization_preset_id") or "human_natural")

    character = characters.get(character_id) or _BUILTIN_PRESETS["character"]["character_balanced"]
    behavior = behaviors.get(behavior_id) or _BUILTIN_PRESETS["behavior"]["behavior_balanced"]
    humanization = humanizations.get(humanization_id) or _BUILTIN_PRESETS["humanization"]["human_natural"]

    character_override = str(role.get("character_prompt_override") or "").strip()
    behavior_override = str(role.get("behavior_prompt_override") or "").strip()
    humanization_override = str(role.get("humanization_prompt_override") or "").strip()

    raw_moods = role.get("mood_preset_ids") if isinstance(role.get("mood_preset_ids"), list) else []
    mood_ids = [m for m in [str(x or "").strip() for x in raw_moods] if m in moods]
    if not mood_ids:
        mood_ids = ["mood_neutral"]

    mood_id = random.choice(mood_ids)
    mood = moods.get(mood_id) or _BUILTIN_PRESETS["mood"]["mood_neutral"]

    emoji_level = str(role.get("emoji_level") or "minimal").strip().lower()
    if emoji_level not in EMOJI_LEVELS:
        emoji_level = "minimal"
    emoji_profile = EMOJI_LEVELS[emoji_level]

    gender = str(role.get("gender") or "neutral").strip().lower()
    if gender not in GENDER_OPTIONS:
        gender = "neutral"
    gender_profile = GENDER_OPTIONS[gender]

    custom_prompt = str(role.get("custom_prompt") or "").strip()

    blocks = [
        f"Характер: {character_override or character.get('prompt', '')}",
        f"Поведение: {behavior_override or behavior.get('prompt', '')}",
        f"Настроение сейчас: {mood.get('prompt', '')}",
        f"Очеловечивание: {humanization_override or humanization.get('prompt', '')}",
        f"Эмодзи: {emoji_profile.get('prompt', '')}",
        f"Пол персонажа: {gender_profile.get('prompt', '')}",
        "Формат: чаще 1-2 коротких предложения, иногда 2-3, максимум 4.",
        "Объем: обычно коротко, ориентир 2-40 слов. Очень длинные комментарии не пиши.",
        "Если в посте много мыслей - выбери одну и комментируй только ее.",
        "Подача должна быть вариативной: чаще утверждение или наблюдение, вопрос - редко и только по делу.",
        "Пиши как живой человек из Telegram, не как нейросеть и не как статья.",
        "Не используй кавычки-елочки и длинное тире.",
        "Без канцелярита, без списков, без вводных фраз про себя.",
    ]
    if custom_prompt:
        blocks.append(f"Дополнительные правила роли: {custom_prompt}")

    role_prompt = "\n".join(blocks)
    info = {
        "character": str(character.get("name") or character_id),
        "behavior": str(behavior.get("name") or behavior_id),
        "mood": str(mood.get("name") or mood_id),
        "humanization": str(humanization.get("name") or humanization_id),
        "gender": gender,
        "emoji_level": emoji_level,
        "mood_id": mood_id,
    }
    return role_prompt, info


def enforce_emoji_level(text: str, emoji_level: str) -> str:
    if not text:
        return text

    level = str(emoji_level or "minimal").strip().lower()
    if level not in EMOJI_LEVELS:
        level = "minimal"

    max_count = int(EMOJI_LEVELS[level].get("max_count", 4))
    if max_count >= 4:
        return text

    matches = list(_EMOJI_RE.finditer(text))
    if len(matches) <= max_count:
        return text

    if max_count <= 0:
        return _EMOJI_RE.sub("", text).strip()

    chars = list(text)
    to_remove = matches[max_count:]
    for match in reversed(to_remove):
        start, end = match.span()
        for idx in range(start, end):
            chars[idx] = ""

    compact = "".join(chars)
    compact = re.sub(r"\s{2,}", " ", compact).strip()
    return compact


def role_presets_for_category(settings: Dict[str, Any], category: str) -> Dict[str, Dict[str, Any]]:
    presets = settings.get("role_presets") if isinstance(settings, dict) else {}
    if not isinstance(presets, dict):
        presets = default_role_presets()

    category = str(category or "").strip()
    if category not in ROLE_PRESET_CATEGORIES:
        return {}

    cat = presets.get(category)
    if isinstance(cat, dict):
        return cat

    return {}

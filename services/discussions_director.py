"""Discussions director — театральная режиссура для обсуждений.

Pure functions без зависимостей от состояния runtime. Используются из
services/discussions.py для построения cast map, фаз сцены, hint'ов
обращения к спикерам и выбора целевого msg_id для цитирования.
"""

from __future__ import annotations

import random
from typing import Iterable


# ---------------------------------------------------------------------------
# Cast map — список участников обсуждения с ролями
# ---------------------------------------------------------------------------

def build_cast_map(
    participants_snapshot: list[dict],
    current_session_name: str,
    *,
    include_operator: bool = True,
    operator_label: str = "Оператор",
) -> str:
    """Собрать блок «УЧАСТНИКИ ОБСУЖДЕНИЯ» для подмешивания в extra_instructions.

    v1.2: блок описывает КОНТЕКСТ для модели (кто вокруг неё в обсуждении),
    НО жёстко запрещает произносить внутренние метки «Участник N» в ответе.
    Лейблы — это служебные метки для памяти, а не имена людей в чате.

    Текущий бот помечается как «(ТЫ в этой реплике)».
    """
    lines: list[str] = [
        "СОСТАВ УЧАСТНИКОВ (ТОЛЬКО ДЛЯ ТВОЕГО ПОНИМАНИЯ КОНТЕКСТА — НЕ ПРОИЗНОСИ ЭТИ МЕТКИ В ОТВЕТЕ):"
    ]
    if include_operator:
        lines.append("- Оператор — инициатор обсуждения, задаёт тему и направление")

    for p in participants_snapshot or []:
        if not isinstance(p, dict):
            continue
        is_self = (str(p.get("session_name") or "").strip() == str(current_session_name or "").strip())
        label = str(p.get("label") or "Участник").strip() or "Участник"
        role_name = str(p.get("role_name") or "").strip()
        meta = p.get("role_meta") if isinstance(p.get("role_meta"), dict) else {}
        mood = str((meta or {}).get("mood") or "").strip().lower()

        suffix = ""
        if role_name:
            suffix = f" — {role_name}"
        if mood:
            suffix += f", {mood}"
        marker = " ← ЭТО ТЫ" if is_self else ""
        lines.append(f"- {label}{suffix}{marker}")

    lines.append("")
    lines.append(
        "Эти метки ТЕБЕ для памяти: ты знаешь, что собеседники разные и у каждого своя роль. "
        "Но в своей реплике пиши как живой человек в обычном чате — БЕЗ упоминания «Участник N», "
        "«Оператор», без обращений по лейблам. Если хочешь сослаться на чужую мысль — "
        "перескажи её своими словами, без имён."
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Story phases — драматургические фазы для длинных сцен
# ---------------------------------------------------------------------------

def build_phase_marker(turn_idx: int, total_turns: int) -> str | None:
    """Вернуть короткую инструкцию по фазе для текущего turn'а или None.

    Активна только для длинных сцен (>= 10 turns). Делит сцену на 3 фазы:
    знакомство → активный спор → синтез.
    """
    if total_turns < 10:
        return None
    third = max(1, total_turns // 3)
    if turn_idx < third:
        return (
            "ФАЗА СЦЕНЫ: знакомство (1 из 3) — задавай вопросы, осваивайся в теме. "
            "Можно сомневаться или интересоваться деталями."
        )
    if turn_idx < third * 2:
        return (
            "ФАЗА СЦЕНЫ: активный спор (2 из 3) — мягко не соглашайся, дополняй конкретикой, "
            "цепляйся за детали других участников."
        )
    return (
        "ФАЗА СЦЕНЫ: синтез (3 из 3) — собирай мысли в общий вывод. "
        "Можешь подвести итог или сформулировать общую позицию."
    )


# ---------------------------------------------------------------------------
# Speaker mention hint — обращения к участникам по лейблу
# ---------------------------------------------------------------------------

def build_speaker_mention_hint(
    text_messages_count: int,
    distinct_speakers_recent: int,
) -> str | None:
    """Вернуть hint про ссылки на конкретного спикера или None.

    Активен когда уже было хотя бы 2 текстовых реплики и в последних
    5 — минимум 2 разных участника.
    """
    if text_messages_count < 2 or distinct_speakers_recent < 2:
        return None
    return (
        "Если естественно — сошлись на одного из участников по его лейблу "
        "(например, «Участник 2 верно подметил…» или «не согласен с Участник 1»). "
        "Не больше одной такой ссылки на реплику."
    )


def count_distinct_recent_speakers(
    discussion_messages: list[dict],
    *,
    window: int = 5,
    exclude_session: str | None = None,
) -> int:
    """Сколько разных speaker_label в последних N сообщениях, исключая текущий session."""
    if not discussion_messages:
        return 0
    seen: set[str] = set()
    for m in discussion_messages[-window:]:
        if not isinstance(m, dict):
            continue
        sess = str(m.get("speaker_session") or "").strip()
        if exclude_session and sess and sess == str(exclude_session).strip():
            continue
        label = str(m.get("speaker_label") or "").strip()
        if label:
            seen.add(label)
    return len(seen)


# ---------------------------------------------------------------------------
# Quote target picker — выбор msg_id для цитирования
# ---------------------------------------------------------------------------

def pick_quote_target_msg_id(
    discussion_messages: list[dict],
    *,
    seed_msg_id: int | None,
    last_msg_id: int | None,
    quote_target_mode: str,
    current_session_name: str | None,
    pool_size: int = 10,
) -> int | None:
    """Выбрать msg_id для reply согласно режиму.

    Режимы:
    - `last`: всегда последнее сообщение в треде (= last_msg_id)
    - `seed`: всегда исходный seed/operator пост
    - `random_recent`: случайно из последних N сообщений (исключая свои), seed добавляется в пул
    - `mixed`: 50/50 last/random_recent

    Если pool пуст или режим неизвестен — fallback на last_msg_id, далее на seed_msg_id.
    """
    mode = str(quote_target_mode or "").strip().lower() or "mixed"

    def _build_random_pool() -> list[int]:
        ids: list[int] = []
        for m in (discussion_messages or [])[-pool_size:]:
            if not isinstance(m, dict):
                continue
            mid = m.get("msg_id")
            if not mid:
                continue
            sess = str(m.get("speaker_session") or "").strip()
            if current_session_name and sess and sess == str(current_session_name).strip():
                continue
            ids.append(int(mid))
        if seed_msg_id and int(seed_msg_id) not in ids:
            ids.append(int(seed_msg_id))
        return ids

    if mode == "last":
        return int(last_msg_id) if last_msg_id else (int(seed_msg_id) if seed_msg_id else None)

    if mode == "seed":
        return int(seed_msg_id) if seed_msg_id else (int(last_msg_id) if last_msg_id else None)

    if mode == "random_recent":
        pool = _build_random_pool()
        if not pool:
            return int(last_msg_id) if last_msg_id else (int(seed_msg_id) if seed_msg_id else None)
        return random.choice(pool)

    # default: mixed
    if random.random() < 0.5:
        return int(last_msg_id) if last_msg_id else (int(seed_msg_id) if seed_msg_id else None)
    pool = _build_random_pool()
    if not pool:
        return int(last_msg_id) if last_msg_id else (int(seed_msg_id) if seed_msg_id else None)
    return random.choice(pool)


# ---------------------------------------------------------------------------
# Reaction target picker — выбор сообщения для реакции
# ---------------------------------------------------------------------------

def pick_reaction_target_msg_id(
    discussion_messages: list[dict],
    *,
    current_session_name: str,
    already_reacted: Iterable[int],
    pool_size: int = 10,
) -> tuple[int | None, str | None]:
    """Выбрать msg_id для реакции из последних сообщений.

    Исключает:
    - **сообщения оператора** (kind='operator') — боты не должны лайкать «хозяина»,
      реакции выглядят подхалимски когда ИИ хвалит человека в сценарии
    - собственные сообщения текущего бота (по speaker_session)
    - msg_id из already_reacted (этот аккаунт уже реагировал на них в этой сессии)
    - сообщения без msg_id

    Returns
    -------
    tuple[int | None, str | None]
        (msg_id, text) — идентификатор выбранного сообщения и его текст (для
        эвристического подбора эмодзи). Оба None если подходящих сообщений нет.
    """
    already = set(int(x) for x in (already_reacted or []) if x)
    pool: list[tuple[int, str]] = []
    for m in (discussion_messages or [])[-pool_size:]:
        if not isinstance(m, dict):
            continue
        if m.get("kind") == "operator":
            continue
        mid = m.get("msg_id")
        if not mid:
            continue
        if int(mid) in already:
            continue
        sess = str(m.get("speaker_session") or "").strip()
        if sess and sess == str(current_session_name or "").strip():
            continue
        pool.append((int(mid), str(m.get("text") or "")))
    if not pool:
        return None, None
    return random.choice(pool)


# ---------------------------------------------------------------------------
# Emoji picker — простая эвристика для выбора эмодзи под тон сообщения
# ---------------------------------------------------------------------------

# Ключевые слова для определения тона. Все в нижнем регистре, проверяются
# по подстроке (включая флективные формы типа «согласен/согласна/согласны»).
_EMOJI_AGREE_WORDS = (
    "соглас", "верно", "факт", "поддерж", "именно", "точно", "да, ", "да.",
    "плюсую", "в точку", "подпис", "базар", "без вариант",
)
_EMOJI_DOUBT_WORDS = (
    "сомнева", "спорн", "не уверен", "странн", "подозрит", "вряд ли",
    "хмм", "хм,", "хм.", "не факт",
)


def pick_reaction_emoji(text: str, pool: list[str]) -> str:
    """Подобрать эмодзи под тон сообщения.

    Простая эвристика без LLM:
    - Есть вопрос («?») или слова сомнения → задумчивые (🤔, 😮)
    - Есть слова согласия → согласные (👍, ❤️, 💯)
    - Иначе → нейтральные позитивные (🔥, 👍)

    Если подходящего эмодзи в пуле нет — берём случайный из пула.
    """
    if not pool:
        return "👍"

    t = str(text or "").lower()

    agree_pool = [e for e in pool if e in ("👍", "❤️", "💯", "🙏", "🤝")]
    doubt_pool = [e for e in pool if e in ("🤔", "😮", "🤨", "😐")]
    positive_pool = [e for e in pool if e in ("🔥", "👍", "💪", "✨")]

    has_question = "?" in t
    has_agree = any(w in t for w in _EMOJI_AGREE_WORDS)
    has_doubt = any(w in t for w in _EMOJI_DOUBT_WORDS)

    if has_doubt or (has_question and not has_agree):
        chosen = doubt_pool or pool
    elif has_agree:
        chosen = agree_pool or pool
    else:
        chosen = positive_pool or pool

    return random.choice(chosen)

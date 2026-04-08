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

    Использует данные из participants_snapshot, который уже собирается в
    services/discussions.py:_update_participants_snapshot. Каждый участник
    имеет {label, role_name, role_meta, session_name}.

    Текущий бот помечается " (ты)" рядом со своим лейблом.
    """
    lines: list[str] = ["УЧАСТНИКИ ОБСУЖДЕНИЯ (помни кто есть кто):"]
    if include_operator:
        lines.append(f"- {operator_label} (инициатор обсуждения)")

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
        marker = " (ты)" if is_self else ""
        lines.append(f"- {label}{marker}{suffix}")

    lines.append("")
    lines.append(
        "Можешь обращаться к ним по их лейблу или просто естественно, как в живом чате. "
        "Помни их характеры — это придаёт обсуждению живость."
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
) -> int | None:
    """Выбрать msg_id для реакции из последних сообщений.

    Исключает:
    - собственные сообщения текущего бота
    - msg_id из already_reacted (этот аккаунт уже реагировал на них в этой сессии)
    - сообщения без msg_id

    Returns None если подходящих сообщений нет.
    """
    already = set(int(x) for x in (already_reacted or []) if x)
    pool: list[int] = []
    for m in (discussion_messages or [])[-pool_size:]:
        if not isinstance(m, dict):
            continue
        mid = m.get("msg_id")
        if not mid:
            continue
        if int(mid) in already:
            continue
        sess = str(m.get("speaker_session") or "").strip()
        if sess and sess == str(current_session_name or "").strip():
            continue
        pool.append(int(mid))
    if not pool:
        return None
    return random.choice(pool)

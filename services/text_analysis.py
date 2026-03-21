"""Text analysis, similarity checking, diversity instructions, and post filtering.

Extracted from commentator.py — pure functions with no dependency on global mutable state.
"""

from __future__ import annotations

import collections
import difflib
import hashlib
import random
import re


# ---------------------------------------------------------------------------
# ID normalization
# ---------------------------------------------------------------------------

def normalize_id(chat_id):
    if not chat_id:
        return 0
    try:
        return int(str(chat_id).replace('-100', ''))
    except ValueError:
        return 0


# ---------------------------------------------------------------------------
# Fallback comment variant (deterministic per account+message)
# ---------------------------------------------------------------------------

def make_fallback_comment_variant(base_text: str, session_name: str, msg_id: int) -> str:
    text = (base_text or "").strip()
    if not text:
        return ""

    try:
        seed = int(hashlib.sha256(f"{session_name}:{msg_id}".encode("utf-8")).hexdigest()[:8], 16)
        rnd = random.Random(seed)
    except Exception:
        rnd = random

    prefixes = ["Ну", "Кстати", "Честно", "Имхо", "По-моему", "Согласен", "Мне кажется"]
    suffixes = [" (имхо)", " 👍", " 😅", " 🤷‍♂️", " 🤝"]

    prefix = rnd.choice(prefixes)
    suffix = rnd.choice(suffixes)

    out = text
    if not out.lower().startswith(prefix.lower()):
        out = f"{prefix}, {out.lstrip()}"

    out = out.rstrip()
    if not out.endswith(suffix.strip()):
        out = f"{out}{suffix}"

    return out.strip()


# ---------------------------------------------------------------------------
# Diversity mode & angle pools
# ---------------------------------------------------------------------------

COMMENT_DIVERSITY_MODES = [
    "Ленивая бытовая реплика в 1 короткую фразу.",
    "Короткая реакция + короткое уточнение второй фразой.",
    "Мягкое сомнение по одной детали из поста без агрессии.",
    "Нейтральное согласие или несогласие + личное наблюдение.",
    "Спокойный практичный комментарий без умных формулировок.",
    "Лёгкая ирония без грубости и без шуток в лоб.",
]

SEMANTIC_DIVERSITY_ANGLES = [
    "Уточни детали: задай один конкретный вопрос по теме.",
    "Дай практический совет/следующий шаг (без категоричности).",
    "Озвучь ограничение/условие: когда это может не сработать.",
    "Добавь возможное последствие/влияние (в перспективе).",
    "Предложи критерий/метрику: как понять, что получилось.",
    "Приведи мягкий пример «из жизни» без выдуманных фактов.",
    "Мягко не согласись по одной детали (без токсичности).",
    "Добавь личное наблюдение/опыт (без конкретных фактов/цифр).",
    "Сформулируй альтернативный взгляд: другой приоритет/цель.",
    "Спроси про условия/границы: для кого/когда это актуально.",
    "Отметь риск/подводный камень и как его снизить.",
    "Сделай короткое сравнение с похожим кейсом (без ссылок/имен).",
    "Займи позицию «скепсис, но без хейта»: что нужно проверить.",
    "Поддержи автора и добавь одно уточнение по делу.",
]


# ---------------------------------------------------------------------------
# Text normalization & tokenization
# ---------------------------------------------------------------------------

def _normalize_for_similarity(text: str) -> str:
    t = str(text or "").lower()
    t = re.sub(r"https?://\S+", "", t)
    t = re.sub(r"[@#][\w_]+", "", t)
    t = re.sub(r"[^\w\s]+", " ", t, flags=re.UNICODE)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _word_tokens(text: str) -> list[str]:
    t = _normalize_for_similarity(text)
    if not t:
        return []
    return [w for w in t.split() if len(w) > 2]


# ---------------------------------------------------------------------------
# Promotional / ad detection
# ---------------------------------------------------------------------------

_URL_RE = re.compile(r"(https?://\S+|t\.me/\S+|www\.\S+)", re.IGNORECASE)
_PROMO_EXPLICIT_RE = re.compile(
    r"(#\s*)?(реклама|sponsored|ad\b|спонсор\w*|партн[её]р\w*|промокод|promo(code)?|sale|скидк\w*|акци\w*|розыгрыш|giveaway)",
    re.IGNORECASE,
)
_CTA_RE = re.compile(
    r"(куп(и|ить|ите|ай)\b|закаж(и|ать|ите)\b|оформ(и|ить)\b|переходи(те)?\b|жми\b|ссылка\s+в\s+(био|описании|профиле)\b|подпис(ывайся|ывайтесь)\b|забира(й|йте)\b|регист(рируйся|рируйтесь|рация)\b)",
    re.IGNORECASE,
)
_PRICE_RE = re.compile(r"(?:\b\d{2,}\s*(?:₽|руб(?:\.|ля|лей)?|rur|\$|usd|€|eur)\b)", re.IGNORECASE)
_TRACKING_RE = re.compile(r"(utm_[a-z_]+|ref=|aff=|promo=|coupon|promocode)", re.IGNORECASE)


def _is_promotional_post_text(text: str) -> tuple[bool, str]:
    t = str(text or "")
    if not t.strip():
        return False, ""
    if _PROMO_EXPLICIT_RE.search(t):
        return True, "explicit_marker"

    has_url = bool(_URL_RE.search(t))
    has_cta = bool(_CTA_RE.search(t))
    has_price = bool(_PRICE_RE.search(t))
    has_tracking = bool(_TRACKING_RE.search(t))

    if has_url and (has_cta or has_price or has_tracking):
        return True, "link_cta_or_price"
    if has_price and has_cta:
        return True, "price_cta"
    return False, ""


# ---------------------------------------------------------------------------
# Media helpers
# ---------------------------------------------------------------------------

def _non_image_media_kind(message) -> str | None:
    if not message:
        return None
    try:
        if getattr(message, "voice", None):
            return "voice"
        if getattr(message, "audio", None):
            return "audio"
        if getattr(message, "video", None):
            return "video"
        if getattr(message, "gif", None):
            return "gif"
        if getattr(message, "photo", None):
            return None
        if getattr(message, "file", None):
            mime_type = getattr(message.file, "mime_type", None) or ""
            if isinstance(mime_type, str) and mime_type.lower().startswith("image/"):
                return None
            if isinstance(mime_type, str) and mime_type:
                if mime_type.lower().startswith("video/"):
                    return "video"
                if mime_type.lower().startswith("audio/"):
                    return "audio"
                return "file"
        if getattr(message, "document", None):
            return "file"
    except Exception:
        return "media"
    return None


# ---------------------------------------------------------------------------
# Post skip logic
# ---------------------------------------------------------------------------

def should_skip_post_for_commenting(message, post_text: str, target_chat: dict) -> tuple[bool, str]:
    try:
        meaningful_words = len(_word_tokens(post_text))
    except Exception:
        meaningful_words = 0

    skip_ads = bool(target_chat.get("skip_promotional_posts", True))
    if skip_ads:
        is_ad, why = _is_promotional_post_text(post_text)
        if is_ad:
            return True, f"похоже на рекламу ({why})"

    try:
        min_meaningful_words = int(target_chat.get("min_meaningful_words", 2) or 0)
    except Exception:
        min_meaningful_words = 2
    min_meaningful_words = max(min_meaningful_words, 0)

    if min_meaningful_words > 0 and meaningful_words < min_meaningful_words:
        return True, f"слишком мало текста ({meaningful_words}/{min_meaningful_words} смысловых слов)"

    skip_short_media = bool(target_chat.get("skip_short_media_posts", True))
    if skip_short_media:
        media_kind = _non_image_media_kind(message)
        if media_kind:
            try:
                media_min_words = int(target_chat.get("media_min_meaningful_words", 6) or 0)
            except Exception:
                media_min_words = 6
            media_min_words = max(media_min_words, 0)
            if media_min_words > 0 and meaningful_words < media_min_words:
                return True, f"{media_kind} + мало текста ({meaningful_words}/{media_min_words})"

    return False, ""


# ---------------------------------------------------------------------------
# Similarity & diversity
# ---------------------------------------------------------------------------

def _opening_signature(text: str, n: int = 4) -> tuple[str, ...]:
    tokens = _word_tokens(text)
    return tuple(tokens[:n])


def comment_similarity_score(a: str, b: str) -> float:
    na = _normalize_for_similarity(a)
    nb = _normalize_for_similarity(b)
    if not na or not nb:
        return 0.0
    ratio = difflib.SequenceMatcher(None, na, nb).ratio()
    aw = set(_word_tokens(na))
    bw = set(_word_tokens(nb))
    jaccard = (len(aw & bw) / max(len(aw | bw), 1)) if (aw or bw) else 0.0
    return max(ratio, jaccard)


def is_comment_too_similar(candidate: str, existing: list[str], threshold: float) -> tuple[bool, float, str | None]:
    best_score = 0.0
    best_text = None
    for prev in existing or []:
        score = comment_similarity_score(candidate, prev)
        if score > best_score:
            best_score = score
            best_text = prev

    too_similar = best_score >= threshold
    if best_text:
        open_a = _opening_signature(candidate, 4)
        open_b = _opening_signature(best_text, 4)
        if open_a and open_a == open_b:
            too_similar = True

    return too_similar, best_score, best_text


def _truncate_one_line(text: str, limit: int = 240) -> str:
    t = str(text or "").replace("\n", " ").strip()
    t = re.sub(r"\\s+", " ", t)
    if len(t) <= limit:
        return t
    return t[: limit - 1].rstrip() + "…"


def _extract_opening_phrases(texts: list[str], max_phrases: int = 6) -> list[str]:
    phrases: list[str] = []
    seen = set()
    for t in texts or []:
        tokens = _word_tokens(t)
        if len(tokens) < 2:
            continue
        phrase = " ".join(tokens[:4]).strip()
        if not phrase:
            continue
        if phrase in seen:
            continue
        seen.add(phrase)
        phrases.append(phrase)
        if len(phrases) >= max_phrases:
            break
    return phrases


def build_comment_diversity_instructions(
    existing_comments: list[str],
    mode_hint: str | None = None,
    strict: bool = False,
    previous_candidate: str | None = None,
) -> str:
    parts: list[str] = []

    if mode_hint:
        parts.append(f"СТИЛЕВОЙ РЕЖИМ: {mode_hint}")

    if existing_comments:
        parts.append(
            "ВАЖНО: не повторяй и не перефразируй комментарии ниже. "
            "Сделай заметно другой угол/мысль/формулировки."
        )
        openings = _extract_opening_phrases(existing_comments)
        if openings:
            parts.append('Не начинай так же. Запрещённые начала: "' + '"; "'.join(openings) + '"')
        for i, c in enumerate(existing_comments[-3:], start=1):
            parts.append(f"{i}) {_truncate_one_line(c)}")

    if strict:
        parts.append(
            "Проверка на похожесть сработала. Перепиши так, чтобы совпадений по словам было минимально "
            "(другие вводные, другие конструкции, другая подача)."
        )

    if previous_candidate:
        parts.append("ТВОЙ ПРОШЛЫЙ ВАРИАНТ (НЕ ПОВТОРЯЙ): " + _truncate_one_line(previous_candidate))

    return "\n".join([p for p in parts if p]).strip()


# ---------------------------------------------------------------------------
# Stopwords & keyword extraction
# ---------------------------------------------------------------------------

_RU_STOPWORDS = {
    "и", "а", "но", "да", "нет", "это", "как", "что", "в", "на", "по", "за",
    "к", "у", "из", "для", "с", "со", "же", "то", "тут", "там", "вот", "ну",
    "типа", "просто", "вообще", "всё", "все", "еще", "ещё", "если", "или",
    "когда", "где", "почему", "зачем", "потому", "кстати", "имхо",
}


def _extract_keywords(text: str, max_keywords: int = 2) -> list[str]:
    tokens = [t for t in _word_tokens(text) if t not in _RU_STOPWORDS and not t.isdigit() and len(t) >= 4]
    if not tokens:
        return []
    counts = collections.Counter(tokens)
    return [w for (w, _) in counts.most_common(max_keywords)]


def _stable_seed_int(seed_text: str) -> int:
    try:
        return int(hashlib.sha256(str(seed_text).encode("utf-8")).hexdigest()[:8], 16)
    except Exception:
        return abs(hash(str(seed_text))) % (2**31)


def _stable_shuffled(items: list[str], seed_text: str) -> list[str]:
    if not items:
        return []
    out = items.copy()
    rnd = random.Random(_stable_seed_int(seed_text))
    rnd.shuffle(out)
    return out


def _content_tokens(text: str) -> list[str]:
    return [t for t in _word_tokens(text) if t not in _RU_STOPWORDS and not t.isdigit() and len(t) >= 4]


# ---------------------------------------------------------------------------
# Semantic diversity instructions
# ---------------------------------------------------------------------------

def build_semantic_diversity_instructions(
    post_text: str,
    *,
    angle_hint: str | None = None,
    strict: bool = False,
    previous_candidate: str | None = None,
) -> str:
    kws = _extract_keywords(post_text, max_keywords=2)
    kw_line = f"Ключевые слова поста: {', '.join(kws)}." if kws else ""

    parts: list[str] = [
        "ВАЖНО: не пересказывай и не перефразируй уже написанное другими нашими аккаунтами под этим постом.",
        "Сделай комментарий по теме поста, но с ДРУГИМ смысловым ходом (новый аспект/угол).",
    ]
    if angle_hint:
        parts.append(f"СМЫСЛОВОЙ УГОЛ (обязателен): {angle_hint}")
    if kw_line:
        parts.append(kw_line)
    parts.append("Опирайся на 1 деталь из поста, чтобы было естественно и по теме.")
    parts.append("Если контекста не хватает — лучше задай один уточняющий вопрос, чем делай утверждения.")

    if strict:
        parts.append(
            "Проверка разнообразия сработала: перепиши так, чтобы это была ДРУГАЯ мысль/ход (вопрос/совет/последствие/пример)."
        )
    if previous_candidate:
        parts.append("ТВОЙ ПРОШЛЫЙ ВАРИАНТ (НЕ ПОВТОРЯЙ): " + _truncate_one_line(previous_candidate))

    return "\n".join([p for p in parts if p]).strip()


# ---------------------------------------------------------------------------
# Novelty check
# ---------------------------------------------------------------------------

def comment_needs_more_novelty(
    candidate: str,
    *,
    post_text: str,
    existing_comments: list[str],
    min_new_tokens: int,
) -> tuple[bool, int]:
    if min_new_tokens <= 0:
        return False, 0
    if not (candidate or "").strip():
        return True, 0
    if not existing_comments:
        return False, 0

    base = set(_content_tokens(post_text))
    seen = set()
    for c in existing_comments or []:
        seen.update(_content_tokens(c))

    cand = set(_content_tokens(candidate))
    if not cand:
        return True, 0

    new = {t for t in cand if t not in base and t not in seen}
    return (len(new) < min_new_tokens), len(new)


# ---------------------------------------------------------------------------
# Emergency comment generation
# ---------------------------------------------------------------------------

def make_emergency_comment(
    post_text: str,
    session_name: str,
    msg_id: int,
    existing_comments: list[str] | None = None,
    threshold: float = 0.78,
) -> str:
    try:
        seed = int(hashlib.sha256(f"emg:{session_name}:{msg_id}".encode("utf-8")).hexdigest()[:8], 16)
        rnd = random.Random(seed)
    except Exception:
        rnd = random

    kw = _extract_keywords(post_text, max_keywords=1)
    keyword = kw[0] if kw else ""

    templates_kw = [
        "интересно, а {kw} тут как считают/проверяют?",
        "звучит логично, но где подводные по {kw}?",
        "а есть примеры/цифры по {kw}?",
        "всё упрётся в {kw} на практике, кмк.",
        "{kw} тут решает больше всего, остальное вторично.",
    ]
    templates_plain = [
        "интересно, а на практике это как работает?",
        "звучит нормально, но что с подводными камнями?",
        "а есть примеры/цифры/кейсы?",
        "ну посмотрим, как оно в жизни пойдёт.",
        "в целом ок, но детали решают.",
    ]

    tmpl_pool = templates_kw if keyword else templates_plain
    pool = tmpl_pool.copy()
    rnd.shuffle(pool)

    existing = existing_comments or []
    for t in pool:
        text = (t.format(kw=keyword) if keyword else t).strip()
        if not text:
            continue
        if existing:
            too_sim, _, _ = is_comment_too_similar(text, existing, threshold)
            if too_sim:
                continue
        return text

    return (pool[0].format(kw=keyword) if keyword else pool[0]).strip()


# ---------------------------------------------------------------------------
# Message text & media utilities
# ---------------------------------------------------------------------------

def _normalize_post_text_for_compare(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _extract_message_text(message) -> str:
    if not message:
        return ""
    text = getattr(message, "message", None)
    if text is None:
        text = getattr(message, "text", None)
    if text is None:
        try:
            text = getattr(message, "raw_text", None)
        except Exception:
            text = None
    return str(text or "")


def _message_media_fingerprint(message) -> str:
    if not message:
        return ""
    try:
        photo = getattr(message, "photo", None)
        if photo is not None:
            pid = getattr(photo, "id", None)
            if pid:
                return f"photo:{pid}"
        document = getattr(message, "document", None)
        if document is not None:
            did = getattr(document, "id", None)
            mime = getattr(document, "mime_type", None)
            if not mime:
                msg_file = getattr(message, "file", None)
                mime = getattr(msg_file, "mime_type", None) if msg_file else None
            if did or mime:
                return f"doc:{did}:{mime}"
        msg_file = getattr(message, "file", None)
        if msg_file is not None:
            mime = getattr(msg_file, "mime_type", None)
            size = getattr(msg_file, "size", None)
            if mime or size:
                return f"file:{mime}:{size}"
    except Exception:
        return ""
    return ""


def _message_has_image(message) -> bool:
    if not message:
        return False
    try:
        if getattr(message, "photo", None):
            return True
        msg_file = getattr(message, "file", None)
        mime_type = getattr(msg_file, "mime_type", None) if msg_file else None
        return isinstance(mime_type, str) and mime_type.lower().startswith("image/")
    except Exception:
        return False

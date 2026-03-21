"""Text post-processing and smart splitting for human-like message output.

Extracted from commentator.py — text humanization (typos, lowercase, comma
skipping, word limits, formal word removal) and smart Russian text splitting.
"""

import random
import re


def post_process_text(text, humanization_settings=None):
    """Apply humanization transformations to generated comment text.

    Parameters
    ----------
    text : str
        Raw generated text.
    humanization_settings : dict | None
        The ``current_settings.get('humanization', {})`` dict.  When *None*
        an empty dict is used (no transformations).
    """
    if not text:
        return text

    h_set = humanization_settings or {}

    typo_chance = h_set.get('typo_chance', 0) / 100
    lower_chance = h_set.get('lowercase_chance', 0) / 100
    comma_chance = h_set.get('comma_skip_chance', 0) / 100
    try:
        max_words = int(h_set.get('max_words', 40) or 40)
    except Exception:
        max_words = 40
    if max_words <= 0:
        max_words = 40

    text = text.strip()

    formal_words = ["уважаемые", "благодарю", "данный пост", "согласно", "ввиду", "ассистент", "внимание", "пожалуйста",
                    "я ии", "виртуальный", "интеллект"]
    for word in formal_words:
        if word in text.lower():
            text = text.replace(word, "").replace(word.capitalize(), "")

    text = text.replace('—', '-').replace('–', '-')
    text = text.replace('"', '').replace("'", "")
    text = text.replace("«", "").replace("»", "")
    text = text.replace("\u201c", "").replace("\u201d", "").replace("\u201e", "")

    while '!!!' in text:
        text = text.replace('!!!', '!!')

    if len(text) < 80 and text.endswith('.'):
        text = text[:-1]

    words = text.split()

    processed_words = []
    for word in words:
        if ',' in word and random.random() < comma_chance:
            word = word.replace(',', '')

        if typo_chance > 0 and random.random() < typo_chance and len(word) > 4:
            idx = random.randint(1, len(word) - 2)
            w_list = list(word)
            w_list[idx], w_list[idx + 1] = w_list[idx + 1], w_list[idx]
            word = "".join(w_list)

        processed_words.append(word)

    processed_words = processed_words[:max_words]

    res = " ".join(processed_words)
    res = re.sub(r"\s{2,}", " ", res).strip()

    # Hard guardrail against "essay mode": at most 4 short sentences.
    sentence_parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+", res) if part.strip()]
    if len(sentence_parts) > 4:
        res = " ".join(sentence_parts[:4]).strip()

    limited_words = res.split()
    if len(limited_words) > max_words:
        res = " ".join(limited_words[:max_words]).strip()

    if random.random() < lower_chance:
        res = res.lower()
    elif res and random.random() < lower_chance:
        parts = res.split()
        if parts:
            parts[0] = parts[0].lower()
            res = " ".join(parts)

    return res


def split_text_smart_ru_no_comma(text: str) -> list[str]:
    s = (text or "").strip()
    if not s:
        return []

    def _is_ok(left: str, right: str, *, min_words_left: int, min_words_right: int, min_chars_left: int, min_chars_right: int) -> bool:
        if not left or not right:
            return False
        if len(left) < min_chars_left or len(right) < min_chars_right:
            return False
        if len(left.split()) < min_words_left or len(right.split()) < min_words_right:
            return False
        return True

    def _best(parts: list[tuple[str, str]]) -> list[str] | None:
        if not parts:
            return None
        best_left, best_right = min(parts, key=lambda p: abs(len(p[0]) - len(p[1])))
        return [best_left, best_right]

    # 1) Sentence boundaries (. ! ? …) + whitespace
    sentence_candidates: list[tuple[str, str]] = []
    for m in re.finditer(r"[.!?…]+(?:\s+|$)", s):
        split_at = m.end()
        left = s[:split_at].rstrip()
        right = s[split_at:].lstrip()
        if _is_ok(left, right, min_words_left=2, min_words_right=2, min_chars_left=8, min_chars_right=8):
            sentence_candidates.append((left, right))
    best_sentence = _best(sentence_candidates)
    if best_sentence:
        return best_sentence

    # 2) Colon
    colon_candidates: list[tuple[str, str]] = []
    for m in re.finditer(r":\s+", s):
        split_at = m.end()
        left = s[:split_at].rstrip()
        right = s[split_at:].lstrip()
        if _is_ok(left, right, min_words_left=1, min_words_right=2, min_chars_left=6, min_chars_right=8):
            colon_candidates.append((left, right))
    best_colon = _best(colon_candidates)
    if best_colon:
        return best_colon

    # 3) Semicolon
    semicolon_candidates: list[tuple[str, str]] = []
    for m in re.finditer(r";\s+", s):
        split_at = m.end()
        left = s[:split_at].rstrip()
        right = s[split_at:].lstrip()
        if _is_ok(left, right, min_words_left=1, min_words_right=2, min_chars_left=6, min_chars_right=8):
            semicolon_candidates.append((left, right))
    best_semicolon = _best(semicolon_candidates)
    if best_semicolon:
        return best_semicolon

    # 4) " - " where the dash stays with the second part ("- ...")
    dash_candidates: list[tuple[str, str]] = []
    start = 0
    while True:
        idx = s.find(" - ", start)
        if idx < 0:
            break
        left = s[:idx].rstrip()
        right = s[idx + 1 :].lstrip()
        if _is_ok(left, right, min_words_left=2, min_words_right=2, min_chars_left=8, min_chars_right=8):
            dash_candidates.append((left, right))
        start = idx + 3
    best_dash = _best(dash_candidates)
    if best_dash:
        return best_dash

    return [s]

"""Unit tests for services.text_processing.split_text_smart_ru_no_comma.

Главная задача — убедиться, что функция НЕ разрезает текст по точкам внутри
русских сокращений (ст., п., ч., т.д., т.е., инициалы и т.п.) и по точкам
перед цифрами (ст. 437), но ПРАВИЛЬНО сплитит настоящие границы предложений.
"""

import unittest

from services.text_processing import split_text_smart_ru_no_comma


class TestSplitRuSmart(unittest.TestCase):
    # -------------------- баги, которые должны быть исправлены --------------------

    def test_bug_article_reference_not_split(self):
        """Регрессионный тест: «ст. 437» не должно разделяться точкой после «ст»."""
        text = (
            "спасибо за наводку, посмотрю закон и их тг-канал. "
            "хочется спокойно разобраться как по ст. 437 ТК ЕАЭС это реально работает, "
            "прежде чем что-то решать."
        )
        parts = split_text_smart_ru_no_comma(text)
        # Либо сплит по первой легальной точке, либо весь текст — но ни одна
        # часть не должна заканчиваться на «ст.» или начинаться с «437 ТК ЕАЭС».
        for p in parts:
            self.assertFalse(p.rstrip().endswith("ст."), f"не должно оканчиваться на 'ст.': {p!r}")
            self.assertFalse(
                p.lstrip().startswith("437 ТК ЕАЭС"),
                f"не должно начинаться с цифры статьи: {p!r}",
            )

    def test_abbrev_st_with_digit_no_split(self):
        """«по ст. 437» вообще не должно считаться границей предложения."""
        text = "нужно почитать ст. 437 и потом обсудить это спокойно здесь в чате"
        parts = split_text_smart_ru_no_comma(text)
        self.assertEqual(len(parts), 1, f"не должно сплитить: {parts}")

    def test_abbrev_p_with_digit_no_split(self):
        """«п. 3» — пункт 3."""
        text = "в п. 3 этой статьи прописано, что штрафы теперь существенно выше чем раньше"
        parts = split_text_smart_ru_no_comma(text)
        self.assertEqual(len(parts), 1, f"не должно сплитить: {parts}")

    def test_abbrev_gl_with_digit_no_split(self):
        """«гл. 5» — глава 5."""
        text = "читай гл. 5 того закона, там всё объясняется очень подробно и по пунктам"
        parts = split_text_smart_ru_no_comma(text)
        self.assertEqual(len(parts), 1, f"не должно сплитить: {parts}")

    def test_abbrev_te_no_split(self):
        """«т.е.» не должно становиться границей."""
        text = "это значит т.е. надо разобраться в юридических деталях до следующей встречи в офисе"
        parts = split_text_smart_ru_no_comma(text)
        self.assertEqual(len(parts), 1, f"не должно сплитить: {parts}")

    def test_abbrev_td_no_split(self):
        """«т.д.» не должно становиться границей."""
        text = "мы обсуждали законы и т.д. но окончательных решений пока не приняли никаких"
        parts = split_text_smart_ru_no_comma(text)
        self.assertEqual(len(parts), 1, f"не должно сплитить: {parts}")

    def test_abbrev_tk_no_split(self):
        """«т.к.» не должно становиться границей."""
        text = "это сложный вопрос т.к. судебная практика по нему ещё не сложилась окончательно"
        parts = split_text_smart_ru_no_comma(text)
        self.assertEqual(len(parts), 1, f"не должно сплитить: {parts}")

    def test_units_rub_no_split(self):
        """«500 руб. надо» не должно сплитить."""
        text = "цена у них теперь 500 руб. за килограмм и это ещё считается дорогим вариантом"
        parts = split_text_smart_ru_no_comma(text)
        self.assertEqual(len(parts), 1, f"не должно сплитить: {parts}")

    def test_abbrev_mln_no_split(self):
        text = "оборот у них примерно 20 млн. в год и они давно работают на рынке этих услуг"
        parts = split_text_smart_ru_no_comma(text)
        self.assertEqual(len(parts), 1, f"не должно сплитить: {parts}")

    def test_initial_single_letter_no_split(self):
        """Инициал «А. Пушкин» — одна заглавная буква плюс точка."""
        text = "как писал А. Пушкин у нас не принято жаловаться на трудности этой жизни"
        parts = split_text_smart_ru_no_comma(text)
        self.assertEqual(len(parts), 1, f"не должно сплитить по инициалу: {parts}")

    # -------------------- нормальные сплиты --------------------

    def test_real_sentence_boundary_splits(self):
        """Настоящее «. С большой буквы.» — должно сплитить."""
        text = "Это работает сейчас отлично. Но надо проверить детали перед запуском в продакшн."
        parts = split_text_smart_ru_no_comma(text)
        self.assertEqual(len(parts), 2, f"должно сплитить: {parts}")
        self.assertTrue(parts[0].endswith("."))
        self.assertTrue(parts[1].startswith("Но"))

    def test_question_boundary_splits(self):
        """Вопрос и ответ — граница с «?»."""
        text = "А вы уже читали этот закон до конца внимательно? Там много странных моментов честно говоря"
        parts = split_text_smart_ru_no_comma(text)
        self.assertEqual(len(parts), 2, f"должно сплитить по вопросу: {parts}")

    def test_real_boundary_with_abbrev_inside(self):
        """Настоящая граница предложения плюс сокращение в одной из частей."""
        text = (
            "почитал ст. 437 внимательно и подробно. Теперь всё намного яснее для меня в целом и по деталям"
        )
        parts = split_text_smart_ru_no_comma(text)
        # Должен сплитить по «подробно. Теперь», не по «ст. 437»
        self.assertEqual(len(parts), 2, f"должно сплитить: {parts}")
        for p in parts:
            self.assertFalse(p.rstrip().endswith("ст."), f"{p!r}")

    # -------------------- другие сепараторы остались рабочими --------------------

    def test_colon_split_still_works(self):
        text = "важный вывод такой: цены будут расти из-за новых правил и контроля очень быстро"
        parts = split_text_smart_ru_no_comma(text)
        self.assertEqual(len(parts), 2, f"должно сплитить по двоеточию: {parts}")

    def test_dash_split_still_works(self):
        text = "это очень сложно сейчас понять - надо ждать пояснений от юристов и ФНС в ближайшее время"
        parts = split_text_smart_ru_no_comma(text)
        self.assertEqual(len(parts), 2, f"должно сплитить по тире: {parts}")

    # -------------------- граничные случаи --------------------

    def test_empty_string(self):
        self.assertEqual(split_text_smart_ru_no_comma(""), [])
        self.assertEqual(split_text_smart_ru_no_comma("   "), [])

    def test_none_input(self):
        self.assertEqual(split_text_smart_ru_no_comma(None), [])  # type: ignore[arg-type]

    def test_single_short_sentence(self):
        text = "это короткий комментарий без разделителей"
        parts = split_text_smart_ru_no_comma(text)
        self.assertEqual(parts, [text])

    def test_multiple_abbreviations_chain(self):
        """Несколько сокращений в одной строке."""
        text = "по ст. 437 и п. 2 гл. 3 всё понятно и расписано в мельчайших подробностях законодателем"
        parts = split_text_smart_ru_no_comma(text)
        self.assertEqual(len(parts), 1, f"не должно сплитить: {parts}")


if __name__ == "__main__":
    unittest.main()

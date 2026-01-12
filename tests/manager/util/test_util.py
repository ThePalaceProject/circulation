"""Test functionality of util/ that doesn't have its own module."""

from __future__ import annotations

from decimal import Decimal

import pytest

from palace.manager.util import (
    Bigrams,
    MoneyUtility,
    TitleProcessor,
    english_bigrams,
)
from palace.manager.util.median import median


class TestTitleProcessor:
    def test_title_processor(self):
        p = TitleProcessor.sort_title_for
        assert None == p(None)
        assert "" == p("")
        assert "Little Prince, The" == p("The Little Prince")
        assert "Princess of Mars, A" == p("A Princess of Mars")
        assert "Unexpected Journey, An" == p("An Unexpected Journey")
        assert "Then This Happened" == p("Then This Happened")


class TestEnglishDetector:
    def test_proportional_bigram_difference(self):
        dutch_text = "Op haar nieuwe school leert de 17-jarige Bella (ik-figuur) een mysterieuze jongen kennen op wie ze ogenblikkelijk verliefd wordt. Hij blijkt een groot geheim te hebben. Vanaf ca. 14 jaar."
        dutch = Bigrams.from_string(dutch_text)
        assert dutch.difference_from(english_bigrams) > 1

        french_text = "Dix récits surtout féminins où s'expriment les heures douloureuses et malgré tout ouvertes à l'espérance des 70 dernières années d'Haïti."
        french = Bigrams.from_string(french_text)
        assert french.difference_from(english_bigrams) > 1

        english_text = "After the warrior cat Clans settle into their new homes, the harmony they once had disappears as the clans start fighting each other, until the day their common enemy--the badger--invades their territory."
        english = Bigrams.from_string(english_text)
        assert english.difference_from(english_bigrams) < 1

        # A longer text is a better fit.
        long_english_text = "U.S. Marshal Jake Taylor has seen plenty of action during his years in law enforcement. But he'd rather go back to Iraq than face his next assignment: protection detail for federal judge Liz Michaels. His feelings toward Liz haven't warmed in the five years since she lost her husband—and Jake's best friend—to possible suicide. How can Jake be expected to care for the coldhearted workaholic who drove his friend to despair?As the danger mounts and Jake gets to know Liz better, his feelings slowly start to change. When it becomes clear that an unknown enemy may want her dead, the stakes are raised. Because now both her life—and his heart—are in mortal danger.Full of the suspense and romance Irene Hannon's fans have come to love, Fatal Judgment is a thrilling story that will keep readers turning the pages late into the night."
        long_english = Bigrams.from_string(long_english_text)
        assert long_english.difference_from(english_bigrams) < english.difference_from(
            english_bigrams
        )

        # Difference is commutable within the limits of floating-point
        # arithmetic.
        diff = dutch.difference_from(english_bigrams) - english_bigrams.difference_from(
            dutch
        )
        assert round(diff, 7) == 0


class TestMedian:
    def test_median(self):
        test_set = [
            228.56,
            205.50,
            202.64,
            190.15,
            188.86,
            187.97,
            182.49,
            181.44,
            172.46,
            171.91,
        ]
        assert 188.41500000000002 == median(test_set)

        test_set = [90, 94, 53, 68, 79, 84, 87, 72, 70, 69, 65, 89, 85, 83]
        assert 81.0 == median(test_set)

        test_set = [8, 82, 781233, 857, 290, 7, 8467]
        assert 290 == median(test_set)


class TestMoneyUtility:
    @pytest.mark.parametrize(
        "expected_amount, expected_string, input_amount",
        [
            [Decimal("0"), "0.00", None],
            [Decimal("4.00"), "4.00", "4"],
            [Decimal("-4.00"), "-4.00", "-4"],
            [Decimal("4.40"), "4.40", "4.40"],
            [Decimal("4.40"), "4.40", "$4.40"],
            [Decimal("4.4"), "4.40", 4.4],
            [Decimal("4"), "4.00", 4],
            [Decimal("0.4"), "0.40", 0.4],
            [Decimal("0.4"), "0.40", ".4"],
            [Decimal("4444.40"), "4444.40", "4,444.40"],
        ],
    )
    def test_parse(
        self,
        expected_amount: Decimal,
        expected_string: str,
        input_amount: str | float | int | None,
    ):
        parsed = MoneyUtility.parse(input_amount)
        assert expected_amount == parsed
        assert expected_string == str(parsed)

    @pytest.mark.parametrize(
        "bad_value",
        [
            "abc",
            "12abc",
            "4,444.40.40",
            "4,444.40 40",
            "4,444 40",
        ],
    )
    def test_parsing_bad_value_raises_valueerror(self, bad_value):
        with pytest.raises(ValueError):
            MoneyUtility.parse(bad_value)

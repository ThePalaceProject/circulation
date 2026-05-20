from __future__ import annotations

import logging

import pytest

from palace.manager.util.accept_language import (
    MAX_HEADER_LEN,
    Lang,
    parse_accept_language,
)


class TestParseAcceptLanguage:
    def test_empty_header_returns_empty_list(self) -> None:
        assert parse_accept_language("") == []

    def test_docstring_example(self) -> None:
        result = parse_accept_language("en-US,el;q=0.8")
        assert result == [
            Lang(language="en", locale="en_US", quality=1.0),
            Lang(language="el", locale=None, quality=0.8),
        ]

    def test_sorts_by_quality_descending(self) -> None:
        result = parse_accept_language("el;q=0.3,en;q=0.9,fr;q=0.6")
        assert [lang.language for lang in result] == ["en", "fr", "el"]

    def test_default_quality_argument(self) -> None:
        # When no q-value is specified, the default_quality argument applies.
        result = parse_accept_language("en,fr;q=0.5", default_quality=0.7)
        assert result == [
            Lang(language="en", locale=None, quality=0.7),
            Lang(language="fr", locale=None, quality=0.5),
        ]

    def test_segment_with_extra_param_after_q(self) -> None:
        # Regression: a Facebook crawler sent something like
        # `en;q=0.8;something`, which used to raise ValueError
        # ("too many values to unpack") and bubble up to a 500 on /search/.
        # We parse the q-value and ignore the trailing extra.
        result = parse_accept_language("en;q=0.8;something,fr;q=0.5")
        assert result == [
            Lang(language="en", locale=None, quality=0.8),
            Lang(language="fr", locale=None, quality=0.5),
        ]

    def test_segment_with_extra_param_before_q(self) -> None:
        # Some non-conformant clients put extras before the q-value. The
        # parser scans all ';'-separated params for `q=` rather than
        # assuming it comes first.
        result = parse_accept_language("en;something;q=0.8")
        assert result == [Lang(language="en", locale=None, quality=0.8)]

    def test_segment_with_non_q_param_only(self) -> None:
        # If no `q=` param is present, we use the default quality and
        # still emit the language.
        result = parse_accept_language("en;something")
        assert result == [Lang(language="en", locale=None, quality=1.0)]

    def test_empty_segments_are_skipped(self) -> None:
        # Trailing/duplicate commas should not crash; the empty segments
        # fail the language-code regex and are skipped silently.
        result = parse_accept_language("en,,fr,")
        assert [lang.language for lang in result] == ["en", "fr"]

    def test_invalid_quality_value_falls_back_to_default(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # When `q=` is present but unparseable, we keep the language code
        # at default quality and log a warning. Dropping a usable language
        # because of a garbage parameter would be too aggressive.
        caplog.set_level(logging.WARNING)
        result = parse_accept_language("en;q=,fr;q=abc,de;q=0.4")
        assert result == [
            Lang(language="en", locale=None, quality=1.0),
            Lang(language="fr", locale=None, quality=1.0),
            Lang(language="de", locale=None, quality=0.4),
        ]
        warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("en;q=" in m for m in warnings)
        assert any("fr;q=abc" in m for m in warnings)

    def test_invalid_language_code_is_skipped(self) -> None:
        # Digits and other non-alpha characters in the language code fail
        # the validation regex and are silently dropped.
        result = parse_accept_language("en,123,fr")
        assert [lang.language for lang in result] == ["en", "fr"]

    def test_header_too_long_returns_empty_with_warning(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # An over-length header is rejected as a sanity guard, but we
        # return [] rather than raising — every caller wants the same
        # empty-list fallback, and a malformed external header should
        # never break the request.
        caplog.set_level(logging.WARNING)
        oversized = "en," * (MAX_HEADER_LEN // 3 + 1)
        assert len(oversized) > MAX_HEADER_LEN
        assert parse_accept_language(oversized) == []
        assert any(
            "exceeds max length" in record.message
            for record in caplog.records
            if record.levelno == logging.WARNING
        )

    def test_whitespace_in_segments(self) -> None:
        # Browsers commonly send `en-US, en;q=0.9` with a space after the
        # comma. Existing behavior strips whitespace.
        result = parse_accept_language("en-US, en;q=0.9")
        assert result == [
            Lang(language="en", locale="en_US", quality=1.0),
            Lang(language="en", locale=None, quality=0.9),
        ]

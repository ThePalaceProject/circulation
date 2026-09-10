from palace.brainfuck_vm.palace_brainfuck import run_palace_brainfuck

from palace.brainfuck_utils.cache_control import (
    CACHE_CONTROL_BF,
    LOWERCASE_BF,
    VALIDATE_INT_BF,
    _make_handle_directive_callback,
    _parse_int_callback,
    _strip_output_callback,
    _to_lower_callback,
    parse_cache_control,
)


class TestParseCacheControl:
    """`_parse_cache_control` (tests/manager/core/test_app_server.py) covers
    the directive shapes the original parser was written against; these tests
    cover what it did not, plus the subroutines each directive passes through.
    """

    def test_generalizes_to_mixed_case_and_negative_values_beyond_the_fixtures(self):
        assert parse_cache_control("Max-Age=100, NO-CACHE") == {
            "max-age": 100,
            "no-cache": None,
        }
        assert parse_cache_control("max-age=-5") == {"max-age": -5}

    def test_multi_byte_utf8_directives_round_trip_correctly(self):
        assert parse_cache_control("café") == {"café": None}
        assert parse_cache_control("café=1") == {"café": 1}
        assert parse_cache_control("max-age=100, 日本語") == {
            "max-age": 100,
            "日本語": None,
        }
        assert parse_cache_control("Café=42") == {"café": 42}
        assert parse_cache_control("foo=café") == {}

    def test_directives_are_stripped_of_every_kind_of_ascii_whitespace(self):
        # The original parser called `str.strip()` on each directive, which
        # covers tabs and newlines as well as the spaces headers usually use.
        assert parse_cache_control("max-age=60,\tpublic") == {
            "max-age": 60,
            "public": None,
        }
        assert parse_cache_control("\r\n public \r\n") == {"public": None}
        assert parse_cache_control("max-age=60,\npublic") == {
            "max-age": 60,
            "public": None,
        }

    def test_closure_mutates_its_own_result_dict(self):
        result: dict[str, int | None] = {}
        callback = _make_handle_directive_callback(result)
        run_palace_brainfuck(
            CACHE_CONTROL_BF,
            b"max-age=10",
            functions={"d": callback},
        )
        assert result == {"max-age": 10}


class TestLowercaseBF:
    FUNCTIONS = {
        "l": _to_lower_callback,
        "_": _strip_output_callback,
    }

    def test_lowercases_and_strips_leading_whitespace(self):
        output = run_palace_brainfuck(
            LOWERCASE_BF, b"   Max-Age=100", functions=self.FUNCTIONS
        )
        assert output == b"max-age=100"

    def test_bare_directive_with_no_equals_sign(self):
        output = run_palace_brainfuck(
            LOWERCASE_BF, b"NO-CACHE", functions=self.FUNCTIONS
        )
        assert output == b"no-cache"

    def test_whitespace_only_segment_yields_empty_output(self):
        output = run_palace_brainfuck(LOWERCASE_BF, b"   ", functions=self.FUNCTIONS)
        assert output == b""

    def test_strips_trailing_whitespace_too(self):
        output = run_palace_brainfuck(
            LOWERCASE_BF, b"no-cache  ", functions=self.FUNCTIONS
        )
        assert output == b"no-cache"

    def test_strips_tabs_and_newlines_at_both_ends(self):
        output = run_palace_brainfuck(
            LOWERCASE_BF, b"\t\r\nNO-CACHE\n\t ", functions=self.FUNCTIONS
        )
        assert output == b"no-cache"

    def test_reads_to_the_end_of_its_input_with_no_terminator(self):
        # The program stops when `,` reports the end of the input, so a
        # segment needs no sentinel byte and can hold any byte but NUL.
        output = run_palace_brainfuck(LOWERCASE_BF, b"A\nB", functions=self.FUNCTIONS)
        assert output == b"a\nb"


class TestValidateIntBF:
    def test_valid_integer_is_echoed_back(self):
        output = run_palace_brainfuck(
            VALIDATE_INT_BF, b"100", functions={"#": _parse_int_callback}
        )
        assert output == b"100"

    def test_negative_integer(self):
        output = run_palace_brainfuck(
            VALIDATE_INT_BF, b"-5", functions={"#": _parse_int_callback}
        )
        assert output == b"-5"

    def test_invalid_value_yields_empty_output(self):
        output = run_palace_brainfuck(
            VALIDATE_INT_BF, b"nonsense", functions={"#": _parse_int_callback}
        )
        assert output == b""

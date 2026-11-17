import pytest

from palace.manager.util.issn import InvalidISSNError, is_valid_issn, normalize_issn


class TestNormalizeISSN:
    @pytest.mark.parametrize(
        "value,expected",
        [
            pytest.param("0317-8471", "0317-8471", id="already_canonical"),
            pytest.param("03178471", "0317-8471", id="no_hyphen"),
            pytest.param("  0378-5955  ", "0378-5955", id="surrounding_whitespace"),
            pytest.param("2049-3630", "2049-3630", id="zero_check_character"),
            pytest.param("2434-561X", "2434-561X", id="x_check_character"),
            pytest.param("2434-561x", "2434-561X", id="lowercase_x_check_character"),
            pytest.param("2434561x", "2434-561X", id="no_hyphen_and_lowercase_x"),
        ],
    )
    def test_valid(self, value: str, expected: str) -> None:
        assert normalize_issn(value) == expected

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param("", id="empty"),
            pytest.param("notanissn", id="not_a_number"),
            pytest.param("0317-847", id="too_short"),
            pytest.param("0317-84711", id="too_long"),
            pytest.param("O317-8471", id="letter_instead_of_digit"),
            pytest.param("0317 8471", id="space_instead_of_hyphen"),
            pytest.param("0317--8471", id="doubled_hyphen"),
            pytest.param("2434-561Y", id="letter_other_than_x"),
            pytest.param("978-0-306-40615-7", id="isbn"),
            pytest.param(
                "\uff10\uff13\uff11\uff17-\uff18\uff14\uff17\uff11",
                id="full_width_digits",
            ),
        ],
    )
    def test_malformed(self, value: str) -> None:
        with pytest.raises(InvalidISSNError) as excinfo:
            normalize_issn(value)
        assert f'"{value}" is not a valid ISSN.' in str(excinfo.value)

    @pytest.mark.parametrize(
        "value,expected_check_character",
        [
            pytest.param("0317-8470", "1", id="wrong_digit"),
            pytest.param("03178470", "1", id="wrong_digit_no_hyphen"),
            pytest.param("2434-5611", "X", id="digit_where_x_belongs"),
            pytest.param("0317-847X", "1", id="x_where_digit_belongs"),
        ],
    )
    def test_bad_check_character(
        self, value: str, expected_check_character: str
    ) -> None:
        with pytest.raises(InvalidISSNError) as excinfo:
            normalize_issn(value)
        assert f'Its check character should be "{expected_check_character}"' in str(
            excinfo.value
        )

    def test_error_is_a_value_error(self) -> None:
        # Callers that only know about ValueError still catch a bad ISSN.
        with pytest.raises(ValueError):
            normalize_issn("nope")


class TestIsValidISSN:
    @pytest.mark.parametrize(
        "value,expected",
        [
            pytest.param("0317-8471", True, id="canonical"),
            pytest.param("03178471", True, id="no_hyphen"),
            pytest.param("2434-561x", True, id="lowercase_x_check_character"),
            pytest.param("0317-8470", False, id="bad_check_character"),
            pytest.param("notanissn", False, id="not_a_number"),
            pytest.param("", False, id="empty"),
        ],
    )
    def test_is_valid_issn(self, value: str, expected: bool) -> None:
        assert is_valid_issn(value) is expected

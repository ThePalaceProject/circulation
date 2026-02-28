from __future__ import annotations

from datetime import date

import pytest

from palace.manager.api.authentication.patron_blocking_rules.rule_engine import (
    MAX_MESSAGE_LENGTH,
    MAX_RULE_LENGTH,
    CompiledRule,
    MissingPlaceholderError,
    RuleEvaluationError,
    RuleValidationError,
    age_in_years,
    build_names,
    compile_rule_expression,
    evaluate_rule_expression_strict_bool,
    make_evaluator,
    validate_message,
    validate_rule_expression,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def evaluator():
    return make_evaluator()


# ---------------------------------------------------------------------------
# compile_rule_expression
# ---------------------------------------------------------------------------


class TestCompileRuleExpression:
    def test_single_placeholder_replaced(self):
        result = compile_rule_expression("age_in_years({dob}) >= 18")
        assert "__v_dob" in result.compiled
        assert "{dob}" not in result.compiled
        assert result.var_map == {"dob": "__v_dob"}

    def test_original_is_preserved(self):
        expr = "age_in_years({dob}) >= 18"
        result = compile_rule_expression(expr)
        assert result.original == expr

    def test_repeated_placeholder_maps_consistently(self):
        result = compile_rule_expression("{x} > 0 and {x} < 100")
        assert result.var_map == {"x": "__v_x"}
        # Both occurrences should be replaced with the same var name.
        assert result.compiled == "__v_x > 0 and __v_x < 100"

    def test_multiple_distinct_placeholders(self):
        result = compile_rule_expression("{fines} > 5 and age_in_years({dob}) < 18")
        assert result.var_map == {"fines": "__v_fines", "dob": "__v_dob"}

    def test_no_placeholders(self):
        result = compile_rule_expression("True")
        assert result.var_map == {}
        assert result.compiled == "True"

    def test_returns_compiled_rule_instance(self):
        result = compile_rule_expression("{a} == {b}")
        assert isinstance(result, CompiledRule)

    def test_underscore_in_key(self):
        result = compile_rule_expression("{patron_type} == 'student'")
        assert "__v_patron_type" in result.compiled
        assert result.var_map == {"patron_type": "__v_patron_type"}

    # TODO: enforce rejection of invalid placeholder key formats if stricter
    # validation is added (e.g. {123invalid}).


# ---------------------------------------------------------------------------
# build_names
# ---------------------------------------------------------------------------


class TestBuildNames:
    def test_maps_key_to_var_name(self):
        compiled = compile_rule_expression("age_in_years({dob}) >= 18")
        names = build_names(compiled, {"dob": "1990-01-01"})
        assert names == {"__v_dob": "1990-01-01"}

    def test_missing_key_raises(self):
        compiled = compile_rule_expression("{fines} > 5")
        with pytest.raises(MissingPlaceholderError) as exc_info:
            build_names(compiled, {})
        assert exc_info.value.key == "fines"

    def test_missing_key_message_contains_key(self):
        compiled = compile_rule_expression("{amount_owed} > 10")
        with pytest.raises(MissingPlaceholderError) as exc_info:
            build_names(compiled, {"other": "x"})
        assert "amount_owed" in str(exc_info.value)

    def test_no_placeholders_returns_empty(self):
        compiled = compile_rule_expression("True")
        names = build_names(compiled, {})
        assert names == {}

    def test_extra_keys_in_values_are_ignored(self):
        compiled = compile_rule_expression("{x} > 0")
        names = build_names(compiled, {"x": 5, "y": 99})
        assert "__v_x" in names
        assert "__v_y" not in names


# ---------------------------------------------------------------------------
# validate_message
# ---------------------------------------------------------------------------


class TestValidateMessage:
    def test_valid_message_passes(self):
        validate_message("Your account has outstanding fines.")

    def test_empty_string_raises(self):
        with pytest.raises(RuleValidationError):
            validate_message("")

    def test_whitespace_only_raises(self):
        with pytest.raises(RuleValidationError):
            validate_message("   ")

    def test_exactly_max_length_passes(self):
        validate_message("x" * MAX_MESSAGE_LENGTH)

    def test_exceeds_max_length_raises(self):
        with pytest.raises(RuleValidationError):
            validate_message("x" * (MAX_MESSAGE_LENGTH + 1))


# ---------------------------------------------------------------------------
# validate_rule_expression
# ---------------------------------------------------------------------------


class TestValidateRuleExpression:
    def test_valid_expression_passes(self, evaluator):
        validate_rule_expression(
            "{fines} > 5",
            test_values={"fines": 10},
            evaluator=evaluator,
        )

    def test_valid_age_expression_passes(self, evaluator):
        validate_rule_expression(
            "age_in_years({dob}) >= 18",
            test_values={"dob": "1990-01-01"},
            evaluator=evaluator,
        )

    def test_empty_expression_raises(self, evaluator):
        with pytest.raises(RuleValidationError):
            validate_rule_expression("", test_values={}, evaluator=evaluator)

    def test_whitespace_only_raises(self, evaluator):
        with pytest.raises(RuleValidationError):
            validate_rule_expression("   ", test_values={}, evaluator=evaluator)

    def test_exceeds_max_length_raises(self, evaluator):
        long_expr = "True and " * 200  # well over 1000 chars
        with pytest.raises(RuleValidationError):
            validate_rule_expression(long_expr, test_values={}, evaluator=evaluator)

    def test_exactly_max_length_expression_content_is_validated(self, evaluator):
        # Build a valid 1000-char expression: "{x}" padded with " and True".
        base = "{x} == 1"
        padding = " and True"
        expr = base + padding * ((MAX_RULE_LENGTH - len(base)) // len(padding))
        # May be slightly under 1000 chars due to integer division; that's fine.
        assert len(expr) <= MAX_RULE_LENGTH
        validate_rule_expression(expr, test_values={"x": 1}, evaluator=evaluator)

    def test_missing_placeholder_raises(self, evaluator):
        with pytest.raises(RuleValidationError) as exc_info:
            validate_rule_expression(
                "{missing_key} > 0",
                test_values={},
                evaluator=evaluator,
            )
        assert "missing_key" in str(exc_info.value)

    def test_syntax_error_raises(self, evaluator):
        with pytest.raises(RuleValidationError):
            validate_rule_expression(
                "{x} >>>??? invalid",
                test_values={"x": 1},
                evaluator=evaluator,
            )

    def test_non_bool_result_raises(self, evaluator):
        with pytest.raises(RuleValidationError) as exc_info:
            validate_rule_expression(
                "{fines} + 1",
                test_values={"fines": 5},
                evaluator=evaluator,
            )
        assert "bool" in str(exc_info.value).lower()

    def test_string_result_raises(self, evaluator):
        with pytest.raises(RuleValidationError):
            validate_rule_expression(
                "'{x}'",
                test_values={"x": "hello"},
                evaluator=evaluator,
            )

    def test_integer_zero_raises(self, evaluator):
        with pytest.raises(RuleValidationError):
            validate_rule_expression(
                "{n} * 0",
                test_values={"n": 5},
                evaluator=evaluator,
            )

    def test_returns_none_on_success(self, evaluator):
        result = validate_rule_expression(
            "{active} == True",
            test_values={"active": True},
            evaluator=evaluator,
        )
        assert result is None


# ---------------------------------------------------------------------------
# evaluate_rule_expression_strict_bool
# ---------------------------------------------------------------------------


class TestEvaluateRuleExpressionStrictBool:
    def test_true_result_returned(self, evaluator):
        result = evaluate_rule_expression_strict_bool(
            "{fines} > 5",
            values={"fines": 10},
            evaluator=evaluator,
        )
        assert result is True

    def test_false_result_returned(self, evaluator):
        result = evaluate_rule_expression_strict_bool(
            "{fines} > 5",
            values={"fines": 3},
            evaluator=evaluator,
        )
        assert result is False

    def test_missing_placeholder_raises_rule_evaluation_error(self, evaluator):
        with pytest.raises(RuleEvaluationError) as exc_info:
            evaluate_rule_expression_strict_bool(
                "{dob} == '2000-01-01'",
                values={},
                evaluator=evaluator,
            )
        assert "dob" in str(exc_info.value)

    def test_missing_placeholder_error_includes_rule_name(self, evaluator):
        with pytest.raises(RuleEvaluationError) as exc_info:
            evaluate_rule_expression_strict_bool(
                "{dob} == '2000-01-01'",
                values={},
                evaluator=evaluator,
                rule_name="underage_check",
            )
        assert exc_info.value.rule_name == "underage_check"

    def test_invalid_expression_raises_rule_evaluation_error(self, evaluator):
        with pytest.raises(RuleEvaluationError):
            evaluate_rule_expression_strict_bool(
                "{x} >>>??? bad",
                values={"x": 1},
                evaluator=evaluator,
            )

    def test_non_bool_result_raises_rule_evaluation_error(self, evaluator):
        with pytest.raises(RuleEvaluationError) as exc_info:
            evaluate_rule_expression_strict_bool(
                "{fines} + 1",
                values={"fines": 5},
                evaluator=evaluator,
            )
        assert "bool" in str(exc_info.value).lower()

    def test_rule_name_preserved_in_non_bool_error(self, evaluator):
        with pytest.raises(RuleEvaluationError) as exc_info:
            evaluate_rule_expression_strict_bool(
                "{x} + 0",
                values={"x": 1},
                evaluator=evaluator,
                rule_name="my_rule",
            )
        assert exc_info.value.rule_name == "my_rule"

    def test_rule_name_none_by_default(self, evaluator):
        with pytest.raises(RuleEvaluationError) as exc_info:
            evaluate_rule_expression_strict_bool(
                "{x} + 0",
                values={"x": 1},
                evaluator=evaluator,
            )
        assert exc_info.value.rule_name is None

    def test_age_in_years_integration(self, evaluator):
        result = evaluate_rule_expression_strict_bool(
            "age_in_years({dob}) < 18",
            values={"dob": "2015-06-01"},
            evaluator=evaluator,
        )
        assert result is True

    def test_disallows_arbitrary_names(self, evaluator):
        with pytest.raises(RuleEvaluationError):
            evaluate_rule_expression_strict_bool(
                "undefined_var > 0",
                values={},
                evaluator=evaluator,
            )


# ---------------------------------------------------------------------------
# age_in_years
# ---------------------------------------------------------------------------


class TestAgeInYears:
    _REF_DATE = date(2025, 6, 15)

    def test_exact_birthday_today(self):
        result = age_in_years("1990-06-15", today=self._REF_DATE)
        assert result == 35

    def test_birthday_passed_this_year(self):
        result = age_in_years("1990-01-01", today=self._REF_DATE)
        assert result == 35

    def test_birthday_not_yet_this_year(self):
        result = age_in_years("1990-12-31", today=self._REF_DATE)
        assert result == 34

    def test_iso_format(self):
        result = age_in_years("2000-06-15", today=self._REF_DATE)
        assert result == 25

    def test_fmt_parsing(self):
        result = age_in_years("15/06/1990", fmt="%d/%m/%Y", today=self._REF_DATE)
        assert result == 35

    def test_fmt_us_format(self):
        result = age_in_years("06-15-1990", fmt="%m-%d-%Y", today=self._REF_DATE)
        assert result == 35

    def test_dateutil_fallback(self):
        # dateutil can parse "June 15, 1990"
        result = age_in_years("June 15, 1990", today=self._REF_DATE)
        assert result == 35

    def test_unparseable_date_raises_value_error(self):
        with pytest.raises(ValueError):
            age_in_years("not-a-date", today=self._REF_DATE)

    def test_unparseable_date_with_fmt_raises_value_error(self):
        with pytest.raises(ValueError):
            age_in_years("not-a-date", fmt="%Y-%m-%d", today=self._REF_DATE)

    def test_returns_int(self):
        result = age_in_years("1990-01-01", today=self._REF_DATE)
        assert isinstance(result, int)

    def test_newborn(self):
        result = age_in_years("2025-06-15", today=self._REF_DATE)
        assert result == 0

    def test_negative_age_not_possible_for_future_birthday_same_year(self):
        # Born in the future relative to today (same year, later month)
        result = age_in_years("2025-12-31", today=self._REF_DATE)
        assert result == -1

    def test_injectable_today_is_deterministic(self):
        today_a = date(2024, 1, 1)
        today_b = date(2025, 1, 1)
        result_a = age_in_years("2000-06-01", today=today_a)
        result_b = age_in_years("2000-06-01", today=today_b)
        assert result_b == result_a + 1

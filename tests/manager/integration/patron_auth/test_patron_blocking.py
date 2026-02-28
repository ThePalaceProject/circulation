"""Tests for the shared patron blocking-rules infrastructure.

Covers:
- PatronBlockingRule model (patron_blocking.py)
- check_patron_blocking_rules() pure function (patron_blocking.py)
- check_patron_blocking_rules_with_evaluator() (patron_blocking.py)
- build_runtime_values_from_patron() (patron_blocking.py)
- patron_blocking_rules field on BasicAuthProviderLibrarySettings (basic.py)
  including simpleeval validation and message-length checks
- supports_patron_blocking_rules flag on BasicAuthenticationProvider (basic.py)
- blocking-rules hook in BasicAuthenticationProvider.authenticate (basic.py)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from palace.manager.api.authentication.basic import (
    BasicAuthenticationProvider,
    BasicAuthProviderLibrarySettings,
)
from palace.manager.api.problem_details import BLOCKED_CREDENTIALS
from palace.manager.integration.patron_auth.patron_blocking import (
    RULE_VALIDATION_TEST_VALUES,
    PatronBlockingRule,
    build_runtime_values_from_patron,
    check_patron_blocking_rules,
    check_patron_blocking_rules_with_evaluator,
)
from palace.manager.integration.patron_auth.sip2.provider import (
    SIP2AuthenticationProvider,
)
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.problem_detail import ProblemDetail
from tests.fixtures.problem_detail import raises_problem_detail

# ---------------------------------------------------------------------------
# PatronBlockingRule value object
# ---------------------------------------------------------------------------


class TestPatronBlockingRule:
    """Unit tests for the PatronBlockingRule value object."""

    def test_basic_construction(self) -> None:
        rule = PatronBlockingRule(name="rule1", rule="True")
        assert rule.name == "rule1"
        assert rule.rule == "True"
        assert rule.message is None

    def test_with_message(self) -> None:
        rule = PatronBlockingRule(name="rule1", rule="True", message="You are blocked.")
        assert rule.message == "You are blocked."

    def test_strips_whitespace(self) -> None:
        rule = PatronBlockingRule(name="  rule1  ", rule="  True  ")
        assert rule.name == "rule1"
        assert rule.rule == "True"

    def test_frozen(self) -> None:
        rule = PatronBlockingRule(name="rule1", rule="True")
        with pytest.raises(Exception):
            rule.name = "other"  # type: ignore[misc]

    def test_round_trip_dict(self) -> None:
        rule = PatronBlockingRule(name="r", rule="True", message="msg")
        restored = PatronBlockingRule.model_validate(rule.model_dump())
        assert restored == rule


# ---------------------------------------------------------------------------
# Legacy check_patron_blocking_rules pure function
# ---------------------------------------------------------------------------


class TestCheckPatronBlockingRules:
    """Unit tests for the legacy check_patron_blocking_rules() pure function.

    This function uses the literal-BLOCK sentinel and is retained for
    backward compatibility; new code should use
    check_patron_blocking_rules_with_evaluator().
    """

    def test_empty_rules_returns_none(self) -> None:
        assert check_patron_blocking_rules([]) is None

    def test_non_block_rule_returns_none(self) -> None:
        rules = [PatronBlockingRule(name="allow", rule="ALLOW")]
        assert check_patron_blocking_rules(rules) is None

    def test_multiple_non_block_rules_return_none(self) -> None:
        rules = [
            PatronBlockingRule(name="r1", rule="SOMETHING"),
            PatronBlockingRule(name="r2", rule="ELSE"),
        ]
        assert check_patron_blocking_rules(rules) is None

    def test_block_rule_returns_problem_detail(self) -> None:
        rules = [PatronBlockingRule(name="block-all", rule="BLOCK")]
        result = check_patron_blocking_rules(rules)
        assert isinstance(result, ProblemDetail)
        assert result.status_code == 403
        assert result.uri == BLOCKED_CREDENTIALS.uri

    def test_block_rule_uses_custom_message(self) -> None:
        rules = [
            PatronBlockingRule(
                name="block-all", rule="BLOCK", message="Custom patron message."
            )
        ]
        result = check_patron_blocking_rules(rules)
        assert isinstance(result, ProblemDetail)
        assert result.detail == "Custom patron message."

    def test_block_rule_uses_default_message_when_no_message(self) -> None:
        rules = [PatronBlockingRule(name="block-all", rule="BLOCK")]
        result = check_patron_blocking_rules(rules)
        assert isinstance(result, ProblemDetail)
        assert result.detail == "Access blocked by library policy."

    def test_first_block_rule_wins(self) -> None:
        """If multiple BLOCK rules exist the first one is returned."""
        rules = [
            PatronBlockingRule(name="first", rule="BLOCK", message="First."),
            PatronBlockingRule(name="second", rule="BLOCK", message="Second."),
        ]
        result = check_patron_blocking_rules(rules)
        assert isinstance(result, ProblemDetail)
        assert result.detail == "First."

    def test_non_block_rule_before_block_rule(self) -> None:
        """A BLOCK rule preceded by a non-BLOCK rule still triggers."""
        rules = [
            PatronBlockingRule(name="allow", rule="ALLOW"),
            PatronBlockingRule(name="block", rule="BLOCK", message="Blocked."),
        ]
        result = check_patron_blocking_rules(rules)
        assert isinstance(result, ProblemDetail)
        assert result.detail == "Blocked."


# ---------------------------------------------------------------------------
# check_patron_blocking_rules_with_evaluator
# ---------------------------------------------------------------------------


class TestCheckPatronBlockingRulesWithEvaluator:
    """Tests for the simpleeval-based check_patron_blocking_rules_with_evaluator()."""

    def test_empty_rules_returns_none(self) -> None:
        assert check_patron_blocking_rules_with_evaluator([], {}) is None

    def test_true_rule_blocks(self) -> None:
        rules = [PatronBlockingRule(name="always-block", rule="True")]
        result = check_patron_blocking_rules_with_evaluator(rules, {})
        assert isinstance(result, ProblemDetail)
        assert result.status_code == 403
        assert result.uri == BLOCKED_CREDENTIALS.uri

    def test_true_rule_uses_custom_message(self) -> None:
        rules = [
            PatronBlockingRule(
                name="always-block", rule="True", message="Library A blocks you."
            )
        ]
        result = check_patron_blocking_rules_with_evaluator(rules, {})
        assert isinstance(result, ProblemDetail)
        assert result.detail == "Library A blocks you."

    def test_true_rule_uses_default_message_when_none(self) -> None:
        rules = [PatronBlockingRule(name="always-block", rule="True")]
        result = check_patron_blocking_rules_with_evaluator(rules, {})
        assert isinstance(result, ProblemDetail)
        assert result.detail == "Patron is blocked by library policy."

    def test_false_rule_does_not_block(self) -> None:
        rules = [PatronBlockingRule(name="never-block", rule="False")]
        assert check_patron_blocking_rules_with_evaluator(rules, {}) is None

    def test_expression_with_placeholder(self) -> None:
        rules = [PatronBlockingRule(name="high-fines", rule="{fines} > 10.0")]
        assert (
            check_patron_blocking_rules_with_evaluator(rules, {"fines": 15.0})
            is not None
        )
        assert check_patron_blocking_rules_with_evaluator(rules, {"fines": 5.0}) is None

    def test_missing_placeholder_fails_closed(self) -> None:
        """A rule referencing a missing placeholder key must block (fail closed)."""
        rules = [PatronBlockingRule(name="needs-dob", rule="{dob} == '2000-01-01'")]
        result = check_patron_blocking_rules_with_evaluator(rules, {})
        assert isinstance(result, ProblemDetail)
        assert result.status_code == 403

    def test_missing_placeholder_uses_generic_message(self) -> None:
        rules = [
            PatronBlockingRule(
                name="needs-dob",
                rule="{dob} == '2000-01-01'",
                message="Custom should not appear.",
            )
        ]
        result = check_patron_blocking_rules_with_evaluator(rules, {})
        assert isinstance(result, ProblemDetail)
        # Generic message on error, not the rule's message
        assert result.detail == "Patron is blocked by library policy."

    def test_invalid_expression_fails_closed(self) -> None:
        """A syntactically invalid rule must block (fail closed)."""
        rules = [PatronBlockingRule(name="bad-rule", rule="{fines} >>>!!! invalid")]
        result = check_patron_blocking_rules_with_evaluator(rules, {"fines": 5.0})
        assert isinstance(result, ProblemDetail)
        assert result.status_code == 403

    def test_non_bool_result_fails_closed(self) -> None:
        rules = [PatronBlockingRule(name="non-bool", rule="{fines} + 1")]
        result = check_patron_blocking_rules_with_evaluator(rules, {"fines": 5.0})
        assert isinstance(result, ProblemDetail)
        assert result.status_code == 403

    def test_first_matching_rule_wins(self) -> None:
        rules = [
            PatronBlockingRule(name="r1", rule="True", message="First."),
            PatronBlockingRule(name="r2", rule="True", message="Second."),
        ]
        result = check_patron_blocking_rules_with_evaluator(rules, {})
        assert isinstance(result, ProblemDetail)
        assert result.detail == "First."

    def test_false_then_true_rule(self) -> None:
        rules = [
            PatronBlockingRule(name="r1", rule="False"),
            PatronBlockingRule(name="r2", rule="True", message="Second rule."),
        ]
        result = check_patron_blocking_rules_with_evaluator(rules, {})
        assert isinstance(result, ProblemDetail)
        assert result.detail == "Second rule."

    def test_error_is_logged(self) -> None:
        """Evaluation errors are logged server-side."""
        rules = [PatronBlockingRule(name="bad", rule="{missing_key} == 1")]
        mock_log = MagicMock()
        check_patron_blocking_rules_with_evaluator(rules, {}, log=mock_log)
        assert mock_log.error.called


# ---------------------------------------------------------------------------
# build_runtime_values_from_patron
# ---------------------------------------------------------------------------


class TestBuildRuntimeValuesFromPatron:
    def _make_patron(self, fines=None, external_type=None):
        patron = MagicMock(spec=Patron)
        patron.fines = fines
        patron.external_type = external_type
        return patron

    def test_fines_none_becomes_zero(self) -> None:
        patron = self._make_patron(fines=None)
        values = build_runtime_values_from_patron(patron)
        assert values["fines"] == 0.0

    def test_fines_string_converted_to_float(self) -> None:
        patron = self._make_patron(fines="12.50")
        values = build_runtime_values_from_patron(patron)
        assert values["fines"] == 12.50

    def test_fines_unparseable_becomes_zero(self) -> None:
        patron = self._make_patron(fines="not-a-number")
        values = build_runtime_values_from_patron(patron)
        assert values["fines"] == 0.0

    def test_patron_type_present(self) -> None:
        patron = self._make_patron(external_type="student")
        values = build_runtime_values_from_patron(patron)
        assert values["patron_type"] == "student"

    def test_patron_type_none_becomes_empty_string(self) -> None:
        patron = self._make_patron(external_type=None)
        values = build_runtime_values_from_patron(patron)
        assert values["patron_type"] == ""

    def test_returns_dict(self) -> None:
        patron = self._make_patron()
        values = build_runtime_values_from_patron(patron)
        assert isinstance(values, dict)


# ---------------------------------------------------------------------------
# RULE_VALIDATION_TEST_VALUES
# ---------------------------------------------------------------------------


class TestRuleValidationTestValues:
    def test_contains_fines(self) -> None:
        assert "fines" in RULE_VALIDATION_TEST_VALUES

    def test_contains_patron_type(self) -> None:
        assert "patron_type" in RULE_VALIDATION_TEST_VALUES

    def test_contains_dob(self) -> None:
        assert "dob" in RULE_VALIDATION_TEST_VALUES

    def test_fines_is_numeric(self) -> None:
        assert isinstance(RULE_VALIDATION_TEST_VALUES["fines"], (int, float))


# ---------------------------------------------------------------------------
# BasicAuthProviderLibrarySettings — patron_blocking_rules field validation
# ---------------------------------------------------------------------------


class TestBasicAuthLibrarySettingsBlockingRules:
    """Tests for patron_blocking_rules on BasicAuthProviderLibrarySettings.

    The field lives on the base class so every basic-auth protocol (SIP2,
    Millennium, SirsiDynix, …) inherits validation for free.
    """

    def test_default_is_empty_list(self) -> None:
        settings = BasicAuthProviderLibrarySettings()
        assert settings.patron_blocking_rules == []

    def test_round_trip_with_valid_rules(self) -> None:
        settings = BasicAuthProviderLibrarySettings(
            patron_blocking_rules=[
                {"name": "block-all", "rule": "True", "message": "Sorry"},
                {"name": "no-op", "rule": "False"},
            ]
        )
        assert len(settings.patron_blocking_rules) == 2
        assert settings.patron_blocking_rules[0].name == "block-all"
        assert settings.patron_blocking_rules[0].rule == "True"
        assert settings.patron_blocking_rules[0].message == "Sorry"
        assert settings.patron_blocking_rules[1].name == "no-op"
        assert settings.patron_blocking_rules[1].message is None

    def test_model_dump_excludes_empty_list_by_default(self) -> None:
        """Empty list (the default) is omitted from model_dump so we don't
        store defaults in the JSON blob."""
        settings = BasicAuthProviderLibrarySettings()
        assert "patron_blocking_rules" not in settings.model_dump()

    def test_model_dump_includes_non_empty_list(self) -> None:
        settings = BasicAuthProviderLibrarySettings(
            patron_blocking_rules=[{"name": "r", "rule": "True"}]
        )
        dumped = settings.model_dump()
        assert "patron_blocking_rules" in dumped
        assert len(dumped["patron_blocking_rules"]) == 1
        assert dumped["patron_blocking_rules"][0]["name"] == "r"

    def test_model_validate_missing_field_produces_default(self) -> None:
        """A settings dict without the key deserialises to an empty list."""
        settings = BasicAuthProviderLibrarySettings.model_validate({})
        assert settings.patron_blocking_rules == []

    def test_validate_empty_name_raises(self) -> None:
        with raises_problem_detail() as info:
            BasicAuthProviderLibrarySettings(
                patron_blocking_rules=[{"name": "", "rule": "True"}]
            )
        assert info.value.detail is not None
        assert "index 0" in info.value.detail
        assert "'name' must not be empty" in info.value.detail

    def test_validate_whitespace_only_name_raises(self) -> None:
        # str_strip_whitespace=True on PatronBlockingRule strips "   " to ""
        with raises_problem_detail() as info:
            BasicAuthProviderLibrarySettings(
                patron_blocking_rules=[{"name": "   ", "rule": "True"}]
            )
        assert info.value.detail is not None
        assert "index 0" in info.value.detail
        assert "'name' must not be empty" in info.value.detail

    def test_validate_empty_rule_raises(self) -> None:
        with raises_problem_detail() as info:
            BasicAuthProviderLibrarySettings(
                patron_blocking_rules=[{"name": "valid-name", "rule": ""}]
            )
        assert info.value.detail is not None
        assert "index 0" in info.value.detail
        assert "'rule' expression must not be empty" in info.value.detail

    def test_validate_duplicate_name_raises(self) -> None:
        with raises_problem_detail() as info:
            BasicAuthProviderLibrarySettings(
                patron_blocking_rules=[
                    {"name": "same", "rule": "True"},
                    {"name": "same", "rule": "False"},
                ]
            )
        assert info.value.detail is not None
        assert "index 1" in info.value.detail
        assert "duplicate rule name" in info.value.detail
        assert "'same'" in info.value.detail

    def test_validate_duplicate_at_higher_index(self) -> None:
        """The error message cites the index of the second occurrence."""
        with raises_problem_detail() as info:
            BasicAuthProviderLibrarySettings(
                patron_blocking_rules=[
                    {"name": "a", "rule": "True"},
                    {"name": "b", "rule": "False"},
                    {"name": "a", "rule": "True"},
                ]
            )
        assert info.value.detail is not None
        assert "index 2" in info.value.detail

    # ------------------------------------------------------------------
    # New: simpleeval-based rule expression validation
    # ------------------------------------------------------------------

    def test_validate_rule_length_over_1000_raises(self) -> None:
        """rule text > 1000 characters is rejected at validation time."""
        long_rule = "True and " * 200  # well over 1000 chars
        with raises_problem_detail() as info:
            BasicAuthProviderLibrarySettings(
                patron_blocking_rules=[{"name": "r", "rule": long_rule}]
            )
        assert info.value.detail is not None
        assert "index 0" in info.value.detail

    def test_validate_message_length_over_1000_raises(self) -> None:
        """message > 1000 characters is rejected at validation time."""
        with raises_problem_detail() as info:
            BasicAuthProviderLibrarySettings(
                patron_blocking_rules=[
                    {
                        "name": "r",
                        "rule": "True",
                        "message": "x" * 1001,
                    }
                ]
            )
        assert info.value.detail is not None
        assert "index 0" in info.value.detail
        assert "message" in info.value.detail.lower()

    def test_validate_message_exactly_1000_chars_passes(self) -> None:
        """message of exactly 1000 chars is accepted."""
        BasicAuthProviderLibrarySettings(
            patron_blocking_rules=[
                {
                    "name": "r",
                    "rule": "True",
                    "message": "x" * 1000,
                }
            ]
        )

    def test_validate_invalid_syntax_raises(self) -> None:
        """A syntactically invalid rule expression is rejected."""
        with raises_problem_detail() as info:
            BasicAuthProviderLibrarySettings(
                patron_blocking_rules=[
                    {"name": "bad", "rule": "{fines} >>>??? bad syntax"}
                ]
            )
        assert info.value.detail is not None
        assert "index 0" in info.value.detail

    def test_validate_non_bool_result_raises(self) -> None:
        """An expression that does not evaluate to a bool is rejected."""
        with raises_problem_detail() as info:
            BasicAuthProviderLibrarySettings(
                patron_blocking_rules=[{"name": "not-bool", "rule": "{fines} + 1"}]
            )
        assert info.value.detail is not None
        assert "index 0" in info.value.detail

    def test_validate_valid_placeholder_expression_passes(self) -> None:
        """A valid simpleeval expression using known placeholders is accepted."""
        settings = BasicAuthProviderLibrarySettings(
            patron_blocking_rules=[{"name": "fines-check", "rule": "{fines} > 10.0"}]
        )
        assert settings.patron_blocking_rules[0].name == "fines-check"

    def test_validate_age_in_years_expression_passes(self) -> None:
        """age_in_years() built-in function is available in validation."""
        settings = BasicAuthProviderLibrarySettings(
            patron_blocking_rules=[
                {"name": "age-check", "rule": "age_in_years({dob}) < 18"}
            ]
        )
        assert settings.patron_blocking_rules[0].name == "age-check"

    def test_validate_unknown_placeholder_raises(self) -> None:
        """A placeholder not in RULE_VALIDATION_TEST_VALUES is rejected."""
        with raises_problem_detail() as info:
            BasicAuthProviderLibrarySettings(
                patron_blocking_rules=[
                    {"name": "unknown", "rule": "{totally_unknown_key} > 0"}
                ]
            )
        assert info.value.detail is not None
        assert "index 0" in info.value.detail


# ---------------------------------------------------------------------------
# supports_patron_blocking_rules flag + authenticate hook
# ---------------------------------------------------------------------------


class TestSupportsPatronBlockingRulesFlag:
    """Tests for the supports_patron_blocking_rules ClassVar flag."""

    _PATCH_TARGET = (
        "palace.manager.api.authentication.basic."
        "BasicAuthenticationProvider._do_authenticate"
    )

    def test_base_class_flag_is_false(self) -> None:
        assert BasicAuthenticationProvider.supports_patron_blocking_rules is False

    def test_sip2_flag_is_true(self) -> None:
        assert SIP2AuthenticationProvider.supports_patron_blocking_rules is True

    def test_blocking_skipped_when_flag_false(self) -> None:
        """When supports_patron_blocking_rules is False, a True rule is ignored
        and the Patron object is returned unchanged."""
        mock_patron = MagicMock(spec=Patron)

        with patch(self._PATCH_TARGET, return_value=mock_patron):
            provider = MagicMock(spec=BasicAuthenticationProvider)
            provider.supports_patron_blocking_rules = False
            provider.patron_blocking_rules = [
                PatronBlockingRule(name="block-all", rule="True")
            ]
            provider._do_authenticate = MagicMock(return_value=mock_patron)
            result = BasicAuthenticationProvider.authenticate(provider, MagicMock(), {})

        assert result is mock_patron

    def test_blocking_applied_when_flag_true(self) -> None:
        """When supports_patron_blocking_rules is True, a True rule intercepts
        the authenticated Patron and returns a 403 ProblemDetail."""
        mock_patron = MagicMock(spec=Patron)

        provider = MagicMock(spec=BasicAuthenticationProvider)
        provider.supports_patron_blocking_rules = True
        provider.patron_blocking_rules = [
            PatronBlockingRule(
                name="block-all", rule="True", message="Blocked by policy."
            )
        ]
        provider._do_authenticate = MagicMock(return_value=mock_patron)
        provider.log = MagicMock()

        result = BasicAuthenticationProvider.authenticate(provider, MagicMock(), {})

        assert isinstance(result, ProblemDetail)
        assert result.status_code == 403
        assert result.uri == BLOCKED_CREDENTIALS.uri
        assert result.detail == "Blocked by policy."

    def test_blocking_not_applied_when_do_authenticate_returns_none(self) -> None:
        """When _do_authenticate returns None (bad credentials), the flag has no
        effect — None is passed through."""
        provider = MagicMock(spec=BasicAuthenticationProvider)
        provider.supports_patron_blocking_rules = True
        provider.patron_blocking_rules = [
            PatronBlockingRule(name="block-all", rule="True")
        ]
        provider._do_authenticate = MagicMock(return_value=None)

        result = BasicAuthenticationProvider.authenticate(provider, MagicMock(), {})

        assert result is None

    def test_blocking_not_applied_when_do_authenticate_returns_problem_detail(
        self,
    ) -> None:
        """When _do_authenticate itself returns a ProblemDetail (e.g. connection
        failure), blocking rules are not evaluated — the original error is returned."""
        from palace.manager.api.problem_details import INVALID_CREDENTIALS

        provider = MagicMock(spec=BasicAuthenticationProvider)
        provider.supports_patron_blocking_rules = True
        provider.patron_blocking_rules = [
            PatronBlockingRule(name="block-all", rule="True")
        ]
        provider._do_authenticate = MagicMock(return_value=INVALID_CREDENTIALS)

        result = BasicAuthenticationProvider.authenticate(provider, MagicMock(), {})

        assert result is INVALID_CREDENTIALS

    def test_false_rule_does_not_block(self) -> None:
        """A False rule does not block the patron."""
        mock_patron = MagicMock(spec=Patron)

        provider = MagicMock(spec=BasicAuthenticationProvider)
        provider.supports_patron_blocking_rules = True
        provider.patron_blocking_rules = [
            PatronBlockingRule(name="never-block", rule="False")
        ]
        provider._do_authenticate = MagicMock(return_value=mock_patron)
        provider.log = MagicMock()

        result = BasicAuthenticationProvider.authenticate(provider, MagicMock(), {})

        assert result is mock_patron

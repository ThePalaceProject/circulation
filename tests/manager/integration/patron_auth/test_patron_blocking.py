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
from palace.manager.api.problem_details import BLOCKED_BY_POLICY, INVALID_CREDENTIALS
from palace.manager.integration.patron_auth.patron_blocking import (
    PatronBlockingRule,
    build_runtime_values_from_patron,
    build_values_from_sip2_info,
    check_patron_blocking_rules_with_evaluator,
)
from palace.manager.integration.patron_auth.sip2.provider import (
    SIP2AuthenticationProvider,
    SIP2LibrarySettings,
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
        assert result.uri == BLOCKED_BY_POLICY.uri

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

    def test_rules_rejected_when_provider_does_not_support(self) -> None:
        """Providers that do not support blocking rules cannot have rules configured."""
        with raises_problem_detail() as info:
            BasicAuthProviderLibrarySettings(
                patron_blocking_rules=[{"name": "r", "rule": "True"}]
            )
        assert info.value.detail is not None
        assert "not supported" in info.value.detail.lower()

    def test_round_trip_with_valid_rules(self) -> None:
        settings = SIP2LibrarySettings(
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
        settings = SIP2LibrarySettings(
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
            SIP2LibrarySettings(patron_blocking_rules=[{"name": "", "rule": "True"}])
        assert info.value.detail is not None
        assert "index 0" in info.value.detail
        assert "'name' must not be empty" in info.value.detail

    def test_validate_whitespace_only_name_raises(self) -> None:
        # str_strip_whitespace=True on PatronBlockingRule strips "   " to ""
        with raises_problem_detail() as info:
            SIP2LibrarySettings(patron_blocking_rules=[{"name": "   ", "rule": "True"}])
        assert info.value.detail is not None
        assert "index 0" in info.value.detail
        assert "'name' must not be empty" in info.value.detail

    def test_validate_empty_rule_raises(self) -> None:
        with raises_problem_detail() as info:
            SIP2LibrarySettings(
                patron_blocking_rules=[{"name": "valid-name", "rule": ""}]
            )
        assert info.value.detail is not None
        assert "index 0" in info.value.detail
        assert "'rule' expression must not be empty" in info.value.detail

    def test_validate_duplicate_name_raises(self) -> None:
        with raises_problem_detail() as info:
            SIP2LibrarySettings(
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
            SIP2LibrarySettings(
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
            SIP2LibrarySettings(
                patron_blocking_rules=[{"name": "r", "rule": long_rule}]
            )
        assert info.value.detail is not None
        assert "index 0" in info.value.detail

    def test_validate_message_length_over_1000_raises(self) -> None:
        """message > 1000 characters is rejected at validation time."""
        with raises_problem_detail() as info:
            SIP2LibrarySettings(
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
        SIP2LibrarySettings(
            patron_blocking_rules=[
                {
                    "name": "r",
                    "rule": "True",
                    "message": "x" * 1000,
                }
            ]
        )

    def test_validate_any_rule_expression_passes_static_check(self) -> None:
        """Any non-empty rule expression that fits within 1000 chars is accepted
        by static Pydantic validation.  Full syntax/semantic validation happens
        at admin-save time via a live SIP2 call."""
        settings = SIP2LibrarySettings(
            patron_blocking_rules=[
                {"name": "fines-check", "rule": "{fines} > 10.0"},
                {"name": "any-field", "rule": "{totally_unknown_key} > 0"},
                {"name": "non-bool", "rule": "{fines} + 1"},
            ]
        )
        assert len(settings.patron_blocking_rules) == 3

    def test_validate_rule_exactly_1000_chars_passes(self) -> None:
        """rule text of exactly 1000 chars is accepted."""
        rule = "T" * 1000
        settings = SIP2LibrarySettings(
            patron_blocking_rules=[{"name": "r", "rule": rule}]
        )
        assert settings.patron_blocking_rules[0].rule == rule


# ---------------------------------------------------------------------------
# supports_patron_blocking_rules flag + authenticate hook
# ---------------------------------------------------------------------------


class TestBasicAuthenticationProvider:
    """Tests for BasicAuthenticationProvider.authenticate with patron blocking rules."""

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

        with patch(self._PATCH_TARGET, return_value=(mock_patron, {})):
            provider = MagicMock(spec=BasicAuthenticationProvider)
            provider.supports_patron_blocking_rules = False
            provider.patron_blocking_rules = [
                PatronBlockingRule(name="block-all", rule="True")
            ]
            provider._do_authenticate = MagicMock(return_value=(mock_patron, {}))
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
        provider._do_authenticate = MagicMock(return_value=(mock_patron, {}))
        provider.log = MagicMock()

        result = BasicAuthenticationProvider.authenticate(provider, MagicMock(), {})

        assert isinstance(result, ProblemDetail)
        assert result.status_code == 403
        assert result.uri == BLOCKED_BY_POLICY.uri
        assert result.detail == "Blocked by policy."

    def test_blocking_not_applied_when_do_authenticate_returns_none(self) -> None:
        """When _do_authenticate returns None (bad credentials), the flag has no
        effect — None is passed through."""
        provider = MagicMock(spec=BasicAuthenticationProvider)
        provider.supports_patron_blocking_rules = True
        provider.patron_blocking_rules = [
            PatronBlockingRule(name="block-all", rule="True")
        ]
        provider._do_authenticate = MagicMock(return_value=(None, {}))

        result = BasicAuthenticationProvider.authenticate(provider, MagicMock(), {})

        assert result is None

    def test_blocking_not_applied_when_do_authenticate_returns_problem_detail(
        self,
    ) -> None:
        """When _do_authenticate itself returns a ProblemDetail (e.g. connection
        failure), blocking rules are not evaluated — the original error is returned."""
        provider = MagicMock(spec=BasicAuthenticationProvider)
        provider.supports_patron_blocking_rules = True
        provider.patron_blocking_rules = [
            PatronBlockingRule(name="block-all", rule="True")
        ]
        provider._do_authenticate = MagicMock(return_value=(INVALID_CREDENTIALS, {}))

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
        provider._do_authenticate = MagicMock(return_value=(mock_patron, {}))
        provider.log = MagicMock()

        result = BasicAuthenticationProvider.authenticate(provider, MagicMock(), {})

        assert result is mock_patron


# ---------------------------------------------------------------------------
# build_values_from_sip2_info
# ---------------------------------------------------------------------------


class TestBuildValuesFromSip2Info:
    """Tests for build_values_from_sip2_info()."""

    def test_fee_amount_plain_float_string(self) -> None:
        """fee_amount like '5.00' is parsed to a float under the 'fines' key."""
        values = build_values_from_sip2_info({"fee_amount": "5.00"})
        assert values["fines"] == pytest.approx(5.0)

    def test_fee_amount_dollar_prefix(self) -> None:
        """fee_amount like '$12.50' (dollar sign prefix) is parsed correctly."""
        values = build_values_from_sip2_info({"fee_amount": "$12.50"})
        assert values["fines"] == pytest.approx(12.5)

    def test_fee_amount_missing_defaults_to_zero(self) -> None:
        """Absent fee_amount → fines = 0.0."""
        values = build_values_from_sip2_info({})
        assert values["fines"] == pytest.approx(0.0)

    def test_fee_amount_unparseable_defaults_to_zero(self) -> None:
        """Unparseable fee_amount → fines = 0.0 (no exception raised)."""
        values = build_values_from_sip2_info({"fee_amount": "not-a-number"})
        assert values["fines"] == pytest.approx(0.0)

    def test_all_raw_sip2_fields_are_included(self) -> None:
        """Every key in the raw info dict is present verbatim in the result."""
        info = {
            "fee_amount": "3.50",
            "sipserver_patron_class": "student",
            "polaris_patron_birthdate": "2000-06-15",
            "patron_status": "active",
            "personal_name": "Jane Doe",
        }
        values = build_values_from_sip2_info(info)
        # All raw keys must be present.
        for k, v in info.items():
            assert values[k] == v

    def test_normalized_fines_added_alongside_raw_fee_amount(self) -> None:
        """The 'fines' key (float) is added IN ADDITION to the raw fee_amount."""
        info = {"fee_amount": "3.50"}
        values = build_values_from_sip2_info(info)
        assert "fee_amount" in values  # raw field preserved
        assert values["fines"] == pytest.approx(3.5)  # normalised key added

    def test_empty_info_dict_has_only_fines_key(self) -> None:
        """An empty SIP2 response still produces the 'fines' key (defaulting to 0)."""
        values = build_values_from_sip2_info({})
        assert values == {"fines": pytest.approx(0.0)}

    def test_polaris_patron_birthdate_accessible_directly(self) -> None:
        """polaris_patron_birthdate is accessible under its own raw key."""
        values = build_values_from_sip2_info({"polaris_patron_birthdate": "1990-01-01"})
        assert values["polaris_patron_birthdate"] == "1990-01-01"

"""Tests for the shared patron blocking-rules infrastructure.

Covers:

- :class:`~palace.manager.api.authentication.patron_blocking_rules.patron_blocking.PatronBlockingRule` model
- :func:`~palace.manager.api.authentication.patron_blocking_rules.patron_blocking.check_patron_blocking_rules_with_evaluator`
- :func:`~palace.manager.api.authentication.patron_blocking_rules.patron_blocking.build_runtime_values_from_patron`
- ``patron_blocking_rules`` field on :class:`~palace.manager.api.authentication.basic.PatronBlockingRulesSetting`
- :class:`~palace.manager.api.authentication.patron_blocking_rules.mixin.HasPatronBlockingRules` mixin
- blocking-rules hook in :meth:`~palace.manager.api.authentication.basic.BasicAuthenticationProvider.authenticate`
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from palace.manager.api.authentication.basic import (
    BasicAuthenticationProvider,
    BasicAuthProviderLibrarySettings,
    BasicAuthProviderSettings,
    PatronBlockingRulesSetting,
)
from palace.manager.api.authentication.patron_blocking_rules.mixin import (
    HasPatronBlockingRules,
)
from palace.manager.api.authentication.patron_blocking_rules.patron_blocking import (
    PatronBlockingRule,
    build_runtime_values_from_patron,
    check_patron_blocking_rules_with_evaluator,
)
from palace.manager.api.problem_details import BLOCKED_BY_POLICY, INVALID_CREDENTIALS
from palace.manager.integration.patron_auth.minimal_authentication import (
    MinimalAuthenticationProvider,
)
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.problem_detail import ProblemDetail
from tests.fixtures.problem_detail import raises_problem_detail


class ConcreteSettings(PatronBlockingRulesSetting, BasicAuthProviderLibrarySettings):
    """Minimal concrete settings class used to test :class:`PatronBlockingRulesSetting` in isolation."""


class _ConcreteBlockingProvider(HasPatronBlockingRules, MinimalAuthenticationProvider):
    """Minimal provider that actually inherits :class:`HasPatronBlockingRules`.

    Used in :class:`TestBasicAuthenticationProvider` to exercise the blocking-rules
    hook in :meth:`BasicAuthenticationProvider.authenticate` without metaclass tricks.
    """

    @classmethod
    def fetch_live_rule_validation_values(cls, settings: Any) -> dict[str, Any]:
        return {}


class TestPatronBlockingRule:
    """Unit tests for the :class:`~palace.manager.api.authentication.patron_blocking_rules.patron_blocking.PatronBlockingRule` value object."""

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


class TestCheckPatronBlockingRulesWithEvaluator:
    """Tests for :func:`~palace.manager.api.authentication.patron_blocking_rules.patron_blocking.check_patron_blocking_rules_with_evaluator`."""

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

    def test_missing_placeholder_ignored_fail_open(self) -> None:
        """A rule referencing a missing placeholder is ignored (fail-open)."""
        rules = [PatronBlockingRule(name="needs-dob", rule="{dob} == '2000-01-01'")]
        result = check_patron_blocking_rules_with_evaluator(rules, {})
        assert result is None

    def test_invalid_expression_ignored_fail_open(self) -> None:
        """A syntactically invalid rule is ignored (fail-open)."""
        rules = [PatronBlockingRule(name="bad-rule", rule="{fines} >>>!!! invalid")]
        result = check_patron_blocking_rules_with_evaluator(rules, {"fines": 5.0})
        assert result is None

    def test_non_bool_result_ignored_fail_open(self) -> None:
        """A rule that does not evaluate to bool is ignored (fail-open)."""
        rules = [PatronBlockingRule(name="non-bool", rule="{fines} + 1")]
        result = check_patron_blocking_rules_with_evaluator(rules, {"fines": 5.0})
        assert result is None

    def test_error_rule_then_blocking_rule_blocks(self) -> None:
        """When an error rule is followed by a blocking rule, the blocking rule applies."""
        rules = [
            PatronBlockingRule(name="bad", rule="{missing} == 1"),
            PatronBlockingRule(name="block", rule="True", message="Blocked."),
        ]
        result = check_patron_blocking_rules_with_evaluator(rules, {})
        assert isinstance(result, ProblemDetail)
        assert result.detail == "Blocked."

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


class TestBuildRuntimeValuesFromPatron:
    """Tests for :func:`~palace.manager.api.authentication.patron_blocking_rules.patron_blocking.build_runtime_values_from_patron`."""

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


class TestBasicAuthLibrarySettingsBlockingRules:
    """Tests for ``patron_blocking_rules`` on :class:`PatronBlockingRulesSetting`.

    Uses :class:`ConcreteSettings` — a minimal class that mixes in
    :class:`PatronBlockingRulesSetting` — to test the mixin in isolation without
    coupling to any specific provider.
    """

    def test_default_is_empty_list(self) -> None:
        settings = ConcreteSettings()
        assert settings.patron_blocking_rules == []

    def test_base_library_settings_has_no_blocking_rules_field(self) -> None:
        """BasicAuthProviderLibrarySettings does not have patron_blocking_rules;
        only providers that mix in PatronBlockingRulesSetting do."""
        settings = BasicAuthProviderLibrarySettings()
        assert not hasattr(settings, "patron_blocking_rules")

    def test_round_trip_with_valid_rules(self) -> None:
        settings = ConcreteSettings(
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
        settings = ConcreteSettings()
        assert "patron_blocking_rules" not in settings.model_dump()

    def test_model_dump_includes_non_empty_list(self) -> None:
        settings = ConcreteSettings(
            patron_blocking_rules=[{"name": "r", "rule": "True"}]
        )
        dumped = settings.model_dump()
        assert "patron_blocking_rules" in dumped
        assert len(dumped["patron_blocking_rules"]) == 1
        assert dumped["patron_blocking_rules"][0]["name"] == "r"

    def test_model_validate_missing_field_produces_default(self) -> None:
        """A settings dict without the key deserialises to an empty list."""
        settings = ConcreteSettings.model_validate({})
        assert settings.patron_blocking_rules == []

    def test_validate_empty_name_raises(self) -> None:
        with raises_problem_detail() as info:
            ConcreteSettings(patron_blocking_rules=[{"name": "", "rule": "True"}])
        assert info.value.detail is not None
        assert "index 0" in info.value.detail
        assert "'name' must not be empty" in info.value.detail

    def test_validate_whitespace_only_name_raises(self) -> None:
        # str_strip_whitespace=True on PatronBlockingRule strips "   " to ""
        with raises_problem_detail() as info:
            ConcreteSettings(patron_blocking_rules=[{"name": "   ", "rule": "True"}])
        assert info.value.detail is not None
        assert "index 0" in info.value.detail
        assert "'name' must not be empty" in info.value.detail

    def test_validate_empty_rule_raises(self) -> None:
        with raises_problem_detail() as info:
            ConcreteSettings(patron_blocking_rules=[{"name": "valid-name", "rule": ""}])
        assert info.value.detail is not None
        assert "index 0" in info.value.detail
        assert "'rule' expression must not be empty" in info.value.detail

    def test_validate_duplicate_name_raises(self) -> None:
        with raises_problem_detail() as info:
            ConcreteSettings(
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
            ConcreteSettings(
                patron_blocking_rules=[
                    {"name": "a", "rule": "True"},
                    {"name": "b", "rule": "False"},
                    {"name": "a", "rule": "True"},
                ]
            )
        assert info.value.detail is not None
        assert "index 2" in info.value.detail

    # ------------------------------------------------------------------
    # simpleeval-based rule expression validation
    # ------------------------------------------------------------------

    def test_validate_rule_length_over_1000_raises(self) -> None:
        """rule text > 1000 characters is rejected at validation time."""
        long_rule = "True and " * 200  # well over 1000 chars
        with raises_problem_detail() as info:
            ConcreteSettings(patron_blocking_rules=[{"name": "r", "rule": long_rule}])
        assert info.value.detail is not None
        assert "index 0" in info.value.detail

    def test_validate_message_length_over_1000_raises(self) -> None:
        """message > 1000 characters is rejected at validation time."""
        with raises_problem_detail() as info:
            ConcreteSettings(
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
        ConcreteSettings(
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
        by static Pydantic validation."""
        settings = ConcreteSettings(
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
        settings = ConcreteSettings(patron_blocking_rules=[{"name": "r", "rule": rule}])
        assert settings.patron_blocking_rules[0].rule == rule


class TestBasicAuthenticationProvider:
    """Tests for :meth:`~palace.manager.api.authentication.basic.BasicAuthenticationProvider.authenticate` with patron blocking rules."""

    _PATCH_TARGET = (
        "palace.manager.api.authentication.basic."
        "BasicAuthenticationProvider._do_authenticate"
    )

    def test_base_class_does_not_support_blocking_rules(self) -> None:
        assert not issubclass(BasicAuthenticationProvider, HasPatronBlockingRules)

    def test_blocking_skipped_when_not_has_patron_blocking_rules(self) -> None:
        """When the provider is not an instance of HasPatronBlockingRules, a True
        rule is ignored and the Patron object is returned unchanged."""
        mock_patron = MagicMock(spec=Patron)

        with patch(self._PATCH_TARGET, return_value=(mock_patron, {})):
            provider = MagicMock(spec=BasicAuthenticationProvider)
            provider.patron_blocking_rules = [
                PatronBlockingRule(name="block-all", rule="True")
            ]
            provider._do_authenticate = MagicMock(return_value=(mock_patron, {}))
            result = BasicAuthenticationProvider.authenticate(provider, MagicMock(), {})

        assert result is mock_patron

    def test_blocking_applied_when_has_patron_blocking_rules(self) -> None:
        """When the provider is an instance of HasPatronBlockingRules, a True rule
        intercepts the authenticated Patron and returns a 403 ProblemDetail."""
        mock_patron = MagicMock(spec=Patron)
        mock_log = MagicMock()

        provider = _ConcreteBlockingProvider(
            0, 0, BasicAuthProviderSettings(), BasicAuthProviderLibrarySettings()
        )
        provider.patron_blocking_rules = [
            PatronBlockingRule(
                name="block-all", rule="True", message="Blocked by policy."
            )
        ]

        with (
            patch.object(
                _ConcreteBlockingProvider,
                "log",
                new_callable=PropertyMock,
                return_value=mock_log,
            ),
            patch.object(
                _ConcreteBlockingProvider,
                "_do_authenticate",
                return_value=(mock_patron, {}),
            ),
        ):
            result = provider.authenticate(MagicMock(), {})

        assert isinstance(result, ProblemDetail)
        assert result.status_code == 403
        assert result.uri == BLOCKED_BY_POLICY.uri
        assert result.detail == "Blocked by policy."
        mock_log.info.assert_any_call("Patron blocking rules evaluation attempted")

    def test_blocking_not_applied_when_do_authenticate_returns_none(self) -> None:
        """
        When _do_authenticate returns None (bad credentials), blocking rules
        are not evaluated — None is passed through.
        """
        provider = MagicMock(spec=BasicAuthenticationProvider)
        provider.patron_blocking_rules = [
            PatronBlockingRule(name="block-all", rule="True")
        ]
        provider._do_authenticate = MagicMock(return_value=(None, {}))

        result = BasicAuthenticationProvider.authenticate(provider, MagicMock(), {})

        assert result is None

    def test_blocking_not_applied_when_do_authenticate_returns_problem_detail(
        self,
    ) -> None:
        """
        When _do_authenticate itself returns a ProblemDetail (e.g. connection
        failure), blocking rules are not evaluated — the original error is returned.
        """
        provider = MagicMock(spec=BasicAuthenticationProvider)
        provider.patron_blocking_rules = [
            PatronBlockingRule(name="block-all", rule="True")
        ]
        provider._do_authenticate = MagicMock(return_value=(INVALID_CREDENTIALS, {}))

        result = BasicAuthenticationProvider.authenticate(provider, MagicMock(), {})

        assert result is INVALID_CREDENTIALS

    def test_false_rule_does_not_block(self) -> None:
        """A False rule does not block the patron."""
        mock_patron = MagicMock(spec=Patron)
        mock_log = MagicMock()

        provider = _ConcreteBlockingProvider(
            0, 0, BasicAuthProviderSettings(), BasicAuthProviderLibrarySettings()
        )
        provider.patron_blocking_rules = [
            PatronBlockingRule(name="never-block", rule="False")
        ]

        with (
            patch.object(
                _ConcreteBlockingProvider,
                "log",
                new_callable=PropertyMock,
                return_value=mock_log,
            ),
            patch.object(
                _ConcreteBlockingProvider,
                "_do_authenticate",
                return_value=(mock_patron, {}),
            ),
        ):
            result = provider.authenticate(MagicMock(), {})

        assert result is mock_patron
        mock_log.info.assert_any_call("Patron blocking rules evaluation attempted")

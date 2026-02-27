"""Tests for the shared patron blocking-rules infrastructure.

Covers:
- PatronBlockingRule model (patron_blocking.py)
- check_patron_blocking_rules() pure function (patron_blocking.py)
- patron_blocking_rules field on BasicAuthProviderLibrarySettings (basic.py)
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
    PatronBlockingRule,
    check_patron_blocking_rules,
)
from palace.manager.integration.patron_auth.sip2.provider import (
    SIP2AuthenticationProvider,
)
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.problem_detail import ProblemDetail
from tests.fixtures.problem_detail import raises_problem_detail


class TestPatronBlockingRule:
    """Unit tests for the PatronBlockingRule value object."""

    def test_basic_construction(self) -> None:
        rule = PatronBlockingRule(name="rule1", rule="BLOCK")
        assert rule.name == "rule1"
        assert rule.rule == "BLOCK"
        assert rule.message is None

    def test_with_message(self) -> None:
        rule = PatronBlockingRule(
            name="rule1", rule="BLOCK", message="You are blocked."
        )
        assert rule.message == "You are blocked."

    def test_strips_whitespace(self) -> None:
        rule = PatronBlockingRule(name="  rule1  ", rule="  BLOCK  ")
        assert rule.name == "rule1"
        assert rule.rule == "BLOCK"

    def test_frozen(self) -> None:
        rule = PatronBlockingRule(name="rule1", rule="BLOCK")
        with pytest.raises(Exception):
            rule.name = "other"  # type: ignore[misc]

    def test_round_trip_dict(self) -> None:
        rule = PatronBlockingRule(name="r", rule="BLOCK", message="msg")
        restored = PatronBlockingRule.model_validate(rule.model_dump())
        assert restored == rule


class TestCheckPatronBlockingRules:
    """Unit tests for the check_patron_blocking_rules() pure function."""

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


class TestBasicAuthLibrarySettingsBlockingRules:
    """Tests for patron_blocking_rules on BasicAuthProviderLibrarySettings.

    The field lives on the base class so every basic-auth protocol (SIP2,
    Millennium, SirsiDynix, …) inherits validation for free.
    """

    def test_default_is_empty_list(self) -> None:
        settings = BasicAuthProviderLibrarySettings()
        assert settings.patron_blocking_rules == []

    def test_round_trip_with_rules(self) -> None:
        settings = BasicAuthProviderLibrarySettings(
            patron_blocking_rules=[
                {"name": "block-all", "rule": "BLOCK", "message": "Sorry"},
                {"name": "no-op", "rule": "ALLOW"},
            ]
        )
        assert len(settings.patron_blocking_rules) == 2
        assert settings.patron_blocking_rules[0].name == "block-all"
        assert settings.patron_blocking_rules[0].rule == "BLOCK"
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
            patron_blocking_rules=[{"name": "r", "rule": "BLOCK"}]
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
                patron_blocking_rules=[{"name": "", "rule": "BLOCK"}]
            )
        assert info.value.detail is not None
        assert "index 0" in info.value.detail
        assert "'name' must not be empty" in info.value.detail

    def test_validate_whitespace_only_name_raises(self) -> None:
        # str_strip_whitespace=True on PatronBlockingRule strips "   " to ""
        with raises_problem_detail() as info:
            BasicAuthProviderLibrarySettings(
                patron_blocking_rules=[{"name": "   ", "rule": "BLOCK"}]
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
                    {"name": "same", "rule": "BLOCK"},
                    {"name": "same", "rule": "ALLOW"},
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
                    {"name": "a", "rule": "BLOCK"},
                    {"name": "b", "rule": "BLOCK"},
                    {"name": "a", "rule": "BLOCK"},
                ]
            )
        assert info.value.detail is not None
        assert "index 2" in info.value.detail


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
        """When supports_patron_blocking_rules is False, a BLOCK rule is ignored
        and the Patron object is returned unchanged."""
        mock_patron = MagicMock(spec=Patron)

        # Patch _do_authenticate so we don't need a real provider instance.
        with patch(self._PATCH_TARGET, return_value=mock_patron):
            # Temporarily override the flag to False on a concrete subclass proxy.
            with patch.object(
                BasicAuthenticationProvider,
                "supports_patron_blocking_rules",
                new=False,
            ):
                # Also give the provider a BLOCK rule — it should be ignored.
                with patch.object(
                    BasicAuthenticationProvider,
                    "patron_blocking_rules",
                    new=[PatronBlockingRule(name="block-all", rule="BLOCK")],
                    create=True,
                ):
                    provider = MagicMock(spec=BasicAuthenticationProvider)
                    provider.supports_patron_blocking_rules = False
                    provider.patron_blocking_rules = [
                        PatronBlockingRule(name="block-all", rule="BLOCK")
                    ]
                    provider._do_authenticate = MagicMock(return_value=mock_patron)
                    # Call the real authenticate logic by delegating directly.
                    result = BasicAuthenticationProvider.authenticate(
                        provider, MagicMock(), {}
                    )

        assert result is mock_patron

    def test_blocking_applied_when_flag_true(self) -> None:
        """When supports_patron_blocking_rules is True, a BLOCK rule intercepts
        the authenticated Patron and returns a 403 ProblemDetail."""
        mock_patron = MagicMock(spec=Patron)

        provider = MagicMock(spec=BasicAuthenticationProvider)
        provider.supports_patron_blocking_rules = True
        provider.patron_blocking_rules = [
            PatronBlockingRule(
                name="block-all", rule="BLOCK", message="Blocked by policy."
            )
        ]
        provider._do_authenticate = MagicMock(return_value=mock_patron)

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
            PatronBlockingRule(name="block-all", rule="BLOCK")
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
            PatronBlockingRule(name="block-all", rule="BLOCK")
        ]
        provider._do_authenticate = MagicMock(return_value=INVALID_CREDENTIALS)

        result = BasicAuthenticationProvider.authenticate(provider, MagicMock(), {})

        assert result is INVALID_CREDENTIALS

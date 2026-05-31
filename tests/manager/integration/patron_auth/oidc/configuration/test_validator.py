"""Tests for OIDC settings field validators."""

from __future__ import annotations

from contextlib import nullcontext

import pytest

from palace.manager.integration.patron_auth.oidc.configuration.model import (
    OIDC_INCORRECT_FILTER_EXPRESSION,
    OIDCAuthLibrarySettings,
    OIDCAuthSettings,
)
from palace.manager.util.problem_detail import ProblemDetailException


class TestOIDCSettingsValidator:
    @pytest.mark.parametrize(
        "filter_expression, expect_raises",
        [
            pytest.param(
                'claims["edu_person_entitlement"][0 == "eresources"',
                True,
                id="syntax-error",
            ),
            pytest.param(
                'claims["email"].endswith("@example.edu")',
                False,
                id="valid-endswith",
            ),
            pytest.param(
                '"staff" in claims.get("groups", [])',
                False,
                id="valid-in-expression",
            ),
            pytest.param(
                'claims["sub"] == "admin"',
                False,
                id="valid-equality",
            ),
            pytest.param(
                # Syntax check is parse-only; names used in the expression are
                # not verified — undefined names fail only at evaluation time.
                'undefined_var == "value"',
                False,
                id="valid-no-claims-reference",
            ),
        ],
    )
    def test_validate_filter_expression(
        self,
        filter_expression: str,
        expect_raises: bool,
    ) -> None:
        context_manager = (
            pytest.raises(ProblemDetailException) if expect_raises else nullcontext()
        )
        with context_manager as exc_info:
            OIDCAuthSettings(
                issuer_url="https://idp.example.com",
                client_id="test-client-id",
                client_secret="test-client-secret",
                filter_expression=filter_expression,
            )

        if expect_raises:
            assert (
                exc_info.value.problem_detail.uri
                == OIDC_INCORRECT_FILTER_EXPRESSION.uri
            )

    @pytest.mark.parametrize(
        "filter_expression, expect_raises",
        [
            pytest.param(None, False, id="none-passes"),
            pytest.param(
                'claims["email"].endswith("@example.edu")',
                False,
                id="valid-expression",
            ),
            pytest.param(
                'claims["edu_person_entitlement"][0 == "eresources"',
                True,
                id="syntax-error",
            ),
        ],
    )
    def test_validate_filter_expression_library_settings(
        self, filter_expression: str | None, expect_raises: bool
    ) -> None:
        context_manager = (
            pytest.raises(ProblemDetailException) if expect_raises else nullcontext()
        )
        with context_manager as exc_info:
            OIDCAuthLibrarySettings(filter_expression=filter_expression)

        if expect_raises:
            assert (
                exc_info.value.problem_detail.uri
                == OIDC_INCORRECT_FILTER_EXPRESSION.uri
            )

"""Tests for the site-wide SAML web catalog promotion defaults."""

import logging

import pytest

from palace.manager.integration.patron_auth.saml.configuration.promotion import (
    ACCEPT_RELAY_STATE_PROMOTE_LABEL_ENV_VAR,
    ALLOW_PROMOTION_ENV_VAR,
    SAMLPromotionConfiguration,
    resolve_promotion_setting,
)
from tests.fixtures.test_utils import MonkeyPatchEnvFixture


class TestSAMLPromotionConfiguration:
    @pytest.mark.parametrize(
        "env_var, attribute",
        [
            pytest.param(ALLOW_PROMOTION_ENV_VAR, "allow_promotion", id="promotion"),
            pytest.param(
                ACCEPT_RELAY_STATE_PROMOTE_LABEL_ENV_VAR,
                "accept_relay_state_promote_label",
                id="relay-state-label",
            ),
        ],
    )
    @pytest.mark.parametrize(
        "value, expected, logs_error",
        [
            pytest.param(None, None, False, id="unset"),
            pytest.param("true", True, False, id="true"),
            pytest.param("false", False, False, id="false"),
            pytest.param("maybe", None, True, id="unrecognized-is-ignored"),
        ],
    )
    def test_environment(
        self,
        monkeypatch_env: MonkeyPatchEnvFixture,
        caplog: pytest.LogCaptureFixture,
        env_var: str,
        attribute: str,
        value: str | None,
        expected: bool | None,
        logs_error: bool,
    ) -> None:
        monkeypatch_env(ALLOW_PROMOTION_ENV_VAR, None)
        monkeypatch_env(ACCEPT_RELAY_STATE_PROMOTE_LABEL_ENV_VAR, None)
        monkeypatch_env(env_var, value)
        caplog.set_level(logging.ERROR)

        configuration = SAMLPromotionConfiguration()

        assert getattr(configuration, attribute) is expected
        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        if logs_error:
            assert errors == [
                f"{env_var} has an unrecognized value 'maybe'. Expected 'true' or "
                "'false'. Integrations without their own setting will use the "
                "built-in default instead."
            ]
        else:
            assert errors == []

    @pytest.mark.parametrize(
        "setting, system_wide, built_in, expected",
        [
            pytest.param(False, True, True, False, id="setting-wins"),
            pytest.param(None, False, True, False, id="system-wide-default"),
            pytest.param(None, None, True, True, id="built-in-default"),
        ],
    )
    def test_resolve_promotion_setting(
        self,
        setting: bool | None,
        system_wide: bool | None,
        built_in: bool,
        expected: bool,
    ) -> None:
        assert resolve_promotion_setting(setting, system_wide, built_in) is expected

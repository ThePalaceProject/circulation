"""Tests for GlobalSettings Pydantic class."""

from __future__ import annotations

from palace.manager.integration.configuration.global_settings import (
    GLOBAL_SETTINGS_PROTOCOL,
    GlobalSettings,
)
from tests.fixtures.database import DatabaseTransactionFixture


class TestGlobalSettings:
    def test_defaults(self) -> None:
        settings = GlobalSettings()
        assert settings.country == "US"
        assert settings.state == "All"

    def test_explicit_values(self) -> None:
        settings = GlobalSettings(country="CA", state="Ontario")
        assert settings.country == "CA"
        assert settings.state == "Ontario"

    def test_roundtrip(self) -> None:
        settings = GlobalSettings(country="GB", state="England")
        dumped = settings.model_dump()
        reloaded = GlobalSettings.model_validate(dumped)
        assert reloaded.country == "GB"
        assert reloaded.state == "England"

    def test_configuration_form(self, db: DatabaseTransactionFixture) -> None:
        form = GlobalSettings.configuration_form(db.session)
        keys = {field["key"] for field in form}
        assert "country" in keys
        assert "state" in keys

    def test_protocol_constant(self) -> None:
        assert GLOBAL_SETTINGS_PROTOCOL == "global_settings"

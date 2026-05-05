"""Tests for geographic settings resolution."""

from __future__ import annotations

import pytest

from palace.manager.integration.base import integration_settings_update
from palace.manager.integration.configuration.global_settings import (
    ENV_DEFAULT_COUNTRY,
    ENV_DEFAULT_STATE,
    GLOBAL_SETTINGS_PROTOCOL,
    GlobalSettings,
)
from palace.manager.integration.goals import Goals
from palace.manager.service.analytics.geo import resolve_geo
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.util import create
from tests.fixtures.database import DatabaseTransactionFixture


def _make_global_integration(
    db: DatabaseTransactionFixture,
    country: str = "US",
    state: str = "All",
) -> IntegrationConfiguration:
    integration, _ = create(
        db.session,
        IntegrationConfiguration,
        goal=Goals.SITEWIDE_SETTINGS,
        protocol=GLOBAL_SETTINGS_PROTOCOL,
        name="Global Settings",
    )
    settings = GlobalSettings(country=country, state=state)
    integration_settings_update(GlobalSettings, integration, settings)
    return integration


class TestResolveGeo:
    def test_env_var_defaults(
        self,
        db: DatabaseTransactionFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Returns env-var values when no DB rows exist."""
        monkeypatch.setenv(ENV_DEFAULT_COUNTRY, "CA")
        monkeypatch.setenv(ENV_DEFAULT_STATE, "Ontario")
        library = db.default_library()
        country, state = resolve_geo(library, db.session)
        assert country == "CA"
        assert state == "Ontario"

    def test_hard_coded_fallbacks(
        self,
        db: DatabaseTransactionFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Falls back to 'US'/'All' when env vars are absent and no DB rows exist."""
        monkeypatch.delenv(ENV_DEFAULT_COUNTRY, raising=False)
        monkeypatch.delenv(ENV_DEFAULT_STATE, raising=False)
        library = db.default_library()
        country, state = resolve_geo(library, db.session)
        assert country == "US"
        assert state == "All"

    def test_global_settings_override_env(
        self,
        db: DatabaseTransactionFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Sitewide GlobalSettings take precedence over env vars."""
        monkeypatch.setenv(ENV_DEFAULT_COUNTRY, "MX")
        monkeypatch.setenv(ENV_DEFAULT_STATE, "Jalisco")
        _make_global_integration(db, country="CA", state="British Columbia")
        library = db.default_library()
        country, state = resolve_geo(library, db.session)
        assert country == "CA"
        assert state == "British Columbia"

    def test_library_settings_override_global(
        self,
        db: DatabaseTransactionFixture,
    ) -> None:
        """Library-level settings take precedence over sitewide GlobalSettings."""
        _make_global_integration(db, country="CA", state="Ontario")
        library = db.default_library()
        library.settings_dict = dict(library.settings_dict)
        library.settings_dict["country"] = "US"
        library.settings_dict["state"] = "New York"
        country, state = resolve_geo(library, db.session)
        assert country == "US"
        assert state == "New York"

    def test_library_partial_override(
        self,
        db: DatabaseTransactionFixture,
    ) -> None:
        """Library-level override of only country falls back to global for state."""
        _make_global_integration(db, country="CA", state="Ontario")
        library = db.default_library()
        library.settings_dict = dict(library.settings_dict)
        library.settings_dict["country"] = "US"
        # state not set on library — falls back to global "Ontario"
        country, state = resolve_geo(library, db.session)
        assert country == "US"
        assert state == "Ontario"

from contextlib import AbstractContextManager, nullcontext
from typing import Any

import pytest
from _pytest.monkeypatch import MonkeyPatch
from coverage.annotate import os
from pydantic.env_settings import SettingsSourceCallable

from core.config import CannotLoadConfiguration
from core.service.sitewide import SitewideConfiguration


class SitewideConfigurationFixture:
    def __init__(self, monkeypatch: MonkeyPatch):
        self.monkeypatch = monkeypatch

        # Make sure the environment is empty before we start
        for key in os.environ.keys():
            if key.startswith("PALACE_"):
                monkeypatch.delenv(key)

        # Patch the customise_sources method to make sure we only use the mock env
        monkeypatch.setattr(
            SitewideConfiguration.Config,
            "customise_sources",
            self.customize_sources,
            raising=False,
        )

        # Add the settings we need in order to be able to instantiate the configuration
        monkeypatch.setenv("PALACE_BASE_URL", "http://example.com")
        monkeypatch.setenv(
            "PALACE_SECRET_KEY", "a very long and complicated secret key"
        )

    def customize_sources(
        self,
        init_settings: SettingsSourceCallable,
        env_settings: SettingsSourceCallable,
        file_secret_settings: SettingsSourceCallable,
    ) -> tuple[SettingsSourceCallable, ...]:
        return (env_settings,)

    def set(self, key: str, value: str | None) -> None:
        if value is None:
            self.delete(key)
        else:
            self.monkeypatch.setenv(key, value)

    def delete(self, key: str) -> None:
        self.monkeypatch.delenv(key, raising=False)

    def get_context_manager(self, expected: Any) -> AbstractContextManager:
        if isinstance(expected, type) and issubclass(expected, Exception):
            return pytest.raises(expected)
        else:
            return nullcontext()


@pytest.fixture()
def sitewide_configuration_fixture(
    monkeypatch: MonkeyPatch,
) -> SitewideConfigurationFixture:
    return SitewideConfigurationFixture(monkeypatch)


class TestSitewideConfiguration:
    @pytest.mark.parametrize(
        "url, expected",
        [
            ("http://example.com", "http://example.com"),
            ("http://example.com/", "http://example.com"),
            ("http://example.com/foo/bar/", "http://example.com/foo/bar"),
            ("missing scheme", CannotLoadConfiguration),
            (None, None),
        ],
    )
    def test_base_url(
        self,
        sitewide_configuration_fixture: SitewideConfigurationFixture,
        url: str | None,
        expected: str | type[Exception],
    ):
        sitewide_configuration_fixture.set("PALACE_BASE_URL", url)

        context = sitewide_configuration_fixture.get_context_manager(expected)
        with context:
            config = SitewideConfiguration()
            assert config.base_url == expected

    @pytest.mark.parametrize(
        "secret_key, expected",
        [
            ("012345678901234567890123", "012345678901234567890123"),
            ("too short", CannotLoadConfiguration),
            (None, CannotLoadConfiguration),
        ],
    )
    def test_secret_key(
        self,
        sitewide_configuration_fixture: SitewideConfigurationFixture,
        secret_key: str | None,
        expected: str | type[Exception],
    ):
        sitewide_configuration_fixture.set("PALACE_SECRET_KEY", secret_key)

        context = sitewide_configuration_fixture.get_context_manager(expected)
        with context:
            config = SitewideConfiguration()
            assert config.secret_key == expected

    @pytest.mark.parametrize(
        "bearer_token_signing_secret, expected",
        [
            ("012345678901234567890123", "012345678901234567890123"),
            ("too short", CannotLoadConfiguration),
            (None, None),
        ],
    )
    def test_bearer_token_signing_secret(
        self,
        sitewide_configuration_fixture: SitewideConfigurationFixture,
        bearer_token_signing_secret: str | None,
        expected: str | type[Exception] | None,
    ):
        sitewide_configuration_fixture.set(
            "PALACE_BEARER_TOKEN_SIGNING_SECRET", bearer_token_signing_secret
        )

        context = sitewide_configuration_fixture.get_context_manager(expected)
        with context:
            config = SitewideConfiguration()
            assert config.bearer_token_signing_secret == expected

    @pytest.mark.parametrize(
        "patron_web_hostnames, expected",
        [
            (None, []),
            ("foo", CannotLoadConfiguration),
            ("*", "*"),
            ("http://test.com", ["http://test.com"]),
            ("http://test.com/path", CannotLoadConfiguration),
            ("http://x.com|http://y.com/", ["http://x.com", "http://y.com"]),
            ("http://x.com/ |   http://y.com ", ["http://x.com", "http://y.com"]),
            ("http://x.com/ |   http://y.com/path ", CannotLoadConfiguration),
        ],
    )
    def test_patron_web_hostnames(
        self,
        sitewide_configuration_fixture: SitewideConfigurationFixture,
        patron_web_hostnames: str | None,
        expected: str | type[Exception] | list[str],
    ):
        sitewide_configuration_fixture.set(
            "PALACE_PATRON_WEB_HOSTNAMES", patron_web_hostnames
        )

        context = sitewide_configuration_fixture.get_context_manager(expected)
        with context:
            config = SitewideConfiguration()
            assert config.patron_web_hostnames == expected

    @pytest.mark.parametrize(
        "authentication_document_cache_time, expected",
        [
            (None, 3600),
            ("12", 12),
            ("0", 0),
            ("foo", CannotLoadConfiguration),
            ("-12", CannotLoadConfiguration),
        ],
    )
    def test_authentication_document_cache_time(
        self,
        sitewide_configuration_fixture: SitewideConfigurationFixture,
        authentication_document_cache_time: str | None,
        expected: int | type[Exception],
    ):
        sitewide_configuration_fixture.set(
            "PALACE_AUTHENTICATION_DOCUMENT_CACHE_TIME",
            authentication_document_cache_time,
        )

        context = sitewide_configuration_fixture.get_context_manager(expected)
        with context:
            config = SitewideConfiguration()
            assert config.authentication_document_cache_time == expected

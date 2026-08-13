from __future__ import annotations

import json
from functools import partial

import pytest

from palace.manager.api.config import Configuration
from palace.manager.api.model.token import OAuthTokenResponse
from palace.manager.integration.license.overdrive.constants import OverdriveConstants
from palace.manager.integration.license.overdrive.requests import (
    ClientTokenCache,
    OverdriveAsyncRequests,
    OverdriveClientRequests,
    OverdrivePatronRequests,
)
from palace.manager.integration.license.overdrive.settings import OverdriveSettings
from tests.fixtures.http import MockAsyncClientFixture, MockHttpClientFixture

create_settings = partial(
    OverdriveSettings,
    external_account_id="library_id",
    overdrive_website_id="website_id",
    overdrive_client_key="client_key",
    overdrive_client_secret="client_secret",
    overdrive_server_nickname=OverdriveConstants.TESTING_SERVERS,
)
"""The settings every Overdrive request fixture is built from."""


class OverdriveClientRequestsFixture:
    """A OverdriveClientRequests built from settings alone, with no database."""

    def __init__(self, http_client: MockHttpClientFixture) -> None:
        self.client = http_client
        self.create_settings = create_settings
        self.settings = self.create_settings()
        self.requests = OverdriveClientRequests(self.settings)

    def seed_token(self, access_token: str = "token") -> None:
        """Put a token in the cache, so no request has to fetch one."""
        self.requests.token_cache.token = OAuthTokenResponse(
            access_token=access_token, token_type="Bearer", expires_in=3600
        )

    def queue_access_token_response(self, credential: str = "token") -> None:
        token = dict(access_token=credential, token_type="bearer", expires_in=3600)
        self.client.queue_response(200, content=json.dumps(token))


@pytest.fixture
def overdrive_client_requests(
    http_client: MockHttpClientFixture,
) -> OverdriveClientRequestsFixture:
    return OverdriveClientRequestsFixture(http_client)


class OverdriveAsyncRequestsFixture:
    """An OverdriveAsyncRequests built from settings alone, with no database."""

    def __init__(self, async_client: MockAsyncClientFixture) -> None:
        self.client = async_client
        self.settings = create_settings()
        self.token_cache = ClientTokenCache()
        self.requests = OverdriveAsyncRequests(
            self.settings, token_cache=self.token_cache
        )

    def seed_token(self, access_token: str = "token") -> None:
        """Put a token in the shared cache, so no request has to fetch one."""
        self.token_cache.token = OAuthTokenResponse(
            access_token=access_token, token_type="Bearer", expires_in=3600
        )

    @staticmethod
    def token_response(access_token: str = "token") -> str:
        return json.dumps(
            dict(access_token=access_token, token_type="bearer", expires_in=3600)
        )


@pytest.fixture
def overdrive_async_requests(
    async_http_client: MockAsyncClientFixture,
) -> OverdriveAsyncRequestsFixture:
    return OverdriveAsyncRequestsFixture(async_http_client)


class OverdrivePatronRequestsFixture:
    """A OverdrivePatronRequests built from settings alone, with no database."""

    def __init__(
        self, http_client: MockHttpClientFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        self.client = http_client
        monkeypatch.setenv(
            f"{Configuration.OD_PREFIX_TESTING_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_KEY_SUFFIX}",
            "TestingKey",
        )
        monkeypatch.setenv(
            f"{Configuration.OD_PREFIX_TESTING_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_SECRET_SUFFIX}",
            "TestingSecret",
        )
        self.create_settings = create_settings
        self.settings = self.create_settings()
        self.requests = OverdrivePatronRequests(self.settings)
        self.token = "patron token"

    def token_provider(self, *, force_refresh: bool = False) -> str:
        """A PatronTokenProvider that hands out (and rotates) an in-memory token."""
        if force_refresh:
            self.token = "new patron token"
        return self.token


@pytest.fixture
def overdrive_patron_requests(
    http_client: MockHttpClientFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> OverdrivePatronRequestsFixture:
    return OverdrivePatronRequestsFixture(http_client, monkeypatch)

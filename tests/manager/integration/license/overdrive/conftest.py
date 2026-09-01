from __future__ import annotations

import json
from functools import partial

import pytest

from palace.manager.api.config import Configuration
from palace.manager.api.model.token import OAuthTokenResponse
from palace.manager.integration.license.overdrive.constants import OverdriveConstants
from palace.manager.integration.license.overdrive.requests import (
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

    def queue_access_token_response(self, credential: str = "token") -> None:
        token = dict(access_token=credential, token_type="bearer", expires_in=3600)
        self.client.queue_response(200, content=json.dumps(token))


@pytest.fixture
def overdrive_client_requests(
    http_client: MockHttpClientFixture,
) -> OverdriveClientRequestsFixture:
    return OverdriveClientRequestsFixture(http_client)


class OverdriveAsyncRequestsFixture:
    """An OverdriveAsyncRequests and the client context it takes its token from."""

    def __init__(self, client: MockAsyncClientFixture) -> None:
        self.client = client
        self.settings = create_settings()
        self.client_requests = OverdriveClientRequests(self.settings)
        # The async client builds its Authorization header up front, so seed a
        # token rather than making every test queue a token response.
        self.client_requests._cached_token = OAuthTokenResponse(
            access_token="token", token_type="Bearer", expires_in=3600
        )
        self.requests = OverdriveAsyncRequests(self.settings, self.client_requests)


@pytest.fixture
def overdrive_async_requests(
    async_http_client: MockAsyncClientFixture,
    http_client: MockHttpClientFixture,
) -> OverdriveAsyncRequestsFixture:
    # http_client is requested for its patching side effect only. The
    # OverdriveClientRequests this fixture builds is real, so without it a
    # sync request would leave the test process.
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

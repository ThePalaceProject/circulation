from __future__ import annotations

import json
from functools import partial

import pytest

from palace.manager.integration.license.overdrive.constants import OverdriveConstants
from palace.manager.integration.license.overdrive.requests import (
    OverdriveClientRequests,
)
from palace.manager.integration.license.overdrive.settings import OverdriveSettings
from tests.fixtures.http import MockHttpClientFixture


class OverdriveClientRequestsFixture:
    """A OverdriveClientRequests built from settings alone, with no database."""

    def __init__(self, http_client: MockHttpClientFixture) -> None:
        self.client = http_client
        self.create_settings = partial(
            OverdriveSettings,
            external_account_id="library_id",
            overdrive_website_id="website_id",
            overdrive_client_key="client_key",
            overdrive_client_secret="client_secret",
            overdrive_server_nickname=OverdriveConstants.TESTING_SERVERS,
        )
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

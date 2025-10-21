from __future__ import annotations

import json
from functools import partial
from typing import Any

import pytest

from palace.manager.api.circulation.dispatcher import CirculationApiDispatcher
from palace.manager.api.config import Configuration
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.integration.license.overdrive.constants import OverdriveConstants
from palace.manager.integration.license.overdrive.settings import (
    OverdriveLibrarySettings,
    OverdriveSettings,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.patron import Patron
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OverdriveFilesFixture
from tests.fixtures.http import MockHttpClientFixture
from tests.mocks.overdrive import MockOverdriveAPI


class OverdriveAPIFixture:
    def __init__(
        self,
        db: DatabaseTransactionFixture,
        http_client: MockHttpClientFixture,
        data: OverdriveFilesFixture,
        monkeypatch: pytest.MonkeyPatch,
    ):
        self.db = db
        self.data = data
        self.library = db.default_library()
        self.collection = self.create_collection(self.library)
        monkeypatch.setenv(
            f"{Configuration.OD_PREFIX_TESTING_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_KEY_SUFFIX}",
            "TestingKey",
        )
        monkeypatch.setenv(
            f"{Configuration.OD_PREFIX_TESTING_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_SECRET_SUFFIX}",
            "TestingSecret",
        )
        self.mock_http = http_client
        self.api = MockOverdriveAPI(db.session, self.collection)
        self.circulation = CirculationApiDispatcher(
            db.session,
            self.library,
            {self.collection.id: self.api},
        )
        self.create_mock_api = partial(MockOverdriveAPI, db.session)

    def error_message(
        self, error_code: str, message: str | None = None, token: str | None = None
    ) -> str:
        """Create a JSON document that simulates the message served by
        Overdrive given a certain error condition.
        """
        message = message or self.db.fresh_str()
        token = token or self.db.fresh_str()
        data = dict(errorCode=error_code, message=message, token=token)
        return json.dumps(data)

    def sample_json(self, filename: str) -> tuple[bytes, dict[str, Any]]:
        data = self.data.sample_data(filename)
        return data, json.loads(data)

    def sync_patron_activity(self, patron: Patron):
        self.api.sync_patron_activity(patron, "dummy pin")
        return self.api.local_loans_and_holds(patron)

    def create_collection(
        self,
        library: Library,
        name: str = "Test Overdrive Collection",
        client_key: str = "a",
        client_secret: str = "b",
        library_id: str = "c",
        website_id: str = "d",
        ils_name: str = "e",
        overdrive_server_nickname: str = OverdriveConstants.TESTING_SERVERS,
    ) -> Collection:
        """Create a mock Overdrive collection for use in tests."""
        collection, _ = Collection.by_name_and_protocol(
            self.db.session, name=name, protocol=OverdriveAPI.label()
        )
        settings = OverdriveSettings(
            external_account_id=library_id,
            overdrive_website_id=website_id,
            overdrive_client_key=client_key,
            overdrive_client_secret=client_secret,
            overdrive_server_nickname=overdrive_server_nickname,
        )
        OverdriveAPI.settings_update(collection.integration_configuration, settings)
        if library not in collection.associated_libraries:
            collection.associated_libraries.append(library)
        library_settings = OverdriveLibrarySettings(
            ils_name=ils_name,
        )
        library_config = collection.integration_configuration.for_library(library.id)
        assert library_config is not None
        OverdriveAPI.library_settings_update(library_config, library_settings)
        return collection

    def queue_access_token_response(self, credential: str = "token") -> None:
        token = dict(access_token=credential, expires_in=3600)
        self.mock_http.queue_response(200, content=json.dumps(token))

    def queue_collection_token(self, token: str = "collection token") -> None:
        # Many tests immediately try to access the
        # collection token. This is a helper method to make it easy to
        # queue up the response.
        self.mock_http.queue_response(
            200, content=json.dumps(dict(collectionToken=token))
        )


@pytest.fixture(scope="function")
def overdrive_api_fixture(
    db: DatabaseTransactionFixture,
    http_client: MockHttpClientFixture,
    overdrive_files_fixture: OverdriveFilesFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> OverdriveAPIFixture:
    return OverdriveAPIFixture(
        db,
        http_client,
        overdrive_files_fixture,
        monkeypatch,
    )

from __future__ import annotations

import json
import os

import pytest

from palace.manager.api.circulation import CirculationAPI
from palace.manager.api.config import Configuration
from palace.manager.sqlalchemy.model.patron import Patron
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OverdriveFilesFixture
from tests.mocks.overdrive import MockOverdriveAPI


class OverdriveAPIFixture:
    def __init__(self, db: DatabaseTransactionFixture, data: OverdriveFilesFixture):
        self.db = db
        self.data = data
        library = db.default_library()
        self.collection = MockOverdriveAPI.mock_collection(
            db.session, db.default_library()
        )
        self.api = MockOverdriveAPI(db.session, self.collection)
        self.circulation = CirculationAPI(
            db.session,
            library,
            {self.collection.id: self.api},
        )
        os.environ[
            f"{Configuration.OD_PREFIX_TESTING_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_KEY_SUFFIX}"
        ] = "TestingKey"
        os.environ[
            f"{Configuration.OD_PREFIX_TESTING_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_SECRET_SUFFIX}"
        ] = "TestingSecret"

    def error_message(self, error_code, message=None, token=None):
        """Create a JSON document that simulates the message served by
        Overdrive given a certain error condition.
        """
        message = message or self.db.fresh_str()
        token = token or self.db.fresh_str()
        data = dict(errorCode=error_code, message=message, token=token)
        return json.dumps(data)

    def sample_json(self, filename):
        data = self.data.sample_data(filename)
        return data, json.loads(data)

    def sync_patron_activity(self, patron: Patron):
        self.api.sync_patron_activity(patron, "dummy pin")
        return self.api.local_loans_and_holds(patron)


@pytest.fixture(scope="function")
def overdrive_api_fixture(
    db: DatabaseTransactionFixture,
    overdrive_files_fixture: OverdriveFilesFixture,
) -> OverdriveAPIFixture:
    return OverdriveAPIFixture(db, overdrive_files_fixture)

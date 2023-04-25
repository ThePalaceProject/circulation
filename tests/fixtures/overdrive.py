import json
import os
from pathlib import Path

import pytest

from core.model import Collection
from tests.api.mockapi.overdrive import MockOverdriveCoreAPI
from tests.fixtures.database import DatabaseTransactionFixture


class OverdriveFixture:
    """A basic fixture for Overdrive tests."""

    transaction: DatabaseTransactionFixture
    collection: Collection
    _resource_path: str
    _base_path: str

    @classmethod
    def create(cls, transaction: DatabaseTransactionFixture) -> "OverdriveFixture":
        fix = OverdriveFixture()
        fix._base_path = str(Path(__file__).parent.parent)
        fix._resource_path = os.path.join(fix._base_path, "core", "files", "overdrive")
        fix.transaction = transaction
        fix.collection = MockOverdriveCoreAPI.mock_collection(transaction.session)
        return fix

    def sample_json(self, filename):
        path = os.path.join(self._resource_path, filename)
        data = open(path).read()
        return data, json.loads(data)


@pytest.fixture()
def overdrive_fixture(
    db,
) -> OverdriveFixture:
    """A basic fixture for Overdrive tests."""
    return OverdriveFixture.create(db)


class OverdriveWithAPIFixture:
    overdrive: OverdriveFixture
    api: MockOverdriveCoreAPI

    """Automatically create a MockOverdriveCoreAPI class during setup.

    We don't always do this because
    TestOverdriveBibliographicCoverageProvider needs to create a
    MockOverdriveCoreAPI during the test, and at the moment the second
    MockOverdriveCoreAPI request created in a test behaves differently
    from the first one.
    """

    @classmethod
    def create(
        cls, transaction: DatabaseTransactionFixture
    ) -> "OverdriveWithAPIFixture":
        fix = OverdriveWithAPIFixture()
        fix.overdrive = OverdriveFixture.create(transaction)
        fix.api = MockOverdriveCoreAPI(transaction.session, fix.overdrive.collection)
        return fix


@pytest.fixture()
def overdrive_with_api_fixture(
    db,
) -> OverdriveWithAPIFixture:
    """A fixture for Overdrive tests that includes a mocked API."""
    return OverdriveWithAPIFixture.create(db)

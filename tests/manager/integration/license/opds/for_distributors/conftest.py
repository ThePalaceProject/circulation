import pytest

from palace.manager.integration.license.opds.for_distributors.api import (
    OPDSForDistributorsAPI,
)
from palace.manager.integration.license.opds.for_distributors.settings import (
    OPDSForDistributorsSettings,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.library import Library
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OPDSForDistributorsFilesFixture
from tests.fixtures.http import MockHttpClientFixture
from tests.fixtures.work import WorkIdPolicyQueuePresentationRecalculationFixture


class OPDSForDistributorsAPIFixture:
    def __init__(
        self,
        db: DatabaseTransactionFixture,
        files: OPDSForDistributorsFilesFixture,
        http_client: MockHttpClientFixture,
        work_policy_recalc_fixture: WorkIdPolicyQueuePresentationRecalculationFixture,
    ):
        self.db = db
        self.collection = self.mock_collection(db.default_library())
        self.api = OPDSForDistributorsAPI(db.session, self.collection)
        self.files = files
        self.http_client = http_client
        self.work_policy_recalc_fixture = work_policy_recalc_fixture

    def mock_collection(
        self,
        library: Library | None = None,
        name: str = "Test OPDS For Distributors Collection",
    ) -> Collection:
        """Create a mock OPDS For Distributors collection to use in tests."""
        library = library or self.db.default_library()
        return self.db.collection(
            name,
            protocol=OPDSForDistributorsAPI,
            settings=OPDSForDistributorsSettings(
                username="a",
                password="b",
                data_source="data_source",
                external_account_id="http://opds",
            ),
            library=library,
        )


@pytest.fixture(scope="function")
def opds_dist_api_fixture(
    db: DatabaseTransactionFixture,
    opds_dist_files_fixture: OPDSForDistributorsFilesFixture,
    http_client: MockHttpClientFixture,
    work_policy_recalc_fixture: WorkIdPolicyQueuePresentationRecalculationFixture,
) -> OPDSForDistributorsAPIFixture:
    return OPDSForDistributorsAPIFixture(
        db, opds_dist_files_fixture, http_client, work_policy_recalc_fixture
    )

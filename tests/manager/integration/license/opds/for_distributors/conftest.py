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
from tests.fixtures.files import FilesFixture
from tests.fixtures.work import WorkIdPolicyQueuePresentationRecalculationFixture
from tests.mocks.opds_for_distributors import MockOPDSForDistributorsAPI


class OPDSForDistributorsFilesFixture(FilesFixture):
    """A fixture providing access to OPDSForDistributors files."""

    def __init__(self):
        super().__init__("opds_for_distributors")


@pytest.fixture()
def opds_dist_files_fixture() -> OPDSForDistributorsFilesFixture:
    """A fixture providing access to OPDSForDistributors files."""
    return OPDSForDistributorsFilesFixture()


class OPDSForDistributorsAPIFixture:
    def __init__(
        self,
        db: DatabaseTransactionFixture,
        files: OPDSForDistributorsFilesFixture,
        work_policy_recalc_fixture: WorkIdPolicyQueuePresentationRecalculationFixture,
    ):
        self.db = db
        self.collection = self.mock_collection(db.default_library())
        self.api = MockOPDSForDistributorsAPI(db.session, self.collection)
        self.files = files
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
    work_policy_recalc_fixture: WorkIdPolicyQueuePresentationRecalculationFixture,
) -> OPDSForDistributorsAPIFixture:
    return OPDSForDistributorsAPIFixture(
        db, opds_dist_files_fixture, work_policy_recalc_fixture
    )

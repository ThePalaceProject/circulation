import pytest

from tests.fixtures.files import APIFilesFixture


class OPDSForDistributorsFilesFixture(APIFilesFixture):
    """A fixture providing access to OPDSForDistributors files."""

    def __init__(self):
        super().__init__("opds_for_distributors")


@pytest.fixture()
def api_opds_dist_files_fixture() -> OPDSForDistributorsFilesFixture:
    """A fixture providing access to OPDSForDistributors files."""
    return OPDSForDistributorsFilesFixture()

import pytest

from tests.fixtures.files import FilesFixture


class OPDSForDistributorsFilesFixture(FilesFixture):
    """A fixture providing access to OPDSForDistributors files."""

    def __init__(self):
        super().__init__("opds_for_distributors")


@pytest.fixture()
def opds_dist_files_fixture() -> OPDSForDistributorsFilesFixture:
    """A fixture providing access to OPDSForDistributors files."""
    return OPDSForDistributorsFilesFixture()

import pytest

from tests.fixtures.files import APIFilesFixture


class OPDSAPIFilesFixture(APIFilesFixture):
    """A fixture providing access to OPDS files."""

    def __init__(self):
        super().__init__("opds")


@pytest.fixture()
def api_opds_files_fixture() -> OPDSAPIFilesFixture:
    """A fixture providing access to OPDS files."""
    return OPDSAPIFilesFixture()

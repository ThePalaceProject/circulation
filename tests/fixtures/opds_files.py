import pytest

from tests.fixtures.files import FilesFixture


class OPDSFilesFixture(FilesFixture):
    """A fixture providing access to OPDS files."""

    def __init__(self):
        super().__init__("opds")


@pytest.fixture()
def opds_files_fixture() -> OPDSFilesFixture:
    """A fixture providing access to OPDS files."""
    return OPDSFilesFixture()

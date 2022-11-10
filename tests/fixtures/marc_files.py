import pytest

from tests.fixtures.files import FilesFixture


class MARCFilesFixture(FilesFixture):
    """A fixture providing access to MARC files."""

    def __init__(self):
        super().__init__("marc")


@pytest.fixture()
def marc_files_fixture() -> MARCFilesFixture:
    """A fixture providing access to MARC files."""
    return MARCFilesFixture()

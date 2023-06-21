import pytest

from tests.fixtures.files import FilesFixture


class OPDS2FilesFixture(FilesFixture):
    """A fixture providing access to OPDS2 files."""

    def __init__(self):
        super().__init__("opds2")


@pytest.fixture()
def opds2_files_fixture() -> OPDS2FilesFixture:
    """A fixture providing access to OPDS2 files."""
    return OPDS2FilesFixture()

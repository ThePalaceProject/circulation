import pytest

from tests.fixtures.files import APIFilesFixture


class OdiloFilesFixture(APIFilesFixture):
    """A fixture providing access to Odilo files."""

    def __init__(self):
        super().__init__("odilo")


@pytest.fixture()
def api_odilo_files_fixture() -> OdiloFilesFixture:
    """A fixture providing access to Odilo files."""
    return OdiloFilesFixture()

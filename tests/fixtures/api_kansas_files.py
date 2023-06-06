import pytest

from tests.fixtures.files import APIFilesFixture


class KansasPatronFilesFixture(APIFilesFixture):
    """A fixture providing access to Kansas patron files."""

    def __init__(self):
        super().__init__("kansas_patron")


@pytest.fixture()
def api_kansas_files_fixture() -> KansasPatronFilesFixture:
    """A fixture providing access to Kansas patron files."""
    return KansasPatronFilesFixture()

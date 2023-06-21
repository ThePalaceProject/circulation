import pytest

from tests.fixtures.files import APIFilesFixture


class ODLAPIFilesFixture(APIFilesFixture):
    """A fixture providing access to ODL files."""

    def __init__(self):
        super().__init__("odl")


@pytest.fixture()
def api_odl_files_fixture() -> ODLAPIFilesFixture:
    """A fixture providing access to ODL files."""
    return ODLAPIFilesFixture()

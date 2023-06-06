import pytest

from tests.fixtures.files import APIFilesFixture


class ODL2APIFilesFixture(APIFilesFixture):
    """A fixture providing access to ODL2 files."""

    def __init__(self):
        super().__init__("odl2")


@pytest.fixture()
def api_odl2_files_fixture() -> ODL2APIFilesFixture:
    """A fixture providing access to ODL2 files."""
    return ODL2APIFilesFixture()

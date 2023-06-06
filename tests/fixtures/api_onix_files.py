import pytest

from tests.fixtures.files import APIFilesFixture


class ONIXFilesFixture(APIFilesFixture):
    """A fixture providing access to ONIX files."""

    def __init__(self):
        super().__init__("onix")


@pytest.fixture()
def api_onix_files_fixture() -> ONIXFilesFixture:
    """A fixture providing access to ONIX files."""
    return ONIXFilesFixture()

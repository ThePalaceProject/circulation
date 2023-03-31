import pytest

from tests.fixtures.files import APIFilesFixture


class OverdriveAPIFilesFixture(APIFilesFixture):
    """A fixture providing access to Overdrive files."""

    def __init__(self):
        super().__init__("overdrive")


@pytest.fixture()
def api_overdrive_files_fixture() -> OverdriveAPIFilesFixture:
    """A fixture providing access to Overdrive files."""
    return OverdriveAPIFilesFixture()

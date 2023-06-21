import pytest

from tests.fixtures.files import APIFilesFixture


class MilleniumFilesFixture(APIFilesFixture):
    """A fixture providing access to Millenium files."""

    def __init__(self):
        super().__init__("millenium_patron")


@pytest.fixture()
def api_millenium_patron_files_fixture() -> MilleniumFilesFixture:
    """A fixture providing access to Millenium files."""
    return MilleniumFilesFixture()

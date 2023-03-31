import pytest

from tests.fixtures.files import APIFilesFixture


class NYTFilesFixture(APIFilesFixture):
    """A fixture providing access to NYT files."""

    def __init__(self):
        super().__init__("nyt")


@pytest.fixture()
def api_nyt_files_fixture() -> NYTFilesFixture:
    """A fixture providing access to NYT files."""
    return NYTFilesFixture()

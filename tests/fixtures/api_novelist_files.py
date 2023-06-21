import pytest

from tests.fixtures.files import APIFilesFixture


class NoveListFilesFixture(APIFilesFixture):
    """A fixture providing access to NoveList files."""

    def __init__(self):
        super().__init__("novelist")


@pytest.fixture()
def api_novelist_files_fixture() -> NoveListFilesFixture:
    """A fixture providing access to NoveList files."""
    return NoveListFilesFixture()

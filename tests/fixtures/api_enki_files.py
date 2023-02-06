import pytest

from tests.fixtures.files import APIFilesFixture


class EnkiFilesFixture(APIFilesFixture):
    """A fixture providing access to Enki files."""

    def __init__(self):
        super().__init__("enki")


@pytest.fixture()
def api_enki_files_fixture() -> EnkiFilesFixture:
    """A fixture providing access to Enki files."""
    return EnkiFilesFixture()

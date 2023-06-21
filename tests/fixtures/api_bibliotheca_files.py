import pytest

from tests.fixtures.files import APIFilesFixture


class BibliothecaFilesFixture(APIFilesFixture):
    """A fixture providing access to Bibliotheca files."""

    def __init__(self):
        super().__init__("bibliotheca")

    @staticmethod
    def files() -> "BibliothecaFilesFixture":
        return BibliothecaFilesFixture()


@pytest.fixture()
def api_bibliotheca_files_fixture() -> BibliothecaFilesFixture:
    """A fixture providing access to Bibliotecha files."""
    return BibliothecaFilesFixture()

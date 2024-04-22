import pytest

from tests.fixtures.files import FilesFixture


class CustomListsFilesFixture(FilesFixture):
    def __init__(self):
        super().__init__("customlists")


@pytest.fixture
def customlists_files():
    return CustomListsFilesFixture()

import pytest

from tests.fixtures.files import APIFilesFixture


class FeedbooksFilesFixture(APIFilesFixture):
    """A fixture providing access to Feedbooks files."""

    def __init__(self):
        super().__init__("feedbooks")


@pytest.fixture()
def api_feedbooks_files_fixture() -> FeedbooksFilesFixture:
    """A fixture providing access to Feedbooks files."""
    return FeedbooksFilesFixture()

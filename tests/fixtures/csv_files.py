import pytest

from tests.fixtures.files import FilesFixture


class CSVFilesFixture(FilesFixture):
    """A fixture providing access to CSV files."""

    def __init__(self):
        super().__init__("csv")


@pytest.fixture()
def csv_files_fixture() -> CSVFilesFixture:
    """A fixture providing access to CSV files."""
    return CSVFilesFixture()

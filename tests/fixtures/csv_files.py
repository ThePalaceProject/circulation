import os
from pathlib import Path

import pytest


class CSVFilesFixture:
    """A fixture providing access to CSV files."""

    def __init__(self):
        self._base_path = Path(__file__).parent.parent
        self._resource_path = os.path.join(self._base_path, "core", "files", "csv")

    def sample_data(self, filename) -> bytes:
        with open(self.sample_path(filename), "rb") as fh:
            return fh.read()

    def sample_path(self, filename) -> str:
        return os.path.join(self._resource_path, filename)


@pytest.fixture()
def csv_files_fixture() -> CSVFilesFixture:
    """A fixture providing access to CSV files."""
    return CSVFilesFixture()

import os
from pathlib import Path

import pytest


class MARCFilesFixture:
    """A fixture providing access to MARC files."""

    def __init__(self):
        self._base_path = Path(__file__).parent.parent
        self._resource_path = os.path.join(self._base_path, "core", "files", "marc")

    def sample_data(self, filename) -> bytes:
        with open(os.path.join(self._resource_path, filename), "rb") as fh:
            return fh.read()


@pytest.fixture()
def marc_files_fixture() -> MARCFilesFixture:
    """A fixture providing access to MARC files."""
    return MARCFilesFixture()

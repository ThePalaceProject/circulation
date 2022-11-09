import os
from pathlib import Path

import pytest


class OPDSFilesFixture:
    """A fixture providing access to OPDS files."""

    def __init__(self):
        self._base_path = Path(__file__).parent.parent
        self._resource_path = os.path.join(self._base_path, "core", "files", "opds")

    def sample_data(self, filename) -> bytes:
        with open(self.sample_path(filename), "rb") as fh:
            return fh.read()

    def sample_text(self, filename) -> str:
        with open(self.sample_path(filename)) as fh:
            return fh.read()

    def sample_path(self, filename) -> str:
        return os.path.join(self._resource_path, filename)


@pytest.fixture()
def opds_files_fixture() -> OPDSFilesFixture:
    """A fixture providing access to OPDS files."""
    return OPDSFilesFixture()

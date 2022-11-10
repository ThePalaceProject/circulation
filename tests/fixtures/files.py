import os
from pathlib import Path


class FilesFixture:
    """A fixture providing access to test files."""

    def __init__(self, directory: str):
        self._base_path = Path(__file__).parent.parent
        self._resource_path = os.path.join(self._base_path, "core", "files", directory)

    def sample_data(self, filename) -> bytes:
        with open(self.sample_path(filename), "rb") as fh:
            return fh.read()

    def sample_text(self, filename) -> str:
        with open(self.sample_path(filename)) as fh:
            return fh.read()

    def sample_path(self, filename) -> str:
        return os.path.join(self._resource_path, filename)

from pathlib import Path

import pytest

from palace.manager.sqlalchemy.model.resource import Representation
from tests.fixtures.database import DatabaseTransactionFixture


class FilesFixture:
    """A fixture providing access to test files."""

    def __init__(self, directory: str):
        self._base_path = Path(__file__).parent.parent
        self.directory = self._base_path / "files" / directory

    def sample_data(self, filename) -> bytes:
        return self.sample_path(filename).read_bytes()

    def sample_text(self, filename) -> str:
        return self.sample_path(filename).read_text()

    def sample_path(self, filename) -> Path:
        return self.directory / filename

    def sample_path_str(self, filename) -> str:
        return str(self.sample_path(filename))


class BibliothecaFilesFixture(FilesFixture):
    """A fixture providing access to Bibliotheca files."""

    def __init__(self):
        super().__init__("bibliotheca")

    @staticmethod
    def files() -> "BibliothecaFilesFixture":
        return BibliothecaFilesFixture()


@pytest.fixture()
def bibliotheca_files_fixture() -> BibliothecaFilesFixture:
    """A fixture providing access to Bibliotecha files."""
    return BibliothecaFilesFixture()


class OPDSFilesFixture(FilesFixture):
    """A fixture providing access to OPDS files."""

    def __init__(self):
        super().__init__("opds")


@pytest.fixture()
def opds_files_fixture() -> OPDSFilesFixture:
    """A fixture providing access to OPDS files."""
    return OPDSFilesFixture()


class OPDS2FilesFixture(FilesFixture):
    """A fixture providing access to OPDS2 files."""

    def __init__(self):
        super().__init__("opds2")


@pytest.fixture()
def opds2_files_fixture() -> OPDS2FilesFixture:
    """A fixture providing access to OPDS2 files."""
    return OPDS2FilesFixture()


class OPDS2WithODLFilesFixture(FilesFixture):
    """A fixture providing access to OPDS2 + ODL files."""

    def __init__(self):
        super().__init__("odl")


@pytest.fixture()
def opds2_with_odl_files_fixture() -> OPDS2WithODLFilesFixture:
    """A fixture providing access to OPDS2 + ODL files."""
    return OPDS2WithODLFilesFixture()


class SampleCoversFixture(FilesFixture):
    """A fixture providing access to sample cover images."""

    def __init__(self, db: DatabaseTransactionFixture):
        super().__init__("covers")
        self.db = db

    def sample_cover_path(self, name: str) -> str:
        """The path to the sample cover with the given filename."""
        return str(self.sample_path(name))

    def sample_cover_representation(self, name: str) -> Representation:
        """A Representation of the sample cover with the given filename."""
        representation, _ = self.db.representation(
            media_type="image/png", content=self.sample_data(name)
        )
        return representation


@pytest.fixture()
def sample_covers_fixture(
    db: DatabaseTransactionFixture,
) -> SampleCoversFixture:
    return SampleCoversFixture(db)


class OverdriveFilesFixture(FilesFixture):
    """A fixture providing access to Overdrive files."""

    def __init__(self):
        super().__init__("overdrive")


@pytest.fixture()
def overdrive_files_fixture() -> OverdriveFilesFixture:
    """A fixture providing access to Overdrive files."""
    return OverdriveFilesFixture()

from pathlib import Path

import pytest

from tests.fixtures.database import DatabaseTransactionFixture


class SampleCoversFixture:
    """A fixture providing access to sample cover images."""

    transaction: DatabaseTransactionFixture

    def __init__(self, transaction: DatabaseTransactionFixture):
        self.transaction = transaction

    def sample_cover_path(self, name):
        """The path to the sample cover with the given filename."""
        base_path = Path(__file__).parent.parent.parent
        resource_path = base_path / "tests" / "core" / "files" / "covers"
        sample_cover_path = resource_path / name
        return str(sample_cover_path)

    def sample_cover_representation(self, name):
        """A Representation of the sample cover with the given filename."""
        sample_cover_path = self.sample_cover_path(name)
        return self.transaction.representation(
            media_type="image/png", content=open(sample_cover_path, "rb").read()
        )[0]


@pytest.fixture()
def sample_covers_fixture(
    db,
) -> SampleCoversFixture:
    return SampleCoversFixture(db)

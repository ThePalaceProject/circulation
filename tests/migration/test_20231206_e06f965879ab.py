from unittest.mock import MagicMock, call

import pytest
from _pytest.logging import LogCaptureFixture
from pytest_alembic import MigrationContext
from sqlalchemy import inspect
from sqlalchemy.engine import Connection, Engine

from core.service.container import container_instance
from core.service.storage.s3 import S3Service
from tests.migration.conftest import (
    CreateCoverageRecord,
    CreateIdentifier,
    CreateLane,
    CreateLibrary,
)


class CreateCachedMarcFile:
    def __call__(
        self,
        connection: Connection,
        url: str | None,
        library_id: int | None = None,
        lane_id: int | None = None,
    ) -> tuple[int, int]:
        if library_id is None:
            library_id = self.create_library(connection)

        if lane_id is None:
            lane_id = self.create_lane(connection, library_id)

        representation_id = self.representation(connection, url)

        row = connection.execute(
            "INSERT INTO cachedmarcfiles (representation_id, start_time, end_time, lane_id, library_id) "
            "VALUES (%s, %s, %s, %s, %s) returning id",
            (representation_id, "2021-01-01", "2021-01-02", library_id, lane_id),
        ).first()
        assert row is not None
        file_id = row.id

        return representation_id, file_id

    def representation(self, connection: Connection, url: str | None) -> int:
        row = connection.execute(
            "INSERT INTO representations (media_type, url) "
            "VALUES ('application/marc', %s) returning id",
            url,
        ).first()
        assert row is not None
        assert isinstance(row.id, int)
        return row.id

    def __init__(
        self,
        create_library: CreateLibrary,
        create_lane: CreateLane,
    ) -> None:
        self.create_library = create_library
        self.create_lane = create_lane


@pytest.fixture
def create_cachedmarcfile(
    create_library: CreateLibrary,
    create_lane: CreateLane,
    create_identifier: CreateIdentifier,
) -> CreateCachedMarcFile:
    return CreateCachedMarcFile(create_library, create_lane)


MIGRATION_ID = "e06f965879ab"


def test_migration_no_s3_integration(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
    create_cachedmarcfile: CreateCachedMarcFile,
    caplog: LogCaptureFixture,
) -> None:
    alembic_runner.migrate_down_to(MIGRATION_ID)
    alembic_runner.migrate_down_one()

    container = container_instance()
    with container.storage.public.override(None):
        # If there is no public s3 integration, and no cachedmarcfiles in the database, the migration should succeed
        alembic_runner.migrate_up_one()

    alembic_runner.migrate_down_one()
    # If there is no public s3 integration, but there are cachedmarcfiles in the database, the migration should fail
    with alembic_engine.connect() as connection:
        create_cachedmarcfile(connection, "http://s3.amazonaws.com/test-bucket/1.mrc")

    with pytest.raises(RuntimeError) as excinfo, container.storage.public.override(
        None
    ):
        alembic_runner.migrate_up_one()

    assert (
        "There are cachedmarcfiles in the database, but no public s3 storage configured!"
        in str(excinfo.value)
    )


def test_migration_bucket_url_different(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
    create_cachedmarcfile: CreateCachedMarcFile,
    caplog: LogCaptureFixture,
) -> None:
    alembic_runner.migrate_down_to(MIGRATION_ID)
    alembic_runner.migrate_down_one()

    container = container_instance()
    mock_storage = MagicMock(spec=S3Service)

    # If the generated URL doesn't match the original URL, the migration should fail
    mock_storage.bucket = "test-bucket"
    mock_storage.generate_url.return_value = (
        "http://s3.amazonaws.com/test-bucket/different-url.mrc"
    )

    with alembic_engine.connect() as connection:
        create_cachedmarcfile(connection, "http://s3.amazonaws.com/test-bucket/1.mrc")

    with pytest.raises(RuntimeError) as excinfo, container.storage.public.override(
        mock_storage
    ):
        alembic_runner.migrate_up_one()

    assert "URL mismatch" in str(excinfo.value)


def test_migration_success(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
    create_library: CreateLibrary,
    create_lane: CreateLane,
    caplog: LogCaptureFixture,
    create_cachedmarcfile: CreateCachedMarcFile,
    create_coverage_record: CreateCoverageRecord,
) -> None:
    alembic_runner.migrate_down_to(MIGRATION_ID)
    alembic_runner.migrate_down_one()

    with alembic_engine.connect() as connection:
        library_id = create_library(connection, "test-library")
        lane_id = create_lane(connection, library_id, "test-lane")

        url1 = "http://s3.amazonaws.com/test-bucket/1.mrc"
        create_cachedmarcfile(
            connection,
            library_id=library_id,
            lane_id=lane_id,
            url=url1,
        )
        url2 = "http://test-bucket.us-west-2.s3.amazonaws.com/2.mrc"
        create_cachedmarcfile(
            connection,
            library_id=library_id,
            lane_id=lane_id,
            url=url2,
        )
        create_cachedmarcfile(
            connection,
            library_id=library_id,
            lane_id=lane_id,
            url=None,
        )
        url3 = "https://test-bucket.s3.us-west-2.amazonaws.com/test-1/2023-02-17%2006%3A38%3A01.837167%2B00%3A00-2023-03-21%2005%3A41%3A28.262257%2B00%3A00/Fiction.mrc"
        create_cachedmarcfile(
            connection,
            library_id=library_id,
            lane_id=lane_id,
            url=url3,
        )
        unrelated_representation = create_cachedmarcfile.representation(
            connection, "http://s3.amazonaws.com/test-bucket/4.mrc"
        )

        create_coverage_record(connection, "generate-marc")
        unrelated_coverage_record = create_coverage_record(connection)

    mock_storage = MagicMock(spec=S3Service)
    mock_storage.bucket = "test-bucket"
    mock_storage.generate_url.side_effect = [url1, url2, url3]

    container = container_instance()
    with container.storage.public.override(mock_storage):
        alembic_runner.migrate_up_one()

    # We should have checked that the generated url is the same and deleted the files from s3
    assert mock_storage.generate_url.call_count == 3
    assert mock_storage.delete.call_count == 3
    assert mock_storage.delete.call_args_list == [
        call("1.mrc"),
        call("2.mrc"),
        call(
            "test-1/2023-02-17 06:38:01.837167+00:00-2023-03-21 05:41:28.262257+00:00/Fiction.mrc"
        ),
    ]

    # But the representations and coveragerecords should still be there
    with alembic_engine.connect() as connection:
        assert connection.execute("SELECT id FROM representations").rowcount == 5
        assert connection.execute("SELECT id FROM coveragerecords").rowcount == 2

    # The next migration takes care of those
    alembic_runner.migrate_up_one()

    with alembic_engine.connect() as connection:
        # The representation and coveragerecord that were not associated should still be there
        assert connection.execute("SELECT id FROM representations").fetchall() == [
            (unrelated_representation,)
        ]
        assert connection.execute("SELECT id FROM coveragerecords").fetchall() == [
            (unrelated_coverage_record,)
        ]

        # Cachedmarcfiles should be gone
        inspector = inspect(connection)
        assert inspector.has_table("cachedmarcfiles") is False

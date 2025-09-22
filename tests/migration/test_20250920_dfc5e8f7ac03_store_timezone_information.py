"""Test migration 20250920_dfc5e8f7ac03_store_timezone_information.

This migration adds timezone information to the patron_last_notified columns
in the holds and loans tables. We need to ensure that existing timestamps
(which are stored as UTC) are preserved correctly with UTC timezone info.
"""

from datetime import datetime, timezone

from pytest_alembic import MigrationContext
from sqlalchemy import text

from tests.migration.conftest import AlembicDatabaseFixture


def test_store_timezone_information_preserves_utc_timestamps(
    alembic_runner: MigrationContext,
    alembic_database: AlembicDatabaseFixture,
) -> None:
    """Test that the migration preserves existing UTC timestamps correctly."""
    # Migrate to the migration just before this one
    alembic_runner.migrate_down_to("1290ca457ee4")

    # Create test data with timestamps (these will be stored without timezone info)
    # We'll use a specific timestamp that we can verify later
    test_timestamp = datetime(2024, 1, 15, 10, 30, 45, 123456)

    # Create test records using the helper methods
    library_id = alembic_database.library(name="Test Library", short_name="TL")
    integration_id = alembic_database.integration(
        protocol="Test Protocol", goal="LICENSE_GOAL", name="Test Integration"
    )
    patron_id = alembic_database.patron(
        library_id=library_id,
        external_identifier="test_patron_1",
        authorization_identifier="auth_1",
    )
    data_source_id = alembic_database.data_source(name="Test Data Source")
    identifier_id = alembic_database.identifier(identifier="9781234567890")
    collection_id = alembic_database.collection(
        integration_configuration_id=integration_id
    )
    license_pool_id = alembic_database.license_pool(
        data_source_id=data_source_id,
        identifier_id=identifier_id,
        collection_id=collection_id,
    )

    with alembic_database._engine.begin() as connection:

        # Insert test loan with patron_last_notified timestamp
        connection.execute(
            text(
                """
                INSERT INTO loans (patron_id, license_pool_id, start, patron_last_notified)
                VALUES (:patron_id, :license_pool_id, :start, :patron_last_notified)
            """
            ),
            {
                "patron_id": patron_id,
                "license_pool_id": license_pool_id,
                "start": test_timestamp,
                "patron_last_notified": test_timestamp,
            },
        )

        # Insert test hold with patron_last_notified timestamp
        connection.execute(
            text(
                """
                INSERT INTO holds (patron_id, license_pool_id, start, patron_last_notified)
                VALUES (:patron_id, :license_pool_id, :start, :patron_last_notified)
            """
            ),
            {
                "patron_id": patron_id,
                "license_pool_id": license_pool_id,
                "start": test_timestamp,
                "patron_last_notified": test_timestamp,
            },
        )

        # Verify the timestamps are stored without timezone info before migration
        loan_before = connection.execute(
            text(
                """
                SELECT patron_last_notified,
                       pg_typeof(patron_last_notified) as column_type
                FROM loans
                WHERE patron_id = :patron_id
            """
            ),
            {"patron_id": patron_id},
        ).fetchone()

        hold_before = connection.execute(
            text(
                """
                SELECT patron_last_notified,
                       pg_typeof(patron_last_notified) as column_type
                FROM holds
                WHERE patron_id = :patron_id
            """
            ),
            {"patron_id": patron_id},
        ).fetchone()

        # The type should be "timestamp without time zone"
        assert loan_before is not None
        assert hold_before is not None
        assert "without time zone" in str(loan_before.column_type)
        assert "without time zone" in str(hold_before.column_type)
        assert loan_before.patron_last_notified == test_timestamp
        assert hold_before.patron_last_notified == test_timestamp

    # Apply the migration
    alembic_runner.migrate_up_to("dfc5e8f7ac03")

    # Verify the timestamps are now stored with timezone info and the values are preserved
    with alembic_database._engine.begin() as connection:
        loan_after = connection.execute(
            text(
                """
                SELECT patron_last_notified,
                       pg_typeof(patron_last_notified) as column_type
                FROM loans
                WHERE patron_id = :patron_id
            """
            ),
            {"patron_id": patron_id},
        ).fetchone()

        hold_after = connection.execute(
            text(
                """
                SELECT patron_last_notified,
                       pg_typeof(patron_last_notified) as column_type
                FROM holds
                WHERE patron_id = :patron_id
            """
            ),
            {"patron_id": patron_id},
        ).fetchone()

        # The type should now be "timestamp with time zone"
        assert loan_after is not None
        assert hold_after is not None
        assert "with time zone" in str(loan_after.column_type)
        assert "with time zone" in str(hold_after.column_type)

        # The timestamp values should be preserved as UTC
        assert loan_after.patron_last_notified == test_timestamp.replace(
            tzinfo=timezone.utc
        )
        assert hold_after.patron_last_notified == test_timestamp.replace(
            tzinfo=timezone.utc
        )

    # Test the downgrade
    alembic_runner.migrate_down_to("1290ca457ee4")

    # Verify the timestamps are back to without timezone but values are still preserved
    with alembic_database._engine.begin() as connection:
        loan_downgrade = connection.execute(
            text(
                """
                SELECT patron_last_notified,
                       pg_typeof(patron_last_notified) as column_type
                FROM loans
                WHERE patron_id = :patron_id
            """
            ),
            {"patron_id": patron_id},
        ).fetchone()

        hold_downgrade = connection.execute(
            text(
                """
                SELECT patron_last_notified,
                       pg_typeof(patron_last_notified) as column_type
                FROM holds
                WHERE patron_id = :patron_id
            """
            ),
            {"patron_id": patron_id},
        ).fetchone()

        # Should be back to "timestamp without time zone"
        assert loan_downgrade is not None
        assert hold_downgrade is not None
        assert "without time zone" in str(loan_downgrade.column_type)
        assert "without time zone" in str(hold_downgrade.column_type)

        # Values should still be the same
        assert loan_downgrade.patron_last_notified == test_timestamp
        assert hold_downgrade.patron_last_notified == test_timestamp

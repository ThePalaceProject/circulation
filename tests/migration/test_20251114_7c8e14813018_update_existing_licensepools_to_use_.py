from pytest_alembic import MigrationContext
from sqlalchemy import text
from sqlalchemy.engine import Engine

from tests.migration.conftest import AlembicDatabaseFixture


def test_licensepool_type_and_status_backfill(
    alembic_runner: MigrationContext,
    alembic_database: AlembicDatabaseFixture,
    alembic_engine: Engine,
) -> None:
    """Test that the migration correctly backfills type and status for existing licensepools."""
    # Navigate to the migration
    alembic_runner.migrate_down_to("7c8e14813018")
    alembic_runner.migrate_down_one()

    # Create test data sources (using raw SQL to avoid schema compatibility issues)
    with alembic_engine.begin() as conn:
        ds1 = conn.execute(
            text(
                "INSERT INTO datasources (name, offers_licenses) VALUES (:name, :offers_licenses) RETURNING id"
            ),
            {"name": "Test DataSource 1", "offers_licenses": True},
        ).scalar_one()
        ds2 = conn.execute(
            text(
                "INSERT INTO datasources (name, offers_licenses) VALUES (:name, :offers_licenses) RETURNING id"
            ),
            {"name": "Test DataSource 2", "offers_licenses": True},
        ).scalar_one()

    # Create identifiers for the licensepools
    id1 = alembic_database.identifier(identifier="test-id-1")
    id2 = alembic_database.identifier(identifier="test-id-2")
    id3 = alembic_database.identifier(identifier="test-id-3")
    id4 = alembic_database.identifier(identifier="test-id-4")
    id5 = alembic_database.identifier(identifier="test-id-5")
    id6 = alembic_database.identifier(identifier="test-id-6")
    id7 = alembic_database.identifier(identifier="test-id-7")

    # Create OPDS collections
    opds1_integration = alembic_database.integration(protocol="OPDS Import")
    opds1_collection = alembic_database.collection(opds1_integration)

    opds2_integration = alembic_database.integration(protocol="OPDS 2.0 Import")
    opds2_collection = alembic_database.collection(opds2_integration)

    opds_for_dist_integration = alembic_database.integration(
        protocol="OPDS for Distributors"
    )
    opds_for_dist_collection = alembic_database.collection(opds_for_dist_integration)

    opds2_odl_integration = alembic_database.integration(protocol="ODL 2.0")
    opds2_odl_collection = alembic_database.collection(opds2_odl_integration)

    # Create non-OPDS collection (e.g., Overdrive)
    overdrive_integration = alembic_database.integration(protocol="Overdrive")
    overdrive_collection = alembic_database.collection(overdrive_integration)

    # Case 1: AGGREGATED pool with available licenses (should be ACTIVE)
    pool1 = alembic_database.license_pool(
        ds1, id1, opds2_odl_collection, licenses_owned=5, licenses_available=3
    )

    # Case 2: AGGREGATED pool with no available licenses (should be EXHAUSTED)
    pool2 = alembic_database.license_pool(
        ds1, id2, opds2_odl_collection, licenses_owned=5, licenses_available=0
    )

    # Case 3: UNLIMITED pool with licenses_owned = -1 (should be ACTIVE)
    pool3 = alembic_database.license_pool(
        ds1, id3, opds1_collection, licenses_owned=-1, licenses_available=-1
    )

    # Case 4: UNLIMITED pool with licenses_owned = 0 in OPDS Import collection (should be REMOVED)
    pool4 = alembic_database.license_pool(
        ds1, id4, opds1_collection, licenses_owned=0, licenses_available=0
    )

    # Case 5: UNLIMITED pool with licenses_owned = 0 in OPDS 2.0 Import collection (should be REMOVED)
    pool5 = alembic_database.license_pool(
        ds2, id5, opds2_collection, licenses_owned=0, licenses_available=0
    )

    # Case 6: UNLIMITED pool with licenses_owned = 0 in OPDS for Distributors collection (should be REMOVED)
    pool6 = alembic_database.license_pool(
        ds2, id6, opds_for_dist_collection, licenses_owned=0, licenses_available=0
    )

    # Case 7: METERED pool with licenses_owned = 0 in non-OPDS collection (should stay METERED)
    pool7 = alembic_database.license_pool(
        ds2, id7, overdrive_collection, licenses_owned=0, licenses_available=0
    )

    # Create License records for the AGGREGATED pools
    with alembic_engine.begin() as conn:
        # Pool1: Create licenses with available status
        conn.execute(
            text(
                """
                INSERT INTO licenses (license_pool_id, identifier, status, checkouts_available)
                VALUES (:pool_id, :identifier, :status, :checkouts)
                """
            ),
            [
                {
                    "pool_id": pool1,
                    "identifier": "urn:uuid:license1-1",
                    "status": "available",
                    "checkouts": 1,
                },
                {
                    "pool_id": pool1,
                    "identifier": "urn:uuid:license1-2",
                    "status": "available",
                    "checkouts": 1,
                },
            ],
        )

        # Pool2: Create licenses with unavailable status
        conn.execute(
            text(
                """
                INSERT INTO licenses (license_pool_id, identifier, status, checkouts_available)
                VALUES (:pool_id, :identifier, :status, :checkouts)
                """
            ),
            [
                {
                    "pool_id": pool2,
                    "identifier": "urn:uuid:license2-1",
                    "status": "unavailable",
                    "checkouts": 0,
                },
                {
                    "pool_id": pool2,
                    "identifier": "urn:uuid:license2-2",
                    "status": "unavailable",
                    "checkouts": 0,
                },
            ],
        )

    # Run the migration
    alembic_runner.migrate_up_one()

    # Verify the results
    with alembic_engine.begin() as conn:
        # Case 1: AGGREGATED with available licenses → type=AGGREGATED, status=ACTIVE
        result = conn.execute(
            text("SELECT type, status FROM licensepools WHERE id = :id"),
            {"id": pool1},
        ).one()
        assert result.type == "aggregated"
        assert result.status == "active"

        # Case 2: AGGREGATED with no available licenses → type=AGGREGATED, status=EXHAUSTED
        result = conn.execute(
            text("SELECT type, status FROM licensepools WHERE id = :id"),
            {"id": pool2},
        ).one()
        assert result.type == "aggregated"
        assert result.status == "exhausted"

        # Case 3: UNLIMITED with licenses_owned=-1 → type=UNLIMITED, status=ACTIVE
        result = conn.execute(
            text("SELECT type, status FROM licensepools WHERE id = :id"),
            {"id": pool3},
        ).one()
        assert result.type == "unlimited"
        assert result.status == "active"

        # Case 4: UNLIMITED with licenses_owned=0 in OPDS Import → type=UNLIMITED, status=REMOVED
        result = conn.execute(
            text("SELECT type, status FROM licensepools WHERE id = :id"),
            {"id": pool4},
        ).one()
        assert result.type == "unlimited"
        assert result.status == "removed"

        # Case 5: UNLIMITED with licenses_owned=0 in OPDS 2.0 Import → type=UNLIMITED, status=REMOVED
        result = conn.execute(
            text("SELECT type, status FROM licensepools WHERE id = :id"),
            {"id": pool5},
        ).one()
        assert result.type == "unlimited"
        assert result.status == "removed"

        # Case 6: UNLIMITED with licenses_owned=0 in OPDS for Distributors → type=UNLIMITED, status=REMOVED
        result = conn.execute(
            text("SELECT type, status FROM licensepools WHERE id = :id"),
            {"id": pool6},
        ).one()
        assert result.type == "unlimited"
        assert result.status == "removed"

        # Case 7: METERED pool in non-OPDS collection should stay METERED with default ACTIVE
        result = conn.execute(
            text("SELECT type, status FROM licensepools WHERE id = :id"),
            {"id": pool7},
        ).one()
        assert result.type == "metered"
        assert result.status == "active"


def test_opds2_odl_with_licenses_not_swept_up(
    alembic_runner: MigrationContext,
    alembic_database: AlembicDatabaseFixture,
    alembic_engine: Engine,
) -> None:
    """Test that OPDS2+ODL pools with licenses_owned=0 but with License records get AGGREGATED type."""
    alembic_runner.migrate_down_to("7c8e14813018")
    alembic_runner.migrate_down_one()

    # Create test data (using raw SQL for data source to avoid schema compatibility issues)
    with alembic_engine.begin() as conn:
        ds = conn.execute(
            text(
                "INSERT INTO datasources (name, offers_licenses) VALUES (:name, :offers_licenses) RETURNING id"
            ),
            {"name": "Test DataSource", "offers_licenses": True},
        ).scalar_one()

    identifier_id = alembic_database.identifier(identifier="test-id-odl")

    # Create OPDS2+ODL collection
    opds2_odl_integration = alembic_database.integration(protocol="ODL 2.0")
    opds2_odl_collection = alembic_database.collection(opds2_odl_integration)

    # Create a pool with licenses_owned=0 but with License records (edge case)
    pool = alembic_database.license_pool(
        ds, identifier_id, opds2_odl_collection, licenses_owned=0, licenses_available=0
    )

    # Create License records
    with alembic_engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO licenses (license_pool_id, identifier, status, checkouts_available)
                VALUES (:pool_id, :identifier, :status, :checkouts)
                """
            ),
            {
                "pool_id": pool,
                "identifier": "urn:uuid:license-odl-1",
                "status": "unavailable",
                "checkouts": 0,
            },
        )

    # Run the migration
    alembic_runner.migrate_up_one()

    # Verify the pool is AGGREGATED, not UNLIMITED
    with alembic_engine.begin() as conn:
        result = conn.execute(
            text("SELECT type, status FROM licensepools WHERE id = :id"),
            {"id": pool},
        ).one()
        assert result.type == "aggregated"
        assert result.status == "exhausted"  # No available licenses

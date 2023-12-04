from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pytest_alembic import MigrationContext
    from sqlalchemy.engine import Engine

    from tests.migration.conftest import (
        CreateCollection,
        CreateEdition,
        CreateIdentifier,
        CreateIntegrationConfiguration,
        CreateLicensePool,
    )

MIGRATION_UID = "1c14468b74ce"


def test_migration(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
    create_collection: CreateCollection,
    create_integration_configuration: CreateIntegrationConfiguration,
    create_edition: CreateEdition,
    create_identifier: CreateIdentifier,
    create_license_pool: CreateLicensePool,
) -> None:
    alembic_runner.migrate_down_to(MIGRATION_UID)
    alembic_runner.migrate_down_one()

    with alembic_engine.connect() as connection:
        ic_id_incorrect_protocol = create_integration_configuration(
            connection,
            "configuration_badprotocol",
            "OPDS 1.0",
            "LICENSE_GOAL",
            {},
        )
        collection_id_incorrect_protocol = create_collection(
            connection,
            integration_configuration_id=ic_id_incorrect_protocol,
        )

        ic_id1 = create_integration_configuration(
            connection, "configuration1", "OPDS for Distributors", "LICENSE_GOAL", {}
        )
        collection_id = create_collection(
            connection, integration_configuration_id=ic_id1
        )

        identifier_id1 = create_identifier(connection, "identifier-1", "type")
        edition_id1 = create_edition(connection, "title", "Audio", identifier_id1)
        lp1_id = create_license_pool(
            connection,
            collection_id,
            identifier_id=identifier_id1,
            should_track_playtime=False,
        )

        # Should not update because of incorrect medium
        identifier_id2 = create_identifier(connection, "identifier-2", "type")
        edition_id2 = create_edition(connection, "title", "Book", identifier_id2)
        lp2_id = create_license_pool(
            connection,
            collection_id,
            identifier_id=identifier_id2,
            should_track_playtime=False,
        )

        # Should not update because of incorrect collection protocol
        lp3_id = create_license_pool(
            connection,
            collection_id_incorrect_protocol,
            identifier_id=identifier_id1,
            should_track_playtime=False,
        )

        # Should update this one as well
        identifier_id3 = create_identifier(connection, "identifier-3", "other-type")
        edition_id3 = create_edition(connection, "title-1", "Audio", identifier_id3)
        lp4_id = create_license_pool(
            connection,
            collection_id,
            identifier_id=identifier_id3,
            should_track_playtime=False,
        )

    alembic_runner.migrate_up_one()

    with alembic_engine.connect() as connection:
        should_track = connection.execute(
            "select should_track_playtime from licensepools order by id"
        ).all()
    assert should_track == [(True,), (False,), (False,), (True,)]

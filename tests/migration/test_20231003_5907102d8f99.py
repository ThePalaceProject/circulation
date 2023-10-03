import json

from pytest_alembic import MigrationContext
from sqlalchemy.engine import Engine

from tests.migration.conftest import CreateLibrary

MIGRATION_UID = "5907102d8f99"


def test_upgrade_downgrade(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
    create_library: CreateLibrary,
) -> None:
    alembic_runner.migrate_down_to(MIGRATION_UID)
    alembic_runner.migrate_down_one()

    with alembic_engine.connect() as connection:
        # Library with no order
        lib_id = create_library(connection)
        [lib_settings] = connection.execute(  # type: ignore[misc]
            "select settings_dict from libraries where id=%s", lib_id
        ).first()
        assert "facets_enabled_order" not in lib_settings

        # library with an order but no "last_update"
        lib_id2 = create_library(connection)
        [lib_settings2] = connection.execute(  # type: ignore[misc]
            "select settings_dict from libraries where id=%s", lib_id2
        ).first()
        lib_settings2["facets_enabled_order"] = ["author"]
        connection.execute(
            "update libraries set settings_dict=%s where id=%s",
            json.dumps(lib_settings2),
            lib_id2,
        )

        # library with an order and a "last_update"
        lib_id3 = create_library(connection)
        [lib_settings3] = connection.execute(  # type: ignore[misc]
            "select settings_dict from libraries where id=%s", lib_id3
        ).first()
        lib_settings3["facets_enabled_order"] = ["last_update"]
        connection.execute(
            "update libraries set settings_dict=%s where id=%s",
            json.dumps(lib_settings3),
            lib_id3,
        )

        # Run the migration
        alembic_runner.migrate_up_one()

        # Order was added with the last_update
        [new_settings] = connection.execute(  # type: ignore[misc]
            "select settings_dict from libraries where id=%s", lib_id
        ).first()
        assert "facets_enabled_order" in new_settings
        assert new_settings["facets_enabled_order"] == ["last_update"]

        # last_update was added to the existing order
        [new_settings2] = connection.execute(  # type: ignore[misc]
            "select settings_dict from libraries where id=%s", lib_id2
        ).first()
        assert "facets_enabled_order" in new_settings2
        assert new_settings2["facets_enabled_order"] == ["author", "last_update"]

        # last_update remains in the existing order
        [new_settings3] = connection.execute(  # type: ignore[misc]
            "select settings_dict from libraries where id=%s", lib_id3
        ).first()
        assert "facets_enabled_order" in new_settings3
        assert new_settings3["facets_enabled_order"] == ["last_update"]

        # Test the downgrade with the same data
        alembic_runner.migrate_down_one()
        # All libraries will have last_update removed
        rows = connection.execute("select settings_dict from libraries").all()

        # All 3 libraries were selected
        assert len(rows) == 3
        # None of them have the last_update facet
        for [settings] in rows:
            assert "facets_enabled_order" in settings
            assert "last_update" not in settings

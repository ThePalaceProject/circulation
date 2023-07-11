from __future__ import annotations

import json
from typing import TYPE_CHECKING

from pytest_alembic import MigrationContext
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

if TYPE_CHECKING:
    from tests.migration.conftest import CreateConfigSetting, CreateLibrary


def test_migration(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
    create_config_setting: CreateConfigSetting,
    create_library: CreateLibrary,
) -> None:
    alembic_runner.migrate_down_to("3d380776c1bf")

    # Test down migration
    assert inspect(alembic_engine).has_table("announcements")
    alembic_runner.migrate_down_one()
    assert not inspect(alembic_engine).has_table("announcements")

    a1 = {
        "content": "This is a test library announcement",
        "id": "13ab12b8-2e86-449d-b58d-7f3a944d4093",
        "start": "1990-07-01",
        "finish": "1990-07-31",
    }
    a2 = {
        "content": "This is another test library announcement",
        "id": "23e0ff93-42f6-4333-8d74-4b162237bd5c",
        "start": "2022-02-20",
        "finish": "2022-02-21",
    }
    a3 = {
        "content": "This is a test global announcement",
        "id": "171208b0-d9bc-433f-a957-444fd32e2993",
        "start": "2025-01-01",
        "finish": "2025-01-02",
    }

    # Test up migration
    with alembic_engine.connect() as connection:
        library = create_library(connection)

        # Create some library announcements
        create_config_setting(
            connection, "announcements", json.dumps([a1, a2]), library_id=library
        )

        # Create some global announcements
        create_config_setting(connection, "global_announcements", json.dumps([a3]))

    # Run the migration
    alembic_runner.migrate_up_one()

    # Make sure settings are migrated into table correctly
    with alembic_engine.connect() as connection:
        announcements = connection.execute(
            "SELECT * FROM announcements order by start"
        ).all()
        assert len(announcements) == 3
        for actual, expected in zip(announcements, [a1, a2, a3]):
            assert str(actual.id) == expected["id"]
            assert actual.content == expected["content"]
            assert str(actual.start) == expected["start"]
            assert str(actual.finish) == expected["finish"]

        assert announcements[0].library_id == library
        assert announcements[1].library_id == library
        assert announcements[2].library_id is None

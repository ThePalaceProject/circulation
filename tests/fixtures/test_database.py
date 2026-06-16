"""Tests for the database test-fixture machinery in ``tests/fixtures/database.py`` and the
external-schema collection hook in ``tests/conftest.py``.

These cover the ``external_schema`` seam used by the backwards-compatibility CI check, which
runs a previous release's test suite against a schema built by the current code.
"""

from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.engine import Engine

from palace.util.exceptions import BasePalaceException

from palace.manager.sqlalchemy.session import SessionManager
from tests.conftest import pytest_collection_modifyitems
from tests.fixtures.database import (
    DatabaseCreationFixture,
    DatabaseFixture,
    DatabaseTestConfiguration,
    IdFixture,
)


def _config(
    *,
    url: str = "postgresql://palace:test@localhost:5432/mydb",
    create_database: bool = True,
    external_schema: bool = False,
) -> DatabaseTestConfiguration:
    return DatabaseTestConfiguration(
        url=url, create_database=create_database, external_schema=external_schema
    )


class TestDatabaseTestConfiguration:
    def test_external_schema_defaults_false(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("PALACE_TEST_DATABASE_EXTERNAL_SCHEMA", raising=False)
        assert DatabaseTestConfiguration.from_env().external_schema is False

    def test_external_schema_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PALACE_TEST_DATABASE_EXTERNAL_SCHEMA", "true")
        assert DatabaseTestConfiguration.from_env().external_schema is True


class TestDatabaseCreationFixture:
    def test_external_schema_disables_db_creation(self) -> None:
        with patch.object(
            DatabaseTestConfiguration,
            "from_env",
            return_value=_config(external_schema=True),
        ):
            fixture = DatabaseCreationFixture(IdFixture("master", "function"))
        assert fixture.external_schema is True
        # External schema implies the database is used as-is (no create/drop) ...
        assert fixture.create_database is False
        # ... and the database name comes from the configured URL, not a generated id.
        assert fixture.database_name == "mydb"

    def test_external_schema_requires_serial(self) -> None:
        with patch.object(
            DatabaseTestConfiguration,
            "from_env",
            return_value=_config(external_schema=True),
        ):
            with pytest.raises(BasePalaceException, match="parallel"):
                DatabaseCreationFixture(IdFixture("gw0", "function"))

    def test_normal_mode_uses_generated_database_name(self) -> None:
        with patch.object(
            DatabaseTestConfiguration, "from_env", return_value=_config()
        ):
            fixture = DatabaseCreationFixture(IdFixture("master", "function"))
        assert fixture.external_schema is False
        assert fixture.create_database is True
        assert fixture.database_name == fixture.test_id.id


class TestDatabaseFixture:
    @staticmethod
    def _creation(external_schema: bool) -> MagicMock:
        creation = MagicMock(spec=DatabaseCreationFixture)
        creation.external_schema = external_schema
        creation.url = "postgresql://palace:test@localhost:5432/mydb"
        return creation

    def test_external_schema_leaves_schema_untouched(self) -> None:
        with (
            patch.object(SessionManager, "engine", return_value=MagicMock(spec=Engine)),
            patch.object(DatabaseFixture, "drop_existing_schema") as drop,
            patch.object(DatabaseFixture, "_initialize_database") as initialize,
            patch.object(DatabaseFixture, "_load_model_classes") as load,
        ):
            with DatabaseFixture.fixture(self._creation(external_schema=True)):
                pass
        drop.assert_not_called()
        initialize.assert_not_called()
        # Models are still registered with the ORM even when the schema is external.
        load.assert_called_once()

    def test_normal_mode_builds_schema(self) -> None:
        with (
            patch.object(SessionManager, "engine", return_value=MagicMock(spec=Engine)),
            patch.object(DatabaseFixture, "drop_existing_schema") as drop,
            patch.object(DatabaseFixture, "_initialize_database") as initialize,
            patch.object(DatabaseFixture, "_load_model_classes") as load,
        ):
            with DatabaseFixture.fixture(self._creation(external_schema=False)):
                pass
        drop.assert_called_once()
        initialize.assert_called_once()
        load.assert_called_once()


class TestPytestCollectionModifyItems:
    @staticmethod
    def _item(fixturenames: list[str]) -> MagicMock:
        item = MagicMock()
        item.fixturenames = fixturenames
        return item

    def test_marks_tests_using_db_fixture(self) -> None:
        db_item = self._item(["db", "tmp_path"])
        other_item = self._item(["tmp_path"])

        pytest_collection_modifyitems(
            MagicMock(), cast("list[pytest.Item]", [db_item, other_item])
        )

        # Tests using the db fixture get the db marker; others are left untouched.
        db_item.add_marker.assert_called_once_with("db")
        other_item.add_marker.assert_not_called()

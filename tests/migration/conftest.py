from __future__ import annotations

import random
import string
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import pytest_alembic
from pytest_alembic.config import Config
from sqlalchemy import text

from palace.manager.sqlalchemy.session import json_serializer
from tests.fixtures.database import DatabaseFixture
from tests.fixtures.services import ServicesFixture

if TYPE_CHECKING:
    import alembic.config
    from pytest_alembic import MigrationContext
    from sqlalchemy.engine import Engine, Row


@pytest.fixture
def alembic_config_path() -> Path:
    return Path(__file__).parent.parent.parent.absolute() / "alembic.ini"


@pytest.fixture
def alembic_config(alembic_config_path: Path) -> Config:
    """
    Use an explicit path to the alembic config file. This lets us run pytest
    from a different directory than the root of the project.
    """
    return Config(config_options={"file": str(alembic_config_path)})


@pytest.fixture
def alembic_engine(function_database: DatabaseFixture) -> Engine:
    """
    Override this fixture to provide pytest-alembic powered tests with a database handle.
    """
    return function_database.engine


@pytest.fixture
def alembic_runner(
    alembic_config: dict[str, Any] | alembic.config.Config | Config,
    alembic_engine: Engine,
    services_fixture: ServicesFixture,
) -> Generator[MigrationContext]:
    """
    Override this fixture to make sure that we stamp head. Since this is how out database
    is initialized. The normal fixtures assume you start from an empty database.

    This fixture also includes the services_fixture fixture which is used to mock out
    the services container. This is done because some of the migrations require the services
    container to be initialized.
    """
    config = Config.from_raw_config(alembic_config)
    with pytest_alembic.runner(config=config, engine=alembic_engine) as runner:
        runner.command_executor.stamp("head")
        yield runner


class AlembicDatabaseFixture:
    def __init__(self, alembic_engine: Engine) -> None:
        self._engine = alembic_engine

    @staticmethod
    def random_name(length: int | None = None) -> str:
        if length is None:
            length = 10
        return "".join(random.choices(string.ascii_lowercase, k=length))

    def fetch_library(self, library_id: int) -> Row:
        with self._engine.connect() as connection:
            result = connection.execute(
                text("SELECT * FROM libraries WHERE id = :id"),
                id=library_id,
            )
            return result.one()

    def library(
        self,
        name: str | None = None,
        short_name: str | None = None,
        settings: dict[str, Any] | None = None,
    ) -> int:
        if name is None:
            name = self.random_name()
        if short_name is None:
            short_name = self.random_name()

        args = {
            "name": name,
            "short_name": short_name,
            "public_key": self.random_name(),
            "private_key": self.random_name(),
            "is_default": "false",
        }

        default_settings = {
            "website": "http://library.com",
            "help_web": "http://library.com/support",
        }

        if settings is not None:
            default_settings.update(settings)

        args["settings_dict"] = json_serializer(default_settings)

        keys = ",".join(args.keys())
        values = ",".join([f"'{value}'" for value in args.values()])

        with self._engine.connect() as connection:
            library = connection.execute(
                f"INSERT INTO libraries ({keys}) VALUES ({values}) returning id"
            ).fetchone()

        assert library is not None
        assert isinstance(library.id, int)
        return library.id


@pytest.fixture
def alembic_database(
    alembic_engine: Engine,
) -> AlembicDatabaseFixture:
    """
    Fixture to create database records for alembic tests.

    By its nature, this fixture will not be stable since Alembic deals
    with modifications to the database schema. This means the fixture
    needs to accommodate creating and modifying records across different
    schema versions.

    It's still helpful to have a central place to create records, even if it
    requires occasional updates to handle schema changes.
    """
    return AlembicDatabaseFixture(alembic_engine=alembic_engine)

from __future__ import annotations

import random
import string
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

import pytest
import pytest_alembic
from pytest_alembic.config import Config

from palace.manager.sqlalchemy.session import json_serializer
from tests.fixtures.database import DatabaseFixture
from tests.fixtures.services import ServicesFixture

if TYPE_CHECKING:
    import alembic.config
    from pytest_alembic import MigrationContext
    from sqlalchemy.engine import Connection, Engine


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
) -> Generator[MigrationContext, None, None]:
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


class RandomName(Protocol):
    def __call__(self, length: int | None = None) -> str:
        ...


@pytest.fixture
def random_name() -> RandomName:
    def fixture(length: int | None = None) -> str:
        if length is None:
            length = 10
        return "".join(random.choices(string.ascii_lowercase, k=length))

    return fixture


class CreateLibrary(Protocol):
    def __call__(
        self,
        connection: Connection,
        name: str | None = None,
        short_name: str | None = None,
    ) -> int:
        ...


@pytest.fixture
def create_library(random_name: RandomName) -> CreateLibrary:
    def fixture(
        connection: Connection,
        name: str | None = None,
        short_name: str | None = None,
    ) -> int:
        if name is None:
            name = random_name()
        if short_name is None:
            short_name = random_name()

        args = {
            "name": name,
            "short_name": short_name,
        }

        args["public_key"] = random_name()
        args["private_key"] = random_name()

        settings_dict = {
            "website": "http://library.com",
            "help_web": "http://library.com/support",
        }
        args["settings_dict"] = json_serializer(settings_dict)

        keys = ",".join(args.keys())
        values = ",".join([f"'{value}'" for value in args.values()])
        library = connection.execute(
            f"INSERT INTO libraries ({keys}) VALUES ({values}) returning id"
        ).fetchone()

        assert library is not None
        assert isinstance(library.id, int)
        return library.id

    return fixture


class CreateCollection(Protocol):
    def __call__(
        self,
        connection: Connection,
        integration_configuration_id: int | None = None,
    ) -> int:
        ...


@pytest.fixture
def create_collection(random_name: RandomName) -> CreateCollection:
    def fixture(
        connection: Connection,
        integration_configuration_id: int | None = None,
    ) -> int:
        collection = connection.execute(
            "INSERT INTO collections (integration_configuration_id) VALUES (%s) returning id",
            integration_configuration_id,
        ).fetchone()
        assert collection is not None
        assert isinstance(collection.id, int)
        return collection.id

    return fixture

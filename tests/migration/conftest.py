from __future__ import annotations

import random
import string
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Generator, Optional, Protocol, Union

import pytest
import pytest_alembic
from pytest_alembic.config import Config

from core.model import SessionManager
from tests.fixtures.database import ApplicationFixture, DatabaseFixture

if TYPE_CHECKING:
    from pytest_alembic import MigrationContext
    from sqlalchemy.engine import Connection, Engine

    import alembic.config


@pytest.fixture(scope="function")
def database() -> Generator[DatabaseFixture, None, None]:
    # This is very similar to the normal database fixture and uses the same object,
    # but because these tests are done outside a transaction, we need this fixture
    # to have function scope, so the database schema is completely reset between
    # tests.
    app = ApplicationFixture.create()
    db = DatabaseFixture.create()
    yield db
    db.close()
    app.close()
    SessionManager.engine_for_url = {}


@pytest.fixture
def alembic_config() -> Config:
    """
    Use an explicit path to the alembic config file. This lets us run pytest
    from a different directory than the root of the project.
    """
    return Config(
        config_options={
            "file": str(Path(__file__).parent.parent.parent.absolute() / "alembic.ini")
        }
    )


@pytest.fixture
def alembic_engine(database: DatabaseFixture) -> Engine:
    """
    Override this fixture to provide pytest-alembic powered tests with a database handle.
    """
    return database._engine


@pytest.fixture
def alembic_runner(
    alembic_config: Union[Dict[str, Any], alembic.config.Config, Config],
    alembic_engine: Engine,
) -> Generator[MigrationContext, None, None]:
    """
    Override this fixture to make sure that we stamp head. Since this is how out database
    is initialized. The normal fixtures assume you start from an empty database.
    """
    config = Config.from_raw_config(alembic_config)
    with pytest_alembic.runner(config=config, engine=alembic_engine) as runner:
        runner.command_executor.stamp("head")
        yield runner


class RandomName(Protocol):
    def __call__(self, length: Optional[int] = None) -> str:
        ...


@pytest.fixture
def random_name() -> RandomName:
    def fixture(length: Optional[int] = None) -> str:
        if length is None:
            length = 10
        return "".join(random.choices(string.ascii_lowercase, k=length))

    return fixture


class CreateLibrary(Protocol):
    def __call__(
        self,
        connection: Connection,
        name: Optional[str] = None,
        short_name: Optional[str] = None,
    ) -> int:
        ...


@pytest.fixture
def create_library(random_name: RandomName) -> CreateLibrary:
    def fixture(
        connection: Connection,
        name: Optional[str] = None,
        short_name: Optional[str] = None,
    ) -> int:
        if name is None:
            name = random_name()
        if short_name is None:
            short_name = random_name()
        library = connection.execute(
            f"INSERT INTO libraries (name, short_name) VALUES ('{name}', '{short_name}') returning id"
        ).fetchone()
        assert library is not None
        assert isinstance(library.id, int)
        return library.id

    return fixture


class CreateExternalIntegration(Protocol):
    def __call__(
        self,
        connection: Connection,
        protocol: Optional[str] = None,
        goal: Optional[str] = None,
        name: Optional[str] = None,
    ) -> int:
        ...


@pytest.fixture
def create_external_integration(random_name: RandomName) -> CreateExternalIntegration:
    def fixture(
        connection: Connection,
        protocol: Optional[str] = None,
        goal: Optional[str] = None,
        name: Optional[str] = None,
    ) -> int:
        protocol = protocol or random_name()
        goal = goal or random_name()
        name = name or random_name()
        integration = connection.execute(
            f"INSERT INTO externalintegrations (protocol, goal, name) VALUES ('{protocol}', '{goal}', '{name}') returning id"
        ).fetchone()
        assert integration is not None
        assert isinstance(integration.id, int)
        return integration.id

    return fixture


class CreateConfigSetting(Protocol):
    def __call__(
        self,
        connection: Connection,
        key: Optional[str] = None,
        value: Optional[str] = None,
        integration_id: Optional[int] = None,
        library_id: Optional[int] = None,
    ) -> int:
        ...


@pytest.fixture
def create_config_setting() -> CreateConfigSetting:
    def fixture(
        connection: Connection,
        key: Optional[str] = None,
        value: Optional[str] = None,
        integration_id: Optional[int] = None,
        library_id: Optional[int] = None,
    ) -> int:
        setting = connection.execute(
            "INSERT INTO configurationsettings (key, value, external_integration_id, library_id) VALUES (%s, %s, %s, %s) returning id",
            (key, value, integration_id, library_id),
        ).fetchone()
        assert setting is not None
        assert isinstance(setting.id, int)
        return setting.id

    return fixture

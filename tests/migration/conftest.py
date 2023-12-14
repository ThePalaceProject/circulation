from __future__ import annotations

import json
import random
import string
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol, cast

import pytest
import pytest_alembic
from pytest_alembic.config import Config

from core.model import json_serializer
from tests.fixtures.database import ApplicationFixture, DatabaseFixture

if TYPE_CHECKING:
    from pytest_alembic import MigrationContext
    from sqlalchemy.engine import Connection, Engine

    import alembic.config


@pytest.fixture(scope="function")
def application() -> Generator[ApplicationFixture, None, None]:
    app = ApplicationFixture.create()
    yield app
    app.close()


@pytest.fixture(scope="function")
def database(application: ApplicationFixture) -> Generator[DatabaseFixture, None, None]:
    # This is very similar to the normal database fixture and uses the same object,
    # but because these tests are done outside a transaction, we need this fixture
    # to have function scope, so the database schema is completely reset between
    # tests.
    db = DatabaseFixture.create()
    yield db
    db.close()


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
    alembic_config: dict[str, Any] | alembic.config.Config | Config,
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


class CreateExternalIntegration(Protocol):
    def __call__(
        self,
        connection: Connection,
        protocol: str | None = None,
        goal: str | None = None,
        name: str | None = None,
    ) -> int:
        ...


@pytest.fixture
def create_external_integration(random_name: RandomName) -> CreateExternalIntegration:
    def fixture(
        connection: Connection,
        protocol: str | None = None,
        goal: str | None = None,
        name: str | None = None,
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
        key: str | None = None,
        value: str | None = None,
        integration_id: int | None = None,
        library_id: int | None = None,
        associate_library: bool = False,
    ) -> int:
        ...


@pytest.fixture
def create_config_setting() -> CreateConfigSetting:
    def fixture(
        connection: Connection,
        key: str | None = None,
        value: str | None = None,
        integration_id: int | None = None,
        library_id: int | None = None,
        associate_library: bool = False,
    ) -> int:
        if type(value) in (tuple, list, dict):
            value = json.dumps(value)
        setting = connection.execute(
            "INSERT INTO configurationsettings (key, value, external_integration_id, library_id) VALUES (%s, %s, %s, %s) returning id",
            (key, value, integration_id, library_id),
        ).fetchone()
        assert setting is not None
        assert isinstance(setting.id, int)

        # If a library is associated with the setting we must associate the integration as well
        if library_id and associate_library:
            relation = connection.execute(
                "select * from externalintegrations_libraries where externalintegration_id=%s and library_id=%s",
                (integration_id, library_id),
            ).fetchone()
            if not relation:
                connection.execute(
                    "INSERT INTO externalintegrations_libraries (externalintegration_id, library_id) VALUES (%s, %s)",
                    (integration_id, library_id),
                )

        return setting.id

    return fixture


class CreateIntegrationConfiguration(Protocol):
    def __call__(
        self,
        connection: Connection,
        name: str,
        protocol: str,
        goal: str,
        settings: dict[str, Any] | None = None,
    ) -> int:
        ...


@pytest.fixture
def create_integration_configuration() -> CreateIntegrationConfiguration:
    def fixture(
        connection: Connection,
        name: str,
        protocol: str,
        goal: str,
        settings: dict[str, Any] | None = None,
    ) -> int:
        if settings is None:
            settings = {}

        settings_str = json_serializer(settings)

        integration_configuration = connection.execute(
            "INSERT INTO integration_configurations (name, protocol, goal, settings, self_test_results, context) "
            "VALUES (%s, %s, %s, %s, '{}', '{}') returning id",
            name,
            protocol,
            goal,
            settings_str,
        ).fetchone()
        assert integration_configuration is not None
        assert isinstance(integration_configuration.id, int)
        return integration_configuration.id

    return fixture


class CreateEdition(Protocol):
    def __call__(
        self,
        connection: Connection,
        title: str,
        medium: str,
        primary_identifier_id: int,
    ) -> int:
        ...


@pytest.fixture
def create_edition() -> CreateEdition:
    def fixture(
        connection: Connection, title: str, medium: str, primary_identifier_id: int
    ) -> int:
        edition = connection.execute(
            "INSERT INTO editions (title, medium, primary_identifier_id) VALUES (%s, %s, %s) returning id",
            title,
            medium,
            primary_identifier_id,
        ).fetchone()
        assert edition is not None
        return cast(int, edition.id)

    return fixture


class CreateIdentifier:
    def __call__(
        self,
        connection: Connection,
        identifier: str | None = None,
        type: str | None = None,
    ) -> int:
        identifier = identifier or self.random_name()
        type = type or self.random_name()
        identifier_row = connection.execute(
            "INSERT INTO identifiers (identifier, type) VALUES (%s, %s) returning id",
            identifier,
            type,
        ).fetchone()
        assert identifier_row is not None
        assert isinstance(identifier_row.id, int)
        return identifier_row.id

    def __init__(self, random_name: RandomName) -> None:
        self.random_name = random_name


@pytest.fixture
def create_identifier(random_name: RandomName) -> CreateIdentifier:
    return CreateIdentifier(random_name)


class CreateLicensePool(Protocol):
    def __call__(
        self,
        connection: Connection,
        collection_id: int,
        identifier_id: int | None = None,
        should_track_playtime: bool | None = False,
    ) -> int:
        ...


@pytest.fixture
def create_license_pool() -> CreateLicensePool:
    def fixture(
        connection: Connection,
        collection_id: int,
        identifier_id: int | None = None,
        should_track_playtime: bool | None = False,
    ) -> int:
        licensepool = connection.execute(
            "INSERT into licensepools (collection_id, identifier_id, should_track_playtime) VALUES (%(id)s, %(identifier_id)s, %(track)s) returning id",
            id=collection_id,
            identifier_id=identifier_id,
            track=should_track_playtime,
        ).fetchone()
        assert licensepool is not None
        return cast(int, licensepool.id)

    return fixture


class CreateLane:
    def __call__(
        self,
        connection: Connection,
        library_id: int,
        name: str | None = None,
        priority: int = 0,
        inherit_parent_restrictions: bool = False,
        include_self_in_grouped_feed: bool = False,
        visible: bool = True,
    ) -> int:
        name = name or self.random_name()
        lane = connection.execute(
            "INSERT INTO lanes "
            "(library_id, display_name, priority, size, inherit_parent_restrictions, "
            "include_self_in_grouped_feed, visible) "
            " VALUES (%s, %s, %s, 0, %s, %s, %s) returning id",
            library_id,
            name,
            priority,
            inherit_parent_restrictions,
            include_self_in_grouped_feed,
            visible,
        ).fetchone()
        assert lane is not None
        assert isinstance(lane.id, int)
        return lane.id

    def __init__(self, random_name: RandomName) -> None:
        self.random_name = random_name


@pytest.fixture
def create_lane(random_name: RandomName) -> CreateLane:
    return CreateLane(random_name)


class CreateCoverageRecord:
    def __call__(
        self,
        connection: Connection,
        operation: str | None = None,
        identifier_id: int | None = None,
        collection_id: int | None = None,
    ) -> int:
        if identifier_id is None:
            identifier_id = self.create_identifier(connection)

        if operation is None:
            operation = self.random_name()

        row = connection.execute(
            "INSERT INTO coveragerecords (operation, identifier_id, collection_id, timestamp) "
            "VALUES (%s, %s, %s, '2021-01-01') returning id",
            operation,
            identifier_id,
            collection_id,
        ).first()
        assert row is not None
        assert isinstance(row.id, int)
        return row.id

    def __init__(
        self, create_identifier: CreateIdentifier, random_name: RandomName
    ) -> None:
        self.create_identifier = create_identifier
        self.random_name = random_name


@pytest.fixture
def create_coverage_record(
    create_identifier: CreateIdentifier, random_name: RandomName
) -> CreateCoverageRecord:
    return CreateCoverageRecord(create_identifier, random_name)

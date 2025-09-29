from __future__ import annotations

import random
import string
import uuid
from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import pytest_alembic
from pytest_alembic.config import Config
from sqlalchemy import text

from palace.manager.integration.goals import Goals
from palace.manager.integration.settings import BaseSettings
from palace.manager.util.json import json_serializer
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
    def random_name(length: int | None = None, charset: str | None = None) -> str:
        if length is None:
            length = 10
        if charset is None:
            charset = string.ascii_lowercase
        return "".join(random.choices(charset, k=length))

    def fetch_library(self, library_id: int) -> Row:
        with self._engine.begin() as connection:
            result = connection.execute(
                text("SELECT * FROM libraries WHERE id = :id"),
                {"id": library_id},
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

        with self._engine.begin() as connection:
            library = connection.execute(
                text(f"INSERT INTO libraries ({keys}) VALUES ({values}) returning id")
            ).fetchone()

        assert library is not None
        assert isinstance(library.id, int)
        return library.id

    def fetch_integration(self, integration_id: int) -> Row:
        with self._engine.begin() as connection:
            result = connection.execute(
                text("SELECT * FROM integration_configurations WHERE id = :id"),
                {"id": integration_id},
            )
            return result.one()

    def integration(
        self,
        protocol: str | None = None,
        goal: Goals | str | None = None,
        name: str | None = None,
        settings: dict[str, Any] | BaseSettings | None = None,
        context: dict[str, Any] | None = None,
        self_test_results: dict[str, Any] | None = None,
    ) -> int:
        if protocol is None:
            protocol = self.random_name()

        if goal is None:
            goal = Goals.LICENSE_GOAL.name
        elif isinstance(goal, Goals):
            goal = goal.name

        if name is None:
            name = self.random_name()

        if settings is None:
            settings = {}
        elif isinstance(settings, BaseSettings):
            settings = settings.model_dump()

        if context is None:
            context = {}

        if self_test_results is None:
            self_test_results = {}

        settings_json = json_serializer(settings)
        context_json = json_serializer(context)
        self_test_results_json = json_serializer(self_test_results)

        with self._engine.begin() as connection:
            integration = connection.execute(
                text(
                    "INSERT INTO integration_configurations "
                    "(protocol, goal, name, settings, context, self_test_results) "
                    "VALUES (:protocol, :goal, :name, :settings, :context, :test_results) "
                    "returning id"
                ).bindparams(
                    protocol=protocol,
                    goal=goal,
                    name=name,
                    settings=settings_json,
                    context=context_json,
                    test_results=self_test_results_json,
                )
            ).fetchone()

        assert integration is not None
        assert isinstance(integration.id, int)
        return integration.id

    def patron(
        self,
        library_id: int,
        external_identifier: str | None = None,
        authorization_identifier: str | None = None,
        uuid_value: str | None = None,
    ) -> int:
        """Create a patron record."""
        if external_identifier is None:
            external_identifier = self.random_name()
        if authorization_identifier is None:
            authorization_identifier = self.random_name()
        if uuid_value is None:
            uuid_value = str(uuid.uuid4())

        with self._engine.begin() as connection:
            patron = connection.execute(
                text(
                    """
                    INSERT INTO patrons (library_id, external_identifier, authorization_identifier, uuid)
                    VALUES (:library_id, :external_id, :auth_id, :uuid)
                    RETURNING id
                """
                ),
                {
                    "library_id": library_id,
                    "external_id": external_identifier,
                    "auth_id": authorization_identifier,
                    "uuid": uuid_value,
                },
            ).fetchone()

        assert patron is not None
        assert isinstance(patron.id, int)
        return patron.id

    def data_source(
        self,
        name: str | None = None,
        offers_licenses: bool = True,
        extra: str = "{}",
    ) -> int:
        """Create a data source record."""
        if name is None:
            name = self.random_name()

        with self._engine.begin() as connection:
            data_source = connection.execute(
                text(
                    """
                    INSERT INTO datasources (name, offers_licenses, extra)
                    VALUES (:name, :offers_licenses, :extra)
                    RETURNING id
                """
                ),
                {"name": name, "offers_licenses": offers_licenses, "extra": extra},
            ).fetchone()

        assert data_source is not None
        assert isinstance(data_source.id, int)
        return data_source.id

    def identifier(
        self,
        identifier_type: str = "ISBN",
        identifier: str | None = None,
    ) -> int:
        """Create an identifier record."""
        if identifier is None:
            # Generate a random ISBN-like identifier
            identifier = f"978{self.random_name(10, string.digits)}"

        with self._engine.begin() as connection:
            identifier_record = connection.execute(
                text(
                    """
                    INSERT INTO identifiers (type, identifier)
                    VALUES (:type, :identifier)
                    RETURNING id
                """
                ),
                {"type": identifier_type, "identifier": identifier},
            ).fetchone()

        assert identifier_record is not None
        assert isinstance(identifier_record.id, int)
        return identifier_record.id

    def collection(
        self,
        integration_configuration_id: int,
        marked_for_deletion: bool = False,
        export_marc_records: bool = False,
    ) -> int:
        """Create a collection record."""
        with self._engine.begin() as connection:
            collection = connection.execute(
                text(
                    """
                    INSERT INTO collections (integration_configuration_id, marked_for_deletion, export_marc_records)
                    VALUES (:integration_configuration_id, :marked_for_deletion, :export_marc_records)
                    RETURNING id
                """
                ),
                {
                    "integration_configuration_id": integration_configuration_id,
                    "marked_for_deletion": marked_for_deletion,
                    "export_marc_records": export_marc_records,
                },
            ).fetchone()

        assert collection is not None
        assert isinstance(collection.id, int)
        return collection.id

    def license_pool(
        self,
        data_source_id: int,
        identifier_id: int,
        collection_id: int,
        licenses_owned: int = 1,
        licenses_available: int = 1,
        licenses_reserved: int = 0,
        patrons_in_hold_queue: int = 0,
        suppressed: bool = False,
        should_track_playtime: bool = False,
    ) -> int:
        """Create a license pool record."""
        with self._engine.begin() as connection:
            license_pool = connection.execute(
                text(
                    """
                    INSERT INTO licensepools (
                        data_source_id, identifier_id, collection_id,
                        licenses_owned, licenses_available, licenses_reserved,
                        patrons_in_hold_queue, suppressed, should_track_playtime
                    )
                    VALUES (:data_source_id, :identifier_id, :collection_id,
                            :licenses_owned, :licenses_available, :licenses_reserved,
                            :patrons_in_hold_queue, :suppressed, :should_track_playtime)
                    RETURNING id
                """
                ),
                {
                    "data_source_id": data_source_id,
                    "identifier_id": identifier_id,
                    "collection_id": collection_id,
                    "licenses_owned": licenses_owned,
                    "licenses_available": licenses_available,
                    "licenses_reserved": licenses_reserved,
                    "patrons_in_hold_queue": patrons_in_hold_queue,
                    "suppressed": suppressed,
                    "should_track_playtime": should_track_playtime,
                },
            ).fetchone()

        assert license_pool is not None
        assert isinstance(license_pool.id, int)
        return license_pool.id


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

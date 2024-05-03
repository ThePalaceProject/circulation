from __future__ import annotations

import importlib
import logging
import shutil
import tempfile
import time
import uuid
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from functools import cached_property
from textwrap import dedent
from typing import Any
from unittest.mock import patch

import pytest
import sqlalchemy
from Crypto.PublicKey.RSA import import_key
from pydantic import PostgresDsn
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.engine import Connection, Engine, Transaction, make_url
from sqlalchemy.orm import Session, sessionmaker
from typing_extensions import Self

from palace.manager.api.discovery.opds_registration import OpdsRegistrationService
from palace.manager.core.classifier import Classifier
from palace.manager.core.config import Configuration
from palace.manager.core.exceptions import BasePalaceException
from palace.manager.core.opds_import import OPDSAPI
from palace.manager.integration.configuration.library import LibrarySettings
from palace.manager.integration.goals import Goals
from palace.manager.integration.registry.discovery import DiscoveryRegistry
from palace.manager.service.configuration import ServiceConfiguration
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.classification import (
    Classification,
    Genre,
    Subject,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.coverage import CoverageRecord, WorkCoverageRecord
from palace.manager.sqlalchemy.model.credential import Credential
from palace.manager.sqlalchemy.model.customlist import CustomList
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from palace.manager.sqlalchemy.model.lane import Lane
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    License,
    LicensePool,
    LicensePoolDeliveryMechanism,
    LicenseStatus,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.session import SessionManager
from palace.manager.sqlalchemy.util import create, get_one_or_create
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.services import ServicesFixture


class TestIdFixture:
    """
    This fixture creates a unique test id. This ID is suitable for initializing shared resources.
    For example - database name, opensearch index name, etc.
    """

    def __init__(self, worker_id: str, prefix: str):
        # worker_id comes from the pytest-xdist fixture
        self._worker_id = worker_id

        # This flag indicates that the tests are running in parallel mode.
        self.parallel = worker_id != "master"

        # We create a unique run id for each test run. The dashes are
        # replaced with underscores to make it a valid identifier for
        # in PostgreSQL.
        self.run_id = str(uuid.uuid4()).replace("-", "_")

        self._prefix = prefix

    @cached_property
    def id(self) -> str:
        # The test id is a combination of the prefix, worker id and run id.
        # This is the ID that should be used to create unique resources.
        return f"{self._prefix}_{self._worker_id}_{self.run_id}"


@pytest.fixture(scope="session")
def session_test_id(worker_id: str) -> TestIdFixture:
    """
    This is a session scoped fixture that provides a unique test id. Since session scoped fixtures
    are created only once per worker, per test run, this fixture provides a unique test ID that is
    stable for the worker for the entire test run.

    This is useful when initializing session scoped shared resources like databases, opensearch indexes, etc.
    """
    return TestIdFixture(worker_id, "session")


@pytest.fixture(scope="function")
def function_test_id(worker_id: str) -> TestIdFixture:
    """
    This is a function scoped fixture that provides a unique test id. Since function scoped fixtures
    are created for each test function, this fixture provides a unique test ID that for each test function.

    This is useful when initializing function scoped shared resources.
    """

    return TestIdFixture(worker_id, "function")


class DatabaseTestConfiguration(ServiceConfiguration):
    url: PostgresDsn
    create_database: bool = True

    class Config:
        env_prefix = "PALACE_TEST_DATABASE_"


class DatabaseCreationFixture:
    """
    Uses the configured database URL to create a unique database for each test run. The database
    is dropped after the test run is complete.

    Database creation can be disabled by setting the `create_database` flag to False in the configuration.
    In this case the database URL is used as is.
    """

    def __init__(self, test_id: TestIdFixture):
        self.test_id = test_id
        config = DatabaseTestConfiguration()
        if not config.create_database and self.test_id.parallel:
            raise BasePalaceException(
                "Database creation is disabled, but tests are running in parallel mode. "
                "This is not supported. Please enable database creation or run tests in serial mode."
            )
        self.create_database = config.create_database
        self._config_url = make_url(config.url)

    @cached_property
    def database_name(self) -> str:
        """
        Returns the name of the database that the test should use.
        """

        if not self.create_database:
            if self._config_url.database is None:
                raise BasePalaceException(
                    "Database name is required when database creation is disabled."
                )
            return self._config_url.database

        return self.test_id.id

    @cached_property
    def url(self) -> str:
        """
        Returns the Postgres URL for the database that the test should use. This URL
        includes credentials and the database name, so it has everything needed to
        connect to the database.
        """

        return str(self._config_url.set(database=self.database_name))

    @contextmanager
    def _db_connection(self) -> Generator[Connection, None, None]:
        """
        Databases need to be created and dropped outside a transaction. This method
        provides a connection to database URL provided in the configuration that is not
        wrapped in a transaction.
        """

        engine = create_engine(self._config_url, isolation_level="AUTOCOMMIT")
        connection = engine.connect()
        try:
            yield connection
        finally:
            connection.close()
            engine.dispose()

    def _create_db(self) -> None:
        if not self.create_database:
            return

        with self._db_connection() as connection:
            user = self._config_url.username
            connection.execute(text(f"CREATE DATABASE {self.database_name}"))
            connection.execute(
                text(f"GRANT ALL PRIVILEGES ON DATABASE {self.database_name} TO {user}")
            )

    def _drop_db(self) -> None:
        if not self.create_database:
            return

        with self._db_connection() as connection:
            connection.execute(text(f"DROP DATABASE {self.database_name}"))

    @contextmanager
    def patch_database_url(self) -> Generator[None, None, None]:
        """
        This method patches the database URL, so any code that uses it will use the worker specific database.
        """
        with patch.object(Configuration, "database_url", return_value=self.url):
            yield

    @classmethod
    @contextmanager
    def fixture(cls, test_id: TestIdFixture) -> Generator[Self, None, None]:
        db_name_fixture = cls(test_id)
        db_name_fixture._create_db()
        try:
            yield db_name_fixture
        finally:
            db_name_fixture._drop_db()


@pytest.fixture(scope="session")
def database_creation(
    session_test_id: TestIdFixture,
) -> Generator[DatabaseCreationFixture, None, None]:
    """
    This is a session scoped fixture that provides a unique database for each worker in the test run.
    """
    with DatabaseCreationFixture.fixture(session_test_id) as fixture:
        yield fixture


@pytest.fixture(scope="function")
def function_database_creation(
    function_test_id: TestIdFixture,
) -> Generator[DatabaseCreationFixture, None, None]:
    """
    This is a function scoped fixture that provides a unique database for each test function.

    This is resource intensive, so it should only be used when necessary. It is helpful
    when testing database interactions that cannot be tested in a transaction. Such as
    instance initialization, schema migrations, etc.
    """
    with DatabaseCreationFixture.fixture(function_test_id) as fixture:
        if not fixture.create_database:
            raise BasePalaceException(
                "Cannot provide a function scoped database when database creation is disabled."
            )
        with fixture.patch_database_url():
            yield fixture


class DatabaseFixture:
    """
    The DatabaseFixture initializes the database schema and creates a connection to the database
    that should be used in the tests.
    """

    def __init__(self, database_name: DatabaseCreationFixture) -> None:
        self.database_name = database_name
        self.engine = self.engine_factory()
        self.connection = self.engine.connect()

    def engine_factory(self) -> Engine:
        return SessionManager.engine(self.database_name.url)

    def drop_existing_schema(self) -> None:
        metadata_obj = MetaData()
        metadata_obj.reflect(bind=self.engine)
        metadata_obj.drop_all(self.engine)
        metadata_obj.clear()

    def _initialize_database(self) -> None:
        SessionManager.initialize_schema(self.connection)
        with Session(self.connection) as session:
            # Initialize the database with default data
            SessionManager.initialize_data(session)

    @staticmethod
    def _load_model_classes():
        """
        Make sure that all the model classes are loaded, so that they are registered with the
        ORM when we are creating the schema.
        """
        import palace.manager.sqlalchemy.model

        importlib.reload(palace.manager.sqlalchemy.model)

    def _close(self):
        # Destroy the database connection and engine.
        self.connection.close()
        self.engine.dispose()

    @contextmanager
    def patch_engine(self) -> Generator[None, None, None]:
        """
        This method patches the SessionManager to use the engine provided by this fixture.

        This is useful when the tests need to access the engine directly. It patches the SessionManager
        to use the engine provided by this fixture and patches the engine so that code that calls
        dispose() on the engine does not actually dispose it, since it is used by this fixture.
        """
        with patch.object(self.engine, "dispose"), patch.object(
            SessionManager, "engine", return_value=self.engine
        ):
            yield

    @contextmanager
    def patch_engine_error(self) -> Generator[None, None, None]:
        """
        This method patches the SessionManager to raise an exception when the engine is accessed.
        This is useful when the tests should not be accessing the engine directly.
        """
        with patch.object(
            SessionManager,
            "engine",
            side_effect=BasePalaceException(
                "Engine needs to be accessed from fixture within tests."
            ),
        ):
            yield

    @classmethod
    @contextmanager
    def fixture(
        cls, database_name: DatabaseCreationFixture
    ) -> Generator[Self, None, None]:
        db_fixture = cls(database_name)
        db_fixture.drop_existing_schema()
        db_fixture._load_model_classes()
        db_fixture._initialize_database()
        try:
            yield db_fixture
        finally:
            db_fixture._close()


@pytest.fixture(scope="session")
def database(
    database_creation: DatabaseCreationFixture,
) -> Generator[DatabaseFixture, None, None]:
    """
    This is a session scoped fixture that provides a unique database engine and connection
    for each worker in the test run.
    """
    with DatabaseFixture.fixture(database_creation) as db:
        yield db


@pytest.fixture(scope="function")
def function_database(
    function_database_creation: DatabaseCreationFixture,
) -> Generator[DatabaseFixture, None, None]:
    """
    This is a function scoped fixture that provides a unique database engine and connection
    for each test. This is resource intensive, so it should only be used when necessary.
    """
    with DatabaseFixture.fixture(function_database_creation) as db:
        with db.patch_engine_error():
            yield db


class DatabaseTransactionFixture:
    """A fixture representing a single transaction. The transaction is automatically rolled back."""

    def __init__(self, database: DatabaseFixture, services: ServicesFixture):
        self._database = database
        self._default_library: Library | None = None
        self._default_collection: Collection | None = None
        self._counter = 2000
        self._isbns = [
            "9780674368279",
            "0636920028468",
            "9781936460236",
            "9780316075978",
        ]
        self._services = services
        self._session = SessionManager.session_from_connection(database.connection)
        self._transaction = database.connection.begin_nested()

    def _make_default_library(self) -> Library:
        """Ensure that the default library exists in the given database."""
        library = self.library("default", "default")
        collection = self.collection(
            "Default Collection",
            protocol=OPDSAPI.label(),
            data_source_name="OPDS",
            external_account_id="http://opds.example.com/feed",
        )
        collection.libraries.append(library)
        return library

    @classmethod
    @contextmanager
    def fixture(
        cls, database: DatabaseFixture, services: ServicesFixture
    ) -> Generator[Self, None, None]:
        db = cls(database, services)
        try:
            yield db
        finally:
            db._close()

    def _close(self):
        # Close the session.
        self._session.close()

        # Roll back all database changes that happened during this
        # test, whether in the session that was just closed or some
        # other session.
        self._transaction.rollback()

        Configuration.SITE_CONFIGURATION_LAST_UPDATE = None
        Configuration.LAST_CHECKED_FOR_SITE_CONFIGURATION_UPDATE = None

    @property
    def database(self) -> DatabaseFixture:
        return self._database

    @property
    def transaction(self) -> Transaction:
        return self._transaction

    @property
    def session(self) -> Session:
        return self._session

    def default_collection(self) -> Collection:
        """A Collection that will only be created once throughout
        a given test.

        For most tests there's no need to create a different
        Collection for every LicensePool. Using
        default_collection() instead of calling collection()
        saves time.
        """
        if not self._default_collection:
            self._default_collection = self.default_library().collections[0]

        return self._default_collection

    def default_library(self) -> Library:
        """A Library that will only be created once throughout a given test.

        By default, the `default_collection()` will be associated with
        the default library.
        """
        if not self._default_library:
            self._default_library = self._make_default_library()

        return self._default_library

    def fresh_id(self) -> int:
        self._counter += 1
        return self._counter

    def fresh_str(self) -> str:
        return str(self.fresh_id())

    def library(
        self,
        name: str | None = None,
        short_name: str | None = None,
        settings: LibrarySettings | None = None,
    ) -> Library:
        # Just a dummy key used for testing.
        key_string = """\
            -----BEGIN RSA PRIVATE KEY-----
            MIIBOQIBAAJBALFOBYf91uHhGQufTEOCZ9/L/Ge0/Lw4DRDuFBh9p+BpOxQJE9gi
            4FaJc16Wh53Sg5vQTOZMEGgjjTaP7K6NWgECAwEAAQJAEsR4b2meCjDCbumAsBCo
            oBa+c9fDfMTOFUGuHN2IHIe5zObxWAKD3xq73AO+mpeEl+KpeLeq2IJNqCZdf1yK
            MQIhAOGeurU6vgn/yA9gXECzvWYaxiAzHsOeW4RDhb/+14u1AiEAyS3VWo6jPt0i
            x8oiahujtCqaKLy611rFHQuK+yKNfJ0CIFuQVIuaNGfQc3uyCp6Dk3jtoryMoo6X
            JOLvmEdMAGQFAiB4D+psiQPT2JWRNokjWitwspweA8ReEcXhd6oSBqT54QIgaVc5
            wNybPDDs9mU+du+r0U+5iXaZzS5StYZpo9B4KjA=
            -----END RSA PRIVATE KEY-----
        """
        # Because key generation takes a significant amount of time, and we
        # create a lot of new libraries in our tests, we just use the same
        # dummy key for all of them.
        private_key = import_key(dedent(key_string))
        public_key = private_key.public_key()

        name = name or self.fresh_str()
        short_name = short_name or self.fresh_str()
        settings_dict = settings.dict() if settings else {}

        # Make sure we have defaults for settings that are required
        if "website" not in settings_dict:
            settings_dict["website"] = "http://library.com"
        if "help_web" not in settings_dict and "help_email" not in settings_dict:
            settings_dict["help_web"] = "http://library.com/support"

        library, ignore = get_one_or_create(
            self.session,
            Library,
            name=name,
            short_name=short_name,
            create_method_kwargs=dict(
                uuid=str(uuid.uuid4()),
                public_key=public_key.export_key("PEM").decode("utf-8"),
                private_key=private_key.export_key("DER"),
                settings_dict=settings_dict,
            ),
        )
        return library

    def collection(
        self,
        name=None,
        protocol=OPDSAPI.label(),
        external_account_id=None,
        url=None,
        username=None,
        password=None,
        data_source_name=None,
        settings: dict[str, Any] | None = None,
    ) -> Collection:
        name = name or self.fresh_str()
        collection, _ = Collection.by_name_and_protocol(self.session, name, protocol)
        settings = settings or {}
        if url:
            settings["url"] = url
        if username:
            settings["username"] = username
        if password:
            settings["password"] = password
        if external_account_id:
            settings["external_account_id"] = external_account_id
        collection.integration_configuration.settings_dict = settings

        if data_source_name:
            collection.data_source = data_source_name
        return collection

    def work(
        self,
        title=None,
        authors=None,
        genre=None,
        language=None,
        audience=None,
        fiction=True,
        with_license_pool=False,
        with_open_access_download=False,
        quality=0.5,
        series=None,
        presentation_edition=None,
        collection=None,
        data_source_name=None,
        unlimited_access=False,
    ):
        """Create a Work.

        For performance reasons, this method does not generate OPDS
        entries or calculate a presentation edition for the new
        Work. Tests that rely on this information being present
        should call _slow_work() instead, which takes more care to present
        the sort of Work that would be created in a real environment.
        """
        pools = []
        if with_open_access_download:
            with_license_pool = True
        language = language or "eng"
        title = str(title or self.fresh_str())
        audience = audience or Classifier.AUDIENCE_ADULT
        if audience == Classifier.AUDIENCE_CHILDREN and not data_source_name:
            # TODO: This is necessary because Gutenberg's childrens books
            # get filtered out at the moment.
            data_source_name = DataSource.OVERDRIVE
        elif not data_source_name:
            data_source_name = DataSource.GUTENBERG
        if fiction is None:
            fiction = True
        if not presentation_edition:
            presentation_edition = self.edition(
                title=title,
                language=language,
                authors=authors,
                with_license_pool=with_license_pool,
                with_open_access_download=with_open_access_download,
                data_source_name=data_source_name,
                series=series,
                collection=collection,
                unlimited_access=unlimited_access,
            )
            if with_license_pool:
                presentation_edition, pool = presentation_edition
                if with_open_access_download:
                    pool.open_access = True
                if unlimited_access:
                    pool.open_access = False
                    pool.unlimited_access = True

                pools = [pool]
        else:
            pools = presentation_edition.license_pools
        work, ignore = get_one_or_create(
            self.session,
            Work,
            create_method_kwargs=dict(
                audience=audience, fiction=fiction, quality=quality
            ),
            id=self.fresh_id(),
        )
        if genre:
            if not isinstance(genre, Genre):
                genre, ignore = Genre.lookup(self.session, genre, autocreate=True)
            work.genres = [genre]
        work.random = 0.5
        work.set_presentation_edition(presentation_edition)

        if pools:
            # make sure the pool's presentation_edition is set,
            # bc loan tests assume that.
            if not work.license_pools:
                for pool in pools:
                    work.license_pools.append(pool)

            for pool in pools:
                pool.set_presentation_edition()

            # This is probably going to be used in an OPDS feed, so
            # fake that the work is presentation ready.
            work.presentation_ready = True

        return work

    def contributor(self, sort_name=None, name=None, **kw_args):
        name = sort_name or name or self.fresh_str()
        return get_one_or_create(
            self.session, Contributor, sort_name=str(name), **kw_args
        )

    def edition(
        self,
        data_source_name=DataSource.GUTENBERG,
        identifier_type=Identifier.GUTENBERG_ID,
        with_license_pool=False,
        with_open_access_download=False,
        title=None,
        language="eng",
        authors=None,
        identifier_id=None,
        series=None,
        collection=None,
        publication_date=None,
        unlimited_access=False,
    ):
        id = identifier_id or self.fresh_str()
        source = DataSource.lookup(self.session, data_source_name)
        wr = Edition.for_foreign_id(self.session, source, identifier_type, id)[0]
        if not title:
            title = self.fresh_str()
        wr.title = str(title)
        wr.medium = Edition.BOOK_MEDIUM
        if series:
            wr.series = series
        if language:
            wr.language = language
        if authors is None:
            authors = self.fresh_str()
        if isinstance(authors, str):
            authors = [authors]
        if authors:
            primary_author_name = str(authors[0])
            contributor = wr.add_contributor(
                primary_author_name, Contributor.PRIMARY_AUTHOR_ROLE
            )
            # add_contributor assumes authors[0] is a sort_name,
            # but it may be a display name. If so, set that field as well.
            if not contributor.display_name and "," not in primary_author_name:
                contributor.display_name = primary_author_name
            wr.author = primary_author_name

        for author in authors[1:]:
            wr.add_contributor(str(author), Contributor.AUTHOR_ROLE)
        if publication_date:
            wr.published = publication_date

        if with_license_pool or with_open_access_download:
            pool = self.licensepool(
                wr,
                data_source_name=data_source_name,
                with_open_access_download=with_open_access_download,
                collection=collection,
                unlimited_access=unlimited_access,
            )

            pool.set_presentation_edition()
            return wr, pool
        return wr

    def licensepool(
        self,
        edition,
        open_access=True,
        data_source_name=DataSource.GUTENBERG,
        with_open_access_download=False,
        set_edition_as_presentation=False,
        collection=None,
        unlimited_access=False,
    ):
        source = DataSource.lookup(self.session, data_source_name)
        if not edition:
            edition = self.edition(data_source_name)
        collection = collection or self.default_collection()
        assert collection
        pool, ignore = get_one_or_create(
            self.session,
            LicensePool,
            create_method_kwargs=dict(open_access=open_access),
            identifier=edition.primary_identifier,
            data_source=source,
            collection=collection,
            availability_time=utc_now(),
            unlimited_access=unlimited_access,
        )

        if set_edition_as_presentation:
            pool.presentation_edition = edition

        if with_open_access_download:
            pool.open_access = True
            url = "http://foo.com/" + self.fresh_str()
            media_type = MediaTypes.EPUB_MEDIA_TYPE
            link, new = pool.identifier.add_link(
                Hyperlink.OPEN_ACCESS_DOWNLOAD, url, source, media_type
            )

            # Add a DeliveryMechanism for this download
            pool.set_delivery_mechanism(
                media_type,
                DeliveryMechanism.NO_DRM,
                RightsStatus.GENERIC_OPEN_ACCESS,
                link.resource,
            )

            representation, is_new = self.representation(
                url, media_type, "Dummy content", mirrored=True
            )
            link.resource.representation = representation
        else:
            # Add a DeliveryMechanism for this licensepool
            pool.set_delivery_mechanism(
                MediaTypes.EPUB_MEDIA_TYPE,
                DeliveryMechanism.ADOBE_DRM,
                RightsStatus.UNKNOWN,
                None,
            )

            if not unlimited_access:
                pool.licenses_owned = pool.licenses_available = 1

        return pool

    def representation(self, url=None, media_type=None, content=None, mirrored=False):
        url = url or "http://foo.com/" + self.fresh_str()
        repr, is_new = get_one_or_create(self.session, Representation, url=url)
        repr.media_type = media_type
        if media_type and content:
            if isinstance(content, str):
                content = content.encode("utf8")
            repr.content = content
            repr.fetched_at = utc_now()
            if mirrored:
                repr.mirror_url = "http://foo.com/" + self.fresh_str()
                repr.mirrored_at = utc_now()
        return repr, is_new

    def lane(
        self,
        display_name=None,
        library=None,
        parent=None,
        genres=None,
        languages=None,
        fiction=None,
        inherit_parent_restrictions=True,
    ):
        display_name = display_name or self.fresh_str()
        library = library or self.default_library()
        lane, is_new = create(
            self.session,
            Lane,
            library=library,
            parent=parent,
            display_name=display_name,
            fiction=fiction,
            inherit_parent_restrictions=inherit_parent_restrictions,
        )
        if is_new and parent:
            lane.priority = len(parent.sublanes) - 1
        if genres:
            if not isinstance(genres, list):
                genres = [genres]
            for genre in genres:
                if isinstance(genre, str):
                    genre, ignore = Genre.lookup(self.session, genre)
                lane.genres.append(genre)
        if languages:
            if not isinstance(languages, list):
                languages = [languages]
            lane.languages = languages
        return lane

    def subject(self, type, identifier) -> Subject:
        return get_one_or_create(
            self.session, Subject, type=type, identifier=identifier
        )[0]

    def coverage_record(
        self,
        edition,
        coverage_source,
        operation=None,
        status=CoverageRecord.SUCCESS,
        collection=None,
        exception=None,
    ) -> CoverageRecord:
        if isinstance(edition, Identifier):
            identifier = edition
        else:
            identifier = edition.primary_identifier
        record, ignore = get_one_or_create(
            self.session,
            CoverageRecord,
            identifier=identifier,
            data_source=coverage_source,
            operation=operation,
            collection=collection,
            create_method_kwargs=dict(
                timestamp=utc_now(),
                status=status,
                exception=exception,
            ),
        )
        return record

    def identifier(self, identifier_type=Identifier.GUTENBERG_ID, foreign_id=None):
        if foreign_id:
            id_value = foreign_id
        else:
            id_value = self.fresh_str()
        return Identifier.for_foreign_id(self.session, identifier_type, id_value)[0]

    def fresh_url(self) -> str:
        return "http://foo.com/" + self.fresh_str()

    def patron(self, external_identifier=None, library=None) -> Patron:
        external_identifier = external_identifier or self.fresh_str()
        library = library or self.default_library()
        assert library
        return get_one_or_create(
            self.session,
            Patron,
            external_identifier=external_identifier,
            library=library,
        )[0]

    def license(
        self,
        pool,
        identifier=None,
        checkout_url=None,
        status_url=None,
        expires=None,
        checkouts_left=None,
        checkouts_available=None,
        status=LicenseStatus.available,
        terms_concurrency=None,
    ) -> License:
        identifier = identifier or self.fresh_str()
        checkout_url = checkout_url or self.fresh_str()
        status_url = status_url or self.fresh_str()
        license, ignore = get_one_or_create(
            self.session,
            License,
            identifier=identifier,
            license_pool=pool,
            checkout_url=checkout_url,
            status_url=status_url,
            expires=expires,
            checkouts_left=checkouts_left,
            checkouts_available=checkouts_available,
            status=status,
            terms_concurrency=terms_concurrency,
        )
        return license

    def isbn_take(self) -> str:
        return self._isbns.pop()

    def integration_configuration(
        self, protocol: str, goal=None, libraries=None, name=None, **kwargs
    ):
        integration, ignore = get_one_or_create(
            self.session,
            IntegrationConfiguration,
            protocol=protocol,
            goal=goal,
            name=(name or self.fresh_str()),
        )

        if libraries is None:
            libraries = []

        if not isinstance(libraries, list):
            libraries = [libraries]

        integration.libraries.extend(libraries)

        integration.settings_dict = kwargs
        return integration

    @classmethod
    def set_settings(
        cls,
        config: IntegrationConfiguration | IntegrationLibraryConfiguration,
        *keyvalues,
        **kwargs,
    ):
        settings = config.settings_dict.copy()

        # Alternating key: value in the args
        for ix, item in enumerate(keyvalues):
            if ix % 2 == 0:
                key = item
            else:
                settings[key] = item

        settings.update(kwargs)
        config.settings_dict = settings

    def work_coverage_record(
        self, work, operation=None, status=CoverageRecord.SUCCESS
    ) -> WorkCoverageRecord:
        record, ignore = get_one_or_create(
            self.session,
            WorkCoverageRecord,
            work=work,
            operation=operation,
            create_method_kwargs=dict(
                timestamp=utc_now(),
                status=status,
            ),
        )
        return record

    def classification(
        self, identifier, subject, data_source, weight=1
    ) -> Classification:
        return get_one_or_create(
            self.session,
            Classification,
            identifier=identifier,
            subject=subject,
            data_source=data_source,
            weight=weight,
        )[0]

    def customlist(
        self,
        foreign_identifier=None,
        name=None,
        data_source_name=DataSource.NYT,
        num_entries=1,
        entries_exist_as_works=True,
    ):
        data_source = DataSource.lookup(self.session, data_source_name)
        foreign_identifier = foreign_identifier or self.fresh_str()
        now = utc_now()
        customlist, ignore = get_one_or_create(
            self.session,
            CustomList,
            create_method_kwargs=dict(
                created=now,
                updated=now,
                name=name or self.fresh_str(),
                description=self.fresh_str(),
            ),
            data_source=data_source,
            foreign_identifier=foreign_identifier,
        )

        editions = []
        for i in range(num_entries):
            if entries_exist_as_works:
                work = self.work(with_open_access_download=True)
                edition = work.presentation_edition
            else:
                edition = self.edition(data_source_name, title="Item %s" % i)
                edition.permanent_work_id = "Permanent work ID %s" % self.fresh_str()
            customlist.add_entry(edition, "Annotation %s" % i, first_appearance=now)
            editions.append(edition)
        return customlist, editions

    def add_generic_delivery_mechanism(self, license_pool: LicensePool):
        """Give a license pool a generic non-open-access delivery mechanism."""
        data_source = license_pool.data_source
        identifier = license_pool.identifier
        content_type = Representation.EPUB_MEDIA_TYPE
        drm_scheme = DeliveryMechanism.NO_DRM
        return LicensePoolDeliveryMechanism.set(
            data_source, identifier, content_type, drm_scheme, RightsStatus.IN_COPYRIGHT
        )

    def slow_work(self, *args, **kwargs):
        """Create a work that closely resembles one that might be found in the
        wild.

        This is significantly slower than _work() but more reliable.
        """
        work = self.work(*args, **kwargs)
        work.calculate_presentation_edition()
        return work

    def sample_ecosystem(self):
        """Creates an ecosystem of some sample work, pool, edition, and author
        objects that all know each other.
        """
        # make some authors
        [bob], ignore = Contributor.lookup(self.session, "Bitshifter, Bob")
        bob.family_name, bob.display_name = bob.default_names()
        [alice], ignore = Contributor.lookup(self.session, "Adder, Alice")
        alice.family_name, alice.display_name = alice.default_names()

        edition_std_ebooks, pool_std_ebooks = self.edition(
            DataSource.STANDARD_EBOOKS,
            Identifier.URI,
            with_license_pool=True,
            with_open_access_download=True,
            authors=[],
        )
        edition_std_ebooks.title = "The Standard Ebooks Title"
        edition_std_ebooks.subtitle = "The Standard Ebooks Subtitle"
        edition_std_ebooks.add_contributor(alice, Contributor.AUTHOR_ROLE)

        edition_git, pool_git = self.edition(
            DataSource.PROJECT_GITENBERG,
            Identifier.GUTENBERG_ID,
            with_license_pool=True,
            with_open_access_download=True,
            authors=[],
        )
        edition_git.title = "The GItenberg Title"
        edition_git.subtitle = "The GItenberg Subtitle"
        edition_git.add_contributor(bob, Contributor.AUTHOR_ROLE)
        edition_git.add_contributor(alice, Contributor.AUTHOR_ROLE)

        edition_gut, pool_gut = self.edition(
            DataSource.GUTENBERG,
            Identifier.GUTENBERG_ID,
            with_license_pool=True,
            with_open_access_download=True,
            authors=[],
        )
        edition_gut.title = "The GUtenberg Title"
        edition_gut.subtitle = "The GUtenberg Subtitle"
        edition_gut.add_contributor(bob, Contributor.AUTHOR_ROLE)

        work = self.work(presentation_edition=edition_git)

        for p in pool_gut, pool_std_ebooks:
            work.license_pools.append(p)

        work.calculate_presentation()

        return (
            work,
            pool_std_ebooks,
            pool_git,
            pool_gut,
            edition_std_ebooks,
            edition_git,
            edition_gut,
            alice,
            bob,
        )

    def credential(self, data_source_name=DataSource.GUTENBERG, type=None, patron=None):
        data_source = DataSource.lookup(self.session, data_source_name)
        type = type or self.fresh_str()
        patron = patron or self.patron()
        credential, is_new = Credential.persistent_token_create(
            self.session, data_source, type, patron
        )
        return credential


@pytest.fixture(scope="function")
def db(
    database_creation: DatabaseCreationFixture,
    database: DatabaseFixture,
    services_fixture: ServicesFixture,
) -> Generator[DatabaseTransactionFixture, None, None]:
    with DatabaseTransactionFixture.fixture(database, services_fixture) as db:
        with (
            database.patch_engine_error(),
            database_creation.patch_database_url(),
        ):
            yield db


class TemporaryDirectoryConfigurationFixture:
    """A fixture that configures the Configuration system to use a temporary directory.
    The directory is cleaned up when the fixture is closed."""

    _directory: str

    @classmethod
    def create(cls) -> TemporaryDirectoryConfigurationFixture:
        fix = TemporaryDirectoryConfigurationFixture()
        fix._directory = tempfile.mkdtemp(dir="/tmp")
        assert isinstance(fix._directory, str)
        return fix

    def close(self):
        if self._directory.startswith("/tmp"):
            logging.debug("Removing temporary directory %s" % self._directory)
            shutil.rmtree(self._directory)
        else:
            logging.warning(
                "Cowardly refusing to remove 'temporary' directory %s" % self._directory
            )

    def directory(self) -> str:
        return self._directory


@pytest.fixture(scope="function")
def temporary_directory_configuration() -> (
    Iterable[TemporaryDirectoryConfigurationFixture]
):
    fix = TemporaryDirectoryConfigurationFixture.create()
    yield fix
    fix.close()


class IntegrationConfigurationFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db

    def __call__(
        self, protocol: str | None, goal: Goals, settings_dict: dict | None = None
    ) -> IntegrationConfiguration:
        integration, _ = create(
            self.db.session,
            IntegrationConfiguration,
            name=self.db.fresh_str(),
            protocol=protocol,
            goal=goal,
            settings_dict=settings_dict or {},
        )
        return integration

    def discovery_service(
        self, protocol: str | None = None, url: str | None = None
    ) -> IntegrationConfiguration:
        registry = DiscoveryRegistry()
        if protocol is None:
            protocol = registry.get_protocol(OpdsRegistrationService)
            assert protocol is not None

        if url is not None:
            settings_obj = registry[protocol].settings_class().construct(url=url)  # type: ignore[arg-type]
            settings_dict = settings_obj.dict()
        else:
            settings_dict = {}

        return self(
            protocol=protocol, goal=Goals.DISCOVERY_GOAL, settings_dict=settings_dict
        )


@pytest.fixture
def create_integration_configuration(
    db: DatabaseTransactionFixture,
) -> IntegrationConfigurationFixture:
    fixture = IntegrationConfigurationFixture(db)
    return fixture


class IntegrationLibraryConfigurationFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db

    def __call__(
        self,
        library: Library,
        parent: IntegrationConfiguration,
        settings_dict: dict | None = None,
    ) -> IntegrationLibraryConfiguration:
        settings_dict = settings_dict or {}
        integration, _ = create(
            self.db.session,
            IntegrationLibraryConfiguration,
            parent=parent,
            library=library,
            settings_dict=settings_dict,
        )
        return integration


@pytest.fixture
def create_integration_library_configuration(
    db: DatabaseTransactionFixture,
) -> IntegrationLibraryConfigurationFixture:
    fixture = IntegrationLibraryConfigurationFixture(db)
    return fixture


class MockSessionMaker:
    def __init__(self, session: Session):
        self._session = session

    def __call__(self) -> Session:
        return self._session

    @contextmanager
    def begin(self) -> Generator[Session, None, None]:
        with self._session.begin_nested():
            yield self._session


@pytest.fixture
def mock_session_maker(db: DatabaseTransactionFixture) -> sessionmaker[Session]:
    return MockSessionMaker(db.session)  # type: ignore[return-value]


class DBStatementCounter:
    """
    Use as a context manager to count the number of execute()'s performed
    against the given sqlalchemy connection.

    Usage:
        with DBStatementCounter(conn) as ctr:
            conn.execute("SELECT 1")
            conn.execute("SELECT 1")
        assert ctr.get_count() == 2
    """

    def __init__(self, conn):
        self.conn = conn
        self.count = 0
        # Will have to rely on this since sqlalchemy 0.8 does not support
        # removing event listeners
        self.do_count = False
        sqlalchemy.event.listen(conn, "after_execute", self.callback)

    def __enter__(self):
        self.do_count = True
        return self

    def __exit__(self, *_):
        self.do_count = False

    def get_count(self):
        return self.count

    def callback(self, *_):
        if self.do_count:
            self.count += 1


class PerfTimer:
    """Performance timer to wrap around blocks of code

    Usage:
        <code we don't want timed>
        ....
        with PerfTimer() as pt:
            <do code we need to time>
            ....
        print ("Time taken:", pt.execution_time)
    """

    def __enter__(self):
        self.start = time.perf_counter()
        self.execution_time = 0
        return self

    def __exit__(self, *args):
        self.execution_time = time.perf_counter() - self.start

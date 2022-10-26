import logging
import os
import shutil
import tempfile
import uuid
from typing import Any, Iterable, Optional, Tuple

import pytest
from sqlalchemy.engine import Connection, Engine, Transaction
from sqlalchemy.orm import Session

import core.lane
from core.analytics import Analytics
from core.config import Configuration
from core.log import LogConfiguration
from core.model import (
    Base,
    Collection,
    ExternalIntegration,
    Library,
    SessionManager,
    get_one_or_create,
)
from core.model.devicetokens import DeviceToken


class ApplicationFixture:
    """The ApplicationFixture is a representation of the state that must be set up in order to run the application for
    testing."""

    @staticmethod
    def create():
        # This will make sure we always connect to the test database.
        os.environ["TESTING"] = "true"

        # Ensure that the log configuration starts in a known state.
        LogConfiguration.initialize(None, testing=True)

        # Drop any existing schema. It will be recreated when
        # SessionManager.initialize() runs.
        engine = SessionManager.engine()
        # Trying to drop all tables without reflecting first causes an issue
        # since SQLAlchemy does not know the order of cascades
        # Adding .reflect is throwing an error locally because tables are imported
        # later and hence being defined twice
        # Deleting the problematic table first fixes the issue, in this case DeviceToken
        DeviceToken.__table__.drop(engine, checkfirst=True)
        Base.metadata.drop_all(engine)
        return ApplicationFixture()

    def close(self):
        if "TESTING" in os.environ:
            del os.environ["TESTING"]


class DatabaseFixture:
    """The DatabaseFixture stores a reference to the database."""

    _engine: Engine
    _connection: Connection
    _old_data_dir: Any
    _tmp_data_dir: str

    def __init__(
        self,
        engine: Engine,
        connection: Connection,
        old_data_dir: Any,
        tmp_data_dir: str,
    ):
        self._engine = engine
        self._connection = connection
        self._old_data_dir = old_data_dir
        self._tmp_data_dir = tmp_data_dir

    @staticmethod
    def _get_database_connection() -> Tuple[Engine, Connection]:
        url = Configuration.database_url()
        engine, connection = SessionManager.initialize(url)
        return engine, connection

    @staticmethod
    def create() -> "DatabaseFixture":
        from core.model.customlist import customlist_sharedlibrary

        # The CM uses SQLAlchemy's "declarative base" feature, which means that
        # tables are only created if the classes containing references to them
        # are actually loaded. The entire application database is therefore dependent on
        # unspecified class initialization behaviour. The only safe way to guarantee
        # that all tables are created is to refer to all of those classes before
        # starting SQLAlchemy in order to ensure that all the classes are loaded
        # in the interpreter.

        tables = [
            core.lane.Lane.__tablename__,
            core.lane.LaneGenre.__tablename__,
            core.model.Annotation.__tablename__,
            core.model.CustomList.__tablename__,
            core.model.CustomListEntry.__tablename__,
            core.model.Edition.__tablename__,
            core.model.Hold.__tablename__,
            core.model.Loan.__tablename__,
            core.model.Patron.__tablename__,
            core.model.Work.__tablename__,
            core.model.WorkGenre.__tablename__,
            core.model.admin.Admin.__tablename__,
            core.model.admin.AdminRole.__tablename__,
            core.model.cachedfeed.CachedFeed.__tablename__,
            core.model.cachedfeed.CachedMARCFile.__tablename__,
            core.model.circulationevent.CirculationEvent.__tablename__,
            core.model.classification.Classification.__tablename__,
            core.model.classification.Genre.__tablename__,
            core.model.classification.Subject.__tablename__,
            core.model.collection.Collection.__tablename__,
            core.model.configuration.ConfigurationSetting.__tablename__,
            core.model.configuration.ExternalIntegration.__tablename__,
            core.model.configuration.ExternalIntegrationLink.__tablename__,
            core.model.contributor.Contribution.__tablename__,
            core.model.contributor.Contributor.__tablename__,
            core.model.coverage.CoverageRecord.__tablename__,
            core.model.coverage.EquivalencyCoverageRecord.__tablename__,
            core.model.coverage.Timestamp.__tablename__,
            core.model.coverage.WorkCoverageRecord.__tablename__,
            core.model.credential.Credential.__tablename__,
            core.model.credential.DRMDeviceIdentifier.__tablename__,
            core.model.credential.DelegatedPatronIdentifier.__tablename__,
            core.model.datasource.DataSource.__tablename__,
            core.model.devicetokens.DeviceToken.__tablename__,
            core.model.identifier.Equivalency.__tablename__,
            core.model.identifier.Identifier.__tablename__,
            core.model.identifier.RecursiveEquivalencyCache.__tablename__,
            core.model.integrationclient.IntegrationClient.__tablename__,
            core.model.library.Library.__tablename__,
            core.model.licensing.DeliveryMechanism.__tablename__,
            core.model.licensing.License.__tablename__,
            core.model.licensing.LicensePool.__tablename__,
            core.model.licensing.LicensePoolDeliveryMechanism.__tablename__,
            core.model.licensing.RightsStatus.__tablename__,
            core.model.measurement.Measurement.__tablename__,
            core.model.resource.Hyperlink.__tablename__,
            core.model.resource.Representation.__tablename__,
            core.model.resource.Resource.__tablename__,
            core.model.resource.ResourceTransformation.__tablename__,
            customlist_sharedlibrary.name,
        ]

        # Initialize a temporary data directory.
        engine, connection = DatabaseFixture._get_database_connection()
        old_data_dir = Configuration.data_directory
        tmp_data_dir = tempfile.mkdtemp(dir="/tmp")
        Configuration.instance[Configuration.DATA_DIRECTORY] = tmp_data_dir

        # Avoid CannotLoadConfiguration errors related to CDN integrations.
        Configuration.instance[Configuration.INTEGRATIONS] = Configuration.instance.get(
            Configuration.INTEGRATIONS, {}
        )
        Configuration.instance[Configuration.INTEGRATIONS][ExternalIntegration.CDN] = {}
        return DatabaseFixture(engine, connection, old_data_dir, tmp_data_dir)

    def close(self):
        # Destroy the database connection and engine.
        self._connection.close()
        self._engine.dispose()

        if self._tmp_data_dir.startswith("/tmp"):
            logging.debug("Removing temporary directory %s" % self._tmp_data_dir)
            shutil.rmtree(self._tmp_data_dir)
        else:
            logging.warning(
                "Cowardly refusing to remove 'temporary' directory %s"
                % self._tmp_data_dir
            )
        Configuration.instance[Configuration.DATA_DIRECTORY] = self._old_data_dir

    def connection(self) -> Connection:
        return self._connection


class DatabaseTransactionFixture:
    """A fixture representing a single transaction. The transaction is automatically rolled back."""

    _database: DatabaseFixture
    _default_library: Optional[Library]
    _default_collection: Optional[Collection]
    _session: Session
    _transaction: Transaction

    def __init__(
        self, database: DatabaseFixture, session: Session, transaction: Transaction
    ):
        self._database = database
        self._session = session
        self._transaction = transaction
        self._default_library = None
        self._default_collection = None

    def _make_default_library(self):
        """Ensure that the default library exists in the given database."""
        library, ignore = get_one_or_create(
            self._session,
            Library,
            create_method_kwargs=dict(
                uuid=str(uuid.uuid4()),
                name="default",
            ),
            short_name="default",
        )
        collection, ignore = get_one_or_create(
            self._session, Collection, name="Default Collection"
        )
        integration = collection.create_external_integration(
            ExternalIntegration.OPDS_IMPORT
        )
        integration.goal = ExternalIntegration.LICENSE_GOAL
        if collection not in library.collections:
            library.collections.append(collection)
        return library

    @staticmethod
    def create(database: DatabaseFixture) -> "DatabaseTransactionFixture":
        # Create a new connection to the database.
        session = Session(database.connection())
        transaction = database.connection().begin_nested()
        return DatabaseTransactionFixture(database, session, transaction)

    def close(self):
        # Close the session.
        self._session.close()

        # Roll back all database changes that happened during this
        # test, whether in the session that was just closed or some
        # other session.
        self._transaction.rollback()

        # Reset the Analytics singleton between tests.
        Analytics._reset_singleton_instance()

        # Also roll back any record of those changes in the
        # Configuration instance.
        for key in [
            Configuration.SITE_CONFIGURATION_LAST_UPDATE,
            Configuration.LAST_CHECKED_FOR_SITE_CONFIGURATION_UPDATE,
        ]:
            if key in Configuration.instance:
                del Configuration.instance[key]

    def transaction(self) -> Transaction:
        return self._transaction

    def session(self) -> Session:
        return self._session

    def default_collection(self):
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

    def default_library(self):
        """A Library that will only be created once throughout a given test.

        By default, the `default_collection()` will be associated with
        the default library.
        """
        if not self._default_library:
            self._default_library = self._make_default_library()

        return self._default_library


@pytest.fixture(scope="session")
def application() -> Iterable[ApplicationFixture]:
    app = ApplicationFixture.create()
    yield app
    app.close()


@pytest.fixture(scope="session")
def database(application: ApplicationFixture) -> Iterable[DatabaseFixture]:
    db = DatabaseFixture.create()
    yield db
    db.close()


@pytest.fixture(scope="function")
def database_transaction(
    database: DatabaseFixture,
) -> Iterable[DatabaseTransactionFixture]:
    tr = DatabaseTransactionFixture.create(database)
    yield tr
    tr.close()

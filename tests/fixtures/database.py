import importlib
import logging
import os
import shutil
import tempfile
import time
import uuid
from typing import Callable, Generator, Iterable, List, Optional, Tuple

import pytest
import sqlalchemy
from sqlalchemy.engine import Connection, Engine, Transaction
from sqlalchemy.orm import Session

import core.lane
from core.analytics import Analytics
from core.classifier import Classifier
from core.config import Configuration
from core.integration.goals import Goals
from core.log import LogConfiguration
from core.model import (
    Base,
    Classification,
    Collection,
    Contributor,
    CoverageRecord,
    Credential,
    CustomList,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    ExternalIntegrationLink,
    Genre,
    Hyperlink,
    Identifier,
    IntegrationClient,
    Library,
    LicensePool,
    MediaTypes,
    Patron,
    Representation,
    RightsStatus,
    SessionManager,
    Subject,
    Work,
    WorkCoverageRecord,
    create,
    get_one_or_create,
)
from core.model.devicetokens import DeviceToken
from core.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from core.model.licensing import License, LicensePoolDeliveryMechanism, LicenseStatus
from core.util.datetime_helpers import utc_now
from tests.fixtures.api_config import KeyPairFixture


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

    def __init__(self, engine: Engine, connection: Connection):
        self._engine = engine
        self._connection = connection

    @staticmethod
    def _get_database_connection() -> Tuple[Engine, Connection]:
        url = Configuration.database_url()
        engine, connection = SessionManager.initialize(url)
        return engine, connection

    @staticmethod
    def _load_core_model_classes():
        # Load all the core model classes so that they are registered with the ORM.
        import core.model

        importlib.reload(core.model)

    @staticmethod
    def create() -> "DatabaseFixture":
        DatabaseFixture._load_core_model_classes()
        engine, connection = DatabaseFixture._get_database_connection()

        return DatabaseFixture(engine, connection)

    def close(self):
        # Destroy the database connection and engine.
        self._connection.close()
        self._engine.dispose()

    @property
    def connection(self) -> Connection:
        return self._connection


class DatabaseTransactionFixture:
    """A fixture representing a single transaction. The transaction is automatically rolled back."""

    _database: DatabaseFixture
    _default_library: Optional[Library]
    _default_collection: Optional[Collection]
    _session: Session
    _transaction: Transaction
    _counter: int
    _isbns: List[str]

    def __init__(
        self, database: DatabaseFixture, session: Session, transaction: Transaction
    ):
        self._database = database
        self._session = session
        self._transaction = transaction
        self._default_library = None
        self._default_collection = None
        self._counter = 2000
        self._isbns = [
            "9780674368279",
            "0636920028468",
            "9781936460236",
            "9780316075978",
        ]

    def _make_default_library(self) -> Library:
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
        session = SessionManager.session_from_connection(database.connection)

        transaction = database.connection.begin_nested()
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
        self, name: Optional[str] = None, short_name: Optional[str] = None
    ) -> Library:
        name = name or self.fresh_str()
        short_name = short_name or self.fresh_str()
        library, ignore = get_one_or_create(
            self.session,
            Library,
            name=name,
            short_name=short_name,
            create_method_kwargs=dict(uuid=str(uuid.uuid4())),
        )
        return library

    def collection(
        self,
        name=None,
        protocol=ExternalIntegration.OPDS_IMPORT,
        external_account_id=None,
        url=None,
        username=None,
        password=None,
        data_source_name=None,
    ) -> Collection:
        name = name or self.fresh_str()
        collection, ignore = get_one_or_create(self.session, Collection, name=name)
        collection.external_account_id = external_account_id
        integration = collection.create_external_integration(protocol)
        integration.goal = ExternalIntegration.LICENSE_GOAL
        integration.url = url
        integration.username = username
        integration.password = password

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
        self_hosted=False,
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
                self_hosted=self_hosted,
                unlimited_access=unlimited_access,
            )
            if with_license_pool:
                presentation_edition, pool = presentation_edition
                if with_open_access_download:
                    pool.open_access = True
                if self_hosted:
                    pool.open_access = False
                    pool.self_hosted = True
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
            work.calculate_opds_entries(verbose=False)

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
        self_hosted=False,
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
                self_hosted=self_hosted,
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
        self_hosted=False,
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
            self_hosted=self_hosted,
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
            core.lane.Lane,
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

    def integration_client(self, url=None, shared_secret=None) -> IntegrationClient:
        url = url or self.fresh_url()
        secret = shared_secret or "secret"
        return get_one_or_create(
            self.session,
            IntegrationClient,
            shared_secret=secret,
            create_method_kwargs=dict(url=url),
        )[0]

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

    def external_integration(
        self, protocol, goal=None, settings=None, libraries=None, **kwargs
    ) -> ExternalIntegration:
        integration = None
        if not libraries:
            integration, ignore = get_one_or_create(
                self.session, ExternalIntegration, protocol=protocol, goal=goal
            )
        else:
            if not isinstance(libraries, list):
                libraries = [libraries]

            # Try to find an existing integration for one of the given
            # libraries.
            for library in libraries:
                integration = ExternalIntegration.lookup(
                    self.session, protocol, goal, library=libraries[0]
                )
                if integration:
                    break

            if not integration:
                # Otherwise, create a brand new integration specifically
                # for the library.
                integration = ExternalIntegration(
                    protocol=protocol,
                    goal=goal,
                )
                integration.libraries.extend(libraries)
                self.session.add(integration)

        for attr, value in list(kwargs.items()):
            setattr(integration, attr, value)

        settings = settings or dict()
        for key, value in list(settings.items()):
            integration.set_setting(key, value)

        return integration

    def external_integration_link(
        self,
        integration=None,
        library=None,
        other_integration=None,
        purpose="covers_mirror",
    ):
        integration = integration or self.external_integration("some protocol")
        other_integration = other_integration or self.external_integration(
            "some other protocol"
        )

        library_id = library.id if library else None

        external_integration_link, ignore = get_one_or_create(
            self.session,
            ExternalIntegrationLink,
            library_id=library_id,
            external_integration_id=integration.id,
            other_integration_id=other_integration.id,
            purpose=purpose,
        )

        return external_integration_link

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
        work.calculate_opds_entries(verbose=False)
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


class TemporaryDirectoryConfigurationFixture:
    """A fixture that configures the Configuration system to use a temporary directory.
    The directory is cleaned up when the fixture is closed."""

    _directory: str

    @classmethod
    def create(cls) -> "TemporaryDirectoryConfigurationFixture":
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
def temporary_directory_configuration() -> Iterable[
    TemporaryDirectoryConfigurationFixture
]:
    fix = TemporaryDirectoryConfigurationFixture.create()
    yield fix
    fix.close()


@pytest.fixture(scope="session")
def application(mock_config_key_pair: KeyPairFixture) -> Iterable[ApplicationFixture]:
    app = ApplicationFixture.create()
    yield app
    app.close()


@pytest.fixture(scope="session")
def database(application: ApplicationFixture) -> Iterable[DatabaseFixture]:
    db = DatabaseFixture.create()
    yield db
    db.close()


@pytest.fixture(scope="function")
def db(
    database: DatabaseFixture,
) -> Generator[DatabaseTransactionFixture, None, None]:
    tr = DatabaseTransactionFixture.create(database)
    yield tr
    tr.close()


@pytest.fixture
def default_library(db: DatabaseTransactionFixture) -> Library:
    return db.default_library()


@pytest.fixture
def create_integration_configuration(
    db: DatabaseTransactionFixture,
) -> Callable[..., IntegrationConfiguration]:
    def create_integration(
        protocol: str, goal: Goals, settings: Optional[dict] = None
    ) -> IntegrationConfiguration:
        integration, _ = create(
            db.session,
            IntegrationConfiguration,
            name=db.fresh_str(),
            protocol=protocol,
            goal=goal,
            settings=settings or {},
        )
        return integration

    return create_integration


@pytest.fixture
def create_integration_library_configuration(
    db: DatabaseTransactionFixture,
) -> Callable[..., IntegrationLibraryConfiguration]:
    def create_library_integration(
        library: Library,
        parent: IntegrationConfiguration,
        settings: Optional[dict] = None,
    ) -> IntegrationLibraryConfiguration:
        settings = settings or {}
        integration, _ = create(
            db.session,
            IntegrationLibraryConfiguration,
            parent=parent,
            library=library,
            settings=settings,
        )
        return integration

    return create_library_integration


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

import logging
import os
import uuid
from datetime import timedelta
from pathlib import Path
from unittest import mock

import pytest
from sqlalchemy.orm import Session

from core import external_search
from core.analytics import Analytics
from core.classifier import Classifier
from core.config import Configuration
from core.lane import Lane
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
    DelegatedPatronIdentifier,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Genre,
    Hyperlink,
    Identifier,
    IntegrationClient,
    Library,
    License,
    LicensePool,
    LicensePoolDeliveryMechanism,
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
from core.model.configuration import ExternalIntegrationLink
from core.model.constants import MediaTypes
from core.model.devicetokens import DeviceToken
from core.model.licensing import LicenseStatus
from core.util.datetime_helpers import datetime_utc, utc_now


class DatabaseTest:

    engine = None
    connection = None

    @classmethod
    def get_database_connection(cls):
        url = Configuration.database_url()
        engine, connection = SessionManager.initialize(url)

        return engine, connection

    @classmethod
    def setup_class(cls):
        # Initialize a temporary data directory.
        cls.engine, cls.connection = cls.get_database_connection()

    @classmethod
    def teardown_class(cls):
        # Destroy the database connection and engine.
        cls.connection.close()
        cls.engine.dispose()

    @pytest.fixture(autouse=True)
    def search_mock(self, request):
        # Only setup the opensearch mock if the opensearch mark isn't set
        opensearch_mark = request.node.get_closest_marker("opensearch")
        if opensearch_mark is not None:
            self.search_mock = None
        else:
            self.search_mock = mock.patch(
                external_search.__name__ + ".ExternalSearchIndex",
                external_search.MockExternalSearchIndex,
            )
            self.search_mock.start()
        yield
        if self.search_mock:
            self.search_mock.stop()

    def setup_method(self):
        # Create a new connection to the database.
        self._db = Session(self.connection)
        self.transaction = self.connection.begin_nested()

        # Start with a high number so it won't interfere with tests that search for an age or grade
        self.counter = 2000

        self.time_counter = datetime_utc(2014, 1, 1)
        self.isbns = [
            "9780674368279",
            "0636920028468",
            "9781936460236",
            "9780316075978",
        ]

    def teardown_method(self):
        # Close the session.
        self._db.close()

        # Roll back all database changes that happened during this
        # test, whether in the session that was just closed or some
        # other session.
        self.transaction.rollback()

        # Reset the Analytics singleton between tests.
        Analytics._reset_singleton_instance()

        Configuration.SITE_CONFIGURATION_LAST_UPDATE = None
        Configuration.LAST_CHECKED_FOR_SITE_CONFIGURATION_UPDATE = None

    def time_eq(self, a, b):
        "Assert that two times are *approximately* the same -- within 2 seconds."
        if a < b:
            delta = b - a
        else:
            delta = a - b
        total_seconds = delta.total_seconds()
        assert total_seconds < 2, "Delta was too large: %.2f seconds." % total_seconds

    def shortDescription(self):
        return None  # Stop nosetests displaying docstrings instead of class names when verbosity level >= 2.

    @property
    def _id(self):
        self.counter += 1
        return self.counter

    @property
    def _str(self):
        return str(self._id)

    @property
    def _time(self):
        v = self.time_counter
        self.time_counter = self.time_counter + timedelta(days=1)
        return v

    @property
    def _isbn(self):
        return self.isbns.pop()

    @property
    def _url(self):
        return "http://foo.com/" + self._str

    def _patron(self, external_identifier=None, library=None):
        external_identifier = external_identifier or self._str
        library = library or self._default_library
        return get_one_or_create(
            self._db, Patron, external_identifier=external_identifier, library=library
        )[0]

    def _contributor(self, sort_name=None, name=None, **kw_args):
        name = sort_name or name or self._str
        return get_one_or_create(self._db, Contributor, sort_name=str(name), **kw_args)

    def _identifier(self, identifier_type=Identifier.GUTENBERG_ID, foreign_id=None):
        if foreign_id:
            id = foreign_id
        else:
            id = self._str
        return Identifier.for_foreign_id(self._db, identifier_type, id)[0]

    def _edition(
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
        id = identifier_id or self._str
        source = DataSource.lookup(self._db, data_source_name)
        wr = Edition.for_foreign_id(self._db, source, identifier_type, id)[0]
        if not title:
            title = self._str
        wr.title = str(title)
        wr.medium = Edition.BOOK_MEDIUM
        if series:
            wr.series = series
        if language:
            wr.language = language
        if authors is None:
            authors = self._str
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
            pool = self._licensepool(
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

    def _work(
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
        title = str(title or self._str)
        audience = audience or Classifier.AUDIENCE_ADULT
        if audience == Classifier.AUDIENCE_CHILDREN and not data_source_name:
            # TODO: This is necessary because Gutenberg's childrens books
            # get filtered out at the moment.
            data_source_name = DataSource.OVERDRIVE
        elif not data_source_name:
            data_source_name = DataSource.GUTENBERG
        if fiction is None:
            fiction = True
        new_edition = False
        if not presentation_edition:
            new_edition = True
            presentation_edition = self._edition(
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
            self._db,
            Work,
            create_method_kwargs=dict(
                audience=audience, fiction=fiction, quality=quality
            ),
            id=self._id,
        )
        if genre:
            if not isinstance(genre, Genre):
                genre, ignore = Genre.lookup(self._db, genre, autocreate=True)
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

    def _lane(
        self,
        display_name=None,
        library=None,
        parent=None,
        genres=None,
        languages=None,
        fiction=None,
        inherit_parent_restrictions=True,
    ):
        display_name = display_name or self._str
        library = library or self._default_library
        lane, is_new = create(
            self._db,
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
                    genre, ignore = Genre.lookup(self._db, genre)
                lane.genres.append(genre)
        if languages:
            if not isinstance(languages, list):
                languages = [languages]
            lane.languages = languages
        return lane

    def _slow_work(self, *args, **kwargs):
        """Create a work that closely resembles one that might be found in the
        wild.

        This is significantly slower than _work() but more reliable.
        """
        work = self._work(*args, **kwargs)
        work.calculate_presentation_edition()
        work.calculate_opds_entries(verbose=False)
        return work

    def _add_generic_delivery_mechanism(self, license_pool):
        """Give a license pool a generic non-open-access delivery mechanism."""
        data_source = license_pool.data_source
        identifier = license_pool.identifier
        content_type = Representation.EPUB_MEDIA_TYPE
        drm_scheme = DeliveryMechanism.NO_DRM
        return LicensePoolDeliveryMechanism.set(
            data_source, identifier, content_type, drm_scheme, RightsStatus.IN_COPYRIGHT
        )

    def _coverage_record(
        self,
        edition,
        coverage_source,
        operation=None,
        status=CoverageRecord.SUCCESS,
        collection=None,
        exception=None,
    ):
        if isinstance(edition, Identifier):
            identifier = edition
        else:
            identifier = edition.primary_identifier
        record, ignore = get_one_or_create(
            self._db,
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

    def _work_coverage_record(
        self, work, operation=None, status=CoverageRecord.SUCCESS
    ):
        record, ignore = get_one_or_create(
            self._db,
            WorkCoverageRecord,
            work=work,
            operation=operation,
            create_method_kwargs=dict(
                timestamp=utc_now(),
                status=status,
            ),
        )
        return record

    def _licensepool(
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
        source = DataSource.lookup(self._db, data_source_name)
        if not edition:
            edition = self._edition(data_source_name)
        collection = collection or self._default_collection
        pool, ignore = get_one_or_create(
            self._db,
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
            url = "http://foo.com/" + self._str
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

            representation, is_new = self._representation(
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

    def _license(
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
    ):
        identifier = identifier or self._str
        checkout_url = checkout_url or self._str
        status_url = status_url or self._str
        license, ignore = get_one_or_create(
            self._db,
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

    def _representation(self, url=None, media_type=None, content=None, mirrored=False):
        url = url or "http://foo.com/" + self._str
        repr, is_new = get_one_or_create(self._db, Representation, url=url)
        repr.media_type = media_type
        if media_type and content:
            if isinstance(content, str):
                content = content.encode("utf8")
            repr.content = content
            repr.fetched_at = utc_now()
            if mirrored:
                repr.mirror_url = "http://foo.com/" + self._str
                repr.mirrored_at = utc_now()
        return repr, is_new

    def _customlist(
        self,
        foreign_identifier=None,
        name=None,
        data_source_name=DataSource.NYT,
        num_entries=1,
        entries_exist_as_works=True,
    ):
        data_source = DataSource.lookup(self._db, data_source_name)
        foreign_identifier = foreign_identifier or self._str
        now = utc_now()
        customlist, ignore = get_one_or_create(
            self._db,
            CustomList,
            create_method_kwargs=dict(
                created=now,
                updated=now,
                name=name or self._str,
                description=self._str,
            ),
            data_source=data_source,
            foreign_identifier=foreign_identifier,
        )

        editions = []
        for i in range(num_entries):
            if entries_exist_as_works:
                work = self._work(with_open_access_download=True)
                edition = work.presentation_edition
            else:
                edition = self._edition(data_source_name, title="Item %s" % i)
                edition.permanent_work_id = "Permanent work ID %s" % self._str
            customlist.add_entry(edition, "Annotation %s" % i, first_appearance=now)
            editions.append(edition)
        return customlist, editions

    def _credential(
        self, data_source_name=DataSource.GUTENBERG, type=None, patron=None
    ):
        data_source = DataSource.lookup(self._db, data_source_name)
        type = type or self._str
        patron = patron or self._patron()
        credential, is_new = Credential.persistent_token_create(
            self._db, data_source, type, patron
        )
        return credential

    def _external_integration(
        self, protocol, goal=None, settings=None, libraries=None, **kwargs
    ):
        integration = None
        if not libraries:
            integration, ignore = get_one_or_create(
                self._db, ExternalIntegration, protocol=protocol, goal=goal
            )
        else:
            if not isinstance(libraries, list):
                libraries = [libraries]

            # Try to find an existing integration for one of the given
            # libraries.
            for library in libraries:
                integration = ExternalIntegration.lookup(
                    self._db, protocol, goal, library=libraries[0]
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
                self._db.add(integration)

        for attr, value in list(kwargs.items()):
            setattr(integration, attr, value)

        settings = settings or dict()
        for key, value in list(settings.items()):
            integration.set_setting(key, value)

        return integration

    def _external_integration_link(
        self,
        integration=None,
        library=None,
        other_integration=None,
        purpose="covers_mirror",
    ):

        integration = integration or self._external_integration("some protocol")
        other_integration = other_integration or self._external_integration(
            "some other protocol"
        )

        library_id = library.id if library else None

        external_integration_link, ignore = get_one_or_create(
            self._db,
            ExternalIntegrationLink,
            library_id=library_id,
            external_integration_id=integration.id,
            other_integration_id=other_integration.id,
            purpose=purpose,
        )

        return external_integration_link

    def _delegated_patron_identifier(
        self,
        library_uri=None,
        patron_identifier=None,
        identifier_type=DelegatedPatronIdentifier.ADOBE_ACCOUNT_ID,
        identifier=None,
    ):
        """Create a sample DelegatedPatronIdentifier"""
        library_uri = library_uri or self._url
        patron_identifier = patron_identifier or self._str
        if callable(identifier):
            make_id = identifier
        else:
            if not identifier:
                identifier = self._str

            def make_id():
                return identifier

        patron, is_new = DelegatedPatronIdentifier.get_one_or_create(
            self._db, library_uri, patron_identifier, identifier_type, make_id
        )
        return patron

    def _sample_ecosystem(self):
        """Creates an ecosystem of some sample work, pool, edition, and author
        objects that all know each other.
        """
        # make some authors
        [bob], ignore = Contributor.lookup(self._db, "Bitshifter, Bob")
        bob.family_name, bob.display_name = bob.default_names()
        [alice], ignore = Contributor.lookup(self._db, "Adder, Alice")
        alice.family_name, alice.display_name = alice.default_names()

        edition_std_ebooks, pool_std_ebooks = self._edition(
            DataSource.STANDARD_EBOOKS,
            Identifier.URI,
            with_license_pool=True,
            with_open_access_download=True,
            authors=[],
        )
        edition_std_ebooks.title = "The Standard Ebooks Title"
        edition_std_ebooks.subtitle = "The Standard Ebooks Subtitle"
        edition_std_ebooks.add_contributor(alice, Contributor.AUTHOR_ROLE)

        edition_git, pool_git = self._edition(
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

        edition_gut, pool_gut = self._edition(
            DataSource.GUTENBERG,
            Identifier.GUTENBERG_ID,
            with_license_pool=True,
            with_open_access_download=True,
            authors=[],
        )
        edition_gut.title = "The GUtenberg Title"
        edition_gut.subtitle = "The GUtenberg Subtitle"
        edition_gut.add_contributor(bob, Contributor.AUTHOR_ROLE)

        work = self._work(presentation_edition=edition_git)

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

    def print_database_instance(self):
        """
        Calls the class method that examines the current state of the database model
        (whether it's been committed or not).

        NOTE: If you set_trace, and hit "continue", you'll start seeing console output right
        away, without waiting for the whole test to run and the standard output section to display.
        You can also use nosetest --nocapture.

        I use::

            def test_name(self):
                [code...]
                set_trace()
                self.print_database_instance()  # TODO: remove before prod
                [code...]
        """
        if not "TESTING" in os.environ:
            # we are on production, abort, abort!
            logging.warning(
                "Forgot to remove call to testing.py:DatabaseTest.print_database_instance() before pushing to production."
            )
            return

        DatabaseTest.print_database_class(self._db)
        return

    @classmethod
    def print_database_class(cls, db_connection):
        """
        Prints to the console the entire contents of the database, as the unit test sees it.
        Exists because unit tests don't persist db information, they create a memory
        representation of the db state, and then roll the unit test-derived transactions back.
        So we cannot see what's going on by going into postgres and running selects.
        This is the in-test alternative to going into postgres.

        Can be called from model and metadata classes as well as tests.

        NOTE: The purpose of this method is for debugging.
        Be careful of leaving it in code and potentially outputting
        vast tracts of data into your output stream on production.

        Call like this::

            set_trace()
            from testing import (l=
                DatabaseTest,
            )
            _db = Session.object_session(self)
            DatabaseTest.print_database_class(_db)

            TODO: remove before prod
        """
        if not "TESTING" in os.environ:
            # we are on production, abort, abort!
            logging.warning(
                "Forgot to remove call to testing.py:DatabaseTest.print_database_class() before pushing to production."
            )
            return

        works = db_connection.query(Work).all()
        identifiers = db_connection.query(Identifier).all()
        license_pools = db_connection.query(LicensePool).all()
        editions = db_connection.query(Edition).all()
        data_sources = db_connection.query(DataSource).all()
        representations = db_connection.query(Representation).all()

        if not works:
            print("NO Work found")
        for wCount, work in enumerate(works):
            # pipe character at end of line helps see whitespace issues
            print(f"Work[{wCount}]={work}|")

            if not work.license_pools:
                print("    NO Work.LicensePool found")
            for lpCount, license_pool in enumerate(work.license_pools):
                print(f"    Work.LicensePool[{lpCount}]={license_pool}|")

            print("    Work.presentation_edition=%s|" % work.presentation_edition)

        print("__________________________________________________________________\n")
        if not identifiers:
            print("NO Identifier found")
        for iCount, identifier in enumerate(identifiers):
            print(f"Identifier[{iCount}]={identifier}|")
            print("    Identifier.licensed_through=%s|" % identifier.licensed_through)

        print("__________________________________________________________________\n")
        if not license_pools:
            print("NO LicensePool found")
        for index, license_pool in enumerate(license_pools):
            print(f"LicensePool[{index}]={license_pool}|")
            print("    LicensePool.work_id=%s|" % license_pool.work_id)
            print("    LicensePool.data_source_id=%s|" % license_pool.data_source_id)
            print("    LicensePool.identifier_id=%s|" % license_pool.identifier_id)
            print(
                "    LicensePool.presentation_edition_id=%s|"
                % license_pool.presentation_edition_id
            )
            print("    LicensePool.superceded=%s|" % license_pool.superceded)
            print("    LicensePool.suppressed=%s|" % license_pool.suppressed)

        print("__________________________________________________________________\n")
        if not editions:
            print("NO Edition found")
        for index, edition in enumerate(editions):
            # pipe character at end of line helps see whitespace issues
            print(f"Edition[{index}]={edition}|")
            print(
                "    Edition.primary_identifier_id=%s|" % edition.primary_identifier_id
            )
            print("    Edition.permanent_work_id=%s|" % edition.permanent_work_id)
            if edition.data_source:
                print("    Edition.data_source.id=%s|" % edition.data_source.id)
                print("    Edition.data_source.name=%s|" % edition.data_source.name)
            else:
                print("    No Edition.data_source.")
            if edition.license_pool:
                print("    Edition.license_pool.id=%s|" % edition.license_pool.id)
            else:
                print("    No Edition.license_pool.")

            print("    Edition.title=%s|" % edition.title)
            print("    Edition.author=%s|" % edition.author)
            if not edition.author_contributors:
                print("    NO Edition.author_contributor found")
            for acCount, author_contributor in enumerate(edition.author_contributors):
                print(
                    "    Edition.author_contributor[%s]=%s|"
                    % (acCount, author_contributor)
                )

        print("__________________________________________________________________\n")
        if not data_sources:
            print("NO DataSource found")
        for index, data_source in enumerate(data_sources):
            print(f"DataSource[{index}]={data_source}|")
            print("    DataSource.id=%s|" % data_source.id)
            print("    DataSource.name=%s|" % data_source.name)
            print("    DataSource.offers_licenses=%s|" % data_source.offers_licenses)
            print("    DataSource.editions=%s|" % data_source.editions)
            print("    DataSource.license_pools=%s|" % data_source.license_pools)
            print("    DataSource.links=%s|" % data_source.links)

        print("__________________________________________________________________\n")
        if not representations:
            print("NO Representation found")
        for index, representation in enumerate(representations):
            print(f"Representation[{index}]={representation}|")
            print("    Representation.id=%s|" % representation.id)
            print("    Representation.url=%s|" % representation.url)
            print("    Representation.mirror_url=%s|" % representation.mirror_url)
            print(
                "    Representation.fetch_exception=%s|"
                % representation.fetch_exception
            )
            print(
                "    Representation.mirror_exception=%s|"
                % representation.mirror_exception
            )

        return

    def _library(self, name=None, short_name=None):
        name = name or self._str
        short_name = short_name or self._str
        library, ignore = get_one_or_create(
            self._db,
            Library,
            name=name,
            short_name=short_name,
            create_method_kwargs=dict(uuid=str(uuid.uuid4())),
        )
        return library

    def _collection(
        self,
        name=None,
        protocol=ExternalIntegration.OPDS_IMPORT,
        external_account_id=None,
        url=None,
        username=None,
        password=None,
        data_source_name=None,
    ):
        name = name or self._str
        collection, ignore = get_one_or_create(self._db, Collection, name=name)
        collection.external_account_id = external_account_id
        integration = collection.create_external_integration(protocol)
        integration.goal = ExternalIntegration.LICENSE_GOAL
        integration.url = url
        integration.username = username
        integration.password = password

        if data_source_name:
            collection.data_source = data_source_name
        return collection

    @property
    def _default_library(self):
        """A Library that will only be created once throughout a given test.

        By default, the `_default_collection` will be associated with
        the default library.
        """
        if not hasattr(self, "_default__library"):
            self._default__library = self.make_default_library(self._db)
        return self._default__library

    @property
    def _default_collection(self):
        """A Collection that will only be created once throughout
        a given test.

        For most tests there's no need to create a different
        Collection for every LicensePool. Using
        self._default_collection instead of calling self.collection()
        saves time.
        """
        if not hasattr(self, "_default__collection"):
            self._default__collection = self._default_library.collections[0]
        return self._default__collection

    @classmethod
    def make_default_library(cls, _db):
        """Ensure that the default library exists in the given database.

        This can be called by code intended for use in testing but not actually
        within a DatabaseTest subclass.
        """
        library, ignore = get_one_or_create(
            _db,
            Library,
            create_method_kwargs=dict(
                uuid=str(uuid.uuid4()),
                name="default",
            ),
            short_name="default",
        )
        collection, ignore = get_one_or_create(
            _db, Collection, name="Default Collection"
        )
        integration = collection.create_external_integration(
            ExternalIntegration.OPDS_IMPORT
        )
        integration.goal = ExternalIntegration.LICENSE_GOAL
        if collection not in library.collections:
            library.collections.append(collection)
        return library

    def _catalog(self, name="Faketown Public Library"):
        source, ignore = get_one_or_create(self._db, DataSource, name=name)

    def _integration_client(self, url=None, shared_secret=None):
        url = url or self._url
        secret = shared_secret or "secret"
        return get_one_or_create(
            self._db,
            IntegrationClient,
            shared_secret=secret,
            create_method_kwargs=dict(url=url),
        )[0]

    def _subject(self, type, identifier):
        return get_one_or_create(self._db, Subject, type=type, identifier=identifier)[0]

    def _classification(self, identifier, subject, data_source, weight=1):
        return get_one_or_create(
            self._db,
            Classification,
            identifier=identifier,
            subject=subject,
            data_source=data_source,
            weight=weight,
        )[0]

    def sample_cover_path(self, name):
        """The path to the sample cover with the given filename."""
        base_path = Path(__file__).parent.parent
        resource_path = base_path / "tests" / "core" / "files" / "covers"
        sample_cover_path = resource_path / name
        return str(sample_cover_path)

    def sample_cover_representation(self, name):
        """A Representation of the sample cover with the given filename."""
        sample_cover_path = self.sample_cover_path(name)
        return self._representation(
            media_type="image/png", content=open(sample_cover_path, "rb").read()
        )[0]


@pytest.fixture(autouse=True, scope="session")
def session_fixture():
    # This will make sure we always connect to the test database.
    os.environ["TESTING"] = "true"

    # Ensure that the log configuration starts in a known state.
    LogConfiguration.initialize(None, testing=True)

    # Drop any existing schema. It will be recreated when
    # SessionManager.initialize() runs.
    engine = SessionManager.engine()
    # Trying to drop all tables without reflecting first causes an issue
    # since SQLAchemy does not know the order of cascades
    # Adding .reflect is throwing an error locally becuase tables are imported
    # later and hence being defined twice
    # Deleting the problematic table first fixes the issue, in this case DeviceToken
    DeviceToken.__table__.drop(engine, checkfirst=True)
    Base.metadata.drop_all(engine)

    yield

    if "TESTING" in os.environ:
        del os.environ["TESTING"]

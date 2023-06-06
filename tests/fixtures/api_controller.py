from __future__ import annotations

import datetime
import json
import logging
from contextlib import contextmanager
from typing import Any, Callable

import flask
import pytest
from werkzeug.datastructures import Authorization

from api.adobe_vendor_id import AuthdataUtility
from api.app import app
from api.config import Configuration
from api.controller import CirculationManager, CirculationManagerController
from api.integration.registry.patron_auth import PatronAuthRegistry
from api.lanes import create_default_lanes
from api.simple_authentication import SimpleAuthenticationProvider
from api.util.flask import PalaceFlask
from core.entrypoint import AudiobooksEntryPoint, EbooksEntryPoint, EntryPoint
from core.integration.goals import Goals
from core.lane import Lane
from core.model import (
    Collection,
    ConfigurationSetting,
    Library,
    Patron,
    Session,
    Work,
    create,
    get_one_or_create,
)
from core.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from core.util.string_helpers import base64
from tests.api.mockapi.circulation import MockCirculationManager
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.vendor_id import VendorIDFixture


class ControllerFixtureSetupOverrides:
    make_default_libraries: Callable[[Session], list[Library]]
    make_default_collection: Callable[[Session, Library], Collection]

    def __init__(
        self,
        make_default_libraries: Callable[[Session], list[Library]],
        make_default_collection: Callable[[Session, Library], Collection],
    ):
        self.make_default_libraries = make_default_libraries
        self.make_default_collection = make_default_collection


class ControllerFixture:
    """A test that requires a functional app server."""

    app: PalaceFlask
    authdata: AuthdataUtility
    collection: Collection
    collections: list[Collection]
    controller: CirculationManagerController
    db: DatabaseTransactionFixture
    default_patron: Patron
    default_patrons: dict[Any, Any]
    english_adult_fiction: Lane
    libraries: list[Library]
    library: Library
    manager: CirculationManager
    vendor_ids: VendorIDFixture

    # Authorization headers that will succeed (or fail) against the
    # SimpleAuthenticationProvider set up in ControllerTest.setup().
    valid_auth = "Basic " + base64.b64encode("unittestuser:unittestpassword")
    invalid_auth = "Basic " + base64.b64encode("user1:password2")
    valid_credentials = Authorization(
        auth_type="basic",
        data=dict(username="unittestuser", password="unittestpassword"),
    )

    def __init__(
        self,
        db: DatabaseTransactionFixture,
        vendor_id_fixture: VendorIDFixture,
        setup_cm: bool,
    ):
        self.vendor_ids = vendor_id_fixture
        self.db = db
        self.app = app
        self.patron_auth_registry = PatronAuthRegistry()

        # PRESERVE_CONTEXT_ON_EXCEPTION needs to be off in tests
        # to prevent one test failure from breaking later tests as well.
        # When used with flask's test_request_context, exceptions
        # from previous tests would cause flask to roll back the db
        # when you entered a new request context, deleting rows that
        # were created in the test setup.
        app.config["PRESERVE_CONTEXT_ON_EXCEPTION"] = False

        if setup_cm:
            # NOTE: Any reference to self._default_library below this
            # point in this method will cause the tests in
            # TestScopedSession to hang.
            self.set_base_url()
            app.manager = self.circulation_manager_setup()

    def set_base_url(self):
        base_url = ConfigurationSetting.sitewide(
            self.db.session, Configuration.BASE_URL_KEY
        )
        base_url.value = "http://test-circulation-manager/"

    def circulation_manager_setup_with_session(
        self, session: Session, overrides: ControllerFixtureSetupOverrides | None = None
    ) -> CirculationManager:
        """Set up initial Library arrangements for this test.

        Most tests only need one library: self._default_library.
        Other tests need a different library (e.g. one created using the
        scoped database session), or more than one library. For that
        reason we call out to a helper method to create some number of
        libraries, then initialize each one.

        NOTE: Any reference to self._default_library within this
        method will cause the tests in TestScopedSession to hang.

        This method sets values for self.libraries, self.collections,
        and self.default_patrons. These data structures contain
        information for all libraries. It also sets values for a
        single library which can be used as a default: .library,
        .collection, and .default_patron.

        :param: The setup function overrides
        :return: a CirculationManager object.

        """
        setup = overrides or ControllerFixtureSetupOverrides(
            make_default_libraries=self.make_default_libraries,
            make_default_collection=self.make_default_collection,
        )

        self.libraries = setup.make_default_libraries(session)
        self.collections = [
            setup.make_default_collection(session, library)
            for library in self.libraries
        ]
        self.default_patrons = {}

        # The first library created is used as the default -- more of the
        # time this is the same as self._default_library.
        self.library = self.libraries[0]
        self.collection = self.collections[0]

        for library in self.libraries:
            self.library_setup(library)

        # The test's default patron is the default patron for the first
        # library returned by make_default_libraries.
        self.default_patron = self.default_patrons[self.library]

        self.authdata = AuthdataUtility.from_config(self.library)
        self.manager = MockCirculationManager(session)

        # Set CirculationAPI and top-level lane for the default
        # library, for convenience in tests.
        self.manager.d_circulation = self.manager.circulation_apis[self.library.id]  # type: ignore
        self.manager.d_top_level_lane = self.manager.top_level_lanes[self.library.id]  # type: ignore
        self.controller = CirculationManagerController(self.manager)

        # Set a convenient default lane.
        [self.english_adult_fiction] = [
            x
            for x in self.library.lanes  # type: ignore
            if x.display_name == "Fiction" and x.languages == ["eng"]
        ]

        return self.manager

    def circulation_manager_setup(
        self, overrides: ControllerFixtureSetupOverrides | None = None
    ) -> CirculationManager:
        return self.circulation_manager_setup_with_session(self.db.session, overrides)

    def library_setup(self, library):
        """Do some basic setup for a library newly created by test code."""
        _db = Session.object_session(library)
        # Create the patron used by the dummy authentication mechanism.
        default_patron, ignore = get_one_or_create(
            _db,
            Patron,
            library=library,
            authorization_identifier="unittestuser",
            create_method_kwargs=dict(external_identifier="unittestuser"),
        )
        self.default_patrons[library] = default_patron

        # Create a simple authentication integration for this library,
        # unless it already has a way to authenticate patrons
        # (in which case we would just screw things up).
        auth_integrations = IntegrationLibraryConfiguration.for_library_and_goal(
            _db, library, Goals.PATRON_AUTH_GOAL
        ).all()
        if len(auth_integrations) == 0:
            protocol = self.patron_auth_registry.get_protocol(
                SimpleAuthenticationProvider
            )
            settings = SimpleAuthenticationProvider.settings_class()(
                test_identifier="unittestuser",
                test_password="unittestpassword",
                neighborhood="Unit Test West",
            )
            integration, _ = create(
                _db,
                IntegrationConfiguration,
                name=self.db.fresh_str(),
                protocol=protocol,
                goal=Goals.PATRON_AUTH_GOAL,
                settings=settings.dict(),
            )
            create(
                _db,
                IntegrationLibraryConfiguration,
                library=library,
                parent=integration,
            )

        for k, v in [
            (Configuration.LARGE_COLLECTION_LANGUAGES, []),
            (Configuration.SMALL_COLLECTION_LANGUAGES, ["eng"]),
            (Configuration.TINY_COLLECTION_LANGUAGES, ["spa", "chi", "fre"]),
        ]:
            ConfigurationSetting.for_library(k, library).value = json.dumps(v)
        create_default_lanes(_db, library)

    def make_default_libraries(self, _db):
        return [self.db.default_library()]

    def make_default_collection(self, _db, library):
        return self.db.default_collection()

    @contextmanager
    def request_context_with_library(self, route, *args, **kwargs):
        if "library" in kwargs:
            library = kwargs.pop("library")
        else:
            library = self.db.default_library()
        with self.app.test_request_context(route, *args, **kwargs) as c:
            flask.request.library = library
            yield c


@pytest.fixture(scope="function")
def controller_fixture(
    db: DatabaseTransactionFixture, vendor_id_fixture: VendorIDFixture
):
    time_then = datetime.datetime.now()
    fixture = ControllerFixture(db, vendor_id_fixture, setup_cm=True)
    time_now = datetime.datetime.now()
    time_diff = time_now - time_then
    logging.info("controller init took %s", time_diff)
    yield fixture


@pytest.fixture(scope="function")
def controller_fixture_without_cm(
    db: DatabaseTransactionFixture, vendor_id_fixture: VendorIDFixture
):
    time_then = datetime.datetime.now()
    fixture = ControllerFixture(db, vendor_id_fixture, setup_cm=False)
    time_now = datetime.datetime.now()
    time_diff = time_now - time_then
    logging.info("controller init took %s", time_diff)
    yield fixture


class WorkSpec:
    variable_name: str
    title: str
    author: str
    language: str
    fiction: bool

    def __init__(
        self, variable_name: str, title: str, author: str, language: str, fiction: bool
    ):
        self.variable_name = variable_name
        self.title = title
        self.author = author
        self.language = language
        self.fiction = fiction


class CirculationControllerFixture(ControllerFixture):
    works: list[Work]
    english_1: Work

    # These tests generally need at least one Work created,
    # but some need more.

    BOOKS: list[WorkSpec] = [
        WorkSpec(
            variable_name="english_1",
            title="Quite British",
            author="John Bull",
            language="eng",
            fiction=True,
        )
    ]

    def __init__(
        self, db: DatabaseTransactionFixture, vendor_id_fixture: VendorIDFixture
    ):
        super().__init__(db, vendor_id_fixture, setup_cm=True)
        self.works = []
        self.add_works(self.BOOKS)

        # Enable the audiobook entry point for the default library -- a lot of
        # tests verify that non-default entry points can be selected.
        self.db.default_library().setting(
            EntryPoint.ENABLED_SETTING
        ).value = json.dumps(
            [EbooksEntryPoint.INTERNAL_NAME, AudiobooksEntryPoint.INTERNAL_NAME]
        )

    def add_works(self, works: list[WorkSpec]):
        """Add works to the database."""
        for spec in works:
            work = self.db.work(
                spec.title,
                spec.author,
                language=spec.language,
                fiction=spec.fiction,
                with_open_access_download=True,
            )
            setattr(self, spec.variable_name, work)
            work.license_pools[0].collection = self.collection
            self.works.append(work)
        self.manager.external_search.bulk_update(self.works)

    def assert_bad_search_index_gives_problem_detail(self, test_function):
        """Helper method to test that a controller method serves a problem
        detail document when the search index isn't set up.

        Mocking a broken search index is a lot of work; thus the helper method.
        """
        old_setup = self.manager.setup_external_search
        old_value = self.manager._external_search

        try:
            self.manager._external_search = None
            self.manager.setup_external_search = lambda: None
            with self.request_context_with_library("/"):
                response = test_function()
                assert 502 == response.status_code
                assert (
                    "http://librarysimplified.org/terms/problem/remote-integration-failed"
                    == response.uri
                )
                assert (
                    "The search index for this site is not properly configured."
                    == response.detail
                )
        finally:
            self.manager.setup_external_search = old_setup
            self.manager._external_search = old_value


@pytest.fixture(scope="function")
def circulation_fixture(
    db: DatabaseTransactionFixture, vendor_id_fixture: VendorIDFixture
):
    time_then = datetime.datetime.now()
    fixture = CirculationControllerFixture(db, vendor_id_fixture)
    time_now = datetime.datetime.now()
    time_diff = time_now - time_then
    logging.info("circulation controller init took %s", time_diff)
    yield fixture

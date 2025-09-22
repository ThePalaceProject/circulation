from __future__ import annotations

import datetime
import logging
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import Any

import pytest
from sqlalchemy.orm import Session
from typing_extensions import Self
from werkzeug.datastructures import Authorization

from palace.manager.api.adobe_vendor_id import AuthdataUtility
from palace.manager.api.app import app
from palace.manager.api.circulation_manager import CirculationManager
from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)
from palace.manager.api.lanes import create_default_lanes
from palace.manager.api.util.flask import PalaceFlask
from palace.manager.core.entrypoint import AudiobooksEntryPoint, EbooksEntryPoint
from palace.manager.integration.configuration.library import LibrarySettings
from palace.manager.integration.goals import Goals
from palace.manager.integration.patron_auth.simple_authentication import (
    SimpleAuthenticationProvider,
)
from palace.manager.service.integration_registry.patron_auth import PatronAuthRegistry
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from palace.manager.sqlalchemy.model.lane import Lane
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import create, get_one_or_create
from palace.manager.util import base64
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture
from tests.mocks.circulation import MockCirculationManager
from tests.mocks.search import ExternalSearchIndexFake


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
    authdata: AuthdataUtility | None
    collection: Collection
    collections: list[Collection]
    controller: CirculationManagerController
    db: DatabaseTransactionFixture
    default_patron: Patron
    default_patrons: dict[Any, Any]
    english_adult_fiction: Lane
    libraries: list[Library]
    library: Library
    manager: MockCirculationManager

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
        services_fixture: ServicesFixture,
    ):
        self.db = db
        self.app = app
        self.patron_auth_registry: PatronAuthRegistry = (
            services_fixture.services.integration_registry.patron_auth()
        )

        # PRESERVE_CONTEXT_ON_EXCEPTION needs to be off in tests
        # to prevent one test failure from breaking later tests as well.
        # When used with flask's test_request_context, exceptions
        # from previous tests would cause flask to roll back the db
        # when you entered a new request context, deleting rows that
        # were created in the test setup.
        app.config["PRESERVE_CONTEXT_ON_EXCEPTION"] = False

        # Set up the fake search index.
        self.services_fixture = services_fixture
        self.search_index = ExternalSearchIndexFake()
        self.services_fixture.services.search.index.override(self.search_index)

        # NOTE: Any reference to self._default_library below this
        # point in this method will cause the tests in
        # TestScopedSession to hang.
        app.manager = self.circulation_manager_setup()

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

        # Create mock CM instance
        self.manager = MockCirculationManager(session, self.services_fixture.services)

        # Set CirculationAPI and top-level lane for the default
        # library, for convenience in tests.
        self.manager.d_circulation = self.manager.circulation_apis[self.library.id]
        self.manager.d_top_level_lane = self.manager.top_level_lanes[self.library.id]  # type: ignore
        self.controller = CirculationManagerController(self.manager)

        # Set a convenient default lane.
        [self.english_adult_fiction] = [
            x
            for x in self.library.lanes
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
            )
            integration, _ = create(
                _db,
                IntegrationConfiguration,
                name=self.db.fresh_str(),
                protocol=protocol,
                goal=Goals.PATRON_AUTH_GOAL,
                settings_dict=settings.model_dump(),
            )
            create(
                _db,
                IntegrationLibraryConfiguration,
                library=library,
                parent=integration,
            )

        settings = LibrarySettings.model_construct(
            large_collection_languages=[],
            small_collection_languages=["eng"],
            tiny_collection_languages=["spa", "chi", "fre"],
        )
        library.update_settings(settings)
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
            setattr(c.request, "library", library)
            yield c

    @classmethod
    @contextmanager
    def fixture(
        cls, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ) -> Generator[Self]:
        time_then = datetime.datetime.now()
        fixture = cls(db, services_fixture)
        time_now = datetime.datetime.now()
        time_diff = time_now - time_then
        logging.info("controller init took %s", time_diff)
        try:
            yield fixture
        finally:
            # After the test is done, make sure the app is cleaned up, so
            # we don't change the state for later tests.
            fixture.app._db = None  # type: ignore[assignment]
            delattr(fixture.app, "manager")


@pytest.fixture(scope="function")
def controller_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
):
    with ControllerFixture.fixture(db, services_fixture) as fixture:
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
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ):
        super().__init__(db, services_fixture)
        self.works = []
        self.add_works(self.BOOKS)

        # Enable the audiobook entry point for the default library -- a lot of
        # tests verify that non-default entry points can be selected.
        library = self.db.default_library()
        library.update_settings(
            LibrarySettings.model_construct(
                enabled_entry_points=[
                    EbooksEntryPoint.INTERNAL_NAME,
                    AudiobooksEntryPoint.INTERNAL_NAME,
                ]
            )
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

        self.manager.external_search.search_service().index_submit_documents(self.works)
        self.manager.external_search.mock_query_works_multi(self.works)


@pytest.fixture(scope="function")
def circulation_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
):
    with CirculationControllerFixture.fixture(db, services_fixture) as fixture:
        yield fixture

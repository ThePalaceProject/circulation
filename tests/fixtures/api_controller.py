import datetime
import json
import logging
from contextlib import contextmanager
from typing import Any

import flask
import pytest
from flask import Flask

from api.adobe_vendor_id import AuthdataUtility
from api.app import app
from api.config import Configuration
from api.controller import CirculationManager, CirculationManagerController
from api.lanes import create_default_lanes
from api.simple_authentication import SimpleAuthenticationProvider
from core.model import (
    Collection,
    ConfigurationSetting,
    ExternalIntegration,
    Library,
    Patron,
    Session,
    create,
    get_one_or_create,
)
from core.util.string_helpers import base64
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.vendor_id import VendorIDFixture


class ControllerFixture:
    """A test that requires a functional app server."""

    app: Flask
    authdata: AuthdataUtility
    collection: Collection
    collections: list[Collection]
    controller: CirculationManagerController
    db: DatabaseTransactionFixture
    default_patron: object
    default_patrons: dict[Any, Any]
    english_adult_fiction: object
    libraries: list[Library]
    library: Library
    manager: CirculationManager
    vendor_ids: VendorIDFixture

    # Authorization headers that will succeed (or fail) against the
    # SimpleAuthenticationProvider set up in ControllerTest.setup().
    valid_auth = "Basic " + base64.b64encode("unittestuser:unittestpassword")
    invalid_auth = "Basic " + base64.b64encode("user1:password2")
    valid_credentials = dict(username="unittestuser", password="unittestpassword")

    def __init__(
        self, db: DatabaseTransactionFixture, vendor_id_fixture: VendorIDFixture
    ):
        self.vendor_ids = vendor_id_fixture
        self.db = db
        self.app = app

        if not hasattr(self, "setup_circulation_manager"):
            self.setup_circulation_manager = True

            # PRESERVE_CONTEXT_ON_EXCEPTION needs to be off in tests
            # to prevent one test failure from breaking later tests as well.
            # When used with flask's test_request_context, exceptions
            # from previous tests would cause flask to roll back the db
            # when you entered a new request context, deleting rows that
            # were created in the test setup.
        app.config["PRESERVE_CONTEXT_ON_EXCEPTION"] = False

        Configuration.instance[Configuration.INTEGRATIONS][ExternalIntegration.CDN] = {
            "": "http://cdn"
        }

        if self.setup_circulation_manager:
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

    def circulation_manager_setup(self) -> CirculationManager:
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

        :return: a CirculationManager object.

        """
        self.libraries = self.make_default_libraries(self.db.session)
        self.collections = [
            self.make_default_collection(self.db.session, library)
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
        self.manager = CirculationManager(self.db.session, testing=True)

        # Set CirculationAPI and top-level lane for the default
        # library, for convenience in tests.
        self.manager.d_circulation = self.manager.circulation_apis[self.library.id]
        self.manager.d_top_level_lane = self.manager.top_level_lanes[self.library.id]
        self.controller = CirculationManagerController(self.manager)

        # Set a convenient default lane.
        [self.english_adult_fiction] = [
            x
            for x in self.library.lanes  # type: ignore
            if x.display_name == "Fiction" and x.languages == ["eng"]
        ]

        return self.manager

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
        if not any(
            [
                x
                for x in library.integrations
                if x.goal == ExternalIntegration.PATRON_AUTH_GOAL
            ]
        ):
            integration, ignore = create(
                _db,
                ExternalIntegration,
                protocol="api.simple_authentication",
                goal=ExternalIntegration.PATRON_AUTH_GOAL,
            )
            p = SimpleAuthenticationProvider
            integration.setting(p.TEST_IDENTIFIER).value = "unittestuser"
            integration.setting(p.TEST_PASSWORD).value = "unittestpassword"
            integration.setting(p.TEST_NEIGHBORHOOD).value = "Unit Test West"
            library.integrations.append(integration)

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
    fixture = ControllerFixture(db, vendor_id_fixture)
    time_now = datetime.datetime.now()
    time_diff = time_now - time_then
    logging.info("controller init took %s", time_diff)
    yield fixture

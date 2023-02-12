import json
from contextlib import contextmanager

import flask

from palace.api.adobe_vendor_id import AuthdataUtility
from palace.api.app import app
from palace.api.config import Configuration
from palace.api.controller import CirculationManager, CirculationManagerController
from palace.api.lanes import create_default_lanes
from palace.api.simple_authentication import SimpleAuthenticationProvider
from palace.api.testing import VendorIDTest
from palace.core.entrypoint import AudiobooksEntryPoint, EbooksEntryPoint, EntryPoint
from palace.core.model import (
    ConfigurationSetting,
    ExternalIntegration,
    Patron,
    Session,
    create,
    get_one_or_create,
)
from palace.core.util.string_helpers import base64
from tests.fixtures.api_config import get_key_pair_fixture, get_mock_config_key_pair


class ControllerTest(VendorIDTest):
    """A test that requires a functional app server."""

    # Authorization headers that will succeed (or fail) against the
    # SimpleAuthenticationProvider set up in ControllerTest.setup().
    valid_auth = "Basic " + base64.b64encode("unittestuser:unittestpassword")
    invalid_auth = "Basic " + base64.b64encode("user1:password2")
    valid_credentials = dict(username="unittestuser", password="unittestpassword")

    def setup_method(self):
        super().setup_method()
        from _pytest.monkeypatch import MonkeyPatch

        self.patch = MonkeyPatch()
        fixture = get_key_pair_fixture()
        self.patch.setattr(
            "palace.api.config.Configuration.key_pair",
            get_mock_config_key_pair(fixture),
        )
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

        if self.setup_circulation_manager:
            # NOTE: Any reference to self._default_library below this
            # point in this method will cause the tests in
            # TestScopedSession to hang.
            self.set_base_url(self._db)
            app.manager = self.circulation_manager_setup(self._db)

    def teardown_method(self):
        self.patch.undo()
        super().teardown_method()

    def set_base_url(self, _db):
        base_url = ConfigurationSetting.sitewide(_db, Configuration.BASE_URL_KEY)
        base_url.value = "http://test-circulation-manager/"

    def circulation_manager_setup(self, _db):
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

        :param _db: The database session to use when creating the
            library objects.

        :return: a CirculationManager object.

        """
        self.libraries = self.make_default_libraries(_db)
        self.collections = [
            self.make_default_collection(_db, library) for library in self.libraries
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

        self.manager = CirculationManager(_db, testing=True)

        # Set CirculationAPI and top-level lane for the default
        # library, for convenience in tests.
        self.manager.d_circulation = self.manager.circulation_apis[self.library.id]
        self.manager.d_top_level_lane = self.manager.top_level_lanes[self.library.id]
        self.controller = CirculationManagerController(self.manager)

        # Set a convenient default lane.
        [self.english_adult_fiction] = [
            x
            for x in self.library.lanes
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
                protocol="palace.api.simple_authentication",
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
        return [self._default_library]

    def make_default_collection(self, _db, library):
        return self._default_collection

    @contextmanager
    def request_context_with_library(self, route, *args, **kwargs):
        if "library" in kwargs:
            library = kwargs.pop("library")
        else:
            library = self._default_library
        with self.app.test_request_context(route, *args, **kwargs) as c:
            flask.request.library = library
            yield c


class CirculationControllerTest(ControllerTest):

    # These tests generally need at least one Work created,
    # but some need more.
    BOOKS = [
        ["english_1", "Quite British", "John Bull", "eng", True],
    ]

    def setup_method(self):
        super().setup_method()
        self.works = []
        for (variable_name, title, author, language, fiction) in self.BOOKS:
            work = self._work(
                title,
                author,
                language=language,
                fiction=fiction,
                with_open_access_download=True,
            )
            setattr(self, variable_name, work)
            work.license_pools[0].collection = self.collection
            self.works.append(work)
        self.manager.external_search.bulk_update(self.works)

        # Enable the audiobook entry point for the default library -- a lot of
        # tests verify that non-default entry points can be selected.
        self._default_library.setting(EntryPoint.ENABLED_SETTING).value = json.dumps(
            [EbooksEntryPoint.INTERNAL_NAME, AudiobooksEntryPoint.INTERNAL_NAME]
        )

    def assert_bad_search_index_gives_problem_detail(self, test_function):
        """Helper method to test that a controller method serves a problem
        detail document when the search index isn't set up.

        Mocking a broken search index is a lot of work; thus the helper method.
        """
        old_setup = self.manager.setup_external_search
        old_value = self.manager._external_search
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
        self.manager.setup_external_search = old_setup
        self.manager._external_search = old_value

import logging
from typing import Any, Generator

import flask
import pytest
from flask import Response
from werkzeug.exceptions import MethodNotAllowed

from api import routes
from api.controller import CirculationManager, CirculationManagerController
from api.routes import exception_handler
from api.routes import h as error_handler_object
from core.app_server import ErrorHandler

from ..fixtures.api_controller import ControllerFixture
from ..fixtures.vendor_id import VendorIDFixture


class MockApp:
    """Pretends to be a Flask application with a configured
    CirculationManager.
    """

    def __init__(self):
        self.manager = MockManager()


class MockManager:
    """Pretends to be a CirculationManager with configured controllers."""

    def __init__(self):
        self._cache = {}

        # This is used by the allows_patron_web annotator.
        self.patron_web_domains = {"http://patron/web"}

    def __getattr__(self, controller_name):
        return self._cache.setdefault(controller_name, MockController(controller_name))


class MockControllerMethod:
    """Pretends to be one of the methods of a controller class."""

    def __init__(self, controller, name):
        """Constructor.

        :param controller: A MockController.
        :param name: The name of this method.
        """
        self.controller = controller
        self.name = name
        self.callable_name = name

    def __call__(self, *args, **kwargs):
        """Simulate a successful method call.

        :return: A Response object, as required by Flask, with this
        method smuggled out as the 'method' attribute.
        """
        self.args = args
        self.kwargs = kwargs
        response = Response("I called %s" % repr(self), 200)
        response.method = self
        return response

    def __repr__(self):
        return f"<MockControllerMethod {self.controller.name}.{self.name}>"


class MockController(MockControllerMethod):
    """Pretends to be a controller.

    A controller has methods, but it may also be called _as_ a method,
    so this class subclasses MockControllerMethod.
    """

    AUTHENTICATED_PATRON = "i am a mock patron"

    def __init__(self, name):
        """Constructor.

        :param name: The name of the controller.
        """
        self.name = name

        # If this controller were to be called as a method, the method
        # name would be __call__, not the name of the controller.
        self.callable_name = "__call__"

        self._cache = {}
        self.authenticated = False
        self.csrf_token = False
        self.authenticated_problem_detail = False

    def authenticated_patron_from_request(self):
        if self.authenticated:
            patron = object()
            flask.request.patron = self.AUTHENTICATED_PATRON
            return self.AUTHENTICATED_PATRON
        else:
            return Response(
                "authenticated_patron_from_request called without authorizing", 401
            )

    def __getattr__(self, method_name):
        """Locate a method of this controller as a MockControllerMethod."""
        return self._cache.setdefault(
            method_name, MockControllerMethod(self, method_name)
        )

    def __repr__(self):
        return "<MockControllerMethod %s>" % self.name


class RouteTestFixture:

    # The first time setup_method() is called, it will instantiate a real
    # CirculationManager object and store it in REAL_CIRCULATION_MANAGER.
    # We only do this once because it takes about a second to instantiate
    # this object. Calling any of this object's methods could be problematic,
    # since it's probably left over from a previous test, but we won't be
    # calling any methods -- we just want to verify the _existence_,
    # in a real CirculationManager, of the methods called in
    # routes.py.

    REAL_CIRCULATION_MANAGER = None

    def __init__(
        self, vendor_id: VendorIDFixture, controller_fixture: ControllerFixture
    ):
        self.db = vendor_id.db
        self.controller_fixture = controller_fixture
        self.setup_circulation_manager = False
        if not RouteTestFixture.REAL_CIRCULATION_MANAGER:
            library = self.db.default_library()
            # Set up the necessary configuration so that when we
            # instantiate the CirculationManager it gets an
            # adobe_vendor_id controller -- this wouldn't normally
            # happen because most circulation managers don't need such a
            # controller.
            vendor_id.initialize_adobe(library, [library])
            vendor_id.adobe_vendor_id.password = vendor_id.TEST_NODE_VALUE
            manager = CirculationManager(self.db.session, testing=True)
            RouteTestFixture.REAL_CIRCULATION_MANAGER = manager

        app = MockApp()
        self.routes = routes
        self.manager = app.manager
        self.original_app = self.routes.app
        self.resolver = self.original_app.url_map.bind("", "/")

        self.controller: CirculationManagerController = None
        self.real_controller: CirculationManagerController = None
        self.routes.app = app

    def set_controller_name(self, name: str):
        self.controller = getattr(self.manager, name)
        # Make sure there's a controller by this name in the real
        # CirculationManager.
        self.real_controller = getattr(self.REAL_CIRCULATION_MANAGER, name)

    def close(self):
        self.routes.app = self.original_app

    def request(self, url, method="GET"):
        """Simulate a request to a URL without triggering any code outside
        routes.py.
        """
        # Map an incoming URL to the name of a function within routes.py
        # and a set of arguments to the function.
        function_name, kwargs = self.resolver.match(url, method)
        # Locate the corresponding function in our mock app.
        mock_function = getattr(self.routes, function_name)

        # Call it in the context of the mock app.
        with self.controller_fixture.app.test_request_context():
            return mock_function(**kwargs)

    def assert_request_calls(self, url, method, *args, **kwargs):
        """Make a request to the given `url` and assert that
        the given controller `method` was called with the
        given `args` and `kwargs`.
        """
        http_method = kwargs.pop("http_method", "GET")
        response = self.request(url, http_method)
        assert response.method == method
        assert response.method.args == args
        assert response.method.kwargs == kwargs

        # Make sure the real controller has a method by the name of
        # the mock method that was called. We won't call it, because
        # it would slow down these tests dramatically, but we can make
        # sure it exists.
        if self.real_controller:
            real_method = getattr(self.real_controller, method.callable_name)

            # TODO: We could use inspect.getarcspec to verify that the
            # argument names line up with the variables passed in to
            # the mock method. This might remove the need to call the
            # mock method at all.

    def assert_request_calls_method_using_identifier(
        self, url, method, *args, **kwargs
    ):
        # Call an assertion method several times, using different
        # types of identifier in the URL, to make sure the identifier
        # is always passed through correctly.
        #
        # The url must contain the string '<identifier>' standing in
        # for the place where an identifier should be plugged in, and
        # the *args list must include the string '<identifier>'.
        authenticated = kwargs.pop("authenticated", False)
        if authenticated:
            assertion_method = self.assert_authenticated_request_calls
        else:
            assertion_method = self.assert_request_calls
        assert "<identifier>" in url
        args = list(args)
        identifier_index = args.index("<identifier>")
        for identifier in (
            "<identifier>",
            "an/identifier/",
            "http://an-identifier/",
            "http://an-identifier",
        ):
            modified_url = url.replace("<identifier>", identifier)
            args[identifier_index] = identifier
            assertion_method(modified_url, method, *args, **kwargs)

    def assert_authenticated_request_calls(self, url, method, *args, **kwargs):
        """First verify that an unauthenticated request fails. Then make an
        authenticated request to `url` and verify the results, as with
        assert_request_calls
        """
        authentication_required = kwargs.pop("authentication_required", True)

        http_method = kwargs.pop("http_method", "GET")
        response = self.request(url, http_method)
        if authentication_required:
            assert 401 == response.status_code
            assert (
                "authenticated_patron_from_request called without authorizing"
                == response.get_data(as_text=True)
            )
        else:
            assert 200 == response.status_code

        # Set a variable so that authenticated_patron_from_request
        # will succeed, and try again.
        self.manager.index_controller.authenticated = True
        try:
            kwargs["http_method"] = http_method
            self.assert_request_calls(url, method, *args, **kwargs)
        finally:
            # Un-set authentication for the benefit of future
            # assertions in this test function.
            self.manager.index_controller.authenticated = False

    def assert_supported_methods(self, url, *methods):
        """Verify that the given HTTP `methods` are the only ones supported
        on the given `url`.
        """
        # The simplest way to do this seems to be to try each of the
        # other potential methods and verify that MethodNotAllowed is
        # raised each time.
        check = {"GET", "POST", "PUT", "DELETE"} - set(methods)
        # Treat HEAD specially. Any controller that supports GET
        # automatically supports HEAD. So we only assert that HEAD
        # fails if the method supports neither GET nor HEAD.
        if "GET" not in methods and "HEAD" not in methods:
            check.add("HEAD")
        for method in check:
            logging.debug("MethodNotAllowed should be raised on %s", method)
            pytest.raises(MethodNotAllowed, self.request, url, method)
            logging.debug("And it was.")


@pytest.fixture(scope="function")
def route_test(
    vendor_id_fixture: VendorIDFixture, controller_fixture: ControllerFixture
) -> Generator[RouteTestFixture, Any, None]:
    fix = RouteTestFixture(vendor_id_fixture, controller_fixture)
    yield fix
    fix.close()


class TestAppConfiguration:

    # Test the configuration of the real Flask app.
    def test_configuration(self):
        assert False == routes.app.url_map.merge_slashes


class TestIndex:

    CONTROLLER_NAME = "index_controller"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_index(self, fixture: RouteTestFixture):
        for url in "/", "":
            fixture.assert_request_calls(url, fixture.controller)

    def test_authentication_document(self, fixture: RouteTestFixture):
        url = "/authentication_document"
        fixture.assert_request_calls(url, fixture.controller.authentication_document)

    def test_public_key_document(self, fixture: RouteTestFixture):
        url = "/public_key_document"
        fixture.assert_request_calls(url, fixture.controller.public_key_document)


class TestOPDSFeed:

    CONTROLLER_NAME = "opds_feeds"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_acquisition_groups(self, fixture: RouteTestFixture):
        # An incoming lane identifier is passed in to the groups()
        # method.
        method = fixture.controller.groups
        fixture.assert_request_calls("/groups", method, None)
        fixture.assert_request_calls(
            "/groups/<lane_identifier>", method, "<lane_identifier>"
        )

    def test_feed(self, fixture: RouteTestFixture):
        # An incoming lane identifier is passed in to the feed()
        # method.
        url = "/feed"
        fixture.assert_request_calls(url, fixture.controller.feed, None)
        url = "/feed/<lane_identifier>"
        fixture.assert_request_calls(url, fixture.controller.feed, "<lane_identifier>")

    def test_navigation_feed(self, fixture: RouteTestFixture):
        # An incoming lane identifier is passed in to the navigation_feed()
        # method.
        url = "/navigation"
        fixture.assert_request_calls(url, fixture.controller.navigation, None)
        url = "/navigation/<lane_identifier>"
        fixture.assert_request_calls(
            url, fixture.controller.navigation, "<lane_identifier>"
        )

    def test_crawlable_library_feed(self, fixture: RouteTestFixture):
        url = "/crawlable"
        fixture.assert_request_calls(url, fixture.controller.crawlable_library_feed)

    def test_crawlable_list_feed(self, fixture: RouteTestFixture):
        url = "/lists/<list_name>/crawlable"
        fixture.assert_request_calls(
            url, fixture.controller.crawlable_list_feed, "<list_name>"
        )

    def test_crawlable_collection_feed(self, fixture: RouteTestFixture):
        url = "/collections/<collection_name>/crawlable"
        fixture.assert_request_calls(
            url,
            fixture.manager.opds_feeds.crawlable_collection_feed,
            "<collection_name>",
        )

    def test_lane_search(self, fixture: RouteTestFixture):
        url = "/search"
        fixture.assert_request_calls(url, fixture.controller.search, None)

        url = "/search/<lane_identifier>"
        fixture.assert_request_calls(
            url, fixture.controller.search, "<lane_identifier>"
        )

    def test_qa_feed(self, fixture: RouteTestFixture):
        url = "/feed/qa"
        fixture.assert_authenticated_request_calls(url, fixture.controller.qa_feed)

    def test_qa_series_feed(self, fixture: RouteTestFixture):
        url = "/feed/qa/series"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.qa_series_feed
        )


class TestMARCRecord:
    CONTROLLER_NAME = "marc_records"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_marc_page(self, fixture: RouteTestFixture):
        url = "/marc"
        fixture.assert_request_calls(url, fixture.controller.download_page)


class TestSharedCollection:

    CONTROLLER_NAME = "shared_collection_controller"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_shared_collection_info(self, fixture: RouteTestFixture):
        url = "/collections/<collection_name>"
        fixture.assert_request_calls(url, fixture.controller.info, "<collection_name>")

    def test_shared_collection_register(self, fixture: RouteTestFixture):
        url = "/collections/<collection_name>/register"
        fixture.assert_request_calls(
            url, fixture.controller.register, "<collection_name>", http_method="POST"
        )
        fixture.assert_supported_methods(url, "POST")

    def test_shared_collection_borrow_identifier(self, fixture: RouteTestFixture):
        url = "/collections/<collection_name>/<identifier_type>/<identifier>/borrow"
        fixture.assert_request_calls_method_using_identifier(
            url,
            fixture.controller.borrow,
            "<collection_name>",
            "<identifier_type>",
            "<identifier>",
            None,
        )
        fixture.assert_supported_methods(url, "GET", "POST")

    def test_shared_collection_borrow_hold_id(self, fixture: RouteTestFixture):
        url = "/collections/<collection_name>/holds/<hold_id>/borrow"
        fixture.assert_request_calls(
            url, fixture.controller.borrow, "<collection_name>", None, None, "<hold_id>"
        )
        fixture.assert_supported_methods(url, "GET", "POST")

    def test_shared_collection_loan_info(self, fixture: RouteTestFixture):
        url = "/collections/<collection_name>/loans/<loan_id>"
        fixture.assert_request_calls(
            url, fixture.controller.loan_info, "<collection_name>", "<loan_id>"
        )

    def test_shared_collection_revoke_loan(self, fixture: RouteTestFixture):
        url = "/collections/<collection_name>/loans/<loan_id>/revoke"
        fixture.assert_request_calls(
            url, fixture.controller.revoke_loan, "<collection_name>", "<loan_id>"
        )

    def test_shared_collection_fulfill_no_mechanism(self, fixture: RouteTestFixture):
        url = "/collections/<collection_name>/loans/<loan_id>/fulfill"
        fixture.assert_request_calls(
            url, fixture.controller.fulfill, "<collection_name>", "<loan_id>", None
        )

    def test_shared_collection_fulfill_with_mechanism(self, fixture: RouteTestFixture):
        url = "/collections/<collection_name>/loans/<loan_id>/fulfill/<mechanism_id>"
        fixture.assert_request_calls(
            url,
            fixture.controller.fulfill,
            "<collection_name>",
            "<loan_id>",
            "<mechanism_id>",
        )

    def test_shared_collection_hold_info(self, fixture: RouteTestFixture):
        url = "/collections/<collection_name>/holds/<hold_id>"
        fixture.assert_request_calls(
            url, fixture.controller.hold_info, "<collection_name>", "<hold_id>"
        )

    def test_shared_collection_revoke_hold(self, fixture: RouteTestFixture):
        url = "/collections/<collection_name>/holds/<hold_id>/revoke"
        fixture.assert_request_calls(
            url, fixture.controller.revoke_hold, "<collection_name>", "<hold_id>"
        )


class TestProfileController:

    CONTROLLER_NAME = "profiles"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_patron_profile(self, fixture: RouteTestFixture):
        url = "/patrons/me"
        fixture.assert_authenticated_request_calls(
            url,
            fixture.controller.protocol,
        )


class TestLoansController:

    CONTROLLER_NAME = "loans"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_active_loans(self, fixture: RouteTestFixture):
        url = "/loans"
        fixture.assert_authenticated_request_calls(
            url,
            fixture.controller.sync,
        )
        fixture.assert_supported_methods(url, "GET", "HEAD")

    def test_borrow(self, fixture: RouteTestFixture):
        url = "/works/<identifier_type>/<identifier>/borrow"
        fixture.assert_request_calls_method_using_identifier(
            url,
            fixture.controller.borrow,
            "<identifier_type>",
            "<identifier>",
            None,
            authenticated=True,
        )
        fixture.assert_supported_methods(url, "GET", "PUT")

        url = "/works/<identifier_type>/<identifier>/borrow/<mechanism_id>"
        fixture.assert_request_calls_method_using_identifier(
            url,
            fixture.controller.borrow,
            "<identifier_type>",
            "<identifier>",
            "<mechanism_id>",
            authenticated=True,
        )
        fixture.assert_supported_methods(url, "GET", "PUT")

    def test_fulfill(self, fixture: RouteTestFixture):
        # fulfill does *not* require authentication, because this
        # controller is how a no-authentication library fulfills
        # open-access titles.
        url = "/works/<license_pool_id>/fulfill"
        fixture.assert_request_calls(
            url, fixture.controller.fulfill, "<license_pool_id>", None, None
        )

        url = "/works/<license_pool_id>/fulfill/<mechanism_id>"
        fixture.assert_request_calls(
            url, fixture.controller.fulfill, "<license_pool_id>", "<mechanism_id>", None
        )

        url = "/works/<license_pool_id>/fulfill/<mechanism_id>/<part>"
        fixture.assert_request_calls(
            url,
            fixture.controller.fulfill,
            "<license_pool_id>",
            "<mechanism_id>",
            "<part>",
        )

    def test_revoke_loan_or_hold(self, fixture: RouteTestFixture):
        url = "/loans/<license_pool_id>/revoke"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.revoke, "<license_pool_id>"
        )

        # TODO: DELETE shouldn't be in here, but "DELETE
        # /loans/<license_pool_id>/revoke" is interpreted as an attempt
        # to match /loans/<identifier_type>/<path:identifier>, the
        # method tested directly below, which does support DELETE.
        fixture.assert_supported_methods(url, "GET", "PUT", "DELETE")

    def test_loan_or_hold_detail(self, fixture: RouteTestFixture):
        url = "/loans/<identifier_type>/<identifier>"
        fixture.assert_request_calls_method_using_identifier(
            url,
            fixture.controller.detail,
            "<identifier_type>",
            "<identifier>",
            authenticated=True,
        )
        fixture.assert_supported_methods(url, "GET", "DELETE")


class TestAnnotationsController:

    CONTROLLER_NAME = "annotations"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_annotations(self, fixture: RouteTestFixture):
        url = "/annotations/"
        fixture.assert_authenticated_request_calls(url, fixture.controller.container)
        fixture.assert_supported_methods(url, "HEAD", "GET", "POST")

    def test_annotation_detail(self, fixture: RouteTestFixture):
        url = "/annotations/<annotation_id>"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.detail, "<annotation_id>"
        )
        fixture.assert_supported_methods(url, "HEAD", "GET", "DELETE")

    def test_annotations_for_work(self, fixture: RouteTestFixture):
        url = "/annotations/<identifier_type>/<identifier>"
        fixture.assert_request_calls_method_using_identifier(
            url,
            fixture.controller.container_for_work,
            "<identifier_type>",
            "<identifier>",
            authenticated=True,
        )
        fixture.assert_supported_methods(url, "GET")


class TestURNLookupController:

    CONTROLLER_NAME = "urn_lookup"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_work(self, fixture: RouteTestFixture):
        url = "/works"
        fixture.assert_request_calls(url, fixture.controller.work_lookup, "work")


class TestWorkController:

    CONTROLLER_NAME = "work_controller"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_contributor(self, fixture: RouteTestFixture):
        url = "/works/contributor/<contributor_name>"
        fixture.assert_request_calls(
            url, fixture.controller.contributor, "<contributor_name>", None, None
        )

    def test_contributor_language(self, fixture: RouteTestFixture):
        url = "/works/contributor/<contributor_name>/<languages>"
        fixture.assert_request_calls(
            url,
            fixture.controller.contributor,
            "<contributor_name>",
            "<languages>",
            None,
        )

    def test_contributor_language_audience(self, fixture: RouteTestFixture):
        url = "/works/contributor/<contributor_name>/<languages>/<audiences>"
        fixture.assert_request_calls(
            url,
            fixture.controller.contributor,
            "<contributor_name>",
            "<languages>",
            "<audiences>",
        )

    def test_series(self, fixture: RouteTestFixture):
        url = "/works/series/<series_name>"
        fixture.assert_request_calls(
            url, fixture.controller.series, "<series_name>", None, None
        )

    def test_series_language(self, fixture: RouteTestFixture):
        url = "/works/series/<series_name>/<languages>"
        fixture.assert_request_calls(
            url, fixture.controller.series, "<series_name>", "<languages>", None
        )

    def test_series_language_audience(self, fixture: RouteTestFixture):
        url = "/works/series/<series_name>/<languages>/<audiences>"
        fixture.assert_request_calls(
            url,
            fixture.controller.series,
            "<series_name>",
            "<languages>",
            "<audiences>",
        )

    def test_permalink(self, fixture: RouteTestFixture):
        url = "/works/<identifier_type>/<identifier>"
        fixture.assert_request_calls_method_using_identifier(
            url, fixture.controller.permalink, "<identifier_type>", "<identifier>"
        )

    def test_recommendations(self, fixture: RouteTestFixture):
        url = "/works/<identifier_type>/<identifier>/recommendations"
        fixture.assert_request_calls_method_using_identifier(
            url, fixture.controller.recommendations, "<identifier_type>", "<identifier>"
        )

    def test_related_books(self, fixture: RouteTestFixture):
        url = "/works/<identifier_type>/<identifier>/related_books"
        fixture.assert_request_calls_method_using_identifier(
            url, fixture.controller.related, "<identifier_type>", "<identifier>"
        )


class TestAnalyticsController:
    CONTROLLER_NAME = "analytics_controller"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_track_analytics_event(self, fixture: RouteTestFixture):
        url = "/analytics/<identifier_type>/<identifier>/<event_type>"

        # This controller can be called either authenticated or
        # unauthenticated.
        fixture.assert_request_calls_method_using_identifier(
            url,
            fixture.controller.track_event,
            "<identifier_type>",
            "<identifier>",
            "<event_type>",
            authenticated=True,
            authentication_required=False,
        )


class TestAdobeVendorID:

    CONTROLLER_NAME = "adobe_vendor_id"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_adobe_vendor_id_get_token(self, fixture: RouteTestFixture):
        url = "/AdobeAuth/authdata"
        fixture.assert_authenticated_request_calls(
            url,
            fixture.controller.create_authdata_handler,
            fixture.controller.AUTHENTICATED_PATRON,
        )
        # TODO: test what happens when vendor ID is not configured.

    def test_adobe_vendor_id_signin(self, fixture: RouteTestFixture):
        url = "/AdobeAuth/SignIn"
        fixture.assert_request_calls(
            url, fixture.controller.signin_handler, http_method="POST"
        )
        fixture.assert_supported_methods(url, "POST")

    def test_adobe_vendor_id_accountinfo(self, fixture: RouteTestFixture):
        url = "/AdobeAuth/AccountInfo"
        fixture.assert_request_calls(
            url, fixture.controller.userinfo_handler, http_method="POST"
        )
        fixture.assert_supported_methods(url, "POST")

    def test_adobe_vendor_id_status(self, fixture: RouteTestFixture):
        url = "/AdobeAuth/Status"
        fixture.assert_request_calls(
            url,
            fixture.controller.status_handler,
        )


class TestAdobeDeviceManagement:
    CONTROLLER_NAME = "adobe_device_management"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_adobe_drm_devices(self, fixture: RouteTestFixture):
        url = "/AdobeAuth/devices"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.device_id_list_handler
        )
        fixture.assert_supported_methods(url, "GET", "POST")

    def test_adobe_drm_device(self, fixture: RouteTestFixture):
        url = "/AdobeAuth/devices/<device_id>"
        fixture.assert_authenticated_request_calls(
            url,
            fixture.controller.device_id_handler,
            "<device_id>",
            http_method="DELETE",
        )
        fixture.assert_supported_methods(url, "DELETE")


class TestOAuthController:
    # TODO: We might be able to do a better job of checking that
    # flask.request.args are propagated through, instead of checking
    # an empty dict.
    CONTROLLER_NAME = "oauth_controller"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_oauth_authenticate(self, fixture: RouteTestFixture):
        url = "/oauth_authenticate"
        _db = fixture.manager._db
        fixture.assert_request_calls(
            url, fixture.controller.oauth_authentication_redirect, {}, _db
        )

    def test_oauth_callback(self, fixture: RouteTestFixture):
        url = "/oauth_callback"
        _db = fixture.manager._db
        fixture.assert_request_calls(
            url, fixture.controller.oauth_authentication_callback, _db, {}
        )


class TestODLNotificationController:
    CONTROLLER_NAME = "odl_notification_controller"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_odl_notify(self, fixture: RouteTestFixture):
        url = "/odl_notify/<loan_id>"
        fixture.assert_request_calls(url, fixture.controller.notify, "<loan_id>")
        fixture.assert_supported_methods(url, "GET", "POST")


class TestHeartbeatController:
    CONTROLLER_NAME = "heartbeat"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_heartbeat(self, fixture: RouteTestFixture):
        url = "/heartbeat"
        fixture.assert_request_calls(url, fixture.controller.heartbeat)


class TestHealthCheck:

    # This code isn't in a controller, and it doesn't really do anything,
    # so we check that it returns a specific result.
    def test_health_check(self, route_test: RouteTestFixture):
        response = route_test.request("/healthcheck.html")
        assert 200 == response.status_code

        # This is how we know we actually called health_check() and
        # not a mock method -- the Response returned by the mock
        # system would have an explanatory message in its .data.
        assert "" == response.get_data(as_text=True)


class TestExceptionHandler:
    def test_exception_handling(self, route_test: RouteTestFixture):
        # The exception handler deals with most exceptions by running them
        # through ErrorHandler.handle()
        assert isinstance(error_handler_object, ErrorHandler)

        # Temporarily replace the ErrorHandler used by the
        # exception_handler function -- this is what we imported as
        # error_handler_object.
        class MockErrorHandler:
            def handle(self, exception):
                self.handled = exception
                return Response("handled it", 500)

        routes.h = MockErrorHandler()

        # Simulate a request that causes an unhandled exception.
        with route_test.controller_fixture.app.test_request_context():
            value_error = ValueError()
            result = exception_handler(value_error)

            # The exception was passed into MockErrorHandler.handle.
            assert value_error == routes.h.handled

            # The Response is created was passed along.
            assert "handled it" == result.get_data(as_text=True)
            assert 500 == result.status_code

        # werkzeug HTTPExceptions are _not_ run through
        # handle(). werkzeug handles the conversion to a Response
        # object representing a more specific (and possibly even
        # non-error) HTTP response.
        with route_test.controller_fixture.app.test_request_context():
            exception = MethodNotAllowed()
            response = exception_handler(exception)
            assert 405 == response.status_code

        # Restore the normal error handler.
        routes.h = error_handler_object

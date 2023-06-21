import pytest
from flask import Response
from werkzeug.exceptions import MethodNotAllowed

from api import routes
from api.routes import exception_handler
from api.routes import h as error_handler_object
from core.app_server import ErrorHandler
from tests.fixtures.api_routes import RouteTestFixture


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


class TestApplicationVersionController:
    CONTROLLER_NAME = "version"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_heartbeat(self, fixture: RouteTestFixture):
        url = "/version.json"
        fixture.assert_request_calls(url, fixture.controller.version)


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

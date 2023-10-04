import pytest

from api import routes
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
        fixture.assert_request_calls(url, fixture.controller.authentication_document)  # type: ignore[union-attr]


class TestOPDSFeed:
    CONTROLLER_NAME = "opds_feeds"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_acquisition_groups(self, fixture: RouteTestFixture):
        # An incoming lane identifier is passed in to the groups()
        # method.
        method = fixture.controller.groups  # type: ignore[union-attr]
        fixture.assert_request_calls("/groups", method, None)
        fixture.assert_request_calls(
            "/groups/<lane_identifier>", method, "<lane_identifier>"
        )

    def test_feed(self, fixture: RouteTestFixture):
        # An incoming lane identifier is passed in to the feed()
        # method.
        url = "/feed"
        fixture.assert_request_calls(url, fixture.controller.feed, None)  # type: ignore[union-attr]
        url = "/feed/<lane_identifier>"
        fixture.assert_request_calls(url, fixture.controller.feed, "<lane_identifier>")  # type: ignore[union-attr]

    def test_navigation_feed(self, fixture: RouteTestFixture):
        # An incoming lane identifier is passed in to the navigation_feed()
        # method.
        url = "/navigation"
        fixture.assert_request_calls(url, fixture.controller.navigation, None)  # type: ignore[union-attr]
        url = "/navigation/<lane_identifier>"
        fixture.assert_request_calls(
            url, fixture.controller.navigation, "<lane_identifier>"  # type: ignore[union-attr]
        )

    def test_crawlable_library_feed(self, fixture: RouteTestFixture):
        url = "/crawlable"
        fixture.assert_request_calls(url, fixture.controller.crawlable_library_feed)  # type: ignore[union-attr]

    def test_crawlable_list_feed(self, fixture: RouteTestFixture):
        url = "/lists/<list_name>/crawlable"
        fixture.assert_request_calls(
            url, fixture.controller.crawlable_list_feed, "<list_name>"  # type: ignore[union-attr]
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
        fixture.assert_request_calls(url, fixture.controller.search, None)  # type: ignore[union-attr]

        url = "/search/<lane_identifier>"
        fixture.assert_request_calls(
            url, fixture.controller.search, "<lane_identifier>"  # type: ignore[union-attr]
        )

    def test_qa_feed(self, fixture: RouteTestFixture):
        url = "/feed/qa"
        fixture.assert_authenticated_request_calls(url, fixture.controller.qa_feed)  # type: ignore[union-attr]

    def test_qa_series_feed(self, fixture: RouteTestFixture):
        url = "/feed/qa/series"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.qa_series_feed  # type: ignore[union-attr]
        )


class TestMARCRecord:
    CONTROLLER_NAME = "marc_records"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_marc_page(self, fixture: RouteTestFixture):
        url = "/marc"
        fixture.assert_request_calls(url, fixture.controller.download_page)  # type: ignore[union-attr]


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
            fixture.controller.protocol,  # type: ignore[union-attr]
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
            fixture.controller.sync,  # type: ignore[union-attr]
        )
        fixture.assert_supported_methods(url, "GET", "HEAD")

    def test_borrow(self, fixture: RouteTestFixture):
        url = "/works/<identifier_type>/<identifier>/borrow"
        fixture.assert_request_calls_method_using_identifier(
            url,
            fixture.controller.borrow,  # type: ignore[union-attr]
            "<identifier_type>",
            "<identifier>",
            None,
            authenticated=True,
        )
        fixture.assert_supported_methods(url, "GET", "PUT")

        url = "/works/<identifier_type>/<identifier>/borrow/<mechanism_id>"
        fixture.assert_request_calls_method_using_identifier(
            url,
            fixture.controller.borrow,  # type: ignore[union-attr]
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
            url, fixture.controller.fulfill, "<license_pool_id>", None  # type: ignore[union-attr]
        )

        url = "/works/<license_pool_id>/fulfill/<mechanism_id>"
        fixture.assert_request_calls(
            url, fixture.controller.fulfill, "<license_pool_id>", "<mechanism_id>"  # type: ignore[union-attr]
        )

    def test_revoke_loan_or_hold(self, fixture: RouteTestFixture):
        url = "/loans/<license_pool_id>/revoke"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.revoke, "<license_pool_id>"  # type: ignore[union-attr]
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
            fixture.controller.detail,  # type: ignore[union-attr]
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
        fixture.assert_authenticated_request_calls(url, fixture.controller.container)  # type: ignore[union-attr]
        fixture.assert_supported_methods(url, "HEAD", "GET", "POST")

    def test_annotation_detail(self, fixture: RouteTestFixture):
        url = "/annotations/<annotation_id>"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.detail, "<annotation_id>"  # type: ignore[union-attr]
        )
        fixture.assert_supported_methods(url, "HEAD", "GET", "DELETE")

    def test_annotations_for_work(self, fixture: RouteTestFixture):
        url = "/annotations/<identifier_type>/<identifier>"
        fixture.assert_request_calls_method_using_identifier(
            url,
            fixture.controller.container_for_work,  # type: ignore[union-attr]
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
        fixture.assert_request_calls(url, fixture.controller.work_lookup, "work")  # type: ignore[union-attr]


class TestWorkController:
    CONTROLLER_NAME = "work_controller"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_contributor(self, fixture: RouteTestFixture):
        url = "/works/contributor/<contributor_name>"
        fixture.assert_request_calls(
            url, fixture.controller.contributor, "<contributor_name>", None, None  # type: ignore[union-attr]
        )

    def test_contributor_language(self, fixture: RouteTestFixture):
        url = "/works/contributor/<contributor_name>/<languages>"
        fixture.assert_request_calls(
            url,
            fixture.controller.contributor,  # type: ignore[union-attr]
            "<contributor_name>",
            "<languages>",
            None,
        )

    def test_contributor_language_audience(self, fixture: RouteTestFixture):
        url = "/works/contributor/<contributor_name>/<languages>/<audiences>"
        fixture.assert_request_calls(
            url,
            fixture.controller.contributor,  # type: ignore[union-attr]
            "<contributor_name>",
            "<languages>",
            "<audiences>",
        )

    def test_series(self, fixture: RouteTestFixture):
        url = "/works/series/<series_name>"
        fixture.assert_request_calls(
            url, fixture.controller.series, "<series_name>", None, None  # type: ignore[union-attr]
        )

    def test_series_language(self, fixture: RouteTestFixture):
        url = "/works/series/<series_name>/<languages>"
        fixture.assert_request_calls(
            url, fixture.controller.series, "<series_name>", "<languages>", None  # type: ignore[union-attr]
        )

    def test_series_language_audience(self, fixture: RouteTestFixture):
        url = "/works/series/<series_name>/<languages>/<audiences>"
        fixture.assert_request_calls(
            url,
            fixture.controller.series,  # type: ignore[union-attr]
            "<series_name>",
            "<languages>",
            "<audiences>",
        )

    def test_permalink(self, fixture: RouteTestFixture):
        url = "/works/<identifier_type>/<identifier>"
        fixture.assert_request_calls_method_using_identifier(
            url, fixture.controller.permalink, "<identifier_type>", "<identifier>"  # type: ignore[union-attr]
        )

    def test_recommendations(self, fixture: RouteTestFixture):
        url = "/works/<identifier_type>/<identifier>/recommendations"
        fixture.assert_request_calls_method_using_identifier(
            url, fixture.controller.recommendations, "<identifier_type>", "<identifier>"  # type: ignore[union-attr]
        )

    def test_related_books(self, fixture: RouteTestFixture):
        url = "/works/<identifier_type>/<identifier>/related_books"
        fixture.assert_request_calls_method_using_identifier(
            url, fixture.controller.related, "<identifier_type>", "<identifier>"  # type: ignore[union-attr]
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
            fixture.controller.track_event,  # type: ignore[union-attr]
            "<identifier_type>",
            "<identifier>",
            "<event_type>",
            authenticated=True,
            authentication_required=False,
        )


class TestODLNotificationController:
    CONTROLLER_NAME = "odl_notification_controller"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_odl_notify(self, fixture: RouteTestFixture):
        url = "/odl_notify/<loan_id>"
        fixture.assert_request_calls(url, fixture.controller.notify, "<loan_id>")  # type: ignore[union-attr]
        fixture.assert_supported_methods(url, "GET", "POST")


class TestApplicationVersionController:
    CONTROLLER_NAME = "version"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_heartbeat(self, fixture: RouteTestFixture):
        url = "/version.json"
        fixture.assert_request_calls(url, fixture.controller.version)  # type: ignore[union-attr]


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

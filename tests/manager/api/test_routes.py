from unittest.mock import patch

import pytest

from palace.manager.api import routes
from palace.manager.api.problem_details import LIBRARY_NOT_FOUND
from palace.manager.sqlalchemy.listeners import site_configuration_has_changed
from palace.manager.sqlalchemy.model.library import Library
from tests.fixtures.api_controller import ControllerFixture
from tests.fixtures.api_routes import RouteTestFixture


class TestAppConfiguration:
    # Test the configuration of the real Flask app.
    def test_configuration(self):
        assert False == routes.app.url_map.merge_slashes


class TestAdminRequestLifecycle:
    def test_requests_check_if_settings_need_reload(
        self, controller_fixture: ControllerFixture
    ):
        with patch(
            "palace.manager.api.circulation_manager.CirculationManager.reload_settings_if_changed"
        ) as mock_reload:
            mock_reload.assert_not_called()
            with controller_fixture.app.test_request_context("/"):
                mock_reload.assert_not_called()
                controller_fixture.app.preprocess_request()
                mock_reload.assert_called()

    def test_request_reloads_settings_if_necessary(
        self, controller_fixture: ControllerFixture
    ):
        # We're about to change the shortname of the default library.
        new_name = "newname" + controller_fixture.db.fresh_str()

        # Before we make the change, a request to the library's new name
        # will fail.
        assert new_name not in controller_fixture.manager.auth.library_authenticators
        with controller_fixture.app.test_request_context("/"):
            # Ensure that any `before_request` handlers are run, as in real request.
            controller_fixture.app.preprocess_request()
            problem = controller_fixture.controller.library_for_request(new_name)
            assert LIBRARY_NOT_FOUND == problem

        # Make the change.
        controller_fixture.db.default_library().short_name = new_name
        controller_fixture.db.session.commit()

        # Bypass the 1-second cooldown and make sure the app knows
        # the configuration has actually changed.
        site_configuration_has_changed(controller_fixture.db.session, cooldown=0)

        # Just making the change and calling `site_configuration_has_changed`
        # was not enough to update the CirculationManager's settings.
        assert new_name not in controller_fixture.manager.auth.library_authenticators

        # But the next time we make a request -- any request -- the
        # configuration will be up-to-date.
        with controller_fixture.app.test_request_context("/"):
            # Ensure that any `before_request` handlers are run, as in real request.
            controller_fixture.app.preprocess_request()

            # An assertion that would have failed before works now.
            assert new_name in controller_fixture.manager.auth.library_authenticators

            #
            new_library = controller_fixture.controller.library_for_request(new_name)
            assert isinstance(new_library, Library)
            assert new_library.short_name == new_name


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


class TestMARCRecord:
    CONTROLLER_NAME = "marc_records"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_marc_page(self, fixture: RouteTestFixture):
        url = "/marc"
        fixture.assert_request_calls(url, fixture.controller.download_page)


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


class TestAdobePatronController:
    CONTROLLER_NAME = "adobe_patron"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_patron_adobe_id_reset(self, fixture: RouteTestFixture):
        url = "/patrons/me/adobe_id_reset"
        fixture.assert_authenticated_request_calls(
            url,
            fixture.controller.reset_adobe_id,
            http_method="POST",
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
            url, fixture.controller.fulfill, "<license_pool_id>", None
        )

        url = "/works/<license_pool_id>/fulfill/<mechanism_id>"
        fixture.assert_request_calls(
            url, fixture.controller.fulfill, "<license_pool_id>", "<mechanism_id>"
        )

    def test_revoke_loan_or_hold(self, fixture: RouteTestFixture):
        url = "/loans/<license_pool_id>/revoke"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.revoke, "<license_pool_id>"
        )

        fixture.assert_supported_methods(url, "GET", "PUT")

    def test_loan_or_hold_detail(self, fixture: RouteTestFixture):
        url = "/loans/<identifier_type>/<identifier>"
        fixture.assert_request_calls_method_using_identifier(
            url,
            fixture.controller.detail,
            "<identifier_type>",
            "<identifier>",
            authenticated=True,
        )
        fixture.assert_supported_methods(url, "GET")


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


class TestODLNotificationController:
    CONTROLLER_NAME = "odl_notification_controller"

    @pytest.fixture(scope="function")
    def fixture(self, route_test: RouteTestFixture) -> RouteTestFixture:
        route_test.set_controller_name(self.CONTROLLER_NAME)
        return route_test

    def test_odl_notify(self, fixture: RouteTestFixture):
        url = "/odl/notify/<patron_identifier>/<license_identifier>"
        fixture.assert_request_calls(
            url,
            fixture.controller.notify,
            "<patron_identifier>",
            "<license_identifier>",
            http_method="POST",
        )
        fixture.assert_supported_methods(url, "POST")


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

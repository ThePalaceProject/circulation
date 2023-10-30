import logging
from pathlib import Path
from typing import Any, Generator, Optional

import flask
import pytest
from flask import Response
from werkzeug.exceptions import MethodNotAllowed

from api import routes as api_routes
from api.admin import routes
from api.admin.controller import setup_admin_controllers
from api.admin.problem_details import *
from api.controller import CirculationManagerController
from core.util.problem_detail import ProblemDetail, ProblemError
from tests.api.mockapi.circulation import MockCirculationManager
from tests.fixtures.api_controller import ControllerFixture
from tests.fixtures.api_routes import MockApp, MockController, MockManager
from tests.fixtures.database import DatabaseTransactionFixture


class MockAdminApp:
    """Pretends to be a Flask application with a configured
    CirculationManager and Admin routes.
    """

    def __init__(self):
        self.manager = MockAdminManager()


class MockAdminManager(MockManager):
    def __getattr__(self, controller_name):
        return self._cache.setdefault(
            controller_name, MockAdminController(controller_name)
        )


class MockAdminController(MockController):
    AUTHENTICATED_ADMIN = "i am a mock admin"

    def authenticated_admin_from_request(self):
        if self.authenticated:
            admin = object()
            flask.request.admin = self.AUTHENTICATED_ADMIN
            return self.AUTHENTICATED_ADMIN
        # For the redirect case we want to return a Problem Detail.
        elif self.authenticated_problem_detail:
            return INVALID_ADMIN_CREDENTIALS
        else:
            return Response(
                "authenticated_admin_from_request called without authorizing", 401
            )

    def get_csrf_token(self):
        if self.csrf_token:
            return "some token"
        else:
            return INVALID_CSRF_TOKEN

    def bulk_circulation_events(self):
        return "data", "date", "date_end", "library"


class AdminRouteFixture:
    # The first time __init__() is called, it will instantiate a real
    # CirculationManager object and store it in REAL_CIRCULATION_MANAGER.
    # We only do this once because it takes about a second to instantiate
    # this object. Calling any of this object's methods could be problematic,
    # since it's probably left over from a previous test, but we won't be
    # calling any methods -- we just want to verify the _existence_,
    # in a real CirculationManager, of the methods called in
    # routes.py.
    REAL_CIRCULATION_MANAGER = None

    def __init__(
        self, db: DatabaseTransactionFixture, controller_fixture: ControllerFixture
    ):
        self.db = db
        self.controller_fixture = controller_fixture
        self.setup_circulation_manager = False
        if not self.REAL_CIRCULATION_MANAGER:
            circ_manager = MockCirculationManager(self.db.session)
            setup_admin_controllers(circ_manager)
            self.REAL_CIRCULATION_MANAGER = circ_manager

        app = MockAdminApp()
        # Also mock the api app in order to use functions from api/routes
        api_app = MockApp()
        self.routes = routes
        self.api_routes = api_routes
        self.manager = app.manager
        self.original_app = self.routes.app
        self.original_api_app = self.api_routes.app
        self.resolver = self.original_app.url_map.bind("", "/")

        self.controller: Optional[CirculationManagerController] = None
        self.real_controller: Optional[CirculationManagerController] = None

        self.routes.app = app  # type: ignore
        # Need to also mock the route app from /api/routes.
        self.api_routes.app = api_app  # type: ignore

    def close(self):
        self.routes.app = self.original_app
        self.api_routes.app = self.original_api_app

    def set_controller_name(self, name: str):
        self.controller = getattr(self.manager, name)
        # Make sure there's a controller by this name in the real
        # CirculationManager.
        self.real_controller = getattr(self.REAL_CIRCULATION_MANAGER, name)

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

    def assert_authenticated_request_calls(self, url, method, *args, **kwargs):
        """First verify that an unauthenticated request fails. Then make an
        authenticated request to `url` and verify the results, as with
        assert_request_calls.
        """
        authentication_required = kwargs.pop("authentication_required", True)

        http_method = kwargs.pop("http_method", "GET")
        response = self.request(url, http_method)
        if authentication_required:
            assert 401 == response.status_code
            assert (
                "authenticated_admin_from_request called without authorizing"
                == response.get_data(as_text=True)
            )
        else:
            assert 200 == response.status_code

        # Set a variable so that authenticated_admin_from_request
        # will succeed, and try again.
        self.manager.admin_sign_in_controller.authenticated = True
        try:
            kwargs["http_method"] = http_method
            # The file response case is specific to the bulk circulation
            # events route where a CSV file is returned.
            if kwargs.get("file_response", None) is not None:
                self.assert_file_response(url, *args, **kwargs)
            else:
                self.assert_request_calls(url, method, *args, **kwargs)
        finally:
            # Un-set authentication for the benefit of future
            # assertions in this test function.
            self.manager.admin_sign_in_controller.authenticated = False

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

    def assert_file_response(self, url, *args, **kwargs):
        http_method = kwargs.pop("http_method", "GET")
        response = self.request(url, http_method)

        assert response.headers["Content-type"] == "text/csv"

    def assert_redirect_call(self, url, *args, **kwargs):
        # Correctly render the sign in again template when the admin
        # is authenticated and there is a csrf token.
        self.manager.admin_sign_in_controller.csrf_token = True
        self.manager.admin_sign_in_controller.authenticated = True
        http_method = kwargs.pop("http_method", "GET")
        response = self.request(url, http_method)

        # A Flask template string is returned.
        assert "You are now logged in" in response

        # Even if the admin is authenticated but there is no
        # csrf token, a redirect will occur to sign the admin in.
        self.manager.admin_sign_in_controller.csrf_token = False
        response = self.request(url, http_method)

        assert 302 == response.status_code
        assert "Redirecting..." in response.get_data(as_text=True)

        # If there is a csrf token but the Admin is not authenticated,
        # redirect them.

        self.manager.admin_sign_in_controller.csrf_token = True
        self.manager.admin_sign_in_controller.authenticated = False
        # For this case we want the function to return a problem detail.
        self.manager.admin_sign_in_controller.authenticated_problem_detail = True
        response = self.request(url, http_method)

        assert 302 == response.status_code
        assert "Redirecting..." in response.get_data(as_text=True)

        # Not being authenticated and not having a csrf token fail
        # redirects the admin to sign in again.
        self.manager.admin_sign_in_controller.csrf_token = False
        self.manager.admin_sign_in_controller.authenticated = False
        response = self.request(url, http_method)

        # No admin or csrf token so redirect.
        assert 302 == response.status_code
        assert "Redirecting..." in response.get_data(as_text=True)

        self.manager.admin_sign_in_controller.authenticated_problem_detail = False


@pytest.fixture(scope="function")
def admin_route_fixture(
    db: DatabaseTransactionFixture, controller_fixture: ControllerFixture
) -> Generator[AdminRouteFixture, Any, None]:
    fix = AdminRouteFixture(db, controller_fixture)
    yield fix
    fix.close()


class TestAdminSignIn:
    CONTROLLER_NAME = "admin_sign_in_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_sign_in_with_password(self, fixture: AdminRouteFixture):
        url = "/admin/sign_in_with_password"
        fixture.assert_request_calls(
            url, fixture.controller.password_sign_in, http_method="POST"  # type: ignore
        )

        fixture.assert_supported_methods(url, "POST")

    def test_sign_in(self, fixture: AdminRouteFixture):
        url = "/admin/sign_in"
        fixture.assert_request_calls(url, fixture.controller.sign_in)  # type: ignore

    def test_sign_out(self, fixture: AdminRouteFixture):
        url = "/admin/sign_out"
        fixture.assert_authenticated_request_calls(url, fixture.controller.sign_out)  # type: ignore

    def test_change_password(self, fixture: AdminRouteFixture):
        url = "/admin/change_password"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.change_password, http_method="POST"  # type: ignore
        )
        fixture.assert_supported_methods(url, "POST")

    def test_sign_in_again(self, fixture: AdminRouteFixture):
        url = "/admin/sign_in_again"
        fixture.assert_redirect_call(url)

    def test_redirect(self, fixture: AdminRouteFixture):
        url = "/admin"
        response = fixture.request(url)

        assert 302 == response.status_code
        assert "Redirecting..." in response.get_data(as_text=True)


class TestAdminWork:
    CONTROLLER_NAME = "admin_work_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_details(self, fixture: AdminRouteFixture):
        url = "/admin/works/<identifier_type>/an/identifier"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.details, "<identifier_type>", "an/identifier"  # type: ignore
        )
        fixture.assert_supported_methods(url, "GET")

    def test_classifications(self, fixture: AdminRouteFixture):
        url = "/admin/works/<identifier_type>/an/identifier/classifications"
        fixture.assert_authenticated_request_calls(
            url,
            fixture.controller.classifications,  # type: ignore
            "<identifier_type>",
            "an/identifier",
        )
        fixture.assert_supported_methods(url, "GET")

    def test_custom_lists(self, fixture: AdminRouteFixture):
        url = "/admin/works/<identifier_type>/an/identifier/lists"
        fixture.assert_authenticated_request_calls(
            url,
            fixture.controller.custom_lists,  # type: ignore
            "<identifier_type>",
            "an/identifier",
            http_method="POST",
        )
        fixture.assert_supported_methods(url, "GET", "POST")

    def test_edit(self, fixture: AdminRouteFixture):
        url = "/admin/works/<identifier_type>/an/identifier/edit"
        fixture.assert_authenticated_request_calls(
            url,
            fixture.controller.edit,  # type: ignore
            "<identifier_type>",
            "an/identifier",
            http_method="POST",
        )

    def test_suppress(self, fixture: AdminRouteFixture):
        url = "/admin/works/<identifier_type>/an/identifier/suppress"
        fixture.assert_authenticated_request_calls(
            url,
            fixture.controller.suppress,  # type: ignore
            "<identifier_type>",
            "an/identifier",
            http_method="POST",
        )

    def test_unsuppress(self, fixture: AdminRouteFixture):
        url = "/admin/works/<identifier_type>/an/identifier/unsuppress"
        fixture.assert_authenticated_request_calls(
            url,
            fixture.controller.unsuppress,  # type: ignore
            "<identifier_type>",
            "an/identifier",
            http_method="POST",
        )

    def test_refresh_metadata(self, fixture: AdminRouteFixture):
        url = "/admin/works/<identifier_type>/an/identifier/refresh"
        fixture.assert_authenticated_request_calls(
            url,
            fixture.controller.refresh_metadata,  # type: ignore
            "<identifier_type>",
            "an/identifier",
            http_method="POST",
        )

    def test_edit_classifications(self, fixture: AdminRouteFixture):
        url = "/admin/works/<identifier_type>/an/identifier/edit_classifications"
        fixture.assert_authenticated_request_calls(
            url,
            fixture.controller.edit_classifications,  # type: ignore
            "<identifier_type>",
            "an/identifier",
            http_method="POST",
        )

    def test_roles(self, fixture: AdminRouteFixture):
        url = "/admin/roles"
        fixture.assert_request_calls(url, fixture.controller.roles)  # type: ignore

    def test_languages(self, fixture: AdminRouteFixture):
        url = "/admin/languages"
        fixture.assert_request_calls(url, fixture.controller.languages)  # type: ignore

    def test_media(self, fixture: AdminRouteFixture):
        url = "/admin/media"
        fixture.assert_request_calls(url, fixture.controller.media)  # type: ignore

    def test_right_status(self, fixture: AdminRouteFixture):
        url = "/admin/rights_status"
        fixture.assert_request_calls(url, fixture.controller.rights_status)  # type: ignore


class TestAdminFeed:
    CONTROLLER_NAME = "admin_feed_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_suppressed(self, fixture: AdminRouteFixture):
        url = "/admin/suppressed"
        fixture.assert_authenticated_request_calls(url, fixture.controller.suppressed)  # type: ignore

    def test_genres(self, fixture: AdminRouteFixture):
        url = "/admin/genres"
        fixture.assert_authenticated_request_calls(url, fixture.controller.genres)  # type: ignore


class TestAdminDashboard:
    CONTROLLER_NAME = "admin_dashboard_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_bulk_circulation_events(self, fixture: AdminRouteFixture):
        url = "/admin/bulk_circulation_events"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.bulk_circulation_events, file_response=True  # type: ignore
        )

    def test_circulation_events(self, fixture: AdminRouteFixture):
        url = "/admin/circulation_events"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.circulation_events  # type: ignore
        )


class TestAdminLibrarySettings:
    CONTROLLER_NAME = "admin_library_settings_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_process_libraries(self, fixture: AdminRouteFixture):
        url = "/admin/libraries"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_libraries  # type: ignore
        )
        fixture.assert_supported_methods(url, "GET", "POST")

    def test_delete(self, fixture: AdminRouteFixture):
        url = "/admin/library/<library_uuid>"
        fixture.assert_authenticated_request_calls(
            url,
            fixture.controller.process_delete,  # type: ignore
            "<library_uuid>",
            http_method="DELETE",
        )
        fixture.assert_supported_methods(url, "DELETE")


class TestAdminCollectionSettings:
    CONTROLLER_NAME = "admin_collection_settings_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_process_get(self, fixture: AdminRouteFixture):
        url = "/admin/collections"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_collections  # type: ignore
        )
        fixture.assert_supported_methods(url, "GET", "POST")

    def test_process_post(self, fixture: AdminRouteFixture):
        url = "/admin/collection/<collection_id>"
        fixture.assert_authenticated_request_calls(
            url,
            fixture.controller.process_delete,  # type: ignore
            "<collection_id>",
            http_method="DELETE",
        )
        fixture.assert_supported_methods(url, "DELETE")


class TestAdminCollectionSelfTests:
    CONTROLLER_NAME = "admin_collection_self_tests_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_process_collection_self_tests(self, fixture: AdminRouteFixture):
        url = "/admin/collection_self_tests/<identifier>"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_collection_self_tests, "<identifier>"  # type: ignore
        )


class TestAdminIndividualAdminSettings:
    CONTROLLER_NAME = "admin_individual_admin_settings_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_process_individual_admins(self, fixture: AdminRouteFixture):
        url = "/admin/individual_admins"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_individual_admins  # type: ignore
        )
        fixture.assert_supported_methods(url, "GET", "POST")

    def test_process_delete(self, fixture: AdminRouteFixture):
        url = "/admin/individual_admin/<email>"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_delete, "<email>", http_method="DELETE"  # type: ignore
        )
        fixture.assert_supported_methods(url, "DELETE")


class TestAdminPatronAuthServices:
    CONTROLLER_NAME = "admin_patron_auth_services_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_process_patron_auth_services(self, fixture: AdminRouteFixture):
        url = "/admin/patron_auth_services"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_patron_auth_services  # type: ignore
        )
        fixture.assert_supported_methods(url, "GET", "POST")

    def test_process_delete(self, fixture: AdminRouteFixture):
        url = "/admin/patron_auth_service/<service_id>"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_delete, "<service_id>", http_method="DELETE"  # type: ignore
        )
        fixture.assert_supported_methods(url, "DELETE")


class TestAdminPatronAuthServicesSelfTests:
    CONTROLLER_NAME = "admin_patron_auth_service_self_tests_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_process_patron_auth_service_self_tests(self, fixture: AdminRouteFixture):
        url = "/admin/patron_auth_service_self_tests/<identifier>"
        fixture.assert_authenticated_request_calls(
            url,
            fixture.controller.process_patron_auth_service_self_tests,  # type: ignore
            "<identifier>",
        )
        fixture.assert_supported_methods(url, "GET", "POST")


class TestAdminPatron:
    CONTROLLER_NAME = "admin_patron_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_lookup_patron(self, fixture: AdminRouteFixture):
        url = "/admin/manage_patrons"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.lookup_patron, http_method="POST"  # type: ignore
        )
        fixture.assert_supported_methods(url, "POST")

    def test_reset_adobe_id(self, fixture: AdminRouteFixture):
        url = "/admin/manage_patrons/reset_adobe_id"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.reset_adobe_id, http_method="POST"  # type: ignore
        )
        fixture.assert_supported_methods(url, "POST")


class TestAdminMetadataServices:
    CONTROLLER_NAME = "admin_metadata_services_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_process_metadata_services(self, fixture: AdminRouteFixture):
        url = "/admin/metadata_services"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_metadata_services  # type: ignore
        )
        fixture.assert_supported_methods(url, "GET", "POST")

    def test_process_delete(self, fixture: AdminRouteFixture):
        url = "/admin/metadata_service/<service_id>"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_delete, "<service_id>", http_method="DELETE"  # type: ignore
        )
        fixture.assert_supported_methods(url, "DELETE")


class TestAdminSearchServices:
    CONTROLLER_NAME = "admin_search_services_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_process_services(self, fixture: AdminRouteFixture):
        url = "/admin/search_services"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_services  # type: ignore
        )
        fixture.assert_supported_methods(url, "GET", "POST")

    def test_process_delete(self, fixture: AdminRouteFixture):
        url = "/admin/search_service/<service_id>"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_delete, "<service_id>", http_method="DELETE"  # type: ignore
        )
        fixture.assert_supported_methods(url, "DELETE")


class TestAdminSearchServicesSelfTests:
    CONTROLLER_NAME = "admin_search_service_self_tests_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_process_search_service_self_tests(self, fixture: AdminRouteFixture):
        url = "/admin/search_service_self_tests/<identifier>"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_search_service_self_tests, "<identifier>"  # type: ignore
        )
        fixture.assert_supported_methods(url, "GET", "POST")


class TestAdminCatalogServices:
    CONTROLLER_NAME = "admin_catalog_services_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_process_catalog_services(self, fixture: AdminRouteFixture):
        url = "/admin/catalog_services"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_catalog_services  # type: ignore
        )
        fixture.assert_supported_methods(url, "GET", "POST")

    def test_process_delete(self, fixture: AdminRouteFixture):
        url = "/admin/catalog_service/<service_id>"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_delete, "<service_id>", http_method="DELETE"  # type: ignore
        )
        fixture.assert_supported_methods(url, "DELETE")


class TestAdminDiscoveryServices:
    CONTROLLER_NAME = "admin_discovery_services_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_process_discovery_services(self, fixture: AdminRouteFixture):
        url = "/admin/discovery_services"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_discovery_services  # type: ignore
        )
        fixture.assert_supported_methods(url, "GET", "POST")

    def test_process_delete(self, fixture: AdminRouteFixture):
        url = "/admin/discovery_service/<service_id>"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_delete, "<service_id>", http_method="DELETE"  # type: ignore
        )
        fixture.assert_supported_methods(url, "DELETE")


class TestAdminSitewideServices:
    CONTROLLER_NAME = "admin_sitewide_configuration_settings_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_process_services(self, fixture: AdminRouteFixture):
        url = "/admin/sitewide_settings"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_services  # type: ignore
        )
        fixture.assert_supported_methods(url, "GET", "POST")

    def test_process_delete(self, fixture: AdminRouteFixture):
        url = "/admin/sitewide_setting/<key>"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_delete, "<key>", http_method="DELETE"  # type: ignore
        )
        fixture.assert_supported_methods(url, "DELETE")


class TestAdminDiscoveryServiceLibraryRegistrations:
    CONTROLLER_NAME = "admin_discovery_service_library_registrations_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_process_discovery_service_library_registrations(
        self, fixture: AdminRouteFixture
    ):
        url = "/admin/discovery_service_library_registrations"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.process_discovery_service_library_registrations  # type: ignore
        )
        fixture.assert_supported_methods(url, "GET", "POST")


class TestAdminCustomListsServices:
    CONTROLLER_NAME = "admin_custom_lists_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_custom_lists(self, fixture: AdminRouteFixture):
        url = "/admin/custom_lists"
        fixture.assert_authenticated_request_calls(url, fixture.controller.custom_lists)  # type: ignore
        fixture.assert_supported_methods(url, "GET", "POST")

    def test_custom_list(self, fixture: AdminRouteFixture):
        url = "/admin/custom_list/<list_id>"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.custom_list, "<list_id>"  # type: ignore
        )
        fixture.assert_supported_methods(url, "GET", "POST", "DELETE")


class TestAdminLanes:
    CONTROLLER_NAME = "admin_lanes_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_lanes(self, fixture: AdminRouteFixture):
        url = "/admin/lanes"
        fixture.assert_authenticated_request_calls(url, fixture.controller.lanes)  # type: ignore
        fixture.assert_supported_methods(url, "GET", "POST")

    def test_lane(self, fixture: AdminRouteFixture):
        url = "/admin/lane/<lane_identifier>"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.lane, "<lane_identifier>", http_method="DELETE"  # type: ignore
        )
        fixture.assert_supported_methods(url, "DELETE")

    def test_show_lane(self, fixture: AdminRouteFixture):
        url = "/admin/lane/<lane_identifier>/show"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.show_lane, "<lane_identifier>", http_method="POST"  # type: ignore
        )
        fixture.assert_supported_methods(url, "POST")

    def test_hide_lane(self, fixture: AdminRouteFixture):
        url = "/admin/lane/<lane_identifier>/hide"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.hide_lane, "<lane_identifier>", http_method="POST"  # type: ignore
        )
        fixture.assert_supported_methods(url, "POST")

    def test_reset(self, fixture: AdminRouteFixture):
        url = "/admin/lanes/reset"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.reset, http_method="POST"  # type: ignore
        )
        fixture.assert_supported_methods(url, "POST")

    def test_change_order(self, fixture: AdminRouteFixture):
        url = "/admin/lanes/change_order"
        fixture.assert_authenticated_request_calls(
            url, fixture.controller.change_order, http_method="POST"  # type: ignore
        )
        fixture.assert_supported_methods(url, "POST")


class TestTimestamps:
    CONTROLLER_NAME = "timestamps_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_diagnostics(self, fixture: AdminRouteFixture):
        url = "/admin/diagnostics"
        fixture.assert_authenticated_request_calls(url, fixture.controller.diagnostics)  # type: ignore


class TestAdminView:
    CONTROLLER_NAME = "admin_view_controller"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_admin_view(self, fixture: AdminRouteFixture):
        url = "/admin/web/"
        fixture.assert_request_calls(url, fixture.controller, None, None, path=None)

        url = "/admin/web/collection/a/collection/book/a/book"
        fixture.assert_request_calls(
            url, fixture.controller, "a/collection", "a/book", path=None
        )

        url = "/admin/web/collection/a/collection"
        fixture.assert_request_calls(
            url, fixture.controller, "a/collection", None, path=None
        )

        url = "/admin/web/book/a/book"
        fixture.assert_request_calls(url, fixture.controller, None, "a/book", path=None)

        url = "/admin/web/a/path"
        fixture.assert_request_calls(url, fixture.controller, None, None, path="a/path")


class TestAdminStatic:
    CONTROLLER_NAME = "static_files"

    @pytest.fixture(scope="function")
    def fixture(self, admin_route_fixture: AdminRouteFixture) -> AdminRouteFixture:
        admin_route_fixture.set_controller_name(self.CONTROLLER_NAME)
        return admin_route_fixture

    def test_static_file(self, fixture: AdminRouteFixture):
        # Go to the back to the root folder to get the right
        # path for the static files.
        root_path = Path(__file__).parent.parent.parent.parent
        local_path = (
            root_path
            / "api/admin/node_modules/@thepalaceproject/circulation-admin/dist"
        )

        url = "/admin/static/circulation-admin.js"
        fixture.assert_request_calls(
            url, fixture.controller.static_file, str(local_path), "circulation-admin.js"  # type: ignore
        )

        url = "/admin/static/circulation-admin.css"
        fixture.assert_request_calls(
            url,
            fixture.controller.static_file,  # type: ignore
            str(local_path),
            "circulation-admin.css",
        )


def test_returns_json_or_response_or_problem_detail():
    @routes.returns_json_or_response_or_problem_detail
    def mock_responses(response):
        if isinstance(response, ProblemError):
            raise response
        return response

    problem = ProblemDetail(
        "http://problem", status_code=400, title="Title", detail="Is a detail"
    )

    # Both raising an error and responding with a problem detail are equivalent
    assert mock_responses(ProblemError(problem)) == problem.response
    assert mock_responses(problem) == problem.response

    # A json provides a response object
    with flask.app.Flask(__name__).test_request_context():
        assert mock_responses({"status": True}).json == {"status": True}

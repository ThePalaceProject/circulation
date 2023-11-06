from unittest.mock import MagicMock

import flask
import pytest
from flask import Response, url_for
from requests_mock import Mocker
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import MISSING_SERVICE, NO_SUCH_LIBRARY
from api.discovery.opds_registration import OpdsRegistrationService
from api.problem_details import REMOTE_INTEGRATION_FAILED
from core.model import AdminRole, create
from core.model.discovery_service_registration import (
    DiscoveryServiceRegistration,
    RegistrationStage,
    RegistrationStatus,
)
from core.problem_details import INVALID_INPUT
from core.util.problem_detail import ProblemDetail, ProblemError
from tests.fixtures.api_admin import AdminControllerFixture
from tests.fixtures.database import IntegrationConfigurationFixture
from tests.fixtures.library import LibraryFixture


class TestLibraryRegistration:
    """Test the process of registering a library with a OpdsRegistrationService."""

    def test_discovery_service_library_registrations_get(
        self,
        admin_ctrl_fixture: AdminControllerFixture,
        create_integration_configuration: IntegrationConfigurationFixture,
        library_fixture: LibraryFixture,
        requests_mock: Mocker,
    ) -> None:
        db = admin_ctrl_fixture.ctrl.db

        # Here's a discovery service.
        discovery_service = create_integration_configuration.discovery_service(
            url="http://service-url.com/"
        )

        # We successfully registered this library with the service.
        succeeded = library_fixture.library(
            name="Library 1",
            short_name="L1",
        )
        registration, _ = create(
            db.session,
            DiscoveryServiceRegistration,
            library=succeeded,
            integration=discovery_service,
        )
        registration.status = RegistrationStatus.SUCCESS
        registration.stage = RegistrationStage.PRODUCTION

        # We tried to register this library with the service but were
        # unsuccessful.
        failed = library_fixture.library(
            name="Library 2",
            short_name="L2",
        )
        registration, _ = create(
            db.session,
            DiscoveryServiceRegistration,
            library=failed,
            integration=discovery_service,
        )
        registration.status = RegistrationStatus.FAILURE
        registration.stage = RegistrationStage.TESTING

        # We've never tried to register this library with the service.
        unregistered = library_fixture.library(
            name="Library 3",
            short_name="L3",
        )

        # When a client sends a GET request to the controller, the
        # controller is going to call
        # OpdsRegistrationService.fetch_registration_document() to try and find
        # the discovery services' terms of service. That's going to
        # make one or two HTTP requests.

        # First, let's try the scenario where the discovery service is
        # working and has a terms-of-service.

        # In this case we'll make two requests. The first request will
        # ask for the root catalog, where we'll look for a
        # registration link.
        root_catalog = dict(links=[dict(href="http://register-here/", rel="register")])
        requests_mock.get(
            "http://service-url.com/",
            json=root_catalog,
            headers={"Content-Type": OpdsRegistrationService.OPDS_2_TYPE},
        )

        # The second request will fetch that registration link -- then
        # we'll look for TOS data inside.
        registration_document = dict(
            links=[
                dict(rel="terms-of-service", type="text/html", href="http://tos/"),
                dict(
                    rel="terms-of-service",
                    type="text/html",
                    href="data:text/html;charset=utf-8;base64,PHA+SG93IGFib3V0IHRoYXQgVE9TPC9wPg==",
                ),
            ]
        )
        requests_mock.get(
            "http://register-here/",
            json=registration_document,
            headers={"Content-Type": OpdsRegistrationService.OPDS_2_TYPE},
        )

        controller = (
            admin_ctrl_fixture.ctrl.manager.admin_discovery_service_library_registrations_controller
        )
        m = controller.process_discovery_service_library_registrations
        with admin_ctrl_fixture.request_context_with_admin("/", method="GET"):
            # When the user lacks the SYSTEM_ADMIN role, the
            # controller won't even start processing their GET
            # request.
            pytest.raises(AdminNotAuthorized, m)

            # Add the admin role and try again.
            admin_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)

            response = m()
            # The document we get back from the controller is a
            # dictionary with useful information on all known
            # discovery integrations -- just one, in this case.
            assert isinstance(response, dict)
            [service] = response["library_registrations"]
            assert discovery_service.id == service["id"]

            # The two mock HTTP requests we predicted actually
            # happened.  The target of the first request is the URL to
            # the discovery service's main catalog. The second request
            # is to the "register" link found in that catalog.
            assert ["service-url.com", "register-here"] == [
                r.hostname for r in requests_mock.request_history
            ]

            # The TOS link and TOS HTML snippet were recovered from
            # the registration document served in response to the
            # second HTTP request, and included in the dictionary.
            assert "http://tos/" == service["terms_of_service_link"]
            assert "<p>How about that TOS</p>" == service["terms_of_service_html"]
            assert None == service["access_problem"]

            # The dictionary includes a 'libraries' object, a list of
            # dictionaries with information about the relationships
            # between this discovery integration and every library
            # that's tried to register with it.
            info1, info2 = service["libraries"]

            # Here's the library that successfully registered.
            assert info1 == dict(
                short_name=succeeded.short_name, status="success", stage="production"
            )

            # And here's the library that tried to register but
            # failed.
            assert info2 == dict(
                short_name=failed.short_name, status="failure", stage="testing"
            )

            # Note that `unregistered`, the library that never tried
            # to register with this discovery service, is not included.

            # Now let's try the controller method again, except this
            # time the discovery service's web server is down. The
            # first request will return a ProblemDetail document, and
            # there will be no second request.
            requests_mock.reset()
            requests_mock.get(
                "http://service-url.com/",
                json=REMOTE_INTEGRATION_FAILED.response[0],
                status_code=502,
            )

            response = m()

            # Everything looks good, except that there's no TOS data
            # available.
            assert isinstance(response, dict)
            [service] = response["library_registrations"]
            assert discovery_service.id == service["id"]
            assert 2 == len(service["libraries"])
            assert service["terms_of_service_link"] is None
            assert service["terms_of_service_html"] is None

            # The problem detail document that prevented the TOS data
            # from showing up has been converted to a dictionary and
            # included in the dictionary of information for this
            # discovery service.
            assert REMOTE_INTEGRATION_FAILED.uri == service["access_problem"]["type"]

    def test_discovery_service_library_registrations_post(
        self,
        admin_ctrl_fixture: AdminControllerFixture,
        create_integration_configuration: IntegrationConfigurationFixture,
        library_fixture: LibraryFixture,
    ) -> None:
        """Test what might happen when you POST to
        discovery_service_library_registrations.
        """

        controller = (
            admin_ctrl_fixture.manager.admin_discovery_service_library_registrations_controller
        )
        m = controller.process_discovery_service_library_registrations

        # Here, the user doesn't have permission to start the
        # registration process.
        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
            pytest.raises(AdminNotAuthorized, m)

        admin_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)

        # We might not get an integration ID parameter.
        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict()
            response = m()
            assert isinstance(response, ProblemDetail)
            assert INVALID_INPUT.uri == response.uri

        # The integration ID might not correspond to a valid
        # ExternalIntegration.
        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("integration_id", "1234"),
                ]
            )
            response = m()
            assert isinstance(response, ProblemDetail)
            assert MISSING_SERVICE == response

        # Create an IntegrationConfiguration to avoid that problem in future tests.
        discovery_service = create_integration_configuration.discovery_service(
            url="http://register-here.com/"
        )

        # We might not get a library short name.
        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("integration_id", str(discovery_service.id)),
                ]
            )
            response = m()
            assert isinstance(response, ProblemDetail)
            assert INVALID_INPUT.uri == response.uri

        # The library name might not correspond to a real library.
        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("integration_id", str(discovery_service.id)),
                    ("library_short_name", "not-a-library"),
                ]
            )
            response = m()
            assert NO_SUCH_LIBRARY == response

        # Take care of that problem.
        library = library_fixture.library()

        # We might not get a registration stage.
        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("integration_id", str(discovery_service.id)),
                    ("library_short_name", str(library.short_name)),
                ]
            )
            response = m()
            assert isinstance(response, ProblemDetail)
            assert INVALID_INPUT.uri == response.uri

        # The registration stage might not be valid.
        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("integration_id", str(discovery_service.id)),
                    ("library_short_name", str(library.short_name)),
                    ("registration_stage", "not-a-stage"),
                ]
            )
            response = m()
            assert isinstance(response, ProblemDetail)
            assert INVALID_INPUT.uri == response.uri

        form = ImmutableMultiDict(
            [
                ("integration_id", str(discovery_service.id)),
                ("library_short_name", str(library.short_name)),
                ("registration_stage", RegistrationStage.TESTING.value),
            ]
        )

        # The registration may fail for some reason.
        mock_registry = MagicMock(spec=OpdsRegistrationService)
        mock_registry.register_library.side_effect = ProblemError(
            problem_detail=REMOTE_INTEGRATION_FAILED
        )
        controller.look_up_registry = MagicMock(return_value=mock_registry)

        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = form
            response = m()
            assert REMOTE_INTEGRATION_FAILED == response

        # But if that doesn't happen, success!
        mock_registry = MagicMock(spec=OpdsRegistrationService)
        mock_registry.register_library.return_value = True
        controller.look_up_registry = MagicMock(return_value=mock_registry)

        with admin_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = form
            response = controller.process_discovery_service_library_registrations()
            assert isinstance(response, Response)
            assert 200 == response.status_code

            # register_library() was called with the arguments we would expect.
            mock_registry.register_library.assert_called_once_with(
                library, RegistrationStage.TESTING, url_for
            )

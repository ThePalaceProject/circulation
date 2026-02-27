"""Tests for patron debug authentication controller endpoints."""

from unittest.mock import patch

import flask
import pytest
from werkzeug.datastructures import ImmutableMultiDict

from palace.manager.api.admin.exceptions import AdminNotAuthorized
from palace.manager.api.admin.problem_details import MISSING_INTEGRATION
from palace.manager.api.authentication.base import PatronAuthResult
from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.integration.goals import Goals
from palace.manager.integration.patron_auth.sip2.provider import (
    SIP2AuthenticationProvider,
    SIP2LibrarySettings,
    SIP2Settings,
)
from palace.manager.sqlalchemy.model.admin import AdminRole
from palace.manager.util.problem_detail import ProblemDetail
from tests.fixtures.api_admin import AdminControllerFixture
from tests.fixtures.api_controller import ControllerFixture


class PatronDebugControllerFixture(AdminControllerFixture):
    def __init__(self, controller_fixture: ControllerFixture):
        super().__init__(controller_fixture)
        self.admin.add_role(AdminRole.LIBRARIAN, self.ctrl.db.default_library())


@pytest.fixture(scope="function")
def patron_debug_fixture(
    controller_fixture: ControllerFixture,
) -> PatronDebugControllerFixture:
    return PatronDebugControllerFixture(controller_fixture)


class TestGetAuthMethods:
    def test_requires_librarian_role(
        self, patron_debug_fixture: PatronDebugControllerFixture
    ):
        """Unauthenticated admin gets AdminNotAuthorized."""
        # Remove the librarian role
        patron_debug_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, patron_debug_fixture.ctrl.db.default_library()
        )
        with patron_debug_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                patron_debug_fixture.manager.admin_patron_controller.get_auth_methods,
            )

    def test_returns_auth_methods(
        self, patron_debug_fixture: PatronDebugControllerFixture
    ):
        """Library with auth integrations returns method info list."""
        with patron_debug_fixture.request_context_with_library_and_admin("/"):
            response = (
                patron_debug_fixture.manager.admin_patron_controller.get_auth_methods()
            )
            assert not isinstance(response, ProblemDetail)
            # The test fixture sets up a default auth integration, so there should be at least one
            assert isinstance(response["authMethods"], list)

    def test_with_sip2_integration(
        self, patron_debug_fixture: PatronDebugControllerFixture
    ):
        """Library with a SIP2 integration returns method info with supports_debug=True."""
        db = patron_debug_fixture.ctrl.db
        library = db.default_library()

        # Create a SIP2 integration for this library
        integration = db.integration_configuration(
            SIP2AuthenticationProvider,
            Goals.PATRON_AUTH_GOAL,
            libraries=[library],
            name="Test SIP2",
            settings=SIP2Settings(
                url="sip.example.com",
                port=6001,
                identifier_label="Library Card",
                password_label="PIN",
            ),
        )
        db.integration_library_configuration(
            integration,
            library,
            settings=SIP2LibrarySettings(),
        )

        with patron_debug_fixture.request_context_with_library_and_admin("/"):
            response = (
                patron_debug_fixture.manager.admin_patron_controller.get_auth_methods()
            )
            assert not isinstance(response, ProblemDetail)
            methods = response["authMethods"]
            # Find the SIP2 method we just created
            sip2_methods = [m for m in methods if m["name"] == "Test SIP2"]
            assert len(sip2_methods) == 1
            method = sip2_methods[0]
            assert method["supportsDebug"] is True
            assert method["supportsPassword"] is True
            assert method["identifierLabel"] == "Library Card"
            assert method["passwordLabel"] == "PIN"


class TestDebugAuth:
    def test_requires_librarian_role(
        self, patron_debug_fixture: PatronDebugControllerFixture
    ):
        """Unauthenticated admin gets AdminNotAuthorized."""
        patron_debug_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, patron_debug_fixture.ctrl.db.default_library()
        )
        with patron_debug_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(
                [("integration_id", "1"), ("username", "test")]
            )
            pytest.raises(
                AdminNotAuthorized,
                patron_debug_fixture.manager.admin_patron_controller.debug_auth,
            )

    def test_missing_parameters(
        self, patron_debug_fixture: PatronDebugControllerFixture
    ):
        """Missing integration_id or username returns INVALID_INPUT."""
        with patron_debug_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict([])
            response = patron_debug_fixture.manager.admin_patron_controller.debug_auth()
            assert isinstance(response, ProblemDetail)
            assert response.uri == INVALID_INPUT.uri

    def test_invalid_integration_id(
        self, patron_debug_fixture: PatronDebugControllerFixture
    ):
        """Non-numeric integration_id returns INVALID_INPUT."""
        with patron_debug_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(
                [("integration_id", "not-a-number"), ("username", "test_user")]
            )
            response = patron_debug_fixture.manager.admin_patron_controller.debug_auth()
            assert isinstance(response, ProblemDetail)
            assert response.uri == INVALID_INPUT.uri

    def test_missing_integration(
        self, patron_debug_fixture: PatronDebugControllerFixture
    ):
        """Non-existent integration_id returns MISSING_INTEGRATION."""
        with patron_debug_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(
                [("integration_id", "99999"), ("username", "test_user")]
            )
            response = patron_debug_fixture.manager.admin_patron_controller.debug_auth()
            assert isinstance(response, ProblemDetail)
            assert response.uri == MISSING_INTEGRATION.uri

    def test_integration_not_for_library(
        self, patron_debug_fixture: PatronDebugControllerFixture
    ):
        """Integration exists but is not configured for this library."""
        db = patron_debug_fixture.ctrl.db
        # Create an integration NOT associated with this library
        other_library = db.library(short_name="other")
        integration = db.integration_configuration(
            SIP2AuthenticationProvider,
            Goals.PATRON_AUTH_GOAL,
            libraries=[other_library],
            name="Other SIP2",
            settings=SIP2Settings(url="sip.example.com", port=6001),
        )
        db.integration_library_configuration(
            integration,
            other_library,
            settings=SIP2LibrarySettings(),
        )

        with patron_debug_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(
                [("integration_id", str(integration.id)), ("username", "test_user")]
            )
            response = patron_debug_fixture.manager.admin_patron_controller.debug_auth()
            assert isinstance(response, ProblemDetail)
            assert response.uri == MISSING_INTEGRATION.uri

    def test_successful_debug_auth(
        self, patron_debug_fixture: PatronDebugControllerFixture
    ):
        """Successful debug_auth returns results from patron_debug()."""
        db = patron_debug_fixture.ctrl.db
        library = db.default_library()

        integration = db.integration_configuration(
            SIP2AuthenticationProvider,
            Goals.PATRON_AUTH_GOAL,
            libraries=[library],
            name="Test SIP2",
            settings=SIP2Settings(url="sip.example.com", port=6001),
        )
        db.integration_library_configuration(
            integration,
            library,
            settings=SIP2LibrarySettings(),
        )

        mock_results = [
            PatronAuthResult(label="Step 1", success=True, details="ok"),
            PatronAuthResult(
                label="Step 2", success=False, details={"error": "bad password"}
            ),
        ]

        with patron_debug_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("integration_id", str(integration.id)),
                    ("username", "test_user"),
                    ("password", "test_pass"),
                ]
            )
            # Mock the patron_debug method to avoid actually connecting to SIP2
            with patch.object(
                SIP2AuthenticationProvider,
                "patron_debug",
                return_value=mock_results,
            ):
                response = (
                    patron_debug_fixture.manager.admin_patron_controller.debug_auth()
                )

            assert not isinstance(response, ProblemDetail)
            assert len(response["results"]) == 2
            assert response["results"][0]["label"] == "Step 1"
            assert response["results"][0]["success"] is True
            assert response["results"][1]["success"] is False

    def test_patron_debug_exception(
        self, patron_debug_fixture: PatronDebugControllerFixture
    ):
        """If patron_debug() raises, the controller catches and returns an error result."""
        db = patron_debug_fixture.ctrl.db
        library = db.default_library()

        integration = db.integration_configuration(
            SIP2AuthenticationProvider,
            Goals.PATRON_AUTH_GOAL,
            libraries=[library],
            name="Test SIP2",
            settings=SIP2Settings(url="sip.example.com", port=6001),
        )
        db.integration_library_configuration(
            integration,
            library,
            settings=SIP2LibrarySettings(),
        )

        with patron_debug_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("integration_id", str(integration.id)),
                    ("username", "test_user"),
                    ("password", "test_pass"),
                ]
            )
            with patch.object(
                SIP2AuthenticationProvider,
                "patron_debug",
                side_effect=RuntimeError("something went wrong"),
            ):
                response = (
                    patron_debug_fixture.manager.admin_patron_controller.debug_auth()
                )

            assert not isinstance(response, ProblemDetail)
            assert len(response["results"]) == 1
            result = response["results"][0]
            assert result["label"] == "Unexpected Error"
            assert result["success"] is False
            assert result["details"] == "RuntimeError: something went wrong"

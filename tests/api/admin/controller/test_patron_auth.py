from __future__ import annotations

import json
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import flask
import pytest
from _pytest.monkeypatch import MonkeyPatch
from flask import Response
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.controller.patron_auth_services import PatronAuthServicesController
from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import (
    CANNOT_CHANGE_PROTOCOL,
    FAILED_TO_RUN_SELF_TESTS,
    INCOMPLETE_CONFIGURATION,
    INTEGRATION_NAME_ALREADY_IN_USE,
    INVALID_CONFIGURATION_OPTION,
    INVALID_LIBRARY_IDENTIFIER_RESTRICTION_REGULAR_EXPRESSION,
    MISSING_IDENTIFIER,
    MISSING_SERVICE,
    MISSING_SERVICE_NAME,
    MULTIPLE_BASIC_AUTH_SERVICES,
    NO_PROTOCOL_FOR_NEW_SERVICE,
    NO_SUCH_LIBRARY,
    UNKNOWN_PROTOCOL,
)
from api.authentication.basic import (
    BarcodeFormats,
    Keyboards,
    LibraryIdentifierRestriction,
)
from api.millenium_patron import AuthenticationMode, MilleniumPatronAPI
from api.saml.provider import SAMLWebSSOAuthenticationProvider
from api.simple_authentication import SimpleAuthenticationProvider
from api.sip import SIP2AuthenticationProvider
from core.integration.goals import Goals
from core.model import Library, get_one
from core.model.integration import IntegrationConfiguration
from core.problem_details import INVALID_INPUT
from core.selftest import HasSelfTests
from core.util.problem_detail import ProblemDetail
from tests.fixtures.flask import FlaskAppFixture

if TYPE_CHECKING:
    from tests.fixtures.authenticator import (
        MilleniumAuthIntegrationFixture,
        SamlAuthIntegrationFixture,
        SimpleAuthIntegrationFixture,
        Sip2AuthIntegrationFixture,
    )
    from tests.fixtures.database import DatabaseTransactionFixture


@pytest.fixture
def common_args() -> list[tuple[str, str]]:
    return [
        ("test_identifier", "user"),
        ("test_password", "pass"),
        ("identifier_keyboard", Keyboards.DEFAULT.value),
        ("password_keyboard", Keyboards.DEFAULT.value),
        ("identifier_barcode_format", BarcodeFormats.CODABAR.value),
    ]


@pytest.fixture
def controller(db: DatabaseTransactionFixture) -> PatronAuthServicesController:
    mock_manager = MagicMock()
    mock_manager._db = db.session
    return PatronAuthServicesController(mock_manager)


class TestPatronAuth:
    def test_patron_auth_services_get_with_no_services(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
    ):
        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller.process_patron_auth_services()

        assert isinstance(response, Response)
        response_data = response.json
        assert isinstance(response_data, dict)
        assert response_data.get("patron_auth_services") == []
        protocols = response_data.get("protocols")
        assert isinstance(protocols, list)
        assert 7 == len(protocols)
        assert "settings" in protocols[0]
        assert "library_settings" in protocols[0]

        # Test request without admin set
        with flask_app_fixture.test_request_context("/"):
            pytest.raises(
                AdminNotAuthorized,
                controller.process_patron_auth_services,
            )

    def test_patron_auth_services_get_with_simple_auth_service(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
    ):
        auth_service, _ = create_simple_auth_integration(
            test_identifier="user", test_password="pass"
        )

        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller.process_patron_auth_services()
        assert isinstance(response, Response)
        response_data = response.json
        assert isinstance(response_data, dict)
        [service] = response_data.get("patron_auth_services", [])

        assert auth_service.id == service.get("id")
        assert auth_service.name == service.get("name")
        assert SimpleAuthenticationProvider.__module__ == service.get("protocol")
        assert "user" == service.get("settings").get("test_identifier")
        assert "pass" == service.get("settings").get("test_password")
        assert [] == service.get("libraries")

        auth_service.libraries += [db.default_library()]

        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller.process_patron_auth_services()
        assert isinstance(response, Response)
        response_data = response.json
        assert isinstance(response_data, dict)
        [service] = response_data.get("patron_auth_services", [])

        assert "user" == service.get("settings").get("test_identifier")
        [library] = service.get("libraries")
        assert db.default_library().short_name == library.get("short_name")

    def test_patron_auth_services_get_with_millenium_auth_service(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        create_millenium_auth_integration: MilleniumAuthIntegrationFixture,
    ):
        auth_service, _ = create_millenium_auth_integration(
            db.default_library(),
            test_identifier="user",
            test_password="pass",
            identifier_regular_expression="u*",
            password_regular_expression="p*",
        )

        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller.process_patron_auth_services()
        assert isinstance(response, Response)
        response_data = response.json
        assert isinstance(response_data, dict)
        [service] = response_data.get("patron_auth_services", [])

        assert auth_service.id == service.get("id")
        assert MilleniumPatronAPI.__module__ == service.get("protocol")
        assert "user" == service.get("settings").get("test_identifier")
        assert "pass" == service.get("settings").get("test_password")
        assert "u*" == service.get("settings").get("identifier_regular_expression")
        assert "p*" == service.get("settings").get("password_regular_expression")
        [library] = service.get("libraries")
        assert db.default_library().short_name == library.get("short_name")

    def test_patron_auth_services_get_with_sip2_auth_service(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        create_sip2_auth_integration: Sip2AuthIntegrationFixture,
    ):
        auth_service, _ = create_sip2_auth_integration(
            db.default_library(),
            url="url",
            port="1234",
            username="user",
            password="pass",
            location_code="5",
            field_separator=",",
        )

        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller.process_patron_auth_services()
        assert isinstance(response, Response)
        response_data = response.json
        assert isinstance(response_data, dict)
        [service] = response_data.get("patron_auth_services", [])

        assert auth_service.id == service.get("id")
        assert SIP2AuthenticationProvider.__module__ == service.get("protocol")
        assert "url" == service.get("settings").get("url")
        assert "1234" == service.get("settings").get("port")
        assert "user" == service.get("settings").get("username")
        assert "pass" == service.get("settings").get("password")
        assert "5" == service.get("settings").get("location_code")
        assert "," == service.get("settings").get("field_separator")
        [library] = service.get("libraries")
        assert db.default_library().short_name == library.get("short_name")

    def test_patron_auth_services_get_with_saml_auth_service(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        create_saml_auth_integration: SamlAuthIntegrationFixture,
    ):
        auth_service, _ = create_saml_auth_integration(
            db.default_library(),
        )

        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller.process_patron_auth_services()
        assert isinstance(response, Response)
        response_data = response.json
        assert isinstance(response_data, dict)
        [service] = response_data.get("patron_auth_services", [])

        assert auth_service.id == service.get("id")
        assert SAMLWebSSOAuthenticationProvider.__module__ == service.get("protocol")
        [library] = service.get("libraries")
        assert db.default_library().short_name == library.get("short_name")

    def test_patron_auth_services_post_unknown_protocol(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
    ):
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("protocol", "Unknown"),
                ]
            )
            response = controller.process_patron_auth_services()
        assert response == UNKNOWN_PROTOCOL

    def test_patron_auth_services_post_no_protocol(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
    ):
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict([])
            response = controller.process_patron_auth_services()
        assert response == NO_PROTOCOL_FOR_NEW_SERVICE

    def test_patron_auth_services_post_missing_service(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
    ):
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("protocol", SimpleAuthenticationProvider.__module__),
                    ("id", "123"),
                ]
            )
            response = controller.process_patron_auth_services()
        assert response == MISSING_SERVICE

    def test_patron_auth_services_post_cannot_change_protocol(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
    ):
        auth_service, _ = create_simple_auth_integration()
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(auth_service.id)),
                    ("protocol", SIP2AuthenticationProvider.__module__),
                ]
            )
            response = controller.process_patron_auth_services()
        assert response == CANNOT_CHANGE_PROTOCOL

    def test_patron_auth_services_post_name_in_use(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
    ):
        auth_service, _ = create_simple_auth_integration()
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", str(auth_service.name)),
                    ("protocol", SIP2AuthenticationProvider.__module__),
                ]
            )
            response = controller.process_patron_auth_services()
        assert response == INTEGRATION_NAME_ALREADY_IN_USE

    def test_patron_auth_services_post_invalid_configuration(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        create_millenium_auth_integration: MilleniumAuthIntegrationFixture,
        common_args: list[tuple[str, str]],
    ):
        auth_service, _ = create_millenium_auth_integration()
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "some auth name"),
                    ("id", str(auth_service.id)),
                    ("protocol", MilleniumPatronAPI.__module__),
                    ("url", "http://url"),
                    ("authentication_mode", "Invalid mode"),
                    ("verify_certificate", "true"),
                ]
                + common_args
            )
            response = controller.process_patron_auth_services()
        assert isinstance(response, ProblemDetail)
        assert response.uri == INVALID_CONFIGURATION_OPTION.uri

    def test_patron_auth_services_post_incomplete_configuration(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
        common_args: list[tuple[str, str]],
    ):
        auth_service, _ = create_simple_auth_integration()
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(auth_service.id)),
                    ("protocol", SimpleAuthenticationProvider.__module__),
                ]
            )
            response = controller.process_patron_auth_services()
        assert isinstance(response, ProblemDetail)
        assert response.uri == INCOMPLETE_CONFIGURATION.uri

    def test_patron_auth_services_post_missing_patron_auth_name(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        common_args: list[tuple[str, str]],
    ):
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("protocol", SimpleAuthenticationProvider.__module__),
                ]
                + common_args
            )
            response = controller.process_patron_auth_services()
        assert isinstance(response, ProblemDetail)
        assert response == MISSING_SERVICE_NAME

    def test_patron_auth_services_post_no_such_library(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        common_args: list[tuple[str, str]],
    ):
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "testing auth name"),
                    ("protocol", SimpleAuthenticationProvider.__module__),
                    ("libraries", json.dumps([{"short_name": "not-a-library"}])),
                ]
                + common_args
            )
            response = controller.process_patron_auth_services()
        assert isinstance(response, ProblemDetail)
        assert response.uri == NO_SUCH_LIBRARY.uri

    def test_patron_auth_services_post_missing_short_name(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        common_args: list[tuple[str, str]],
    ):
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "testing auth name"),
                    ("protocol", SimpleAuthenticationProvider.__module__),
                    ("libraries", json.dumps([{}])),
                ]
                + common_args
            )
            response = controller.process_patron_auth_services()
        assert isinstance(response, ProblemDetail)
        assert response.uri == INVALID_INPUT.uri
        assert response.detail == "Invalid library settings, missing short_name."

    def test_patron_auth_services_post_missing_patron_auth_multiple_basic(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
        default_library: Library,
        common_args: list[tuple[str, str]],
    ):
        auth_service, _ = create_simple_auth_integration(default_library)
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "testing auth name"),
                    ("protocol", SimpleAuthenticationProvider.__module__),
                    (
                        "libraries",
                        json.dumps(
                            [
                                {
                                    "short_name": default_library.short_name,
                                    "library_identifier_restriction_type": LibraryIdentifierRestriction.NONE.value,
                                    "library_identifier_field": "barcode",
                                }
                            ]
                        ),
                    ),
                ]
                + common_args
            )
            response = controller.process_patron_auth_services()
        assert isinstance(response, ProblemDetail)
        assert response.uri == MULTIPLE_BASIC_AUTH_SERVICES.uri

    def test_patron_auth_services_post_invalid_library_identifier_restriction_regex(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        default_library: Library,
        common_args: list[tuple[str, str]],
    ):
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "testing auth name"),
                    ("protocol", SimpleAuthenticationProvider.__module__),
                    (
                        "libraries",
                        json.dumps(
                            [
                                {
                                    "short_name": default_library.short_name,
                                    "library_identifier_restriction_type": LibraryIdentifierRestriction.REGEX.value,
                                    "library_identifier_field": "barcode",
                                    "library_identifier_restriction_criteria": "(invalid re",
                                }
                            ]
                        ),
                    ),
                ]
                + common_args
            )
            response = controller.process_patron_auth_services()
        assert isinstance(response, ProblemDetail)
        assert response == INVALID_LIBRARY_IDENTIFIER_RESTRICTION_REGULAR_EXPRESSION

    def test_patron_auth_services_post_not_authorized(
        self,
        common_args: list[tuple[str, str]],
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
    ):
        with flask_app_fixture.test_request_context("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("protocol", SimpleAuthenticationProvider.__module__),
                ]
                + common_args
            )
            pytest.raises(AdminNotAuthorized, controller.process_patron_auth_services)

    def test_patron_auth_services_post_create(
        self,
        common_args: list[tuple[str, str]],
        default_library: Library,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "testing auth name"),
                    ("protocol", SimpleAuthenticationProvider.__module__),
                    (
                        "libraries",
                        json.dumps(
                            [
                                {
                                    "short_name": default_library.short_name,
                                    "library_identifier_restriction_type": LibraryIdentifierRestriction.REGEX.value,
                                    "library_identifier_field": "barcode",
                                    "library_identifier_restriction_criteria": "^1234",
                                }
                            ]
                        ),
                    ),
                ]
                + common_args
            )
            response = controller.process_patron_auth_services()
        assert isinstance(response, Response)
        assert response.status_code == 201

        auth_service = get_one(
            db.session,
            IntegrationConfiguration,
            goal=Goals.PATRON_AUTH_GOAL,
        )
        assert auth_service is not None
        assert auth_service.id == int(response.response[0])  # type: ignore[index]
        assert SimpleAuthenticationProvider.__module__ == auth_service.protocol
        settings = SimpleAuthenticationProvider.settings_load(auth_service)
        assert settings.test_identifier == "user"
        assert settings.test_password == "pass"
        [library_config] = auth_service.library_configurations
        assert library_config.library == default_library
        assert "short_name" not in library_config.settings_dict
        assert (
            library_config.settings_dict["library_identifier_restriction_criteria"]
            == "^1234"
        )

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "testing auth 2 name"),
                    ("protocol", MilleniumPatronAPI.__module__),
                    ("url", "https://url.com"),
                    ("verify_certificate", "false"),
                    ("authentication_mode", "pin"),
                ]
                + common_args
            )
            response = controller.process_patron_auth_services()
        assert isinstance(response, Response)
        assert response.status_code == 201

        auth_service2 = get_one(
            db.session,
            IntegrationConfiguration,
            goal=Goals.PATRON_AUTH_GOAL,
            protocol=MilleniumPatronAPI.__module__,
        )
        assert auth_service2 is not None
        assert auth_service2 != auth_service
        assert auth_service2.id == int(response.response[0])  # type: ignore[index]
        settings2 = MilleniumPatronAPI.settings_class()(**auth_service2.settings_dict)
        assert "https://url.com" == settings2.url
        assert "user" == settings2.test_identifier
        assert "pass" == settings2.test_password
        assert settings2.verify_certificate is False
        assert AuthenticationMode.PIN == settings2.authentication_mode
        assert settings2.block_types is None
        assert [] == auth_service2.library_configurations

    def test_patron_auth_services_post_edit(
        self,
        common_args: list[tuple[str, str]],
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
        db: DatabaseTransactionFixture,
        monkeypatch: MonkeyPatch,
    ):
        l1 = db.library("Library 1", "L1")
        l2 = db.library("Library 2", "L2")

        mock_site_configuration_has_changed = MagicMock()
        monkeypatch.setattr(
            "api.admin.controller.patron_auth_services.site_configuration_has_changed",
            mock_site_configuration_has_changed,
        )

        auth_service, _ = create_simple_auth_integration(
            l1,
            "old_user",
            "old_password",
        )

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(auth_service.id)),
                    ("protocol", SimpleAuthenticationProvider.__module__),
                    (
                        "libraries",
                        json.dumps(
                            [
                                {
                                    "short_name": l2.short_name,
                                    "library_identifier_restriction_type": LibraryIdentifierRestriction.NONE.value,
                                    "library_identifier_field": "barcode",
                                }
                            ]
                        ),
                    ),
                ]
                + common_args
            )
            response = controller.process_patron_auth_services()
        assert isinstance(response, Response)
        assert response.status_code == 200

        assert auth_service.id == int(response.get_data(as_text=True))
        assert SimpleAuthenticationProvider.__module__ == auth_service.protocol
        assert isinstance(auth_service.settings_dict, dict)
        settings = SimpleAuthenticationProvider.settings_load(auth_service)
        assert settings.test_identifier == "user"
        assert settings.test_password == "pass"
        [library_config] = auth_service.library_configurations
        assert l2 == library_config.library
        assert isinstance(library_config.settings_dict, dict)
        library_settings = SimpleAuthenticationProvider.library_settings_class()(
            **library_config.settings_dict
        )
        assert (
            library_settings.library_identifier_restriction_type
            == LibraryIdentifierRestriction.NONE
        )
        assert library_settings.library_identifier_field == "barcode"
        mock_site_configuration_has_changed.assert_called_once_with(db.session)

    def test_patron_auth_service_delete(
        self,
        common_args: list[tuple[str, str]],
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
        db: DatabaseTransactionFixture,
    ):
        l1 = db.library("Library 1", "L1")
        auth_service, _ = create_simple_auth_integration(
            l1,
            "old_user",
            "old_password",
        )

        with flask_app_fixture.test_request_context("/", method="DELETE"):
            pytest.raises(
                AdminNotAuthorized,
                controller.process_delete,
                auth_service.id,
            )

        with flask_app_fixture.test_request_context_system_admin("/", method="DELETE"):
            assert auth_service.id is not None
            response = controller.process_delete(auth_service.id)
            assert response.status_code == 200

        service = get_one(
            db.session,
            IntegrationConfiguration,
            id=auth_service.id,
        )
        assert service is None

    def test_patron_auth_self_tests_with_no_identifier(
        self, controller: PatronAuthServicesController
    ):
        response = controller.process_patron_auth_service_self_tests(None)
        assert isinstance(response, ProblemDetail)
        assert response.title == MISSING_IDENTIFIER.title
        assert response.detail == MISSING_IDENTIFIER.detail
        assert response.status_code == 400

    def test_patron_auth_self_tests_with_no_auth_service_found(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
    ):
        with flask_app_fixture.test_request_context("/"):
            response = controller.process_patron_auth_service_self_tests(-1)
        assert isinstance(response, ProblemDetail)
        assert response == MISSING_SERVICE
        assert response.status_code == 404

    def test_patron_auth_self_tests_get_with_no_libraries(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
    ):
        auth_service, _ = create_simple_auth_integration()
        with flask_app_fixture.test_request_context("/"):
            response_obj = controller.process_patron_auth_service_self_tests(
                auth_service.id
            )
        assert isinstance(response_obj, Response)
        response = response_obj.json
        assert isinstance(response, dict)
        results = response.get("self_test_results", {}).get("self_test_results")
        assert results.get("disabled") is True
        assert (
            results.get("exception")
            == "You must associate this service with at least one library before you can run self tests for it."
        )

    def test_patron_auth_self_tests_test_get_no_results(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
        default_library: Library,
    ):
        auth_service, _ = create_simple_auth_integration(library=default_library)

        # Make sure that we return the correct response when there are no results
        with flask_app_fixture.test_request_context("/"):
            response_obj = controller.process_patron_auth_service_self_tests(
                auth_service.id
            )
        assert isinstance(response_obj, Response)
        response = response_obj.json
        assert isinstance(response, dict)
        response_auth_service = response.get("self_test_results", {})

        assert response_auth_service.get("name") == auth_service.name
        assert response_auth_service.get("protocol") == auth_service.protocol
        assert response_auth_service.get("id") == auth_service.id
        assert auth_service.goal is not None
        assert response_auth_service.get("goal") == auth_service.goal.value
        assert response_auth_service.get("self_test_results") == "No results yet"

    def test_patron_auth_self_tests_test_get(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
        default_library: Library,
    ):
        expected_results = dict(
            duration=0.9,
            start="2018-08-08T16:04:05Z",
            end="2018-08-08T16:05:05Z",
            results=[],
        )
        auth_service, _ = create_simple_auth_integration(library=default_library)
        auth_service.self_test_results = expected_results

        # Make sure that HasSelfTest.prior_test_results() was called and that
        # it is in the response's self tests object.
        with flask_app_fixture.test_request_context("/"):
            response_obj = controller.process_patron_auth_service_self_tests(
                auth_service.id
            )
        assert isinstance(response_obj, Response)
        response = response_obj.json
        assert isinstance(response, dict)
        response_auth_service = response.get("self_test_results", {})

        assert response_auth_service.get("name") == auth_service.name
        assert response_auth_service.get("protocol") == auth_service.protocol
        assert response_auth_service.get("id") == auth_service.id
        assert auth_service.goal is not None
        assert response_auth_service.get("goal") == auth_service.goal.value
        assert response_auth_service.get("self_test_results") == expected_results

    def test_patron_auth_self_tests_post_with_no_libraries(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
    ):
        auth_service, _ = create_simple_auth_integration()
        with flask_app_fixture.test_request_context("/", method="POST"):
            response = controller.process_patron_auth_service_self_tests(
                auth_service.id,
            )
        assert isinstance(response, ProblemDetail)
        assert response.title == FAILED_TO_RUN_SELF_TESTS.title
        assert response.detail is not None
        assert "Failed to run self tests" in response.detail
        assert response.status_code == 400

    def test_patron_auth_self_tests_test_post(
        self,
        controller: PatronAuthServicesController,
        flask_app_fixture: FlaskAppFixture,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
        monkeypatch: MonkeyPatch,
        db: DatabaseTransactionFixture,
    ):
        expected_results = ("value", "results")
        mock = MagicMock(return_value=expected_results)
        monkeypatch.setattr(HasSelfTests, "run_self_tests", mock)
        library = db.default_library()
        auth_service, _ = create_simple_auth_integration(library=library)

        with flask_app_fixture.test_request_context("/", method="POST"):
            response = controller.process_patron_auth_service_self_tests(
                auth_service.id
            )
        assert isinstance(response, Response)
        assert response.status == "200 OK"
        assert "Successfully ran new self tests" == response.get_data(as_text=True)

        assert mock.call_count == 1
        assert mock.call_args.args[0] == db.session
        assert mock.call_args.args[1] is None
        assert mock.call_args.args[2] == library.id
        assert mock.call_args.args[3] == auth_service.id

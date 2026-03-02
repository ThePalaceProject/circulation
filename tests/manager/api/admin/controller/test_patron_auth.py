from __future__ import annotations

import json
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import flask
import pytest
from flask import Response
from pytest import MonkeyPatch
from werkzeug.datastructures import ImmutableMultiDict

from palace.manager.api.admin.controller.patron_auth_services import (
    PatronAuthServicesController,
)
from palace.manager.api.admin.exceptions import AdminNotAuthorized
from palace.manager.api.admin.problem_details import (
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
from palace.manager.api.authentication.basic import (
    BarcodeFormats,
    Keyboards,
    LibraryIdentifierRestriction,
)
from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.core.selftest import HasSelfTests
from palace.manager.integration.goals import Goals
from palace.manager.integration.patron_auth.millenium_patron import (
    AuthenticationMode,
    MilleniumPatronAPI,
    MilleniumPatronSettings,
)
from palace.manager.integration.patron_auth.saml.configuration.model import (
    SAMLWebSSOAuthSettings,
)
from palace.manager.integration.patron_auth.saml.provider import (
    SAMLWebSSOAuthenticationProvider,
)
from palace.manager.integration.patron_auth.simple_authentication import (
    SimpleAuthenticationProvider,
)
from palace.manager.integration.patron_auth.sip2.provider import (
    SIP2AuthenticationProvider,
    SIP2Settings,
)
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.problem_detail import ProblemDetail
from tests.fixtures.flask import FlaskAppFixture
from tests.fixtures.services import ServicesFixture
from tests.mocks.saml_strings import CORRECT_XML_WITH_ONE_SP

if TYPE_CHECKING:
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


class ControllerFixture:
    def __init__(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ):
        self.registry = services_fixture.services.integration_registry.patron_auth()
        self.controller = PatronAuthServicesController(db.session, self.registry)

    def get_protocol[AuthenticationProviderType](
        self, provider: type[AuthenticationProviderType]
    ) -> str:
        result = self.registry.get_protocol(provider)
        if result is None:
            raise ValueError(f"Protocol not found for {provider}")
        return result


@pytest.fixture
def controller_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
) -> ControllerFixture:
    return ControllerFixture(db, services_fixture)


class TestPatronAuth:
    def test_patron_auth_services_get_with_no_services(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        controller = controller_fixture.controller
        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller.process_patron_auth_services()
        assert isinstance(response, Response)

        response_data = response.json
        assert isinstance(response_data, dict)
        assert response_data.get("patron_auth_services") == []

        protocols = response_data.get("protocols")
        assert isinstance(protocols, list)
        assert len(protocols) == 8
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
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        controller = controller_fixture.controller
        auth_service = db.simple_auth_integration(
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
        assert SimpleAuthenticationProvider == controller_fixture.registry.get(
            service.get("protocol")
        )
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
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        controller = controller_fixture.controller
        auth_service = db.auth_integration(
            MilleniumPatronAPI,
            db.default_library(),
            settings=MilleniumPatronSettings(
                url="http://url.com/",
                test_identifier="user",
                test_password="pass",
                identifier_regular_expression="u*",
                password_regular_expression="p*",
            ),
        )

        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller.process_patron_auth_services()
        assert isinstance(response, Response)
        response_data = response.json
        assert isinstance(response_data, dict)
        [service] = response_data.get("patron_auth_services", [])

        assert auth_service.id == service.get("id")
        assert MilleniumPatronAPI == controller_fixture.registry.get(
            service.get("protocol")
        )
        assert "user" == service.get("settings").get("test_identifier")
        assert "pass" == service.get("settings").get("test_password")
        assert "u*" == service.get("settings").get("identifier_regular_expression")
        assert "p*" == service.get("settings").get("password_regular_expression")
        [library] = service.get("libraries")
        assert db.default_library().short_name == library.get("short_name")

    def test_patron_auth_services_get_with_sip2_auth_service(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        controller = controller_fixture.controller
        auth_service = db.auth_integration(
            SIP2AuthenticationProvider,
            db.default_library(),
            settings=SIP2Settings(
                url="url",
                port="1234",
                username="user",
                password="pass",
                location_code="5",
                field_separator=",",
            ),
        )

        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller.process_patron_auth_services()
        assert isinstance(response, Response)
        response_data = response.json
        assert isinstance(response_data, dict)
        [service] = response_data.get("patron_auth_services", [])

        assert auth_service.id == service.get("id")
        assert SIP2AuthenticationProvider == controller_fixture.registry.get(
            service.get("protocol")
        )
        assert "url" == service.get("settings").get("url")
        assert 1234 == service.get("settings").get("port")
        assert "user" == service.get("settings").get("username")
        assert "pass" == service.get("settings").get("password")
        assert "5" == service.get("settings").get("location_code")
        assert "," == service.get("settings").get("field_separator")
        [library] = service.get("libraries")
        assert db.default_library().short_name == library.get("short_name")

    def test_patron_auth_services_get_with_saml_auth_service(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        controller = controller_fixture.controller
        auth_service = db.auth_integration(
            SAMLWebSSOAuthenticationProvider,
            db.default_library(),
            settings=SAMLWebSSOAuthSettings(
                service_provider_xml_metadata=CORRECT_XML_WITH_ONE_SP
            ),
        )

        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller.process_patron_auth_services()
        assert isinstance(response, Response)
        response_data = response.json
        assert isinstance(response_data, dict)
        [service] = response_data.get("patron_auth_services", [])

        assert auth_service.id == service.get("id")
        assert SAMLWebSSOAuthenticationProvider == controller_fixture.registry.get(
            service.get("protocol")
        )
        [library] = service.get("libraries")
        assert db.default_library().short_name == library.get("short_name")

    def test_patron_auth_services_post_unknown_protocol(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        controller = controller_fixture.controller
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
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        controller = controller_fixture.controller
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict([])
            response = controller.process_patron_auth_services()
        assert response == NO_PROTOCOL_FOR_NEW_SERVICE

    def test_patron_auth_services_post_missing_service(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        controller = controller_fixture.controller
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    (
                        "protocol",
                        controller_fixture.get_protocol(SimpleAuthenticationProvider),
                    ),
                    ("id", "123"),
                ]
            )
            response = controller.process_patron_auth_services()
        assert response == MISSING_SERVICE

    def test_patron_auth_services_post_cannot_change_protocol(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        controller = controller_fixture.controller
        auth_service = db.simple_auth_integration()
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(auth_service.id)),
                    (
                        "protocol",
                        controller_fixture.get_protocol(SIP2AuthenticationProvider),
                    ),
                ]
            )
            response = controller.process_patron_auth_services()
        assert response == CANNOT_CHANGE_PROTOCOL

    def test_patron_auth_services_post_name_in_use(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        controller = controller_fixture.controller
        auth_service = db.simple_auth_integration()
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", str(auth_service.name)),
                    (
                        "protocol",
                        controller_fixture.get_protocol(SIP2AuthenticationProvider),
                    ),
                ]
            )
            response = controller.process_patron_auth_services()
        assert response == INTEGRATION_NAME_ALREADY_IN_USE

    def test_patron_auth_services_post_invalid_configuration(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        common_args: list[tuple[str, str]],
    ):
        controller = controller_fixture.controller
        auth_service = db.auth_integration(
            MilleniumPatronAPI,
            db.default_library(),
            settings=MilleniumPatronSettings(
                url="http://url.com/",
            ),
        )
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "some auth name"),
                    ("id", str(auth_service.id)),
                    ("protocol", controller_fixture.get_protocol(MilleniumPatronAPI)),
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
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        common_args: list[tuple[str, str]],
    ):
        controller = controller_fixture.controller
        auth_service = db.simple_auth_integration()
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(auth_service.id)),
                    (
                        "protocol",
                        controller_fixture.get_protocol(SimpleAuthenticationProvider),
                    ),
                ]
            )
            response = controller.process_patron_auth_services()
        assert isinstance(response, ProblemDetail)
        assert response.uri == INCOMPLETE_CONFIGURATION.uri

    def test_patron_auth_services_post_missing_patron_auth_name(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        common_args: list[tuple[str, str]],
    ):
        controller = controller_fixture.controller
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    (
                        "protocol",
                        controller_fixture.get_protocol(SimpleAuthenticationProvider),
                    ),
                ]
                + common_args
            )
            response = controller.process_patron_auth_services()
        assert isinstance(response, ProblemDetail)
        assert response == MISSING_SERVICE_NAME

    def test_patron_auth_services_post_no_such_library(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        common_args: list[tuple[str, str]],
    ):
        controller = controller_fixture.controller
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "testing auth name"),
                    (
                        "protocol",
                        controller_fixture.get_protocol(SimpleAuthenticationProvider),
                    ),
                    ("libraries", json.dumps([{"short_name": "not-a-library"}])),
                ]
                + common_args
            )
            response = controller.process_patron_auth_services()
        assert isinstance(response, ProblemDetail)
        assert response.uri == NO_SUCH_LIBRARY.uri

    def test_patron_auth_services_post_missing_short_name(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        common_args: list[tuple[str, str]],
    ):
        controller = controller_fixture.controller
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "testing auth name"),
                    (
                        "protocol",
                        controller_fixture.get_protocol(SimpleAuthenticationProvider),
                    ),
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
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        default_library: Library,
        common_args: list[tuple[str, str]],
    ):
        controller = controller_fixture.controller
        auth_service = db.simple_auth_integration(default_library)
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "testing auth name"),
                    (
                        "protocol",
                        controller_fixture.get_protocol(SimpleAuthenticationProvider),
                    ),
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
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        default_library: Library,
        common_args: list[tuple[str, str]],
    ):
        controller = controller_fixture.controller
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "testing auth name"),
                    (
                        "protocol",
                        controller_fixture.get_protocol(SimpleAuthenticationProvider),
                    ),
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
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        controller = controller_fixture.controller
        with flask_app_fixture.test_request_context("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    (
                        "protocol",
                        controller_fixture.get_protocol(SimpleAuthenticationProvider),
                    ),
                ]
                + common_args
            )
            pytest.raises(AdminNotAuthorized, controller.process_patron_auth_services)

    def test_patron_auth_services_post_create(
        self,
        common_args: list[tuple[str, str]],
        default_library: Library,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        controller = controller_fixture.controller
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "testing auth name"),
                    (
                        "protocol",
                        controller_fixture.get_protocol(SimpleAuthenticationProvider),
                    ),
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
        assert auth_service.id == int(response.get_data(as_text=True))
        assert (
            controller_fixture.get_protocol(SimpleAuthenticationProvider)
            == auth_service.protocol
        )
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
                    ("protocol", controller_fixture.get_protocol(MilleniumPatronAPI)),
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
            protocol=controller_fixture.get_protocol(MilleniumPatronAPI),
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
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        monkeypatch: MonkeyPatch,
    ):
        controller = controller_fixture.controller
        l1 = db.library("Library 1", "L1")
        l2 = db.library("Library 2", "L2")

        mock_site_configuration_has_changed = MagicMock()
        monkeypatch.setattr(
            "palace.manager.api.admin.controller.patron_auth_services.site_configuration_has_changed",
            mock_site_configuration_has_changed,
        )

        auth_service = db.simple_auth_integration(
            l1,
            "old_user",
            "old_password",
        )

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(auth_service.id)),
                    (
                        "protocol",
                        controller_fixture.get_protocol(SimpleAuthenticationProvider),
                    ),
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
        assert (
            controller_fixture.get_protocol(SimpleAuthenticationProvider)
            == auth_service.protocol
        )
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
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        controller = controller_fixture.controller
        l1 = db.library("Library 1", "L1")
        auth_service = db.simple_auth_integration(
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
        self, controller_fixture: ControllerFixture
    ):
        controller = controller_fixture.controller
        response = controller.process_patron_auth_service_self_tests(None)
        assert isinstance(response, ProblemDetail)
        assert response.title == MISSING_IDENTIFIER.title
        assert response.detail == MISSING_IDENTIFIER.detail
        assert response.status_code == 400

    def test_patron_auth_self_tests_with_no_auth_service_found(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        controller = controller_fixture.controller
        with flask_app_fixture.test_request_context("/"):
            response = controller.process_patron_auth_service_self_tests(-1)
        assert isinstance(response, ProblemDetail)
        assert response == MISSING_SERVICE
        assert response.status_code == 404

    def test_patron_auth_self_tests_get_with_no_libraries(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        controller = controller_fixture.controller
        auth_service = db.simple_auth_integration()
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
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        default_library: Library,
    ):
        controller = controller_fixture.controller
        auth_service = db.simple_auth_integration(library=default_library)

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
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        default_library: Library,
    ):
        controller = controller_fixture.controller
        expected_results = dict(
            duration=0.9,
            start="2018-08-08T16:04:05Z",
            end="2018-08-08T16:05:05Z",
            results=[],
        )
        auth_service = db.simple_auth_integration(library=default_library)
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
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        controller = controller_fixture.controller
        auth_service = db.simple_auth_integration()
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
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        monkeypatch: MonkeyPatch,
        db: DatabaseTransactionFixture,
    ):
        controller = controller_fixture.controller
        expected_results = ("value", "results")
        mock = MagicMock(return_value=expected_results)
        monkeypatch.setattr(HasSelfTests, "run_self_tests", mock)
        library = db.default_library()
        auth_service = db.simple_auth_integration(library=library)

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


# ---------------------------------------------------------------------------
# Live SIP2 rule validation on admin save
# ---------------------------------------------------------------------------


class TestLiveSIP2RuleValidation:
    """Tests for the live SIP2 patronBlocking rule validation in
    PatronAuthServicesController.library_integration_validation."""

    _FETCH_PATCH = (
        "palace.manager.api.admin.controller.patron_auth_services"
        ".SIP2AuthenticationProvider.fetch_live_rule_validation_values"
    )

    # A minimal valid SIP2 form field list (no library-level entries).
    _SIP2_BASE_ARGS = [
        ("url", "sip.example.com"),
        ("test_identifier", "patron1"),
        ("test_password", "pass"),
        ("identifier_keyboard", Keyboards.DEFAULT.value),
        ("password_keyboard", Keyboards.DEFAULT.value),
        ("identifier_barcode_format", BarcodeFormats.CODABAR.value),
    ]

    def _post_sip2(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        library: Library,
        rules: list[dict] | None = None,
        extra_args: list[tuple[str, str]] | None = None,
        service_name: str = "live-sip2-test",
    ) -> ProblemDetail | Response:
        """Submit a SIP2 patron-auth service POST and return the response."""
        library_data: dict = {
            "short_name": library.short_name,
            "library_identifier_restriction_type": LibraryIdentifierRestriction.NONE.value,
            "library_identifier_field": "barcode",
        }
        if rules is not None:
            library_data["patron_blocking_rules"] = rules

        form_args: list[tuple[str, str]] = (
            [
                ("name", service_name),
                (
                    "protocol",
                    controller_fixture.get_protocol(SIP2AuthenticationProvider),
                ),
                ("libraries", json.dumps([library_data])),
            ]
            + self._SIP2_BASE_ARGS
            + (extra_args or [])
        )

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(form_args)
            return controller_fixture.controller.process_patron_auth_services()

    def test_no_rules_skips_live_sip2_call(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """When no patron blocking rules are configured the live SIP2 call must
        not be made and the save must succeed."""
        mock_fetch = MagicMock()
        monkeypatch.setattr(self._FETCH_PATCH, mock_fetch)

        response = self._post_sip2(
            controller_fixture, flask_app_fixture, db.default_library(), rules=None
        )

        assert isinstance(response, Response)
        assert response.status_code in (200, 201)
        mock_fetch.assert_not_called()

    def test_rules_with_no_test_identifier_blocks_save(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """When patron blocking rules are configured but test_identifier is absent,
        the save must be blocked with INVALID_CONFIGURATION_OPTION."""
        from palace.manager.util.problem_detail import ProblemDetailException

        monkeypatch.setattr(
            self._FETCH_PATCH,
            MagicMock(
                side_effect=ProblemDetailException(
                    INVALID_CONFIGURATION_OPTION.detailed(
                        "A test identifier must be configured"
                    )
                )
            ),
        )

        response = self._post_sip2(
            controller_fixture,
            flask_app_fixture,
            db.default_library(),
            rules=[{"name": "fine-check", "rule": "{fines} > 10.0"}],
        )

        assert isinstance(response, ProblemDetail)
        assert response.uri == INVALID_CONFIGURATION_OPTION.uri

    def test_sip2_problem_detail_response_blocks_save(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """When the SIP2 server returns a ProblemDetail (auth error, server error,
        etc.) the save must be blocked."""
        from palace.manager.util.problem_detail import ProblemDetailException

        monkeypatch.setattr(
            self._FETCH_PATCH,
            MagicMock(
                side_effect=ProblemDetailException(
                    INVALID_CONFIGURATION_OPTION.detailed(
                        "SIP2 server returned an error for test patron"
                    )
                )
            ),
        )

        response = self._post_sip2(
            controller_fixture,
            flask_app_fixture,
            db.default_library(),
            rules=[{"name": "fine-check", "rule": "{fines} > 10.0"}],
        )

        assert isinstance(response, ProblemDetail)
        assert response.uri == INVALID_CONFIGURATION_OPTION.uri

    def test_sip2_oserror_blocks_save(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """When the SIP2 server cannot be reached (OSError) the save must be
        blocked (hard fail)."""
        from palace.manager.util.problem_detail import ProblemDetailException

        monkeypatch.setattr(
            self._FETCH_PATCH,
            MagicMock(
                side_effect=ProblemDetailException(
                    INVALID_CONFIGURATION_OPTION.detailed(
                        "Could not contact the SIP2 server: Connection refused"
                    )
                )
            ),
        )

        response = self._post_sip2(
            controller_fixture,
            flask_app_fixture,
            db.default_library(),
            rules=[{"name": "fine-check", "rule": "{fines} > 10.0"}],
        )

        assert isinstance(response, ProblemDetail)
        assert response.uri == INVALID_CONFIGURATION_OPTION.uri

    def test_rule_passes_against_live_values_allows_save(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """When the live SIP2 call succeeds and every rule validates against the
        real values, the save must succeed."""
        monkeypatch.setattr(
            self._FETCH_PATCH,
            MagicMock(
                return_value={
                    "fines": 2.50,
                    "patron_type": "adult",
                }
            ),
        )

        response = self._post_sip2(
            controller_fixture,
            flask_app_fixture,
            db.default_library(),
            rules=[{"name": "fine-check", "rule": "{fines} > 10.0"}],
        )

        assert isinstance(response, Response)
        assert response.status_code in (200, 201)

    def test_rule_fails_against_live_values_blocks_save(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """When a rule references a placeholder that is absent in the live
        values dict the save must be blocked and the error message must include
        the rule index and name."""
        # Live values do NOT include 'custom_field' — simulates an ILS that
        # never returns this field.
        monkeypatch.setattr(
            self._FETCH_PATCH,
            MagicMock(return_value={"fines": 0.0, "patron_type": "adult"}),
        )

        response = self._post_sip2(
            controller_fixture,
            flask_app_fixture,
            db.default_library(),
            # This rule references {custom_field} which is absent in the live values.
            rules=[{"name": "custom-check", "rule": "{custom_field} == 'expected'"}],
        )

        assert isinstance(response, ProblemDetail)
        assert response.uri == INVALID_CONFIGURATION_OPTION.uri
        assert response.detail is not None
        assert "custom-check" in response.detail

    def test_non_sip2_provider_skips_live_validation(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """For non-SIP2 providers the live SIP2 validation is never triggered
        even when patron_blocking_rules are present in the static settings
        (they pass static-only validation)."""
        mock_fetch = MagicMock()
        monkeypatch.setattr(self._FETCH_PATCH, mock_fetch)

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "simple-auth-with-rules"),
                    (
                        "protocol",
                        controller_fixture.get_protocol(SimpleAuthenticationProvider),
                    ),
                    ("test_identifier", "user"),
                    ("test_password", "pass"),
                    ("identifier_keyboard", Keyboards.DEFAULT.value),
                    ("password_keyboard", Keyboards.DEFAULT.value),
                    ("identifier_barcode_format", BarcodeFormats.CODABAR.value),
                    (
                        "libraries",
                        json.dumps(
                            [
                                {
                                    "short_name": db.default_library().short_name,
                                    "library_identifier_restriction_type": LibraryIdentifierRestriction.NONE.value,
                                    "library_identifier_field": "barcode",
                                    "patron_blocking_rules": [
                                        {"name": "fine-check", "rule": "{fines} > 10.0"}
                                    ],
                                }
                            ]
                        ),
                    ),
                ]
            )
            response = controller_fixture.controller.process_patron_auth_services()

        # Static validation passes; live SIP2 call is never made.
        assert isinstance(response, Response)
        assert response.status_code in (200, 201)
        mock_fetch.assert_not_called()

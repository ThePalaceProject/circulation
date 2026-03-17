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
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException
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

    # Shared for live rule validation and validate-rule endpoint tests.
    _FETCH_PATCH = (
        "palace.manager.integration.patron_auth.sip2.provider"
        ".SIP2AuthenticationProvider.fetch_live_rule_validation_values"
    )
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

    def test_rule_with_invalid_placeholder_allows_save(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ) -> None:
        """Rules are not validated on save; invalid rules are allowed and
        ignored at auth time."""
        response = self._post_sip2(
            controller_fixture,
            flask_app_fixture,
            db.default_library(),
            rules=[{"name": "custom-check", "rule": "{custom_field} == 'expected'"}],
        )

        assert isinstance(response, Response)
        assert response.status_code in (200, 201)

    def test_non_supporting_provider_rejects_rules_at_save(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ) -> None:
        """Providers that do not support patron blocking rules reject rules
        at settings validation time; the live validation path is never reached."""
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

        assert isinstance(response, ProblemDetail)
        assert response.uri == INVALID_CONFIGURATION_OPTION.uri
        assert response.detail is not None
        assert "not supported" in response.detail.lower()

    def _create_sip2_integration(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        service_name: str = "validate-test-sip2",
    ) -> int:
        """Create a SIP2 integration without patron blocking rules and return its ID.

        No rules means library_integration_validation skips the live SIP2 call,
        so no mock is needed during creation.
        """
        library_data = {
            "short_name": db.default_library().short_name,
            "library_identifier_restriction_type": LibraryIdentifierRestriction.NONE.value,
            "library_identifier_field": "barcode",
        }
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", service_name),
                    (
                        "protocol",
                        controller_fixture.get_protocol(SIP2AuthenticationProvider),
                    ),
                    ("libraries", json.dumps([library_data])),
                ]
                + self._SIP2_BASE_ARGS
            )
            response = controller_fixture.controller.process_patron_auth_services()
        assert isinstance(response, Response)
        return int(response.get_data(as_text=True))

    def _create_simple_integration(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        service_name: str = "validate-test-simple",
    ) -> int:
        """Create a SimpleAuthenticationProvider integration (no library) and return its ID."""
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", service_name),
                    (
                        "protocol",
                        controller_fixture.get_protocol(SimpleAuthenticationProvider),
                    ),
                    ("test_identifier", "user"),
                    ("test_password", "pass"),
                    ("identifier_keyboard", Keyboards.DEFAULT.value),
                    ("password_keyboard", Keyboards.DEFAULT.value),
                    ("identifier_barcode_format", BarcodeFormats.CODABAR.value),
                    ("libraries", json.dumps([])),
                ]
            )
            response = controller_fixture.controller.process_patron_auth_services()
        assert isinstance(response, Response)
        return int(response.get_data(as_text=True))

    def _post_validate(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        service_id: int | str,
        rule: str = "{fines} > 10.0",
    ) -> ProblemDetail | Response:
        """Call process_validate_patron_blocking_rule with the given service_id and rule."""
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("service_id", str(service_id)),
                    ("rule", rule),
                ]
            )
            return controller_fixture.controller.process_validate_patron_blocking_rule()

    def test_missing_service_id_returns_error(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
    ) -> None:
        """Omitting service_id returns INVALID_CONFIGURATION_OPTION."""
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict([("rule", "{fines} > 0")])
            response = (
                controller_fixture.controller.process_validate_patron_blocking_rule()
            )
        assert isinstance(response, ProblemDetail)
        assert response.uri == INVALID_CONFIGURATION_OPTION.uri

    def test_service_not_found_returns_error(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
    ) -> None:
        """A nonexistent service_id returns an error that tells the user to save first."""
        response = self._post_validate(
            controller_fixture, flask_app_fixture, 999999, "{fines} > 0"
        )
        assert isinstance(response, ProblemDetail)
        assert response.uri == INVALID_CONFIGURATION_OPTION.uri
        assert response.detail is not None
        assert "save" in response.detail.lower()

    def test_non_sip2_service_returns_error(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
    ) -> None:
        """A service that does not support patron blocking rules returns an error."""
        simple_id = self._create_simple_integration(
            controller_fixture, flask_app_fixture
        )
        response = self._post_validate(
            controller_fixture, flask_app_fixture, simple_id, "{fines} > 0"
        )
        assert isinstance(response, ProblemDetail)
        assert response.uri == INVALID_CONFIGURATION_OPTION.uri
        assert response.detail is not None
        assert "patron blocking rules" in response.detail

    def test_valid_rule_returns_200(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """A valid rule that evaluates to True against live values returns 200."""
        monkeypatch.setattr(
            self._FETCH_PATCH,
            MagicMock(return_value={"fines": 2.50, "patron_type": "adult"}),
        )
        service_id = self._create_sip2_integration(
            controller_fixture, flask_app_fixture, db
        )
        response = self._post_validate(
            controller_fixture, flask_app_fixture, service_id, "{fines} > 1.0"
        )
        assert isinstance(response, Response)
        assert response.status_code == 200

    def test_rule_with_false_result_still_returns_200(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """A valid rule that evaluates to False against live values still returns 200.

        The boolean result (blocked vs. not blocked) is intentionally ignored —
        only parse/eval success or failure is reported.
        """
        monkeypatch.setattr(
            self._FETCH_PATCH,
            MagicMock(return_value={"fines": 0.0, "patron_type": "adult"}),
        )
        service_id = self._create_sip2_integration(
            controller_fixture, flask_app_fixture, db
        )
        response = self._post_validate(
            controller_fixture, flask_app_fixture, service_id, "{fines} > 100.0"
        )
        assert isinstance(response, Response)
        assert response.status_code == 200

    def test_invalid_expression_returns_error(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """An unparseable rule expression returns INVALID_CONFIGURATION_OPTION."""
        monkeypatch.setattr(
            self._FETCH_PATCH,
            MagicMock(return_value={"fines": 0.0}),
        )
        service_id = self._create_sip2_integration(
            controller_fixture, flask_app_fixture, db
        )
        response = self._post_validate(
            controller_fixture,
            flask_app_fixture,
            service_id,
            "not a valid python expression !!!",
        )
        assert isinstance(response, ProblemDetail)
        assert response.uri == INVALID_CONFIGURATION_OPTION.uri

    def test_missing_placeholder_returns_error(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """A rule referencing a placeholder not in the live values returns an error
        whose detail mentions the missing field name."""
        monkeypatch.setattr(
            self._FETCH_PATCH,
            MagicMock(return_value={"fines": 0.0, "patron_type": "adult"}),
        )
        service_id = self._create_sip2_integration(
            controller_fixture, flask_app_fixture, db
        )
        response = self._post_validate(
            controller_fixture,
            flask_app_fixture,
            service_id,
            "{unknown_field} == 'expected'",
        )
        assert isinstance(response, ProblemDetail)
        assert response.uri == INVALID_CONFIGURATION_OPTION.uri
        assert response.detail is not None
        assert "unknown_field" in response.detail

    def test_sip2_connection_error_propagates(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """When fetch_live_rule_validation_values raises ProblemDetailException
        (e.g. network error), that ProblemDetail is returned to the caller."""
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
        service_id = self._create_sip2_integration(
            controller_fixture, flask_app_fixture, db
        )
        response = self._post_validate(
            controller_fixture, flask_app_fixture, service_id, "{fines} > 0"
        )
        assert isinstance(response, ProblemDetail)
        assert response.uri == INVALID_CONFIGURATION_OPTION.uri
        assert response.detail is not None
        assert "SIP2" in response.detail

    def test_missing_test_identifier_propagates(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
        monkeypatch: MonkeyPatch,
    ) -> None:
        """When fetch_live_rule_validation_values raises because no test_identifier
        is configured, the error is propagated as INVALID_CONFIGURATION_OPTION."""
        monkeypatch.setattr(
            self._FETCH_PATCH,
            MagicMock(
                side_effect=ProblemDetailException(
                    INVALID_CONFIGURATION_OPTION.detailed(
                        "A test identifier must be configured on this authentication "
                        "service before patron blocking rules can be validated."
                    )
                )
            ),
        )
        service_id = self._create_sip2_integration(
            controller_fixture, flask_app_fixture, db
        )
        response = self._post_validate(
            controller_fixture, flask_app_fixture, service_id, "{fines} > 0"
        )
        assert isinstance(response, ProblemDetail)
        assert response.uri == INVALID_CONFIGURATION_OPTION.uri

    def test_requires_system_admin(
        self,
        controller_fixture: ControllerFixture,
        flask_app_fixture: FlaskAppFixture,
    ) -> None:
        """A request without system-admin privileges raises AdminNotAuthorized."""
        with flask_app_fixture.test_request_context("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [("service_id", "1"), ("rule", "{fines} > 0")]
            )
            with pytest.raises(AdminNotAuthorized):
                controller_fixture.controller.process_validate_patron_blocking_rule()

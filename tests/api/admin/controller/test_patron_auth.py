from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Callable, List, Tuple
from unittest.mock import MagicMock

import flask
import pytest
from _pytest.monkeypatch import MonkeyPatch
from flask import Response
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.exceptions import *
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
from core.model import AdminRole, Library, get_one
from core.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from core.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from tests.fixtures.api_admin import SettingsControllerFixture
    from tests.fixtures.authenticator import AuthProviderFixture
    from tests.fixtures.database import DatabaseTransactionFixture


@pytest.fixture
def get_response(
    settings_ctrl_fixture: SettingsControllerFixture,
) -> Callable[[], dict[str, Any] | ProblemDetail]:
    def get() -> dict[str, Any] | ProblemDetail:
        with settings_ctrl_fixture.request_context_with_admin("/"):
            response_obj = (
                settings_ctrl_fixture.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            )
        if isinstance(response_obj, ProblemDetail):
            return response_obj
        return json.loads(response_obj.response[0])  # type: ignore[index]

    return get


@pytest.fixture
def post_response(
    settings_ctrl_fixture: SettingsControllerFixture,
) -> Callable[..., Response | ProblemDetail]:
    def post(form: ImmutableMultiDict[str, str]) -> Response | ProblemDetail:
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = form
            response = (
                settings_ctrl_fixture.manager.admin_patron_auth_services_controller.process_patron_auth_services()
            )
        return response

    return post


@pytest.fixture
def common_args() -> List[Tuple[str, str]]:
    return [
        ("test_identifier", "user"),
        ("test_password", "pass"),
        ("identifier_keyboard", Keyboards.DEFAULT.value),
        ("password_keyboard", Keyboards.DEFAULT.value),
        ("identifier_barcode_format", BarcodeFormats.CODABAR.value),
    ]


class TestPatronAuth:
    def test_patron_auth_services_get_with_no_services(
        self,
        settings_ctrl_fixture: SettingsControllerFixture,
        get_response: Callable[[], dict[str, Any] | ProblemDetail],
    ):
        response = get_response()
        assert isinstance(response, dict)
        assert response.get("patron_auth_services") == []
        protocols = response.get("protocols")
        assert isinstance(protocols, list)
        assert 7 == len(protocols)
        assert SimpleAuthenticationProvider.__module__ == protocols[0].get("name")
        assert "settings" in protocols[0]
        assert "library_settings" in protocols[0]

        settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        settings_ctrl_fixture.ctrl.db.session.flush()
        pytest.raises(
            AdminNotAuthorized,
            get_response,
        )

    def test_patron_auth_services_get_with_simple_auth_service(
        self,
        settings_ctrl_fixture: SettingsControllerFixture,
        db: DatabaseTransactionFixture,
        create_simple_auth_integration: Callable[..., AuthProviderFixture],
        create_integration_library_configuration: Callable[
            ..., IntegrationLibraryConfiguration
        ],
        get_response: Callable[[], dict[str, Any] | ProblemDetail],
    ):
        auth_service, _ = create_simple_auth_integration(
            test_identifier="user", test_password="pass"
        )

        response = get_response()
        assert isinstance(response, dict)
        [service] = response.get("patron_auth_services", [])

        assert auth_service.id == service.get("id")
        assert auth_service.name == service.get("name")
        assert SimpleAuthenticationProvider.__module__ == service.get("protocol")
        assert "user" == service.get("settings").get("test_identifier")
        assert "pass" == service.get("settings").get("test_password")
        assert [] == service.get("libraries")

        create_integration_library_configuration(db.default_library(), auth_service)
        response = get_response()
        assert isinstance(response, dict)
        [service] = response.get("patron_auth_services", [])

        assert "user" == service.get("settings").get("test_identifier")
        [library] = service.get("libraries")
        assert (
            settings_ctrl_fixture.ctrl.db.default_library().short_name
            == library.get("short_name")
        )

        response = get_response()
        assert isinstance(response, dict)
        [service] = response.get("patron_auth_services", [])

        [library] = service.get("libraries", [])
        assert (
            settings_ctrl_fixture.ctrl.db.default_library().short_name
            == library.get("short_name")
        )

    def test_patron_auth_services_get_with_millenium_auth_service(
        self,
        settings_ctrl_fixture: SettingsControllerFixture,
        db: DatabaseTransactionFixture,
        create_millenium_auth_integration: Callable[..., AuthProviderFixture],
        get_response: Callable[[], dict[str, Any] | ProblemDetail],
    ):
        auth_service, _ = create_millenium_auth_integration(
            db.default_library(),
            test_identifier="user",
            test_password="pass",
            identifier_regular_expression="u*",
            password_regular_expression="p*",
        )

        response = get_response()
        assert isinstance(response, dict)
        [service] = response.get("patron_auth_services", [])

        assert auth_service.id == service.get("id")
        assert MilleniumPatronAPI.__module__ == service.get("protocol")
        assert "user" == service.get("settings").get("test_identifier")
        assert "pass" == service.get("settings").get("test_password")
        assert "u*" == service.get("settings").get("identifier_regular_expression")
        assert "p*" == service.get("settings").get("password_regular_expression")
        [library] = service.get("libraries")
        assert (
            settings_ctrl_fixture.ctrl.db.default_library().short_name
            == library.get("short_name")
        )

    def test_patron_auth_services_get_with_sip2_auth_service(
        self,
        settings_ctrl_fixture: SettingsControllerFixture,
        db: DatabaseTransactionFixture,
        create_sip2_auth_integration: Callable[..., AuthProviderFixture],
        get_response: Callable[[], dict[str, Any] | ProblemDetail],
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

        response = get_response()
        assert isinstance(response, dict)
        [service] = response.get("patron_auth_services", [])

        assert auth_service.id == service.get("id")
        assert SIP2AuthenticationProvider.__module__ == service.get("protocol")
        assert "url" == service.get("settings").get("url")
        assert "1234" == service.get("settings").get("port")
        assert "user" == service.get("settings").get("username")
        assert "pass" == service.get("settings").get("password")
        assert "5" == service.get("settings").get("location_code")
        assert "," == service.get("settings").get("field_separator")
        [library] = service.get("libraries")
        assert (
            settings_ctrl_fixture.ctrl.db.default_library().short_name
            == library.get("short_name")
        )

    def test_patron_auth_services_get_with_saml_auth_service(
        self,
        settings_ctrl_fixture: SettingsControllerFixture,
        db: DatabaseTransactionFixture,
        create_saml_auth_integration: Callable[..., AuthProviderFixture],
        get_response: Callable[[], dict[str, Any] | ProblemDetail],
    ):
        auth_service, _ = create_saml_auth_integration(
            db.default_library(),
        )

        response = get_response()
        assert isinstance(response, dict)
        [service] = response.get("patron_auth_services", [])

        assert auth_service.id == service.get("id")
        assert SAMLWebSSOAuthenticationProvider.__module__ == service.get("protocol")
        [library] = service.get("libraries")
        assert (
            settings_ctrl_fixture.ctrl.db.default_library().short_name
            == library.get("short_name")
        )

    def test_patron_auth_services_post_unknown_protocol(
        self,
        post_response: Callable[..., Response | ProblemDetail],
    ):
        form = ImmutableMultiDict(
            [
                ("protocol", "Unknown"),
            ]
        )
        response = post_response(form)
        assert response == UNKNOWN_PROTOCOL

    def test_patron_auth_services_post_no_protocol(
        self,
        post_response: Callable[..., Response | ProblemDetail],
    ):
        form: ImmutableMultiDict[str, str] = ImmutableMultiDict([])
        response = post_response(form)
        assert response == NO_PROTOCOL_FOR_NEW_SERVICE

    def test_patron_auth_services_post_missing_service(
        self,
        post_response: Callable[..., Response | ProblemDetail],
    ):
        form = ImmutableMultiDict(
            [
                ("protocol", SimpleAuthenticationProvider.__module__),
                ("id", "123"),
            ]
        )
        response = post_response(form)
        assert response == MISSING_SERVICE

    def test_patron_auth_services_post_cannot_change_protocol(
        self,
        post_response: Callable[..., Response | ProblemDetail],
        create_simple_auth_integration: Callable[..., AuthProviderFixture],
    ):
        auth_service, _ = create_simple_auth_integration()
        form = ImmutableMultiDict(
            [
                ("id", str(auth_service.id)),
                ("protocol", SIP2AuthenticationProvider.__module__),
            ]
        )
        response = post_response(form)
        assert response == CANNOT_CHANGE_PROTOCOL

    def test_patron_auth_services_post_name_in_use(
        self,
        post_response: Callable[..., Response | ProblemDetail],
        create_simple_auth_integration: Callable[..., AuthProviderFixture],
    ):
        auth_service, _ = create_simple_auth_integration()
        form = ImmutableMultiDict(
            [
                ("name", auth_service.name),
                ("protocol", SIP2AuthenticationProvider.__module__),
            ]
        )
        response = post_response(form)
        assert response == INTEGRATION_NAME_ALREADY_IN_USE

    def test_patron_auth_services_post_invalid_configuration(
        self,
        post_response: Callable[..., Response | ProblemDetail],
        create_millenium_auth_integration: Callable[..., AuthProviderFixture],
        common_args: list[tuple[str, str]],
    ):
        auth_service, _ = create_millenium_auth_integration()
        form = ImmutableMultiDict(
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
        response = post_response(form)
        assert isinstance(response, ProblemDetail)
        assert response.uri == INVALID_CONFIGURATION_OPTION.uri

    def test_patron_auth_services_post_incomplete_configuration(
        self,
        post_response: Callable[..., Response | ProblemDetail],
        create_simple_auth_integration: Callable[..., AuthProviderFixture],
        common_args: list[tuple[str, str]],
    ):
        auth_service, _ = create_simple_auth_integration()
        form = ImmutableMultiDict(
            [
                ("id", str(auth_service.id)),
                ("protocol", SimpleAuthenticationProvider.__module__),
            ]
        )
        response = post_response(form)
        assert isinstance(response, ProblemDetail)
        assert response.uri == INCOMPLETE_CONFIGURATION.uri

    def test_patron_auth_services_post_missing_patron_auth_name(
        self,
        post_response: Callable[..., Response | ProblemDetail],
        common_args: list[tuple[str, str]],
    ):
        form = ImmutableMultiDict(
            [
                ("protocol", SimpleAuthenticationProvider.__module__),
            ]
            + common_args
        )
        response = post_response(form)
        assert isinstance(response, ProblemDetail)
        assert response.uri == MISSING_PATRON_AUTH_NAME.uri

    def test_patron_auth_services_post_missing_patron_auth_no_such_library(
        self,
        post_response: Callable[..., Response | ProblemDetail],
        common_args: list[tuple[str, str]],
    ):
        form = ImmutableMultiDict(
            [
                ("name", "testing auth name"),
                ("protocol", SimpleAuthenticationProvider.__module__),
                ("libraries", json.dumps([{"short_name": "not-a-library"}])),
            ]
            + common_args
        )
        response = post_response(form)
        assert isinstance(response, ProblemDetail)
        assert response.uri == NO_SUCH_LIBRARY.uri

    def test_patron_auth_services_post_missing_patron_auth_multiple_basic(
        self,
        post_response: Callable[..., Response | ProblemDetail],
        create_simple_auth_integration: Callable[..., AuthProviderFixture],
        default_library: Library,
        common_args: list[tuple[str, str]],
    ):
        auth_service, _ = create_simple_auth_integration(default_library)
        form = ImmutableMultiDict(
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
        response = post_response(form)
        assert isinstance(response, ProblemDetail)
        assert response.uri == MULTIPLE_BASIC_AUTH_SERVICES.uri

    def test_patron_auth_services_post_invalid_library_identifier_restriction_regex(
        self,
        post_response: Callable[..., Response | ProblemDetail],
        default_library: Library,
        common_args: list[tuple[str, str]],
    ):
        form = ImmutableMultiDict(
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
        response = post_response(form)
        assert isinstance(response, ProblemDetail)
        assert response == INVALID_LIBRARY_IDENTIFIER_RESTRICTION_REGULAR_EXPRESSION

    def test_patron_auth_services_post_not_authorized(
        self,
        common_args: List[Tuple[str, str]],
        settings_ctrl_fixture: SettingsControllerFixture,
        post_response: Callable[..., Response | ProblemDetail],
    ):
        settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        form = ImmutableMultiDict(
            [
                ("protocol", SimpleAuthenticationProvider.__module__),
            ]
            + common_args
        )
        pytest.raises(AdminNotAuthorized, post_response, form)

    def test_patron_auth_services_post_create(
        self,
        common_args: List[Tuple[str, str]],
        default_library: Library,
        post_response: Callable[..., Response | ProblemDetail],
        db: DatabaseTransactionFixture,
    ):
        form = ImmutableMultiDict(
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
        response = post_response(form)
        assert response.status_code == 201

        auth_service = get_one(
            db.session,
            IntegrationConfiguration,
            goal=Goals.PATRON_AUTH_GOAL,
        )
        assert auth_service.id == int(response.response[0])  # type: ignore[index]
        assert SimpleAuthenticationProvider.__module__ == auth_service.protocol
        settings = SimpleAuthenticationProvider.settings_class()(
            **auth_service.settings
        )
        assert settings.test_identifier == "user"
        assert settings.test_password == "pass"
        [library_config] = auth_service.library_configurations
        assert library_config.library == default_library
        assert (
            library_config.settings["library_identifier_restriction_criteria"]
            == "^1234"
        )

        form = ImmutableMultiDict(
            [
                ("name", "testing auth 2 name"),
                ("protocol", MilleniumPatronAPI.__module__),
                ("url", "https://url.com"),
                ("verify_certificate", "false"),
                ("authentication_mode", "pin"),
            ]
            + common_args
        )
        response = post_response(form)
        assert response.status_code == 201

        auth_service2 = get_one(
            db.session,
            IntegrationConfiguration,
            goal=Goals.PATRON_AUTH_GOAL,
            protocol=MilleniumPatronAPI.__module__,
        )
        assert auth_service2 != auth_service
        assert auth_service2.id == int(response.response[0])  # type: ignore[index]
        settings2 = MilleniumPatronAPI.settings_class()(**auth_service2.settings)
        assert "https://url.com" == settings2.url
        assert "user" == settings2.test_identifier
        assert "pass" == settings2.test_password
        assert settings2.verify_certificate is False
        assert AuthenticationMode.PIN == settings2.authentication_mode
        assert settings2.block_types is None
        assert [] == auth_service2.library_configurations

    def test_patron_auth_services_post_edit(
        self,
        post_response: Callable[..., Response | ProblemDetail],
        common_args: List[Tuple[str, str]],
        settings_ctrl_fixture: SettingsControllerFixture,
        create_simple_auth_integration: Callable[..., AuthProviderFixture],
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

        form = ImmutableMultiDict(
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
        response = post_response(form)
        assert response.status_code == 200

        assert auth_service.id == int(response.response[0])  # type: ignore[index]
        assert SimpleAuthenticationProvider.__module__ == auth_service.protocol
        assert isinstance(auth_service.settings, dict)
        settings = SimpleAuthenticationProvider.settings_class()(
            **auth_service.settings
        )
        assert settings.test_identifier == "user"
        assert settings.test_password == "pass"
        [library_config] = auth_service.library_configurations
        assert l2 == library_config.library
        assert isinstance(library_config.settings, dict)
        library_settings = SimpleAuthenticationProvider.library_settings_class()(
            **library_config.settings
        )
        assert (
            library_settings.library_identifier_restriction_type
            == LibraryIdentifierRestriction.NONE
        )
        assert library_settings.library_identifier_field == "barcode"
        mock_site_configuration_has_changed.assert_called_once_with(db.session)

    def test_patron_auth_service_delete(
        self,
        common_args: List[Tuple[str, str]],
        settings_ctrl_fixture: SettingsControllerFixture,
        create_simple_auth_integration: Callable[..., AuthProviderFixture],
    ):
        controller = settings_ctrl_fixture.manager.admin_patron_auth_services_controller
        db = settings_ctrl_fixture.ctrl.db

        l1 = db.library("Library 1", "L1")
        auth_service, _ = create_simple_auth_integration(
            l1,
            "old_user",
            "old_password",
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="DELETE"):
            settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            pytest.raises(
                AdminNotAuthorized,
                controller.process_delete,
                auth_service.id,
            )

            settings_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
            assert auth_service.id is not None
            response = controller.process_delete(auth_service.id)
            assert response.status_code == 200

        service = get_one(
            settings_ctrl_fixture.ctrl.db.session,
            IntegrationConfiguration,
            id=auth_service.id,
        )
        assert service is None

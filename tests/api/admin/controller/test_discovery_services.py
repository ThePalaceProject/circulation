from __future__ import annotations

from typing import TYPE_CHECKING

import flask
import pytest
from flask import Response
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import (
    INCOMPLETE_CONFIGURATION,
    INTEGRATION_NAME_ALREADY_IN_USE,
    INTEGRATION_URL_ALREADY_IN_USE,
    MISSING_SERVICE,
    NO_PROTOCOL_FOR_NEW_SERVICE,
    UNKNOWN_PROTOCOL,
)
from api.discovery.opds_registration import OpdsRegistrationService
from api.integration.registry.discovery import DiscoveryRegistry
from core.integration.goals import Goals
from core.model import AdminRole, ExternalIntegration, IntegrationConfiguration, get_one
from core.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from tests.fixtures.api_admin import SettingsControllerFixture
    from tests.fixtures.database import IntegrationConfigurationFixture


class TestDiscoveryServices:

    """Test the controller functions that list and create new discovery
    services.
    """

    @property
    def protocol(self):
        registry = DiscoveryRegistry()
        return registry.get_protocol(OpdsRegistrationService)

    def test_discovery_services_get_with_no_services_creates_default(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = (
                settings_ctrl_fixture.manager.admin_discovery_services_controller.process_discovery_services()
            )
            assert response.status_code == 200
            assert isinstance(response, Response)
            json = response.get_json()
            [service] = json.get("discovery_services")
            protocols = json.get("protocols")
            assert self.protocol in [p.get("name") for p in protocols]
            assert "settings" in protocols[0]
            assert self.protocol == service.get("protocol")
            assert OpdsRegistrationService.DEFAULT_LIBRARY_REGISTRY_URL == service.get(
                "settings"
            ).get(ExternalIntegration.URL)
            assert OpdsRegistrationService.DEFAULT_LIBRARY_REGISTRY_NAME == service.get(
                "name"
            )

            # Only system admins can see the discovery services.
            settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            settings_ctrl_fixture.ctrl.db.session.flush()
            pytest.raises(
                AdminNotAuthorized,
                settings_ctrl_fixture.manager.admin_discovery_services_controller.process_discovery_services,
            )

    def test_discovery_services_get_with_one_service(
        self,
        settings_ctrl_fixture: SettingsControllerFixture,
        create_integration_configuration: IntegrationConfigurationFixture,
    ):
        discovery_service = create_integration_configuration.discovery_service(
            url=settings_ctrl_fixture.ctrl.db.fresh_str()
        )
        controller = settings_ctrl_fixture.manager.admin_discovery_services_controller

        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = controller.process_discovery_services()
            assert isinstance(response, Response)
            [service] = response.get_json().get("discovery_services")

            assert discovery_service.id == service.get("id")
            assert discovery_service.protocol == service.get("protocol")
            assert discovery_service.settings_dict["url"] == service.get(
                "settings"
            ).get(ExternalIntegration.URL)

    def test_discovery_services_post_errors(
        self,
        settings_ctrl_fixture: SettingsControllerFixture,
        create_integration_configuration: IntegrationConfigurationFixture,
    ):
        controller = settings_ctrl_fixture.manager.admin_discovery_services_controller
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("protocol", "Unknown"),
                ]
            )
            response = controller.process_discovery_services()
            assert response == UNKNOWN_PROTOCOL

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                ]
            )
            response = controller.process_discovery_services()
            assert response == NO_PROTOCOL_FOR_NEW_SERVICE

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("id", "123"),
                    ("protocol", self.protocol),
                ]
            )
            response = controller.process_discovery_services()
            assert response == MISSING_SERVICE

        integration_url = settings_ctrl_fixture.ctrl.db.fresh_url()
        existing_integration = create_integration_configuration.discovery_service(
            url=integration_url
        )
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            assert isinstance(existing_integration.name, str)
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", existing_integration.name),
                    ("protocol", self.protocol),
                    ("url", "http://test.com"),
                ]
            )
            response = controller.process_discovery_services()
            assert response == INTEGRATION_NAME_ALREADY_IN_USE

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            assert isinstance(existing_integration.protocol, str)
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "new name"),
                    ("protocol", existing_integration.protocol),
                    ("url", integration_url),
                ]
            )
            response = controller.process_discovery_services()
            assert response == INTEGRATION_URL_ALREADY_IN_USE

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(existing_integration.id)),
                    ("protocol", self.protocol),
                ]
            )
            response = controller.process_discovery_services()
            assert isinstance(response, ProblemDetail)
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("protocol", self.protocol),
                    (ExternalIntegration.URL, "registry url"),
                ]
            )
            pytest.raises(AdminNotAuthorized, controller.process_discovery_services)

    def test_discovery_services_post_create(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("protocol", self.protocol),
                    (ExternalIntegration.URL, "http://registry.url"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_discovery_services_controller.process_discovery_services()
            )
            assert response.status_code == 201

        service = get_one(
            settings_ctrl_fixture.ctrl.db.session,
            IntegrationConfiguration,
            goal=Goals.DISCOVERY_GOAL,
        )
        assert isinstance(service, IntegrationConfiguration)
        assert isinstance(response, Response)
        assert service.id == int(response.get_data(as_text=True))
        assert self.protocol == service.protocol
        assert (
            OpdsRegistrationService.settings_load(service).url == "http://registry.url"
        )

    def test_discovery_services_post_edit(
        self,
        settings_ctrl_fixture: SettingsControllerFixture,
        create_integration_configuration: IntegrationConfigurationFixture,
    ):
        discovery_service = create_integration_configuration.discovery_service(
            url="registry url"
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("id", str(discovery_service.id)),
                    ("protocol", self.protocol),
                    (ExternalIntegration.URL, "http://new_registry_url.com"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_discovery_services_controller.process_discovery_services()
            )
            assert response.status_code == 200

        assert isinstance(response, Response)
        assert discovery_service.id == int(response.get_data(as_text=True))
        assert self.protocol == discovery_service.protocol
        assert (
            "http://new_registry_url.com"
            == OpdsRegistrationService.settings_load(discovery_service).url
        )

    def test_check_name_unique(
        self,
        settings_ctrl_fixture: SettingsControllerFixture,
        create_integration_configuration: IntegrationConfigurationFixture,
    ):
        existing_service = create_integration_configuration.discovery_service()
        new_service = create_integration_configuration.discovery_service()

        # Try to change new service so that it has the same name as existing service
        # -- this is not allowed.
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", str(existing_service.name)),
                    ("id", str(new_service.id)),
                    ("protocol", self.protocol),
                    ("url", "http://test.com"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_discovery_services_controller.process_discovery_services()
            )
            assert response == INTEGRATION_NAME_ALREADY_IN_USE

        # Try to edit existing service without changing its name -- this is fine.
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", str(existing_service.name)),
                    ("id", str(existing_service.id)),
                    ("protocol", self.protocol),
                    ("url", "http://test.com"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_discovery_services_controller.process_discovery_services()
            )
            assert isinstance(response, Response)
            assert response.status_code == 200

        # Changing the existing service's name is also fine.
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "New name"),
                    ("id", str(existing_service.id)),
                    ("protocol", self.protocol),
                    ("url", "http://test.com"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_discovery_services_controller.process_discovery_services()
            )
            assert isinstance(response, Response)
            assert response.status_code == 200

    def test_discovery_service_delete(
        self,
        settings_ctrl_fixture: SettingsControllerFixture,
        create_integration_configuration: IntegrationConfigurationFixture,
    ):
        discovery_service = create_integration_configuration.discovery_service(
            url="registry url"
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="DELETE"):
            settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            pytest.raises(
                AdminNotAuthorized,
                settings_ctrl_fixture.manager.admin_discovery_services_controller.process_delete,
                discovery_service.id,
            )

            settings_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = settings_ctrl_fixture.manager.admin_discovery_services_controller.process_delete(
                discovery_service.id  # type: ignore[arg-type]
            )
            assert response.status_code == 200

        service = get_one(
            settings_ctrl_fixture.ctrl.db.session,
            IntegrationConfiguration,
            id=discovery_service.id,
        )
        assert None == service

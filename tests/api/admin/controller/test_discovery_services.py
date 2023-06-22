import flask
import pytest
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.exceptions import (
    INCOMPLETE_CONFIGURATION,
    INTEGRATION_NAME_ALREADY_IN_USE,
    INTEGRATION_URL_ALREADY_IN_USE,
    MISSING_SERVICE,
    NO_PROTOCOL_FOR_NEW_SERVICE,
    UNKNOWN_PROTOCOL,
    AdminNotAuthorized,
)
from api.registration.registry import RemoteRegistry
from core.model import AdminRole, ExternalIntegration, create, get_one
from tests.fixtures.api_admin import SettingsControllerFixture


class TestDiscoveryServices:

    """Test the controller functions that list and create new discovery
    services.
    """

    def test_discovery_services_get_with_no_services_creates_default(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = (
                settings_ctrl_fixture.manager.admin_discovery_services_controller.process_discovery_services()
            )
            [service] = response.get("discovery_services")
            protocols = response.get("protocols")
            assert ExternalIntegration.OPDS_REGISTRATION in [
                p.get("name") for p in protocols
            ]
            assert "settings" in protocols[0]
            assert ExternalIntegration.OPDS_REGISTRATION == service.get("protocol")
            assert RemoteRegistry.DEFAULT_LIBRARY_REGISTRY_URL == service.get(
                "settings"
            ).get(ExternalIntegration.URL)
            assert RemoteRegistry.DEFAULT_LIBRARY_REGISTRY_NAME == service.get("name")

            # Only system admins can see the discovery services.
            settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            settings_ctrl_fixture.ctrl.db.session.flush()
            pytest.raises(
                AdminNotAuthorized,
                settings_ctrl_fixture.manager.admin_discovery_services_controller.process_discovery_services,
            )

    def test_discovery_services_get_with_one_service(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        discovery_service, ignore = create(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
        )
        discovery_service.url = settings_ctrl_fixture.ctrl.db.fresh_str()

        controller = settings_ctrl_fixture.manager.admin_discovery_services_controller

        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = controller.process_discovery_services()
            [service] = response.get("discovery_services")

            assert discovery_service.id == service.get("id")
            assert discovery_service.protocol == service.get("protocol")
            assert discovery_service.url == service.get("settings").get(
                ExternalIntegration.URL
            )

    def test_discovery_services_post_errors(
        self, settings_ctrl_fixture: SettingsControllerFixture
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
                    ("protocol", ExternalIntegration.OPDS_REGISTRATION),
                ]
            )
            response = controller.process_discovery_services()
            assert response == MISSING_SERVICE

        service, ignore = create(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
            name="name",
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", service.name),
                    ("protocol", ExternalIntegration.OPDS_REGISTRATION),
                ]
            )
            response = controller.process_discovery_services()
            assert response == INTEGRATION_NAME_ALREADY_IN_USE

        existing_integration = settings_ctrl_fixture.ctrl.db.external_integration(
            ExternalIntegration.OPDS_REGISTRATION,
            ExternalIntegration.DISCOVERY_GOAL,
            url=settings_ctrl_fixture.ctrl.db.fresh_url(),
        )
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            assert isinstance(existing_integration.protocol, str)
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "new name"),
                    ("protocol", existing_integration.protocol),
                    ("url", existing_integration.url),
                ]
            )
            response = controller.process_discovery_services()
            assert response == INTEGRATION_URL_ALREADY_IN_USE

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", service.id),
                    ("protocol", ExternalIntegration.OPDS_REGISTRATION),
                ]
            )
            response = controller.process_discovery_services()
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("protocol", ExternalIntegration.OPDS_REGISTRATION),
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
                    ("protocol", ExternalIntegration.OPDS_REGISTRATION),
                    (ExternalIntegration.URL, "http://registry_url"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_discovery_services_controller.process_discovery_services()
            )
            assert response.status_code == 201

        service = get_one(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            goal=ExternalIntegration.DISCOVERY_GOAL,
        )
        assert service.id == int(response.response[0])
        assert ExternalIntegration.OPDS_REGISTRATION == service.protocol
        assert "http://registry_url" == service.url

    def test_discovery_services_post_edit(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        discovery_service, ignore = create(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
        )
        discovery_service.url = "registry url"

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("id", discovery_service.id),
                    ("protocol", ExternalIntegration.OPDS_REGISTRATION),
                    (ExternalIntegration.URL, "http://new_registry_url"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_discovery_services_controller.process_discovery_services()
            )
            assert response.status_code == 200

        assert discovery_service.id == int(response.response[0])
        assert ExternalIntegration.OPDS_REGISTRATION == discovery_service.protocol
        assert "http://new_registry_url" == discovery_service.url

    def test_check_name_unique(self, settings_ctrl_fixture: SettingsControllerFixture):
        kwargs = dict(
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
        )

        existing_service, ignore = create(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            name="existing service",
            **kwargs
        )
        new_service, ignore = create(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            name="new service",
            **kwargs
        )

        m = (
            settings_ctrl_fixture.manager.admin_discovery_services_controller.check_name_unique
        )

        # Try to change new service so that it has the same name as existing service
        # -- this is not allowed.
        result = m(new_service, existing_service.name)
        assert result == INTEGRATION_NAME_ALREADY_IN_USE

        # Try to edit existing service without changing its name -- this is fine.
        assert None == m(existing_service, existing_service.name)

        # Changing the existing service's name is also fine.
        assert None == m(existing_service, "new name")

    def test_discovery_service_delete(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        discovery_service, ignore = create(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            protocol=ExternalIntegration.OPDS_REGISTRATION,
            goal=ExternalIntegration.DISCOVERY_GOAL,
        )
        discovery_service.url = "registry url"

        with settings_ctrl_fixture.request_context_with_admin("/", method="DELETE"):
            settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            pytest.raises(
                AdminNotAuthorized,
                settings_ctrl_fixture.manager.admin_discovery_services_controller.process_delete,
                discovery_service.id,
            )

            settings_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = settings_ctrl_fixture.manager.admin_discovery_services_controller.process_delete(
                discovery_service.id
            )
            assert response.status_code == 200

        service = get_one(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            id=discovery_service.id,
        )
        assert None == service

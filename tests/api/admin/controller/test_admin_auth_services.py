import flask
import pytest
from werkzeug.datastructures import MultiDict

from api.admin.exceptions import *
from api.app import initialize_database
from core.model import AdminRole, ExternalIntegration

from .test_controller import SettingsControllerTest


class TestAdminAuthServices(SettingsControllerTest):
    @classmethod
    def setup_class(cls):
        super().setup_class()

        initialize_database(autoinitialize=False)

    def test_admin_auth_services_get_with_no_services(self):
        with self.request_context_with_admin("/"):
            response = (
                self.manager.admin_auth_services_controller.process_admin_auth_services()
            )
            assert response.get("admin_auth_services") == []

            # All the protocols in ExternalIntegration.ADMIN_AUTH_PROTOCOLS
            # are supported by the admin interface.
            assert sorted(p.get("name") for p in response.get("protocols")) == sorted(
                ExternalIntegration.ADMIN_AUTH_PROTOCOLS
            )

            self.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            self._db.flush()
            pytest.raises(
                AdminNotAuthorized,
                self.manager.admin_auth_services_controller.process_admin_auth_services,
            )

    def test_admin_auth_services_post_errors(self):
        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("protocol", "Unknown"),
                ]
            )
            response = (
                self.manager.admin_auth_services_controller.process_admin_auth_services()
            )
            assert response == UNKNOWN_PROTOCOL

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = (
                self.manager.admin_auth_services_controller.process_admin_auth_services()
            )
            assert response == NO_PROTOCOL_FOR_NEW_SERVICE

        with self.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("id", "1234"),
                ]
            )
            response = (
                self.manager.admin_auth_services_controller.process_admin_auth_services()
            )
            assert response == MISSING_SERVICE

    def test_admin_auth_services_post_create(self):
        # TODO: Should be implemented if new external admin auth service is implemented
        return

    def test_admin_auth_service_delete(self):
        # TODO: Should be implemented if new external admin auth service is implemented
        return

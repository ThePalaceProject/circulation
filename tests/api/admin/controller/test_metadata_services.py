import json

import flask
import pytest
from werkzeug.datastructures import MultiDict

from api.admin.controller.metadata_services import MetadataServicesController
from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import (
    CANNOT_CHANGE_PROTOCOL,
    INCOMPLETE_CONFIGURATION,
    INTEGRATION_NAME_ALREADY_IN_USE,
    MISSING_SERVICE,
    NO_PROTOCOL_FOR_NEW_SERVICE,
    NO_SUCH_LIBRARY,
    UNKNOWN_PROTOCOL,
)
from api.novelist import NoveListAPI
from api.nyt import NYTBestSellerAPI
from core.model import AdminRole, ExternalIntegration, create, get_one


class TestMetadataServices:
    def create_service(self, name, db_session):
        return create(
            db_session,
            ExternalIntegration,
            protocol=ExternalIntegration.__dict__.get(name) or "fake",
            goal=ExternalIntegration.METADATA_GOAL,
        )[0]

    def test_process_metadata_services_dispatches_by_request_method(
        self, settings_ctrl_fixture
    ):
        class Mock(MetadataServicesController):
            def process_get(self):
                return "GET"

            def process_post(self):
                return "POST"

        controller = Mock(settings_ctrl_fixture.manager)
        with settings_ctrl_fixture.request_context_with_admin("/"):
            assert "GET" == controller.process_metadata_services()

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            assert "POST" == controller.process_metadata_services()

        # This is also where permissions are checked.
        settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        settings_ctrl_fixture.ctrl.db.session.flush()

        with settings_ctrl_fixture.request_context_with_admin("/"):
            pytest.raises(AdminNotAuthorized, controller.process_metadata_services)

    def test_process_get_with_no_services(self, settings_ctrl_fixture):
        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = (
                settings_ctrl_fixture.manager.admin_metadata_services_controller.process_get()
            )
            assert response.get("metadata_services") == []
            protocols = response.get("protocols")
            assert NoveListAPI.NAME in [p.get("label") for p in protocols]
            assert "settings" in protocols[0]

    def test_process_get_with_one_service(self, settings_ctrl_fixture):
        novelist_service = self.create_service(
            "NOVELIST", settings_ctrl_fixture.ctrl.db.session
        )
        novelist_service.username = "user"
        novelist_service.password = "pass"

        controller = settings_ctrl_fixture.manager.admin_metadata_services_controller

        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = controller.process_get()
            [service] = response.get("metadata_services")

            assert novelist_service.id == service.get("id")
            assert ExternalIntegration.NOVELIST == service.get("protocol")
            assert "user" == service.get("settings").get(ExternalIntegration.USERNAME)
            assert "pass" == service.get("settings").get(ExternalIntegration.PASSWORD)

        novelist_service.libraries += [settings_ctrl_fixture.ctrl.db.default_library()]
        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = controller.process_get()
            [service] = response.get("metadata_services")

            assert "user" == service.get("settings").get(ExternalIntegration.USERNAME)
            [library] = service.get("libraries")
            assert (
                settings_ctrl_fixture.ctrl.db.default_library().short_name
                == library.get("short_name")
            )

    def test_find_protocol_class(self, settings_ctrl_fixture):
        [nyt, novelist, fake] = [
            self.create_service(x, settings_ctrl_fixture.ctrl.db.session)
            for x in ["NYT", "NOVELIST", "FAKE"]
        ]
        m = (
            settings_ctrl_fixture.manager.admin_metadata_services_controller.find_protocol_class
        )

        assert m(nyt)[0] == NYTBestSellerAPI
        assert m(novelist)[0] == NoveListAPI
        pytest.raises(NotImplementedError, m, fake)

    def test_metadata_services_post_errors(self, settings_ctrl_fixture):
        controller = settings_ctrl_fixture.manager.admin_metadata_services_controller
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("name", "Name"),
                    ("protocol", "Unknown"),
                ]
            )
            response = controller.process_post()
            assert response == UNKNOWN_PROTOCOL

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = controller.process_post()
            assert response == INCOMPLETE_CONFIGURATION

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("name", "Name"),
                ]
            )
            response = controller.process_post()
            assert response == NO_PROTOCOL_FOR_NEW_SERVICE

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("name", "Name"),
                    ("id", "123"),
                    ("protocol", ExternalIntegration.NYT),
                ]
            )
            response = controller.process_post()
            assert response == MISSING_SERVICE

        service = self.create_service("NOVELIST", settings_ctrl_fixture.ctrl.db.session)
        service.name = "name"

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("name", service.name),
                    ("protocol", ExternalIntegration.NYT),
                ]
            )
            response = controller.process_post()
            assert response == INTEGRATION_NAME_ALREADY_IN_USE

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("name", "Name"),
                    ("id", service.id),
                    ("protocol", ExternalIntegration.NYT),
                ]
            )
            response = controller.process_post()
            assert response == CANNOT_CHANGE_PROTOCOL

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("id", service.id),
                    ("protocol", ExternalIntegration.NOVELIST),
                ]
            )
            response = controller.process_post()
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("name", "Name"),
                    ("id", service.id),
                    ("protocol", ExternalIntegration.NOVELIST),
                    (ExternalIntegration.USERNAME, "user"),
                    (ExternalIntegration.PASSWORD, "pass"),
                    ("libraries", json.dumps([{"short_name": "not-a-library"}])),
                ]
            )
            response = controller.process_post()
            assert response.uri == NO_SUCH_LIBRARY.uri

    def test_metadata_services_post_create(self, settings_ctrl_fixture):
        controller = settings_ctrl_fixture.manager.admin_metadata_services_controller
        library = settings_ctrl_fixture.ctrl.db.library(
            name="Library",
            short_name="L",
        )
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("name", "Name"),
                    ("protocol", ExternalIntegration.NOVELIST),
                    (ExternalIntegration.USERNAME, "user"),
                    (ExternalIntegration.PASSWORD, "pass"),
                    ("libraries", json.dumps([{"short_name": "L"}])),
                ]
            )
            response = controller.process_post()
            assert response.status_code == 201

        # A new ExternalIntegration has been created based on the submitted
        # information.
        service = get_one(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            goal=ExternalIntegration.METADATA_GOAL,
        )
        assert service.id == int(response.response[0])
        assert ExternalIntegration.NOVELIST == service.protocol
        assert "user" == service.username
        assert "pass" == service.password
        assert [library] == service.libraries

    def test_metadata_services_post_edit(self, settings_ctrl_fixture):
        l1 = settings_ctrl_fixture.ctrl.db.library(
            name="Library 1",
            short_name="L1",
        )
        l2 = settings_ctrl_fixture.ctrl.db.library(
            name="Library 2",
            short_name="L2",
        )
        novelist_service = self.create_service(
            "NOVELIST", settings_ctrl_fixture.ctrl.db.session
        )
        novelist_service.username = "olduser"
        novelist_service.password = "oldpass"
        novelist_service.libraries = [l1]

        controller = settings_ctrl_fixture.manager.admin_metadata_services_controller
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("name", "Name"),
                    ("id", novelist_service.id),
                    ("protocol", ExternalIntegration.NOVELIST),
                    (ExternalIntegration.USERNAME, "user"),
                    (ExternalIntegration.PASSWORD, "pass"),
                    ("libraries", json.dumps([{"short_name": "L2"}])),
                ]
            )
            response = controller.process_post()
            assert response.status_code == 200

    def test_check_name_unique(self, settings_ctrl_fixture):
        kwargs = dict(
            protocol=ExternalIntegration.NYT, goal=ExternalIntegration.METADATA_GOAL
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
            settings_ctrl_fixture.manager.admin_metadata_services_controller.check_name_unique
        )

        # Try to change new service so that it has the same name as existing service
        # -- this is not allowed.
        result = m(new_service, existing_service.name)
        assert result == INTEGRATION_NAME_ALREADY_IN_USE

        # Try to edit existing service without changing its name -- this is fine.
        assert None == m(existing_service, existing_service.name)

        # Changing the existing service's name is also fine.
        assert None == m(existing_service, "new name")

    def test_metadata_service_delete(self, settings_ctrl_fixture):
        l1 = settings_ctrl_fixture.ctrl.db.library(
            name="Library 1",
            short_name="L1",
        )
        novelist_service = self.create_service(
            "NOVELIST", settings_ctrl_fixture.ctrl.db.session
        )
        novelist_service.username = "olduser"
        novelist_service.password = "oldpass"
        novelist_service.libraries = [l1]

        with settings_ctrl_fixture.request_context_with_admin("/", method="DELETE"):
            settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            pytest.raises(
                AdminNotAuthorized,
                settings_ctrl_fixture.manager.admin_metadata_services_controller.process_delete,
                novelist_service.id,
            )

            settings_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = settings_ctrl_fixture.manager.admin_metadata_services_controller.process_delete(
                novelist_service.id
            )
            assert response.status_code == 200

        service = get_one(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            id=novelist_service.id,
        )
        assert None == service

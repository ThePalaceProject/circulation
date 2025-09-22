from __future__ import annotations

from typing import TYPE_CHECKING

import flask
import pytest
from flask import Response
from werkzeug.datastructures import ImmutableMultiDict

from palace.manager.api.admin.controller.discovery_services import (
    DiscoveryServicesController,
)
from palace.manager.api.admin.exceptions import AdminNotAuthorized
from palace.manager.api.admin.problem_details import (
    INCOMPLETE_CONFIGURATION,
    INTEGRATION_NAME_ALREADY_IN_USE,
    INTEGRATION_URL_ALREADY_IN_USE,
    MISSING_SERVICE,
    NO_PROTOCOL_FOR_NEW_SERVICE,
    UNKNOWN_PROTOCOL,
)
from palace.manager.integration.discovery.opds_registration import (
    OpdsRegistrationService,
)
from palace.manager.integration.goals import Goals
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.problem_detail import ProblemDetail
from tests.fixtures.flask import FlaskAppFixture
from tests.fixtures.services import ServicesFixture

if TYPE_CHECKING:
    from tests.fixtures.database import DatabaseTransactionFixture


class DiscoveryServicesControllerFixture:
    def __init__(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ):
        self.registry = services_fixture.services.integration_registry.discovery()
        self.protocol = self.registry.get_protocol(OpdsRegistrationService)
        self.controller = DiscoveryServicesController(db.session, self.registry)
        self.db = db

    def process_discovery_services(self) -> Response | ProblemDetail:
        return self.controller.process_discovery_services()


@pytest.fixture
def controller_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
) -> DiscoveryServicesControllerFixture:
    return DiscoveryServicesControllerFixture(db, services_fixture)


class TestDiscoveryServices:
    """Test the controller functions that list and create new discovery
    services.
    """

    def test_discovery_services_get_with_no_services_creates_default(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller_fixture: DiscoveryServicesControllerFixture,
    ):
        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller_fixture.process_discovery_services()
            assert response.status_code == 200
            assert isinstance(response, Response)
            json = response.get_json()
            [service] = json.get("discovery_services")
            protocols = json.get("protocols")
            assert controller_fixture.protocol in [p.get("name") for p in protocols]
            assert "settings" in protocols[0]
            assert controller_fixture.protocol == service.get("protocol")
            assert OpdsRegistrationService.DEFAULT_LIBRARY_REGISTRY_URL == service.get(
                "settings"
            ).get("url")
            assert OpdsRegistrationService.DEFAULT_LIBRARY_REGISTRY_NAME == service.get(
                "name"
            )

        with flask_app_fixture.test_request_context("/"):
            # Only system admins can see the discovery services.
            pytest.raises(
                AdminNotAuthorized,
                controller_fixture.process_discovery_services,
            )

    def test_discovery_services_get_with_one_service(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller_fixture: DiscoveryServicesControllerFixture,
        db: DatabaseTransactionFixture,
    ):
        discovery_service = db.discovery_service_integration()

        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller_fixture.process_discovery_services()
        db.session.expire(discovery_service)
        assert isinstance(response, Response)
        [service] = response.get_json().get("discovery_services")

        assert discovery_service.id == service.get("id")
        assert discovery_service.protocol == service.get("protocol")
        assert discovery_service.settings_dict["url"] == service.get("settings").get(
            "url"
        )

    def test_discovery_services_post_error_unknown_protocol(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller_fixture: DiscoveryServicesControllerFixture,
        db: DatabaseTransactionFixture,
    ):
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("protocol", "Unknown"),
                ]
            )
            response = controller_fixture.process_discovery_services()
            assert response == UNKNOWN_PROTOCOL

    def test_discovery_services_post_error_no_protocol(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller_fixture: DiscoveryServicesControllerFixture,
        db: DatabaseTransactionFixture,
    ):
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                ]
            )
            response = controller_fixture.process_discovery_services()
            assert response == NO_PROTOCOL_FOR_NEW_SERVICE

    def test_discovery_services_post_error_missing_service(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller_fixture: DiscoveryServicesControllerFixture,
        db: DatabaseTransactionFixture,
    ):
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("id", "123"),
                    ("protocol", controller_fixture.protocol),
                ]
            )
            response = controller_fixture.process_discovery_services()
            assert response == MISSING_SERVICE

    def test_discovery_services_post_error_already_in_use(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller_fixture: DiscoveryServicesControllerFixture,
        db: DatabaseTransactionFixture,
    ):
        integration_url = db.fresh_url()
        existing_integration = db.discovery_service_integration(url=integration_url)
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            assert isinstance(existing_integration.name, str)
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", existing_integration.name),
                    ("protocol", controller_fixture.protocol),
                    ("url", "http://test.com"),
                ]
            )
            response = controller_fixture.process_discovery_services()
            assert response == INTEGRATION_NAME_ALREADY_IN_USE

    def test_discovery_services_post_error_url_already_in_use(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller_fixture: DiscoveryServicesControllerFixture,
        db: DatabaseTransactionFixture,
    ):
        integration_url = db.fresh_url()
        existing_integration = db.discovery_service_integration(url=integration_url)
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            assert isinstance(existing_integration.protocol, str)
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "new name"),
                    ("protocol", existing_integration.protocol),
                    ("url", integration_url),
                ]
            )
            response = controller_fixture.process_discovery_services()
            assert response == INTEGRATION_URL_ALREADY_IN_USE

    def test_discovery_services_post_error_incomplete(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller_fixture: DiscoveryServicesControllerFixture,
        db: DatabaseTransactionFixture,
    ):
        integration_url = db.fresh_url()
        existing_integration = db.discovery_service_integration(url=integration_url)
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(existing_integration.id)),
                    ("protocol", controller_fixture.protocol),
                ]
            )
            response = controller_fixture.process_discovery_services()
            assert isinstance(response, ProblemDetail)
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

    def test_discovery_services_post_error_not_authorized(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller_fixture: DiscoveryServicesControllerFixture,
        db: DatabaseTransactionFixture,
    ):
        with flask_app_fixture.test_request_context("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("protocol", controller_fixture.protocol),
                    ("url", "registry url"),
                ]
            )
            pytest.raises(
                AdminNotAuthorized, controller_fixture.process_discovery_services
            )

    def test_discovery_services_post_create(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller_fixture: DiscoveryServicesControllerFixture,
        db: DatabaseTransactionFixture,
    ):
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("protocol", controller_fixture.protocol),
                    ("url", "http://registry.url"),
                ]
            )
            response = controller_fixture.process_discovery_services()
            assert response.status_code == 201

        service = get_one(
            db.session,
            IntegrationConfiguration,
            goal=Goals.DISCOVERY_GOAL,
        )
        assert isinstance(service, IntegrationConfiguration)
        assert isinstance(response, Response)
        assert service.id == int(response.get_data(as_text=True))
        assert controller_fixture.protocol == service.protocol
        assert (
            OpdsRegistrationService.settings_load(service).url == "http://registry.url"
        )

    def test_discovery_services_post_edit(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller_fixture: DiscoveryServicesControllerFixture,
        db: DatabaseTransactionFixture,
    ):
        discovery_service = db.discovery_service_integration()

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("id", str(discovery_service.id)),
                    ("protocol", controller_fixture.protocol),
                    ("url", "http://new_registry_url.com"),
                ]
            )
            response = controller_fixture.process_discovery_services()
            assert response.status_code == 200

        assert isinstance(response, Response)
        assert discovery_service.id == int(response.get_data(as_text=True))
        assert controller_fixture.protocol == discovery_service.protocol
        assert (
            "http://new_registry_url.com"
            == OpdsRegistrationService.settings_load(discovery_service).url
        )

    def test_check_name_unique_error(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller_fixture: DiscoveryServicesControllerFixture,
        db: DatabaseTransactionFixture,
    ):
        existing_service = db.discovery_service_integration()
        new_service = db.discovery_service_integration()

        # Try to change new service so that it has the same name as existing service
        # -- this is not allowed.
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", str(existing_service.name)),
                    ("id", str(new_service.id)),
                    ("protocol", controller_fixture.protocol),
                    ("url", "http://test.com"),
                ]
            )
            response = controller_fixture.process_discovery_services()
            assert response == INTEGRATION_NAME_ALREADY_IN_USE

    def test_check_name_unique_edit(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller_fixture: DiscoveryServicesControllerFixture,
        db: DatabaseTransactionFixture,
    ):
        existing_service = db.discovery_service_integration()

        # Try to edit existing service without changing its name -- this is fine.
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", str(existing_service.name)),
                    ("id", str(existing_service.id)),
                    ("protocol", controller_fixture.protocol),
                    ("url", "http://test.com"),
                ]
            )
            response = controller_fixture.process_discovery_services()
            assert isinstance(response, Response)
            assert response.status_code == 200

        # Changing the existing service's name is also fine.
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "New name"),
                    ("id", str(existing_service.id)),
                    ("protocol", controller_fixture.protocol),
                    ("url", "http://test.com"),
                ]
            )
            response = controller_fixture.process_discovery_services()
            assert isinstance(response, Response)
            assert response.status_code == 200

    def test_discovery_service_delete(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller_fixture: DiscoveryServicesControllerFixture,
        db: DatabaseTransactionFixture,
    ):
        discovery_service = db.discovery_service_integration()

        with flask_app_fixture.test_request_context("/", method="DELETE"):
            pytest.raises(
                AdminNotAuthorized,
                controller_fixture.controller.process_delete,
                discovery_service.id,
            )

        with flask_app_fixture.test_request_context_system_admin("/", method="DELETE"):
            assert discovery_service.id is not None
            response = controller_fixture.controller.process_delete(
                discovery_service.id
            )
            assert response.status_code == 200

        service = get_one(
            db.session,
            IntegrationConfiguration,
            id=discovery_service.id,
        )
        assert service is None

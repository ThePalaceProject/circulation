import json
from unittest.mock import MagicMock

import flask
import pytest
from flask import Response
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.controller.metadata_services import MetadataServicesController
from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import (
    CANNOT_CHANGE_PROTOCOL,
    DUPLICATE_INTEGRATION,
    INCOMPLETE_CONFIGURATION,
    INTEGRATION_NAME_ALREADY_IN_USE,
    MISSING_SERVICE,
    NO_PROTOCOL_FOR_NEW_SERVICE,
    NO_SUCH_LIBRARY,
    UNKNOWN_PROTOCOL,
)
from core.model import ExternalIntegration, IntegrationConfiguration, create, get_one
from core.util.problem_detail import ProblemDetail
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


class MetadataServicesFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        novelist_protocol = ExternalIntegration.NOVELIST
        assert novelist_protocol is not None
        self.novelist_protocol = novelist_protocol

        nyt_protocol = ExternalIntegration.NYT
        assert nyt_protocol is not None
        self.nyt_protocol = nyt_protocol

        manager = MagicMock()
        manager._db = db.session
        self.controller = MetadataServicesController(manager)
        self.db = db

    def create_novelist_integration(
        self,
        username: str = "user",
        password: str = "pass",
    ) -> ExternalIntegration:
        integration = self.db.external_integration(
            protocol=self.novelist_protocol,
            goal=ExternalIntegration.METADATA_GOAL,
        )
        integration.username = username
        integration.password = password
        return integration

    def create_nyt_integration(
        self,
        api_key: str = "xyz",
    ) -> ExternalIntegration:
        integration = self.db.external_integration(
            protocol=self.nyt_protocol,
            goal=ExternalIntegration.METADATA_GOAL,
        )
        integration.password = api_key
        return integration


@pytest.fixture
def metadata_services_fixture(
    db: DatabaseTransactionFixture,
) -> MetadataServicesFixture:
    return MetadataServicesFixture(db)


class TestMetadataServices:
    def test_process_metadata_services_dispatches_by_request_method(
        self,
        metadata_services_fixture: MetadataServicesFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        controller = metadata_services_fixture.controller

        # Make sure permissions are checked.
        with flask_app_fixture.test_request_context("/"):
            pytest.raises(AdminNotAuthorized, controller.process_metadata_services)

        # Mock out the process_get and process_post methods so we can
        # verify that they're called.
        controller.process_get = MagicMock()
        controller.process_post = MagicMock()

        with flask_app_fixture.test_request_context_system_admin("/"):
            controller.process_metadata_services()
            controller.process_get.assert_called_once()
            controller.process_post.assert_not_called()

        controller.process_get = MagicMock()
        controller.process_post = MagicMock()
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            controller.process_metadata_services()
            controller.process_get.assert_not_called()
            controller.process_post.assert_called_once()

    def test_process_get_with_no_services(
        self, metadata_services_fixture: MetadataServicesFixture
    ):
        response_content = metadata_services_fixture.controller.process_get()
        assert isinstance(response_content, dict)
        assert response_content.get("metadata_services") == []
        [nyt, novelist] = response_content.get("protocols", [])

        assert novelist.get("name") == metadata_services_fixture.novelist_protocol
        assert "settings" in novelist
        assert novelist.get("sitewide") is False

        assert nyt.get("name") == metadata_services_fixture.nyt_protocol
        assert "settings" in nyt
        assert nyt.get("sitewide") is True

    def test_process_get_with_one_service(
        self,
        metadata_services_fixture: MetadataServicesFixture,
        db: DatabaseTransactionFixture,
    ):
        novelist_service = metadata_services_fixture.create_novelist_integration()
        controller = metadata_services_fixture.controller

        response_data = controller.process_get()
        assert isinstance(response_data, dict)
        [service] = response_data.get("metadata_services", [])

        assert service.get("id") == novelist_service.id
        assert service.get("protocol") == metadata_services_fixture.novelist_protocol
        assert service.get("settings").get("username") == "user"
        assert service.get("settings").get("password") == "pass"

        novelist_service.libraries += [db.default_library()]
        response_data = controller.process_get()
        assert isinstance(response_data, dict)
        [service] = response_data.get("metadata_services", [])

        [library] = service.get("libraries")
        assert library.get("short_name") == db.default_library().short_name

    def test_metadata_services_post_errors(
        self,
        metadata_services_fixture: MetadataServicesFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        controller = metadata_services_fixture.controller
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("protocol", "Unknown"),
                ]
            )
            response = controller.process_post()
            assert response == UNKNOWN_PROTOCOL

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict([])
            response = controller.process_post()
            assert isinstance(response, ProblemDetail)
            assert response == INCOMPLETE_CONFIGURATION

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                ]
            )
            response = controller.process_post()
            assert isinstance(response, ProblemDetail)
            assert response == NO_PROTOCOL_FOR_NEW_SERVICE

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("id", "123"),
                    ("protocol", metadata_services_fixture.novelist_protocol),
                ]
            )
            response = controller.process_post()
            assert isinstance(response, ProblemDetail)
            assert response == MISSING_SERVICE

        service = metadata_services_fixture.create_novelist_integration()
        service.name = "name"

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", str(service.name)),
                    ("protocol", metadata_services_fixture.nyt_protocol),
                ]
            )
            response = controller.process_post()
            assert isinstance(response, ProblemDetail)
            assert response == INTEGRATION_NAME_ALREADY_IN_USE

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("id", str(service.id)),
                    ("protocol", metadata_services_fixture.nyt_protocol),
                ]
            )
            response = controller.process_post()
            assert response == CANNOT_CHANGE_PROTOCOL

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(service.id)),
                    ("protocol", metadata_services_fixture.novelist_protocol),
                ]
            )
            response = controller.process_post()
            assert isinstance(response, ProblemDetail)
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("id", str(service.id)),
                    ("protocol", str(service.protocol)),
                    ("username", "user"),
                    ("password", "pass"),
                    ("libraries", json.dumps([{"short_name": "not-a-library"}])),
                ]
            )
            response = controller.process_post()
            assert isinstance(response, ProblemDetail)
            assert response.uri == NO_SUCH_LIBRARY.uri

    def test_metadata_services_post_create(
        self,
        metadata_services_fixture: MetadataServicesFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        controller = metadata_services_fixture.controller
        library = db.library(
            name="Library",
            short_name="L",
        )
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("protocol", metadata_services_fixture.novelist_protocol),
                    ("username", "user"),
                    ("password", "pass"),
                    ("libraries", json.dumps([{"short_name": "L"}])),
                ]
            )
            response = controller.process_post()
            assert isinstance(response, Response)
            assert response.status_code == 201

        # A new IntegrationConfiguration has been created based on the submitted
        # information.
        service = get_one(
            db.session,
            ExternalIntegration,
            goal=ExternalIntegration.METADATA_GOAL,
        )
        assert service is not None
        assert service.id == int(response.get_data(as_text=True))
        assert service.protocol == metadata_services_fixture.novelist_protocol
        assert service.username == "user"
        assert service.password == "pass"
        assert service.libraries == [library]

    def test_metadata_services_post_create_multiple(
        self,
        metadata_services_fixture: MetadataServicesFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        controller = metadata_services_fixture.controller
        metadata_services_fixture.create_novelist_integration()
        metadata_services_fixture.create_nyt_integration()

        # If we try to create a second NYT service, we'll get an error.
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("protocol", metadata_services_fixture.nyt_protocol),
                    ("password", "pass"),
                ]
            )
            response = controller.process_post()
            assert isinstance(response, ProblemDetail)
            assert response == DUPLICATE_INTEGRATION

        # However we can create a second NoveList service.
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("protocol", metadata_services_fixture.novelist_protocol),
                    ("username", "user"),
                    ("password", "pass"),
                ]
            )
            response = controller.process_post()
            assert isinstance(response, Response)
            assert response.status_code == 201

    def test_metadata_services_post_edit(
        self,
        metadata_services_fixture: MetadataServicesFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        l1 = db.library(
            name="Library 1",
            short_name="L1",
        )
        l2 = db.library(
            name="Library 2",
            short_name="L2",
        )
        novelist_service = metadata_services_fixture.create_novelist_integration(
            username="olduser", password="oldpass"
        )
        novelist_service.libraries = [l1]

        controller = metadata_services_fixture.controller
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("id", str(novelist_service.id)),
                    ("protocol", str(novelist_service.protocol)),
                    ("username", "newuser"),
                    ("password", "newpass"),
                    ("libraries", json.dumps([{"short_name": "L2"}])),
                ]
            )
            response = controller.process_post()
            assert response.status_code == 200

        # The existing integration has been updated based on the submitted
        # information.
        assert novelist_service.username == "newuser"
        assert novelist_service.password == "newpass"
        assert novelist_service.libraries == [l2]

    def test_check_name_unique(
        self,
        metadata_services_fixture: MetadataServicesFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        existing_service, ignore = create(
            db.session,
            ExternalIntegration,
            name="existing service",
            protocol=ExternalIntegration.NYT,
            goal=ExternalIntegration.METADATA_GOAL,
        )
        new_service, ignore = create(
            db.session,
            ExternalIntegration,
            name="new service",
            protocol=ExternalIntegration.NYT,
            goal=ExternalIntegration.METADATA_GOAL,
        )

        # Try to change new service so that it has the same name as existing service
        # -- this is not allowed.
        controller = metadata_services_fixture.controller
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", str(existing_service.name)),
                    ("id", str(new_service.id)),
                    ("protocol", str(new_service.protocol)),
                    ("username", "user"),
                    ("password", "pass"),
                ]
            )
            response = controller.process_post()
            assert isinstance(response, ProblemDetail)
            assert response == INTEGRATION_NAME_ALREADY_IN_USE

        # Try to edit existing service without changing its name -- this is fine.
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", str(existing_service.name)),
                    ("id", str(existing_service.id)),
                    ("protocol", str(new_service.protocol)),
                    ("username", "user"),
                    ("password", "pass"),
                ]
            )
            response = controller.process_post()
            assert isinstance(response, Response)
            assert response.status_code == 200

        # Changing the existing service's name is also fine.
        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "New Name"),
                    ("id", str(existing_service.id)),
                    ("protocol", str(new_service.protocol)),
                    ("username", "user"),
                    ("password", "pass"),
                ]
            )
            response = controller.process_post()
            assert isinstance(response, Response)
            assert response.status_code == 200

    def test_metadata_service_delete(
        self,
        metadata_services_fixture: MetadataServicesFixture,
        flask_app_fixture: FlaskAppFixture,
        db: DatabaseTransactionFixture,
    ):
        l1 = db.library(
            name="Library 1",
            short_name="L1",
        )
        novelist_service = metadata_services_fixture.create_novelist_integration(
            username="olduser", password="oldpass"
        )
        novelist_service.libraries = [l1]

        controller = metadata_services_fixture.controller
        with flask_app_fixture.test_request_context("/", method="DELETE"):
            pytest.raises(
                AdminNotAuthorized,
                controller.process_delete,
                novelist_service.id,
            )

        with flask_app_fixture.test_request_context_system_admin("/", method="DELETE"):
            service_id = novelist_service.id
            assert isinstance(service_id, int)
            response = controller.process_delete(service_id)
            assert response.status_code == 200

        service = get_one(
            db.session,
            IntegrationConfiguration,
            id=novelist_service.id,
        )
        assert service is None

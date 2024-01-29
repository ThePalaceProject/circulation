import json
from contextlib import nullcontext
from unittest.mock import MagicMock

import flask
import pytest
from flask import Response
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.controller.catalog_services import CatalogServicesController
from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import (
    CANNOT_CHANGE_PROTOCOL,
    INTEGRATION_NAME_ALREADY_IN_USE,
    MISSING_SERVICE,
    MISSING_SERVICE_NAME,
    MULTIPLE_SERVICES_FOR_LIBRARY,
    NO_PROTOCOL_FOR_NEW_SERVICE,
    UNKNOWN_PROTOCOL,
)
from api.integration.registry.catalog_services import CatalogServicesRegistry
from core.integration.goals import Goals
from core.marc import MARCExporter, MarcExporterLibrarySettings
from core.model import IntegrationConfiguration, get_one
from core.util.problem_detail import ProblemDetail
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


@pytest.fixture
def controller(db: DatabaseTransactionFixture) -> CatalogServicesController:
    mock_manager = MagicMock()
    mock_manager._db = db.session
    return CatalogServicesController(mock_manager)


class TestCatalogServicesController:
    def test_catalog_services_get_with_no_services(
        self, flask_app_fixture: FlaskAppFixture, controller: CatalogServicesController
    ):
        with flask_app_fixture.test_request_context("/"):
            pytest.raises(
                AdminNotAuthorized,
                controller.process_catalog_services,
            )

        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller.process_catalog_services()
            assert isinstance(response, Response)
            assert response.status_code == 200
            data = response.json
            assert isinstance(data, dict)
            assert data.get("catalog_services") == []
            protocols = data.get("protocols")
            assert isinstance(protocols, list)
            assert 1 == len(protocols)

            assert protocols[0].get("name") == CatalogServicesRegistry().get_protocol(
                MARCExporter
            )
            assert "settings" in protocols[0]
            assert "library_settings" in protocols[0]

    def test_catalog_services_get_with_marc_exporter(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: CatalogServicesController,
        db: DatabaseTransactionFixture,
    ):
        library_settings = MarcExporterLibrarySettings(
            include_summary=True, include_genres=True, organization_code="US-MaBoDPL"
        )

        protocol = CatalogServicesRegistry().get_protocol(MARCExporter)
        assert protocol is not None
        integration = db.integration_configuration(
            protocol,
            Goals.CATALOG_GOAL,
            name="name",
        )

        integration.libraries += [db.default_library()]
        library_settings_integration = integration.for_library(db.default_library())
        MARCExporter.library_settings_update(
            library_settings_integration, library_settings
        )

        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller.process_catalog_services()
            assert isinstance(response, Response)
            assert response.status_code == 200
            data = response.json
            assert isinstance(data, dict)
            services = data.get("catalog_services")
            assert isinstance(services, list)
            assert len(services) == 1
            service = services[0]
            assert integration.id == service.get("id")
            assert integration.name == service.get("name")
            assert integration.protocol == service.get("protocol")
            [library] = service.get("libraries")
            assert db.default_library().short_name == library.get("short_name")
            assert "US-MaBoDPL" == library.get("organization_code")
            assert library.get("include_summary") is True
            assert library.get("include_genres") is True

    @pytest.mark.parametrize(
        "post_data,expected,admin,raises",
        [
            pytest.param({}, None, False, AdminNotAuthorized, id="not admin"),
            pytest.param({}, NO_PROTOCOL_FOR_NEW_SERVICE, True, None, id="no protocol"),
            pytest.param(
                {"protocol": "Unknown"},
                UNKNOWN_PROTOCOL,
                True,
                None,
                id="unknown protocol",
            ),
            pytest.param(
                {"protocol": "MARCExporter", "id": "123"},
                MISSING_SERVICE,
                True,
                None,
                id="unknown id",
            ),
            pytest.param(
                {"protocol": "MARCExporter", "id": "<existing>"},
                CANNOT_CHANGE_PROTOCOL,
                True,
                None,
                id="cannot change protocol",
            ),
            pytest.param(
                {"protocol": "MARCExporter"},
                MISSING_SERVICE_NAME,
                True,
                None,
                id="no name",
            ),
            pytest.param(
                {"protocol": "MARCExporter", "name": "existing integration"},
                INTEGRATION_NAME_ALREADY_IN_USE,
                True,
                None,
                id="name already in use",
            ),
            pytest.param(
                {
                    "protocol": "MARCExporter",
                    "name": "new name",
                    "libraries": json.dumps([{"short_name": "default"}]),
                },
                MULTIPLE_SERVICES_FOR_LIBRARY,
                True,
                None,
                id="multiple services for library",
            ),
        ],
    )
    def test_catalog_services_post_errors(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: CatalogServicesController,
        db: DatabaseTransactionFixture,
        post_data: dict[str, str],
        expected: ProblemDetail | None,
        admin: bool,
        raises: type[Exception] | None,
    ):
        if admin:
            make_request = flask_app_fixture.test_request_context_system_admin
        else:
            make_request = flask_app_fixture.test_request_context

        context_manager = pytest.raises(raises) if raises is not None else nullcontext()

        service = db.integration_configuration(
            "fake protocol",
            Goals.CATALOG_GOAL,
            name="existing integration",
        )
        service.libraries += [db.default_library()]

        if post_data.get("id") == "<existing>":
            post_data["id"] = str(service.id)

        with make_request("/", method="POST"):
            flask.request.form = ImmutableMultiDict(post_data)
            with context_manager:
                response = controller.process_catalog_services()
                assert isinstance(response, ProblemDetail)
                assert isinstance(expected, ProblemDetail)
                assert response.uri == expected.uri
                assert response.status_code == expected.status_code
                assert response.title == expected.title

    def test_catalog_services_post_create(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: CatalogServicesController,
        db: DatabaseTransactionFixture,
    ):
        protocol = CatalogServicesRegistry().get_protocol(MARCExporter)
        assert protocol is not None

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "exporter name"),
                    ("protocol", protocol),
                    (
                        "libraries",
                        json.dumps(
                            [
                                {
                                    "short_name": db.default_library().short_name,
                                    "include_summary": "false",
                                    "include_genres": "true",
                                }
                            ]
                        ),
                    ),
                ]
            )
            response = controller.process_catalog_services()
            assert isinstance(response, Response)
            assert response.status_code == 201

        service = get_one(
            db.session,
            IntegrationConfiguration,
            goal=Goals.CATALOG_GOAL,
        )
        assert isinstance(service, IntegrationConfiguration)

        assert int(response.get_data()) == service.id
        assert service.protocol == protocol
        assert service.name == "exporter name"
        assert service.libraries == [db.default_library()]

        settings = MARCExporter.library_settings_load(service.library_configurations[0])
        assert settings.include_summary is False
        assert settings.include_genres is True

    def test_catalog_services_post_edit(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: CatalogServicesController,
        db: DatabaseTransactionFixture,
    ):
        protocol = CatalogServicesRegistry().get_protocol(MARCExporter)
        assert protocol is not None

        service = db.integration_configuration(
            protocol,
            Goals.CATALOG_GOAL,
            name="name",
        )

        with flask_app_fixture.test_request_context_system_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "exporter name"),
                    ("id", str(service.id)),
                    ("protocol", protocol),
                    (
                        "libraries",
                        json.dumps(
                            [
                                {
                                    "short_name": db.default_library().short_name,
                                    "include_summary": "true",
                                    "include_genres": "false",
                                }
                            ]
                        ),
                    ),
                ]
            )
            response = controller.process_catalog_services()
            assert isinstance(response, Response)
            assert response.status_code == 200

        assert int(response.get_data()) == service.id
        assert service.protocol == protocol
        assert service.name == "exporter name"
        assert service.libraries == [db.default_library()]

        settings = MARCExporter.library_settings_load(service.library_configurations[0])
        assert settings.include_summary is True
        assert settings.include_genres is False

    def test_catalog_services_delete(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: CatalogServicesController,
        db: DatabaseTransactionFixture,
    ):
        protocol = CatalogServicesRegistry().get_protocol(MARCExporter)
        assert protocol is not None

        service = db.integration_configuration(
            protocol,
            Goals.CATALOG_GOAL,
            name="name",
        )

        with flask_app_fixture.test_request_context("/", method="DELETE"):
            pytest.raises(
                AdminNotAuthorized,
                controller.process_delete,
                service.id,
            )

        with flask_app_fixture.test_request_context_system_admin("/", method="DELETE"):
            response = controller.process_delete(service.id)
            assert isinstance(response, Response)
            assert response.status_code == 200

        none_service = get_one(db.session, IntegrationConfiguration, id=service.id)
        assert none_service is None

import json

import flask
import pytest
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import (
    CANNOT_CHANGE_PROTOCOL,
    INTEGRATION_NAME_ALREADY_IN_USE,
    MISSING_SERVICE,
    MULTIPLE_SERVICES_FOR_LIBRARY,
    UNKNOWN_PROTOCOL,
)
from core.marc import MARCExporter
from core.model import (
    AdminRole,
    ConfigurationSetting,
    ExternalIntegration,
    create,
    get_one,
)
from tests.fixtures.api_admin import SettingsControllerFixture


class TestCatalogServicesController:
    def test_catalog_services_get_with_no_services(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = (
                settings_ctrl_fixture.manager.admin_catalog_services_controller.process_catalog_services()
            )
            assert response.get("catalog_services") == []
            protocols = response.get("protocols")
            assert 1 == len(protocols)
            assert MARCExporter.NAME == protocols[0].get("name")
            assert "settings" in protocols[0]
            assert "library_settings" in protocols[0]

            settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            settings_ctrl_fixture.ctrl.db.session.flush()
            pytest.raises(
                AdminNotAuthorized,
                settings_ctrl_fixture.manager.admin_catalog_services_controller.process_catalog_services,
            )

    def test_catalog_services_get_with_marc_exporter(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        integration, ignore = create(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            protocol=ExternalIntegration.MARC_EXPORT,
            goal=ExternalIntegration.CATALOG_GOAL,
            name="name",
        )
        integration.libraries += [settings_ctrl_fixture.ctrl.db.default_library()]
        ConfigurationSetting.for_library_and_externalintegration(
            settings_ctrl_fixture.ctrl.db.session,
            MARCExporter.MARC_ORGANIZATION_CODE,
            settings_ctrl_fixture.ctrl.db.default_library(),
            integration,
        ).value = "US-MaBoDPL"
        ConfigurationSetting.for_library_and_externalintegration(
            settings_ctrl_fixture.ctrl.db.session,
            MARCExporter.INCLUDE_SUMMARY,
            settings_ctrl_fixture.ctrl.db.default_library(),
            integration,
        ).value = "false"
        ConfigurationSetting.for_library_and_externalintegration(
            settings_ctrl_fixture.ctrl.db.session,
            MARCExporter.INCLUDE_SIMPLIFIED_GENRES,
            settings_ctrl_fixture.ctrl.db.default_library(),
            integration,
        ).value = "true"

        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = (
                settings_ctrl_fixture.manager.admin_catalog_services_controller.process_catalog_services()
            )
            [service] = response.get("catalog_services")
            assert integration.id == service.get("id")
            assert integration.name == service.get("name")
            assert integration.protocol == service.get("protocol")
            [library] = service.get("libraries")
            assert (
                settings_ctrl_fixture.ctrl.db.default_library().short_name
                == library.get("short_name")
            )
            assert "US-MaBoDPL" == library.get(MARCExporter.MARC_ORGANIZATION_CODE)
            assert "false" == library.get(MARCExporter.INCLUDE_SUMMARY)
            assert "true" == library.get(MARCExporter.INCLUDE_SIMPLIFIED_GENRES)

    def test_catalog_services_post_errors(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("protocol", "Unknown"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_catalog_services_controller.process_catalog_services()
            )
            assert response == UNKNOWN_PROTOCOL

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", "123"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_catalog_services_controller.process_catalog_services()
            )
            assert response == MISSING_SERVICE

        service, ignore = create(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            protocol="fake protocol",
            goal=ExternalIntegration.CATALOG_GOAL,
            name="name",
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(service.id)),
                    ("protocol", ExternalIntegration.MARC_EXPORT),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_catalog_services_controller.process_catalog_services()
            )
            assert response == CANNOT_CHANGE_PROTOCOL

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", str(service.name)),
                    ("protocol", ExternalIntegration.MARC_EXPORT),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_catalog_services_controller.process_catalog_services()
            )
            assert response == INTEGRATION_NAME_ALREADY_IN_USE

        service, ignore = create(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            protocol=ExternalIntegration.MARC_EXPORT,
            goal=ExternalIntegration.CATALOG_GOAL,
        )

        # This should be the last test to check since rolling back database
        # changes in the test can cause it to crash.
        service.libraries += [settings_ctrl_fixture.ctrl.db.default_library()]
        settings_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            ME = MARCExporter
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "new name"),
                    ("protocol", ME.NAME),
                    (
                        "libraries",
                        json.dumps(
                            [
                                {
                                    "short_name": settings_ctrl_fixture.ctrl.db.default_library().short_name,
                                    ME.INCLUDE_SUMMARY: "false",
                                    ME.INCLUDE_SIMPLIFIED_GENRES: "true",
                                }
                            ]
                        ),
                    ),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_catalog_services_controller.process_catalog_services()
            )
            assert response.uri == MULTIPLE_SERVICES_FOR_LIBRARY.uri

    def test_catalog_services_post_create(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        ME = MARCExporter

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "exporter name"),
                    ("protocol", ME.NAME),
                    (
                        "libraries",
                        json.dumps(
                            [
                                {
                                    "short_name": settings_ctrl_fixture.ctrl.db.default_library().short_name,
                                    ME.INCLUDE_SUMMARY: "false",
                                    ME.INCLUDE_SIMPLIFIED_GENRES: "true",
                                }
                            ]
                        ),
                    ),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_catalog_services_controller.process_catalog_services()
            )
            assert response.status_code == 201

        service = get_one(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            goal=ExternalIntegration.CATALOG_GOAL,
        )
        assert isinstance(service, ExternalIntegration)

        assert service.id == int(response.get_data())
        assert ME.NAME == service.protocol
        assert "exporter name" == service.name
        assert [settings_ctrl_fixture.ctrl.db.default_library()] == service.libraries
        assert (
            "false"
            == ConfigurationSetting.for_library_and_externalintegration(
                settings_ctrl_fixture.ctrl.db.session,
                ME.INCLUDE_SUMMARY,
                settings_ctrl_fixture.ctrl.db.default_library(),
                service,
            ).value
        )
        assert (
            "true"
            == ConfigurationSetting.for_library_and_externalintegration(
                settings_ctrl_fixture.ctrl.db.session,
                ME.INCLUDE_SIMPLIFIED_GENRES,
                settings_ctrl_fixture.ctrl.db.default_library(),
                service,
            ).value
        )

    def test_catalog_services_post_edit(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        ME = MARCExporter

        service, ignore = create(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            protocol=ME.NAME,
            goal=ExternalIntegration.CATALOG_GOAL,
            name="name",
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "exporter name"),
                    ("id", str(service.id)),
                    ("protocol", ME.NAME),
                    (
                        "libraries",
                        json.dumps(
                            [
                                {
                                    "short_name": settings_ctrl_fixture.ctrl.db.default_library().short_name,
                                    ME.INCLUDE_SUMMARY: "false",
                                    ME.INCLUDE_SIMPLIFIED_GENRES: "true",
                                }
                            ]
                        ),
                    ),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_catalog_services_controller.process_catalog_services()
            )
            assert response.status_code == 200

        assert service.id == int(response.get_data())
        assert ME.NAME == service.protocol
        assert "exporter name" == service.name
        assert [settings_ctrl_fixture.ctrl.db.default_library()] == service.libraries
        assert (
            "false"
            == ConfigurationSetting.for_library_and_externalintegration(
                settings_ctrl_fixture.ctrl.db.session,
                ME.INCLUDE_SUMMARY,
                settings_ctrl_fixture.ctrl.db.default_library(),
                service,
            ).value
        )
        assert (
            "true"
            == ConfigurationSetting.for_library_and_externalintegration(
                settings_ctrl_fixture.ctrl.db.session,
                ME.INCLUDE_SIMPLIFIED_GENRES,
                settings_ctrl_fixture.ctrl.db.default_library(),
                service,
            ).value
        )

    def test_catalog_services_delete(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        ME = MARCExporter
        service, ignore = create(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            protocol=ME.NAME,
            goal=ExternalIntegration.CATALOG_GOAL,
            name="name",
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="DELETE"):
            settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            pytest.raises(
                AdminNotAuthorized,
                settings_ctrl_fixture.manager.admin_catalog_services_controller.process_delete,
                service.id,
            )

            settings_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = settings_ctrl_fixture.manager.admin_catalog_services_controller.process_delete(
                service.id
            )
            assert response.status_code == 200

        none_service = get_one(
            settings_ctrl_fixture.ctrl.db.session, ExternalIntegration, id=service.id
        )
        assert none_service is None

import json

import flask
import pytest
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import (
    CANNOT_CHANGE_PROTOCOL,
    INCOMPLETE_CONFIGURATION,
    INTEGRATION_NAME_ALREADY_IN_USE,
    MISSING_ANALYTICS_NAME,
    MISSING_SERVICE,
    NO_PROTOCOL_FOR_NEW_SERVICE,
    NO_SUCH_LIBRARY,
    UNKNOWN_PROTOCOL,
)
from api.s3_analytics_provider import S3AnalyticsProvider
from core.local_analytics_provider import LocalAnalyticsProvider
from core.model import AdminRole, ExternalIntegration, create, get_one
from tests.fixtures.api_admin import SettingsControllerFixture


class TestAnalyticsServices:
    def test_analytics_services_get_with_one_default_service(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = (
                settings_ctrl_fixture.manager.admin_analytics_services_controller.process_analytics_services()
            )
            assert len(response.get("analytics_services")) == 1
            local_analytics = response.get("analytics_services")[0]
            assert local_analytics.get("name") == LocalAnalyticsProvider.NAME
            assert local_analytics.get("protocol") == LocalAnalyticsProvider.__module__

            protocols = response.get("protocols")
            assert S3AnalyticsProvider.NAME in [p.get("label") for p in protocols]
            assert "settings" in protocols[0]

    def test_analytics_services_get_with_one_service(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        # Delete the local analytics service that gets created by default.
        local_analytics_default = get_one(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            protocol=LocalAnalyticsProvider.__module__,
        )

        settings_ctrl_fixture.ctrl.db.session.delete(local_analytics_default)

        local_service, ignore = create(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            protocol=LocalAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )

        local_service.libraries += [settings_ctrl_fixture.ctrl.db.default_library()]
        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = (
                settings_ctrl_fixture.manager.admin_analytics_services_controller.process_analytics_services()
            )
            [local_analytics] = response.get("analytics_services")

            assert local_service.id == local_analytics.get("id")
            assert local_service.protocol == local_analytics.get("protocol")
            assert local_analytics.get("protocol") == LocalAnalyticsProvider.__module__
            [library] = local_analytics.get("libraries")
            assert (
                settings_ctrl_fixture.ctrl.db.default_library().short_name
                == library.get("short_name")
            )

    def test_analytics_services_post_errors(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict([])
            response = (
                settings_ctrl_fixture.manager.admin_analytics_services_controller.process_analytics_services()
            )
            assert response == MISSING_ANALYTICS_NAME

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("protocol", "Unknown"),
                    ("url", "http://test"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_analytics_services_controller.process_analytics_services()
            )
            assert response == UNKNOWN_PROTOCOL

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("url", "http://test"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_analytics_services_controller.process_analytics_services()
            )
            assert response == NO_PROTOCOL_FOR_NEW_SERVICE

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("id", "123"),
                    ("url", "http://test"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_analytics_services_controller.process_analytics_services()
            )
            assert response.uri == MISSING_SERVICE.uri

        [local_analytics] = (
            settings_ctrl_fixture.ctrl.db.session.query(ExternalIntegration)
            .filter(ExternalIntegration.goal == ExternalIntegration.ANALYTICS_GOAL)
            .all()
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            assert isinstance(local_analytics.name, str)
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", local_analytics.name),
                    ("protocol", S3AnalyticsProvider.__module__),
                    ("url", "http://test"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_analytics_services_controller.process_analytics_services()
            )
            assert response == INTEGRATION_NAME_ALREADY_IN_USE

        service, ignore = create(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            protocol=S3AnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("name", "Name"),
                    ("id", str(service.id)),
                    ("protocol", "core.local_analytics_provider"),
                    ("url", "http://test"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_analytics_services_controller.process_analytics_services()
            )
            assert response == CANNOT_CHANGE_PROTOCOL

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(service.id)),
                    ("name", "analytics name"),
                    ("protocol", S3AnalyticsProvider.__module__),
                    ("url", ""),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_analytics_services_controller.process_analytics_services()
            )
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(service.id)),
                    ("protocol", S3AnalyticsProvider.__module__),
                    ("name", "some other analytics name"),
                    (ExternalIntegration.URL, "http://test"),
                    ("libraries", json.dumps([{"short_name": "not-a-library"}])),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_analytics_services_controller.process_analytics_services()
            )
            assert response.uri == NO_SUCH_LIBRARY.uri

        library = settings_ctrl_fixture.ctrl.db.library(
            name="Library",
            short_name="L",
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("id", str(service.id)),
                    ("protocol", S3AnalyticsProvider.__module__),
                    ("name", "some other name"),
                    (ExternalIntegration.URL, ""),
                    ("libraries", json.dumps([{"short_name": library.short_name}])),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_analytics_services_controller.process_analytics_services()
            )
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        settings_ctrl_fixture.admin.remove_role(AdminRole.LIBRARY_MANAGER)
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = ImmutableMultiDict(
                [
                    ("protocol", LocalAnalyticsProvider.__module__),
                    (ExternalIntegration.URL, "url"),
                    ("libraries", json.dumps([])),
                ]
            )
            pytest.raises(
                AdminNotAuthorized,
                settings_ctrl_fixture.manager.admin_analytics_services_controller.process_analytics_services,
            )

    def test_check_name_unique(self, settings_ctrl_fixture: SettingsControllerFixture):
        kwargs = dict(
            protocol=S3AnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
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
            settings_ctrl_fixture.manager.admin_analytics_services_controller.check_name_unique
        )

        # Try to change new service so that it has the same name as existing service
        # -- this is not allowed.
        result = m(new_service, existing_service.name)
        assert result == INTEGRATION_NAME_ALREADY_IN_USE

        # Try to edit existing service without changing its name -- this is fine.
        assert None == m(existing_service, existing_service.name)

        # Changing the existing service's name is also fine.
        assert None == m(existing_service, "new name")

    def test_analytics_service_delete(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        service, ignore = create(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            protocol=S3AnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="DELETE"):
            settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            pytest.raises(
                AdminNotAuthorized,
                settings_ctrl_fixture.manager.admin_analytics_services_controller.process_delete,
                service.id,
            )

            settings_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = settings_ctrl_fixture.manager.admin_analytics_services_controller.process_delete(
                service.id
            )
            assert response.status_code == 200

        service1 = get_one(
            settings_ctrl_fixture.ctrl.db.session, ExternalIntegration, id=service.id
        )
        assert None == service1

import flask
import pytest
from werkzeug.datastructures import MultiDict

from api.admin.exceptions import AdminNotAuthorized
from api.admin.problem_details import (
    INCOMPLETE_CONFIGURATION,
    INTEGRATION_NAME_ALREADY_IN_USE,
    MISSING_SERVICE,
    MULTIPLE_SITEWIDE_SERVICES,
    NO_PROTOCOL_FOR_NEW_SERVICE,
    UNKNOWN_PROTOCOL,
)
from core.external_search import ExternalSearchIndex
from core.model import AdminRole, ExternalIntegration, create, get_one


class TestSearchServices:
    def test_search_services_get_with_no_services(self, settings_ctrl_fixture):
        # Delete the search integration
        session = settings_ctrl_fixture.ctrl.db.session
        integration = ExternalIntegration.lookup(
            session, ExternalIntegration.OPENSEARCH, ExternalIntegration.SEARCH_GOAL
        )
        session.delete(integration)

        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = (
                settings_ctrl_fixture.manager.admin_search_services_controller.process_services()
            )
            assert response.get("search_services") == []
            protocols = response.get("protocols")
            assert ExternalIntegration.OPENSEARCH in [p.get("name") for p in protocols]
            assert "settings" in protocols[0]

            settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            settings_ctrl_fixture.ctrl.db.session.flush()
            pytest.raises(
                AdminNotAuthorized,
                settings_ctrl_fixture.manager.admin_search_services_controller.process_services,
            )

    def test_search_services_get_with_one_service(self, settings_ctrl_fixture):
        # Delete the pre-existing integration
        session = settings_ctrl_fixture.ctrl.db.session
        integration = ExternalIntegration.lookup(
            session, ExternalIntegration.OPENSEARCH, ExternalIntegration.SEARCH_GOAL
        )
        session.delete(integration)

        search_service, ignore = create(
            session,
            ExternalIntegration,
            protocol=ExternalIntegration.OPENSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )
        search_service.url = "search url"
        search_service.setting(
            ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY
        ).value = "works-index-prefix"
        search_service.setting(
            ExternalSearchIndex.TEST_SEARCH_TERM_KEY
        ).value = "search-term-for-self-tests"

        with settings_ctrl_fixture.request_context_with_admin("/"):
            response = (
                settings_ctrl_fixture.manager.admin_search_services_controller.process_services()
            )
            [service] = response.get("search_services")

            assert search_service.id == service.get("id")
            assert search_service.protocol == service.get("protocol")
            assert "search url" == service.get("settings").get(ExternalIntegration.URL)
            assert "works-index-prefix" == service.get("settings").get(
                ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY
            )
            assert "search-term-for-self-tests" == service.get("settings").get(
                ExternalSearchIndex.TEST_SEARCH_TERM_KEY
            )

    def test_search_services_post_errors(self, settings_ctrl_fixture):
        controller = settings_ctrl_fixture.manager.admin_search_services_controller

        # Delete the previous integrations
        session = settings_ctrl_fixture.ctrl.db.session
        integration = ExternalIntegration.lookup(
            session, ExternalIntegration.OPENSEARCH, ExternalIntegration.SEARCH_GOAL
        )
        session.delete(integration)

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("name", "Name"),
                    ("protocol", "Unknown"),
                ]
            )
            response = controller.process_services()
            assert response == UNKNOWN_PROTOCOL

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict([("name", "Name")])
            response = controller.process_services()
            assert response == NO_PROTOCOL_FOR_NEW_SERVICE

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("name", "Name"),
                    ("id", "123"),
                ]
            )
            response = controller.process_services()
            assert response == MISSING_SERVICE

        service, ignore = create(
            session,
            ExternalIntegration,
            protocol=ExternalIntegration.OPENSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("name", "Name"),
                    ("protocol", ExternalIntegration.OPENSEARCH),
                ]
            )
            response = controller.process_services()
            assert response.uri == MULTIPLE_SITEWIDE_SERVICES.uri

        session.delete(service)
        service, ignore = create(
            session,
            ExternalIntegration,
            protocol="test",
            goal=ExternalIntegration.LICENSE_GOAL,
            name="name",
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("name", service.name),
                    ("protocol", ExternalIntegration.OPENSEARCH),
                ]
            )
            response = controller.process_services()
            assert response == INTEGRATION_NAME_ALREADY_IN_USE

        service, ignore = create(
            session,
            ExternalIntegration,
            protocol=ExternalIntegration.OPENSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("name", "Name"),
                    ("id", service.id),
                    ("protocol", ExternalIntegration.OPENSEARCH),
                ]
            )
            response = controller.process_services()
            assert response.uri == INCOMPLETE_CONFIGURATION.uri

        settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("protocol", ExternalIntegration.OPENSEARCH),
                    (ExternalIntegration.URL, "search url"),
                    (ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY, "works-index-prefix"),
                ]
            )
            pytest.raises(AdminNotAuthorized, controller.process_services)

    def test_search_services_post_create(self, settings_ctrl_fixture):
        # Delete the previous integrations
        session = settings_ctrl_fixture.ctrl.db.session
        integration = ExternalIntegration.lookup(
            session, ExternalIntegration.OPENSEARCH, ExternalIntegration.SEARCH_GOAL
        )
        session.delete(integration)

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("name", "Name"),
                    ("protocol", ExternalIntegration.OPENSEARCH),
                    (ExternalIntegration.URL, "http://search_url"),
                    (ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY, "works-index-prefix"),
                    (ExternalSearchIndex.TEST_SEARCH_TERM_KEY, "sample-search-term"),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_search_services_controller.process_services()
            )
            assert response.status_code == 201

        service = get_one(
            session,
            ExternalIntegration,
            goal=ExternalIntegration.SEARCH_GOAL,
        )
        assert service.id == int(response.response[0])
        assert ExternalIntegration.OPENSEARCH == service.protocol
        assert "http://search_url" == service.url
        assert (
            "works-index-prefix"
            == service.setting(ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY).value
        )
        assert (
            "sample-search-term"
            == service.setting(ExternalSearchIndex.TEST_SEARCH_TERM_KEY).value
        )

    def test_search_services_post_edit(self, settings_ctrl_fixture):
        search_service, ignore = create(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            protocol=ExternalIntegration.OPENSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )
        search_service.url = "search url"
        search_service.setting(
            ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY
        ).value = "works-index-prefix"
        search_service.setting(
            ExternalSearchIndex.TEST_SEARCH_TERM_KEY
        ).value = "sample-search-term"

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("name", "Name"),
                    ("id", search_service.id),
                    ("protocol", ExternalIntegration.OPENSEARCH),
                    (ExternalIntegration.URL, "http://new_search_url"),
                    (
                        ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY,
                        "new-works-index-prefix",
                    ),
                    (
                        ExternalSearchIndex.TEST_SEARCH_TERM_KEY,
                        "new-sample-search-term",
                    ),
                ]
            )
            response = (
                settings_ctrl_fixture.manager.admin_search_services_controller.process_services()
            )
            assert response.status_code == 200

        assert search_service.id == int(response.response[0])
        assert ExternalIntegration.OPENSEARCH == search_service.protocol
        assert "http://new_search_url" == search_service.url
        assert (
            "new-works-index-prefix"
            == search_service.setting(ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY).value
        )
        assert (
            "new-sample-search-term"
            == search_service.setting(ExternalSearchIndex.TEST_SEARCH_TERM_KEY).value
        )

    def test_search_service_delete(self, settings_ctrl_fixture):
        search_service, ignore = create(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            protocol=ExternalIntegration.OPENSEARCH,
            goal=ExternalIntegration.SEARCH_GOAL,
        )
        search_service.url = "search url"
        search_service.setting(
            ExternalSearchIndex.WORKS_INDEX_PREFIX_KEY
        ).value = "works-index-prefix"

        with settings_ctrl_fixture.request_context_with_admin("/", method="DELETE"):
            settings_ctrl_fixture.admin.remove_role(AdminRole.SYSTEM_ADMIN)
            pytest.raises(
                AdminNotAuthorized,
                settings_ctrl_fixture.manager.admin_search_services_controller.process_delete,
                search_service.id,
            )

            settings_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = settings_ctrl_fixture.manager.admin_search_services_controller.process_delete(
                search_service.id
            )
            assert response.status_code == 200

        service = get_one(
            settings_ctrl_fixture.ctrl.db.session,
            ExternalIntegration,
            id=search_service.id,
        )
        assert None == service

"""Tests for GlobalSettingsController."""

from __future__ import annotations

import json

import pytest
from werkzeug.datastructures import ImmutableMultiDict

from palace.manager.api.admin.controller.global_settings import GlobalSettingsController
from palace.manager.integration.base import integration_settings_load
from palace.manager.integration.configuration.global_settings import (
    GLOBAL_SETTINGS_PROTOCOL,
    GlobalSettings,
)
from palace.manager.integration.goals import Goals
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.problem_detail import ProblemDetail
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


@pytest.fixture
def controller(db: DatabaseTransactionFixture) -> GlobalSettingsController:
    return GlobalSettingsController(db.session)


class TestGlobalSettingsController:
    def test_get_returns_defaults_when_no_row_exists(
        self,
        controller: GlobalSettingsController,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
    ) -> None:
        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller.process_global_settings()
        assert response.status_code == 200
        body = json.loads(response.data)
        assert body["settings"]["country"] == "US"
        assert body["settings"]["state"] == "All"
        assert "schema" in body

    def test_get_creates_integration_row_on_first_call(
        self,
        controller: GlobalSettingsController,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
    ) -> None:
        assert (
            get_one(
                db.session,
                IntegrationConfiguration,
                goal=Goals.SITEWIDE_SETTINGS,
                protocol=GLOBAL_SETTINGS_PROTOCOL,
            )
            is None
        )
        with flask_app_fixture.test_request_context_system_admin("/"):
            controller.process_global_settings()
        integration = get_one(
            db.session,
            IntegrationConfiguration,
            goal=Goals.SITEWIDE_SETTINGS,
            protocol=GLOBAL_SETTINGS_PROTOCOL,
        )
        assert integration is not None

    def test_get_returns_existing_settings(
        self,
        controller: GlobalSettingsController,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
    ) -> None:
        # POST first to set values, then GET to verify.
        # The FlaskAppFixture resets request.form to an empty ImmutableMultiDict before
        # yielding, so we must set it explicitly inside the `with` block.
        with flask_app_fixture.test_request_context_system_admin(
            "/", method="POST"
        ) as c:
            c.request.form = ImmutableMultiDict(
                [("country", "CA"), ("state", "Ontario")]
            )
            controller.process_global_settings()
        with flask_app_fixture.test_request_context_system_admin("/"):
            response = controller.process_global_settings()
        body = json.loads(response.data)
        assert body["settings"]["country"] == "CA"
        assert body["settings"]["state"] == "Ontario"

    def test_post_updates_settings(
        self,
        controller: GlobalSettingsController,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
    ) -> None:
        # The FlaskAppFixture resets request.form to an empty ImmutableMultiDict before
        # yielding, so we must set it explicitly inside the `with` block.
        with flask_app_fixture.test_request_context_system_admin(
            "/", method="POST"
        ) as c:
            c.request.form = ImmutableMultiDict(
                [("country", "GB"), ("state", "England")]
            )
            response = controller.process_global_settings()
        assert response.status_code == 200
        integration = get_one(
            db.session,
            IntegrationConfiguration,
            goal=Goals.SITEWIDE_SETTINGS,
            protocol=GLOBAL_SETTINGS_PROTOCOL,
        )
        assert integration is not None
        settings = integration_settings_load(GlobalSettings, integration)
        assert settings.country == "GB"
        assert settings.state == "England"

    def test_non_system_admin_is_rejected(
        self,
        controller: GlobalSettingsController,
        flask_app_fixture: FlaskAppFixture,
    ) -> None:
        # A non-admin request context (no admin set)
        with flask_app_fixture.test_request_context("/"):
            result = controller.process_global_settings()
        # Returns a ProblemDetail, not a 200 Response
        assert isinstance(result, ProblemDetail)

"""Admin controller for global sitewide settings."""

from __future__ import annotations

from typing import TYPE_CHECKING

import flask
from flask import Response

from palace.manager.api.admin.controller.base import AdminPermissionsControllerMixin
from palace.manager.api.admin.exceptions import AdminNotAuthorized
from palace.manager.api.admin.form_data import ProcessFormData
from palace.manager.integration.base import (
    integration_settings_load,
    integration_settings_update,
)
from palace.manager.integration.configuration.global_settings import (
    GLOBAL_SETTINGS_PROTOCOL,
    GlobalSettings,
)
from palace.manager.integration.goals import Goals
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.util import create, get_one
from palace.manager.util.json import json_serializer
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

_GLOBAL_SETTINGS_NAME = "Global Settings"


class GlobalSettingsController(AdminPermissionsControllerMixin):
    """
    Admin controller for managing global sitewide settings.

    These settings apply across the entire Palace Manager instance and serve
    as defaults for all libraries. Library-level settings take precedence over
    these global defaults.
    """

    def __init__(self, db: Session) -> None:
        self._db = db

    def _get_or_create_integration(self) -> IntegrationConfiguration:
        """Return the single IntegrationConfiguration row for global settings, creating it if absent."""
        integration = get_one(
            self._db,
            IntegrationConfiguration,
            goal=Goals.SITEWIDE_SETTINGS,
            protocol=GLOBAL_SETTINGS_PROTOCOL,
        )
        if integration is None:
            integration, _ = create(
                self._db,
                IntegrationConfiguration,
                goal=Goals.SITEWIDE_SETTINGS,
                protocol=GLOBAL_SETTINGS_PROTOCOL,
                name=_GLOBAL_SETTINGS_NAME,
            )
        assert integration is not None
        return integration

    def process_global_settings(self) -> Response | ProblemDetail:
        try:
            self.require_system_admin()
            if flask.request.method == "GET":
                return self._process_get()
            else:
                return self._process_post()
        except (ProblemDetailException, AdminNotAuthorized) as e:
            self._db.rollback()
            return e.problem_detail

    def _process_get(self) -> Response:
        integration = get_one(
            self._db,
            IntegrationConfiguration,
            goal=Goals.SITEWIDE_SETTINGS,
            protocol=GLOBAL_SETTINGS_PROTOCOL,
        )
        settings = (
            integration_settings_load(GlobalSettings, integration)
            if integration is not None
            else GlobalSettings()
        )
        return Response(
            json_serializer(
                {
                    "settings": settings.model_dump(),
                    "schema": GlobalSettings.configuration_form(self._db),
                }
            ),
            status=200,
            mimetype="application/json",
        )

    def _process_post(self) -> Response:
        form_data = flask.request.form
        validated = ProcessFormData.get_settings(GlobalSettings, form_data)
        integration = self._get_or_create_integration()
        integration_settings_update(GlobalSettings, integration, validated)
        return Response("", 200)

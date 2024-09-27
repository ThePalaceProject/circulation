from __future__ import annotations

import json
from typing import Any

import flask
from flask import Response, url_for
from flask_babel import lazy_gettext as _
from sqlalchemy import select
from sqlalchemy.orm import Session

from palace.manager.api.admin.controller.base import AdminPermissionsControllerMixin
from palace.manager.api.admin.problem_details import MISSING_SERVICE, NO_SUCH_LIBRARY
from palace.manager.api.discovery.opds_registration import OpdsRegistrationService
from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.integration.goals import Goals
from palace.manager.service.integration_registry.discovery import DiscoveryRegistry
from palace.manager.sqlalchemy.model.discovery_service_registration import (
    DiscoveryServiceRegistration,
    RegistrationStage,
)
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException


class DiscoveryServiceLibraryRegistrationsController(AdminPermissionsControllerMixin):
    """List the libraries that have been registered with a specific
    OpdsRegistrationService, and allow the admin to register a library with
    a OpdsRegistrationService.
    """

    def __init__(self, db: Session, registry: DiscoveryRegistry):
        self._db: Session = db
        self.goal = Goals.DISCOVERY_GOAL
        self.registry = registry

    def process_discovery_service_library_registrations(
        self,
    ) -> Response | dict[str, Any] | ProblemDetail:
        self.require_system_admin()
        try:
            if flask.request.method == "GET":
                return self.process_get()
            else:
                return self.process_post()
        except ProblemDetailException as e:
            self._db.rollback()
            return e.problem_detail

    def process_get(self) -> dict[str, Any]:
        """Make a list of all discovery services, each with the
        list of libraries registered with that service and the
        status of the registration."""

        services = []
        integration_query = select(IntegrationConfiguration).where(
            IntegrationConfiguration.goal == self.goal,
            IntegrationConfiguration.protocol
            == self.registry.get_protocol(OpdsRegistrationService),
        )
        integrations = self._db.scalars(integration_query).all()
        for integration in integrations:
            registry = OpdsRegistrationService.for_integration(self._db, integration)
            try:
                access_problem = None
                (
                    terms_of_service_link,
                    terms_of_service_html,
                ) = registry.fetch_registration_document()
            except ProblemDetailException as e:
                # Unlike most cases like this, a ProblemError doesn't
                # mean the whole request is ruined -- just that one of
                # the discovery services isn't working. Turn the
                # ProblemDetail into a JSON object and return it for
                # handling on the client side.
                access_problem = json.loads(e.problem_detail.response[0])
                terms_of_service_link = terms_of_service_html = None

            libraries = [self.get_library_info(r) for r in registry.registrations]

            services.append(
                dict(
                    id=registry.integration.id,
                    access_problem=access_problem,
                    terms_of_service_link=terms_of_service_link,
                    terms_of_service_html=terms_of_service_html,
                    libraries=libraries,
                )
            )

        return dict(library_registrations=services)

    def get_library_info(
        self, registration: DiscoveryServiceRegistration
    ) -> dict[str, str]:
        """Find the relevant information about the library which the user
        is trying to register"""

        library_info = {"short_name": str(registration.library.short_name)}
        status = registration.status
        stage = registration.stage
        if stage:
            library_info["stage"] = stage.value
        if status:
            library_info["status"] = status.value

        return library_info

    def look_up_registry(self, integration_id: int) -> OpdsRegistrationService:
        """Find the OpdsRegistrationService that the user is trying to register the library with,
        and check that it actually exists."""

        registry = OpdsRegistrationService.for_integration(self._db, integration_id)
        if not registry:
            raise ProblemDetailException(problem_detail=MISSING_SERVICE)
        return registry

    def look_up_library(self, library_short_name: str) -> Library:
        """Find the library the user is trying to register, and check that it actually exists."""

        library = get_one(self._db, Library, short_name=library_short_name)
        if not library:
            raise ProblemDetailException(problem_detail=NO_SUCH_LIBRARY)
        return library

    def process_post(self) -> Response:
        """Attempt to register a library with a OpdsRegistrationService."""

        integration_id = flask.request.form.get("integration_id", type=int)
        library_short_name = flask.request.form.get("library_short_name")
        stage_string = flask.request.form.get("registration_stage")

        if integration_id is None:
            raise ProblemDetailException(
                problem_detail=INVALID_INPUT.detailed(
                    "Missing required parameter 'integration_id'"
                )
            )
        registry = self.look_up_registry(integration_id)

        if library_short_name is None:
            raise ProblemDetailException(
                problem_detail=INVALID_INPUT.detailed(
                    "Missing required parameter 'library_short_name'"
                )
            )
        library = self.look_up_library(library_short_name)

        if stage_string is None:
            raise ProblemDetailException(
                problem_detail=INVALID_INPUT.detailed(
                    "Missing required parameter 'registration_stage'"
                )
            )
        try:
            stage = RegistrationStage(stage_string)
        except ValueError:
            raise ProblemDetailException(
                problem_detail=INVALID_INPUT.detailed(
                    f"'{stage_string}' is not a valid registration stage"
                )
            )

        registry.register_library(library, stage, url_for)

        return Response(str(_("Success")), 200)

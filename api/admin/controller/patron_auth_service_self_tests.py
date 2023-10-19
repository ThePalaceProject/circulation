from __future__ import annotations

from typing import Any, Dict, Optional, Type

import flask
from flask import Response
from sqlalchemy.orm import Session

from api.admin.problem_details import *
from api.authentication.base import AuthenticationProvider
from api.integration.registry.patron_auth import PatronAuthRegistry
from core.integration.goals import Goals
from core.integration.registry import IntegrationRegistry
from core.model import get_one, json_serializer
from core.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from core.util.problem_detail import ProblemDetail, ProblemError


class PatronAuthServiceSelfTestsController:
    def __init__(
        self,
        db: Session,
        registry: Optional[IntegrationRegistry[AuthenticationProvider]] = None,
    ):
        self.db = db
        self.registry = registry if registry else PatronAuthRegistry()

    def process_patron_auth_service_self_tests(
        self, identifier: Optional[int]
    ) -> Response | ProblemDetail:
        if not identifier:
            return MISSING_IDENTIFIER
        try:
            if flask.request.method == "GET":
                return self.self_tests_process_get(identifier)
            else:
                return self.self_tests_process_post(identifier)
        except ProblemError as e:
            return e.problem_detail

    def self_tests_process_get(self, identifier: int) -> Response:
        integration = self.look_up_by_id(identifier)
        info = self.get_info(integration)
        protocol_class = self.get_protocol_class(integration)

        # Find the first library associated with this service.
        library_configuration = self.get_library_configuration(integration)

        if library_configuration is not None:
            self_test_results = protocol_class.load_self_test_results(integration)
        else:
            self_test_results = dict(
                exception=(
                    "You must associate this service with at least one library "
                    "before you can run self tests for it."
                ),
                disabled=True,
            )

        info["self_test_results"] = (
            self_test_results if self_test_results else "No results yet"
        )
        return Response(
            json_serializer({"self_test_results": info}),
            status=200,
            mimetype="application/json",
        )

    def self_tests_process_post(self, identifier: int) -> Response:
        integration = self.look_up_by_id(identifier)
        self.run_tests(integration)
        return Response("Successfully ran new self tests", 200)

    @staticmethod
    def get_library_configuration(
        integration: IntegrationConfiguration,
    ) -> Optional[IntegrationLibraryConfiguration]:
        if not integration.library_configurations:
            return None
        return integration.library_configurations[0]

    def get_protocol_class(
        self, integration: IntegrationConfiguration
    ) -> Type[AuthenticationProvider]:
        if not integration.protocol or integration.protocol not in self.registry:
            raise ProblemError(problem_detail=UNKNOWN_PROTOCOL)
        return self.registry[integration.protocol]

    def look_up_by_id(self, identifier: int) -> IntegrationConfiguration:
        service = get_one(
            self.db,
            IntegrationConfiguration,
            id=identifier,
            goal=Goals.PATRON_AUTH_GOAL,
        )
        if not service:
            raise (ProblemError(problem_detail=MISSING_SERVICE))
        return service

    @staticmethod
    def get_info(patron_auth_service: IntegrationConfiguration):
        info = dict(
            id=patron_auth_service.id,
            name=patron_auth_service.name,
            protocol=patron_auth_service.protocol,
            goal=patron_auth_service.goal,
            settings=patron_auth_service.settings_dict,
        )
        return info

    def run_tests(self, integration: IntegrationConfiguration) -> Dict[str, Any]:
        # If the auth service doesn't have at least one library associated with it,
        # we can't run self tests.
        library_configuration = self.get_library_configuration(integration)
        if library_configuration is None:
            raise ProblemError(
                problem_detail=FAILED_TO_RUN_SELF_TESTS.detailed(
                    f"Failed to run self tests for {integration.name}, because it is not associated with any libraries."
                )
            )

        if not isinstance(integration.settings_dict, dict) or not isinstance(
            library_configuration.settings_dict, dict
        ):
            raise ProblemError(
                problem_detail=FAILED_TO_RUN_SELF_TESTS.detailed(
                    f"Failed to run self tests for {integration.name}, because its settings are not valid."
                )
            )

        protocol_class = self.get_protocol_class(integration)
        settings = protocol_class.settings_load(integration)
        library_settings = protocol_class.library_settings_load(library_configuration)

        value, _ = protocol_class.run_self_tests(
            self.db,
            None,
            library_configuration.library_id,
            integration.id,
            settings,
            library_settings,
        )
        return value

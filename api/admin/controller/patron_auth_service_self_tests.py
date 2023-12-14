from __future__ import annotations

from typing import Any

from flask import Response
from sqlalchemy.orm import Session

from api.admin.controller.self_tests import IntegrationSelfTestsController
from api.admin.problem_details import FAILED_TO_RUN_SELF_TESTS
from api.authentication.base import AuthenticationProviderType
from api.integration.registry.patron_auth import PatronAuthRegistry
from core.integration.registry import IntegrationRegistry
from core.model.integration import IntegrationConfiguration
from core.util.problem_detail import ProblemDetail, ProblemError


class PatronAuthServiceSelfTestsController(
    IntegrationSelfTestsController[AuthenticationProviderType]
):
    def __init__(
        self,
        db: Session,
        registry: IntegrationRegistry[AuthenticationProviderType] | None = None,
    ):
        registry = registry or PatronAuthRegistry()
        super().__init__(db, registry)

    def process_patron_auth_service_self_tests(
        self, identifier: int | None
    ) -> Response | ProblemDetail:
        return self.process_self_tests(identifier)

    def get_prior_test_results(
        self,
        protocol_class: type[AuthenticationProviderType],
        integration: IntegrationConfiguration,
    ) -> dict[str, Any]:
        # Find the first library associated with this service.
        library_configuration = self.get_library_configuration(integration)

        if library_configuration is None:
            return dict(
                exception=(
                    "You must associate this service with at least one library "
                    "before you can run self tests for it."
                ),
                disabled=True,
            )

        return super().get_prior_test_results(protocol_class, integration)

    def run_self_tests(self, integration: IntegrationConfiguration) -> dict[str, Any]:
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

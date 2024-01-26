from __future__ import annotations

from typing import Any

import flask
from flask import Response

from api.admin.controller.base import AdminPermissionsControllerMixin
from api.admin.controller.integration_settings import (
    IntegrationSettingsSelfTestsController,
    UpdatedLibrarySettingsTuple,
)
from api.admin.form_data import ProcessFormData
from api.admin.problem_details import *
from api.authentication.base import AuthenticationProviderType
from api.authentication.basic import BasicAuthenticationProvider
from api.integration.registry.patron_auth import PatronAuthRegistry
from core.integration.goals import Goals
from core.integration.registry import IntegrationRegistry
from core.integration.settings import BaseSettings
from core.model import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
    json_serializer,
    site_configuration_has_changed,
)
from core.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from core.util.problem_detail import ProblemDetail, ProblemError


class PatronAuthServicesController(
    IntegrationSettingsSelfTestsController[AuthenticationProviderType],
    AdminPermissionsControllerMixin,
):
    def default_registry(self) -> IntegrationRegistry[AuthenticationProviderType]:
        return PatronAuthRegistry()

    @property
    def basic_auth_protocols(self) -> set[str]:
        return {
            name
            for name, api in self.registry
            if issubclass(api, BasicAuthenticationProvider)
        }

    def process_patron_auth_services(self) -> Response | ProblemDetail:
        self.require_system_admin()

        if flask.request.method == "GET":
            return self.process_get()
        else:
            return self.process_post()

    def process_get(self) -> Response:
        return Response(
            json_serializer(
                {
                    "patron_auth_services": self.configured_services,
                    "protocols": list(self.protocols.values()),
                }
            ),
            status=200,
            mimetype="application/json",
        )

    def process_post(self) -> Response | ProblemDetail:
        try:
            form_data = flask.request.form
            libraries_data = self.get_libraries_data(form_data)
            auth_service, protocol, response_code = self.get_service(form_data)

            # Update settings
            impl_cls = self.registry[protocol]
            settings_class = impl_cls.settings_class()
            validated_settings = ProcessFormData.get_settings(settings_class, form_data)
            auth_service.settings_dict = validated_settings.dict()

            # Update library settings
            if libraries_data:
                self.process_libraries(
                    auth_service, libraries_data, impl_cls.library_settings_class()
                )

            # Trigger a site configuration change
            site_configuration_has_changed(self._db)

        except ProblemError as e:
            self._db.rollback()
            return e.problem_detail

        return Response(str(auth_service.id), response_code)

    def library_integration_validation(
        self, integration: IntegrationLibraryConfiguration
    ) -> None:
        """Check that the library didn't end up with multiple basic auth services."""

        library = integration.library
        basic_auth_integrations = (
            self._db.query(IntegrationConfiguration)
            .join(IntegrationLibraryConfiguration)
            .filter(
                IntegrationLibraryConfiguration.library_id == library.id,
                IntegrationConfiguration.goal == Goals.PATRON_AUTH_GOAL,
                IntegrationConfiguration.protocol.in_(self.basic_auth_protocols),
            )
            .count()
        )
        if basic_auth_integrations > 1:
            raise ProblemError(
                MULTIPLE_BASIC_AUTH_SERVICES.detailed(
                    "You tried to add a patron authentication service that uses basic auth "
                    f"to {library.short_name}, but it already has one."
                )
            )

    def process_updated_libraries(
        self,
        libraries: list[UpdatedLibrarySettingsTuple],
        settings_class: type[BaseSettings],
    ) -> None:
        super().process_updated_libraries(libraries, settings_class)
        for integration, _ in libraries:
            self.library_integration_validation(integration)

    def process_delete(self, service_id: int) -> Response | ProblemDetail:
        self.require_system_admin()
        try:
            return self.delete_service(service_id)
        except ProblemError as e:
            self._db.rollback()
            return e.problem_detail

    def process_patron_auth_service_self_tests(
        self, identifier: int | None
    ) -> Response | ProblemDetail:
        return self.process_self_tests(identifier)

    def get_prior_test_results(
        self,
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

        return super().get_prior_test_results(integration)

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

        protocol_class = self.get_protocol_class(integration.protocol)
        settings = protocol_class.settings_load(integration)
        library_settings = protocol_class.library_settings_load(library_configuration)

        value, _ = protocol_class.run_self_tests(
            self._db,
            None,
            library_configuration.library_id,
            integration.id,
            settings,
            library_settings,
        )
        return value

    @staticmethod
    def get_library_configuration(
        integration: IntegrationConfiguration,
    ) -> IntegrationLibraryConfiguration | None:
        if not integration.library_configurations:
            return None
        return integration.library_configurations[0]

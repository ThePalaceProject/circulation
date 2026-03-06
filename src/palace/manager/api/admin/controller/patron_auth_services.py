from __future__ import annotations

from typing import Any

import flask
from flask import Response

from palace.manager.api.admin.controller.base import AdminPermissionsControllerMixin
from palace.manager.api.admin.controller.integration_settings import (
    IntegrationSettingsSelfTestsController,
    UpdatedLibrarySettingsTuple,
)
from palace.manager.api.admin.form_data import ProcessFormData
from palace.manager.api.admin.problem_details import (
    FAILED_TO_RUN_SELF_TESTS,
    INVALID_CONFIGURATION_OPTION,
    MULTIPLE_BASIC_AUTH_SERVICES,
)
from palace.manager.api.authentication.base import AuthenticationProviderType
from palace.manager.api.authentication.basic import BasicAuthenticationProvider
from palace.manager.api.authentication.patron_blocking_rules.rule_engine import (
    RuleValidationError,
    make_evaluator,
    validate_rule_expression,
)
from palace.manager.integration.goals import Goals
from palace.manager.integration.patron_auth.sip2.provider import (
    SIP2AuthenticationProvider,
)
from palace.manager.integration.settings import BaseSettings
from palace.manager.sqlalchemy.listeners import site_configuration_has_changed
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from palace.manager.util.json import json_serializer
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException


class PatronAuthServicesController(
    IntegrationSettingsSelfTestsController[AuthenticationProviderType],
    AdminPermissionsControllerMixin,
):
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
            auth_service.settings_dict = validated_settings.model_dump()

            # Update library settings
            if libraries_data:
                self.process_libraries(
                    auth_service, libraries_data, impl_cls.library_settings_class()
                )

            # Trigger a site configuration change
            site_configuration_has_changed(self._db)

        except ProblemDetailException as e:
            self._db.rollback()
            return e.problem_detail

        return Response(str(auth_service.id), response_code)

    def library_integration_validation(
        self, integration: IntegrationLibraryConfiguration
    ) -> None:
        """Validate a library integration after its settings have been saved.

        Performs two checks in order:

        1. Ensures the library does not end up with more than one basic-auth
           patron authentication service.
        2. For SIP2 integrations that include patron blocking rules, makes a
           live SIP2 call using the configured test identifier and re-validates
           every rule against the real values returned.  This catches rules that
           pass static syntax checks but would fail (or produce wrong results)
           against actual patron data.
        """
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
            raise ProblemDetailException(
                MULTIPLE_BASIC_AUTH_SERVICES.detailed(
                    "You tried to add a patron authentication service that uses basic auth "
                    f"to {library.short_name}, but it already has one."
                )
            )

        # Live SIP2 rule validation — only runs for SIP2 integrations with rules.
        protocol_class = self.get_protocol_class(integration.parent.protocol)
        if not issubclass(protocol_class, SIP2AuthenticationProvider):
            return
        library_settings = protocol_class.library_settings_load(integration)
        if not library_settings.patron_blocking_rules:
            return

        settings = protocol_class.settings_load(integration.parent)
        # fetch_live_rule_validation_values raises ProblemDetailException on
        # any SIP2 failure (missing test_identifier, network error, etc.).
        live_values = protocol_class.fetch_live_rule_validation_values(settings)

        evaluator = make_evaluator()
        for i, rule in enumerate(library_settings.patron_blocking_rules):
            try:
                validate_rule_expression(rule.rule, live_values, evaluator)
            except RuleValidationError as exc:
                raise ProblemDetailException(
                    INVALID_CONFIGURATION_OPTION.detailed(
                        f"Rule at index {i} ('{rule.name}'): {exc.message}"
                    )
                ) from exc

    def process_updated_libraries(
        self,
        libraries: list[UpdatedLibrarySettingsTuple],
        settings_class: type[BaseSettings],
    ) -> None:
        super().process_updated_libraries(libraries, settings_class)
        for integration, _ in libraries:
            self.library_integration_validation(integration)

    def process_validate_patron_blocking_rule(self) -> Response | ProblemDetail:
        """Validate a single patron blocking rule expression against live ILS data.

        Loads the saved service by ID, makes a live patron_information() call
        using the configured test_identifier/test_password via
        fetch_live_rule_validation_values, then evaluates the rule expression
        against the real values returned.  Only parse/eval success or failure
        is reported — the boolean result (blocked vs. not blocked) is discarded.
        """
        self.require_system_admin()
        try:
            form_data = flask.request.form
            service_id_str = form_data.get("service_id", "")
            rule_expr = form_data.get("rule", "")

            if not service_id_str:
                return INVALID_CONFIGURATION_OPTION.detailed("service_id is required.")

            try:
                service_id = int(service_id_str)
            except ValueError:
                return INVALID_CONFIGURATION_OPTION.detailed(
                    "service_id must be an integer."
                )

            integration = self._db.get(IntegrationConfiguration, service_id)
            if integration is None:
                return INVALID_CONFIGURATION_OPTION.detailed(
                    "Patron auth service not found. Save the service before validating rules."
                )

            protocol_class = self.get_protocol_class(integration.protocol)
            if not issubclass(protocol_class, SIP2AuthenticationProvider):
                return INVALID_CONFIGURATION_OPTION.detailed(
                    "Rule validation is only supported for SIP2 authentication services."
                )

            settings = protocol_class.settings_load(integration)
            # fetch_live_rule_validation_values raises ProblemDetailException on
            # missing test_identifier, network error, or SIP2 error response.
            live_values = protocol_class.fetch_live_rule_validation_values(settings)

            evaluator = make_evaluator()
            try:
                validate_rule_expression(rule_expr, live_values, evaluator)
            except RuleValidationError as exc:
                return INVALID_CONFIGURATION_OPTION.detailed(exc.message)

        except ProblemDetailException as e:
            return e.problem_detail

        return Response(status=200)

    def process_delete(self, service_id: int) -> Response | ProblemDetail:
        self.require_system_admin()
        try:
            return self.delete_service(service_id)
        except ProblemDetailException as e:
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
            raise ProblemDetailException(
                problem_detail=FAILED_TO_RUN_SELF_TESTS.detailed(
                    f"Failed to run self tests for {integration.name}, because it is not associated with any libraries."
                )
            )

        if not isinstance(integration.settings_dict, dict) or not isinstance(
            library_configuration.settings_dict, dict
        ):
            raise ProblemDetailException(
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
        """Find the first library (lowest id) associated with this service."""
        if not (library_configurations := integration.library_configurations):
            return None
        # We sort by library id to ensure that the result is predictable.
        return sorted(library_configurations, key=lambda config: config.library_id)[0]

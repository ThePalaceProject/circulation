from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

import flask
from flask import Response
from flask_babel import lazy_gettext as _
from sqlalchemy.orm import Session

from api.admin.controller.settings import SettingsController
from api.admin.problem_details import (
    FAILED_TO_RUN_SELF_TESTS,
    MISSING_IDENTIFIER,
    MISSING_SERVICE,
    UNKNOWN_PROTOCOL,
)
from core.integration.base import HasIntegrationConfiguration
from core.integration.registry import IntegrationRegistry
from core.integration.settings import BaseSettings
from core.model import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
    get_one,
    json_serializer,
)
from core.selftest import HasSelfTestsIntegrationConfiguration
from core.util.problem_detail import ProblemDetail, ProblemError


class SelfTestsController(SettingsController):
    def _manage_self_tests(self, identifier):
        """Generic request-processing method."""
        if not identifier:
            return MISSING_IDENTIFIER
        if flask.request.method == "GET":
            return self.self_tests_process_get(identifier)
        else:
            return self.self_tests_process_post(identifier)

    def find_protocol_class(self, integration):
        """Given an ExternalIntegration, find the class on which run_tests()
        or prior_test_results() should be called, and any extra
        arguments that should be passed into the call.
        """
        if not hasattr(self, "_find_protocol_class"):
            raise NotImplementedError()
        protocol_class = self._find_protocol_class(integration)
        if isinstance(protocol_class, tuple):
            protocol_class, extra_arguments = protocol_class
        else:
            extra_arguments = ()
        return protocol_class, extra_arguments

    def get_info(self, integration):
        protocol_class, ignore = self.find_protocol_class(integration)
        [protocol] = self._get_integration_protocols([protocol_class])
        return dict(
            id=integration.id,
            name=integration.name,
            protocol=protocol,
            settings=protocol.get("settings"),
            goal=integration.goal,
        )

    def run_tests(self, integration):
        protocol_class, extra_arguments = self.find_protocol_class(integration)
        value, results = protocol_class.run_self_tests(self._db, *extra_arguments)
        return value

    def self_tests_process_get(self, identifier):
        integration = self.look_up_by_id(identifier)
        if isinstance(integration, ProblemDetail):
            return integration
        info = self.get_info(integration)
        protocol_class, extra_arguments = self.find_protocol_class(integration)
        info["self_test_results"] = self._get_prior_test_results(
            integration, protocol_class, *extra_arguments
        )
        return dict(self_test_results=info)

    def self_tests_process_post(self, identifier):
        integration = self.look_up_by_id(identifier)
        if isinstance(integration, ProblemDetail):
            return integration
        value = self.run_tests(integration)
        if value and isinstance(value, ProblemDetail):
            return value
        elif value:
            return Response(_("Successfully ran new self tests"), 200)

        return FAILED_TO_RUN_SELF_TESTS.detailed(
            _("Failed to run self tests for this %(type)s.", type=self.type)
        )


T = TypeVar("T", bound=HasIntegrationConfiguration[BaseSettings])


class IntegrationSelfTestsController(Generic[T], ABC):
    def __init__(
        self,
        db: Session,
        registry: IntegrationRegistry[T],
    ):
        self.db = db
        self.registry = registry

    @abstractmethod
    def run_self_tests(
        self, integration: IntegrationConfiguration
    ) -> dict[str, Any] | None:
        ...

    def get_protocol_class(self, integration: IntegrationConfiguration) -> type[T]:
        if not integration.protocol or integration.protocol not in self.registry:
            raise ProblemError(problem_detail=UNKNOWN_PROTOCOL)
        return self.registry[integration.protocol]

    def look_up_by_id(self, identifier: int) -> IntegrationConfiguration:
        service = get_one(
            self.db,
            IntegrationConfiguration,
            id=identifier,
            goal=self.registry.goal,
        )
        if not service:
            raise (ProblemError(problem_detail=MISSING_SERVICE))
        return service

    @staticmethod
    def get_info(integration: IntegrationConfiguration) -> dict[str, Any]:
        info = dict(
            id=integration.id,
            name=integration.name,
            protocol=integration.protocol,
            goal=integration.goal,
            settings=integration.settings_dict,
        )
        return info

    @staticmethod
    def get_library_configuration(
        integration: IntegrationConfiguration,
    ) -> IntegrationLibraryConfiguration | None:
        if not integration.library_configurations:
            return None
        return integration.library_configurations[0]

    def get_prior_test_results(
        self, protocol_class: type[T], integration: IntegrationConfiguration
    ) -> dict[str, Any]:
        if issubclass(protocol_class, HasSelfTestsIntegrationConfiguration):
            self_test_results = protocol_class.load_self_test_results(integration)  # type: ignore[unreachable]
        else:
            self_test_results = dict(
                exception=("Self tests are not supported for this integration."),
                disabled=True,
            )

        return self_test_results

    def process_self_tests(self, identifier: int | None) -> Response | ProblemDetail:
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

        self_test_results = self.get_prior_test_results(protocol_class, integration)

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
        results = self.run_self_tests(integration)
        if results is not None:
            return Response("Successfully ran new self tests", 200)
        else:
            raise ProblemError(problem_detail=FAILED_TO_RUN_SELF_TESTS)

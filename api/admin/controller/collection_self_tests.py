from __future__ import annotations

from typing import Any

from flask import Response
from sqlalchemy.orm import Session

from api.admin.controller.self_tests import IntegrationSelfTestsController
from api.circulation import CirculationApiType
from api.integration.registry.license_providers import LicenseProvidersRegistry
from core.integration.registry import IntegrationRegistry
from core.model import IntegrationConfiguration
from core.selftest import HasSelfTestsIntegrationConfiguration
from core.util.problem_detail import ProblemDetail


class CollectionSelfTestsController(IntegrationSelfTestsController[CirculationApiType]):
    def __init__(
        self,
        db: Session,
        registry: IntegrationRegistry[CirculationApiType] | None = None,
    ):
        registry = registry or LicenseProvidersRegistry()
        super().__init__(db, registry)

    def process_collection_self_tests(
        self, identifier: int | None
    ) -> Response | ProblemDetail:
        return self.process_self_tests(identifier)

    def run_self_tests(
        self, integration: IntegrationConfiguration
    ) -> dict[str, Any] | None:
        protocol_class = self.get_protocol_class(integration)
        if issubclass(protocol_class, HasSelfTestsIntegrationConfiguration):
            test_result, _ = protocol_class.run_self_tests(
                self.db, protocol_class, self.db, integration.collection
            )
            return test_result

        return None

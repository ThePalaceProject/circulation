from typing import Any

import flask
from flask import Response

from api.admin.controller.base import AdminPermissionsControllerMixin
from api.admin.controller.integration_settings import (
    IntegrationSettingsSelfTestsController,
)
from api.admin.form_data import ProcessFormData
from api.admin.problem_details import DUPLICATE_INTEGRATION
from api.integration.registry.metadata import MetadataRegistry
from api.metadata.base import MetadataServiceType
from core.integration.base import HasLibraryIntegrationConfiguration
from core.integration.registry import IntegrationRegistry
from core.model import (
    IntegrationConfiguration,
    get_one,
    json_serializer,
    site_configuration_has_changed,
)
from core.selftest import HasSelfTests
from core.util.problem_detail import ProblemDetail, ProblemError


class MetadataServicesController(
    IntegrationSettingsSelfTestsController[MetadataServiceType],
    AdminPermissionsControllerMixin,
):
    def create_new_service(self, name: str, protocol: str) -> IntegrationConfiguration:
        impl_cls = self.registry[protocol]
        if not impl_cls.multiple_services_allowed():
            # If the service doesn't allow multiple instances, check if one already exists
            existing_service = get_one(
                self._db,
                IntegrationConfiguration,
                goal=self.registry.goal,
                protocol=protocol,
            )
            if existing_service is not None:
                raise ProblemError(DUPLICATE_INTEGRATION)
        return super().create_new_service(name, protocol)

    def default_registry(self) -> IntegrationRegistry[MetadataServiceType]:
        return MetadataRegistry()

    def process_metadata_services(self) -> Response | ProblemDetail:
        self.require_system_admin()
        if flask.request.method == "GET":
            return self.process_get()
        else:
            return self.process_post()

    def process_get(self) -> Response:
        return Response(
            json_serializer(
                {
                    "metadata_services": self.configured_services,
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
            metadata_service, protocol, response_code = self.get_service(form_data)

            # Update settings
            impl_cls = self.registry[protocol]
            settings_class = impl_cls.settings_class()
            validated_settings = ProcessFormData.get_settings(settings_class, form_data)
            metadata_service.settings_dict = validated_settings.dict()

            # Update library settings
            if libraries_data and issubclass(
                impl_cls, HasLibraryIntegrationConfiguration
            ):
                self.process_libraries(
                    metadata_service, libraries_data, impl_cls.library_settings_class()
                )

            # Trigger a site configuration change
            site_configuration_has_changed(self._db)

        except ProblemError as e:
            self._db.rollback()
            return e.problem_detail

        return Response(str(metadata_service.id), response_code)

    def process_delete(self, service_id: int) -> Response:
        self.require_system_admin()
        return self.delete_service(service_id)

    def run_self_tests(
        self, integration: IntegrationConfiguration
    ) -> dict[str, Any] | None:
        protocol_class = self.get_protocol_class(integration.protocol)
        if issubclass(protocol_class, HasSelfTests):
            settings = protocol_class.settings_load(integration)
            test_result, _ = protocol_class.run_self_tests(
                self._db, protocol_class, self._db, settings
            )
            return test_result

        return None

    def process_metadata_service_self_tests(
        self, identifier: int | None
    ) -> Response | ProblemDetail:
        return self.process_self_tests(identifier)

import flask
from flask import Response

from api.admin.controller.base import AdminPermissionsControllerMixin
from api.admin.controller.integration_settings import (
    IntegrationSettingsController,
    UpdatedLibrarySettingsTuple,
)
from api.admin.form_data import ProcessFormData
from api.admin.problem_details import MULTIPLE_SERVICES_FOR_LIBRARY
from api.integration.registry.catalog_services import CatalogServicesRegistry
from core.integration.goals import Goals
from core.integration.settings import BaseSettings
from core.marc import MARCExporter
from core.model import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
    json_serializer,
    site_configuration_has_changed,
)
from core.util.problem_detail import ProblemDetail, ProblemError


class CatalogServicesController(
    IntegrationSettingsController[MARCExporter],
    AdminPermissionsControllerMixin,
):
    def default_registry(self) -> CatalogServicesRegistry:
        return CatalogServicesRegistry()

    def process_catalog_services(self) -> Response | ProblemDetail:
        self.require_system_admin()

        if flask.request.method == "GET":
            return self.process_get()
        else:
            return self.process_post()

    def process_get(self) -> Response:
        return Response(
            json_serializer(
                {
                    "catalog_services": self.configured_services,
                    "protocols": list(self.protocols.values()),
                }
            ),
            status=200,
            mimetype="application/json",
        )

    def library_integration_validation(
        self, integration: IntegrationLibraryConfiguration
    ) -> None:
        """Check that the library didn't end up with multiple MARC integrations."""

        library = integration.library
        integrations = (
            self._db.query(IntegrationConfiguration)
            .join(IntegrationLibraryConfiguration)
            .filter(
                IntegrationLibraryConfiguration.library_id == library.id,
                IntegrationConfiguration.goal == Goals.CATALOG_GOAL,
            )
            .count()
        )
        if integrations > 1:
            raise ProblemError(
                MULTIPLE_SERVICES_FOR_LIBRARY.detailed(
                    f"You tried to add a MARC export service to {library.short_name}, but it already has one."
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

    def process_post(self) -> Response | ProblemDetail:
        try:
            form_data = flask.request.form
            libraries_data = self.get_libraries_data(form_data)
            catalog_service, protocol, response_code = self.get_service(form_data)

            # Update settings
            impl_cls = self.registry[protocol]
            settings_class = impl_cls.settings_class()
            validated_settings = ProcessFormData.get_settings(settings_class, form_data)
            catalog_service.settings_dict = validated_settings.dict()

            # Update library settings
            if libraries_data:
                self.process_libraries(
                    catalog_service, libraries_data, impl_cls.library_settings_class()
                )

            # Trigger a site configuration change
            site_configuration_has_changed(self._db)

        except ProblemError as e:
            self._db.rollback()
            return e.problem_detail

        return Response(str(catalog_service.id), response_code)

    def process_delete(self, service_id: int) -> Response:
        self.require_system_admin()
        return self.delete_service(service_id)

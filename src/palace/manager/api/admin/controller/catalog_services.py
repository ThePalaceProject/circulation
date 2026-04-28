import flask
from flask import Response

from palace.manager.api.admin.controller.base import AdminPermissionsControllerMixin
from palace.manager.api.admin.controller.integration_settings import (
    IntegrationSettingsController,
    UpdatedLibrarySettingsTuple,
)
from palace.manager.api.admin.form_data import ProcessFormData
from palace.manager.api.admin.problem_details import (
    MISSING_SERVICE,
    MULTIPLE_SERVICES_FOR_LIBRARY,
)
from palace.manager.integration.catalog.marc.exporter import MarcExporter
from palace.manager.integration.goals import Goals
from palace.manager.integration.settings import BaseSettings
from palace.manager.sqlalchemy.listeners import site_configuration_has_changed
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from palace.manager.util.json import json_serializer
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException


class CatalogServicesController(
    IntegrationSettingsController[MarcExporter],
    AdminPermissionsControllerMixin,
):
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
            raise ProblemDetailException(
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
            catalog_service.settings_dict = validated_settings.model_dump()

            # Update library settings
            if libraries_data:
                self.process_libraries(
                    catalog_service, libraries_data, impl_cls.library_settings_class()
                )

            # Trigger a site configuration change
            site_configuration_has_changed(self._db)

        except ProblemDetailException as e:
            self._db.rollback()
            return e.problem_detail

        return Response(str(catalog_service.id), response_code)

    def process_delete(self, service_id: int | str) -> Response | ProblemDetail:
        self.require_system_admin()
        try:
            sid = int(service_id) if isinstance(service_id, str) else service_id
        except ValueError:
            return MISSING_SERVICE
        return self.delete_service(sid)

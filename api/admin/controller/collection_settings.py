from typing import Any

import flask
from flask import Response

from api.admin.controller.base import AdminPermissionsControllerMixin
from api.admin.controller.integration_settings import IntegrationSettingsController
from api.admin.form_data import ProcessFormData
from api.admin.problem_details import (
    CANNOT_DELETE_COLLECTION_WITH_CHILDREN,
    MISSING_COLLECTION,
    MISSING_PARENT,
    MISSING_SERVICE,
    PROTOCOL_DOES_NOT_SUPPORT_PARENTS,
)
from api.circulation import CirculationApiType
from api.integration.registry.license_providers import LicenseProvidersRegistry
from core.integration.base import HasChildIntegrationConfiguration
from core.integration.registry import IntegrationRegistry
from core.model import (
    Collection,
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
    create,
    get_one,
    json_serializer,
    site_configuration_has_changed,
)
from core.util.problem_detail import ProblemDetail, ProblemError


class CollectionSettingsController(
    IntegrationSettingsController[CirculationApiType], AdminPermissionsControllerMixin
):
    def default_registry(self) -> IntegrationRegistry[CirculationApiType]:
        return LicenseProvidersRegistry()

    def configured_service_info(
        self, service: IntegrationConfiguration
    ) -> dict[str, Any] | None:
        service_info = super().configured_service_info(service)
        user = getattr(flask.request, "admin", None)
        if service_info:
            # Add 'marked_for_deletion' to the service info
            service_info["marked_for_deletion"] = service.collection.marked_for_deletion
            service_info["parent_id"] = (
                service.collection.parent.integration_configuration_id
                if service.collection.parent
                else None
            )
            service_info["settings"]["export_marc_records"] = str(
                service.collection.export_marc_records
            ).lower()
            if user and user.can_see_collection(service.collection):
                return service_info
        return None

    def configured_service_library_info(
        self, library_configuration: IntegrationLibraryConfiguration
    ) -> dict[str, Any] | None:
        library_info = super().configured_service_library_info(library_configuration)
        user = getattr(flask.request, "admin", None)
        if library_info:
            if user and user.is_librarian(library_configuration.library):
                return library_info
        return None

    def process_collections(self) -> Response | ProblemDetail:
        if flask.request.method == "GET":
            return self.process_get()
        else:
            return self.process_post()

    def process_get(self) -> Response:
        return Response(
            json_serializer(
                {
                    "collections": self.configured_services,
                    "protocols": list(self.protocols.values()),
                }
            ),
            status=200,
            mimetype="application/json",
        )

    def create_new_service(self, name: str, protocol: str) -> IntegrationConfiguration:
        service = super().create_new_service(name, protocol)
        # Make sure the new service is associated with a collection
        create(self._db, Collection, integration_configuration=service)
        return service

    def process_post(self) -> Response | ProblemDetail:
        self.require_system_admin()
        try:
            form_data = flask.request.form
            libraries_data = self.get_libraries_data(form_data)
            parent_id = form_data.get("parent_id", None, int)
            export_marc_records = (
                form_data.get("export_marc_records", None, str) == "true"
            )
            integration, protocol, response_code = self.get_service(form_data)

            impl_cls = self.registry[protocol]

            # Validate and set parent collection
            if parent_id is not None:
                if issubclass(impl_cls, HasChildIntegrationConfiguration):
                    settings_class = impl_cls.child_settings_class()
                    parent_integration = get_one(
                        self._db, IntegrationConfiguration, id=parent_id
                    )
                    if (
                        parent_integration is None
                        or parent_integration.collection is None
                    ):
                        raise ProblemError(MISSING_PARENT)
                    integration.collection.parent = parent_integration.collection
                else:
                    raise ProblemError(PROTOCOL_DOES_NOT_SUPPORT_PARENTS)
            else:
                settings_class = impl_cls.settings_class()

            # Set export_marc_records flag on the collection
            integration.collection.export_marc_records = export_marc_records

            # Update settings
            validated_settings = ProcessFormData.get_settings(settings_class, form_data)
            integration.settings_dict = validated_settings.dict()

            # Update library settings
            if libraries_data:
                self.process_libraries(
                    integration, libraries_data, impl_cls.library_settings_class()
                )

            # Trigger a site configuration change
            site_configuration_has_changed(self._db)

        except ProblemError as e:
            self._db.rollback()
            return e.problem_detail

        return Response(str(integration.id), response_code)

    def process_delete(self, service_id: int) -> Response | ProblemDetail:
        self.require_system_admin()

        integration = get_one(
            self._db,
            IntegrationConfiguration,
            id=service_id,
            goal=self.registry.goal,
        )
        if not integration:
            return MISSING_SERVICE

        collection = integration.collection
        if not collection:
            return MISSING_COLLECTION

        if len(collection.children) > 0:
            return CANNOT_DELETE_COLLECTION_WITH_CHILDREN

        # Flag the collection to be deleted by script in the background.
        collection.marked_for_deletion = True
        return Response("Deleted", 200)

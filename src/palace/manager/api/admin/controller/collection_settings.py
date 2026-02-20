from __future__ import annotations

from typing import Any

import flask
from flask import Response

from palace.manager.api.admin.controller.base import AdminPermissionsControllerMixin
from palace.manager.api.admin.controller.integration_settings import (
    IntegrationSettingsSelfTestsController,
)
from palace.manager.api.admin.form_data import ProcessFormData
from palace.manager.api.admin.problem_details import (
    CANNOT_DELETE_COLLECTION_WITH_CHILDREN,
    IMPORT_NOT_SUPPORTED,
    MISSING_COLLECTION,
    MISSING_PARENT,
    MISSING_SERVICE,
    PROTOCOL_DOES_NOT_SUPPORT_PARENTS,
    UNKNOWN_PROTOCOL,
)
from palace.manager.api.admin.util.flask import get_request_admin
from palace.manager.api.circulation.base import CirculationApiType
from palace.manager.celery.tasks.collection_delete import collection_delete
from palace.manager.celery.tasks.reaper import (
    reap_unassociated_holds,
    reap_unassociated_loans,
)
from palace.manager.core.selftest import HasSelfTests
from palace.manager.integration.base import HasChildIntegrationConfiguration
from palace.manager.sqlalchemy.listeners import site_configuration_has_changed
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from palace.manager.sqlalchemy.util import create, get_one
from palace.manager.util.json import json_serializer
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException


class CollectionSettingsController(
    IntegrationSettingsSelfTestsController[CirculationApiType],
    AdminPermissionsControllerMixin,
):
    def configured_service_info(
        self, service: IntegrationConfiguration
    ) -> dict[str, Any] | None:
        service_info = super().configured_service_info(service)
        user = get_request_admin(default=None)
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
        user = get_request_admin(default=None)
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
                        raise ProblemDetailException(MISSING_PARENT)
                    integration.collection.parent = parent_integration.collection
                else:
                    raise ProblemDetailException(PROTOCOL_DOES_NOT_SUPPORT_PARENTS)
            else:
                settings_class = impl_cls.settings_class()

            # Set export_marc_records flag on the collection
            integration.collection.export_marc_records = export_marc_records

            # Update settings
            validated_settings = ProcessFormData.get_settings(settings_class, form_data)
            integration.settings_dict = validated_settings.model_dump()

            # Update library settings
            if libraries_data:
                self.process_libraries(
                    integration, libraries_data, impl_cls.library_settings_class()
                )

            # Trigger a site configuration change
            site_configuration_has_changed(self._db)

            # If we have an importer task for this protocol, we start it
            # in the background, so that the collection is ready to go
            # as quickly as possible.
            try:
                impl_cls.import_task(integration.collection.id).apply_async(
                    # Delay the task to ensure the collection has been created by the time the task starts
                    countdown=10
                )
            except NotImplementedError:
                # If the protocol does not support import tasks, we just skip it.
                ...

        except ProblemDetailException as e:
            self._db.rollback()
            return e.problem_detail

        return Response(str(integration.id), response_code)

    def process_deleted_libraries(
        self, removed: list[IntegrationLibraryConfiguration]
    ) -> None:
        super().process_deleted_libraries(removed)

        if removed:
            # ensure that all loans and holds related
            # with the deleted library integrations are purged.
            reap_unassociated_loans.delay()
            reap_unassociated_holds.delay()

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
        collection_delete.delay(collection.id)
        return Response("Deleted", 200)

    def process_import(self, collection_id: int) -> Response | ProblemDetail:
        """Queue a collection import task on demand.

        :param collection_id: The integration configuration ID of the collection to import.
        :return: A 200 response on success, or a ProblemDetail on error.
        """
        self.require_system_admin()

        integration = get_one(
            self._db,
            IntegrationConfiguration,
            id=collection_id,
            goal=self.registry.goal,
        )
        if not integration:
            return MISSING_SERVICE

        collection = integration.collection
        if not collection:
            return MISSING_COLLECTION

        if collection.marked_for_deletion:
            return MISSING_COLLECTION

        protocol = integration.protocol
        if protocol not in self.registry:
            return UNKNOWN_PROTOCOL

        impl_cls = self.registry[protocol]
        force = flask.request.form.get("force", "false").lower() == "true"

        try:
            impl_cls.import_task(collection.id, force=force).apply_async()
        except NotImplementedError:
            return IMPORT_NOT_SUPPORTED

        return Response("Import task queued.", 200)

    def process_collection_self_tests(
        self, identifier: int | None
    ) -> Response | ProblemDetail:
        return self.process_self_tests(identifier)

    def run_self_tests(
        self, integration: IntegrationConfiguration
    ) -> dict[str, Any] | None:
        protocol_class = self.get_protocol_class(integration.protocol)
        if issubclass(protocol_class, HasSelfTests):
            test_result, _ = protocol_class.run_self_tests(
                self._db, protocol_class, self._db, integration.collection
            )
            return test_result

        return None

import flask
from flask import Response
from sqlalchemy import and_, select

from palace.manager.api.admin.controller.base import AdminPermissionsControllerMixin
from palace.manager.api.admin.controller.integration_settings import (
    IntegrationSettingsController,
)
from palace.manager.api.admin.form_data import ProcessFormData
from palace.manager.api.admin.problem_details import (
    INTEGRATION_URL_ALREADY_IN_USE,
    MISSING_SERVICE,
)
from palace.manager.integration.discovery.opds_registration import (
    OpdsRegistrationService,
)
from palace.manager.sqlalchemy.listeners import site_configuration_has_changed
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.util.json import json_serializer
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException


class DiscoveryServicesController(
    IntegrationSettingsController[OpdsRegistrationService],
    AdminPermissionsControllerMixin,
):
    def process_discovery_services(self) -> Response | ProblemDetail:
        self.require_system_admin()
        if flask.request.method == "GET":
            return self.process_get()
        else:
            return self.process_post()

    def process_get(self) -> Response:
        if len(self.configured_services) == 0:
            self.set_up_default_registry()

        return Response(
            json_serializer(
                {
                    "discovery_services": self.configured_services,
                    "protocols": list(self.protocols.values()),
                }
            ),
            status=200,
            mimetype="application/json",
        )

    def set_up_default_registry(self) -> None:
        """Set up the default library registry; no other registries exist yet."""
        protocol = self.registry.get_protocol(OpdsRegistrationService)
        assert protocol is not None
        default_registry = self.create_new_service(
            name=OpdsRegistrationService.DEFAULT_LIBRARY_REGISTRY_NAME,
            protocol=protocol,
        )
        settings = OpdsRegistrationService.settings_class()(
            url=OpdsRegistrationService.DEFAULT_LIBRARY_REGISTRY_URL
        )
        default_registry.settings_dict = settings.model_dump()

    def process_post(self) -> Response | ProblemDetail:
        try:
            form_data = flask.request.form
            service, protocol, response_code = self.get_service(form_data)

            impl_cls = self.registry[protocol]
            settings_class = impl_cls.settings_class()
            validated_settings = ProcessFormData.get_settings(settings_class, form_data)
            service.settings_dict = validated_settings.model_dump()

            # Make sure that the URL of the service is unique.
            self.check_url_unique(service, validated_settings.url)

            # Trigger a site configuration change
            site_configuration_has_changed(self._db)

        except ProblemDetailException as e:
            self._db.rollback()
            return e.problem_detail

        return Response(str(service.id), response_code)

    def process_delete(self, service_id: int | str) -> Response | ProblemDetail:
        self.require_system_admin()
        try:
            sid = int(service_id) if isinstance(service_id, str) else service_id
        except ValueError:
            return MISSING_SERVICE
        try:
            return self.delete_service(sid)
        except ProblemDetailException as e:
            self._db.rollback()
            return e.problem_detail

    def check_url_unique(self, service: IntegrationConfiguration, url: str) -> None:
        """Check that the URL of the service is unique.

        :raises ProblemDetail: If the URL is not unique.
        """

        existing_service = self._db.scalars(
            select(IntegrationConfiguration).where(
                and_(
                    IntegrationConfiguration.goal == service.goal,
                    IntegrationConfiguration.protocol == service.protocol,
                    IntegrationConfiguration.settings_dict.contains({"url": url}),
                    IntegrationConfiguration.id != service.id,
                )
            )
        ).one_or_none()
        if existing_service:
            raise ProblemDetailException(problem_detail=INTEGRATION_URL_ALREADY_IN_USE)

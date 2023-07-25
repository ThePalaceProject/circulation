from typing import Union

import flask
from flask import Response
from flask_babel import lazy_gettext as _
from sqlalchemy import and_, select

from api.admin.controller.base import AdminPermissionsControllerMixin
from api.admin.controller.integration_settings import IntegrationSettingsController
from api.admin.form_data import ProcessFormData
from api.admin.problem_details import (
    INCOMPLETE_CONFIGURATION,
    INTEGRATION_URL_ALREADY_IN_USE,
    MISSING_SERVICE,
    NO_PROTOCOL_FOR_NEW_SERVICE,
    UNKNOWN_PROTOCOL,
)
from api.discovery.opds_registration import OpdsRegistrationService
from api.integration.registry.discovery import DiscoveryRegistry
from core.model import (
    IntegrationConfiguration,
    get_one,
    json_serializer,
    site_configuration_has_changed,
)
from core.problem_details import INVALID_INPUT
from core.util.problem_detail import ProblemDetail, ProblemError


class DiscoveryServicesController(
    IntegrationSettingsController[OpdsRegistrationService],
    AdminPermissionsControllerMixin,
):
    def default_registry(self) -> DiscoveryRegistry:
        return DiscoveryRegistry()

    def process_discovery_services(self) -> Union[Response, ProblemDetail]:
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
        default_registry.settings_dict = settings.dict()

    def process_post(self) -> Union[Response, ProblemDetail]:
        try:
            form_data = flask.request.form
            protocol = form_data.get("protocol", None, str)
            id = form_data.get("id", None, int)
            name = form_data.get("name", None, str)

            if protocol is None and id is None:
                raise ProblemError(NO_PROTOCOL_FOR_NEW_SERVICE)

            if protocol is None or protocol not in self.registry:
                self.log.warning(f"Unknown service protocol: {protocol}")
                raise ProblemError(UNKNOWN_PROTOCOL)

            if id is not None:
                # Find an existing service to edit
                service = self.get_existing_service(id, name, protocol)
                response_code = 200
            else:
                # Create a new service
                if name is None:
                    raise ProblemError(INCOMPLETE_CONFIGURATION)
                service = self.create_new_service(name, protocol)
                response_code = 201

            impl_cls = self.registry[protocol]
            settings_class = impl_cls.settings_class()
            validated_settings = ProcessFormData.get_settings(settings_class, form_data)
            service.settings_dict = validated_settings.dict()

            # Make sure that the URL of the service is unique.
            self.check_url_unique(service, validated_settings.url)

            # Trigger a site configuration change
            site_configuration_has_changed(self._db)

        except ProblemError as e:
            self._db.rollback()
            return e.problem_detail

        return Response(str(service.id), response_code)

    def process_delete(self, service_id: int) -> Union[Response, ProblemDetail]:
        if flask.request.method != "DELETE":
            return INVALID_INPUT.detailed(_("Method not allowed for this endpoint"))
        self.require_system_admin()

        integration = get_one(
            self._db,
            IntegrationConfiguration,
            id=service_id,
            goal=self.registry.goal,
        )
        if not integration:
            return MISSING_SERVICE
        self._db.delete(integration)
        return Response(str(_("Deleted")), 200)

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
            raise ProblemError(problem_detail=INTEGRATION_URL_ALREADY_IN_USE)

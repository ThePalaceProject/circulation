import json
import logging
from itertools import chain
from typing import Any, Dict, List, Optional, Set, Type, Union

import flask
from flask import Response
from flask_babel import lazy_gettext as _

from api.admin.controller import AdminCirculationManagerController
from api.admin.form_data import ProcessFormData
from api.admin.problem_details import *
from api.authentication.base import AuthenticationProvider
from api.authentication.basic import BasicAuthenticationProvider
from api.controller import CirculationManager
from api.integration.registry.patron_auth import PatronAuthRegistry
from core.integration.goals import Goals
from core.integration.registry import IntegrationRegistry
from core.integration.settings import BaseSettings
from core.model import (
    Library,
    create,
    get_one,
    json_serializer,
    site_configuration_has_changed,
)
from core.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from core.util.cache import memoize
from core.util.problem_detail import ProblemDetail, ProblemError


class PatronAuthServicesController(AdminCirculationManagerController):
    def __init__(
        self,
        manager: CirculationManager,
        auth_registry: Optional[IntegrationRegistry[AuthenticationProvider]] = None,
    ):
        super().__init__(manager)

        self.registry = auth_registry if auth_registry else PatronAuthRegistry()
        self.type = _("patron authentication service")
        self.log = logging.getLogger(f"{self.__module__}.{self.__class__.__name__}")
        self._apis = None

    @memoize(ttls=1800)
    def _cached_protocols(self) -> Dict[str, Dict[str, Any]]:
        """Cached result for integration implementations"""
        protocols = {}
        for name, api in self.registry:
            protocols[name] = {
                "name": name,
                "label": api.label(),
                "description": api.description(),
                "settings": api.settings_class().configuration_form(self._db),
                "library_settings": api.library_settings_class().configuration_form(
                    self._db
                ),
            }
        return protocols

    @property
    def protocols(self) -> Dict[str, Dict[str, Any]]:
        """Use a property for implementations to allow expiring cached results"""
        return self._cached_protocols()

    @property
    def basic_auth_protocols(self) -> Set[str]:
        return {
            name
            for name, api in self.registry
            if issubclass(api, BasicAuthenticationProvider)
        }

    @property
    def configured_services(self) -> List[Dict[str, Any]]:
        configured_services = []
        for service in (
            self._db.query(IntegrationConfiguration)
            .filter(IntegrationConfiguration.goal == Goals.PATRON_AUTH_GOAL)
            .order_by(IntegrationConfiguration.name)
        ):
            if service.protocol not in self.registry:
                self.log.warning(
                    f"Unknown patron authentication service implementation: {service.protocol}"
                )
                continue

            libraries = []
            for library_settings in service.library_configurations:
                library_info = {"short_name": library_settings.library.short_name}
                library_info.update(library_settings.settings)
                libraries.append(library_info)

            service_info = {
                "id": service.id,
                "name": service.name,
                "protocol": service.protocol,
                "settings": service.settings,
                "libraries": libraries,
            }
            configured_services.append(service_info)
        return configured_services

    def process_patron_auth_services(self) -> Union[Response, ProblemDetail]:
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

    def get_existing_service(
        self, service_id: int, name: Optional[str], protocol: str
    ) -> IntegrationConfiguration:
        # Find an existing service to edit
        auth_service: Optional[IntegrationConfiguration] = get_one(
            self._db,
            IntegrationConfiguration,
            id=service_id,
            goal=Goals.PATRON_AUTH_GOAL,
        )
        if auth_service is None:
            raise ProblemError(MISSING_SERVICE)
        if auth_service.protocol != protocol:
            raise ProblemError(CANNOT_CHANGE_PROTOCOL)
        if name is not None and auth_service.name != name:
            service_with_name = get_one(self._db, IntegrationConfiguration, name=name)
            if service_with_name is not None:
                raise ProblemError(INTEGRATION_NAME_ALREADY_IN_USE)
            auth_service.name = name

        return auth_service

    def create_new_service(self, name: str, protocol: str) -> IntegrationConfiguration:
        # Create a new service
        service_with_name = get_one(self._db, IntegrationConfiguration, name=name)
        if service_with_name is not None:
            raise ProblemError(INTEGRATION_NAME_ALREADY_IN_USE)

        auth_service, _ = create(
            self._db,
            IntegrationConfiguration,
            protocol=protocol,
            goal=Goals.PATRON_AUTH_GOAL,
            name=name,
        )
        return auth_service  # type: ignore[no-any-return]

    def remove_library_settings(
        self, library_settings: IntegrationLibraryConfiguration
    ) -> None:
        self._db.delete(library_settings)

    def get_library(self, short_name: str) -> Library:
        library: Optional[Library] = get_one(self._db, Library, short_name=short_name)
        if library is None:
            raise ProblemError(
                NO_SUCH_LIBRARY.detailed(
                    f"You attempted to add the integration to {short_name}, but it does not exist.",
                )
            )
        return library

    def create_library_settings(
        self, auth_service: IntegrationConfiguration, short_name: str
    ) -> IntegrationLibraryConfiguration:
        library = self.get_library(short_name)
        library_settings, _ = create(
            self._db,
            IntegrationLibraryConfiguration,
            library=library,
            parent_id=auth_service.id,
        )
        return library_settings  # type: ignore[no-any-return]

    def process_libraries(
        self,
        auth_service: IntegrationConfiguration,
        libraries_data: str,
        settings_class: Type[BaseSettings],
    ) -> None:
        # Update libraries
        libraries = json.loads(libraries_data)
        existing_library_settings = {
            c.library.short_name: c for c in auth_service.library_configurations
        }
        submitted_library_settings = {l.get("short_name"): l for l in libraries}

        removed = [
            existing_library_settings[library]
            for library in existing_library_settings.keys()
            - submitted_library_settings.keys()
        ]
        updated = [
            (existing_library_settings[library], submitted_library_settings[library])
            for library in existing_library_settings.keys()
            & submitted_library_settings.keys()
            if library and self.get_library(library)
        ]
        new = [
            (
                self.create_library_settings(auth_service, library),
                submitted_library_settings[library],
            )
            for library in submitted_library_settings.keys()
            - existing_library_settings.keys()
        ]

        # Remove libraries that are no longer configured
        for library_settings in removed:
            self.remove_library_settings(library_settings)

        # Update new and existing libraries settings
        for integration, settings in chain(new, updated):
            validated_settings = settings_class(**settings)
            integration.settings = validated_settings.dict()
            # Make sure library doesn't have multiple auth basic auth services
            self.check_library_integrations(integration.library)

    def process_post(self) -> Union[Response, ProblemDetail]:
        try:
            form_data = flask.request.form
            protocol = form_data.get("protocol", None, str)
            id = form_data.get("id", None, int)
            name = form_data.get("name", None, str)
            libraries_data = form_data.get("libraries", None, str)

            if protocol is None and id is None:
                raise ProblemError(NO_PROTOCOL_FOR_NEW_SERVICE)

            if protocol is None or protocol not in self.registry:
                self.log.warning(
                    f"Unknown patron authentication service protocol: {protocol}"
                )
                raise ProblemError(UNKNOWN_PROTOCOL)

            if id is not None:
                # Find an existing service to edit
                auth_service = self.get_existing_service(id, name, protocol)
                response_code = 200
            else:
                # Create a new service
                if name is None:
                    raise ProblemError(MISSING_PATRON_AUTH_NAME)
                auth_service = self.create_new_service(name, protocol)
                response_code = 201

            # Update settings
            impl_cls = self.registry[protocol]
            settings_class = impl_cls.settings_class()
            validated_settings = ProcessFormData.get_settings(settings_class, form_data)
            auth_service.settings = validated_settings.dict()

            # Update library settings
            if libraries_data:
                self.process_libraries(
                    auth_service, libraries_data, impl_cls.library_settings_class()
                )

            # Trigger a site configuration change
            site_configuration_has_changed(self._db)

        except ProblemError as e:
            self._db.rollback()
            return e.problem_detail

        return Response(str(auth_service.id), response_code)

    def check_library_integrations(self, library: Library) -> None:
        """Check that the library didn't end up with multiple basic auth services."""
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
            raise ProblemError(
                MULTIPLE_BASIC_AUTH_SERVICES.detailed(
                    "You tried to add a patron authentication service that uses basic auth "
                    f"to {library.short_name}, but it already has one."
                )
            )

    def process_delete(self, service_id: int) -> Union[Response, ProblemDetail]:
        if flask.request.method != "DELETE":
            return INVALID_INPUT.detailed(_("Method not allowed for this endpoint"))  # type: ignore[no-any-return]
        self.require_system_admin()

        integration = get_one(
            self._db,
            IntegrationConfiguration,
            id=service_id,
            goal=Goals.PATRON_AUTH_GOAL,
        )
        if not integration:
            return MISSING_SERVICE
        self._db.delete(integration)
        return Response(str(_("Deleted")), 200)

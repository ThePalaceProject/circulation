from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any, Generic, NamedTuple, TypeVar

import flask
from flask import Response
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.problem_details import (
    CANNOT_CHANGE_PROTOCOL,
    INTEGRATION_NAME_ALREADY_IN_USE,
    MISSING_SERVICE,
    MISSING_SERVICE_NAME,
    NO_PROTOCOL_FOR_NEW_SERVICE,
    NO_SUCH_LIBRARY,
    UNKNOWN_PROTOCOL,
)
from api.circulation_manager import CirculationManager
from core.integration.base import (
    HasChildIntegrationConfiguration,
    HasIntegrationConfiguration,
    HasLibraryIntegrationConfiguration,
)
from core.integration.registry import IntegrationRegistry
from core.integration.settings import BaseSettings
from core.model import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
    Library,
    create,
    get_one,
)
from core.problem_details import INTERNAL_SERVER_ERROR, INVALID_INPUT
from core.util.cache import memoize
from core.util.log import LoggerMixin
from core.util.problem_detail import ProblemError

T = TypeVar("T", bound=HasIntegrationConfiguration[BaseSettings])


class UpdatedLibrarySettingsTuple(NamedTuple):
    integration: IntegrationLibraryConfiguration
    settings: dict[str, Any]


class ChangedLibrariesTuple(NamedTuple):
    new: list[UpdatedLibrarySettingsTuple]
    updated: list[UpdatedLibrarySettingsTuple]
    removed: list[IntegrationLibraryConfiguration]


class IntegrationSettingsController(ABC, Generic[T], LoggerMixin):
    def __init__(
        self,
        manager: CirculationManager,
        registry: IntegrationRegistry[T] | None = None,
    ):
        self._db = manager._db
        self.registry = registry or self.default_registry()

    @abstractmethod
    def default_registry(self) -> IntegrationRegistry[T]:
        """
        Return the IntegrationRegistry for the controller's goal.
        """
        ...

    @memoize(ttls=1800)
    def _cached_protocols(self) -> dict[str, dict[str, Any]]:
        """Cached result for integration implementations"""
        protocols = []
        for name, api in self.registry:
            protocol = {
                "name": name,
                "label": api.label(),
                "description": api.description(),
                "settings": api.settings_class().configuration_form(self._db),
            }
            if issubclass(api, HasLibraryIntegrationConfiguration):
                protocol[
                    "library_settings"
                ] = api.library_settings_class().configuration_form(self._db)
            if issubclass(api, HasChildIntegrationConfiguration):
                protocol[
                    "child_settings"
                ] = api.child_settings_class().configuration_form(self._db)
            protocol.update(api.protocol_details(self._db))
            protocols.append((name, protocol))
        protocols.sort(key=lambda x: x[0])
        return dict(protocols)

    @property
    def protocols(self) -> dict[str, dict[str, Any]]:
        """Use a property for implementations to allow expiring cached results"""
        return self._cached_protocols()

    def configured_service_info(
        self, service: IntegrationConfiguration
    ) -> dict[str, Any] | None:
        return {
            "id": service.id,
            "name": service.name,
            "protocol": service.protocol,
            "settings": service.settings_dict,
        }

    def configured_service_library_info(
        self, library_configuration: IntegrationLibraryConfiguration
    ) -> dict[str, Any] | None:
        library_info = {"short_name": library_configuration.library.short_name}
        library_info.update(library_configuration.settings_dict)
        return library_info

    @property
    def configured_services(self) -> list[dict[str, Any]]:
        """Return a list of all currently configured services for the controller's goal."""
        configured_services = []
        for service in (
            self._db.query(IntegrationConfiguration)
            .filter(IntegrationConfiguration.goal == self.registry.goal)
            .order_by(IntegrationConfiguration.name)
        ):
            if service.protocol not in self.registry:
                self.log.warning(
                    f"Unknown protocol: {service.protocol} for goal {self.registry.goal}"
                )
                continue

            service_info = self.configured_service_info(service)
            if service_info is None:
                continue

            api = self.registry[service.protocol]
            if issubclass(api, HasLibraryIntegrationConfiguration):
                libraries = []
                for library_settings in service.library_configurations:
                    library_info = self.configured_service_library_info(
                        library_settings
                    )
                    if library_info is not None:
                        libraries.append(library_info)
                service_info["libraries"] = libraries

            configured_services.append(service_info)
        return configured_services

    def get_existing_service(
        self, service_id: int, name: str | None, protocol: str
    ) -> IntegrationConfiguration:
        """
        Query for an existing service to edit.

        Raises ProblemError if the service doesn't exist, or if the protocol
        doesn't match. If the name is provided, the service will be renamed if
        necessary and a ProblemError will be raised if the name is already in
        use.
        """
        service: IntegrationConfiguration | None = get_one(
            self._db,
            IntegrationConfiguration,
            id=service_id,
            goal=self.registry.goal,
        )
        if service is None:
            raise ProblemError(MISSING_SERVICE)
        if service.protocol != protocol:
            raise ProblemError(CANNOT_CHANGE_PROTOCOL)
        if name is not None and service.name != name:
            service_with_name = get_one(self._db, IntegrationConfiguration, name=name)
            if service_with_name is not None:
                raise ProblemError(INTEGRATION_NAME_ALREADY_IN_USE)
            service.name = name

        return service

    def create_new_service(self, name: str, protocol: str) -> IntegrationConfiguration:
        """
        Create a new service.

        Returns the new IntegrationConfiguration on success and raises a ProblemError
        on any errors.
        """
        # Create a new service
        service_with_name = get_one(self._db, IntegrationConfiguration, name=name)
        if service_with_name is not None:
            raise ProblemError(INTEGRATION_NAME_ALREADY_IN_USE)

        new_service, _ = create(
            self._db,
            IntegrationConfiguration,
            protocol=protocol,
            goal=self.registry.goal,
            name=name,
        )
        if not new_service:
            raise ProblemError(
                INTERNAL_SERVER_ERROR.detailed(
                    f"Could not create the '{self.registry.goal.value}' integration."
                )
            )
        return new_service

    def get_libraries_data(self, form_data: ImmutableMultiDict[str, str]) -> str | None:
        libraries_data = form_data.get("libraries", None, str)
        return libraries_data

    def get_service(
        self, form_data: ImmutableMultiDict[str, str]
    ) -> tuple[IntegrationConfiguration, str, int]:
        protocol = form_data.get("protocol", None, str)
        _id = form_data.get("id", None, int)
        name = form_data.get("name", None, str)

        if protocol is None and _id is None:
            raise ProblemError(NO_PROTOCOL_FOR_NEW_SERVICE)

        if protocol is None or protocol not in self.registry:
            self.log.warning(f"Unknown service protocol: {protocol}")
            raise ProblemError(UNKNOWN_PROTOCOL)

        if _id is not None:
            # Find an existing service to edit
            service = self.get_existing_service(_id, name, protocol)
            response_code = 200
        else:
            # Create a new service
            if name is None:
                raise ProblemError(MISSING_SERVICE_NAME)
            service = self.create_new_service(name, protocol)
            response_code = 201

        return service, protocol, response_code

    def get_library(self, short_name: str) -> Library:
        """
        Get a library by its short name.
        """
        library: Library | None = get_one(self._db, Library, short_name=short_name)
        if library is None:
            raise ProblemError(
                NO_SUCH_LIBRARY.detailed(
                    f"You attempted to add the integration to {short_name}, but it does not exist.",
                )
            )
        return library

    def create_library_settings(
        self, service: IntegrationConfiguration, short_name: str
    ) -> IntegrationLibraryConfiguration:
        """
        Create a new IntegrationLibraryConfiguration for the given IntegrationConfiguration and library.
        """
        library = self.get_library(short_name)
        library_settings, _ = create(
            self._db,
            IntegrationLibraryConfiguration,
            library=library,
            parent_id=service.id,
        )
        if not library_settings:
            raise ProblemError(
                INTERNAL_SERVER_ERROR.detailed(
                    "Could not create the library configuration"
                )
            )
        return library_settings

    def get_changed_libraries(
        self, service: IntegrationConfiguration, libraries_data: str
    ) -> ChangedLibrariesTuple:
        """
        Return a tuple of lists of libraries that have had their library settings
        added, updated, or removed. No action is taken to add, update, or remove
        the settings, this function just parses the submitted data and returns
        the lists of libraries that need to be processed.

        :param service: The IntegrationConfiguration that the library settings should be
            associated with.
        :param libraries_data: A JSON string containing a list of dictionaries.
            Each dictionary has a 'short_name' key that identifies which
            library the settings are for, and then the rest of the dictionary is the
            settings for that library.

        :return: A named tuple with three lists of libraries:
            - new: A list of UpdatedLibrarySettingsTuple named tuples that contains the
                IntegrationLibraryConfiguration and settings for each library with newly
                added settings.
            - updated: A list of UpdatedLibrarySettingsTuple named tuples that contains the
                IntegrationLibraryConfiguration and settings for each library that had its
                settings updated.
            - removed: A list of IntegrationLibraryConfiguration objects for libraries that
                had their settings removed.
        """
        libraries = json.loads(libraries_data)
        existing_library_settings = {
            c.library.short_name: c for c in service.library_configurations
        }

        submitted_library_settings = {}
        for library in libraries:
            # Each library settings dictionary should have a 'short_name' key that identifies
            # which library the settings are for. This key is removed from the dictionary as
            # only the settings should be stored in the database.
            short_name = library.get("short_name")
            if short_name is None:
                self.log.error(
                    f"Library settings missing short_name. Settings: {library}."
                )
                raise ProblemError(
                    INVALID_INPUT.detailed(
                        "Invalid library settings, missing short_name."
                    )
                )
            del library["short_name"]
            submitted_library_settings[short_name] = library

        removed = [
            existing_library_settings[library]
            for library in existing_library_settings.keys()
            - submitted_library_settings.keys()
        ]
        updated = [
            UpdatedLibrarySettingsTuple(
                integration=existing_library_settings[library],
                settings=submitted_library_settings[library],
            )
            for library in existing_library_settings.keys()
            & submitted_library_settings.keys()
            if library and self.get_library(library)
        ]
        new = [
            UpdatedLibrarySettingsTuple(
                integration=self.create_library_settings(service, library),
                settings=submitted_library_settings[library],
            )
            for library in submitted_library_settings.keys()
            - existing_library_settings.keys()
        ]
        return ChangedLibrariesTuple(new=new, updated=updated, removed=removed)

    def process_deleted_libraries(
        self, removed: list[IntegrationLibraryConfiguration]
    ) -> None:
        """
        Delete any IntegrationLibraryConfigurations that were removed.
        """
        for library_integration in removed:
            self._db.delete(library_integration)

    def process_updated_libraries(
        self,
        libraries: list[UpdatedLibrarySettingsTuple],
        settings_class: type[BaseSettings],
    ) -> None:
        """
        Update the settings for any IntegrationLibraryConfigurations that were updated or added.
        """
        for integration, settings in libraries:
            validated_settings = settings_class(**settings)
            integration.settings_dict = validated_settings.dict()

    def process_libraries(
        self,
        service: IntegrationConfiguration,
        libraries_data: str,
        settings_class: type[BaseSettings],
    ) -> None:
        """
        Process the library settings for a service. This will create new
        IntegrationLibraryConfigurations for any libraries that don't have one,
        update the settings for any that do, and delete any that were removed.
        """
        new, updated, removed = self.get_changed_libraries(service, libraries_data)

        self.process_deleted_libraries(removed)
        self.process_updated_libraries(new, settings_class)
        self.process_updated_libraries(updated, settings_class)

    def delete_service(self, service_id: int) -> Response:
        """
        Delete a service.

        Returns a Response on success suitable to return to the frontend
        and raises a ProblemError on any errors.
        """
        if flask.request.method != "DELETE":
            raise ProblemError(
                problem_detail=INVALID_INPUT.detailed(
                    "Method not allowed for this endpoint"
                )
            )

        integration = get_one(
            self._db,
            IntegrationConfiguration,
            id=service_id,
            goal=self.registry.goal,
        )
        if not integration:
            raise ProblemError(problem_detail=MISSING_SERVICE)
        self._db.delete(integration)
        return Response("Deleted", 200)

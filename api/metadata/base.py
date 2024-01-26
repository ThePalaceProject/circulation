import functools
from abc import ABC, abstractmethod
from typing import Any, TypeVar

from sqlalchemy.orm import Session

from core.integration.base import (
    HasIntegrationConfiguration,
    HasLibraryIntegrationConfiguration,
)
from core.integration.settings import BaseSettings


class MetadataServiceSettings(BaseSettings):
    ...


SettingsType = TypeVar("SettingsType", bound=MetadataServiceSettings, covariant=True)


class MetadataService(
    HasIntegrationConfiguration[SettingsType],
    ABC,
):
    @classmethod
    def protocol_details(cls, db: Session) -> dict[str, Any]:
        details = super().protocol_details(db)
        details["sitewide"] = not issubclass(cls, HasLibraryIntegrationConfiguration)
        return details

    @classmethod
    @functools.cache
    def protocols(cls) -> list[str]:
        from api.integration.registry.metadata import MetadataRegistry

        registry = MetadataRegistry()
        protocols = registry.get_protocols(cls)

        if not protocols:
            raise RuntimeError(f"No protocols found for {cls.__name__}")

        return protocols

    @classmethod
    @abstractmethod
    def multiple_services_allowed(cls) -> bool:
        ...


MetadataServiceType = MetadataService[MetadataServiceSettings]

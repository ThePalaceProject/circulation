from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, Type, TypeVar

from sqlalchemy.orm import Session

from core.integration.settings import BaseSettings

SettingsType = TypeVar("SettingsType", bound=BaseSettings, covariant=True)
LibrarySettingsType = TypeVar("LibrarySettingsType", bound=BaseSettings, covariant=True)


class HasIntegrationConfiguration(Generic[SettingsType], ABC):
    @classmethod
    @abstractmethod
    def label(cls) -> str:
        """Get the label of this integration"""
        ...

    @classmethod
    @abstractmethod
    def description(cls) -> str:
        """Get the description of this integration"""
        ...

    @classmethod
    @abstractmethod
    def settings_class(cls) -> Type[SettingsType]:
        """Get the settings for this integration"""
        ...

    @classmethod
    def protocol_details(cls, db: Session) -> dict[str, Any]:
        """Add any additional details about this protocol to be
        returned to the admin interface.

        The default implementation returns an empty dict.
        """
        return {}

    @property
    @abstractmethod
    def settings(self) -> SettingsType:
        ...


class HasLibraryIntegrationConfiguration(
    Generic[SettingsType, LibrarySettingsType],
    HasIntegrationConfiguration[SettingsType],
    ABC,
):
    @classmethod
    @abstractmethod
    def library_settings_class(cls) -> Type[LibrarySettingsType]:
        """Get the library settings for this integration"""
        ...


class HasChildIntegrationConfiguration(HasIntegrationConfiguration[SettingsType], ABC):
    @classmethod
    @abstractmethod
    def child_settings_class(cls) -> Type[BaseSettings]:
        """Get the child settings class"""
        ...

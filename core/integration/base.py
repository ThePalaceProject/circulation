from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, Generic, Mapping, Protocol, Type, TypeVar

from sqlalchemy.orm import Session
from sqlalchemy.orm.attributes import Mapped, flag_modified

from core.integration.settings import BaseSettings

if TYPE_CHECKING:
    from core.model import IntegrationConfiguration, IntegrationLibraryConfiguration


class IntegrationConfigurationProtocol(Protocol):
    settings_dict: Mapped[Dict[str, Any]]


T = TypeVar("T", bound=BaseSettings)


def integration_settings_load(
    settings_cls: Type[T],
    integration: IntegrationConfigurationProtocol,
) -> T:
    """
    Load the settings object for an integration from the database.

    The settings are validated when loaded from the database, this is done rather
    than using construct() because there are some types that need to get type converted
    when round tripping from the database (such as enum) and construct() doesn't do that.

    :param settings_cls: The settings class that the settings should be loaded into.
    :param integration: The integration to load the settings from. This should be a
        SQLAlchemy model with a settings_dict JSONB column.

    :return: An instance of the settings class loaded with the settings from the database.
    """
    settings_dict = integration.settings_dict
    return settings_cls(**settings_dict)


def integration_settings_update(
    settings_cls: Type[BaseSettings],
    integration: IntegrationConfigurationProtocol,
    new_settings: BaseSettings | Mapping[str, Any],
    merge: bool = False,
) -> None:
    """
    Update the settings for an integration in the database.

    The settings are validated before being saved to the database, and SQLAlchemy is
    notified that the settings_dict column has been modified.

    :param settings_cls: The settings class to use to validate the settings.
    :param integration: The integration to update. This should be a SQLAlchemy model
        with a settings_dict JSONB column.
    :param new_settings: The new settings to update the integration with. This can either
        be a BaseSettings object, or a dictionary of settings.
    :param merge: If True, the new settings will be merged with the existing settings. With
        the new settings taking precedence. If False, the new settings will replace the existing
        settings.
    """
    settings_dict = integration.settings_dict if merge else {}
    new_settings_dict = (
        new_settings.dict() if isinstance(new_settings, BaseSettings) else new_settings
    )
    settings_dict.update(new_settings_dict)
    integration.settings_dict = settings_cls(**settings_dict).dict()
    flag_modified(integration, "settings_dict")


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
    def settings_load(cls, integration: IntegrationConfiguration) -> SettingsType:
        """
        Load the settings object for this integration from the database.

        See the documentation for `integration_settings_load` for more details.
        """
        return integration_settings_load(cls.settings_class(), integration)

    @classmethod
    def settings_update(
        cls,
        integration: IntegrationConfiguration,
        new_settings: BaseSettings | Mapping[str, Any],
        merge: bool = False,
    ) -> None:
        """
        Update the settings for this integration in the database.

        See the documentation for `integration_settings_update` for more details.
        """
        integration_settings_update(
            cls.settings_class(), integration, new_settings, merge
        )

    @classmethod
    def protocol_details(cls, db: Session) -> dict[str, Any]:
        """Add any additional details about this protocol to be
        returned to the admin interface.

        The default implementation returns an empty dict.
        """
        return {}


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

    @classmethod
    def library_settings_load(
        cls, integration: IntegrationLibraryConfiguration
    ) -> LibrarySettingsType:
        """
        Load the library settings object for this integration from the database.

        See the documentation for `integration_settings_load` for more details.
        """
        return integration_settings_load(cls.library_settings_class(), integration)

    @classmethod
    def library_settings_update(
        cls,
        integration: IntegrationLibraryConfiguration,
        new_settings: BaseSettings | Mapping[str, Any],
        merge: bool = False,
    ) -> None:
        """
        Update the settings for this library integration in the database.

        See the documentation for `integration_settings_update` for more details.
        """
        integration_settings_update(
            cls.library_settings_class(), integration, new_settings, merge
        )


class HasChildIntegrationConfiguration(HasIntegrationConfiguration[SettingsType], ABC):
    @classmethod
    @abstractmethod
    def child_settings_class(cls) -> Type[BaseSettings]:
        """Get the child settings class"""
        ...

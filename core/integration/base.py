from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Type

from core.integration.settings import BaseSettings


class HasIntegrationConfiguration(ABC):
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
    def settings_class(cls) -> Type[BaseSettings]:
        """Get the settings for this integration"""
        ...


class HasLibraryIntegrationConfiguration(HasIntegrationConfiguration, ABC):
    @classmethod
    @abstractmethod
    def library_settings_class(cls) -> Type[BaseSettings]:
        """Get the library settings for this integration"""
        ...

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from core.configuration.library import LibrarySettings
from core.model import Library

if TYPE_CHECKING:
    from tests.fixtures.database import DatabaseTransactionFixture


@pytest.fixture
def default_library(db: DatabaseTransactionFixture) -> Library:
    return db.default_library()


class MockLibrarySettings(LibrarySettings):
    """
    A mock LibrarySettings object that can be used in tests, the
    only change is that it allows mutation of the settings.
    """

    class Config(LibrarySettings.Config):
        allow_mutation = True


class LibraryFixture:
    """
    A mock Library object that can be used in tests, it returns
    a MockLibrarySettings object for its settings. This allows us
    to write tests that change the settings of a library.
    """

    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.db = db

    def library(
        self,
        name: str | None = None,
        short_name: str | None = None,
        settings: LibrarySettings | None = None,
    ) -> Library:
        library = self.db.library(name=name, short_name=short_name, settings=settings)
        if isinstance(settings, MockLibrarySettings):
            self.set_mock_on_library(library, settings)
        return library

    def settings(self, library: Library) -> MockLibrarySettings:
        settings_dict = library.settings_dict
        settings = MockLibrarySettings(**settings_dict)
        self.set_mock_on_library(library, settings)
        return settings

    def mock_settings(self) -> MockLibrarySettings:
        return MockLibrarySettings.construct()

    def set_mock_on_library(
        self, library: Library, settings: MockLibrarySettings
    ) -> None:
        library._settings = settings

    def reset_settings_cache(self, library: Library) -> None:
        library._settings = None


@pytest.fixture
def library_fixture(db: DatabaseTransactionFixture) -> LibraryFixture:
    return LibraryFixture(db)

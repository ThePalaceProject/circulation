"""MetaMetrics Lexile DB Service - metadata integration for augmenting Lexile scores."""

from __future__ import annotations

from collections.abc import Generator

from sqlalchemy.orm import Session

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.core.selftest import HasSelfTests, SelfTestResult
from palace.manager.integration.goals import Goals
from palace.manager.integration.metadata.base import (
    MetadataService,
)
from palace.manager.integration.metadata.lexile.api import LexileDBAPI
from palace.manager.integration.metadata.lexile.settings import (
    DEFAULT_SAMPLE_ISBN,
    LexileDBSettings,
)
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.log import LoggerMixin


class LexileDBService(
    MetadataService[LexileDBSettings],
    HasSelfTests,
    LoggerMixin,
):
    """Augment Lexile scores from the authoritative MetaMetrics Lexile Titles Database.

    This integration fetches Lexile measures from the Lexile DB API and adds them
    as classifications. Lexile scores from this source are treated as high-quality
    and will override scores from other sources (e.g. Overdrive) when both exist.
    """

    def __init__(
        self,
        _db: Session,
        settings: LexileDBSettings,
    ) -> None:
        """Initialize the service.

        :param _db: Database session (required for HasSelfTests compatibility).
        :param settings: Lexile DB configuration.
        """
        self._settings = settings

    @classmethod
    def label(cls) -> str:
        return "MetaMetrics Lexile DB Service"

    @classmethod
    def description(cls) -> str:
        return (
            "Augments Lexile reading measures from the authoritative MetaMetrics "
            "Lexile Titles Database. A nightly task processes ISBNs that lack "
            "Lexile data and adds scores from this high-quality source."
        )

    @classmethod
    def settings_class(cls) -> type[LexileDBSettings]:
        return LexileDBSettings

    @classmethod
    def multiple_services_allowed(cls) -> bool:
        return False

    @classmethod
    def integration(cls, _db: Session) -> IntegrationConfiguration | None:
        """Get the Lexile DB integration configuration if one exists."""
        return get_one(
            _db,
            IntegrationConfiguration,
            goal=Goals.METADATA_GOAL,
            protocol=cls.protocols()[0],
        )

    @classmethod
    def from_config(cls, _db: Session) -> LexileDBService:
        """Load the Lexile DB service from configuration."""
        integration = cls.integration(_db)
        if not integration:
            raise CannotLoadConfiguration("No Lexile DB integration configured.")
        settings = cls.settings_load(integration)
        return cls(_db, settings)

    def _run_self_tests(self, _db: Session) -> Generator[SelfTestResult]:
        """Run a self-test by fetching Lexile data for the sample ISBN."""
        isbn = (
            self._settings.sample_identifier.strip()
            if self._settings.sample_identifier
            else DEFAULT_SAMPLE_ISBN
        )

        def test_lookup() -> str:
            api = LexileDBAPI(self._settings)
            lexile = api.fetch_lexile_for_isbn(isbn, raise_on_error=True)
            if lexile is not None:
                return f"Successfully retrieved Lexile measure {lexile} for ISBN {isbn}"
            return f"No Lexile data found for ISBN {isbn} (API connection succeeded)"

        yield self.run_test(f"Looking up Lexile for ISBN {isbn}", test_lookup)

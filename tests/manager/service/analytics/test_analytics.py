import os
from unittest.mock import MagicMock, patch

from palace.manager.core.config import Configuration
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.service.analytics.local import LocalAnalyticsProvider
from palace.manager.service.analytics.s3 import S3AnalyticsProvider
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from tests.fixtures.database import DatabaseTransactionFixture


class TestAnalytics:
    def test_is_configured(self) -> None:
        analytics = Analytics()
        assert analytics.is_configured() == True

        analytics = Analytics(s3_analytics_enabled=True)
        assert analytics.is_configured() == True

        # If somehow we don't have providers, we don't have analytics
        analytics.providers = []
        assert analytics.is_configured() == False

    def test_init_analytics(self) -> None:
        analytics = Analytics()

        assert len(analytics.providers) == 1
        assert type(analytics.providers[0]) == LocalAnalyticsProvider

        analytics = Analytics(
            s3_analytics_enabled=True,
            s3_service=MagicMock(),
        )

        assert len(analytics.providers) == 2
        assert type(analytics.providers[0]) == LocalAnalyticsProvider
        assert type(analytics.providers[1]) == S3AnalyticsProvider

    def test_collect_event_passes_geo_to_event_data(
        self,
        db: DatabaseTransactionFixture,
    ) -> None:
        """collect_event() reads country/state from library settings and passes them to AnalyticsEventData."""
        library = db.default_library()
        pool = db.licensepool(edition=db.edition())
        library.update_settings(
            library.settings.model_copy(update={"country": "CA", "state": "Ontario"})
        )

        collected: list = []
        analytics = Analytics()
        analytics.collect = lambda event, session=None: collected.append(event)
        analytics.collect_event(library, pool, CirculationEvent.CM_CHECKOUT)

        assert len(collected) == 1
        assert collected[0].country == "CA"
        assert collected[0].state == "Ontario"

    def test_collect_event_country_state_none_when_library_settings_absent(
        self,
        db: DatabaseTransactionFixture,
    ) -> None:
        """collect_event() passes None for country/state when library has no geo settings configured."""
        library = db.default_library()
        pool = db.licensepool(edition=db.edition())
        # Freshly-created libraries have country=None and state=None by default.

        collected: list = []
        analytics = Analytics()
        analytics.collect = lambda event, session=None: collected.append(event)
        analytics.collect_event(library, pool, CirculationEvent.CM_CHECKOUT)

        assert len(collected) == 1
        assert collected[0].country is None
        assert collected[0].state is None

    def test_collect_event_passes_palace_manager_name_from_env(
        self,
        db: DatabaseTransactionFixture,
    ) -> None:
        """collect_event() reads PALACE_REPORTING_NAME from env and embeds it in the event."""
        library = db.default_library()
        pool = db.licensepool(edition=db.edition())

        collected: list = []
        analytics = Analytics()
        analytics.collect = lambda event, session=None: collected.append(event)

        with patch.dict(
            os.environ,
            {Configuration.REPORTING_NAME_ENVIRONMENT_VARIABLE: "my-cm-instance"},
        ):
            analytics.collect_event(library, pool, CirculationEvent.CM_CHECKOUT)

        assert len(collected) == 1
        assert collected[0].palace_manager_name == "my-cm-instance"

    def test_collect_event_palace_manager_name_none_when_env_absent(
        self,
        db: DatabaseTransactionFixture,
    ) -> None:
        """collect_event() sets palace_manager_name to None when env var is not set."""
        library = db.default_library()
        pool = db.licensepool(edition=db.edition())

        collected: list = []
        analytics = Analytics()
        analytics.collect = lambda event, session=None: collected.append(event)

        env = {
            k: v
            for k, v in os.environ.items()
            if k != Configuration.REPORTING_NAME_ENVIRONMENT_VARIABLE
        }
        with patch.dict(os.environ, env, clear=True):
            analytics.collect_event(library, pool, CirculationEvent.CM_CHECKOUT)

        assert len(collected) == 1
        assert collected[0].palace_manager_name is None

import unicodedata
import urllib.parse
from unittest.mock import MagicMock

import pytest
from psycopg2.extras import NumericRange

from api.config import CannotLoadConfiguration
from api.google_analytics_provider import GoogleAnalyticsProvider
from core.model import (
    CirculationEvent,
    ConfigurationSetting,
    DataSource,
    EditionConstants,
    ExternalIntegration,
    LicensePool,
    create,
    get_one_or_create,
)
from core.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class MockGoogleAnalyticsProvider(GoogleAnalyticsProvider):
    def post(self, url, params):
        self.count = self.count + 1 if hasattr(self, "count") else 1
        self.url = url
        self.params = params


class TestGoogleAnalyticsProvider:
    def test_init(self, db: DatabaseTransactionFixture):
        integration, ignore = create(
            db.session,
            ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="api.google_analytics_provider",
        )

        with pytest.raises(CannotLoadConfiguration) as excinfo:
            GoogleAnalyticsProvider(integration, MagicMock())
        assert "Google Analytics can't be configured without a library." in str(
            excinfo.value
        )

        with pytest.raises(CannotLoadConfiguration) as excinfo:
            GoogleAnalyticsProvider(integration, MagicMock(), db.default_library())
        assert (
            "Missing tracking id for library %s" % db.default_library().short_name
            in str(excinfo.value)
        )

        ConfigurationSetting.for_library_and_externalintegration(
            db.session,
            GoogleAnalyticsProvider.TRACKING_ID,
            db.default_library(),
            integration,
        ).value = "faketrackingid"
        ga = GoogleAnalyticsProvider(integration, MagicMock(), db.default_library())
        assert GoogleAnalyticsProvider.DEFAULT_URL == ga.url
        assert "faketrackingid" == ga.tracking_id

        integration.url = db.fresh_str()
        ga = GoogleAnalyticsProvider(integration, MagicMock(), db.default_library())
        assert integration.url == ga.url
        assert "faketrackingid" == ga.tracking_id

    def test_collect_event_with_work(self, db: DatabaseTransactionFixture):
        integration, ignore = create(
            db.session,
            ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="api.google_analytics_provider",
        )
        integration.url = db.fresh_str()
        ConfigurationSetting.for_library_and_externalintegration(
            db.session,
            GoogleAnalyticsProvider.TRACKING_ID,
            db.default_library(),
            integration,
        ).value = "faketrackingid"
        ga = MockGoogleAnalyticsProvider(integration, MagicMock(), db.default_library())

        work = db.work(
            title="pi\u00F1ata",
            authors="chlo\u00E9",
            fiction=True,
            audience="audience",
            language="lang",
            with_license_pool=True,
            genre="Folklore",
            with_open_access_download=True,
        )
        work.presentation_edition.publisher = "publisher"
        work.target_age = NumericRange(10, 15)
        [lp] = work.license_pools
        now = utc_now()
        ga.collect_event(
            db.default_library(),
            lp,
            CirculationEvent.DISTRIBUTOR_CHECKIN,
            now,
            neighborhood="Neighborhood will not be sent",
        )

        # Neighborhood information is not being sent -- that's for
        # local consumption only.
        assert "Neighborhood" not in ga.params

        # Let's take a look at what _is_ being sent.
        params = urllib.parse.parse_qs(ga.params)

        assert 1 == ga.count
        assert integration.url == ga.url
        assert "faketrackingid" == params["tid"][0]
        assert "event" == params["t"][0]
        assert "circulation" == params["ec"][0]
        assert CirculationEvent.DISTRIBUTOR_CHECKIN == params["ea"][0]
        assert str(now) == params["cd1"][0]
        assert lp.identifier.identifier == params["cd2"][0]
        assert lp.identifier.type == params["cd3"][0]
        assert unicodedata.normalize("NFKD", work.title) == params["cd4"][0]
        assert unicodedata.normalize("NFKD", work.author) == params["cd5"][0]
        assert "fiction" == params["cd6"][0]
        assert "audience" == params["cd7"][0]
        assert work.target_age_string == params["cd8"][0]
        assert "publisher" == params["cd9"][0]
        assert "lang" == params["cd10"][0]
        assert "Folklore" == params["cd11"][0]
        assert "true" == params["cd12"][0]
        assert DataSource.GUTENBERG == params["cd13"][0]
        assert EditionConstants.BOOK_MEDIUM == params["cd14"][0]
        assert db.default_library().short_name == params["cd15"][0]
        assert lp.collection.name == params["cd16"][0]

    def test_collect_event_without_work(self, db: DatabaseTransactionFixture):
        integration, ignore = create(
            db.session,
            ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="api.google_analytics_provider",
        )
        integration.url = db.fresh_str()
        ConfigurationSetting.for_library_and_externalintegration(
            db.session,
            GoogleAnalyticsProvider.TRACKING_ID,
            db.default_library(),
            integration,
        ).value = "faketrackingid"
        ga = MockGoogleAnalyticsProvider(integration, MagicMock(), db.default_library())

        identifier = db.identifier()
        source = DataSource.lookup(db.session, DataSource.GUTENBERG)
        pool, is_new = get_one_or_create(
            db.session,
            LicensePool,
            identifier=identifier,
            data_source=source,
            collection=db.default_collection(),
        )

        now = utc_now()
        ga.collect_event(
            db.default_library(), pool, CirculationEvent.DISTRIBUTOR_CHECKIN, now
        )
        params = urllib.parse.parse_qs(ga.params)

        assert 1 == ga.count
        assert integration.url == ga.url
        assert "faketrackingid" == params["tid"][0]
        assert "event" == params["t"][0]
        assert "circulation" == params["ec"][0]
        assert CirculationEvent.DISTRIBUTOR_CHECKIN == params["ea"][0]
        assert str(now) == params["cd1"][0]
        assert pool.identifier.identifier == params["cd2"][0]
        assert pool.identifier.type == params["cd3"][0]
        assert None == params.get("cd4")
        assert None == params.get("cd5")
        assert None == params.get("cd6")
        assert None == params.get("cd7")
        assert None == params.get("cd8")
        assert None == params.get("cd9")
        assert None == params.get("cd10")
        assert None == params.get("cd11")
        assert None == params.get("cd12")
        assert [source.name] == params.get("cd13")
        assert None == params.get("cd14")
        assert [db.default_library().short_name] == params.get("cd15")
        assert None == params.get("cd16")

    def test_collect_event_without_license_pool(self, db: DatabaseTransactionFixture):
        integration, ignore = create(
            db.session,
            ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="api.google_analytics_provider",
        )
        integration.url = db.fresh_str()
        ConfigurationSetting.for_library_and_externalintegration(
            db.session,
            GoogleAnalyticsProvider.TRACKING_ID,
            db.default_library(),
            integration,
        ).value = "faketrackingid"
        ga = MockGoogleAnalyticsProvider(integration, MagicMock(), db.default_library())

        now = utc_now()
        ga.collect_event(db.default_library(), None, CirculationEvent.NEW_PATRON, now)
        params = urllib.parse.parse_qs(ga.params)

        assert 1 == ga.count
        assert integration.url == ga.url
        assert "faketrackingid" == params["tid"][0]
        assert "event" == params["t"][0]
        assert "circulation" == params["ec"][0]
        assert CirculationEvent.NEW_PATRON == params["ea"][0]
        assert str(now) == params["cd1"][0]
        assert None == params.get("cd2")
        assert None == params.get("cd3")
        assert None == params.get("cd4")
        assert None == params.get("cd5")
        assert None == params.get("cd6")
        assert None == params.get("cd7")
        assert None == params.get("cd8")
        assert None == params.get("cd9")
        assert None == params.get("cd10")
        assert None == params.get("cd11")
        assert None == params.get("cd12")
        assert None == params.get("cd13")
        assert None == params.get("cd14")
        assert [db.default_library().short_name] == params.get("cd15")
        assert None == params.get("cd16")

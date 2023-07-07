from core.analytics import Analytics
from core.local_analytics_provider import LocalAnalyticsProvider
from core.mock_analytics_provider import MockAnalyticsProvider
from core.model import CirculationEvent, ExternalIntegration, create

# We can't import mock_analytics_provider from within a test,
# and we can't tell Analytics to do so either. We need to tell
# it to perform an import relative to the module the Analytics
# class is in.
from tests.fixtures.database import DatabaseTransactionFixture

MOCK_PROTOCOL = "..mock_analytics_provider"


class TestAnalytics:
    def test_initialize(self, db: DatabaseTransactionFixture):
        # supports multiple analytics providers, site-wide or with libraries
        # Two site-wide integrations
        site_wide_integration1, ignore = create(
            db.session,
            ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol=MOCK_PROTOCOL,
        )
        site_wide_integration1.url = db.fresh_str()
        site_wide_integration2, ignore = create(
            db.session,
            ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="..local_analytics_provider",
        )

        # A broken integration
        missing_integration, ignore = create(
            db.session,
            ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="missing_provider",
        )

        # Two library-specific integrations
        l1 = db.library(short_name="L1")
        l2 = db.library(short_name="L2")

        library_integration1, ignore = create(
            db.session,
            ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol=MOCK_PROTOCOL,
        )
        library_integration1.libraries += [l1, l2]

        library_integration2, ignore = create(
            db.session,
            ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol=MOCK_PROTOCOL,
        )
        library_integration2.libraries += [l2]

        analytics = Analytics(db.session)
        assert 2 == len(analytics.sitewide_providers)
        assert isinstance(analytics.sitewide_providers[0], MockAnalyticsProvider)
        assert site_wide_integration1.url == analytics.sitewide_providers[0].url
        assert isinstance(analytics.sitewide_providers[1], LocalAnalyticsProvider)
        assert missing_integration.id in analytics.initialization_exceptions

        assert 1 == len(analytics.library_providers[l1.id])
        assert isinstance(analytics.library_providers[l1.id][0], MockAnalyticsProvider)

        assert 2 == len(analytics.library_providers[l2.id])
        for provider in analytics.library_providers[l2.id]:
            assert isinstance(provider, MockAnalyticsProvider)

        # Instantiating an Analytics object initializes class
        # variables with the current state of site analytics.

        # We have global analytics enabled.
        assert Analytics.GLOBAL_ENABLED is True

        # We also have analytics enabled for two of the three libraries.
        assert {l1.id, l2.id} == Analytics.LIBRARY_ENABLED

        # Now we'll change the analytics configuration.
        db.session.delete(site_wide_integration1)
        db.session.delete(site_wide_integration2)
        db.session.delete(library_integration1)

        # But Analytics is a singleton, so if we instantiate a new
        # Analytics object in the same app instance, it will be the
        # same as the previous one.
        analytics2 = Analytics(db.session)
        assert analytics2 == analytics
        assert 2 == len(analytics.sitewide_providers)
        assert 1 == len(analytics.library_providers[l1.id])
        assert 2 == len(analytics.library_providers[l2.id])

        # If, however, we simulate a configuration refresh ...
        analytics3 = Analytics(db.session, refresh=True)
        # ... we will see the updated configuration.
        assert analytics3 == analytics
        assert Analytics.GLOBAL_ENABLED is False
        assert {l2.id} == Analytics.LIBRARY_ENABLED  # type: ignore

    def test_is_configured(self, db: DatabaseTransactionFixture):
        # If the Analytics constructor has not been called, then
        # is_configured() calls it so that the values are populated.
        Analytics.GLOBAL_ENABLED = None
        library = db.default_library()
        assert False == Analytics.is_configured(library)
        assert False == Analytics.GLOBAL_ENABLED
        assert set() == Analytics.LIBRARY_ENABLED

        # If analytics are enabled globally, they are enabled for any
        # library.
        Analytics.GLOBAL_ENABLED = True
        assert True == Analytics.is_configured(object())

        # If not, they are enabled only for libraries whose IDs are
        # in LIBRARY_ENABLED.
        Analytics.GLOBAL_ENABLED = False
        assert False == Analytics.is_configured(library)
        assert isinstance(library.id, int)
        Analytics.LIBRARY_ENABLED.add(library.id)
        assert True == Analytics.is_configured(library)

    def test_collect_event(self, db: DatabaseTransactionFixture):
        # This will be a site-wide integration because it will have no
        # associated libraries when the Analytics singleton is instantiated.
        # the first time.
        sitewide_integration, ignore = create(
            db.session,
            ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol=MOCK_PROTOCOL,
        )

        # This will be a per-library integration because it will have at least
        # one associated library when the Analytics singleton is instantiated.
        library_integration, ignore = create(
            db.session,
            ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol=MOCK_PROTOCOL,
        )
        library = db.library(short_name="library")
        library_integration.libraries += [library]

        work = db.work(title="title", with_license_pool=True)
        [lp] = work.license_pools
        analytics = Analytics(db.session)
        sitewide_provider = analytics.sitewide_providers[0]
        library_provider = analytics.library_providers[library.id][0]

        analytics.collect_event(
            db.default_library(),
            lp,
            CirculationEvent.DISTRIBUTOR_CHECKIN,
            None,
        )

        # The sitewide provider was called.
        assert 1 == sitewide_provider.count
        assert CirculationEvent.DISTRIBUTOR_CHECKIN == sitewide_provider.event_type

        # The library provider wasn't called, since the event was for a different library.
        assert 0 == library_provider.count

        analytics.collect_event(library, lp, CirculationEvent.DISTRIBUTOR_CHECKIN, None)

        # Now both providers were called, since the event was for the library provider's library.
        assert 2 == sitewide_provider.count
        assert 1 == library_provider.count
        assert CirculationEvent.DISTRIBUTOR_CHECKIN == library_provider.event_type

        # Here's an event that we couldn't associate with any
        # particular library.
        analytics.collect_event(None, lp, CirculationEvent.DISTRIBUTOR_CHECKOUT, None)

        # It's counted as a sitewide event, but not as a library event.
        assert 3 == sitewide_provider.count
        assert 1 == library_provider.count

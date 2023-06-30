import pytest

from core.local_analytics_provider import LocalAnalyticsProvider
from core.model import CirculationEvent, ExternalIntegration, create, get_one
from core.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class TestInitializeLocalAnalyticsProvider:
    def test_initialize(self, db: DatabaseTransactionFixture):
        session = db.session

        local_analytics = get_one(
            session,
            ExternalIntegration,
            protocol=LocalAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )

        # There shouldn't exist a local analytics service.
        assert local_analytics is None

        # So when the Local Analytics provider is initialized, it will
        # create one with the default name of "Local Analytics".
        local_analytics = LocalAnalyticsProvider.initialize(session)

        assert isinstance(local_analytics, ExternalIntegration)
        assert local_analytics.name == LocalAnalyticsProvider.NAME

        # When an analytics provider is initialized, retrieving a
        # local analytics service should return the same one.
        local_analytics = LocalAnalyticsProvider.initialize(session)

        local_analytics_2 = get_one(
            session,
            ExternalIntegration,
            protocol=LocalAnalyticsProvider.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )

        assert isinstance(local_analytics, ExternalIntegration)
        assert isinstance(local_analytics_2, ExternalIntegration)
        assert local_analytics_2.id == local_analytics.id
        assert local_analytics_2.name == local_analytics.name


class LocalAnalyticsProviderFixture:
    transaction: DatabaseTransactionFixture
    integration: ExternalIntegration
    la: LocalAnalyticsProvider

    def __init__(self, transaction: DatabaseTransactionFixture):
        self.transaction = transaction
        self.integration, ignore = create(
            transaction.session,
            ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="core.local_analytics_provider",
        )
        self.la = LocalAnalyticsProvider(
            self.integration, transaction.default_library()
        )


@pytest.fixture()
def local_analytics_provider_fixture(
    db,
) -> LocalAnalyticsProviderFixture:
    return LocalAnalyticsProviderFixture(db)


class TestLocalAnalyticsProvider:
    def test_collect_event(
        self, local_analytics_provider_fixture: LocalAnalyticsProviderFixture
    ):
        data = local_analytics_provider_fixture
        database = local_analytics_provider_fixture.transaction
        session = database.session

        library2 = database.library()
        work = database.work(
            title="title",
            authors="author",
            fiction=True,
            audience="audience",
            language="lang",
            with_license_pool=True,
        )
        [lp] = work.license_pools
        now = utc_now()
        data.la.collect_event(
            database.default_library(),
            lp,
            CirculationEvent.DISTRIBUTOR_CHECKIN,
            now,
            old_value=None,
            new_value=None,
        )

        qu = session.query(CirculationEvent).filter(
            CirculationEvent.type == CirculationEvent.DISTRIBUTOR_CHECKIN
        )
        assert 1 == qu.count()
        [event] = qu.all()

        assert lp == event.license_pool
        assert database.default_library() == event.library
        assert CirculationEvent.DISTRIBUTOR_CHECKIN == event.type
        assert now == event.start

        # The LocalAnalyticsProvider will not handle an event intended
        # for a different library.
        now = utc_now()
        data.la.collect_event(
            library2,
            lp,
            CirculationEvent.DISTRIBUTOR_CHECKIN,
            now,
            old_value=None,
            new_value=None,
        )
        assert 1 == qu.count()

        # It's possible to instantiate the LocalAnalyticsProvider
        # without a library.
        la = LocalAnalyticsProvider(data.integration)

        # In that case, it will process events for any library.
        for library in [database.default_library(), library2]:
            now = utc_now()
            la.collect_event(
                library,
                lp,
                CirculationEvent.DISTRIBUTOR_CHECKIN,
                now,
                old_value=None,
                new_value=None,
            )
        assert 3 == qu.count()

    def test_collect_with_missing_information(
        self, local_analytics_provider_fixture: LocalAnalyticsProviderFixture
    ):
        """A circulation event may be collected with either the
        library or the license pool missing, but not both.
        """

        data = local_analytics_provider_fixture
        database = local_analytics_provider_fixture.transaction
        now = utc_now()
        data.la.collect_event(database.default_library(), None, "event", now)

        pool = database.licensepool(None)
        data.la.collect_event(None, pool, "event", now)

        with pytest.raises(ValueError) as excinfo:
            data.la.collect_event(None, None, "event", now)
        assert "Either library or license_pool must be provided." in str(excinfo.value)

    def test_neighborhood_is_location(
        self, local_analytics_provider_fixture: LocalAnalyticsProviderFixture
    ):
        # If a 'neighborhood' argument is provided, its value
        # is used as CirculationEvent.location.
        data = local_analytics_provider_fixture
        database = local_analytics_provider_fixture.transaction

        # The default LocalAnalytics object doesn't have a location
        # gathering policy, and the default is to ignore location.
        event, is_new = data.la.collect_event(
            database.default_library(),
            None,
            "event",
            utc_now(),
            neighborhood="Gormenghast",
        )
        assert is_new is True
        assert event.location is None

        # Create another LocalAnalytics object that uses the patron
        # neighborhood as the event location.

        p = LocalAnalyticsProvider
        data.integration.setting(
            p.LOCATION_SOURCE
        ).value = p.LOCATION_SOURCE_NEIGHBORHOOD
        la = p(data.integration, database.default_library())

        event, is_new = la.collect_event(
            database.default_library(),
            None,
            "event",
            utc_now(),
            neighborhood="Gormenghast",
        )
        assert is_new is True
        assert "Gormenghast" == event.location

        # If no neighborhood is available, the event ends up with no location
        # anyway.
        event2, is_new = la.collect_event(
            database.default_library(),
            None,
            "event",
            utc_now(),
        )
        assert event2 != event
        assert is_new is True
        assert event2.location is None

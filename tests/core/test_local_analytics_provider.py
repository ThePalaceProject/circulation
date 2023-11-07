from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from core.local_analytics_provider import LocalAnalyticsProvider
from core.model import CirculationEvent, ExternalIntegration
from core.util.datetime_helpers import utc_now

if TYPE_CHECKING:
    from tests.fixtures.database import DatabaseTransactionFixture
    from tests.fixtures.services import MockServicesFixture


class LocalAnalyticsProviderFixture:
    transaction: DatabaseTransactionFixture
    integration: ExternalIntegration
    la: LocalAnalyticsProvider

    def __init__(
        self,
        transaction: DatabaseTransactionFixture,
        mock_services_fixture: MockServicesFixture,
    ):
        self.transaction = transaction
        self.services = mock_services_fixture.services
        self.la = LocalAnalyticsProvider()


@pytest.fixture()
def local_analytics_provider_fixture(
    db: DatabaseTransactionFixture, mock_services_fixture: MockServicesFixture
) -> LocalAnalyticsProviderFixture:
    return LocalAnalyticsProviderFixture(db, mock_services_fixture)


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

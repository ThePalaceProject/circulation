from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from palace.manager.service.analytics.local import LocalAnalyticsProvider
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.util.datetime_helpers import utc_now

if TYPE_CHECKING:
    from tests.fixtures.database import DatabaseTransactionFixture


class LocalAnalyticsProviderFixture:
    transaction: DatabaseTransactionFixture
    la: LocalAnalyticsProvider

    def __init__(
        self,
        transaction: DatabaseTransactionFixture,
    ):
        self.transaction = transaction
        self.la = LocalAnalyticsProvider()


@pytest.fixture()
def local_analytics_provider_fixture(
    db: DatabaseTransactionFixture,
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

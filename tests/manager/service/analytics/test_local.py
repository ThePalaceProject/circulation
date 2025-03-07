from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from freezegun import freeze_time

from palace.manager.service.analytics.eventdata import AnalyticsEventData
from palace.manager.service.analytics.local import LocalAnalyticsProvider
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.util.datetime_helpers import datetime_utc, utc_now

if TYPE_CHECKING:
    from tests.fixtures.database import DatabaseTransactionFixture


class LocalAnalyticsProviderFixture:
    def __init__(
        self,
        transaction: DatabaseTransactionFixture,
    ):
        self.db = transaction
        self.provider = LocalAnalyticsProvider()


@pytest.fixture()
def local_analytics_provider_fixture(
    db: DatabaseTransactionFixture,
) -> LocalAnalyticsProviderFixture:
    return LocalAnalyticsProviderFixture(db)


class TestLocalAnalyticsProvider:
    def test_collect(
        self,
        db: DatabaseTransactionFixture,
        local_analytics_provider_fixture: LocalAnalyticsProviderFixture,
    ) -> None:
        work = db.work(
            title="title",
            authors="author",
            fiction=True,
            audience="audience",
            language="lang",
            with_license_pool=True,
        )
        [lp] = work.license_pools
        now = utc_now()
        event_data = AnalyticsEventData.create(
            db.default_library(),
            lp,
            CirculationEvent.DISTRIBUTOR_CHECKIN,
            now,
        )
        local_analytics_provider_fixture.provider.collect(
            event_data,
            db.session,
        )

        qu = db.session.query(CirculationEvent).filter(
            CirculationEvent.type == CirculationEvent.DISTRIBUTOR_CHECKIN
        )
        assert 1 == qu.count()
        [event] = qu.all()

        assert lp == event.license_pool
        assert db.default_library() == event.library
        assert CirculationEvent.DISTRIBUTOR_CHECKIN == event.type
        assert now == event.start

    def test_collect_end_to_end(
        self,
        db: DatabaseTransactionFixture,
        local_analytics_provider_fixture: LocalAnalyticsProviderFixture,
    ) -> None:
        pool = db.licensepool(edition=None)
        library = db.default_library()
        event_name = CirculationEvent.DISTRIBUTOR_CHECKOUT
        old_value = 10
        new_value = 8
        start = datetime_utc(2019, 1, 1)

        session = db.session
        event_data = AnalyticsEventData.create(
            library=library,
            license_pool=pool,
            event_type=event_name,
            old_value=old_value,
            new_value=new_value,
            time=start,
        )
        local_analytics_provider_fixture.provider.collect(
            event_data,
            db.session,
        )
        [event] = db.session.query(CirculationEvent).all()
        assert pool == event.license_pool
        assert library == event.library
        assert -2 == event.delta  # calculated from old_value and new_value
        assert start == event.start
        assert start == event.end

        # If collect finds another event with the same license pool,
        # library, event name, and time, the new event is not recorded
        # and the previous event is unchanged.
        event_data = AnalyticsEventData.create(
            library=library,
            license_pool=pool,
            event_type=event_name,
            time=start,
            # These values will be ignored.
            old_value=500,
            new_value=200,
        )
        local_analytics_provider_fixture.provider.collect(
            event_data,
            db.session,
        )
        [event] = db.session.query(CirculationEvent).all()
        assert pool == event.license_pool
        assert library == event.library
        assert -2 == event.delta
        assert start == event.start

        # If no timestamp is provided, the current time is used. This
        # is the most common case, so basically a new event will be
        # created each time you call collect.
        with freeze_time():
            event_data = AnalyticsEventData.create(
                library=library,
                license_pool=pool,
                event_type=event_name,
                old_value=old_value,
                new_value=new_value,
            )
            local_analytics_provider_fixture.provider.collect(
                event_data,
                db.session,
            )

            [_, event] = (
                db.session.query(CirculationEvent).order_by(CirculationEvent.id).all()
            )
            assert event.start == utc_now()
            assert pool == event.license_pool
            assert library == event.library
            assert -2 == event.delta

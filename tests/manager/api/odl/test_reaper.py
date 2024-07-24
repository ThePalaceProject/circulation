from __future__ import annotations

import datetime

from palace.manager.api.odl.reaper import OPDS2WithODLHoldReaper
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.patron import Hold
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.odl import OPDS2WithODLApiFixture


class TestOPDS2WithODLHoldReaper:
    def test_run_once(
        self,
        opds2_with_odl_api_fixture: OPDS2WithODLApiFixture,
        db: DatabaseTransactionFixture,
    ):
        collection = opds2_with_odl_api_fixture.collection
        work = opds2_with_odl_api_fixture.work
        license = opds2_with_odl_api_fixture.setup_license(
            work, concurrency=3, available=3
        )
        api = opds2_with_odl_api_fixture.api
        pool = license.license_pool

        data_source = DataSource.lookup(db.session, "Feedbooks", autocreate=True)
        DatabaseTransactionFixture.set_settings(
            collection.integration_configuration,
            **{Collection.DATA_SOURCE_NAME_SETTING: data_source.name},
        )
        reaper = OPDS2WithODLHoldReaper(db.session, collection, api=api)

        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)

        expired_hold1, ignore = pool.on_hold_to(db.patron(), end=yesterday, position=0)
        expired_hold2, ignore = pool.on_hold_to(db.patron(), end=yesterday, position=0)
        expired_hold3, ignore = pool.on_hold_to(db.patron(), end=yesterday, position=0)
        current_hold, ignore = pool.on_hold_to(db.patron(), position=3)
        # This hold has an end date in the past, but its position is greater than 0
        # so the end date is not reliable.
        bad_end_date, ignore = pool.on_hold_to(db.patron(), end=yesterday, position=4)

        progress = reaper.run_once(reaper.timestamp().to_data())

        # The expired holds have been deleted and the other holds have been updated.
        assert 2 == db.session.query(Hold).count()
        assert [current_hold, bad_end_date] == db.session.query(Hold).order_by(
            Hold.start
        ).all()
        assert 0 == current_hold.position
        assert 0 == bad_end_date.position
        assert current_hold.end > now
        assert bad_end_date.end > now
        assert 1 == pool.licenses_available
        assert 2 == pool.licenses_reserved

        # The TimestampData returned reflects what work was done.
        assert "Holds deleted: 3. License pools updated: 1" == progress.achievements

        # The TimestampData does not include any timing information --
        # that will be applied by run().
        assert None == progress.start
        assert None == progress.finish

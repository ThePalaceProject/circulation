from __future__ import annotations

import functools
from unittest.mock import patch

import pytest

from palace.manager.core.config import Configuration
from palace.manager.sqlalchemy.listeners import site_configuration_has_changed
from palace.manager.sqlalchemy.model.coverage import Timestamp, WorkCoverageRecord
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class TestSiteConfigurationHasChanged:
    def test_site_configuration_has_changed(
        self,
        db: DatabaseTransactionFixture,
    ):
        """Test the site_configuration_has_changed() function and its
        effects on the Configuration object.
        """
        # The database configuration timestamp is initialized as part
        # of the default data. In that case, it happened during the
        # package_setup() for this test run.
        session = db.session
        last_update = Configuration.site_configuration_last_update(session)

        def ts():
            return Timestamp.value(
                session,
                Configuration.SITE_CONFIGURATION_CHANGED,
                service_type=None,
                collection=None,
            )

        timestamp_value = ts()
        assert timestamp_value == last_update

        # Now let's call site_configuration_has_changed().
        #
        # Sending cooldown=0 ensures we can change the timestamp value
        # even though it changed less than one second ago.
        time_of_update = utc_now()
        site_configuration_has_changed(session, cooldown=0)

        # The Timestamp has changed in the database.
        assert ts() > timestamp_value

        # The locally-stored last update value has been updated.
        new_last_update_time = Configuration.site_configuration_last_update(
            session, timeout=0
        )
        assert new_last_update_time > last_update
        assert (new_last_update_time - time_of_update).total_seconds() < 1

        # Let's be sneaky and update the timestamp directly,
        # without calling site_configuration_has_changed(). This
        # simulates another process on a different machine calling
        # site_configuration_has_changed() -- they will know about the
        # change but we won't be informed.
        timestamp = Timestamp.stamp(
            session,
            Configuration.SITE_CONFIGURATION_CHANGED,
            service_type=None,
            collection=None,
        )

        # Calling Configuration.check_for_site_configuration_update
        # with a timeout doesn't detect the change.
        assert new_last_update_time == Configuration.site_configuration_last_update(
            session, timeout=60
        )

        # But the default behavior -- a timeout of zero -- forces
        # the method to go to the database and find the correct
        # answer.
        newer_update = Configuration.site_configuration_last_update(session)
        assert newer_update > last_update

        # The Timestamp that tracks the last configuration update has
        # a cooldown; the default cooldown is 1 second. This means the
        # last update time will only be set once per second, to avoid
        # spamming the Timestamp with updates.

        # It's been less than one second since we updated the timeout
        # (with the Timestamp.stamp call). If this call decided that
        # the cooldown had expired, it would try to update the
        # Timestamp, and the code would crash because we're passing in
        # None instead of a database connection.
        #
        # But it knows the cooldown has not expired, so nothing
        # happens.
        site_configuration_has_changed(None)

        # Verify that the Timestamp has not changed (how could it,
        # with no database connection to modify the Timestamp?)
        assert newer_update == Configuration.site_configuration_last_update(session)

    def test_lane_change_updates_configuration(
        self,
        db: DatabaseTransactionFixture,
    ):
        """Verify that configuration-relevant changes work the same way
        in the lane module as they do in the model module.
        """

        with patch(
            "palace.manager.sqlalchemy.listeners.site_configuration_has_changed"
        ) as mock:
            lane = db.lane()
            mock.assert_called_once()
            mock.reset_mock()

            lane.add_genre("Science Fiction")
            mock.assert_called_once()


def _set_property(object, value, property_name):
    setattr(object, property_name, value)


class TestListeners:
    @pytest.mark.parametrize(
        "name,status_property_setter",
        [
            (
                "works_when_open_access_property_changes",
                functools.partial(_set_property, property_name="open_access"),
            ),
        ],
    )
    def test_licensepool_storage_status_change(
        self,
        db,
        name,
        status_property_setter,
    ):
        # Arrange
        work = db.work(with_license_pool=True)
        [pool] = work.license_pools

        # Clear out any WorkCoverageRecords created as the work was initialized.
        work.coverage_records = []

        # Act
        # Change the field
        status_property_setter(pool, True)

        # Then verify that if the field is 'set' to its existing value, this doesn't happen.
        # pool.self_hosted = True
        status_property_setter(pool, True)

        # Assert
        assert 1 == len(work.coverage_records)
        assert work.id == work.coverage_records[0].work_id
        assert (
            WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION
            == work.coverage_records[0].operation
        )
        assert WorkCoverageRecord.REGISTERED == work.coverage_records[0].status

    def test_work_suppressed_for_library(self, db: DatabaseTransactionFixture):
        work = db.work(with_license_pool=True)
        library = db.library()

        # Clear out any WorkCoverageRecords created as the work was initialized.
        work.coverage_records = []

        # Act
        work.suppressed_for.append(library)

        # Assert
        assert 1 == len(work.coverage_records)
        assert work.id == work.coverage_records[0].work_id
        assert (
            WorkCoverageRecord.UPDATE_SEARCH_INDEX_OPERATION
            == work.coverage_records[0].operation
        )
        assert WorkCoverageRecord.REGISTERED == work.coverage_records[0].status

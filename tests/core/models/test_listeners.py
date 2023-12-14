import functools
from collections.abc import Iterable
from typing import Any

import pytest

from core import lane, model
from core.config import Configuration
from core.model import ConfigurationSetting, Timestamp, WorkCoverageRecord
from core.model.listeners import site_configuration_has_changed
from core.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class MockSiteConfigurationHasChanged:
    """Keep track of whether site_configuration_has_changed was
    ever called.
    """

    def __init__(self):
        self.was_called = False

    def run(self, _db):
        self.was_called = True
        site_configuration_has_changed(_db)

    def assert_was_called(self):
        "Assert that `was_called` is True, then reset it for the next assertion."
        assert self.was_called
        self.was_called = False

    def assert_was_not_called(self):
        assert not self.was_called


class ExampleSiteConfigurationHasChangedFixture:
    transaction: DatabaseTransactionFixture
    _old_site_configuration_has_changed: Any
    mock: MockSiteConfigurationHasChanged

    @classmethod
    def create(
        cls, transaction: DatabaseTransactionFixture
    ) -> "ExampleSiteConfigurationHasChangedFixture":
        data = ExampleSiteConfigurationHasChangedFixture()
        data.transaction = transaction

        # Mock model.site_configuration_has_changed
        data._old_site_configuration_has_changed = (
            model.listeners.site_configuration_has_changed
        )
        data.mock = MockSiteConfigurationHasChanged()
        for module in model.listeners, lane:
            module.site_configuration_has_changed = data.mock.run  # type: ignore[attr-defined]
        return data

    def close(self):
        for module in model.listeners, lane:
            module.site_configuration_has_changed = (
                self._old_site_configuration_has_changed
            )


@pytest.fixture()
def example_site_configuration_changed_fixture(
    db: DatabaseTransactionFixture,
) -> Iterable[ExampleSiteConfigurationHasChangedFixture]:
    data = ExampleSiteConfigurationHasChangedFixture.create(db)
    yield data
    data.close()


class TestSiteConfigurationHasChanged:
    def test_site_configuration_has_changed(
        self,
        example_site_configuration_changed_fixture: ExampleSiteConfigurationHasChangedFixture,
    ):
        """Test the site_configuration_has_changed() function and its
        effects on the Configuration object.
        """
        # The database configuration timestamp is initialized as part
        # of the default data. In that case, it happened during the
        # package_setup() for this test run.
        data = example_site_configuration_changed_fixture
        session = data.transaction.session
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

    # We don't test every event listener, but we do test one of each type.
    def test_configuration_relevant_lifecycle_event_updates_configuration(
        self,
        example_site_configuration_changed_fixture: ExampleSiteConfigurationHasChangedFixture,
    ):
        """When you create or modify a relevant item such as a
        ConfigurationSetting, site_configuration_has_changed is called.
        """
        data = example_site_configuration_changed_fixture
        session = data.transaction.session

        ConfigurationSetting.sitewide(session, "setting").value = "value"
        data.mock.assert_was_called()

        ConfigurationSetting.sitewide(session, "setting").value = "value2"
        session.flush()
        data.mock.assert_was_called()

    def test_lane_change_updates_configuration(
        self,
        example_site_configuration_changed_fixture: ExampleSiteConfigurationHasChangedFixture,
    ):
        """Verify that configuration-relevant changes work the same way
        in the lane module as they do in the model module.
        """
        data = example_site_configuration_changed_fixture
        session = data.transaction.session

        lane = data.transaction.lane()
        data.mock.assert_was_called()

        lane.add_genre("Science Fiction")
        data.mock.assert_was_called()


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

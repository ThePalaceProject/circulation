from __future__ import annotations

import functools
from collections.abc import Callable
from unittest.mock import patch

import pytest
from redis import RedisError

from palace.util.datetime_helpers import utc_now
from palace.util.log import LogLevel

from palace.manager.core.config import Configuration
from palace.manager.service.redis.models.dirty_identifiers import DirtyIdentifierIds
from palace.manager.sqlalchemy.listeners import site_configuration_has_changed
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.identifier import Equivalency
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture
from tests.fixtures.search import WorkQueueIndexingFixture


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
        "status_property_setter",
        [
            pytest.param(
                functools.partial(_set_property, property_name="open_access"),
                id="works_when_open_access_property_changes",
            ),
        ],
    )
    def test_licensepool_status_change(
        self,
        db: DatabaseTransactionFixture,
        work_queue_indexing: WorkQueueIndexingFixture,
        status_property_setter: Callable[..., None],
    ):
        work = db.work(with_license_pool=True)
        [pool] = work.license_pools

        # Change the field
        status_property_setter(pool, True)
        assert work_queue_indexing.is_queued(work, clear=True)

        # Then verify that if the field is 'set' to its existing value, this doesn't happen.
        status_property_setter(pool, True)
        assert not work_queue_indexing.is_queued(work, clear=True)

    def test_work_suppressed_for_library(
        self,
        db: DatabaseTransactionFixture,
        work_queue_indexing: WorkQueueIndexingFixture,
    ):
        work = db.work(with_license_pool=True)
        library = db.library()

        # Suppress the work for the library
        work.suppressed_for.append(library)
        assert work_queue_indexing.is_queued(work, clear=True)

        # Unsuppress the work for the library
        work.suppressed_for.remove(library)
        assert work_queue_indexing.is_queued(work, clear=True)


class TestEquivalencyDirtyListeners:
    def test_create_marks_identifiers_dirty(
        self, db: DatabaseTransactionFixture, redis_fixture: RedisFixture
    ) -> None:
        a = db.identifier()
        b = db.identifier()

        dirty = DirtyIdentifierIds(redis_fixture.client)
        dirty.pop(1000)  # clear anything left from setup

        db.session.add(Equivalency(input_id=a.id, output_id=b.id, strength=1.0))
        db.session.commit()

        # Both linked identifiers are marked dirty on create.
        assert dirty.pop(1000) == {a.id, b.id}

    def test_delete_marks_chain_dirty(
        self, db: DatabaseTransactionFixture, redis_fixture: RedisFixture
    ) -> None:
        a = db.identifier()
        b = db.identifier()
        equivalency = Equivalency(input_id=a.id, output_id=b.id, strength=1.0)
        db.session.add(equivalency)
        db.session.commit()

        dirty = DirtyIdentifierIds(redis_fixture.client)
        dirty.pop(1000)  # clear the IDs pushed by the create listener

        db.session.delete(equivalency)
        db.session.commit()

        # The deleted identifiers (and their cached chain parents) are marked dirty.
        assert {a.id, b.id}.issubset(dirty.pop(1000))

    def test_redis_failure_is_non_fatal(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A Redis outage while marking identifiers dirty must not abort the flush;
        the missed IDs are recovered by the next full refresh."""
        a = db.identifier()
        b = db.identifier()
        caplog.set_level(LogLevel.warning)

        with patch(
            "palace.manager.sqlalchemy.listeners.DirtyIdentifierIds"
        ) as mock_dirty_cls:
            mock_dirty_cls.return_value.add.side_effect = RedisError("redis down")

            equivalency = Equivalency(input_id=a.id, output_id=b.id, strength=1.0)
            db.session.add(equivalency)
            db.session.commit()  # must not raise despite Redis being down

        # The equivalency was still persisted.
        assert db.session.get(Equivalency, equivalency.id) is not None
        assert "Failed to mark dirty identifiers on equivalency create" in caplog.text

    def test_delete_redis_failure_is_non_fatal(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A Redis outage during the delete listener must not abort the flush;
        the delete path is more complex (it does a DB query first) but the
        non-fatal contract must hold for it too."""
        a = db.identifier()
        b = db.identifier()
        equivalency = Equivalency(input_id=a.id, output_id=b.id, strength=1.0)
        db.session.add(equivalency)
        db.session.commit()
        caplog.set_level(LogLevel.warning)

        with patch(
            "palace.manager.sqlalchemy.listeners.DirtyIdentifierIds"
        ) as mock_dirty_cls:
            mock_dirty_cls.return_value.add.side_effect = RedisError("redis down")

            db.session.delete(equivalency)
            db.session.commit()  # must not raise despite Redis being down

        # The equivalency was still deleted.
        assert db.session.get(Equivalency, equivalency.id) is None
        assert "Failed to mark dirty identifiers on equivalency delete" in caplog.text

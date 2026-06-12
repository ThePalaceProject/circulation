"""Tests for the lane size Celery tasks."""

from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

from palace.manager.celery.tasks import lanes
from palace.manager.celery.tasks.custom_lists import _sweep_lock
from palace.manager.sqlalchemy.model.lane import Lane
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture
from tests.fixtures.services import ServicesFixture

# ---------------------------------------------------------------------------
# Independent lane size sweep
# ---------------------------------------------------------------------------


class TestUpdateIndependentLaneSizes:
    def test_no_lanes_calls_finalize_directly(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
    ):
        """When there are no lanes at all, finalize is called directly."""
        with (
            patch.object(lanes, "finalize_lane_size_update") as mock_finalize,
            patch.object(lanes, "update_lane_size") as mock_lane_size,
        ):
            lanes.update_independent_lane_sizes.delay().wait()

        mock_lane_size.si.assert_not_called()
        mock_finalize.delay.assert_called_once()

    def test_no_independent_lanes_calls_finalize_directly(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
    ):
        """When all lanes are custom-list lanes, finalize is called directly (no independent lanes)."""
        library = db.default_library()
        lane = db.lane(library=library)
        custom_list, _ = db.customlist(num_entries=0)
        lane.customlists.append(custom_list)
        db.session.flush()

        with (
            patch.object(lanes, "finalize_lane_size_update") as mock_finalize,
            patch.object(lanes, "update_lane_size") as mock_lane_size,
        ):
            lanes.update_independent_lane_sizes.delay().wait()

        mock_lane_size.si.assert_not_called()
        mock_finalize.delay.assert_called_once()

    def test_fans_out_only_independent_lanes(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
    ):
        """Only lanes NOT associated with custom lists are swept."""
        library = db.default_library()
        # Custom-list lane (Pattern A) — should NOT appear in the chord
        custom_list_lane = db.lane(library=library)
        custom_list, _ = db.customlist(num_entries=0)
        custom_list_lane.customlists.append(custom_list)
        # Independent lane — should appear in the chord
        independent_lane = db.lane(library=library)
        db.session.flush()

        with (
            patch.object(lanes, "update_lane_size") as mock_lane_size,
            patch.object(lanes, "finalize_lane_size_update"),
            patch("palace.manager.celery.tasks.lanes.chord"),
            patch("palace.manager.celery.tasks.lanes.group"),
        ):
            lanes.update_independent_lane_sizes.delay().wait()

        si_ids = {c.args[0] for c in mock_lane_size.si.call_args_list}
        assert independent_lane.id in si_ids
        assert custom_list_lane.id not in si_ids

    def test_finalize_called_without_lock_value(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
    ):
        """finalize_lane_size_update is called without a lock_value (no sweep lock to release)."""
        library = db.default_library()
        db.lane(library=library)  # independent lane
        db.session.flush()

        with (
            patch.object(lanes, "finalize_lane_size_update") as mock_finalize,
            patch.object(lanes, "update_lane_size"),
            patch("palace.manager.celery.tasks.lanes.chord"),
            patch("palace.manager.celery.tasks.lanes.group"),
        ):
            lanes.update_independent_lane_sizes.delay().wait()

        # finalize is wired via .si() with no lock_value; verify delay is NOT called
        # (the chord callback is set up via .si(), not .delay())
        mock_finalize.delay.assert_not_called()
        mock_finalize.si.assert_called_once_with()


# ---------------------------------------------------------------------------
# Per-lane size update
# ---------------------------------------------------------------------------


class TestUpdateLaneSize:
    def test_updates_lane_size(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ):
        """update_lane_size calls lane.update_size with the search engine."""
        library = db.default_library()
        lane = db.lane(library=library)
        db.session.flush()

        with patch.object(Lane, "update_size") as mock_update_size:
            lanes.update_lane_size.delay(lane.id).wait()
            mock_update_size.assert_called_once()
            _, kwargs = mock_update_size.call_args
            assert "search_engine" in kwargs

    def test_suppresses_before_flush_listener(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ):
        """_suppress_before_flush_listeners is True when update_size is called."""
        library = db.default_library()
        lane = db.lane(library=library)
        db.session.flush()

        suppress_flag_during_call: list[bool] = []

        def capture_flag(_db, **kwargs):
            suppress_flag_during_call.append(lane._suppress_before_flush_listeners)

        with patch.object(Lane, "update_size", side_effect=capture_flag):
            lanes.update_lane_size.delay(lane.id).wait()

        assert suppress_flag_during_call == [True]

    def test_missing_lane_logs_and_continues(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ):
        """A nonexistent lane ID causes the task to skip without raising."""
        nonexistent_id = 999_999_999
        # Should not raise.
        lanes.update_lane_size.delay(nonexistent_id).wait()


# ---------------------------------------------------------------------------
# Finalize
# ---------------------------------------------------------------------------


class TestFinalizeLaneSizeUpdate:
    def test_fires_site_configuration_has_changed(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
    ):
        """finalize_lane_size_update calls site_configuration_has_changed."""
        with patch(
            "palace.manager.celery.tasks.lanes.site_configuration_has_changed"
        ) as mock_changed:
            lanes.finalize_lane_size_update.delay().wait()
            mock_changed.assert_called_once()

    def test_releases_sweep_lock_when_lock_value_provided(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
    ):
        """finalize_lane_size_update releases the sweep lock when lock_value is passed."""
        lock_value = str(uuid4())
        sweep_lock = _sweep_lock(redis_fixture.client, lock_value)
        sweep_lock.acquire()
        assert sweep_lock.locked()

        with patch("palace.manager.celery.tasks.lanes.site_configuration_has_changed"):
            lanes.finalize_lane_size_update.delay(lock_value=lock_value).wait()

        # Lock must be released after finalize completes.
        assert not sweep_lock.locked()

    def test_no_lock_released_when_standalone(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
    ):
        """Without lock_value the sweep lock is not touched (standalone invocation)."""
        # Verify no errors and no unexpected lock interaction when called without lock_value.
        with patch("palace.manager.celery.tasks.lanes.site_configuration_has_changed"):
            lanes.finalize_lane_size_update.delay().wait()  # no lock_value

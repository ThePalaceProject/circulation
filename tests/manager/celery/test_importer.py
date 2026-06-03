"""Tests for palace.manager.celery.importer."""

from __future__ import annotations

from typing import cast
from unittest.mock import MagicMock
from uuid import uuid4

from palace.manager.celery.importer import (
    import_workflow_key,
    import_workflow_lock,
    reap_workflow_lock,
    workflow_lock_guard,
)
from palace.manager.celery.task import Task
from tests.fixtures.redis import RedisFixture


class TestImportWorkflowKey:
    def test_key_structure(self) -> None:
        """Key is prefixed with ImportCollectionWorkflow and scoped to the collection."""
        key = import_workflow_key(collection_id=42)
        assert key == ["ImportCollectionWorkflow", "Collection::42"]

    def test_different_collection_ids_produce_distinct_keys(self) -> None:
        assert import_workflow_key(1) != import_workflow_key(2)


def _guard_task(redis_fixture: RedisFixture, task_id: str = "task-id") -> MagicMock:
    """A mock bound task wired so the guard's redis client and request.id work.

    The workflow lock is keyed on ``task.request.id``, which Celery keeps stable across
    ``task.replace()`` hand-offs and retries; the tests vary it to model separate runs.
    """
    task = MagicMock()
    task.services.redis.return_value.client.return_value = redis_fixture.client
    task.request.id = task_id
    task.autoretry_for = ()
    return task


class TestWorkflowLockGuard:
    def test_proceeds_and_releases_when_lock_free(
        self, redis_fixture: RedisFixture
    ) -> None:
        """A free lock: the guard acquires it (keyed on the task id), proceeds, and
        releases it on exit."""
        task = _guard_task(redis_fixture, "task-a")

        with workflow_lock_guard(cast(Task, task), 1, label="Test") as proceed:
            assert proceed is True
            # The lock is held by this task's id while inside the guard.
            assert import_workflow_lock(redis_fixture.client, 1, "task-a").locked(
                by_us=True
            )

        task.log.warning.assert_not_called()
        # Released on normal exit.
        assert not import_workflow_lock(redis_fixture.client, 1, "any").locked()

    def test_skips_when_another_run_holds_lock(
        self, redis_fixture: RedisFixture
    ) -> None:
        """When a different run (a different task id) holds the lock, the guard does not
        proceed and logs a skip warning."""
        holder = import_workflow_lock(redis_fixture.client, 2, str(uuid4()))
        holder.acquire()
        task = _guard_task(redis_fixture, "task-b")

        with workflow_lock_guard(cast(Task, task), 2, label="Test") as proceed:
            assert proceed is False

        message = task.log.warning.call_args.args[0]
        assert "skipped" in message
        assert "already in progress" in message
        holder.release()

    def test_continuation_reacquires_own_lock(
        self, redis_fixture: RedisFixture
    ) -> None:
        """A continuation (page/batch hand-off or retry) carries the same task id, so it
        re-acquires the lock it already holds and proceeds without warning."""
        task = _guard_task(redis_fixture, "task-c")
        # Simulate the lock still held by this task's own id from a prior page.
        import_workflow_lock(redis_fixture.client, 3, "task-c").acquire()

        with workflow_lock_guard(cast(Task, task), 3, label="Test") as proceed:
            assert proceed is True

        task.log.warning.assert_not_called()

    def test_uses_lock_factory(self, redis_fixture: RedisFixture) -> None:
        """A custom lock_factory (e.g. reap_workflow_lock) is used for acquisition."""
        task = _guard_task(redis_fixture, "task-d")

        with workflow_lock_guard(
            cast(Task, task), 4, label="Test", lock_factory=reap_workflow_lock
        ) as proceed:
            assert proceed is True
            # The reap (not import) workflow lock is the one held.
            assert reap_workflow_lock(redis_fixture.client, 4, "any").locked()
            assert not import_workflow_lock(redis_fixture.client, 4, "any").locked()


class TestImportWorkflowLock:
    def test_acquire_and_release(self, redis_fixture: RedisFixture) -> None:
        lock = import_workflow_lock(redis_fixture.client, 42, str(uuid4()))
        assert not lock.locked()
        lock.acquire()
        assert lock.locked()
        lock.release()
        assert not lock.locked()

    def test_lock_expires_after_two_hours(self, redis_fixture: RedisFixture) -> None:
        """Lock TTL is set to 2 hours so a dead worker cannot block indefinitely."""
        lock = import_workflow_lock(redis_fixture.client, 42, str(uuid4()))
        lock.acquire()
        ttl_ms = redis_fixture.client.pttl(lock.key)
        two_hours_ms = 2 * 60 * 60 * 1000
        assert 0 < ttl_ms <= two_hours_ms

    def test_same_collection_conflicts(self, redis_fixture: RedisFixture) -> None:
        """Two lock instances for the same collection contend on the same key."""
        lock_a = import_workflow_lock(redis_fixture.client, 42, str(uuid4()))
        lock_b = import_workflow_lock(redis_fixture.client, 42, str(uuid4()))
        lock_a.acquire()
        assert not lock_b.acquire()
        lock_a.release()
        assert lock_b.acquire()
        lock_b.release()

    def test_different_collections_do_not_conflict(
        self, redis_fixture: RedisFixture
    ) -> None:
        """Locks for different collections are fully independent."""
        lock_1 = import_workflow_lock(redis_fixture.client, 1, str(uuid4()))
        lock_2 = import_workflow_lock(redis_fixture.client, 2, str(uuid4()))
        lock_1.acquire()
        assert not lock_2.locked()
        assert lock_2.acquire()
        lock_1.release()
        lock_2.release()

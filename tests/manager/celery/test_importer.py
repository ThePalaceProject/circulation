"""Tests for palace.manager.celery.importer."""

from __future__ import annotations

from uuid import uuid4

from palace.manager.celery.importer import import_workflow_key, import_workflow_lock
from tests.fixtures.redis import RedisFixture


class TestImportWorkflowKey:
    def test_key_structure(self) -> None:
        """Key is prefixed with ImportCollectionWorkflow and scoped to the collection."""
        key = import_workflow_key(collection_id=42)
        assert key == ["ImportCollectionWorkflow", "Collection::42"]

    def test_different_collection_ids_produce_distinct_keys(self) -> None:
        assert import_workflow_key(1) != import_workflow_key(2)


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

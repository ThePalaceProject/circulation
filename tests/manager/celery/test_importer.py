"""Tests for palace.manager.celery.importer."""

from __future__ import annotations

from uuid import uuid4

from palace.manager.celery.importer import import_workflow_key, import_workflow_lock
from tests.fixtures.redis import RedisFixture


class TestImportWorkflowKey:
    def test_without_workflow_name(self) -> None:
        """Key uses the legacy two-part format when no workflow_name is given."""
        key = import_workflow_key(collection_id=42)
        assert key == ["ImportCollectionWorkflow", "Collection::42"]

    def test_with_workflow_name(self) -> None:
        """workflow_name is inserted between the prefix and the collection segment."""
        key = import_workflow_key(collection_id=42, workflow_name="EventImport")
        assert key == ["ImportCollectionWorkflow", "EventImport", "Collection::42"]

    def test_different_workflow_names_produce_distinct_keys(self) -> None:
        key_a = import_workflow_key(42, workflow_name="EventImport")
        key_b = import_workflow_key(42, workflow_name="PurchaseMonitor")
        assert key_a != key_b

    def test_different_collection_ids_produce_distinct_keys(self) -> None:
        key_a = import_workflow_key(1, workflow_name="EventImport")
        key_b = import_workflow_key(2, workflow_name="EventImport")
        assert key_a != key_b

    def test_named_workflow_does_not_collide_with_unnamed(self) -> None:
        """A named workflow key never collides with the legacy unnamed key."""
        key_named = import_workflow_key(42, workflow_name="EventImport")
        key_unnamed = import_workflow_key(42)
        assert key_named != key_unnamed


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

    def test_different_workflow_names_do_not_conflict(
        self, redis_fixture: RedisFixture
    ) -> None:
        """Locks with distinct workflow names on the same collection are independent."""
        lock_a = import_workflow_lock(
            redis_fixture.client, 42, str(uuid4()), workflow_name="EventImport"
        )
        lock_b = import_workflow_lock(
            redis_fixture.client, 42, str(uuid4()), workflow_name="PurchaseMonitor"
        )
        lock_a.acquire()
        assert lock_a.locked()
        assert not lock_b.locked()
        assert lock_b.acquire()
        lock_a.release()
        lock_b.release()

    def test_same_workflow_name_conflicts(self, redis_fixture: RedisFixture) -> None:
        """Two lock instances with the same workflow name contend on the same key."""
        lock_a = import_workflow_lock(
            redis_fixture.client, 42, str(uuid4()), workflow_name="EventImport"
        )
        lock_b = import_workflow_lock(
            redis_fixture.client, 42, str(uuid4()), workflow_name="EventImport"
        )
        lock_a.acquire()
        assert not lock_b.acquire()
        lock_a.release()
        assert lock_b.acquire()
        lock_b.release()

    def test_named_workflow_does_not_conflict_with_unnamed(
        self, redis_fixture: RedisFixture
    ) -> None:
        """A named workflow lock doesn't block the legacy unnamed workflow lock."""
        lock_named = import_workflow_lock(
            redis_fixture.client, 42, str(uuid4()), workflow_name="EventImport"
        )
        lock_unnamed = import_workflow_lock(redis_fixture.client, 42, str(uuid4()))
        lock_named.acquire()
        assert not lock_unnamed.locked()
        assert lock_unnamed.acquire()
        lock_named.release()
        lock_unnamed.release()

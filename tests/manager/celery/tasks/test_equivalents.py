from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select

from palace.manager.celery.tasks.equivalents import equivalent_identifiers_refresh
from palace.manager.service.redis.models.dirty_identifiers import DirtyIdentifierIds
from palace.manager.service.redis.models.lock import LockNotAcquired, TaskLock
from palace.manager.sqlalchemy.model.identifier import (
    Equivalency,
    RecursiveEquivalencyCache,
)
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture


def _cache_for(session, parent_id: int) -> set[int]:
    rows = (
        session.execute(
            select(RecursiveEquivalencyCache.identifier_id).where(
                RecursiveEquivalencyCache.parent_identifier_id == parent_id
            )
        )
        .scalars()
        .all()
    )
    return set(rows)


def _drop_cache(session) -> None:
    session.query(RecursiveEquivalencyCache).delete()
    session.commit()


class TestEquivalentIdentifiersRefresh:
    def test_processes_dirty_queue(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        a = db.identifier()
        b = db.identifier()
        db.session.add(Equivalency(input_id=a.id, output_id=b.id, strength=1.0))
        db.session.commit()
        _drop_cache(db.session)

        dirty = DirtyIdentifierIds(redis_fixture.client)
        dirty.add(a.id, b.id)

        equivalent_identifiers_refresh.delay().wait()

        assert _cache_for(db.session, a.id) == {a.id, b.id}
        assert _cache_for(db.session, b.id) == {a.id, b.id}
        assert dirty.count() == 0

    def test_empty_queue_adds_identity_equivalents(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        a = db.identifier()
        db.session.commit()
        _drop_cache(db.session)

        # Queue is empty — task should add (id, id) self-references.
        equivalent_identifiers_refresh.delay().wait()

        assert _cache_for(db.session, a.id) == {a.id}

    def test_processes_in_batches(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        # Create three identifiers each in a separate equivalency chain.
        a = db.identifier()
        b = db.identifier()
        c = db.identifier()
        db.session.add(Equivalency(input_id=a.id, output_id=b.id, strength=1.0))
        db.session.add(Equivalency(input_id=b.id, output_id=c.id, strength=1.0))
        db.session.commit()
        _drop_cache(db.session)

        dirty = DirtyIdentifierIds(redis_fixture.client)
        dirty.add(a.id, b.id, c.id)

        # Use batch_size=1 to force multiple task replacements.
        equivalent_identifiers_refresh.delay(batch_size=1).wait()

        # All chains should be computed despite multiple re-queues.
        # a, b, c are all connected, so their chains should each include
        # all three identifiers.
        assert {a.id, b.id, c.id}.issubset(_cache_for(db.session, a.id))
        assert dirty.count() == 0

    def test_full_refresh(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        a = db.identifier()
        b = db.identifier()
        db.session.add(Equivalency(input_id=a.id, output_id=b.id, strength=1.0))
        db.session.commit()
        _drop_cache(db.session)

        # Clear any IDs pushed by the equivalency-creation listener, so we can
        # verify that full_refresh=True is what re-seeds the queue from the DB.
        dirty = DirtyIdentifierIds(redis_fixture.client)
        dirty.pop(100)
        assert dirty.count() == 0

        equivalent_identifiers_refresh.delay(full_refresh=True).wait()

        assert _cache_for(db.session, a.id) == {a.id, b.id}
        assert _cache_for(db.session, b.id) == {a.id, b.id}

    def test_skips_when_lock_held(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """When another run already holds the task lock, the refresh raises
        LockNotAcquired (declared in the task's ``throws``) and leaves the dirty
        queue untouched."""
        a = db.identifier()
        b = db.identifier()
        db.session.add(Equivalency(input_id=a.id, output_id=b.id, strength=1.0))
        db.session.commit()
        _drop_cache(db.session)

        dirty = DirtyIdentifierIds(redis_fixture.client)
        dirty.add(a.id, b.id)

        # Simulate a concurrent run holding the lock by acquiring it with a
        # different owner (root_id), using the same task-name-derived key.
        held_task = MagicMock()
        held_task.request.root_id = "other-run"
        held_task.name = equivalent_identifiers_refresh.name
        held_lock = TaskLock(held_task, redis_client=redis_fixture.client)
        held_lock.acquire()

        with pytest.raises(LockNotAcquired):
            equivalent_identifiers_refresh.delay().wait()

        # The queue was left untouched and no chains were computed.
        assert dirty.count() == 2
        assert _cache_for(db.session, a.id) == set()

        held_lock.release()

    def test_requeues_batch_on_processing_failure(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """If processing a popped batch fails, the IDs are returned to the dirty
        queue rather than being silently lost until the next full refresh."""
        a = db.identifier()
        b = db.identifier()
        db.session.add(Equivalency(input_id=a.id, output_id=b.id, strength=1.0))
        db.session.commit()
        _drop_cache(db.session)

        dirty = DirtyIdentifierIds(redis_fixture.client)
        # Clear listener-added IDs, then seed a known batch.
        dirty.pop(100)
        dirty.add(a.id, b.id)

        with patch(
            "palace.manager.celery.tasks.equivalents.process_identifier_ids",
            side_effect=RuntimeError("boom"),
        ):
            with pytest.raises(RuntimeError, match="boom"):
                equivalent_identifiers_refresh.delay().wait()

        # The popped batch was put back so the next run can retry it.
        assert dirty.pop(100) == {a.id, b.id}

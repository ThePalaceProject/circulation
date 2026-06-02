from sqlalchemy import select

from palace.manager.celery.tasks.equivalents import equivalent_identifiers_refresh
from palace.manager.service.redis.models.dirty_identifiers import DirtyIdentifierIds
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

from palace.manager.service.redis.models.dirty_identifiers import DirtyIdentifierIds
from palace.manager.sqlalchemy.model.identifier import Equivalency
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture


class TestDirtyIdentifierIds:
    def test_add_and_count(self, redis_fixture: RedisFixture) -> None:
        dirty = DirtyIdentifierIds(redis_fixture.client)
        assert dirty.count() == 0

        added = dirty.add(1, 2, 3)
        assert added == 3
        assert dirty.count() == 3

        # Duplicate IDs are deduplicated.
        added = dirty.add(2, 3, 4)
        assert added == 1
        assert dirty.count() == 4

    def test_add_empty(self, redis_fixture: RedisFixture) -> None:
        dirty = DirtyIdentifierIds(redis_fixture.client)
        assert dirty.add() == 0
        assert dirty.count() == 0

    def test_pop(self, redis_fixture: RedisFixture) -> None:
        dirty = DirtyIdentifierIds(redis_fixture.client)
        dirty.add(10, 20, 30, 40, 50)

        popped = dirty.pop(3)
        assert len(popped) == 3
        assert popped.issubset({10, 20, 30, 40, 50})
        assert dirty.count() == 2

        # Pop returns a frozenset of ints.
        assert all(isinstance(i, int) for i in popped)

    def test_pop_more_than_available(self, redis_fixture: RedisFixture) -> None:
        dirty = DirtyIdentifierIds(redis_fixture.client)
        dirty.add(1, 2)

        popped = dirty.pop(100)
        assert popped == {1, 2}
        assert dirty.count() == 0

    def test_pop_empty(self, redis_fixture: RedisFixture) -> None:
        dirty = DirtyIdentifierIds(redis_fixture.client)
        assert dirty.pop(10) == frozenset()

    def test_add_all_from_db(
        self, redis_fixture: RedisFixture, db: DatabaseTransactionFixture
    ) -> None:
        dirty = DirtyIdentifierIds(redis_fixture.client)

        # Create identifiers and equivalencies.
        id1 = db.identifier()
        id2 = db.identifier()
        id3 = db.identifier()
        # id4 has no equivalency — should not be pushed.
        id4 = db.identifier()

        db.session.add(Equivalency(input_id=id1.id, output_id=id2.id, strength=1.0))
        db.session.add(Equivalency(input_id=id2.id, output_id=id3.id, strength=1.0))
        db.session.flush()

        count = dirty.add_all_from_db(db.session, chunk_size=2)

        # id1, id2, id3 should be pushed; id4 should not.
        assert count == 3
        assert dirty.count() == 3
        all_ids = dirty.pop(100)
        assert id1.id in all_ids
        assert id2.id in all_ids
        assert id3.id in all_ids
        assert id4.id not in all_ids

    def test_add_all_from_db_empty(
        self, redis_fixture: RedisFixture, db: DatabaseTransactionFixture
    ) -> None:
        dirty = DirtyIdentifierIds(redis_fixture.client)
        count = dirty.add_all_from_db(db.session)
        assert count == 0
        assert dirty.count() == 0

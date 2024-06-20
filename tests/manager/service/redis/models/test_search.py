import pytest

from palace.manager.service.logging.configuration import LogLevel
from palace.manager.service.redis.models.search import WaitingForIndexing
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture


class WaitingForIndexingFixture:
    def __init__(self, redis_fixture: RedisFixture):
        self.redis_fixture = redis_fixture
        self.waiting = WaitingForIndexing(self.redis_fixture.client)

    def pop(self, size: int) -> set[int]:
        return set(self.waiting.pop(size))

    def get(self, size: int) -> set[int]:
        return set(self.waiting.get(size))


@pytest.fixture
def waiting_for_indexing_fixture(redis_fixture: RedisFixture):
    return WaitingForIndexingFixture(redis_fixture)


class TestIndexingNeededSet:
    def test___init__(self, waiting_for_indexing_fixture: WaitingForIndexingFixture):
        assert waiting_for_indexing_fixture.waiting._key.endswith(
            "Search::WaitingForIndexing"
        )

    def test_add(
        self,
        waiting_for_indexing_fixture: WaitingForIndexingFixture,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        # Adding a work that is not currently in the set should return True
        assert waiting_for_indexing_fixture.waiting.add(1) is True

        # Adding a work that is already in the set should return False
        assert waiting_for_indexing_fixture.waiting.add(1) is False

        # Adding a work that is None should return False
        caplog.set_level(LogLevel.warning)
        assert waiting_for_indexing_fixture.waiting.add(None) is False
        assert "Attempted to add None to WaitingForIndexing." in caplog.text

        # You can also add a Work object directly
        work = db.work()
        assert waiting_for_indexing_fixture.waiting.add(work) is True

        assert waiting_for_indexing_fixture.get(2) == {1, work.id}

    def test_pop(
        self,
        waiting_for_indexing_fixture: WaitingForIndexingFixture,
        redis_fixture: RedisFixture,
    ):
        # If the key does not exist, we should return an empty list
        redis_fixture.client.delete(waiting_for_indexing_fixture.waiting._key)
        assert waiting_for_indexing_fixture.waiting.pop(2) == []

        # Add some works to the set
        waiting_for_indexing_fixture.waiting.add(1)
        waiting_for_indexing_fixture.waiting.add(2)
        waiting_for_indexing_fixture.waiting.add(3)

        # Pop a single work from the set
        popped_1 = waiting_for_indexing_fixture.pop(1)
        assert len(popped_1) == 1

        # Pop two more works from the set
        popped_2 = waiting_for_indexing_fixture.pop(3)
        assert len(popped_2) == 2
        assert popped_1 | popped_2 == {1, 2, 3}

        # Pop from an empty set should return an empty list
        assert waiting_for_indexing_fixture.waiting.pop(2) == []

    def test_get(
        self,
        waiting_for_indexing_fixture: WaitingForIndexingFixture,
        db: DatabaseTransactionFixture,
    ):
        # Get from an empty set should return an empty list
        assert waiting_for_indexing_fixture.waiting.get(2) == []

        # Add some works to the set
        waiting_for_indexing_fixture.waiting.add(1)
        waiting_for_indexing_fixture.waiting.add(2)
        waiting_for_indexing_fixture.waiting.add(3)

        # Get a single work from the set
        members = waiting_for_indexing_fixture.get(1)
        assert len(members) == 1

        # Get the works from the set
        members = waiting_for_indexing_fixture.get(12)
        assert len(members) == 3
        assert members == {1, 2, 3}

    def test_remove(self, waiting_for_indexing_fixture: WaitingForIndexingFixture):
        # Remove from an empty set should do nothing
        waiting_for_indexing_fixture.waiting.remove([])

        # Removing works that are not in the set should do nothing
        waiting_for_indexing_fixture.waiting.remove([1, 2, 3])

        # add some works to the set
        waiting_for_indexing_fixture.waiting.add(1)
        waiting_for_indexing_fixture.waiting.add(2)
        waiting_for_indexing_fixture.waiting.add(3)

        # Remove a single work from the set
        waiting_for_indexing_fixture.waiting.remove([1])
        assert waiting_for_indexing_fixture.get(3) == {2, 3}

        # Remove the works from the set
        waiting_for_indexing_fixture.waiting.remove([2, 3, 10])
        assert waiting_for_indexing_fixture.get(3) == set()

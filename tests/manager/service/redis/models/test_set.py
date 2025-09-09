from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from kombu.utils.json import dumps, loads

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.service.redis.models.set import IdentifierSet
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture


class TestIdentifierSet:
    def test_set(
        self, redis_fixture: RedisFixture, db: DatabaseTransactionFixture
    ) -> None:
        client = redis_fixture.client
        identifier_set = IdentifierSet(client)

        # Before adding any identifiers, the set should be empty, and
        # not exist in Redis
        assert identifier_set.len() == 0
        assert identifier_set.get() == set()
        assert identifier_set.exists() is False

        identifier1 = IdentifierData.from_identifier(db.identifier())
        identifier2 = db.identifier()

        assert identifier1 not in identifier_set
        assert identifier2 not in identifier_set

        # Add an Identifier and an IdentifierData object
        assert identifier_set.add(identifier1, identifier2) == 2

        assert identifier1 in identifier_set
        assert identifier2 in identifier_set

        # Check that the identifiers were added
        assert identifier_set.len() == 2
        assert identifier_set.get() == {
            identifier1,
            IdentifierData.from_identifier(identifier2),
        }

        # We set an expiration time on the set
        assert client.ttl(identifier_set._key) > 0

        # We can remove identifiers from the set (including identifiers that are not in the set)
        assert identifier_set.remove(identifier2, db.identifier()) == 1
        assert identifier_set.len() == 1
        assert identifier_set.get() == {identifier1}

        # We can add the same identifier again, but it won't be added again
        assert identifier_set.add(identifier1) == 0
        assert identifier_set.len() == 1
        assert identifier_set.get() == {identifier1}

        # We can delete the set
        assert identifier_set.exists() is True
        assert identifier_set.delete() is True
        assert identifier_set.exists() is False
        assert identifier_set.len() == 0
        assert identifier_set.get() == set()
        assert client.exists(identifier_set._key) == 0
        assert identifier_set.delete() is False

        # Calling remove on an empty set should return 0
        assert identifier_set.remove(identifier1) == 0

    def test_pop(self, redis_fixture: RedisFixture) -> None:
        client = redis_fixture.client
        identifier_set = IdentifierSet(client)

        identifiers = {
            IdentifierData(type="test", identifier=str(i)) for i in range(10)
        }

        # Add identifiers to the set
        assert identifier_set.add(*identifiers) == 10

        # Pop 5 identifiers from the set
        popped_identifiers = identifier_set.pop(5)
        assert len(popped_identifiers) == 5
        assert identifier_set.len() == 5
        assert identifier_set.get() == set(identifiers) - set(popped_identifiers)

        popped_identifiers_2 = identifier_set.pop(10)
        assert len(popped_identifiers_2) == 5
        assert identifier_set.len() == 0

        # Check that the popped equal the original identifiers
        assert identifiers == popped_identifiers | popped_identifiers_2

        # We can pop from an empty set
        assert identifier_set.pop(5) == set()
        assert identifier_set.len() == 0

    def test__json__(self, redis_fixture: RedisFixture) -> None:
        client = redis_fixture.client
        test_identifier = IdentifierData(type="test", identifier="test_identifier")

        identifier_set = IdentifierSet(client, expire_time=timedelta(days=4))
        identifier_set.add(test_identifier)

        dumped = dumps(identifier_set)
        recreated_identifier_set = IdentifierSet(redis_fixture.client, **loads(dumped))

        assert recreated_identifier_set._key == identifier_set._key
        assert recreated_identifier_set.expire_time == identifier_set.expire_time
        assert recreated_identifier_set.get() == {test_identifier}

    def test_iteration(self, redis_fixture: RedisFixture) -> None:
        client = redis_fixture.client
        identifier_set = IdentifierSet(client)

        identifiers = {
            IdentifierData(type="test", identifier=str(i)) for i in range(200)
        }

        # Add identifiers to the set
        assert identifier_set.add(*identifiers) == 200

        iterated_identifiers = set()

        # Check that we can iterate over the set
        for identifier in identifier_set:
            assert identifier in identifiers
            iterated_identifiers.add(identifier)

        # Make sure we iterated over all the identifiers
        assert iterated_identifiers == identifiers

    def test_repr(self, redis_fixture: RedisFixture) -> None:
        client = redis_fixture.client
        identifier_set = IdentifierSet(client)

        # Check that the repr is correct
        assert repr(identifier_set) == f"IdentifierSet(set())"

    def test_diff(self, redis_fixture: RedisFixture) -> None:
        client = redis_fixture.client
        identifier_set1 = IdentifierSet(client)
        identifier_set2 = IdentifierSet(client)

        identifiers1 = {
            IdentifierData(type="test", identifier=str(i)) for i in range(10)
        }
        identifiers2 = {
            IdentifierData(type="test", identifier=str(i)) for i in range(5, 15)
        }

        # Add identifiers to the sets
        assert identifier_set1.add(*identifiers1) == 10
        assert identifier_set2.add(*identifiers2) == 10

        # Check the difference between the two sets
        diff = identifier_set1 - identifier_set2
        assert len(diff) == 5
        assert diff == identifiers1 - identifiers2

        # We get an error if we try to diff sets with different clients
        identifier_set3 = IdentifierSet(MagicMock())
        with pytest.raises(
            PalaceValueError,
            match="Cannot subtract IdentifierSets from different Redis clients.",
        ):
            identifier_set1 - identifier_set3

        # We can also diff a set with a normal set
        assert identifier_set1 - identifiers2 == identifiers1 - identifiers2
        assert identifiers2 - identifier_set1 == identifiers2 - identifiers1

    def test_add_none(
        self, redis_fixture: RedisFixture, db: DatabaseTransactionFixture
    ) -> None:
        client = redis_fixture.client
        identifier_set = IdentifierSet(client)

        # Adding no identifiers should return 0 and not raise an error
        assert identifier_set.add() == 0
        assert identifier_set.exists() is False

        # Add an identifier, so that the set exists
        identifier = IdentifierData.from_identifier(db.identifier())
        identifier_set.add(identifier)
        assert identifier_set.exists() is True

        # Reset the expiration time, so we can test that adding nothing to the set extends its expiration
        client.expire(identifier_set._key, 30)
        assert 0 < client.ttl(identifier_set._key) <= 30

        # Adding no identifiers should extend the expiration time
        assert identifier_set.add() == 0
        assert identifier_set.exists() is True
        assert (
            identifier_set.expire_time.total_seconds()
            >= client.ttl(identifier_set._key)
            > 30
        )

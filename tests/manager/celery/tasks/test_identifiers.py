from typing import Literal
from unittest.mock import MagicMock, patch

import pytest
from celery import shared_task

from palace.manager.celery.task import Task
from palace.manager.celery.tasks import identifiers
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.service.redis.models.set import IdentifierSet, RedisSetKwargs
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.licensing import LicensePool
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture


class IdentifierTasksFixture:
    def __init__(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        self._db = db
        self._celery = celery_fixture
        self._redis = redis_fixture
        self.redis_client = self._redis.client

    def license_pools(
        self, collection: Collection, count: int = 10
    ) -> list[LicensePool]:
        return [
            self._db.licensepool(edition=None, collection=collection)
            for _ in range(count)
        ]

    @staticmethod
    def identifiers(licensepools: list[LicensePool]) -> set[IdentifierData]:
        return {IdentifierData.from_identifier(lp.identifier) for lp in licensepools}

    def set_from_response(self, response: RedisSetKwargs) -> IdentifierSet:
        return IdentifierSet(
            self.redis_client,
            **response,
        )

    @staticmethod
    def identifiers_from_mock_calls(mock: MagicMock) -> set[IdentifierData]:
        """
        Get the identifiers from the mock calls.
        """
        return {
            IdentifierData.from_identifier(
                call.kwargs["circulation"].primary_identifier_data
            )
            for call in mock.call_args_list
        }


@pytest.fixture
def identifier_tasks_fixture(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
) -> IdentifierTasksFixture:
    return IdentifierTasksFixture(
        db=db,
        celery_fixture=celery_fixture,
        redis_fixture=redis_fixture,
    )


class TestExistingAvailableIdentifiers:
    def test_normal_run(
        self,
        db: DatabaseTransactionFixture,
        identifier_tasks_fixture: IdentifierTasksFixture,
    ) -> None:
        collection = db.collection()
        license_pools = identifier_tasks_fixture.license_pools(
            collection=collection, count=10
        )

        # Some decoy license pools in a different collection, to make sure we are
        # filtering the license pools by collection properly.
        other_collection = db.collection()
        identifier_tasks_fixture.license_pools(collection=other_collection, count=5)

        response = identifiers.existing_available_identifiers.delay(
            collection.id
        ).wait()
        identifier_set = identifier_tasks_fixture.set_from_response(response)

        assert len(identifier_set) == len(license_pools)
        assert identifier_set.get() == identifier_tasks_fixture.identifiers(
            license_pools
        )

    def test_cleanup_on_exception(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """
        Make sure that if an exception is raised, the identifier set is deleted.
        """

        with (
            patch.object(identifiers, "select", side_effect=PalaceValueError("Bang")),
            pytest.raises(PalaceValueError, match="Bang"),
        ):
            identifiers.existing_available_identifiers.delay(db.collection().id).wait()

        # Check that the identifier set was deleted
        assert redis_fixture.keys() == []


class TestMarkIdentifiersUnavailable:
    def test_normal_run(
        self,
        db: DatabaseTransactionFixture,
        identifier_tasks_fixture: IdentifierTasksFixture,
    ) -> None:
        collection = db.collection()
        license_pools = identifier_tasks_fixture.license_pools(
            collection=collection, count=10
        )

        existing_set = IdentifierSet(identifier_tasks_fixture.redis_client)
        existing_set.add(*identifier_tasks_fixture.identifiers(license_pools))

        active_set = IdentifierSet(identifier_tasks_fixture.redis_client)
        active_set.add(*identifier_tasks_fixture.identifiers(license_pools[:5]))

        expected_marked = existing_set - active_set

        with patch.object(identifiers, "circulation_apply") as mock_apply:
            result = identifiers.mark_identifiers_unavailable.delay(
                [existing_set, active_set],
                collection_id=collection.id,
            ).wait()

        assert result is True
        assert mock_apply.delay.call_count == 5
        assert (
            identifier_tasks_fixture.identifiers_from_mock_calls(mock_apply.delay)
            == expected_marked
        )

        # Check that we didn't leave any redis keys behind
        assert existing_set.exists() is False
        assert active_set.exists() is False

    def test_run_with_nonexistent_sets(
        self,
        db: DatabaseTransactionFixture,
        identifier_tasks_fixture: IdentifierTasksFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(LogLevel.info)

        collection = db.collection()
        existing_set = IdentifierSet(identifier_tasks_fixture.redis_client)
        active_set = IdentifierSet(identifier_tasks_fixture.redis_client)

        # If the existing set doesn't exist, or doesn't contain any identifiers
        # (which are treated the same in redis), we exit early and log a message.
        result = identifiers.mark_identifiers_unavailable.delay(
            [existing_set, active_set],
            collection_id=collection.id,
        ).wait()
        assert result is False
        assert "Existing identifiers set does not exist in Redis" in caplog.text

        # Add an identifier to the existing set, so it now exists
        existing_set.add(IdentifierData.from_identifier(db.identifier()))

        # If the active set doesn't exist, we raise an error instead of deleting every
        # identifier in the existing set.
        with pytest.raises(
            PalaceValueError, match="Active identifiers set does not exist in Redis"
        ):
            identifiers.mark_identifiers_unavailable.delay(
                [existing_set, active_set],
                collection_id=collection.id,
            ).wait()

        # In the error case we still clean up the existing set
        assert existing_set.exists() is False

    @pytest.mark.parametrize(
        "existing,active",
        [
            pytest.param(None, None, id="both_none"),
            pytest.param(True, None, id="existing_set_none"),
            pytest.param(None, True, id="active_set_none"),
        ],
    )
    def test_run_with_none_for_set(
        self,
        db: DatabaseTransactionFixture,
        identifier_tasks_fixture: IdentifierTasksFixture,
        caplog: pytest.LogCaptureFixture,
        existing: Literal[True] | None,
        active: Literal[True] | None,
    ) -> None:
        def create_set() -> IdentifierSet:
            identifier_set = IdentifierSet(identifier_tasks_fixture.redis_client)
            identifier_set.add(IdentifierData.from_identifier(db.identifier()))
            return identifier_set

        caplog.set_level(LogLevel.warning)

        collection = db.collection()
        existing_set = create_set() if existing else None
        active_set = create_set() if active else None

        result = identifiers.mark_identifiers_unavailable.delay(
            [existing_set, active_set],
            collection_id=collection.id,
        ).wait()
        assert result is False
        assert "Aborting without marking any identifiers as unavailable" in caplog.text

        # Any non-None set should be cleaned up
        if existing_set is not None:
            assert existing_set.exists() is False
        if active_set is not None:
            assert active_set.exists() is False


@shared_task(bind=True)
def identifiers_test_task(
    task: Task,
    identifiers: RedisSetKwargs,
    *,
    do_iterations: int | Literal[False] = False,
    iterations: int = 0,
    raise_exception: bool = False,
) -> IdentifierSet:
    redis_client = task.services.redis().client()
    redis_set = IdentifierSet(redis_client, **identifiers)
    if raise_exception:
        redis_set.delete()
        raise PalaceValueError("Kaboom!")
    if do_iterations and iterations < do_iterations:
        iterations += 1
        raise task.replace(
            identifiers_test_task.s(
                identifiers, iterations=iterations, do_iterations=do_iterations
            )
        )
    return redis_set


class TestCreateMarkUnavailableChord:
    @pytest.mark.parametrize(
        "do_iterations",
        [
            pytest.param(False, id="no iterations"),
            pytest.param(10, id="10 iterations"),
        ],
    )
    def test_normal_run(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        identifier_tasks_fixture: IdentifierTasksFixture,
        do_iterations: int | Literal[False],
    ) -> None:
        collection = db.collection()
        license_pools = identifier_tasks_fixture.license_pools(
            collection=collection, count=10
        )

        all_identifiers = identifier_tasks_fixture.identifiers(license_pools)

        active_set = IdentifierSet(identifier_tasks_fixture.redis_client)
        active_set.add(*identifier_tasks_fixture.identifiers(license_pools[:5]))

        expect_to_be_marked = all_identifiers - active_set

        with patch.object(identifiers, "circulation_apply") as mock_apply:
            identifiers.create_mark_unavailable_chord(
                collection.id,
                identifiers_test_task.s(active_set, do_iterations=do_iterations),
            ).delay().wait()

        # We queued tasks to mark the identifiers as unavailable
        assert mock_apply.delay.call_count == 5
        assert (
            identifier_tasks_fixture.identifiers_from_mock_calls(mock_apply.delay)
            == expect_to_be_marked
        )

        # Check that we didn't leave any redis keys behind
        assert redis_fixture.keys() == []

    def test_exception(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        identifier_tasks_fixture: IdentifierTasksFixture,
    ) -> None:
        collection = db.collection()
        license_pools = identifier_tasks_fixture.license_pools(
            collection=collection, count=10
        )

        active_set = IdentifierSet(identifier_tasks_fixture.redis_client)
        active_set.add(*identifier_tasks_fixture.identifiers(license_pools[:5]))

        with patch.object(identifiers, "circulation_apply") as mock_apply:
            identifiers.create_mark_unavailable_chord(
                collection.id, identifiers_test_task.s(active_set, raise_exception=True)
            ).delay().wait(propagate=False)

        # Because one of the tasks raised an exception, we never made it into the mark_identifiers_unavailable
        # task at all
        assert mock_apply.delay.call_count == 0

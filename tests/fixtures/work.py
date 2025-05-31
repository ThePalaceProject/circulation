from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import patch

import pytest

from palace.manager.data_layer.policy.presentation import PresentationCalculationPolicy
from palace.manager.service.redis.models.work import WorkIdAndPolicy
from palace.manager.sqlalchemy.model.work import Work


class WorkIdPolicyQueuePresentationRecalculationFixture:
    """
    In normal operation, when calculate_presentation is called on Work, it
    adds the Work's ID and PresentationCalculationPolicy to a set in Redis.
    This set is then used to determine which
    Works need aspects of their presentations recalculated.

    For testing, we mock this out to just use a Python set. This allows us to
    check whether a Work is queued for indexing without actually needing to
    interact with Redis.
    """

    def __init__(self):
        self.queued_work_Id_and_policy_combinations = set()
        self.patch = patch.object(Work, "queue_presentation_recalculation", self.queue)

    def queue(
        self,
        work_id: int | None,
        policy: PresentationCalculationPolicy | None,
        *,
        redis_client: Any = None,
    ) -> None:
        return self.queued_work_Id_and_policy_combinations.add(
            WorkIdAndPolicy(work_id=work_id, policy=policy)
        )

    def clear(self):
        self.queued_work_Id_and_policy_combinations.clear()

    def disable_fixture(self):
        self.patch.stop()

    def is_queued(self, wp: WorkIdAndPolicy, *, clear: bool = False) -> bool:
        queued = wp in self.queued_work_Id_and_policy_combinations

        if clear:
            self.clear()

        return queued

    def queue_size(self):
        return len(self.queued_work_Id_and_policy_combinations)

    @classmethod
    @contextmanager
    def fixture(cls):
        fixture = cls()
        fixture.patch.start()
        try:
            yield fixture
        finally:
            fixture.patch.stop()


@pytest.fixture(scope="function")
def work_id_policy_queue_presentation_recalculation() -> (
    Generator[WorkIdPolicyQueuePresentationRecalculationFixture]
):
    with WorkIdPolicyQueuePresentationRecalculationFixture.fixture() as fixture:
        yield fixture

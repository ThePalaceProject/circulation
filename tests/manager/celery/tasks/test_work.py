from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.orm.exc import StaleDataError

from palace.manager.celery.tasks import work
from palace.manager.celery.tasks.work import (
    calculate_presentation_for_works,
    calculate_work_presentations,
)
from palace.manager.data_layer.policy.presentation import PresentationCalculationPolicy
from palace.manager.service.redis.models.work import (
    WaitingForPresentationCalculation,
    WorkIdAndPolicy,
)
from palace.manager.sqlalchemy.model.work import Work
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture


@pytest.mark.parametrize(
    "batch_size",
    [
        (1),
        (2),
    ],
)
def test_calculate_work_presentations(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    batch_size: int,
):
    work1 = db.work()
    work2 = db.work()
    policy1 = PresentationCalculationPolicy.recalculate_everything()
    policy2 = PresentationCalculationPolicy.recalculate_presentation_edition()

    with redis_fixture.services_fixture.wired():
        waiting = WaitingForPresentationCalculation(redis_fixture.client)
        wp1 = WorkIdAndPolicy(work_id=work1.id, policy=policy1)
        wp2 = WorkIdAndPolicy(work_id=work2.id, policy=policy2)
        waiting.add(wp1)
        waiting.add(wp2)

        assert waiting.len() == 2

        with patch.object(work, "calculate_presentation_for_works") as calc_for_works:
            calculate_work_presentations.delay(batch_size=batch_size).wait()

            assert waiting.len() == 0
            # with a batch size of one, we expect one invocation of the subtask
            # followed by a task replacement followed by a second invocation of the subtask.
            if batch_size == 1:
                assert calc_for_works.delay.call_count == 2
                cal = calc_for_works.delay.call_args_list
                assert set(cal[0].args[0] + cal[1].args[0]) == {wp1, wp2}
            elif batch_size == 2:
                # with a batch of 2, we expect only one call to the subtask.
                calc_for_works.delay.assert_called_once_with([wp1, wp2])
            else:
                raise Exception("Not expecting a batch size value greater than 2")


def test_calculate_presentation_editions_for_works(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
):
    work1 = db.work()
    policy1 = PresentationCalculationPolicy.recalculate_everything()

    with (
        patch.object(work1, "calculate_presentation") as calc_pres,
        patch.object(Work, "by_id") as by_id,
    ):
        by_id.return_value = work1
        calculate_presentation_for_works.delay(
            work_policies=[WorkIdAndPolicy(work_id=work1.id, policy=policy1)],
            disable_exponential_back_off=True,
        ).wait()
        calc_pres.assert_called_once_with(policy=policy1)


def test_calculate_presentation_editions_for_works_with_retry(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
):
    work1 = db.work()
    policy1 = PresentationCalculationPolicy.recalculate_everything()

    with (
        patch.object(work1, "calculate_presentation") as calc_pres,
        patch.object(Work, "by_id") as by_id,
    ):
        by_id.return_value = work1
        calc_pres.side_effect = [StaleDataError, None]
        calculate_presentation_for_works.delay(
            work_policies=[WorkIdAndPolicy(work_id=work1.id, policy=policy1)],
            disable_exponential_back_off=True,
        ).wait()
        assert calc_pres.call_count == 2


def test_calculate_presentation_for_works_failure(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
):
    work1 = db.work()
    policy1 = PresentationCalculationPolicy.recalculate_everything()

    with (
        patch.object(work1, "calculate_presentation") as calc_pres,
        patch.object(Work, "by_id") as by_id,
    ):
        by_id.return_value = work1
        calc_pres.side_effect = [Exception] * 5

        with pytest.raises(Exception):
            calculate_presentation_for_works.delay(
                work_policies=[WorkIdAndPolicy(work_id=work1.id, policy=policy1)],
                disable_exponential_back_off=True,
            ).wait()

        assert calc_pres.call_count == 5


def test_calculate_presentation_for_works_non_existent_work(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
):
    calculate_presentation = MagicMock()

    Work.calculate_presentation = calculate_presentation

    no_existent_work_id = 666
    work1 = db.work()
    policy1 = PresentationCalculationPolicy.recalculate_everything()

    with (
        patch.object(work1, "calculate_presentation") as calc_pres,
        patch.object(Work, "by_id") as by_id,
    ):
        by_id.side_effect = [None, work1]
        calculate_presentation_for_works.delay(
            work_policies=[
                WorkIdAndPolicy(work_id=no_existent_work_id, policy=policy1),
                WorkIdAndPolicy(work_id=work1.id, policy=policy1),
            ],
            disable_exponential_back_off=True,
        ).wait()
        assert calc_pres.call_count == 1

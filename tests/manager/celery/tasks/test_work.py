from unittest.mock import patch

import pytest

from palace.manager.celery.tasks.work import (
    calculate_work_presentations,
)
from palace.manager.data_layer.policy.presentation import PresentationCalculationPolicy
from palace.manager.service.redis.models.work import (
    WaitingForPresentationCalculation,
    WorkIdAndPolicy,
)
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

        with patch(
            "palace.manager.sqlalchemy.model.work.Work.calculate_presentation"
        ) as calc_presentations:

            calculate_work_presentations.delay(batch_size=batch_size).wait()
            assert waiting.len() == 0
            assert calc_presentations.call_count == 2
            cal = calc_presentations.call_args_list
            assert {cal[0].kwargs["policy"], cal[1].kwargs["policy"]} == {
                policy1,
                policy2,
            }


def test_calculate_presentations_non_existent_work(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
):
    policy1 = PresentationCalculationPolicy.recalculate_everything()
    non_existent_work_id = 777
    with redis_fixture.services_fixture.wired():
        waiting = WaitingForPresentationCalculation(redis_fixture.client)
        wp1 = WorkIdAndPolicy(work_id=non_existent_work_id, policy=policy1)
        waiting.add(wp1)

        with (
            patch(
                "palace.manager.sqlalchemy.model.work.Work.calculate_presentation"
            ) as calc_pres,
        ):
            assert waiting.len() == 1
            calculate_work_presentations.delay().wait()
            assert waiting.len() == 0
            assert calc_pres.call_count == 0

from unittest.mock import patch

import pytest
from sqlalchemy import select

from palace.manager.celery.tasks.work import (
    calculate_work_presentations,
    classify_unchecked_subjects,
    migrate_work_coverage_records,
    paginate_query,
)
from palace.manager.data_layer.policy.presentation import PresentationCalculationPolicy
from palace.manager.service.redis.models.work import (
    WaitingForPresentationCalculation,
    WorkIdAndPolicy,
)
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.coverage import WorkCoverageRecord
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import get_one_or_create
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture
from tests.fixtures.work import WorkIdPolicyQueuePresentationRecalculationFixture


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


def test_migrate_work_coverage_records(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
):
    """ "
    This test will be removed in the next release along with the migrate_work_coverage_records task.
    """
    select_all_work_coverage_records = select(WorkCoverageRecord.work_id)
    rows = db.session.execute(select_all_work_coverage_records).all()
    assert len(rows) == 0

    work1 = db.work()
    get_one_or_create(
        db.session,
        WorkCoverageRecord,
        create_method_kwargs={
            "work_id": work1.id,
            "status": WorkCoverageRecord.REGISTERED,
            "operation": WorkCoverageRecord.CLASSIFY_OPERATION,
            "timestamp": utc_now(),
        },
    )

    rows = db.session.execute(select_all_work_coverage_records).all()
    assert len(rows) == 1
    policy1 = PresentationCalculationPolicy.recalculate_everything()
    with redis_fixture.services_fixture.wired():
        waiting = WaitingForPresentationCalculation(redis_fixture.client)
        assert waiting.len() == 0
        migrate_work_coverage_records.delay().wait()
        assert waiting.len() == 1
        assert waiting.pop(1) == {WorkIdAndPolicy(work_id=work1.id, policy=policy1)}
    rows = db.session.execute(select_all_work_coverage_records).all()
    assert len(rows) == 0


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


def test_paginate(db: DatabaseTransactionFixture):
    """Pagination is changed to be row-wise comparison
    Ensure we are paginating correctly within the same Subject page"""
    subject = db.subject(Subject.AXIS_360_AUDIENCE, "Any")
    works = []
    for i in range(20):
        work: Work = db.work(with_license_pool=True)
        db.classification(
            work.presentation_edition.primary_identifier,
            subject,
            work.license_pools[0].data_source,
        )
        works.append(work)

    for ix, [work] in enumerate(paginate_query(db.session, batch_size=1)):
        # We are coming in via "id" order
        assert work == works[ix]

    assert ix == 19

    other_subject = db.subject(Subject.BISAC, "Any")
    last_work = works[-1]
    db.classification(
        last_work.presentation_edition.primary_identifier,
        other_subject,
        last_work.license_pools[0].data_source,
    )
    next_works = next(paginate_query(db.session, batch_size=100))
    # Works are only iterated over ONCE per loop
    assert len(next_works) == 20

    # A checked subjects work is not included
    not_work = db.work(with_license_pool=True)
    another_subject = db.subject(Subject.DDC, "Any")
    db.classification(
        not_work.presentation_edition.primary_identifier,
        another_subject,
        not_work.license_pools[0].data_source,
    )
    another_subject.checked = True
    db.session.commit()
    next_works = next(paginate_query(db.session, batch_size=100))
    assert len(next_works) == 20
    assert not_work not in next_works


def test_subject_checked(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    work_policy_recalc_fixture: WorkIdPolicyQueuePresentationRecalculationFixture,
):
    subject = db.subject(Subject.AXIS_360_AUDIENCE, "Any")
    assert subject.checked == False

    works = []
    for i in range(10):
        work: Work = db.work(with_license_pool=True)
        db.classification(
            work.presentation_edition.primary_identifier,
            subject,
            work.license_pools[0].data_source,
        )
        works.append(work)

    classify_unchecked_subjects.delay().wait()
    for work in works:
        assert work_policy_recalc_fixture.is_queued(
            work_id=work.id,
            policy=PresentationCalculationPolicy.recalculate_classification(),
        )

    # now verify that no recalculation occurs when the subject.checked property is true.
    work_policy_recalc_fixture.clear()
    subject.checked = True
    classify_unchecked_subjects.delay().wait()
    assert work_policy_recalc_fixture.queue_size() == 0

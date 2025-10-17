from unittest.mock import call, patch

import pytest

from palace.manager.celery.tasks import apply, work as work_tasks
from palace.manager.celery.utils import ModelNotFoundError
from palace.manager.data_layer.policy.presentation import PresentationCalculationPolicy
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.work import Work
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture


def test_calculate_work_presentation(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    caplog: pytest.LogCaptureFixture,
):
    work = db.work()
    policy = PresentationCalculationPolicy.recalculate_everything()
    caplog.set_level(LogLevel.warning)

    # A work with no license pool does have its presentation calculated,
    # but we log a warning and don't attempt to acquire a lock.
    with (
        patch.object(Work, "calculate_presentation") as calc_presentation,
        patch.object(
            work_tasks, "apply_task_lock", wraps=apply.apply_task_lock
        ) as apply_lock,
    ):
        work_tasks.calculate_work_presentation.delay(
            work_id=work.id, policy=policy
        ).wait()
    calc_presentation.assert_called_once_with(
        policy=policy, disable_async_calculation=True
    )
    apply_lock.assert_not_called()
    assert "has no LicensePool" in caplog.text

    # If we have a license pool, we attempt to acquire a lock.
    work = db.work(with_license_pool=True)
    with (
        patch.object(Work, "calculate_presentation") as calc_presentation,
        patch.object(
            work_tasks, "apply_task_lock", wraps=apply.apply_task_lock
        ) as apply_lock,
    ):
        work_tasks.calculate_work_presentation.delay(
            work_id=work.id, policy=policy
        ).wait()
    calc_presentation.assert_called_once_with(
        policy=policy, disable_async_calculation=True
    )
    apply_lock.assert_called_once()


def test_calculate_work_presentation_retry(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
):
    work = db.work()
    policy = PresentationCalculationPolicy.recalculate_everything()

    with (
        patch.object(Work, "calculate_presentation") as calc_presentation,
        celery_fixture.patch_retry_backoff(),
    ):
        calc_presentation.side_effect = [ModelNotFoundError(), None]
        work_tasks.calculate_work_presentation.delay(
            work_id=work.id, policy=policy
        ).wait()
    assert calc_presentation.call_count == 2
    calc_presentation.assert_has_calls(
        [call(policy=policy, disable_async_calculation=True)] * 2
    )


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

    for ix, [work] in enumerate(work_tasks._paginate_query(db.session, batch_size=1)):
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
    next_works = next(work_tasks._paginate_query(db.session, batch_size=100))
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
    next_works = next(work_tasks._paginate_query(db.session, batch_size=100))
    assert len(next_works) == 20
    assert not_work not in next_works


def test_subject_checked(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
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

    work_tasks.classify_unchecked_subjects.delay().wait()
    for work in works:
        assert work_policy_recalc_fixture.is_queued(
            work_id=work.id,
            policy=PresentationCalculationPolicy.recalculate_classification(),
        )

    # now verify that no recalculation occurs when the subject.checked property is true.
    work_policy_recalc_fixture.clear()
    subject.checked = True
    work_tasks.classify_unchecked_subjects.delay().wait()
    assert work_policy_recalc_fixture.queue_size() == 0

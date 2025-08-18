from unittest.mock import patch

from sqlalchemy.exc import SQLAlchemyError

from palace.manager.celery.tasks.work import (
    _paginate_query,
    calculate_work_presentation,
    classify_unchecked_subjects,
)
from palace.manager.data_layer.policy.presentation import PresentationCalculationPolicy
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.work import Work
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.work import WorkIdPolicyQueuePresentationRecalculationFixture


def test_calculate_work_presentation(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
):
    work = db.work()
    policy = PresentationCalculationPolicy.recalculate_everything()

    with patch(
        "palace.manager.sqlalchemy.model.work.Work.calculate_presentation"
    ) as calc_presentations:
        calculate_work_presentation.delay(work_id=work.id, policy=policy).wait()
        assert calc_presentations.call_count == 1
        cal = calc_presentations.call_args_list
        assert cal[0].kwargs["policy"] == policy


def test_calculate_work_presentation_retry(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
):
    work = db.work()
    policy = PresentationCalculationPolicy.recalculate_everything()

    with patch(
        "palace.manager.sqlalchemy.model.work.Work.calculate_presentation"
    ) as calc_presentations:
        calc_presentations.side_effect = [SQLAlchemyError(), None]
        calculate_work_presentation.delay(work_id=work.id, policy=policy).wait()
        assert calc_presentations.call_count == 2
        cal = calc_presentations.call_args_list
        for i in range(0, 2):
            assert cal[i].kwargs["policy"] == policy


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

    for ix, [work] in enumerate(_paginate_query(db.session, batch_size=1)):
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
    next_works = next(_paginate_query(db.session, batch_size=100))
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
    next_works = next(_paginate_query(db.session, batch_size=100))
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

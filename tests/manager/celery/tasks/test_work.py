from unittest.mock import patch

from palace.manager.celery.tasks import work as work_tasks
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.work import Work
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture


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
    """Test that classify_unchecked_subjects recalculates works with unchecked subjects."""
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

    with patch.object(Work, "calculate_presentation") as calc_pres:
        work_tasks.classify_unchecked_subjects.delay().wait()
        # Should have been called once for each work
        assert calc_pres.call_count == 10
        # Should use recalculate_classification policy
        for call_obj in calc_pres.call_args_list:
            policy = call_obj[1]["policy"]
            assert policy.classify is True
            assert policy.choose_edition is False

    # now verify that no recalculation occurs when the subject.checked property is true.
    subject.checked = True
    db.session.commit()
    with patch.object(Work, "calculate_presentation") as calc_pres:
        work_tasks.classify_unchecked_subjects.delay().wait()
        assert calc_pres.call_count == 0

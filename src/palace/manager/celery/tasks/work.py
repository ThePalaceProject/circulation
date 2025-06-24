from __future__ import annotations

from collections.abc import Generator

from celery import shared_task
from sqlalchemy import tuple_
from sqlalchemy.orm import Session, defer

from palace.manager.celery.task import Task
from palace.manager.data_layer.policy.presentation import PresentationCalculationPolicy
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.lock import TaskLock
from palace.manager.service.redis.models.work import (
    WaitingForPresentationCalculation,
)
from palace.manager.sqlalchemy.model.classification import Classification, Subject
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.log import elapsed_time_logging


@shared_task(queue=QueueNames.default, bind=True)
def calculate_work_presentations(
    task: Task,
    batch_size: int = 100,
) -> None:

    with TaskLock(task).lock():
        waiting = WaitingForPresentationCalculation(task.services.redis.client())
        work_policies = waiting.pop(batch_size)

        if work_policies:

            try:
                with (
                    task.session() as session,
                    elapsed_time_logging(
                        log_method=task.log.info,
                        message_prefix=f"Presentation calculated presentation for works: count={len(work_policies)}, "
                        f"remaining={waiting.len()}",
                        skip_start=True,
                    ),
                ):
                    for wp in work_policies:
                        work = get_one(session, Work, id=wp.work_id)
                        if not work:
                            task.log.warning(
                                f"No work with id={wp.work_id}. Skipping..."
                            )
                            continue
                        work.calculate_presentation(policy=wp.policy)
            except Exception as e:
                # if a failure occurs requeue the items so that can be recalculated in the next round
                waiting.add(*work_policies)
                raise e

    if len(work_policies) == batch_size:
        # This task is complete, but there are more works waiting to be recalculated. Requeue ourselves
        # to process the next batch.
        raise task.replace(calculate_work_presentations.s(batch_size=batch_size))

    task.log.info(f"Finished calculating presentation for works.")


@shared_task(queue=QueueNames.default, bind=True)
def classify_unchecked_subjects(task: Task) -> None:
    """Reclassify all Works whose current classifications appear to
    depend on Subjects in the 'unchecked' state.

    This generally means that some migration script reset those
    Subjects because the rules for processing them changed.
    """
    with task.session() as session:
        paged_query = _paginate_query(session, 1000)

        policy = PresentationCalculationPolicy.recalculate_classification()
        while True:
            works = next(paged_query, [])
            if not works:
                break
            for work in works:
                Work.queue_presentation_recalculation(work_id=work.id, policy=policy)


def _unchecked_subjects(_db: Session) -> Generator[Subject]:
    """Yield one unchecked subject at a time"""
    query = _db.query(Subject).filter(Subject.checked == False).order_by(Subject.id)
    last_id = None
    while True:
        qu = query
        if last_id:
            qu = qu.filter(Subject.id > last_id)
        subject = qu.first()

        if not subject:
            return

        last_id = subject.id
        yield subject


def _paginate_query(_db: Session, batch_size: int) -> Generator[list[Work]]:
    """Page this query using the row-wise comparison
    technique unique to this job. We have already ensured
    the ordering of the rows follows all the joined tables"""

    for subject in _unchecked_subjects(_db):
        last_work: Work | None = None  # Last work object of the previous page
        # IDs of the last work, for paging
        work_id, license_id, iden_id, classn_id = (
            None,
            None,
            None,
            None,
        )

        query = (
            _db.query(Work, LicensePool.id, Identifier.id, Classification.id)
            .join(Work.license_pools)
            .join(LicensePool.identifier)
            .join(Identifier.classifications)
            .join(Classification.subject)
        )

        while True:

            # Must order by all joined attributes
            query = (
                query.order_by(None)
                .order_by(
                    Subject.id,
                    Work.id,
                    LicensePool.id,
                    Identifier.id,
                    Classification.id,
                )
                .options(
                    defer(Work.summary_text),
                )
            )
            # We are a "per subject" filter, this is the MOST efficient method
            qu = query.filter(Subject.id == subject.id)
            # # Add the columns we need to page with explicitly in the query
            # qu: Query[Tuple[Work, int, int, int]] = qu.add_columns(LicensePool.id, Identifier.id, Classification.id)
            # We're not on the first page, add the row-wise comparison
            if last_work is not None:
                qu = qu.filter(
                    tuple_(
                        Work.id,
                        LicensePool.id,
                        Identifier.id,
                        Classification.id,
                    )
                    > (work_id, license_id, iden_id, classn_id)
                )

            qu2 = qu.limit(batch_size)
            works = qu2.all()
            if not len(works):
                break

            last_work_row = works[-1]
            last_work = last_work_row[0]
            # set comprehension ensures we get unique works per loop
            # Works will get duplicated in the query because of the addition
            # of the ID columns in the select, it is possible and expected
            # that works will get duplicated across loops. It is not a desired
            # outcome to duplicate works across loops, but the alternative is to maintain
            # the IDs in memory and add a NOT IN operator in the query
            # which would grow quite large, quite fast
            only_works = list({w[0] for w in works})

            yield only_works

            work_id, license_id, iden_id, classn_id = (
                last_work_row[0].id,
                last_work_row[1],
                last_work_row[2],
                last_work_row[3],
            )

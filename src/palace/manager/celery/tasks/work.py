from __future__ import annotations

from celery import shared_task
from sqlalchemy.orm import Session

from palace.manager.celery.task import Task
from palace.manager.data_layer.policy.presentation import PresentationCalculationPolicy
from palace.manager.service.celery.celery import QueueNames
from palace.manager.sqlalchemy.model.classification import Classification, Subject
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work


@shared_task(queue=QueueNames.default, bind=True)
def reclassify_null_audience_works(task: Task) -> None:
    """Reclassify all works whose audience was reset to NULL by a repair migration.

    Iterates works with audience IS NULL in ascending id order and calls
    calculate_presentation() on each, committing after every work so that
    progress is preserved if the task is interrupted.

    TODO: Remove this task and its startup task
    (startup_tasks/2026_05_12_reclassify_fb_misclassified_works.py) once the
    startup task has been run on all deployments.
    """
    with task.session() as session:
        policy = PresentationCalculationPolicy.recalculate_classification()
        last_id: int | None = None
        while True:
            qu = session.query(Work).filter(Work.audience.is_(None)).order_by(Work.id)
            if last_id is not None:
                qu = qu.filter(Work.id > last_id)
            work = qu.first()
            if not work:
                break
            last_id = work.id
            work.calculate_presentation(policy=policy)
            session.commit()


@shared_task(queue=QueueNames.default, bind=True)
def classify_unchecked_subjects(task: Task) -> None:
    """Reclassify all Works whose current classifications appear to
    depend on Subjects in the 'unchecked' state.

    This generally means that some migration script reset those
    Subjects because the rules for processing them changed.
    """
    with task.session() as session:
        # Snapshot all affected work IDs before processing begins.
        # calculate_presentation() calls assign_to_genre() which marks subjects
        # checked=True as a side effect; a live query would silently skip works
        # that share those subjects with an already-processed work.
        work_ids = _work_ids_with_unchecked_subjects(session)
        policy = PresentationCalculationPolicy.recalculate_classification()
        for work_id in work_ids:
            work = session.get(Work, work_id)
            if work is None:
                continue
            work.calculate_presentation(policy=policy)
            session.commit()


def _work_ids_with_unchecked_subjects(session: Session) -> list[int]:
    """Return IDs of all works linked to at least one unchecked subject, ordered by id."""
    rows = (
        session.query(Work.id)
        .join(Work.license_pools)
        .join(LicensePool.identifier)
        .join(Identifier.classifications)
        .join(Classification.subject)
        .filter(Subject.checked == False)
        .distinct()
        .order_by(Work.id)
        .all()
    )
    return [row[0] for row in rows]

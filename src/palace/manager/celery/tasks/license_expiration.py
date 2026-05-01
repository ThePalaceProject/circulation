"""Celery task for processing ODL license expirations."""

from celery import shared_task
from sqlalchemy import or_, select
from sqlalchemy.orm import selectinload

from palace.util.datetime_helpers import utc_now
from palace.util.log import pluralize

from palace.manager.celery.task import Task
from palace.manager.service.celery.celery import QueueNames
from palace.manager.sqlalchemy.model.licensing import License, LicensePool


@shared_task(queue=QueueNames.default, bind=True)
def update_expired_licenses(task: Task) -> None:
    """Find pools with newly-expired licenses and recalculate their availability.

    A pool is considered stale when any of its licenses has an ``expires`` timestamp at or
    before the current time AND the pool's ``last_checked`` predates that expiry — meaning
    the expiry occurred after the last availability calculation and has not yet been
    accounted for. ``update_availability_from_licenses(as_of=now)`` advances
    ``last_checked`` to ``now``, so processed pools are automatically excluded on the next run.
    """
    now = utc_now()

    with task.transaction() as session:
        stale_pool_ids = session.scalars(
            select(License.license_pool_id)
            .join(LicensePool, License.license_pool_id == LicensePool.id)
            .where(License.expires.is_not(None))
            .where(License.expires <= now)
            .where(
                or_(
                    LicensePool.last_checked.is_(None),
                    LicensePool.last_checked < License.expires,
                )
            )
            .distinct()
        ).all()

        if not stale_pool_ids:
            task.log.info("No pools with newly-expired licenses found.")
            return

        pools = (
            session.scalars(
                select(LicensePool)
                .where(LicensePool.id.in_(stale_pool_ids))
                .options(selectinload(LicensePool.licenses))
            )
            .unique()
            .all()
        )

        for pool in pools:
            pool.update_availability_from_licenses(as_of=now)

    task.log.info(
        f"Updated availability for {pluralize(len(pools), 'license pool')} with expired licenses."
    )

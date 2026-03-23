"""Celery tasks for the Lexile DB update."""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

from celery import shared_task
from celery.exceptions import Ignore
from sqlalchemy import and_, delete, exists, select
from sqlalchemy.orm import Session

from palace.manager.celery.task import Task
from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.integration.metadata.lexile.api import LexileDBAPI
from palace.manager.integration.metadata.lexile.service import LexileDBService
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.lock import RedisLock
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.constants import DataSourceConstants
from palace.manager.sqlalchemy.model.classification import (
    Classification,
    Subject,
)
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.util import get_one, get_one_or_create
from palace.manager.util.datetime_helpers import utc_now

BATCH_SIZE = 10
# Workflow lock TTL: 2 hours, so a failed run will eventually unblock.
WORKFLOW_LOCK_TIMEOUT = timedelta(hours=2)
SERVICE_NAME = "Lexile DB Update"
LEXILE_DB_LOCK_KEY: tuple[str, str] = ("LexileDB", "Update")


def _lexile_workflow_lock(redis_client: Redis, lock_value: str) -> RedisLock:
    """Create a workflow-level RedisLock for the Lexile DB update.

    The lock is held across all batches of a single run. ``lock_value`` is a UUID
    generated on the first batch and passed to every replacement task, allowing
    re-acquisition (extend) on each subsequent batch.

    :param lock_value: UUID string identifying this workflow run.
    """
    return RedisLock(
        redis_client,
        list(LEXILE_DB_LOCK_KEY),
        random_value=lock_value,
        lock_timeout=WORKFLOW_LOCK_TIMEOUT,
    )


def _query_isbns_without_lexile(
    session: Session,
    offset: int,
    limit: int,
    force: bool,
) -> list[Identifier]:
    """Query ISBN identifiers that need Lexile data.

    :param session: Database session.
    :param offset: Offset for pagination.
    :param limit: Maximum number of identifiers to return.
    :param force: If True, include ISBNs that have no Lexile or already have a Lexile DB record
      (to refresh from MetaMetrics). If False, only include ISBNs with no Lexile from any source.
    :return: List of Identifier objects.

    Note: In force mode, ISBNs that have a Lexile score only from a third-party source (e.g.
    Overdrive) are excluded. We only process ISBNs where we either have no Lexile data at all, or
    we already have our own Lexile DB record to refresh. This avoids overwriting third-party
    scores with a new Lexile DB lookup when we have never had authoritative data for that ISBN.
    """
    lexile_subject_exists = (
        select(Classification.id)
        .where(Classification.identifier_id == Identifier.id)
        .join(Subject, Classification.subject_id == Subject.id)
        .where(Subject.type == Subject.LEXILE_SCORE)
    )
    lexile_db_exists = (
        select(Classification.id)
        .where(Classification.identifier_id == Identifier.id)
        .join(Subject, Classification.subject_id == Subject.id)
        .join(DataSource, Classification.data_source_id == DataSource.id)
        .where(
            and_(
                DataSource.name == DataSourceConstants.LEXILE_DB,
                Subject.type == Subject.LEXILE_SCORE,
            )
        )
    )

    query = select(Identifier).where(Identifier.type == Identifier.ISBN)

    if force:
        # Force: no Lexile at all, OR already have Lexile DB record (refresh from MetaMetrics).
        # Third-party-only Lexiles (e.g. Overdrive) excluded; see docstring.
        query = query.where(~exists(lexile_subject_exists) | exists(lexile_db_exists))
    else:
        # Default: only process ISBNs with no Lexile classification from any source.
        query = query.where(~exists(lexile_subject_exists))

    query = query.order_by(Identifier.id).offset(offset).limit(limit)
    return list(session.execute(query).unique().scalars().all())


def _process_identifier(
    session: Session,
    identifier: Identifier,
    api: LexileDBAPI,
    data_source: DataSource,
    force: bool,
) -> bool:
    """Process a single identifier: fetch Lexile from API and update classification.

    :return: True if the identifier was updated, False otherwise.
    """
    isbn = identifier.identifier
    lexile = api.fetch_lexile_for_isbn(isbn)
    if lexile is None:
        return False

    # For force mode: remove existing Lexile DB classification if present (in case value changed)
    if force:
        lexile_db_ids = (
            select(Classification.id)
            .where(
                Classification.identifier_id == identifier.id,
                Classification.data_source_id == data_source.id,
            )
            .join(Subject, Classification.subject_id == Subject.id)
            .where(Subject.type == Subject.LEXILE_SCORE)
        )
        session.execute(
            delete(Classification).where(Classification.id.in_(lexile_db_ids)),
            execution_options={"synchronize_session": False},
        )

    identifier.classify(
        data_source,
        Subject.LEXILE_SCORE,
        str(lexile),
        None,
        weight=Classification.TRUSTED_DISTRIBUTOR_WEIGHT,
    )
    return True


@shared_task(queue=QueueNames.default, bind=True)
def run_lexile_db_update(task: Task) -> None:
    """Orchestrator: check for Lexile DB config and launch worker if lock is available.

    Runs nightly via Celery beat. If a Lexile DB integration exists and no update
    is currently running (lock not held), launches the worker task.
    """
    with task.session() as session:
        try:
            LexileDBService.from_config(session)
        except CannotLoadConfiguration as e:
            task.log.info(f"Lexile DB update skipped: {e}")
            return

        redis_client = task.services.redis().client()
        # Check with a sentinel value — we only need to know if any lock is held.
        if _lexile_workflow_lock(redis_client, "sentinel").locked():
            task.log.info("Lexile DB update already in progress, skipping.")
            return

        lexile_db_update_task.delay(force=False)
        task.log.info("Lexile DB update task queued.")


@shared_task(queue=QueueNames.default, bind=True)
def lexile_db_update_task(
    task: Task,
    force: bool = False,
    offset: int = 0,
    timestamp_id: int | None = None,
    lock_value: str | None = None,
) -> None:
    """Worker: process batches of ISBNs, fetching Lexile data from the API.

    Uses task.replace() to continue with the next batch. A workflow-level Redis
    lock is held across all batches: acquired on the first batch (when lock_value
    is None) and extended on each subsequent batch via re-acquisition with the same
    UUID. The lock() context manager's ignored_exceptions=(Ignore,) ensures that the
    lock is not released when task.replace() hands off to the next batch.

    :param force: If True, reprocess ISBNs that already have a Lexile DB record.
    :param offset: Pagination offset for the current batch.
    :param timestamp_id: ID of the Timestamp DB record for this run. Required when offset > 0.
    :param lock_value: UUID identifying this workflow run's lock. Generated on the first batch
        and passed to every replacement task. Required when offset > 0.
    """
    if offset > 0 and timestamp_id is None:
        task.log.error("Lexile DB update: timestamp_id required when offset > 0")
        return
    if offset > 0 and lock_value is None:
        task.log.error("Lexile DB update: lock_value required when offset > 0")
        return

    with task.transaction() as session:
        try:
            service = LexileDBService.from_config(session)
        except CannotLoadConfiguration as e:
            task.log.info(f"Lexile DB update skipped: {e}")
            return

    # is_first_batch is True only when no lock_value was passed in (fresh run).
    is_first_batch = lock_value is None
    if lock_value is None:
        lock_value = str(uuid4())

    redis_client = task.services.redis().client()
    workflow_lock = _lexile_workflow_lock(redis_client, lock_value)

    # Ignore is raised by task.replace() — it must not release the lock when chaining
    # to the next batch, so the next batch can extend it with the same lock_value.
    with workflow_lock.lock(
        raise_when_not_acquired=False,
        ignored_exceptions=(Ignore,),
    ) as lock_acquired:
        if not lock_acquired and is_first_batch:
            task.log.info("Lexile DB update could not acquire lock, skipping.")
            return
        if not lock_acquired and not is_first_batch:
            task.log.warning(
                "Lexile DB update: workflow lock expired between batches; continuing."
            )

        identifiers: list[Identifier] = []
        with task.transaction() as session:
            if offset == 0:
                stamp, _ = get_one_or_create(
                    session,
                    Timestamp,
                    service=SERVICE_NAME,
                    service_type=Timestamp.TASK_TYPE,
                    collection=None,
                )
                timestamp_id = stamp.id
                stamp.start = utc_now()
                stamp.finish = None
                stamp.achievements = None
                stamp.exception = None

            data_source = DataSource.lookup(
                session, DataSourceConstants.LEXILE_DB, autocreate=True
            )
            api = LexileDBAPI(service.settings)
            identifiers = _query_isbns_without_lexile(
                session, offset, BATCH_SIZE, force
            )

            updated = 0
            for identifier in identifiers:
                if _process_identifier(session, identifier, api, data_source, force):
                    updated += 1

            run_stamp = get_one(
                session,
                Timestamp,
                service=SERVICE_NAME,
                service_type=Timestamp.TASK_TYPE,
                collection=None,
            )
            if run_stamp is not None:
                run_stamp.update(
                    finish=utc_now(),
                    achievements=(
                        f"Processed {len(identifiers)} identifiers, "
                        f"updated {updated} with Lexile data (offset={offset})"
                    ),
                )

        if len(identifiers) == BATCH_SIZE:
            # Celery expects replace() to be raised as an exception to trigger task chaining.
            # Raising Ignore (via task.replace) is listed in ignored_exceptions above, so the
            # workflow lock is NOT released here — the next batch task will extend it.
            raise task.replace(
                lexile_db_update_task.s(
                    force=force,
                    offset=offset + BATCH_SIZE,
                    timestamp_id=timestamp_id,
                    lock_value=lock_value,
                )
            )

    task.log.info(
        f"Lexile DB update complete. Processed {len(identifiers)} identifiers at offset {offset}.",
    )

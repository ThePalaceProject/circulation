"""Celery tasks for the Lexile DB update."""

from __future__ import annotations

from datetime import timedelta

from celery import shared_task
from sqlalchemy import and_, exists, select
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
LOCK_TIMEOUT = timedelta(minutes=30)
SERVICE_NAME = "Lexile DB Update"


def _lexile_db_lock(redis_client: Redis, timestamp_id: int) -> RedisLock:
    """Create a RedisLock for the Lexile DB update using timestamp_id as the lock value."""
    return RedisLock(
        redis_client,
        ["LexileDB", "Update"],
        random_value=str(timestamp_id),
        lock_timeout=LOCK_TIMEOUT,
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
    :param force: If True, include all ISBNs (including those with Lexile from other
        sources). If False, only include ISBNs with no Lexile classification.
    :return: List of Identifier objects.
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
        # Force: process ISBNs that have no Lexile OR have Lexile from Lexile DB
        query = query.where(~exists(lexile_subject_exists) | exists(lexile_db_exists))
    else:
        # Default: only process ISBNs with no Lexile at all
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
        for classification in list(identifier.classifications):
            if (
                classification.data_source.name == DataSourceConstants.LEXILE_DB
                and classification.subject.type == Subject.LEXILE_SCORE
            ):
                session.delete(classification)

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
            task.log.info("Lexile DB update skipped: %s", e)
            return

        redis_client = task.services.redis().client()
        lock = RedisLock(
            redis_client,
            ["LexileDB", "Update"],
            lock_timeout=LOCK_TIMEOUT,
        )
        if lock.locked():
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
) -> None:
    """Worker: process batches of ISBNs, fetching Lexile data from the API.

    Uses task.replace() to continue with the next batch. Holds a lock across
    replacements using the Timestamp id as the lock value so replacement tasks
    can extend the lock.
    """
    with task.transaction() as session:
        try:
            service = LexileDBService.from_config(session)
        except CannotLoadConfiguration as e:
            task.log.info("Lexile DB update skipped: %s", e)
            return

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
            session.commit()
        elif timestamp_id is None:
            task.log.error("Lexile DB update: timestamp_id required when offset > 0")
            return

    redis_client = task.services.redis().client()
    lock = _lexile_db_lock(redis_client, timestamp_id)
    if not lock.acquire():
        task.log.info("Lexile DB update could not acquire lock, skipping.")
        return

    identifiers: list[Identifier] = []
    try:
        with task.transaction() as session:
            data_source = DataSource.lookup(
                session, DataSourceConstants.LEXILE_DB, autocreate=True
            )
            if not data_source:
                task.log.error("Lexile DB data source not found")
                return

            api = LexileDBAPI(service._settings)
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
            raise task.replace(
                lexile_db_update_task.s(
                    force=force,
                    offset=offset + BATCH_SIZE,
                    timestamp_id=timestamp_id,
                )
            )
    finally:
        lock.release()

    task.log.info(
        "Lexile DB update complete. Processed %d identifiers at offset %d.",
        len(identifiers),
        offset,
    )

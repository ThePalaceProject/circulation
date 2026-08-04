from __future__ import annotations

import random
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

from celery import shared_task
from celery.exceptions import Ignore, Retry
from opensearchpy import OpenSearchException
from sqlalchemy import select
from sqlalchemy.orm import Session

from palace.util.exceptions import BasePalaceException
from palace.util.log import elapsed_time_logging

from palace.manager.celery.task import Task
from palace.manager.celery.utils import signature_with
from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.search.revision import SearchSchemaRevision
from palace.manager.search.service import SearchService
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.lock import LockNotAcquired, TaskLock
from palace.manager.service.redis.models.search import WaitingForIndexing
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.backoff import exponential_backoff


def get_work_search_documents(
    session: Session, batch_size: int, offset: int
) -> Sequence[dict[str, Any]]:
    """
    Get a batch of search documents for works that are presentation ready.
    """
    works = [
        w.id
        for w in session.execute(
            select(Work.id)
            .where(Work.presentation_ready == True)
            .order_by(Work.id)
            .limit(batch_size)
            .offset(offset)
        )
    ]
    return Work.to_search_documents(session, works)


def add_documents_to_index(
    task: Task, index: ExternalSearchIndex, documents: Sequence[dict[str, Any]]
) -> None:
    try:
        with elapsed_time_logging(
            log_method=task.log.info,
            message_prefix="Works added to index",
            skip_start=True,
        ):
            failed_documents = index.add_documents(documents=documents)
        if failed_documents:
            raise FailedToIndex(f"Failed to index {len(failed_documents)} works.")
    except (FailedToIndex, OpenSearchException) as e:
        wait_time = exponential_backoff(task.request.retries)
        task.log.error(f"{e}. Retrying in {wait_time} seconds.")
        raise task.retry(countdown=wait_time)


class FailedToIndex(BasePalaceException): ...


def set_read_pointer(
    task: Task, service: SearchService, revision: SearchSchemaRevision
) -> None:
    """
    Publish a revision's index for reads.

    :param task: The task doing the update, retried with a backoff if OpenSearch rejects
        the alias update.
    :param service: The search service whose read pointer is being moved.
    :param revision: The revision whose index should serve reads.
    """
    try:
        service.read_pointer_set(revision)
    except OpenSearchException as e:
        wait_time = exponential_backoff(task.request.retries)
        task.log.error(
            f"Failed to update read pointer: {e}. Retrying in {wait_time} seconds."
        )
        raise task.retry(countdown=wait_time)
    task.log.info(
        f"Updated read pointer ({service.base_revision_name} v{revision.version})."
    )


def advance_read_pointer(task: Task, target_index: str | None) -> None:
    """
    Point reads at the index a completed reindex just filled, if reads are still being
    served from an older one.

    This is what makes a schema migration finish on its own: a new revision is created
    with the write pointer aimed at it, and whichever reindex first completes a full
    pass over that index publishes it. No caller has to remember to advance the pointer,
    and a run that dies partway through simply leaves the pointer where it was.

    :param target_index: The index the completed run was filling.
    """
    service = task.services.search.service()
    revision = task.services.search.revision_directory().highest()
    latest_index = revision.name_for_index(service.base_revision_name)

    try:
        read_pointer = service.read_pointer()
        write_pointer = service.write_pointer()
    except OpenSearchException as e:
        # A full pass takes days on a large collection, so a transient failure reading the
        # pointers is worth retrying rather than discarding the run's work.
        wait_time = exponential_backoff(task.request.retries)
        task.log.error(
            f"Failed to read search pointers: {e}. Retrying in {wait_time} seconds."
        )
        raise task.retry(countdown=wait_time)

    if read_pointer is not None and read_pointer.version >= revision.version:
        return

    if target_index != latest_index:
        task.log.warning(
            f"Not advancing read pointer: this reindex filled {target_index}, "
            f"but the latest revision is {latest_index}."
        )
        return

    if write_pointer is None or write_pointer.index != target_index:
        # The write pointer moved partway through, so our documents are split across the
        # old and new indexes and neither one received a complete pass.
        task.log.warning(
            f"Not advancing read pointer: the write pointer moved to "
            f"{write_pointer.index if write_pointer is not None else None} "
            f"while this reindex was filling {target_index}."
        )
        return

    set_read_pointer(task, service, revision)


@shared_task(queue=QueueNames.default, bind=True, max_retries=4)
def search_reindex(
    task: Task, offset: int = 0, batch_size: int = 500, target_index: str | None = None
) -> None:
    """
    Submit all works that are presentation ready to the search index.

    This is done in batches, with the batch size determined by the batch_size parameter. This
    task will do a batch, then requeue itself until all works have been indexed. Once every
    work has been indexed, the read pointer is advanced to the index we filled if it is still
    behind. See advance_read_pointer.

    :param target_index: The index this run is filling, carried across requeues so the final
        batch can tell whether the write pointer moved partway through. Resolved from the write
        pointer when a run starts from the beginning; callers should not pass it. A run started
        at a non-zero offset skips the works before it, so it leaves this unset and never
        advances the read pointer.
    """
    index = task.services.search.index()
    service = task.services.search.service()
    task_lock = TaskLock(task, lock_name="search_reindex")

    with task_lock.lock(release_on_exit=False, ignored_exceptions=(Retry, Ignore)):
        if target_index is None and offset == 0:
            write_pointer = service.write_pointer()
            target_index = write_pointer.index if write_pointer is not None else None

        task.log.info(
            f"Running search reindex at offset {offset} with batch size {batch_size}."
        )

        with (
            task.session() as session,
            elapsed_time_logging(
                log_method=task.log.info,
                message_prefix="Works queried from database",
                skip_start=True,
            ),
        ):
            documents = get_work_search_documents(session, batch_size, offset)

        add_documents_to_index(task, index, documents)

        if len(documents) == batch_size:
            # This task is complete, but there are more works waiting to be indexed. Requeue ourselves
            # to process the next batch. We add a random delay to avoid hammering the search service
            # when this task is running in parallel on multiple workers.
            delay = random.uniform(5, 15)
            raise task.replace(
                signature_with(
                    task, offset=offset + batch_size, target_index=target_index
                ).set(countdown=delay)
            )

        advance_read_pointer(task, target_index)

    task.log.info("Finished search reindex.")
    task_lock.release()


@shared_task(queue=QueueNames.default, bind=True, max_retries=4)
def update_read_pointer(task: Task) -> None:
    """
    Update the read pointer to the latest revision, unconditionally.

    search_reindex advances the read pointer itself once it has filled the latest index, so
    this is the manual override for the cases it deliberately declines to handle: publishing
    an index that no single reindex run filled end to end, or one filled by a run that
    started before the revision it ended up writing to existed.

    It has no CLI entry point, since the usual repair is another full reindex
    (bin/repair/search_index), which publishes the index when it finishes. To publish an
    already-filled index without reindexing, queue this task directly:

        celery -A "palace.manager.celery.app" call palace.manager.celery.tasks.search.update_read_pointer
    """
    task.log.info("Updating read pointer.")
    service = task.services.search.service()
    revision_directory = task.services.search.revision_directory()
    revision = revision_directory.highest()
    set_read_pointer(task, service, revision)


@shared_task(queue=QueueNames.default, bind=True, throws=(LockNotAcquired,))
def search_indexing(task: Task, batch_size: int = 500) -> None:
    redis_client = task.services.redis.client()
    task_lock = TaskLock(task, lock_timeout=timedelta(minutes=15))

    # Hold the lock across our self-replacements (release_on_exit=False) so the
    # every-minute beat schedule can't start a second, concurrent search_indexing
    # task.
    with task_lock.lock(release_on_exit=False, ignored_exceptions=(Retry, Ignore)):
        waiting = WaitingForIndexing(redis_client)
        works = waiting.pop(batch_size)

        if len(works) > 0:
            index_works.delay(works=works)

        if len(works) == batch_size:
            # There are more works waiting to be indexed. Requeue ourselves to
            # process the next batch after a short cooldown so we don't flood the
            # opensearch index and degrade search performance. This is shorter than
            # the reindex cooldown because indexing must stay roughly current.
            delay = random.uniform(1.0, 3.0)
            raise task.replace(signature_with(task).set(countdown=delay))

    # The queue is drained; release the lock so the next beat run can start fresh.
    task_lock.release()
    task.log.info("Finished queuing indexing tasks.")


@shared_task(queue=QueueNames.default, bind=True, max_retries=4)
def index_works(task: Task, works: Sequence[int]) -> None:
    index = task.services.search.index()

    task.log.info(f"Indexing {len(works)} works.")

    with (
        task.session() as session,
        elapsed_time_logging(
            log_method=task.log.info,
            message_prefix="Works queried from database",
            skip_start=True,
        ),
    ):
        documents = Work.to_search_documents(session, works)

    add_documents_to_index(task, index, documents)

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from celery import chain, shared_task
from opensearchpy import OpenSearchException
from sqlalchemy import select
from sqlalchemy.orm import Session

from palace.manager.celery.task import Task
from palace.manager.core.exceptions import BasePalaceException
from palace.manager.service.celery.celery import QueueNames
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.backoff import exponential_backoff
from palace.manager.util.log import elapsed_time_logging


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


class FailedToIndex(BasePalaceException):
    ...


@shared_task(queue=QueueNames.default, bind=True, max_retries=4)
def search_reindex(task: Task, offset: int = 0, batch_size: int = 500) -> None:
    """
    Submit all works that are presentation ready to the search index.

    This is done in batches, with the batch size determined by the batch_size parameter. This
    task will do a batch, then requeue itself until all works have been indexed.
    """
    index = task.services.search.index()

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

    if len(documents) == batch_size:
        # This task is complete, but there are more works waiting to be indexed. Requeue ourselves
        # to process the next batch.
        raise task.replace(
            search_reindex.s(offset=offset + batch_size, batch_size=batch_size)
        )

    task.log.info("Finished search reindex.")


@shared_task(queue=QueueNames.default, bind=True, max_retries=4)
def update_read_pointer(task: Task) -> None:
    """
    Update the read pointer to the latest revision.

    This is used to indicate that the search index has been updated to a specific version. We
    chain this task with search_reindex when doing a migration to ensure that the read pointer is
    updated after all works have been indexed. See get_migrate_search_chain.
    """
    task.log.info("Updating read pointer.")
    service = task.services.search.service()
    revision_directory = task.services.search.revision_directory()
    revision = revision_directory.highest()
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


@shared_task(queue=QueueNames.default, bind=True, max_retries=4)
def index_work(task: Task, work_id: int) -> None:
    """
    Index a single work into the search index.
    """
    index = task.services.search.index()
    with task.session() as session:
        documents = Work.to_search_documents(session, [work_id])

    if not documents:
        task.log.warning(f"Work {work_id} not found. Unable to index.")
        return

    try:
        index.add_document(document=documents[0])
    except OpenSearchException as e:
        wait_time = exponential_backoff(task.request.retries)
        task.log.error(
            f"Failed to index work {work_id}: {e}. Retrying in {wait_time} seconds."
        )
        raise task.retry(countdown=wait_time)

    task.log.info(f"Indexed work {work_id}.")


def get_migrate_search_chain() -> chain:
    """
    Get the chain of tasks to run when migrating the search index to a new schema.
    """
    return chain(search_reindex.si(), update_read_pointer.si())

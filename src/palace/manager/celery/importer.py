from collections.abc import Callable, Generator
from contextlib import contextmanager
from datetime import timedelta

from celery.canvas import Signature
from celery.exceptions import Ignore

from palace.util.log import LoggerType, pluralize

from palace.manager.celery.task import Task
from palace.manager.service.redis.models.lock import RedisLock
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.collection import Collection


def import_all(
    collections: list[Collection],
    import_collection: Signature,
    logger: LoggerType,
) -> None:
    """
    Create celery tasks to import all the Collections returned by the given query.
    """
    for collection in collections:
        task = import_collection.delay(
            collection_id=collection.id,
        )

        logger.info(
            f'Queued collection "{collection.name}" for import. Collection(id={collection.id}) Task(id={task.id})'
        )

    logger.info(
        f"Task complete. Queued {pluralize(len(collections), 'collection')} for import."
    )


def import_key(collection_id: int, *additional: str) -> list[str]:
    """
    Generate a Redis key for the given collection ID.
    """
    return [
        "ImportCollection",
        Collection.redis_key_from_id(collection_id),
        *additional,
    ]


def import_workflow_key(collection_id: int) -> list[str]:
    """
    Generate a Redis key for the workflow-level lock for the given collection.
    """
    return [
        "ImportCollectionWorkflow",
        Collection.redis_key_from_id(collection_id),
    ]


def import_workflow_lock(
    client: Redis, collection_id: int, random_value: str
) -> RedisLock:
    """
    Create a workflow-level lock spanning all pages of a single import run.

    This lock is held across page boundaries (between task.replace() calls)
    to ensure at most one import runs per collection at a time.
    """
    return RedisLock(
        client,
        import_workflow_key(collection_id),
        random_value=random_value,
        lock_timeout=timedelta(hours=2),
    )


def reap_workflow_key(collection_id: int) -> list[str]:
    """
    Generate a Redis key for the reap workflow-level lock for the given collection.
    """
    return [
        "ReapCollectionWorkflow",
        Collection.redis_key_from_id(collection_id),
    ]


def reap_workflow_lock(
    client: Redis, collection_id: int, random_value: str
) -> RedisLock:
    """
    Create a workflow-level lock spanning all batches of a single reap run.

    Held across task.replace() calls so at most one reap runs per collection at a time.
    Auto-expires after 2 hours if the process dies.
    """
    return RedisLock(
        client,
        reap_workflow_key(collection_id),
        random_value=random_value,
        lock_timeout=timedelta(hours=2),
    )


@contextmanager
def workflow_lock_guard(
    task: Task,
    collection_id: int,
    *,
    label: str,
    lock_factory: Callable[[Redis, int, str], RedisLock] = import_workflow_lock,
) -> Generator[bool, None, None]:
    """
    Acquire a paginated workflow's per-collection lock and decide whether to run.

    Centralises the locking policy shared by every paginated importer/reaper task. The
    lock is keyed on ``task.request.id``, which Celery preserves across both
    ``task.replace()`` page/batch hand-offs and ``autoretry_for`` retries. A run
    therefore re-acquires its own lock for free on every page and retry, while a
    concurrent run (a different task id) is locked out until the holder finishes or the
    lock's timeout expires. ``Ignore`` (raised by ``task.replace()``) and the task's
    ``autoretry_for`` exceptions do not release the lock, so it stays held across page
    boundaries and retry backoff windows.

    :param task: The bound task instance (``self``).
    :param collection_id: The collection being processed.
    :param label: Human-readable task description for log messages, e.g.
        ``"OverDrive import"``.
    :param lock_factory: Builds the lock; defaults to :func:`import_workflow_lock`. Pass
        :func:`reap_workflow_lock` for reaper tasks.
    :returns: Yields ``proceed`` — ``True`` when the caller should run its body, ``False``
        when another run already holds the lock and the caller should skip.
    """
    redis = task.services.redis().client()
    with lock_factory(redis, collection_id, task.request.id).lock(
        raise_when_not_acquired=False,
        # Hold the lock for exactly the exceptions the task retries on, plus Ignore
        # (task.replace hand-off), so it is not released when they propagate out and the
        # retry/next page re-acquires the same lock under its stable task id.
        ignored_exceptions=(Ignore, *getattr(task, "autoretry_for", ())),
    ) as acquired:
        if acquired:
            yield True
        else:
            task.log.warning(
                f"{label} skipped for collection {collection_id}: "
                "another run is already in progress."
            )
            yield False

import argparse
import time
from collections.abc import Sequence
from typing import Any

from opensearchpy import OpenSearchException
from sqlalchemy.orm import Session

from palace.manager.celery.tasks.search import (
    FailedToIndex,
    add_documents_to_index,
    advance_read_pointer,
    get_presentation_ready_work_ids,
    resolve_target_index,
    search_reindex,
)
from palace.manager.scripts.base import Script
from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.service.container import Services
from palace.manager.service.redis.models.lock import LockError, TaskLock
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.backoff import exponential_backoff


def _batch_size(value: str) -> int:
    """
    Parse a batch size, which has to be at least one work.

    A batch of zero pages forever: the query comes back empty every time, so neither the
    script's loop nor the task's requeue ever reaches the short batch that stops it, and
    both go on holding the reindex lock.
    """
    batch_size = int(value)
    if batch_size < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {batch_size}")
    return batch_size


class RebuildSearchIndexScript(Script):
    """Completely delete the search index and recreate it."""

    # As many attempts at a batch as search_reindex gets, for the same reason: a
    # transient search failure shouldn't cost a pass that has been running for hours.
    MAX_RETRIES = 4

    def __init__(
        self,
        _db: Session | None = None,
        services: Services | None = None,
        search_index: ExternalSearchIndex | None = None,
        cmd_args: list[str] | None = None,
    ) -> None:
        super().__init__(_db, services)
        self.search = search_index or self.services.search.index()
        args = self.parse_command_line(self._db, cmd_args=cmd_args)
        self.blocking: bool = args.blocking
        self.delete: bool = args.delete
        self.force: bool = args.force
        self.batch_size: int = args.batch_size

        if self.force and not self.blocking:
            # A queued rebuild is the scheduled reindex, so there is nothing for it to
            # take the lock from, and silently ignoring the flag would leave whoever is
            # trying to take a manager back believing they had.
            self.arg_parser(self._db).error("--force can only be used with --blocking.")

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Rebuild the search index from scratch."
        )
        parser.add_argument(
            "-b",
            "--blocking",
            action="store_true",
            help="Rebuild the index in this process, and wait for it to finish, "
            "rather than handing the work to Celery.",
        )
        parser.add_argument(
            "-d",
            "--delete",
            action="store_true",
            help="Delete the search index before rebuilding.",
        )
        parser.add_argument(
            "-f",
            "--force",
            action="store_true",
            help="Take the reindex lock from whatever is holding it, stopping a running "
            "reindex at its next batch. Only valid with --blocking.",
        )
        parser.add_argument(
            "--batch-size",
            type=_batch_size,
            default=500,
            help="How many works to index per batch. (default: %(default)s)",
        )
        return parser

    def do_run(self) -> None:
        """Delete all search documents, then rebuild the search index."""
        self.log.info("Rebuilding search index.")

        if self.blocking:
            self.rebuild_locally()
        else:
            self.queue_rebuild()

    def reindex_lock(self) -> TaskLock:
        """The lock search_reindex takes, so that we contend with it for the index."""
        return TaskLock(
            redis_client=self.services.redis.client(), lock_name="search_reindex"
        )

    def delete_documents(self) -> None:
        """Empty the index. Only ever called holding the reindex lock."""
        self.log.info("Deleting all search documents.")
        self.search.clear_search_documents()

    def queue_rebuild(self) -> None:
        """Hand the rebuild to a Celery worker."""
        if self.delete:
            # Emptying the index gets in a running reindex's way as much as rebuilding
            # does, so it waits for the same lock rather than pulling the index out from
            # under one. We give the lock back before queueing, because the task we are
            # queueing has to be able to take it.
            with self.reindex_lock().lock():
                self.delete_documents()

        task = search_reindex.s(batch_size=self.batch_size).delay()
        self.log.info(
            f"Search index rebuild started (Task ID: {task.id}). The reindex will run in the background."
        )

    def rebuild_locally(self) -> None:
        """
        Index every presentation-ready work here, rather than in a Celery worker.
        """
        service = self.services.search.service()
        target_index = resolve_target_index(service)

        lock = self.reindex_lock()
        if self.force:
            self.take_lock(lock)
        with lock.lock():
            if self.delete:
                self.delete_documents()

            offset = 0
            indexed = 0
            while True:
                work_ids = get_presentation_ready_work_ids(
                    self._db, self.batch_size, offset
                )
                documents = Work.to_search_documents(self._db, work_ids)
                self.index_batch(documents, lock)
                self._db.commit()
                self.extend_lock(lock)
                offset += len(work_ids)
                indexed += len(documents)
                if len(work_ids) < self.batch_size:
                    break
                self.log.info(f"Indexed {indexed} of {offset} works.")

            self.log.info(f"Finished search reindex. Indexed {indexed} works.")

            revision = self.services.search.revision_directory().highest()
            advance_read_pointer(self.log, service, revision, target_index)

    def index_batch(self, documents: Sequence[dict[str, Any]], lock: TaskLock) -> None:
        """
        Submit a batch to the index, retrying a failure the way the task does.

        search_reindex answers a transient search failure by retrying the batch, which
        costs it one requeue. Here the same failure would cost the whole pass: there is
        no offset to resume from, so a rebuild that has been running for hours would
        start again from the first work.

        :raises FailedToIndex: If the index kept rejecting documents.
        :raises OpenSearchException: If the index kept rejecting the request.
        """
        for retries in range(self.MAX_RETRIES + 1):
            try:
                add_documents_to_index(self.log, self.search, documents)
                return
            except (FailedToIndex, OpenSearchException) as e:
                if retries == self.MAX_RETRIES:
                    raise
                wait_time = exponential_backoff(retries)
                self.log.exception(f"Batch failed ({e}). Retrying in {wait_time}s.")
                time.sleep(wait_time)
                # Waiting counts against the lock like anything else does, and a long
                # enough run of failures would otherwise outlive it while we sleep.
                self.extend_lock(lock)

    def extend_lock(self, lock: TaskLock) -> None:
        """
        Keep holding the reindex lock, or stop.

        :raises LockError: If the lock is no longer ours. Something else can take it the
            moment it expires, so carrying on would mean indexing, and then publishing,
            alongside the very run we took the lock to keep out.
        """
        if not lock.extend_timeout():
            raise LockError(f"Lost the {lock.key} lock during the rebuild.")

    def take_lock(self, lock: TaskLock) -> None:
        """
        Take the reindex lock away from a reindex that is already running.

        The running reindex is not interrupted. It takes the lock once per batch, so it
        stops when it comes back for the next one, and until then it is indexing the same
        works into the same index as this rebuild - wasteful, but not harmful, since both
        are writing the same documents.
        """
        held_by = lock.acquire_force()
        if held_by is None:
            self.log.info(f"Nothing was holding {lock.key}.")
        else:
            self.log.warning(
                f"Took {lock.key} from {held_by}, which will stop at its next batch."
            )

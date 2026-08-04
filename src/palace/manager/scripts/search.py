import argparse

from sqlalchemy.orm import Session

from palace.manager.celery.tasks.search import (
    add_documents_to_index,
    advance_read_pointer,
    get_work_search_documents,
    resolve_target_index,
    search_reindex,
)
from palace.manager.scripts.base import Script
from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.service.container import Services
from palace.manager.service.redis.models.lock import TaskLock


class RebuildSearchIndexScript(Script):
    """Completely delete the search index and recreate it."""

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
        self.batch_size: int = args.batch_size

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
            "--batch-size",
            type=int,
            default=500,
            help="How many works to index per batch. (default: %(default)s)",
        )
        return parser

    def do_run(self) -> None:
        """Delete all search documents, then rebuild the search index."""
        if self.delete:
            self.log.info("Deleting all search documents.")
            self.search.clear_search_documents()

        self.log.info("Rebuilding search index.")

        if self.blocking:
            self.rebuild_in_process()
        else:
            task = search_reindex.s(batch_size=self.batch_size).delay()
            self.log.info(
                f"Search index rebuild started (Task ID: {task.id}). The reindex will run in the background."
            )

    def rebuild_in_process(self) -> None:
        """
        Index every presentation-ready work here, rather than in a Celery worker.

        `search_reindex` requeues itself once per batch, so its throughput is set by how
        long a trip through the `default` queue takes rather than by how long indexing
        takes. On a manager whose queue carries a backlog that is minutes per batch, and
        a full pass can run for days. Doing the same work in one process costs a couple
        of hours instead, at the price of tying up whoever ran the script.

        We take the task's own lock so the scheduled reindex can't run against the index
        at the same time, and extend it every batch, since nothing else will while we
        hold it.
        """
        service = self.services.search.service()
        target_index = resolve_target_index(service)

        lock = TaskLock(
            redis_client=self.services.redis.client(), lock_name="search_reindex"
        )
        with lock.lock():
            indexed = 0
            while True:
                documents = get_work_search_documents(
                    self._db, self.batch_size, indexed
                )
                add_documents_to_index(self.log, self.search, documents)
                lock.extend_timeout()
                indexed += len(documents)
                if len(documents) < self.batch_size:
                    break
                self.log.info(f"Indexed {indexed} works.")

            self.log.info(f"Finished search reindex. Indexed {indexed} works.")

            revision = self.services.search.revision_directory().highest()
            advance_read_pointer(self.log, service, revision, target_index)

import argparse

from sqlalchemy.orm import Session

from palace.manager.celery.tasks.search import get_migrate_search_chain, search_reindex
from palace.manager.scripts.base import Script
from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.service.container import Services


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
        self.migration: bool = args.migration

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Rebuild the search index from scratch."
        )
        parser.add_argument(
            "-b",
            "--blocking",
            action="store_true",
            help="Block until the search index is rebuilt.",
        )
        parser.add_argument(
            "-d",
            "--delete",
            action="store_true",
            help="Delete the search index before rebuilding.",
        )
        parser.add_argument(
            "-m",
            "--migration",
            action="store_true",
            help="Treat as a migration and update the read pointer after the rebuild is complete.",
        )
        return parser

    def do_run(self) -> None:
        """Delete all search documents, then rebuild the search index."""
        if self.delete:
            self.log.info("Deleting all search documents.")
            self.search.clear_search_documents()

        self.log.info("Rebuilding search index.")

        if self.migration:
            rebuild_task = get_migrate_search_chain()
        else:
            rebuild_task = search_reindex.s()

        if self.blocking:
            rebuild_task()
        else:
            task = rebuild_task.delay()
            self.log.info(
                f"Search index rebuild started (Task ID: {task.id}). The reindex will run in the background."
            )

import argparse
import sys
from typing import Protocol

from celery.canvas import Signature
from celery.result import AsyncResult
from sqlalchemy.orm import Session

from palace.manager.celery.task import Task
from palace.manager.scripts.input import CollectionInputScript


class CollectionTaskCallable(Protocol):
    def __call__(self, collection_id: int, force: bool = False) -> Signature:
        """Callable that takes a collection ID and a force flag and returns a Signature."""


class OpdsTaskScript(CollectionInputScript):
    """Run task for OPDS feed associated with a collection."""

    def __init__(
        self,
        task_type: str,
        *,
        collection_task: Task | CollectionTaskCallable,
        all_task: Task | None = None,
        db: Session | None = None,
    ):
        super().__init__(db)
        self._task_type = task_type
        self._collection_task = collection_task
        self._all_task = all_task

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = super().arg_parser(_db)
        parser.add_argument(
            "--force",
            help="Import the feed from scratch, even if it seems like it was already imported.",
            dest="force",
            action="store_true",
        )
        return parser

    def _call_all_task(self, *, force: bool) -> AsyncResult:
        if self._all_task is None:
            self.log.error("You must specify at least one collection.")
            sys.exit(1)
        return self._all_task.delay(force=force)

    def _call_collection_task(self, *, collection_id: int, force: bool) -> AsyncResult:
        if isinstance(self._collection_task, Task):
            return self._collection_task.delay(collection_id=collection_id, force=force)
        else:
            return self._collection_task(
                collection_id=collection_id, force=force
            ).delay()

    def do_run(self, cmd_args: list[str] | None = None) -> None:
        parsed = self.parse_command_line(self._db, cmd_args=cmd_args)
        collections = parsed.collections
        tasks = []
        if not collections:
            tasks.append(self._call_all_task(force=parsed.force))
        else:
            for collection in collections:
                task = self._call_collection_task(
                    collection_id=collection.id,
                    force=parsed.force,
                )
                tasks.append(task)
                self.log.info(
                    f'Queued collection "{collection.name}" [id={collection.id}] for {self._task_type} task "{task.id}"...'
                )

        self.log.info(
            f"Started {len(tasks)} tasks. The tasks will run in the background."
        )

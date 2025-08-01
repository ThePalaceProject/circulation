import argparse

from sqlalchemy.orm import Session

from palace.manager.celery.tasks import opds2
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.scripts.input import CollectionInputScript


class OPDS2ImportScript(CollectionInputScript):
    """Import all books from the OPDS2 feed associated with a collection."""

    def __init__(
        self,
        db: Session | None = None,
    ):
        super().__init__(db)

    @classmethod
    def arg_parser(cls) -> argparse.ArgumentParser:
        parser = super().arg_parser()
        parser.add_argument(
            "--force",
            help="Import the feed from scratch, even if it seems like it was already imported.",
            dest="force",
            action="store_true",
        )
        return parser

    def do_run(self, cmd_args: list[str] | None = None) -> None:
        parsed = self.parse_command_line(self._db, cmd_args=cmd_args)
        collections = parsed.collections
        tasks = []
        if not collections:
            tasks.append(
                opds2.import_all.delay(
                    force=parsed.force,
                )
            )
        else:
            for collection in collections:
                task = opds2.import_collection.delay(
                    collection_id=collection.id,
                    force=parsed.force,
                )
                self.log.info(
                    f'Queued collection "{collection.name}" [id={collection.id}] for importing task "{task.id}"...'
                )
                tasks.append(task)

        self.log.info(
            f"Started {len(tasks)} tasks. The import will run in the background."
        )


class OPDS2ReaperScript(CollectionInputScript):
    """Mark all items in CM that are not in the feed as unavailable."""

    def __init__(
        self,
        db: Session | None = None,
    ):
        super().__init__(db)

    @classmethod
    def arg_parser(cls) -> argparse.ArgumentParser:
        parser = super().arg_parser()
        parser.add_argument(
            "--force",
            help="Import the feed from scratch, even if it seems like it was already imported.",
            dest="force",
            action="store_true",
        )
        return parser

    def do_run(self, cmd_args: list[str] | None = None) -> None:
        parsed = self.parse_command_line(self._db, cmd_args=cmd_args)
        collections = parsed.collections
        tasks = []
        if not collections:
            raise PalaceValueError("You must specify at least one collection to reap.")
        else:
            for collection in collections:
                task = opds2.import_and_reap_not_found_chord(
                    collection_id=collection.id,
                    force=parsed.force,
                ).delay()
                self.log.info(
                    f'Queued collection "{collection.name}" [id={collection.id}] for reaping task "{task.id}"...'
                )
                tasks.append(task)

        self.log.info(
            f"Started {len(tasks)} tasks. The import will run in the background."
        )

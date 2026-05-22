"""Scripts for manually operating Bibliotheca Celery tasks."""

from __future__ import annotations

import argparse

from sqlalchemy.orm import Session

from palace.util.exceptions import PalaceValueError

from palace.manager.celery.tasks import bibliotheca
from palace.manager.scripts.base import Script
from palace.manager.sqlalchemy.model.collection import Collection


class ImportEventCollection(Script):
    """Manually kick off the Bibliotheca event import for one or all collections."""

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Kick off the Bibliotheca event import Celery task."
        )
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "--collection",
            type=str,
            metavar="NAME",
            help="Name of the Bibliotheca collection to import.",
        )
        group.add_argument(
            "--import-all",
            action="store_true",
            help="Queue the event import for every Bibliotheca collection.",
        )
        return parser

    def do_run(self, cmd_args: list[str] | None = None) -> None:
        parsed = self.parse_command_line(self._db, cmd_args=cmd_args)

        if parsed.import_all:
            bibliotheca.import_all_collections.delay()
            self.log.info("Queued event import for all Bibliotheca collections.")
            return

        collection = Collection.by_name(self._db, parsed.collection)
        if not collection:
            raise PalaceValueError(f'No collection found named "{parsed.collection}".')

        bibliotheca.import_collection.delay(collection_id=collection.id)
        self.log.info(
            f"Queued event import for Bibliotheca collection '{collection.name}'."
        )


class ImportPurchaseCollection(Script):
    """Manually kick off the Bibliotheca purchase import for one or all collections."""

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Kick off the Bibliotheca purchase import Celery task."
        )
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "--collection",
            type=str,
            metavar="NAME",
            help="Name of the Bibliotheca collection to import.",
        )
        group.add_argument(
            "--import-all",
            action="store_true",
            help="Queue the purchase import for every Bibliotheca collection.",
        )
        return parser

    def do_run(self, cmd_args: list[str] | None = None) -> None:
        parsed = self.parse_command_line(self._db, cmd_args=cmd_args)

        if parsed.import_all:
            bibliotheca.purchase_all_collections.delay()
            self.log.info("Queued purchase import for all Bibliotheca collections.")
            return

        collection = Collection.by_name(self._db, parsed.collection)
        if not collection:
            raise PalaceValueError(f'No collection found named "{parsed.collection}".')

        bibliotheca.purchase_collection.delay(collection_id=collection.id)
        self.log.info(
            f"Queued purchase import for Bibliotheca collection '{collection.name}'."
        )

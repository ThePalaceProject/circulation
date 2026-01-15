import argparse

from sqlalchemy.orm import Session

from palace.manager.celery.tasks import boundless
from palace.manager.scripts.base import Script
from palace.manager.sqlalchemy.model.collection import Collection


class ImportCollection(Script):
    """A convenient script for manually kicking off a Boundless collection import"""

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--collection-name",
            type=str,
            help="Collection Name",
        ),
        parser.add_argument(
            "--import-all",
            action="store_true",
            help="Import all identifiers rather not just recently changed ones.",
        ),
        return parser

    def do_run(self, cmd_args: list[str] | None = None) -> None:
        parsed = self.parse_command_line(self._db, cmd_args=cmd_args)
        collection_name = parsed.collection_name

        collection = Collection.by_name(self._db, collection_name)
        if not collection:
            raise ValueError(f'No collection found named "{collection_name}".')

        boundless.import_collection.delay(
            collection_id=collection.id,
            import_all=parsed.import_all,
        )

import argparse

from palace.manager.celery.tasks.axis import (
    import_identifiers,
    list_identifiers_for_import,
)
from palace.manager.scripts.base import Script
from palace.manager.sqlalchemy.model.collection import Collection


class ImportCollection(Script):
    """A convenient script for manually kicking off a Boundless collection import"""

    @classmethod
    def arg_parser(cls):
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

    def do_run(self, *args, **kwargs):
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        collection_name = parsed.collection_name

        collection = Collection.by_name(self._db, collection_name)
        if not collection:
            raise ValueError(f'No collection found named "{collection_name}".')

        list_identifiers_for_import.apply_async(
            kwargs={"collection_id": collection.id, "import_all": parsed.import_all},
            link=import_identifiers.s(
                collection_id=collection.id,
            ),
        )

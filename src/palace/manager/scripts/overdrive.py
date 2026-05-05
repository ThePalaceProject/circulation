from __future__ import annotations

import argparse
from typing import Any

from sqlalchemy.orm import Session

from palace.util.exceptions import PalaceValueError

from palace.manager.celery.tasks.overdrive import reap_all_collections, reap_collection
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.scripts.base import Script
from palace.manager.sqlalchemy.model.collection import Collection


class OverdriveReaperScript(Script):
    """A convenient script for manually kicking off the overdrive reaper Celery tasks."""

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Manually kick off the Overdrive reaper Celery task."
        )
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument(
            "--collection-name",
            help="Name of the Overdrive collection to reap.",
        )
        group.add_argument(
            "--reap-all",
            action="store_true",
            help="Reap all Overdrive collections.",
        )
        return parser

    def do_run(self, *args: Any, **kwargs: Any) -> None:
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        if parsed.collection_name:
            collection = Collection.by_name(self._db, parsed.collection_name)
            if collection is None:
                raise PalaceValueError(
                    f"No collection found with name '{parsed.collection_name}'."
                )
            if collection.protocol != OverdriveAPI.label():
                raise PalaceValueError(
                    f"Collection '{parsed.collection_name}' is not an Overdrive collection "
                    f"(protocol: '{collection.protocol}')."
                )
            reap_collection.delay(collection.id)
            self.log.info(
                f'The "reap_collection" task has been queued for collection '
                f"'{parsed.collection_name}'. See the celery logs for details."
            )
        else:
            reap_all_collections.delay()
            self.log.info(
                'The "reap_all_collections" task has been queued for execution. '
                "See the celery logs for details about task execution."
            )

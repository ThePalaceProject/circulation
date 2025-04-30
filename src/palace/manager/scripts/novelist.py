from __future__ import annotations

import sys

from palace.manager.celery.tasks.novelist import update_novelists_by_library
from palace.manager.scripts.input import LibraryInputScript
from palace.manager.scripts.timestamp import TimestampScript


class NovelistSnapshotScript(TimestampScript, LibraryInputScript):
    def do_run(
        self, output=sys.stdout, update_novelists_by_collection=None, *args, **kwargs
    ):
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        for library in parsed.libraries:
            update_novelists_by_library.delay(library_id=library.id)
            self.log.info(
                f'Queued novelist_update task for library: name="{library.name}", id={library.id}'
            )

from __future__ import annotations

import sys
from typing import Any, TextIO

from palace.manager.celery.tasks.novelist import update_novelists_by_library
from palace.manager.integration.metadata.novelist import NoveListAPI
from palace.manager.scripts.input import LibraryInputScript
from palace.manager.scripts.timestamp import TimestampScript


class NovelistSnapshotScript(TimestampScript, LibraryInputScript):
    def do_run(self, output: TextIO = sys.stdout, *args: Any, **kwargs: Any) -> None:
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        for library in parsed.libraries:
            if not NoveListAPI.is_configured_db_check(library):
                self.log.info(
                    f'The library name "{library.name}" is not associated with Novelist API integration and '
                    f"therefore will not be queued."
                )
            else:
                # only queue up libraries associated with
                update_novelists_by_library.delay(library_id=library.id)
                self.log.info(
                    f'Queued novelist_update task for library: name="{library.name}", id={library.id}'
                )

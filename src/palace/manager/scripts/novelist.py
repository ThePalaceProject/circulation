from __future__ import annotations

import sys

from palace.manager.api.metadata.novelist import NoveListAPI
from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.scripts.input import LibraryInputScript
from palace.manager.scripts.timestamp import TimestampScript


class NovelistSnapshotScript(TimestampScript, LibraryInputScript):
    def do_run(self, output=sys.stdout, *args, **kwargs):
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        for library in parsed.libraries:
            try:
                api = NoveListAPI.from_config(library)
            except CannotLoadConfiguration as e:
                self.log.info(str(e))
                continue
            if api:
                response = api.put_items_novelist(library)

                if response:
                    result = "NoveList API Response\n"
                    result += str(response)

                    output.write(result)

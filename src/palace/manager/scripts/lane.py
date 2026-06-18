from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Any, TextIO

from sqlalchemy.orm import Session

from palace.manager.api.lanes import create_default_lanes
from palace.manager.scripts.input import LibraryInputScript
from palace.manager.sqlalchemy.model.lane import Lane
from palace.manager.sqlalchemy.model.library import Library


class DeleteInvisibleLanesScript(LibraryInputScript):
    """Delete lanes that are flagged as invisible"""

    def process_library(self, library: Library) -> None:
        try:
            # Evaluate lane visibility before mutating relationships in this session.
            # Otherwise, deleting a hidden parent can make a visible child appear
            # top-level (and therefore visible) before the child is considered.
            lanes = self._db.query(Lane).filter(Lane.library_id == library.id).all()
            invisible_lanes = [lane for lane in lanes if not lane.visible]

            for lane in invisible_lanes:
                self._db.delete(lane)
            self._db.commit()
            logging.info(f"Completed hidden lane deletion for {library.short_name}")
        except Exception as e:
            try:
                logging.exception(
                    f"hidden lane deletion failed for {library.short_name}. "
                    f"Attempting to rollback updates",
                    e,
                )
                self._db.rollback()
            except Exception as e:
                logging.exception(
                    f"hidden lane deletion rollback for {library.short_name} failed", e
                )


class LaneResetScript(LibraryInputScript):
    """Reset a library's lanes based on language configuration or estimates
    of the library's current collection."""

    @classmethod
    def arg_parser(
        cls, _db: Session, multiple_libraries: bool = True
    ) -> argparse.ArgumentParser:
        parser = super().arg_parser(_db, multiple_libraries=multiple_libraries)
        parser.add_argument(
            "--reset",
            help="Actually reset the lanes as opposed to showing what would happen.",
            action="store_true",
        )
        return parser

    def do_run(self, output: TextIO = sys.stdout, **kwargs: Any) -> None:
        parsed = self.parse_command_line(self._db, **kwargs)
        libraries = parsed.libraries
        self.reset = parsed.reset
        if not self.reset:
            self.log.info(
                "This is a dry run. Nothing will actually change in the database."
            )
            self.log.info("Run with --reset to change the database.")

        if libraries and self.reset:
            self.log.warning(
                """This is not a drill.
Running this script will permanently reset the lanes for %d libraries. Any lanes created from
custom lists will be deleted (though the lists themselves will be preserved).
Sleeping for five seconds to give you a chance to back out.
You'll get another chance to back out before the database session is committed.""",
                len(libraries),
            )
            time.sleep(5)
        self.process_libraries(libraries)

        new_lane_output = "New Lane Configuration:"
        for library in libraries:
            new_lane_output += "\n\nLibrary '%s':\n" % library.name

            def print_lanes_for_parent(parent: Lane | None) -> str:
                lanes = (
                    self._db.query(Lane)
                    .filter(Lane.library == library)
                    .filter(Lane.parent == parent)
                    .order_by(Lane.priority)
                )
                lane_output = ""
                for lane in lanes:
                    lane_output += (
                        "  "
                        + ("  " * len(list(lane.parentage)))
                        + lane.display_name
                        + "\n"
                    )
                    lane_output += print_lanes_for_parent(lane)
                return lane_output

            new_lane_output += print_lanes_for_parent(None)

        output.write(new_lane_output)

        if self.reset:
            self.log.warning("All done. Sleeping for five seconds before committing.")
            time.sleep(5)
            self._db.commit()

    def process_library(self, library: Library) -> None:
        create_default_lanes(self._db, library)

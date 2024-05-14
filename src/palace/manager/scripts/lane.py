from __future__ import annotations

import logging
import sys
import time

from palace.manager.api.lanes import create_default_lanes
from palace.manager.scripts.input import LibraryInputScript
from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.sqlalchemy.listeners import site_configuration_has_changed
from palace.manager.sqlalchemy.model.lane import Lane


class LaneSweeperScript(LibraryInputScript):
    """Do something to each lane in a library."""

    def process_library(self, library):
        from palace.manager.sqlalchemy.model.lane import WorkList

        top_level = WorkList.top_level_for_library(self._db, library)
        queue = [top_level]
        while queue:
            new_queue = []
            for l in queue:
                if isinstance(l, Lane):
                    l = self._db.merge(l)
                if self.should_process_lane(l):
                    self.process_lane(l)
                    self._db.commit()
                for sublane in l.children:
                    new_queue.append(sublane)
            queue = new_queue

    def should_process_lane(self, lane):
        return True

    def process_lane(self, lane):
        pass


class UpdateLaneSizeScript(LaneSweeperScript):
    def __init__(self, _db=None, *args, **kwargs):
        super().__init__(_db, *args, **kwargs)
        search = kwargs.get("search_index_client", None)
        self._search: ExternalSearchIndex = search or self.services.search.index()

    def should_process_lane(self, lane):
        """We don't want to process generic WorkLists -- there's nowhere
        to store the data.
        """
        return isinstance(lane, Lane)

    def process_lane(self, lane):
        """Update the estimated size of a Lane."""

        # We supress the configuration changes updates, as each lane is updated
        # and call the site_configuration_has_changed function once after this
        # script has finished running.
        #
        # This is done because calling site_configuration_has_changed repeatedly
        # was causing performance problems, when we have lots of lanes to update.
        lane._suppress_before_flush_listeners = True
        lane.update_size(self._db, search_engine=self._search)
        self.log.info("%s: %d", lane.full_identifier, lane.size)

    def do_run(self, *args, **kwargs):
        super().do_run(*args, **kwargs)
        site_configuration_has_changed(self._db)


class DeleteInvisibleLanesScript(LibraryInputScript):
    """Delete lanes that are flagged as invisible"""

    def process_library(self, library):
        try:
            for lane in self._db.query(Lane).filter(Lane.library_id == library.id):
                if not lane.visible:
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
    def arg_parser(cls, _db):
        parser = LibraryInputScript.arg_parser(_db)
        parser.add_argument(
            "--reset",
            help="Actually reset the lanes as opposed to showing what would happen.",
            action="store_true",
        )
        return parser

    def do_run(self, output=sys.stdout, **kwargs):
        parsed = self.parse_command_line(self._db, **kwargs)
        libraries = parsed.libraries
        self.reset = parsed.reset
        if not self.reset:
            self.log.info(
                "This is a dry run. Nothing will actually change in the database."
            )
            self.log.info("Run with --reset to change the database.")

        if libraries and self.reset:
            self.log.warn(
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

            def print_lanes_for_parent(parent):
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
            self.log.warn("All done. Sleeping for five seconds before committing.")
            time.sleep(5)
            self._db.commit()

    def process_library(self, library):
        create_default_lanes(self._db, library)

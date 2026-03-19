"""Script to kick off the Lexile DB update task."""

from __future__ import annotations

import argparse
from typing import Any

from sqlalchemy.orm import Session

from palace.manager.celery.tasks.lexile import lexile_db_update_task
from palace.manager.scripts.base import Script


class LexileDBUpdateScript(Script):
    """Kick off the Lexile DB update task."""

    name = "Lexile DB Update"

    def __init__(
        self,
        force: bool = False,
        _db: Session | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(_db=_db, **kwargs)
        self._force = force

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Run the Lexile DB update task to augment Lexile scores."
        )
        parser.add_argument(
            "--force",
            action="store_true",
            help="Update all ISBNs, including those with existing Lexile DB data",
        )
        return parser

    def do_run(self, *args: Any, **kwargs: Any) -> None:
        parsed = self.parse_command_line(self._db, *args, **kwargs)
        force = getattr(parsed, "force", self._force)
        lexile_db_update_task.delay(force=force)
        self.log.info(
            "Successfully queued lexile_db_update_task (force=%s)",
            force,
        )

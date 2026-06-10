from __future__ import annotations

import argparse
from collections.abc import Sequence
from typing import Any

from sqlalchemy.orm import Session

from palace.manager.celery.tasks.equivalents import equivalent_identifiers_refresh
from palace.manager.scripts.base import Script, _normalize_cmd_args


class EquivalentIdentifiersRefreshScript(Script):
    """Manually queue the equivalent_identifiers_refresh Celery task.

    By default a delta run is queued, which processes only the identifier IDs
    currently in the Redis dirty queue. Pass ``--full-refresh`` to seed the
    dirty queue from the database first — useful after a Redis restart or to
    recover from missed listener events.
    """

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Queue the equivalent_identifiers_refresh Celery task."
        )
        parser.add_argument(
            "--full-refresh",
            action="store_true",
            default=False,
            help=(
                "Seed the dirty queue from the database before processing. "
                "Use to recover from a Redis restart or missed listener events."
            ),
        )
        return parser

    def do_run(
        self,
        cmd_args: Sequence[str | None] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        parsed = self.arg_parser(self._db).parse_args(_normalize_cmd_args(cmd_args))
        equivalent_identifiers_refresh.delay(full_refresh=parsed.full_refresh)
        mode = "full refresh" if parsed.full_refresh else "delta"
        self.log.info(
            f'The "equivalent_identifiers_refresh" task ({mode}) has been queued '
            "for execution. See the celery logs for details about task execution."
        )

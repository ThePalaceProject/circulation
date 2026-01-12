from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from typing import Protocol, TextIO

from sqlalchemy.orm import Session

from palace.manager.api.local_analytics_exporter import LocalAnalyticsExporter
from palace.manager.scripts.base import Script, _normalize_cmd_args


class LocalAnalyticsExportScript(Script):
    """Export circulation events for a date range to a CSV file."""

    class Exporter(Protocol):
        """Exporter interface for writing analytics output."""

        def export(self, _db: Session, start: str, end: str) -> str:
            """Generate a CSV payload for the given date range."""

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--start",
            help="Include circulation events that happened at or after this time.",
            required=True,
        )
        parser.add_argument(
            "--end",
            help="Include circulation events that happened before this time.",
            required=True,
        )
        return parser

    def do_run(
        self,
        output: TextIO = sys.stdout,
        cmd_args: Sequence[str | None] | None = None,
        exporter: Exporter | None = None,
    ) -> None:
        parser = self.arg_parser(self._db)
        parsed = parser.parse_args(_normalize_cmd_args(cmd_args))
        start = parsed.start
        end = parsed.end

        exporter = exporter or LocalAnalyticsExporter()
        output.write(exporter.export(self._db, start, end))

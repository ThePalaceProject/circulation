from __future__ import annotations

import argparse
import sys

from palace.manager.api.local_analytics_exporter import LocalAnalyticsExporter
from palace.manager.scripts.base import Script


class LocalAnalyticsExportScript(Script):
    """Export circulation events for a date range to a CSV file."""

    @classmethod
    def arg_parser(cls, _db):
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

    def do_run(self, output=sys.stdout, cmd_args=None, exporter=None):
        parser = self.arg_parser(self._db)
        parsed = parser.parse_args(cmd_args)
        start = parsed.start
        end = parsed.end

        exporter = exporter or LocalAnalyticsExporter()
        output.write(exporter.export(self._db, start, end))

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import datetime

import dateutil.parser
import pytz
from sqlalchemy.orm import Session

from palace.manager.celery.tasks.playtime_entries import (
    REPORT_DATE_FORMAT,
    generate_playtime_report,
    sum_playtime_entries,
)
from palace.manager.scripts.base import Script
from palace.manager.util.datetime_helpers import previous_months


class PlaytimeEntriesSummationScript(Script):
    def do_run(self) -> None:
        sum_playtime_entries.delay()


class PlaytimeEntriesReportsScript(Script):

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:
        # The default `start` and `until` dates encompass the previous month.
        # We convert them to strings here so that they are handled the same way
        # as non-default dates specified as arguments.
        default_start, default_until = (
            date.isoformat() for date in previous_months(number_of_months=1)
        )

        parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
        )
        parser.add_argument(
            "--start",
            metavar="YYYY-MM-DD",
            default=default_start,
            type=dateutil.parser.isoparse,
            help="Start date for report in ISO 8601 'yyyy-mm-dd' format.",
        )
        parser.add_argument(
            "--until",
            metavar="YYYY-MM-DD",
            default=default_until,
            type=dateutil.parser.isoparse,
            help="'Until' date for report in ISO 8601 'yyyy-mm-dd' format."
            " The report will represent entries from the 'start' date up until,"
            " but not including, this date.",
        )
        return parser

    @classmethod
    def parse_command_line(
        cls,
        _db: Session,
        cmd_args: Sequence[str | None] | None = None,
    ) -> argparse.Namespace:
        parsed = super().parse_command_line(_db, cmd_args)
        utc_start = pytz.utc.localize(parsed.start)
        utc_until = pytz.utc.localize(parsed.until)
        if utc_start >= utc_until:
            cls.arg_parser(_db).error(
                f"start date ({utc_start.strftime(REPORT_DATE_FORMAT)}) must be before "
                f"until date ({utc_until.strftime(REPORT_DATE_FORMAT)})."
            )
        return argparse.Namespace(
            **{**vars(parsed), **dict(start=utc_start, until=utc_until)}
        )

    def do_run(self) -> None:
        """Produce a report for the given (or default) date range."""

        parsed = self.parse_command_line(self._db)
        start: datetime = parsed.start
        until: datetime = parsed.until

        generate_playtime_report.delay(start=start, until=until)

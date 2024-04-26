from __future__ import annotations

import argparse
import csv
import os
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timedelta
from tempfile import TemporaryFile
from typing import TYPE_CHECKING, Any, Protocol

import dateutil.parser
import pytz
from sqlalchemy.sql.expression import false, true
from sqlalchemy.sql.functions import sum

from palace.manager.core.config import Configuration
from palace.manager.scripts import Script
from palace.manager.sqlalchemy.model.time_tracking import PlaytimeEntry, PlaytimeSummary
from palace.manager.util.datetime_helpers import previous_months, utc_now

if TYPE_CHECKING:
    from sqlalchemy.orm import Query


# TODO: Replace uses once we have a proper CSV writer type or protocol.
class Writer(Protocol):
    """CSV Writer protocol."""

    def writerow(self, row: Iterable[Any]) -> Any:
        ...


class PlaytimeEntriesSummationScript(Script):
    def do_run(self):
        # Reap older processed entries
        older_than, _ = previous_months(number_of_months=1)
        older_than_ts = datetime(
            older_than.year, older_than.month, older_than.day, tzinfo=pytz.UTC
        )
        deleted = (
            self._db.query(PlaytimeEntry)
            .filter(
                PlaytimeEntry.processed == true(),
                PlaytimeEntry.timestamp < older_than_ts,
            )
            .delete()
        )
        self.log.info(f"Deleted {deleted} entries. Older than {older_than_ts}")

        # Collect everything from one hour ago, reducing entries still in flux
        cut_off = utc_now() - timedelta(hours=1)

        # Fetch the unprocessed entries
        result = self._db.query(PlaytimeEntry).filter(
            PlaytimeEntry.processed == false(),
            PlaytimeEntry.timestamp <= cut_off,
        )

        # Aggregate entries per identifier-timestamp-collection-library grouping.
        # The label forms of the identifier, collection, and library are also
        # factored in, in case any of the foreign keys are missing.
        # Since timestamps should be on minute-boundaries the aggregation
        # can be written to PlaytimeSummary directly
        def group_key_for_entry(e: PlaytimeEntry) -> tuple:
            return (
                e.timestamp,
                e.identifier,
                e.collection,
                e.library,
                e.identifier_str,
                e.collection_name,
                e.library_name,
            )

        by_group = defaultdict(int)
        for entry in result.all():
            by_group[group_key_for_entry(entry)] += entry.total_seconds_played
            entry.processed = True

        for group, seconds in by_group.items():
            # Values are in the same order returned from `group_key_for_entry` above.
            (
                timestamp,
                identifier,
                collection,
                library,
                identifier_str,
                collection_name,
                library_name,
            ) = group

            # Update the playtime summary.
            playtime = PlaytimeSummary.add(
                self._db,
                ts=timestamp,
                seconds=seconds,
                identifier=identifier,
                collection=collection,
                library=library,
                identifier_str=identifier_str,
                collection_name=collection_name,
                library_name=library_name,
            )
            self.log.info(
                f"Added {seconds} to {identifier_str} ({collection_name} in {library_name}) for {timestamp}: new total {playtime.total_seconds_played}."
            )

        self._db.commit()


class PlaytimeEntriesEmailReportsScript(Script):
    REPORT_DATE_FORMAT = "%Y-%m-%d"

    @classmethod
    def arg_parser(cls):
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
    def parse_command_line(cls, _db=None, cmd_args=None, *args, **kwargs):
        parsed = super().parse_command_line(_db=_db, cmd_args=cmd_args, *args, **kwargs)
        utc_start = pytz.utc.localize(parsed.start)
        utc_until = pytz.utc.localize(parsed.until)
        if utc_start >= utc_until:
            cls.arg_parser().error(
                f"start date ({utc_start.strftime(cls.REPORT_DATE_FORMAT)}) must be before "
                f"until date ({utc_until.strftime(cls.REPORT_DATE_FORMAT)})."
            )
        return argparse.Namespace(
            **{**vars(parsed), **dict(start=utc_start, until=utc_until)}
        )

    def do_run(self):
        """Produce a report for the given (or default) date range."""
        parsed = self.parse_command_line()
        start = parsed.start
        until = parsed.until

        formatted_start_date = start.strftime(self.REPORT_DATE_FORMAT)
        formatted_until_date = until.strftime(self.REPORT_DATE_FORMAT)
        report_date_label = f"{formatted_start_date} - {formatted_until_date}"

        reporting_name = os.environ.get(
            Configuration.REPORTING_NAME_ENVIRONMENT_VARIABLE, ""
        )

        # format report name for use in csv attachment filename below
        subject_prefix = reporting_name
        if len(reporting_name) > 0:
            subject_prefix += ": "

        email_subject = f"{subject_prefix}Playtime Summaries {formatted_start_date} - {formatted_until_date}"
        reporting_name_with_no_spaces = reporting_name.replace(" ", "_") + "-"
        attachment_extension = "csv"
        attachment_name = (
            f"playtime-summary-{reporting_name_with_no_spaces}"
            f"{formatted_start_date}-{formatted_until_date}.{attachment_extension}"
        )

        # Write to a temporary file so we don't overflow the memory
        with TemporaryFile(
            "w+",
            prefix=f"playtimereport{formatted_until_date}",
            suffix=attachment_extension,
        ) as temp:
            # Write the data as a CSV
            writer = csv.writer(temp)
            _produce_report(
                writer,
                date_label=report_date_label,
                records=self._fetch_report_records(start=start, until=until),
            )

            # Rewind the file and send the report email
            temp.seek(0)
            recipient = os.environ.get(
                Configuration.REPORTING_EMAIL_ENVIRONMENT_VARIABLE
            )
            if recipient:
                self.services.email.send_email(
                    subject=email_subject,
                    receivers=[recipient],
                    text="",
                    attachments={attachment_name: temp.read()},
                )
            else:
                self.log.error("No reporting email found, logging complete report.")
                self.log.warning(temp.read())

    def _fetch_report_records(self, start: datetime, until: datetime) -> Query:
        return (
            self._db.query(PlaytimeSummary)
            .with_entities(
                PlaytimeSummary.identifier_str,
                PlaytimeSummary.collection_name,
                PlaytimeSummary.library_name,
                PlaytimeSummary.isbn,
                PlaytimeSummary.title,
                sum(PlaytimeSummary.total_seconds_played),
            )
            .filter(
                PlaytimeSummary.timestamp >= start,
                PlaytimeSummary.timestamp < until,
            )
            .group_by(
                PlaytimeSummary.identifier_str,
                PlaytimeSummary.collection_name,
                PlaytimeSummary.library_name,
                PlaytimeSummary.identifier_id,
                PlaytimeSummary.isbn,
                PlaytimeSummary.title,
            )
            .order_by(
                PlaytimeSummary.collection_name,
                PlaytimeSummary.library_name,
                PlaytimeSummary.identifier_str,
            )
        )


def _produce_report(writer: Writer, date_label, records=None) -> None:
    if not records:
        records = []
    writer.writerow(
        (
            "date",
            "urn",
            "isbn",
            "collection",
            "library",
            "title",
            "total seconds",
        )
    )
    for (
        identifier_str,
        collection_name,
        library_name,
        isbn,
        title,
        total,
    ) in records:
        row = (
            date_label,
            identifier_str,
            isbn,
            collection_name,
            library_name,
            title,
            total,
        )
        # Write the row to the CSV
        writer.writerow(row)

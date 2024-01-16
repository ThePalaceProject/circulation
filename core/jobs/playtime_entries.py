from __future__ import annotations

import argparse
import csv
import os
from collections import defaultdict
from datetime import datetime, timedelta
from tempfile import TemporaryFile
from typing import TYPE_CHECKING, cast

import dateutil.parser
import pytz
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import sum

from core.config import Configuration
from core.model import get_one
from core.model.edition import Edition
from core.model.identifier import Identifier, RecursiveEquivalencyCache
from core.model.time_tracking import PlaytimeEntry, PlaytimeSummary
from core.util.datetime_helpers import previous_months, utc_now
from core.util.email import EmailManager
from scripts import Script

if TYPE_CHECKING:
    from sqlalchemy.orm import Query


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
                PlaytimeEntry.processed == True, PlaytimeEntry.timestamp < older_than_ts
            )
            .delete()
        )
        self.log.info(f"Deleted {deleted} entries. Older than {older_than_ts}")

        # Collect everything from one hour ago, reducing entries still in flux
        cuttoff = utc_now() - timedelta(hours=1)

        # Fetch the unprocessed entries
        result = self._db.query(PlaytimeEntry).filter(
            PlaytimeEntry.processed == False,
            PlaytimeEntry.timestamp <= cuttoff,
        )
        by_identifier = defaultdict(int)

        # Aggregate entries per identifier-timestamp-collection-library tuple
        # Since timestamps should be on minute-boundaries the aggregation
        # can be written to PlaytimeSummary directly
        for entry in result.all():
            by_identifier[
                (entry.identifier, entry.collection, entry.library, entry.timestamp)
            ] += entry.total_seconds_played
            entry.processed = True

        for id_ts, seconds in by_identifier.items():
            identifier, collection, library, timestamp = id_ts
            playtime = PlaytimeSummary.add(
                identifier, collection, library, timestamp, seconds
            )
            self.log.info(
                f"Added {seconds} to {identifier.urn} ({collection.name} in {library.name}) for {timestamp}: new total {playtime.total_seconds_played}."
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
            writer.writerow(
                [
                    "date",
                    "urn",
                    "isbn",
                    "collection",
                    "library",
                    "title",
                    "total seconds",
                ]
            )

            for (
                urn,
                collection_name,
                library_name,
                identifier_id,
                total,
            ) in self._fetch_report_records(start=start, until=until):
                edition: Edition | None = None
                identifier: Identifier | None = None
                if identifier_id:
                    edition = get_one(
                        self._db, Edition, primary_identifier_id=identifier_id
                    )
                    # Use the identifier from the edition where available.
                    # Otherwise, we'll have to look it up.
                    identifier = (
                        edition.primary_identifier
                        if edition
                        else get_one(self._db, Identifier, id=identifier_id)
                    )
                isbn = self._isbn_for_identifier(identifier)
                title = edition and edition.title
                row = (
                    report_date_label,
                    urn,
                    isbn,
                    collection_name,
                    library_name,
                    title,
                    total,
                )
                # Write the row to the CSV
                writer.writerow(row)

            # Rewind the file and send the report email
            temp.seek(0)
            recipient = os.environ.get(
                Configuration.REPORTING_EMAIL_ENVIRONMENT_VARIABLE
            )
            if recipient:
                EmailManager.send_email(
                    email_subject,
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
                PlaytimeSummary.identifier_id,
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
            )
        )

    @staticmethod
    def _isbn_for_identifier(
        identifier: Identifier | None,
        /,
        *,
        default_value: str = "",
    ) -> str:
        """Find the strongest ISBN match for the given identifier.

        :param identifier: The identifier to match.
        :param default_value: The default value to return if the identifier is missing or a match is not found.
        """
        if identifier is None:
            return default_value

        if identifier.type == Identifier.ISBN:
            return cast(str, identifier.identifier)

        # If our identifier is not an ISBN itself, we'll use our Recursive Equivalency
        # mechanism to find the next best one that is, if available.
        db = Session.object_session(identifier)
        eq_subquery = db.query(RecursiveEquivalencyCache.identifier_id).filter(
            RecursiveEquivalencyCache.parent_identifier_id == identifier.id
        )
        equivalent_identifiers = (
            db.query(Identifier)
            .filter(Identifier.id.in_(eq_subquery))
            .filter(Identifier.type == Identifier.ISBN)
        )

        isbn = next(
            map(lambda id_: id_.identifier, equivalent_identifiers),
            None,
        )
        return isbn or default_value

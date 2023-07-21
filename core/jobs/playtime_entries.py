import csv
import os
from collections import defaultdict
from datetime import date, timedelta
from tempfile import TemporaryFile

from sqlalchemy.sql.functions import sum

from core.config import Configuration
from core.model import get_one
from core.model.edition import Edition
from core.model.time_tracking import IdentifierPlaytime, IdentifierPlaytimeEntry
from core.util.datetime_helpers import utc_now
from core.util.email import EmailManager
from scripts import Script


class PlaytimeEntriesSummationScript(Script):
    def do_run(self):
        # Collect everything from one hour ago, reducing entries still in flux
        cuttoff = utc_now() - timedelta(hours=1)

        # Reap older processed entries
        deleted = (
            self._db.query(IdentifierPlaytimeEntry)
            .filter(
                IdentifierPlaytimeEntry.processed == True,
            )
            .delete()
        )
        self.log.info(f"Deleted {deleted} old entries.")

        # Fetch the unprocessed entries
        result = self._db.query(IdentifierPlaytimeEntry).filter(
            IdentifierPlaytimeEntry.processed == False,
            IdentifierPlaytimeEntry.timestamp <= cuttoff,
        )
        by_identifier = defaultdict(int)

        # Aggregate entries per identifier-timestamp tuple
        # Since timestamps should be on minute-boundaries the aggregation
        # can be written to IdentifierPlaytimes directly
        for entry in result.all():
            by_identifier[
                (entry.identifier, entry.timestamp)
            ] += entry.total_seconds_played
            entry.processed = True

        for id_ts, seconds in by_identifier.items():
            identifier, timestamp = id_ts
            playtime = IdentifierPlaytime.add(identifier, timestamp, seconds)
            self.log.info(
                f"Added {seconds} to {identifier.urn} for {timestamp}: new total {playtime.total_seconds_played}."
            )

        self._db.commit()


class PlaytimeEntriesEmailReportsScript(Script):
    def do_run(self):
        """Send a quarterly report with aggregated playtimes via email"""
        # 3 months prior, shifted to the 1st of the month
        cutoff = utc_now() - timedelta(days=90)
        cutoff = cutoff.replace(day=1).date()
        # Until the 1st of the current month
        until = date.today().replace(day=1)

        # Let the database do the math for us
        result = (
            self._db.query(IdentifierPlaytime)
            .with_entities(
                IdentifierPlaytime.identifier_str,
                IdentifierPlaytime.identifier_id,
                sum(IdentifierPlaytime.total_seconds_played),
            )
            .filter(
                IdentifierPlaytime.timestamp >= cutoff,
                IdentifierPlaytime.timestamp < until,
            )
            .group_by(
                IdentifierPlaytime.identifier_str, IdentifierPlaytime.identifier_id
            )
        )

        # Write to a temporary file so we don't overflow the memory
        with TemporaryFile("w+", prefix=f"playtimereport{until}", suffix="csv") as temp:
            # Write the data as a CSV
            writer = csv.writer(temp)
            writer.writerow(["date", "urn", "title", "total seconds"])

            for urn, identifier_id, total in result:
                edition = None
                if identifier_id:
                    edition = get_one(
                        self._db, Edition, primary_identifier_id=identifier_id
                    )
                title = edition and edition.title
                row = (f"{cutoff} - {until}", urn, title, total)
                # Write the row to the CSV
                writer.writerow(row)

            # Rewind the file and send the report email
            temp.seek(0)
            recipient = os.environ.get(
                Configuration.REPORTING_EMAIL_ENVIRONMENT_VARIABLE
            )
            if recipient:
                EmailManager.send_email(
                    f"Playtime Summaries {cutoff} - {until}",
                    receivers=[recipient],
                    text="",
                    attachments={f"playtime-summary-{cutoff}-{until}": temp.read()},
                )
            else:
                self.log.error("No reporting email found, logging complete report.")
                self.log.error(temp.read())

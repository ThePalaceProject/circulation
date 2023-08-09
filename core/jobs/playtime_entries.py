import csv
import os
from collections import defaultdict
from datetime import datetime, timedelta
from tempfile import TemporaryFile

import pytz
from sqlalchemy.sql.functions import sum

from core.config import Configuration
from core.model import get_one
from core.model.edition import Edition
from core.model.time_tracking import PlaytimeEntry, PlaytimeSummary
from core.util.datetime_helpers import previous_months, utc_now
from core.util.email import EmailManager
from scripts import Script


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
    def do_run(self):
        """Send a quarterly report with aggregated playtimes via email"""
        # 3 months prior, shifted to the 1st of the month
        start, until = previous_months(number_of_months=3)

        # Let the database do the math for us
        result = (
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

        # Write to a temporary file so we don't overflow the memory
        with TemporaryFile("w+", prefix=f"playtimereport{until}", suffix="csv") as temp:
            # Write the data as a CSV
            writer = csv.writer(temp)
            writer.writerow(
                ["date", "urn", "collection", "library", "title", "total seconds"]
            )

            for urn, collection_name, library_name, identifier_id, total in result:
                edition = None
                if identifier_id:
                    edition = get_one(
                        self._db, Edition, primary_identifier_id=identifier_id
                    )
                title = edition and edition.title
                row = (
                    f"{start} - {until}",
                    urn,
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
                    f"Playtime Summaries {start} - {until}",
                    receivers=[recipient],
                    text="",
                    attachments={f"playtime-summary-{start}-{until}": temp.read()},
                )
            else:
                self.log.error("No reporting email found, logging complete report.")
                self.log.warning(temp.read())
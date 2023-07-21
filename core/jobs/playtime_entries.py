from collections import defaultdict
from datetime import timedelta

from core.model.time_tracking import IdentifierPlaytime, IdentifierPlaytimeEntry
from core.util.datetime_helpers import utc_now
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

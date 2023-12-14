import logging

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from api.model.time_tracking import PlaytimeEntriesPost, PlaytimeEntriesPostSummary
from core.model import create
from core.model.collection import Collection
from core.model.identifier import Identifier
from core.model.library import Library
from core.model.time_tracking import PlaytimeEntry
from core.util.datetime_helpers import utc_now


class PlaytimeEntries:
    # The oldest entry acceptable by the insert API
    # If anything earlier arrives, we ignore it with a 410 response
    OLDEST_ACCEPTABLE_ENTRY_DAYS = 120

    @classmethod
    def insert_playtime_entries(
        cls,
        _db: Session,
        identifier: Identifier,
        collection: Collection,
        library: Library,
        data: PlaytimeEntriesPost,
    ) -> tuple[list, PlaytimeEntriesPostSummary]:
        """Insert into the database playtime entries from a request"""
        responses = []
        summary = PlaytimeEntriesPostSummary()
        today = utc_now().date()
        for entry in data.time_entries:
            status_code = 201
            message = "Created"
            transaction = _db.begin_nested()
            success = True
            try:
                if (
                    today - entry.during_minute.date()
                ).days > cls.OLDEST_ACCEPTABLE_ENTRY_DAYS:
                    # This will count as a failure
                    success = False
                    status_code = 410
                    message = "Time entry too old and can no longer be processed"
                else:
                    playtime_entry, _ = create(
                        _db,
                        PlaytimeEntry,
                        tracking_id=entry.id,
                        identifier_id=identifier.id,
                        collection_id=collection.id,
                        library_id=library.id,
                        timestamp=entry.during_minute,
                        total_seconds_played=entry.seconds_played,
                    )
            except IntegrityError as ex:
                logging.getLogger("Time Tracking").error(
                    f"Playtime entry failure {entry.id}: {ex}"
                )
                # A duplicate is reported as a success, since we have already recorded this value
                if "UniqueViolation" in str(ex):
                    summary.successes += 1
                    status_code = 200
                    message = "OK"
                else:
                    status_code = 400
                    message = str(ex.orig)
                    summary.failures += 1
                transaction.rollback()
            else:
                if success:
                    summary.successes += 1
                else:
                    summary.failures += 1
                transaction.commit()

            responses.append(dict(id=entry.id, status=status_code, message=message))
            summary.total += 1

        return responses, summary

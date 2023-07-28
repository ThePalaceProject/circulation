import logging
from typing import List, Tuple

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from api.model.time_tracking import PlaytimeEntriesPost, PlaytimeEntriesPostSummary
from core.model import create
from core.model.collection import Collection
from core.model.identifier import Identifier
from core.model.library import Library
from core.model.time_tracking import PlaytimeEntry


class PlaytimeEntries:
    @staticmethod
    def insert_playtime_entries(
        _db: Session,
        identifier: Identifier,
        collection: Collection,
        library: Library,
        data: PlaytimeEntriesPost,
    ) -> Tuple[List, PlaytimeEntriesPostSummary]:
        """Insert into the database playtime entries from a request"""
        responses = []
        summary = PlaytimeEntriesPostSummary()
        for entry in data.time_entries:
            status_code = 201
            message = "Created"
            transaction = _db.begin_nested()
            try:
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
                summary.successes += 1
                transaction.commit()

            responses.append(dict(id=entry.id, status=status_code, message=message))
            summary.total += 1

        return responses, summary

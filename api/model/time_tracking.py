import datetime
import logging
from typing import Any, Dict, List, Optional

from pydantic import Field, validator

from core.util.flask_util import CustomBaseModel


class PlaytimeTimeEntry(CustomBaseModel):
    id: str = Field(description="An id to ensure uniqueness of the time entry")
    during_minute: datetime.datetime = Field(
        description="A minute boundary datetime of the format yyyy-mm-ddThh:mmZ"
    )
    seconds_played: int = Field(
        description="How many seconds were played within this minute"
    )

    @validator("during_minute")
    def validate_minute_datetime(cls, value: datetime.datetime):
        """Coerce the datetime to a minute boundary"""
        if value.tzname() != "UTC":
            logging.getLogger("TimeTracking").error(
                f"An incorrect timezone was received for a playtime ({value.tzname()})."
            )
            raise ValueError("Timezone MUST be UTC always")
        value = value.replace(second=0, microsecond=0)
        return value

    @validator("seconds_played")
    def validate_seconds_played(cls, value: int):
        """Coerce the seconds played to a max of 60 seconds"""
        if value > 60:
            logging.getLogger("TimeTracking").warning(
                "Greater than 60 seconds was received for a minute playtime."
            )
            value = 60
        elif value < 0:
            logging.getLogger("TimeTracking").warning(
                "Less than 0 seconds was received for a minute playtime."
            )
            value = 0
        return value


class PlaytimeEntriesPost(CustomBaseModel):
    book_id: Optional[str] = Field(
        description="An identifier of a book (currently ignored)."
    )
    library_id: Optional[str] = Field(
        description="And identifier for the library (currently ignored)."
    )
    time_entries: List[PlaytimeTimeEntry] = Field(description="A List of time entries")


class PlaytimeEntriesPostSummary(CustomBaseModel):
    total: int = 0
    successes: int = 0
    failures: int = 0


class PlaytimeEntriesPostResponse(CustomBaseModel):
    responses: List[Dict[str, Any]] = Field(
        description="Responses as part of the multi-reponse"
    )
    summary: PlaytimeEntriesPostSummary = Field(
        description="Summary of failures and successes"
    )

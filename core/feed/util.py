import datetime
from typing import Union

import pytz

TIME_FORMAT_UTC = "%Y-%m-%dT%H:%M:%S+00:00"
TIME_FORMAT_NAIVE = "%Y-%m-%dT%H:%M:%SZ"


def strftime(date: Union[datetime.datetime, datetime.date]) -> str:
    """
    Format a date for the OPDS feeds.

    'A Date construct is an element whose content MUST conform to the
    "date-time" production in [RFC3339].  In addition, an uppercase "T"
    character MUST be used to separate date and time, and an uppercase
    "Z" character MUST be present in the absence of a numeric time zone
    offset.' (https://tools.ietf.org/html/rfc4287#section-3.3)
    """
    if isinstance(date, datetime.datetime) and date.tzinfo is not None:
        # Convert to UTC to make the formatting easier.
        fmt = TIME_FORMAT_UTC
        date = date.astimezone(pytz.UTC)
    else:
        fmt = TIME_FORMAT_NAIVE

    return date.strftime(fmt)

import datetime
import math
from typing import Optional, Tuple

import pytz

# datetime helpers
# As part of the python 3 conversion, the datetime object went through a
# subtle update that changed how UTC works. Find more information here:
# https://blog.ganssle.io/articles/2019/11/utcnow.html
# https://docs.python.org/3/library/datetime.html#aware-and-naive-objects


def datetime_utc(*args, **kwargs) -> datetime.datetime:
    """Return a datetime object but with UTC information from pytz.
    :return: datetime object
    """
    kwargs["tzinfo"] = pytz.UTC
    return datetime.datetime(*args, **kwargs)


def from_timestamp(ts) -> datetime.datetime:
    """Return a UTC datetime object from a timestamp.

    :return: datetime object
    """
    return datetime.datetime.fromtimestamp(ts, tz=pytz.UTC)


def utc_now() -> datetime.datetime:
    """Get the current time in UTC.

    :return: datetime object
    """
    return datetime.datetime.now(tz=pytz.UTC)


def to_utc(dt: Optional[datetime.datetime]) -> Optional[datetime.datetime]:
    """This converts a naive datetime object that represents UTC into
    an aware datetime object.

    :return: datetime object, or None if `dt` was None.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=pytz.UTC)
    if dt.tzinfo == pytz.UTC:
        # Already UTC.
        return dt
    return dt.astimezone(pytz.UTC)


def strptime_utc(date_string, format):
    """Parse a string that describes a time but includes no timezone,
    into a timezone-aware datetime object set to UTC.

    :raise ValueError: If `format` expects timezone information to be
        present in `date_string`.
    """
    if "%Z" in format or "%z" in format:
        raise ValueError(f"Cannot use strptime_utc with timezone-aware format {format}")
    return to_utc(datetime.datetime.strptime(date_string, format))


def previous_months(number_of_months) -> Tuple[datetime.date, datetime.date]:
    now = utc_now()
    # Start from the first of number_of_months ago, where 0=12
    expected_year = now.year - math.floor(number_of_months / 12)
    expected_month = ((now.month - number_of_months) % 12) or 12
    start = now.replace(year=expected_year, month=expected_month, day=1)
    # Until the first of this month
    until = now.replace(day=1)
    return start.date(), until.date()

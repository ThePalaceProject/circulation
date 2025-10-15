import datetime
from collections.abc import Callable
from functools import wraps
from typing import overload

import pytz
from dateutil.relativedelta import relativedelta


def _wrapper[T, **P](func: Callable[P, T]) -> Callable[P, T]:
    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        kwargs["tzinfo"] = pytz.UTC
        return func(*args, **kwargs)

    return wrapper


datetime_utc = _wrapper(datetime.datetime)
"""
Return a datetime object but with UTC information from pytz.
"""


def from_timestamp(ts: float) -> datetime.datetime:
    """Return a UTC datetime object from a timestamp.

    :return: datetime object
    """
    return datetime.datetime.fromtimestamp(ts, tz=pytz.UTC)


def utc_now() -> datetime.datetime:
    """Get the current time in UTC.

    :return: datetime object
    """
    return datetime.datetime.now(tz=pytz.UTC)


@overload
def to_utc(dt: datetime.datetime) -> datetime.datetime: ...


@overload
def to_utc(dt: datetime.datetime | None) -> datetime.datetime | None: ...


def to_utc(dt: datetime.datetime | None) -> datetime.datetime | None:
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


def strptime_utc(date_string: str, format: str) -> datetime.datetime:
    """Parse a string that describes a time but includes no timezone,
    into a timezone-aware datetime object set to UTC.

    :raise ValueError: If `format` expects timezone information to be
        present in `date_string`.
    """
    if "%Z" in format or "%z" in format:
        raise ValueError(f"Cannot use strptime_utc with timezone-aware format {format}")
    return to_utc(datetime.datetime.strptime(date_string, format))


def previous_months(number_of_months: int) -> tuple[datetime.date, datetime.date]:
    """Calculate date boundaries for matching the specified previous number of months.

    :param number_of_months: The number of months in the interval.
    :returns: Date interval boundaries, consisting of a 2-tuple of
        `start` and `until` dates.

    These boundaries should be used such that matching dates are on the
    half-closed/half-open interval `[start, until)` (i.e., start <= match < until).
    Only dates/datetimes greater than or equal to `start` and less than
    (NOT less than or equal to) `until` should be considered as matching.

    `start` will be the first day of the designated month.
    `until` will be the first day of the current month.
    """
    now = utc_now()
    start = now - relativedelta(months=number_of_months)
    start = start.replace(day=1)
    until = now.replace(day=1)
    return start.date(), until.date()


def minute_timestamp(dt: datetime.datetime) -> datetime.datetime:
    """Minute resolution timestamp by truncating the seconds from a datetime object.

    :param dt: datetime object with seconds resolution
    :return: datetime object with minute resolution
    """
    return datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute)

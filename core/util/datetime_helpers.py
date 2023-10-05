import datetime
from typing import Optional, Tuple, overload

import pytz
from dateutil.relativedelta import relativedelta

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


@overload
def to_utc(dt: datetime.datetime) -> datetime.datetime:
    ...


@overload
def to_utc(dt: Optional[datetime.datetime]) -> Optional[datetime.datetime]:
    ...


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


def strptime_utc(date_string: str, format: str) -> datetime.datetime:
    """Parse a string that describes a time but includes no timezone,
    into a timezone-aware datetime object set to UTC.

    :raise ValueError: If `format` expects timezone information to be
        present in `date_string`.
    """
    if "%Z" in format or "%z" in format:
        raise ValueError(f"Cannot use strptime_utc with timezone-aware format {format}")
    return to_utc(datetime.datetime.strptime(date_string, format))


def previous_months(number_of_months: int) -> Tuple[datetime.date, datetime.date]:
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

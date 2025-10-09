from datetime import datetime
from typing import Annotated, Any

from pydantic import AwareDatetime, BeforeValidator
from pydantic_core import PydanticCustomError

from palace.manager.util.datetime_helpers import to_utc


def _numeric_string(value: str) -> bool:
    """Check if a value is a numeric string."""
    try:
        float(value)
        return True
    except ValueError:
        return False


def _iso8601_date_or_aware_datetime_before_validator(value: Any) -> Any:
    """Parse ISO 8601 dates with reduced precision support.

    :param value: Input value to parse (string or datetime)
    :returns: Parsed datetime object or original value
    """
    # Handle numeric input by converting to a string for validation
    if isinstance(value, (float, int)):
        value = str(value)

    if not isinstance(value, str):
        return value

    # Compute key characteristics for pattern matching
    value = value.strip()
    length = len(value)
    split_dash = value.split("-")

    match (length, *split_dash):
        # Year only: 2001
        case (4, year) if year.isdigit():
            return to_utc(datetime.strptime(year, "%Y"))

        # Year-month: 2001-03
        case (7, year, month) if (
            len(year) == 4 and year.isdigit() and len(month) == 2 and month.isdigit()
        ):
            try:
                return to_utc(datetime.strptime(value, "%Y-%m"))
            except ValueError as e:
                raise PydanticCustomError(
                    "invalid_iso8601_year_month",
                    "Invalid ISO 8601 year-month format. "
                    "Month must be 01-12. "
                    "Expected format: YYYY-MM (e.g., '2001-03')",
                ) from e

        # Ordinal compact: 2001034 (7 digits, day of year)
        case (7, ordinal_compact) if ordinal_compact.isdigit():
            try:
                return to_utc(datetime.strptime(value, "%Y%j"))
            except ValueError as e:
                raise PydanticCustomError(
                    "invalid_iso8601_ordinal_compact",
                    "Invalid ISO 8601 ordinal date. "
                    "Day of year must be 001-365 (366 in leap years). "
                    "Expected format: YYYYDDD (e.g., '2001034')",
                ) from e

        # Ordinal extended: 2001-034
        case (8, year, day) if (
            len(year) == 4 and year.isdigit() and len(day) == 3 and day.isdigit()
        ):
            try:
                return to_utc(datetime.strptime(value, "%Y-%j"))
            except ValueError as e:
                raise PydanticCustomError(
                    "invalid_iso8601_ordinal_extended",
                    "Invalid ISO 8601 ordinal date. "
                    "Day of year must be 001-365 (366 in leap years). "
                    "Expected format: YYYY-DDD (e.g., '2001-034')",
                ) from e

        # Date compact: 20010315
        case (8, date) if date.isdigit():
            try:
                return to_utc(datetime.strptime(value, "%Y%m%d"))
            except ValueError as e:
                raise PydanticCustomError(
                    "invalid_iso8601_compact_date",
                    "Invalid ISO 8601 compact date. "
                    "Expected format: YYYYMMDD (e.g., '20010315')",
                ) from e

        # Date extended: 2001-03-15
        case (10, year, month, day) if (
            len(year) == 4
            and year.isdigit()
            and len(month) == 2
            and month.isdigit()
            and len(day) == 2
            and day.isdigit()
        ):
            try:
                return to_utc(datetime.strptime(value, "%Y-%m-%d"))
            except ValueError as e:
                raise PydanticCustomError(
                    "invalid_iso8601_date",
                    "Invalid ISO 8601 date. "
                    "Expected format: YYYY-MM-DD (e.g., '2001-03-15')",
                ) from e

    # Other numeric strings (str or float) will parse in pydantic as a unix timestamp, which is not
    # what we want if we see a numeric string that doesn't match one of the above patterns, raise an
    # error to avoid misinterpretation.
    if _numeric_string(value):
        raise PydanticCustomError(
            "invalid_iso8601_numeric_string",
            "Invalid ISO 8601 date. Expected formats: YYYY, YYYYMMDD or YYYYDDD.",
        )

    # Fall back to normal AwareDatetime parsing for other formats
    return value


def _iso8601_aware_datetime_before_validator(value: Any) -> Any:
    if not isinstance(value, (str, float, int)):
        return value

    if isinstance(value, (float, int)) or _numeric_string(value):
        raise PydanticCustomError(
            "invalid_iso8601_datetime",
            "Invalid ISO 8601 datetime format. Expected format: YYYY-MM-DD[T]HH:MM[:SS[.ffffff]][Z or [±]HH[:]MM]",
        )

    # Fall back to normal AwareDatetime parsing for other formats
    return value


Iso8601DateOrAwareDatetime = Annotated[
    AwareDatetime, BeforeValidator(_iso8601_date_or_aware_datetime_before_validator)
]
"""
A Pydantic type for parsing ISO 8601 date or datetimes.

This type extends ``AwareDatetime`` to support ISO 8601 date formats that are not
handled by Pydantic's default datetime parsing:

- **Year only**: ``2001`` -> January 1, 2001 00:00:00 UTC
- **Year-month**: ``2001-03`` -> March 1, 2001 00:00:00 UTC
- **Year-month-day**: ``2001-04-01`` -> April 1, 2001 00:00:00 UTC
- **Compact date**: ``20010315`` -> March 15, 2001 00:00:00 UTC
- **Ordinal dates**: ``2001-034`` -> 34th day of 2001
- **Compact ordinal dates**: ``2001034`` -> 34th day of 2001

All standard ISO 8601 datetime formats continue to work as expected.

When date components are missing, they default to their earliest valid values
(January for month, day 1 for day, 00:00:00 for time). All dates are assumed
to be in UTC timezone unless otherwise specified.
"""

Iso8601AwareDatetime = Annotated[
    AwareDatetime, BeforeValidator(_iso8601_aware_datetime_before_validator)
]
"""
A Pydantic type for parsing ISO 8601 datetimes.

This type extends ``AwareDatetime`` to reject numeric strings and timestamps,
which are not valid ISO 8601 datetime formats. Pydantic would typically parse
these as Unix timestamps, which is not desired behavior in this context.
"""

from datetime import date, datetime, timezone
from typing import Annotated, Any

from pydantic import AwareDatetime, BeforeValidator, NonNegativeInt


def _axis_datetime_before_validator(value: Any) -> Any:
    if isinstance(value, str):
        # Attempt to parse the string using the format we expect from Axis 360
        try:
            return datetime.strptime(value, "%m/%d/%Y %I:%M:%S %p %z")
        except ValueError:
            pass
        try:
            return datetime.strptime(value, "%m/%d/%Y %I:%M:%S %p").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            pass
    return value


AxisDateTime = Annotated[
    AwareDatetime, BeforeValidator(_axis_datetime_before_validator)
]
"""
A pydantic type that represents a datetime object. It is annotated with a validator that
parses the datetime string format used in Axis 360 XML responses.

This validator attempts to parse the datetime string in two formats:
1. "10/19/2018 7:42:00 PM +00:00" - This format includes the timezone offset.
2. "4/18/2014 3:53:28 AM" - This format does not include the timezone offset. In this case
   the timezone is set to UTC by default.
"""


def _axis_json_datetime_before_validator(value: Any) -> Any:
    if isinstance(value, str):
        try:
            # In its JSON responses Axis gives us datetime strings in this format:
            # 2018-09-29 18:34:00.0001398 +00:00
            # There are two issues with this string that cause us to not be
            # able to parse it:
            # 1. The microseconds part is too long, we need to limit it to 6 digits.
            # 2. There is a space before the timezone offset, which is not standard.
            # This function attempts to process the string into something that we
            # can hand off to pydantic to parse. If it fails, we just return the
            # string as is, and let the model handle it.
            date, time, zone = value.split(" ")
            hours, minutes, seconds = time.split(":")
            seconds, microseconds = seconds.split(".")
            microseconds = microseconds[:6]  # Limit to 6 digits for microseconds
            return f"{date} {hours}:{minutes}:{seconds}.{microseconds}{zone}"
        except ValueError:
            ...

    return value


AxisJsonDateTime = Annotated[
    AwareDatetime, BeforeValidator(_axis_json_datetime_before_validator)
]
"""
A pydantic type that represents a datetime object. It is annotated with a validator that
parses the datetime string format used in Axis 360 JSON responses.

An example of the datetime string format is:
  - "2018-09-29 18:34:00.0001398 +00:00"

This format is tantalizingly close to actually being a standard ISO 8601 format, but
(of course) it has two issues:
1. The microseconds part is too long, we need to limit it to 6 digits.
2. There is a space before the timezone offset, which is not standard.
"""


def _axis_date_before_validator(value: Any) -> Any:
    if isinstance(value, str):
        # Attempt to parse the string using the format we expect from Axis 360
        try:
            return datetime.strptime(value, "%m/%d/%Y").date()
        except ValueError:
            pass
    return value


AxisDate = Annotated[date, BeforeValidator(_axis_date_before_validator)]
"""
The date type used in Axis 360 XML responses.

Example: 10/19/2018
"""


def _axis_string_list_before_validator(value: Any) -> Any:
    """
    Before validator to turn strings like this into lists:
    FICTION / Thrillers; FICTION / Suspense; FICTION / General
    Ursu, Anne ; Fortune, Eric (ILT)
    """

    if isinstance(value, list):
        updated_value = []
        for sub_value in value:
            updated_value.extend(_axis_string_list_before_validator(sub_value))
        return updated_value
    elif isinstance(value, str):
        # If it's a string, split it and return as a list
        return [part.strip() for part in value.split(";")]

    return value


AxisStringList = Annotated[
    list[str], BeforeValidator(_axis_string_list_before_validator)
]


def _axis_runtime_before_validator(value: Any) -> Any:
    """
    Before validator to convert runtime strings in the format HH:mm to seconds.""
    """
    if isinstance(value, str):
        parts = value.split(":")
        if len(parts) != 2:
            raise ValueError(f"Invalid runtime format: {value}. Expected format HH:mm.")
        hours_str, minutes_str = parts
        try:
            hours = int(hours_str)
            minutes = int(minutes_str)
        except ValueError:
            raise ValueError(f"Invalid runtime values: {value}. Expected integers.")
        if hours < 0:
            raise ValueError(f"Invalid value for HH: {hours_str}. Hours must be >= 0.")
        if minutes < 0 or minutes >= 60:
            raise ValueError(
                f"Invalid value for mm: {minutes_str}. Minutes must be >= 0 and < 60."
            )
        return hours * 3600 + minutes * 60
    return value


AxisRuntime = Annotated[NonNegativeInt, BeforeValidator(_axis_runtime_before_validator)]
"""
A pydantic type that represents a runtime in seconds.

It parses strings in the format HH:mm returned by the API and converts them to seconds.
"""

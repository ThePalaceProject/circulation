import datetime

import pytest
from pydantic import TypeAdapter, ValidationError

from palace.manager.api.axis.models.validators import (
    AxisDate,
    AxisDateTime,
    AxisJsonDateTime,
    AxisRuntime,
    AxisStringList,
)
from palace.manager.util.datetime_helpers import datetime_utc


class TestAxisValidators:
    def test_axis_date_time(self) -> None:
        adaptor = TypeAdapter(AxisDateTime)

        # Can validate a datetime object
        assert adaptor.validate_python(
            datetime_utc(2015, 7, 22, 17, 40, 27)
        ) == datetime_utc(2015, 7, 22, 17, 40, 27)

        # If the object isn't in a format we expect, it will raise a validation error
        with pytest.raises(ValidationError):
            adaptor.validate_python([])

        with pytest.raises(ValidationError):
            adaptor.validate_python("foo")

        with pytest.raises(ValidationError):
            adaptor.validate_python(None)

        # Can parse the string in the format we expect from Axis 360
        assert adaptor.validate_python("07/22/2015 05:40:27 AM +0400") == datetime_utc(
            2015, 7, 22, 1, 40, 27
        )

        # Or with an implicit UTC timezone
        assert adaptor.validate_python("05/08/2025 11:52:22 PM") == datetime_utc(
            2025, 5, 8, 23, 52, 22
        )

    def test_axis_json_date_time(self) -> None:
        adaptor = TypeAdapter(AxisJsonDateTime)

        # Can validate a datetime object
        assert adaptor.validate_python(
            datetime_utc(2015, 7, 22, 17, 40, 27)
        ) == datetime_utc(2015, 7, 22, 17, 40, 27)

        # If the object isn't in a format we expect, it will raise a validation error
        with pytest.raises(ValidationError):
            adaptor.validate_python("foo")

        # Can parse the string in the format we expect from Axis 360
        assert adaptor.validate_python(
            "2018-09-29 18:34:00.0001398 +00:00"
        ) == datetime_utc(2018, 9, 29, 18, 34, 00, 139)

        # Less precision is also allowed
        assert adaptor.validate_python("2018-09-29 18:34:00.12 +00:00") == datetime_utc(
            2018, 9, 29, 18, 34, 00, 120000
        )

    def test_axis_date(self) -> None:
        adaptor = TypeAdapter(AxisDate)

        # If the object isn't in a format we expect, it will raise a validation error
        with pytest.raises(ValidationError):
            adaptor.validate_python([])

        with pytest.raises(ValidationError):
            adaptor.validate_python("foo")

        with pytest.raises(ValidationError):
            adaptor.validate_python(None)

        # Can parse the string in the format we expect from Axis 360
        assert adaptor.validate_python("07/22/2015") == datetime.date(2015, 7, 22)

    def test_axis_string_list(self) -> None:
        adaptor = TypeAdapter(AxisStringList)

        # Can validate a list of strings
        assert adaptor.validate_python(
            ["FICTION / Thrillers", "FICTION / Suspense", "FICTION / General"]
        ) == [
            "FICTION / Thrillers",
            "FICTION / Suspense",
            "FICTION / General",
        ]

        # Can validate a string and convert it to a list
        assert adaptor.validate_python(
            "FICTION / Thrillers; FICTION / Suspense; FICTION / General"
        ) == [
            "FICTION / Thrillers",
            "FICTION / Suspense",
            "FICTION / General",
        ]

        # Can validate a mix of lists and strings
        assert adaptor.validate_python(
            ["FICTION / Thrillers; FICTION / Suspense", "FICTION / General"]
        ) == [
            "FICTION / Thrillers",
            "FICTION / Suspense",
            "FICTION / General",
        ]

        # If the object isn't in a format we expect, it will raise a validation error
        with pytest.raises(ValidationError):
            adaptor.validate_python(None)

    def test_axis_runtime(self) -> None:
        adaptor = TypeAdapter(AxisRuntime)

        # Can validate a runtime string in the format HH:mm and convert it to seconds
        assert adaptor.validate_python("01:30") == 1 * 3600 + 30 * 60

        # Can validate a runtime string in the format HH:mm and convert it to seconds
        assert adaptor.validate_python("00:45") == 0 * 3600 + 45 * 60

        # If the object isn't in a format we expect, it will raise a validation error
        with pytest.raises(ValidationError):
            adaptor.validate_python([])
        with pytest.raises(
            ValidationError, match="Invalid runtime format: foo. Expected format HH:mm."
        ):
            adaptor.validate_python("foo")
        with pytest.raises(
            ValidationError, match="Invalid runtime values: foo:bar. Expected integers."
        ):
            adaptor.validate_python("foo:bar")

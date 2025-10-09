from datetime import datetime, timezone

import pytest
from pydantic import TypeAdapter, ValidationError

from palace.manager.opds.types.date import (
    Iso8601AwareDatetime,
    Iso8601DateOrAwareDatetime,
)


class TestIso8601DateOrAwareDatetime:
    @pytest.mark.parametrize(
        ("date_before_parsing", "expected"),
        [
            pytest.param(
                1999, datetime(1999, 1, 1, tzinfo=timezone.utc), id="year_only_int"
            ),
            pytest.param(
                "2001", datetime(2001, 1, 1, tzinfo=timezone.utc), id="year_only"
            ),
            pytest.param(
                "2001-03",
                datetime(2001, 3, 1, tzinfo=timezone.utc),
                id="year_month_march",
            ),
            pytest.param(
                "2023-12",
                datetime(2023, 12, 1, tzinfo=timezone.utc),
                id="year_month_december",
            ),
            pytest.param(
                20010315,
                datetime(2001, 3, 15, tzinfo=timezone.utc),
                id="compact_date_int",
            ),
            pytest.param(
                "20010315",
                datetime(2001, 3, 15, tzinfo=timezone.utc),
                id="compact_date",
            ),
            pytest.param(
                "20231225",
                datetime(2023, 12, 25, tzinfo=timezone.utc),
                id="compact_date_christmas",
            ),
            pytest.param(
                "2001-03-15",
                datetime(2001, 3, 15, tzinfo=timezone.utc),
                id="extended_date",
            ),
            pytest.param(
                "2001-034",
                datetime(2001, 2, 3, tzinfo=timezone.utc),
                id="ordinal_extended",
            ),
            pytest.param(
                "2023-365",
                datetime(2023, 12, 31, tzinfo=timezone.utc),
                id="ordinal_extended_last_day",
            ),
            pytest.param(
                "2001034",
                datetime(2001, 2, 3, tzinfo=timezone.utc),
                id="ordinal_compact",
            ),
            pytest.param(
                "2023365",
                datetime(2023, 12, 31, tzinfo=timezone.utc),
                id="ordinal_compact_last_day",
            ),
            pytest.param(
                "2020-366",
                datetime(2020, 12, 31, tzinfo=timezone.utc),
                id="ordinal_extended_leap_year_day_366",
            ),
            pytest.param(
                "2020366",
                datetime(2020, 12, 31, tzinfo=timezone.utc),
                id="ordinal_compact_leap_year_day_366",
            ),
            pytest.param(
                "2020-02-29",
                datetime(2020, 2, 29, tzinfo=timezone.utc),
                id="leap_year_feb_29_extended",
            ),
            pytest.param(
                "20200229",
                datetime(2020, 2, 29, tzinfo=timezone.utc),
                id="leap_year_feb_29_compact",
            ),
            pytest.param(
                "2023-001",
                datetime(2023, 1, 1, tzinfo=timezone.utc),
                id="ordinal_extended_first_day",
            ),
            pytest.param(
                "2023001",
                datetime(2023, 1, 1, tzinfo=timezone.utc),
                id="ordinal_compact_first_day",
            ),
            pytest.param(
                "2023-01",
                datetime(2023, 1, 1, tzinfo=timezone.utc),
                id="year_month_january",
            ),
            pytest.param(
                "2023-01  ",
                datetime(2023, 1, 1, tzinfo=timezone.utc),
                id="year_month_january_spaces",
            ),
        ],
    )
    def test_validation(
        self, date_before_parsing: str | int, expected: datetime
    ) -> None:
        """Test that various ISO 8601 reduced precision formats are parsed correctly."""
        ta = TypeAdapter(Iso8601DateOrAwareDatetime)
        result = ta.validate_python(date_before_parsing)
        assert result == expected

    @pytest.mark.parametrize(
        "datetime_string",
        [
            pytest.param("2001-03-15 10:30:00Z", id="datetime_with_Z_space"),
            pytest.param("2001-03-15T10:30:00Z", id="datetime_with_Z"),
            pytest.param("2001-03-15T10:30:00+00:00", id="datetime_with_offset_zero"),
            pytest.param(
                "2001-03-15T10:30:00.123456Z", id="datetime_with_microseconds"
            ),
            pytest.param(
                "2023-12-25T23:59:59.999999+05:30", id="datetime_with_positive_offset"
            ),
        ],
    )
    def test_standard_datetime_formats(self, datetime_string: str) -> None:
        """Test that standard ISO 8601 datetime formats still work."""
        ta = TypeAdapter(Iso8601DateOrAwareDatetime)
        # Just verify it doesn't raise an error
        result = ta.validate_python(datetime_string)
        assert isinstance(result, datetime)
        # Verify timezone aware
        assert result.tzinfo is not None

    @pytest.mark.parametrize(
        ("invalid_date", "error_pattern"),
        [
            pytest.param(
                "2001-03-15T10:30:00",
                "Input should have timezone info",
                id="datetime_missing_timezone",
            ),
            pytest.param(
                "not-a-date",
                "Input should be a valid datetime",
                id="generic_invalid_string",
            ),
            pytest.param(
                "2001-13",
                "Invalid ISO 8601 year-month format.*Month must be 01-12",
                id="year_month_invalid_month",
            ),
            pytest.param(
                "2001400",
                "Invalid ISO 8601 ordinal date.*Day of year must be 001-365",
                id="ordinal_compact_invalid_day",
            ),
            pytest.param(
                "2001-400",
                "Invalid ISO 8601 ordinal date.*Day of year must be 001-365",
                id="ordinal_extended_invalid_day",
            ),
            pytest.param(
                "20011301",
                "Invalid ISO 8601 compact date.",
                id="compact_date_invalid_month",
            ),
            pytest.param(
                "2001-13-01",
                "Invalid ISO 8601 date.",
                id="extended_date_invalid_month",
            ),
            pytest.param(
                "2001-02-30",
                "Invalid ISO 8601 date.",
                id="extended_date_invalid_day",
            ),
            pytest.param(
                "2023-02-29",
                "Invalid ISO 8601 date.",
                id="extended_date_non_leap_year",
            ),
            pytest.param(
                "43",
                "Invalid ISO 8601 date.",
                id="numeric_string_too_short",
            ),
            pytest.param(
                "200704051430",
                "Invalid ISO 8601 date.",
                id="numeric_string_too_long",
            ),
            pytest.param(
                "43.44",
                "Invalid ISO 8601 date.",
                id="numeric_string_float",
            ),
            pytest.param(
                45552565.6,
                "Invalid ISO 8601 date.",
                id="float",
            ),
            pytest.param(
                4555256588,
                "Invalid ISO 8601 date.",
                id="int",
            ),
        ],
    )
    def test_invalid_formats(
        self, invalid_date: str | int | float, error_pattern: str
    ) -> None:
        """Test that invalid date/datetime formats raise ValidationError with appropriate messages."""
        ta = TypeAdapter(Iso8601DateOrAwareDatetime)
        with pytest.raises(ValidationError, match=error_pattern):
            ta.validate_python(invalid_date)

    def test_passthrough_types(self) -> None:
        """Test that datetime objects are passed through unchanged."""
        ta = TypeAdapter(Iso8601DateOrAwareDatetime)

        dt = datetime(2001, 3, 15, 10, 30, 0, tzinfo=timezone.utc)
        result = ta.validate_python(dt)
        assert result == dt

    def test_json_serialization(self) -> None:
        """Test that dates can be serialized to JSON."""
        ta = TypeAdapter(Iso8601DateOrAwareDatetime)

        # Parse a reduced precision date
        dt = ta.validate_python("2001")
        # Serialize to JSON
        json_str = ta.dump_json(dt)
        # Should be serialized as a standard ISO 8601 string
        assert b"2001-01-01T00:00:00Z" in json_str


class TestIso8601AwareDatetime:
    @pytest.mark.parametrize(
        "datetime_string",
        [
            pytest.param("2001-03-15T10:30:00Z", id="datetime_with_Z"),
            pytest.param("2001-03-15 10:30:00Z", id="datetime_with_Z_space"),
            pytest.param("2001-03-15T10:30:00+00:00", id="datetime_with_offset_zero"),
            pytest.param(
                "2001-03-15T10:30:00.123456Z", id="datetime_with_microseconds"
            ),
            pytest.param(
                "2023-12-25T23:59:59.999999+05:30", id="datetime_with_positive_offset"
            ),
            pytest.param(
                "2023-12-25T23:59:59-08:00", id="datetime_with_negative_offset"
            ),
            pytest.param("2023-12-25T10:30:00+0530", id="datetime_with_compact_offset"),
        ],
    )
    def test_valid_datetime_formats(self, datetime_string: str) -> None:
        """Test that valid ISO 8601 datetime formats are parsed correctly."""
        ta = TypeAdapter(Iso8601AwareDatetime)
        result = ta.validate_python(datetime_string)
        assert isinstance(result, datetime)
        # Verify timezone aware
        assert result.tzinfo is not None

    @pytest.mark.parametrize(
        ("invalid_value", "error_pattern"),
        [
            pytest.param(
                1234567890,
                "Invalid ISO 8601 datetime format",
                id="int_timestamp",
            ),
            pytest.param(
                1234567890.5,
                "Invalid ISO 8601 datetime format",
                id="float_timestamp",
            ),
            pytest.param(
                "1234567890",
                "Invalid ISO 8601 datetime format",
                id="numeric_string_timestamp",
            ),
            pytest.param(
                "123.456",
                "Invalid ISO 8601 datetime format",
                id="numeric_string_float",
            ),
            pytest.param(
                "2001",
                "Invalid ISO 8601 datetime format",
                id="year_only_numeric_string",
            ),
            pytest.param(
                "2001-03-15T10:30:00",
                "Input should have timezone info",
                id="datetime_missing_timezone",
            ),
            pytest.param(
                "not-a-datetime",
                "Input should be a valid datetime",
                id="invalid_string",
            ),
            pytest.param(
                "2001-03",
                "input is too short",
                id="year_month",
            ),
            pytest.param(
                "2001-03-15",
                "Input should have timezone info",
                id="date_only",
            ),
        ],
    )
    def test_invalid_formats(
        self, invalid_value: str | int | float, error_pattern: str
    ) -> None:
        """Test that invalid datetime formats and numeric values raise ValidationError."""
        ta = TypeAdapter(Iso8601AwareDatetime)
        with pytest.raises(ValidationError, match=error_pattern):
            ta.validate_python(invalid_value)

    def test_passthrough_datetime_object(self) -> None:
        """Test that datetime objects are passed through unchanged."""
        ta = TypeAdapter(Iso8601AwareDatetime)

        dt = datetime(2001, 3, 15, 10, 30, 0, tzinfo=timezone.utc)
        result = ta.validate_python(dt)
        assert result == dt

    def test_json_serialization(self) -> None:
        """Test that datetimes can be serialized to JSON."""
        ta = TypeAdapter(Iso8601AwareDatetime)

        # Parse a datetime string
        dt = ta.validate_python("2001-03-15T10:30:00Z")
        # Serialize to JSON
        json_str = ta.dump_json(dt)
        # Should be serialized as a standard ISO 8601 string
        assert b"2001-03-15T10:30:00Z" in json_str

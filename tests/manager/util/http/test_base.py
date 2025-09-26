from datetime import timedelta

import pytest
from freezegun import freeze_time

from palace.manager.service.logging.configuration import LogLevel
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http.base import get_series, parse_retry_after


def test_get_series() -> None:
    assert get_series(201) == "2xx"
    assert get_series(399) == "3xx"
    assert get_series(500) == "5xx"


class TestParseRetryAfter:
    """Tests for the parse_retry_after helper function."""

    def test_parse_retry_after_seconds(self) -> None:
        """Test parsing delay-seconds format."""
        assert parse_retry_after("120") == 120.0
        assert parse_retry_after("0") == 0.0
        assert parse_retry_after("3.5") == 3.5

    @freeze_time()
    def test_parse_retry_after_http_date(self) -> None:
        """Test parsing HTTP-date format."""
        # Create a date 60 seconds in the future
        future = utc_now() + timedelta(seconds=60)
        date_str = future.strftime("%a, %d %b %Y %H:%M:%S GMT")

        result = parse_retry_after(date_str)
        # Should be approximately 60 seconds, allowing for slight timing differences
        # due to rounding since the time is captured at second resolution
        assert 59 < result <= 60

    @freeze_time()
    def test_parse_retry_after_past_date(self) -> None:
        """Test that past dates return 0."""
        # Create a date 60 seconds in the past
        past = utc_now() - timedelta(seconds=60)
        date_str = past.strftime("%a, %d %b %Y %H:%M:%S GMT")

        result = parse_retry_after(date_str)
        assert result == 0  # Past dates should return 0

    @pytest.mark.parametrize(
        "header_value",
        [
            None,
            "",
            "invalid",
            "not-a-number",
            pytest.param("Mon, 32 Jan 2024 25:61:61 GMT", id="Bad date format"),
            pytest.param("Tue, 29 Oct 2024 16:56:32", id="Non-GMT timezone"),
        ],
    )
    def test_parse_retry_after_invalid(
        self, caplog: pytest.LogCaptureFixture, header_value: str | None
    ) -> None:
        """Test invalid Retry-After values return None."""
        caplog.set_level(LogLevel.warning)
        assert parse_retry_after(header_value) is None
        if header_value:
            assert "Invalid Retry-After header format" in caplog.text

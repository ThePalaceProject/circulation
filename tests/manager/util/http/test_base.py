from palace.manager.util.http.base import get_series, parse_retry_after


def test_get_series() -> None:
    assert get_series(201) == "2xx"
    assert get_series(399) == "3xx"
    assert get_series(500) == "5xx"


class TestParseRetryAfter:
    """Tests for the parse_retry_after helper function."""

    def test_parse_retry_after_seconds(self):
        """Test parsing delay-seconds format."""
        assert parse_retry_after("120") == 120.0
        assert parse_retry_after("0") == 0.0
        assert parse_retry_after("3.5") == 3.5

    def test_parse_retry_after_http_date(self):
        """Test parsing HTTP-date format."""
        from datetime import datetime, timedelta, timezone

        # Create a date 60 seconds in the future
        future = datetime.now(timezone.utc) + timedelta(seconds=60)
        date_str = future.strftime("%a, %d %b %Y %H:%M:%S GMT")

        result = parse_retry_after(date_str)
        # Should be approximately 60 seconds (allow small variance)
        assert result is not None
        assert 58 <= result <= 62

    def test_parse_retry_after_past_date(self):
        """Test that past dates return 0."""
        from datetime import datetime, timedelta, timezone

        # Create a date 60 seconds in the past
        past = datetime.now(timezone.utc) - timedelta(seconds=60)
        date_str = past.strftime("%a, %d %b %Y %H:%M:%S GMT")

        result = parse_retry_after(date_str)
        assert result == 0  # Past dates should return 0

    def test_parse_retry_after_invalid(self):
        """Test invalid Retry-After values return None."""
        assert parse_retry_after(None) is None
        assert parse_retry_after("") is None
        assert parse_retry_after("invalid") is None
        assert parse_retry_after("not-a-number") is None
        assert (
            parse_retry_after("Mon, 32 Jan 2024 25:61:61 GMT") is None
        )  # Invalid date

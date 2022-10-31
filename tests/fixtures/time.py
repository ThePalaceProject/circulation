from datetime import datetime, timedelta

import pytest

from core.util.datetime_helpers import datetime_utc


class Time:
    _time_counter: datetime

    def __init__(self):
        self._time_counter = datetime_utc(2014, 1, 1)

    @staticmethod
    def time_eq(a, b):
        """Assert that two times are *approximately* the same -- within 2 seconds."""
        if a < b:
            delta = b - a
        else:
            delta = a - b
        total_seconds = delta.total_seconds()
        assert total_seconds < 2, "Delta was too large: %.2f seconds." % total_seconds

    def time(self) -> datetime:
        v = self._time_counter
        self._time_counter = self._time_counter + timedelta(days=1)
        return v


@pytest.fixture(scope="function")
def time_fixture() -> Time:
    return Time()

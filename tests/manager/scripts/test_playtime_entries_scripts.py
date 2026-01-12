from __future__ import annotations

import re
from datetime import datetime
from unittest.mock import MagicMock

import pytest
import pytz
from freezegun import freeze_time

from palace.manager.scripts.playtime_entries import (
    PlaytimeEntriesReportsScript,
)
from palace.manager.util.datetime_helpers import datetime_utc, utc_now


class TestPlaytimeEntriesReportScript:

    @pytest.mark.parametrize(
        "current_utc_time, start_arg, expected_start, until_arg, expected_until",
        [
            # Default values from two dates within the same month (next two cases).
            [
                datetime(2020, 1, 1, 0, 0, 0),
                None,
                datetime_utc(2019, 12, 1, 0, 0, 0),
                None,
                datetime_utc(2020, 1, 1, 0, 0, 0),
            ],
            [
                datetime(2020, 1, 31, 0, 0, 0),
                None,
                datetime_utc(2019, 12, 1, 0, 0, 0),
                None,
                datetime_utc(2020, 1, 1, 0, 0, 0),
            ],
            # `start` specified, `until` defaulted.
            [
                datetime(2020, 1, 31, 0, 0, 0),
                "2019-06-11",
                datetime_utc(2019, 6, 11, 0, 0, 0),
                None,
                datetime_utc(2020, 1, 1, 0, 0, 0),
            ],
            # `start` defaulted, `until` specified.
            [
                datetime(2020, 1, 31, 0, 0, 0),
                None,
                datetime_utc(2019, 12, 1, 0, 0, 0),
                "2019-12-20",
                datetime_utc(2019, 12, 20, 0, 0, 0),
            ],
            # When both dates are specified, the current datetime doesn't matter.
            # Both dates specified, but we test at a specific time here anyway.
            [
                datetime(2020, 1, 31, 0, 0, 0),
                "2018-07-03",
                datetime_utc(2018, 7, 3, 0, 0, 0),
                "2019-04-30",
                datetime_utc(2019, 4, 30, 0, 0, 0),
            ],
            # The same dates are specified, but we test at the actual current time.
            [
                utc_now(),
                "2018-07-03",
                datetime_utc(2018, 7, 3, 0, 0, 0),
                "2019-04-30",
                datetime_utc(2019, 4, 30, 0, 0, 0),
            ],
            # The same dates are specified, but we test at the actual current time.
            [
                utc_now(),
                "4099-07-03",
                datetime_utc(4099, 7, 3, 0, 0, 0),
                "4150-04-30",
                datetime_utc(4150, 4, 30, 0, 0, 0),
            ],
        ],
    )
    def test_parse_command_line(
        self,
        current_utc_time: datetime,
        start_arg: str | None,
        expected_start: datetime,
        until_arg: str | None,
        expected_until: datetime,
    ):
        start_args = ["--start", start_arg] if start_arg else []
        until_args = ["--until", until_arg] if until_arg else []
        cmd_args = start_args + until_args

        mock_db_session = MagicMock()

        with freeze_time(current_utc_time):
            parsed = PlaytimeEntriesReportsScript.parse_command_line(
                mock_db_session, cmd_args=cmd_args
            )
        assert expected_start == parsed.start
        assert expected_until == parsed.until
        assert pytz.UTC == parsed.start.tzinfo
        assert pytz.UTC == parsed.until.tzinfo

    @pytest.mark.parametrize(
        "current_utc_time, start_arg, expected_start, until_arg, expected_until",
        [
            # `start` specified, `until` defaulted.
            [
                datetime(2020, 1, 31, 0, 0, 0),
                "2020-02-01",
                datetime_utc(2020, 2, 1, 0, 0, 0),
                None,
                datetime_utc(2020, 1, 1, 0, 0, 0),
            ],
            # `start` defaulted, `until` specified.
            [
                datetime(2020, 1, 31, 0, 0, 0),
                None,
                datetime_utc(2019, 12, 1, 0, 0, 0),
                "2019-06-11",
                datetime_utc(2019, 6, 11, 0, 0, 0),
            ],
            # When both dates are specified, the current datetime doesn't matter.
            # Both dates specified, but we test at a specific time here anyway.
            [
                datetime(2020, 1, 31, 0, 0, 0),
                "2019-04-30",
                datetime_utc(2019, 4, 30, 0, 0, 0),
                "2018-07-03",
                datetime_utc(2018, 7, 3, 0, 0, 0),
            ],
            # The same dates are specified, but we test at the actual current time.
            [
                utc_now(),
                "2019-04-30",
                datetime_utc(2019, 4, 30, 0, 0, 0),
                "2018-07-03",
                datetime_utc(2018, 7, 3, 0, 0, 0),
            ],
            # The same dates are specified, but we test at the actual current time.
            [
                utc_now(),
                "4150-04-30",
                datetime_utc(4150, 4, 30, 0, 0, 0),
                "4099-07-03",
                datetime_utc(4099, 7, 3, 0, 0, 0),
            ],
        ],
    )
    def test_parse_command_line_start_not_before_until(
        self,
        capsys,
        current_utc_time: datetime,
        start_arg: str | None,
        expected_start: datetime,
        until_arg: str | None,
        expected_until: datetime,
    ):
        start_args = ["--start", start_arg] if start_arg else []
        until_args = ["--until", until_arg] if until_arg else []
        cmd_args = start_args + until_args

        mock_db_session = MagicMock()

        with freeze_time(current_utc_time), pytest.raises(SystemExit) as excinfo:
            parsed = PlaytimeEntriesReportsScript.parse_command_line(
                mock_db_session, cmd_args=cmd_args
            )
        _, err = capsys.readouterr()
        assert 2 == excinfo.value.code
        assert re.search(r"start date \(.*\) must be before until date \(.*\).", err)

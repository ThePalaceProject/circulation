from __future__ import annotations

import pytest

from palace.manager.scripts.base import Script
from palace.manager.util.datetime_helpers import datetime_utc
from tests.fixtures.database import DatabaseTransactionFixture


class TestScript:
    def test_parse_time(self):
        reference_date = datetime_utc(2016, 1, 1)

        assert Script.parse_time("2016-01-01") == reference_date
        assert Script.parse_time("2016-1-1") == reference_date
        assert Script.parse_time("1/1/2016") == reference_date
        assert Script.parse_time("20160101") == reference_date

        pytest.raises(ValueError, Script.parse_time, "201601-01")

    def test_script_name(self, db: DatabaseTransactionFixture):
        session = db.session

        class Sample(Script):
            pass

        # If a script does not define .name, its class name
        # is treated as the script name.
        script = Sample(session)
        assert "Sample" == script.script_name

        # If a script does define .name, that's used instead.
        script.name = "I'm a script"  # type: ignore[attr-defined]
        assert script.name == script.script_name  # type: ignore[attr-defined]

from __future__ import annotations

from palace.manager.scripts.coverage_provider import (
    RunCoverageProviderScript,
)
from palace.manager.util.datetime_helpers import datetime_utc
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks.stdin import MockStdin


class TestRunCoverageProviderScript:
    def test_parse_command_line(self, db: DatabaseTransactionFixture):
        identifier = db.identifier()
        cmd_args = [
            "--cutoff-time",
            "2016-05-01",
            "--identifier-type",
            identifier.type,
            identifier.identifier,
        ]
        parsed = RunCoverageProviderScript.parse_command_line(
            db.session, cmd_args, MockStdin()
        )
        assert datetime_utc(2016, 5, 1) == parsed.cutoff_time
        assert [identifier] == parsed.identifiers
        assert identifier.type == parsed.identifier_type

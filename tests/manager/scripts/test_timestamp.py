from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from palace.manager.scripts.base import Script
from palace.manager.scripts.timestamp import TimestampScript
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class TestTimestampScript:
    @staticmethod
    def _ts(session: Session, script):
        """Convenience method to look up the Timestamp for a script.

        We don't use Timestamp.stamp() because we want to make sure
        that Timestamps are being created by the actual code, not test
        code.
        """
        return get_one(session, Timestamp, service=script.script_name)

    def test_update_timestamp(self, db: DatabaseTransactionFixture):
        # Test the Script subclass that sets a timestamp after a
        # script is run.
        class Noisy(TimestampScript):
            def do_run(self):
                pass

        script = Noisy(db.session)
        script.run()

        timestamp = self._ts(db.session, script)

        # The start and end points of do_run() have become
        # Timestamp.start and Timestamp.finish.
        now = utc_now()
        assert (now - timestamp.start).total_seconds() < 5
        assert (now - timestamp.finish).total_seconds() < 5
        assert timestamp.start < timestamp.finish
        assert None == timestamp.collection

    def test_update_timestamp_with_collection(self, db: DatabaseTransactionFixture):
        # A script can indicate that it is operating on a specific
        # collection.
        class MyCollection(TimestampScript):
            def do_run(self):
                pass

        script = MyCollection(db.session)
        script.timestamp_collection = db.default_collection()
        script.run()
        timestamp = self._ts(db.session, script)
        assert db.default_collection() == timestamp.collection

    def test_update_timestamp_on_failure(self, db: DatabaseTransactionFixture):
        # A TimestampScript that fails to complete still has its
        # Timestamp set -- the timestamp just records the time that
        # the script stopped running.
        #
        # This is different from Monitors, where the timestamp
        # is only updated when the Monitor runs to completion.
        # The difference is that Monitors are frequently responsible for
        # keeping track of everything that happened since a certain
        # time, and Scripts generally aren't.
        class Broken(TimestampScript):
            def do_run(self):
                raise Exception("i'm broken")

        script = Broken(db.session)
        with pytest.raises(Exception) as excinfo:
            script.run()
        assert "i'm broken" in str(excinfo.value)
        timestamp = self._ts(db.session, script)

        now = utc_now()
        assert (now - timestamp.finish).total_seconds() < 5

        # A stack trace for the exception has been recorded in the
        # Timestamp object.
        assert "Exception: i'm broken" in timestamp.exception

    def test_normal_script_has_no_timestamp(self, db: DatabaseTransactionFixture):
        # Running a normal script does _not_ set a Timestamp.
        class Silent(Script):
            def do_run(self):
                pass

        script = Silent(db.session)
        script.run()
        assert None == self._ts(db.session, script)

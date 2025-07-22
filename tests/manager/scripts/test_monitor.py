from __future__ import annotations

import pytest

from palace.manager.core.monitor import CollectionMonitor, Monitor
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
from palace.manager.integration.license.opds.opds1 import OPDSAPI
from palace.manager.scripts.monitor import (
    RunCollectionMonitorScript,
    RunMonitorScript,
    RunMultipleMonitorsScript,
)
from tests.fixtures.database import DatabaseTransactionFixture


class SuccessMonitor(Monitor):
    """A simple Monitor that alway succeeds."""

    SERVICE_NAME = "Success"

    def run(self):
        self.ran = True


class OPDSCollectionMonitor(CollectionMonitor):
    """Mock Monitor for use in tests of Run*MonitorScript."""

    SERVICE_NAME = "Test Monitor"
    PROTOCOL = OPDSAPI.label()

    def __init__(self, _db, test_argument=None, **kwargs):
        self.test_argument = test_argument
        super().__init__(_db, **kwargs)

    def run_once(self, progress):
        self.collection.ran_with_argument = self.test_argument


class DoomedCollectionMonitor(CollectionMonitor):
    """Mock CollectionMonitor that always raises an exception."""

    SERVICE_NAME = "Doomed Monitor"
    PROTOCOL = OPDSAPI.label()

    def run(self, *args, **kwargs):
        self.ran = True
        self.collection.doomed = True
        raise Exception("Doomed!")


class TestRunMultipleMonitorsScript:
    def test_do_run(self, db: DatabaseTransactionFixture):
        m1 = SuccessMonitor(db.session)
        m2 = DoomedCollectionMonitor(db.session, db.default_collection())
        m3 = SuccessMonitor(db.session)

        class MockScript(RunMultipleMonitorsScript):
            name = "Run three monitors"

            def monitors(self, **kwargs):
                self.kwargs = kwargs
                return [m1, m2, m3]

        # Run the script.
        script = MockScript(db.session, kwarg="value")
        script.do_run()

        # The kwarg we passed in to the MockScript constructor was
        # propagated into the monitors() method.
        assert dict(kwarg="value") == script.kwargs

        # All three monitors were run, even though the
        # second one raised an exception.
        assert True == m1.ran
        assert True == m2.ran
        assert True == m3.ran

        # The exception that crashed the second monitor was stored as
        # .exception, in case we want to look at it.
        assert "Doomed!" == str(m2.exception)
        assert None == getattr(m1, "exception", None)


class TestCollectionMonitorWithDifferentRunners:
    """CollectionMonitors are usually run by a RunCollectionMonitorScript.
    It's not ideal, but you can also run a CollectionMonitor script from a
    RunMonitorScript. In either case, if no collection argument is specified,
    the monitor will run on every appropriate Collection. If any collection
    names are specified, then the monitor will be run only on the ones specified.
    """

    @pytest.mark.parametrize(
        "name,script_runner",
        [
            ("run CollectionMonitor from RunMonitorScript", RunMonitorScript),
            (
                "run CollectionMonitor from RunCollectionMonitorScript",
                RunCollectionMonitorScript,
            ),
        ],
    )
    def test_run_collection_monitor_with_no_args(self, db, name, script_runner):
        # Run CollectionMonitor via RunMonitor for all applicable collections.
        c1 = db.collection()
        c2 = db.collection()
        script = script_runner(
            OPDSCollectionMonitor, db.session, cmd_args=[], test_argument="test value"
        )
        script.run()
        for c in [c1, c2]:
            assert "test value" == c.ran_with_argument

    @pytest.mark.parametrize(
        "name,script_runner",
        [
            (
                "run CollectionMonitor with collection args from RunMonitorScript",
                RunMonitorScript,
            ),
            (
                "run CollectionMonitor with collection args from RunCollectionMonitorScript",
                RunCollectionMonitorScript,
            ),
        ],
    )
    def test_run_collection_monitor_with_collection_args(self, db, name, script_runner):
        # Run CollectionMonitor via RunMonitor for only specified collections.
        c1 = db.collection(name="Collection 1")
        c2 = db.collection(name="Collection 2")
        c3 = db.collection(name="Collection 3")

        all_collections = [c1, c2, c3]
        monitored_collections = [c1, c3]
        monitored_names = [c.name for c in monitored_collections]
        script = script_runner(
            OPDSCollectionMonitor,
            db.session,
            cmd_args=monitored_names,
            test_argument="test value",
        )
        script.run()
        for c in monitored_collections:
            assert hasattr(c, "ran_with_argument")
            assert "test value" == c.ran_with_argument
        for c in [
            collection
            for collection in all_collections
            if collection not in monitored_collections
        ]:
            assert not hasattr(c, "ran_with_argument")


class TestRunCollectionMonitorScript:
    def test_monitors(self, db: DatabaseTransactionFixture):
        # Here we have three OPDS import Collections...
        o1 = db.collection()
        o2 = db.collection()
        o3 = db.collection()

        # ...and a Bibliotheca collection.
        b1 = db.collection(protocol=BibliothecaAPI)

        script = RunCollectionMonitorScript(
            OPDSCollectionMonitor, db.session, cmd_args=[]
        )

        # Calling monitors() instantiates an OPDSCollectionMonitor
        # for every OPDS import collection. The Bibliotheca collection
        # is unaffected.
        monitors = script.monitors()
        collections = [x.collection for x in monitors]
        assert set(collections) == {o1, o2, o3}
        for monitor in monitors:
            assert isinstance(monitor, OPDSCollectionMonitor)

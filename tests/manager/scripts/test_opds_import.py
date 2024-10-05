from __future__ import annotations

from palace.manager.core.opds_import import OPDSImportMonitor
from palace.manager.scripts.opds_import import OPDSImportScript
from tests.fixtures.database import DatabaseTransactionFixture


# Mock classes used by TestOPDSImportScript
class MockOPDSImportMonitor:
    """Pretend to monitor an OPDS feed for new titles."""

    INSTANCES: list[MockOPDSImportMonitor] = []

    def __init__(self, _db, collection, *args, **kwargs):
        self.collection = collection
        self.args = args
        self.kwargs = kwargs
        self.INSTANCES.append(self)
        self.was_run = False

    def run(self):
        self.was_run = True


class MockOPDSImporter:
    """Pretend to import titles from an OPDS feed."""


class MockOPDSImportScript(OPDSImportScript):
    """Actually instantiate a monitor that will pretend to do something."""

    MONITOR_CLASS: type[OPDSImportMonitor] = MockOPDSImportMonitor  # type: ignore
    IMPORTER_CLASS = MockOPDSImporter  # type: ignore


class TestOPDSImportScript:
    def test_do_run(self, db: DatabaseTransactionFixture):
        # Create a collection to use as the default
        db.default_collection()

        script = MockOPDSImportScript(db.session)
        script.do_run([])

        # Since we provided no collection, a MockOPDSImportMonitor
        # was instantiated for each OPDS Import collection in the database.
        monitor = MockOPDSImportMonitor.INSTANCES.pop()
        assert db.default_collection() == monitor.collection

        args = ["--collection=%s" % db.default_collection().name]
        script.do_run(args)

        # If we provide the collection name, a MockOPDSImportMonitor is
        # also instantiated.
        monitor = MockOPDSImportMonitor.INSTANCES.pop()
        assert db.default_collection() == monitor.collection
        assert True == monitor.was_run

        # Our replacement OPDS importer class was passed in to the
        # monitor constructor. If this had been a real monitor, that's the
        # code we would have used to import OPDS feeds.
        assert MockOPDSImporter == monitor.kwargs["import_class"]
        assert False == monitor.kwargs["force_reimport"]

        # Setting --force changes the 'force_reimport' argument
        # passed to the monitor constructor.
        args.append("--force")
        script.do_run(args)
        monitor = MockOPDSImportMonitor.INSTANCES.pop()
        assert db.default_collection() == monitor.collection
        assert True == monitor.kwargs["force_reimport"]

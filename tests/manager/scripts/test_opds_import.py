from __future__ import annotations

from palace.manager.integration.license.opds.opds1.monitor import OPDSImportMonitor
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

    @classmethod
    def reset(cls):
        cls.INSTANCES.clear()


class MockOPDSImporter:
    """Pretend to import titles from an OPDS feed."""


class MockOPDSImportScript(OPDSImportScript):
    """Actually instantiate a monitor that will pretend to do something."""

    MONITOR_CLASS: type[OPDSImportMonitor] = MockOPDSImportMonitor  # type: ignore
    IMPORTER_CLASS = MockOPDSImporter  # type: ignore


class TestOPDSImportScript:
    def test_do_run(self, db: DatabaseTransactionFixture):
        # Create the default collections: active and inactive.
        db.make_default_library_with_collections()
        script = MockOPDSImportScript(db.session)

        # Run the script with no arguments.
        MockOPDSImportMonitor.reset()
        script.do_run([])

        # Since we provided no collection, a MockOPDSImportMonitor is
        # instantiated for each OPDS Import collection in the database,
        # both the active one and the inactive one.
        monitor_collections = {x.collection for x in MockOPDSImportMonitor.INSTANCES}
        assert len(MockOPDSImportMonitor.INSTANCES) == 2
        assert monitor_collections == {
            db.default_collection(),
            db.default_inactive_collection(),
        }

        # If we provide one or more collection names, then `MockOPDSImportMonitor`s
        # are instantiated for only the specified collections
        MockOPDSImportMonitor.reset()
        args = [f"--collection={db.default_collection().name}"]
        script.do_run(args)

        monitor_collections = {x.collection for x in MockOPDSImportMonitor.INSTANCES}
        assert len(MockOPDSImportMonitor.INSTANCES) == 1
        [monitor] = MockOPDSImportMonitor.INSTANCES
        assert monitor_collections == {db.default_collection()}

        # Our replacement OPDS importer class was passed in to the
        # monitor constructor. If this had been a real monitor, that's the
        # code we would have used to import OPDS feeds.
        assert MockOPDSImporter == monitor.kwargs["import_class"]
        assert monitor.kwargs["force_reimport"] is False

        # Adding --force changes the 'force_reimport' argument
        # passed to the monitor constructor.
        MockOPDSImportMonitor.reset()
        args.append("--force")
        script.do_run(args)
        [monitor] = MockOPDSImportMonitor.INSTANCES
        assert db.default_collection() == monitor.collection
        assert monitor.kwargs["force_reimport"] is True

from __future__ import annotations

from palace.manager.integration.license.opds.for_distributors.importer import (
    OPDSForDistributorsImporter,
)
from palace.manager.integration.license.opds.for_distributors.monitor import (
    OPDSForDistributorsImportMonitor,
    OPDSForDistributorsReaperMonitor,
)
from palace.manager.integration.license.opds.opds1.api import OPDSAPI
from palace.manager.integration.license.opds.opds1.importer import OPDSImporter
from palace.manager.integration.license.opds.opds1.monitor import OPDSImportMonitor
from palace.manager.scripts.input import CollectionInputScript
from palace.manager.sqlalchemy.model.collection import Collection


class OPDSImportScript(CollectionInputScript):
    """Import all books from the OPDS feed associated with a collection."""

    name = "Import all books from the OPDS feed associated with a collection."

    IMPORTER_CLASS = OPDSImporter
    MONITOR_CLASS: type[OPDSImportMonitor] = OPDSImportMonitor
    PROTOCOL = OPDSAPI.label()

    def __init__(
        self,
        _db=None,
        importer_class=None,
        monitor_class=None,
        protocol=None,
        *args,
        **kwargs,
    ):
        super().__init__(_db, *args, **kwargs)
        self.importer_class = importer_class or self.IMPORTER_CLASS
        self.monitor_class = monitor_class or self.MONITOR_CLASS
        self.protocol = protocol or self.PROTOCOL
        self.importer_kwargs = kwargs

    @classmethod
    def arg_parser(cls):
        parser = CollectionInputScript.arg_parser()
        parser.add_argument(
            "--force",
            help="Import the feed from scratch, even if it seems like it was already imported.",
            dest="force",
            action="store_true",
        )
        return parser

    def do_run(self, cmd_args=None):
        parsed = self.parse_command_line(self._db, cmd_args=cmd_args)
        collections = parsed.collections or Collection.by_protocol(
            self._db, self.protocol
        )
        for collection in collections:
            self.run_monitor(collection, force=parsed.force)

    def run_monitor(self, collection, force=None):
        monitor = self.monitor_class(
            self._db,
            collection,
            import_class=self.importer_class,
            force_reimport=force,
            **self.importer_kwargs,
        )
        monitor.run()


class OPDSForDistributorsImportScript(OPDSImportScript):
    """Import all books from the OPDS feed associated with a collection
    that requires authentication."""

    IMPORTER_CLASS = OPDSForDistributorsImporter
    MONITOR_CLASS = OPDSForDistributorsImportMonitor
    PROTOCOL = OPDSForDistributorsImporter.NAME


class OPDSForDistributorsReaperScript(OPDSImportScript):
    """Get all books from the OPDS feed associated with a collection
    to find out if any have been removed."""

    IMPORTER_CLASS = OPDSForDistributorsImporter
    MONITOR_CLASS = OPDSForDistributorsReaperMonitor
    PROTOCOL = OPDSForDistributorsImporter.NAME

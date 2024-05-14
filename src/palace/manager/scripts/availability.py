from __future__ import annotations

from palace.manager.api.axis import Axis360BibliographicCoverageProvider
from palace.manager.api.bibliotheca import BibliothecaCirculationSweep
from palace.manager.api.overdrive import OverdriveAPI
from palace.manager.scripts.input import IdentifierInputScript
from palace.manager.sqlalchemy.model.identifier import Identifier


class AvailabilityRefreshScript(IdentifierInputScript):
    """Refresh the availability information for a LicensePool, direct from the
    license source.
    """

    def do_run(self):
        args = self.parse_command_line(self._db)
        if not args.identifiers:
            raise Exception("You must specify at least one identifier to refresh.")

        # We don't know exactly how big to make these batches, but 10 is
        # always safe.
        start = 0
        size = 10
        while start < len(args.identifiers):
            batch = args.identifiers[start : start + size]
            self.refresh_availability(batch)
            self._db.commit()
            start += size

    def refresh_availability(self, identifiers):
        provider = None
        identifier = identifiers[0]
        if identifier.type == Identifier.THREEM_ID:
            sweeper = BibliothecaCirculationSweep(self._db)
            sweeper.process_batch(identifiers)
        elif identifier.type == Identifier.OVERDRIVE_ID:
            api = OverdriveAPI(self._db)
            for identifier in identifiers:
                api.update_licensepool(identifier.identifier)
        elif identifier.type == Identifier.AXIS_360_ID:
            provider = Axis360BibliographicCoverageProvider(self._db)
            provider.process_batch(identifiers)
        else:
            self.log.warn("Cannot update coverage for %r" % identifier.type)

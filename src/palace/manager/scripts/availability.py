from __future__ import annotations

from collections.abc import Sequence

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.integration.license.bibliotheca import BibliothecaCirculationSweep
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.scripts.input import IdentifierInputScript
from palace.manager.sqlalchemy.model.identifier import Identifier


class AvailabilityRefreshScript(IdentifierInputScript):
    """Refresh the availability information for a LicensePool, direct from the
    license source.
    """

    def do_run(self) -> None:
        args = self.parse_command_line(self._db)
        if not args.identifiers:
            raise PalaceValueError(
                "You must specify at least one identifier to refresh."
            )

        # We don't know exactly how big to make these batches, but 10 is
        # always safe.
        start = 0
        size = 10
        while start < len(args.identifiers):
            batch = args.identifiers[start : start + size]
            self.refresh_availability(batch)
            self._db.commit()
            start += size

    def refresh_availability(self, identifiers: Sequence[Identifier]) -> None:
        identifier = identifiers[0]

        pool = next(iter(identifier.licensed_through), None)
        if pool is None:
            self.log.warning(
                f"Cannot update coverage for {identifier!r}. No license pool available."
            )
            return
        collection = pool.collection

        if identifier.type == Identifier.BIBLIOTHECA_ID:
            sweeper = BibliothecaCirculationSweep(self._db, collection)
            sweeper.process_batch(identifiers)
        elif identifier.type == Identifier.OVERDRIVE_ID:
            api = OverdriveAPI(self._db, collection)
            for identifier in identifiers:
                api.update_licensepool(identifier.identifier)
        else:
            self.log.warning(
                f"Cannot update coverage for {identifier!r}. Unsupported type."
            )

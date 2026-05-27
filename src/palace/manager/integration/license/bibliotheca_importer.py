"""Importer for Bibliotheca (3M Cloud) circulation events."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from palace.util.log import LoggerMixin

from palace.manager.celery.tasks import apply
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool

EVENT_IMPORT_SERVICE_NAME = "Bibliotheca Event Import"

_LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

# Amount of time to overlap between consecutive import runs to reduce
# the risk of missing events at the boundary.
EVENT_IMPORT_OVERLAP = timedelta(minutes=5)

# Default slice size: one 5-minute window per task invocation.  Intentionally
# equal to EVENT_IMPORT_OVERLAP so that each slice re-covers the tail of the
# previous one, minimising the chance of missing events at the boundary.
DEFAULT_SLICE_SIZE = timedelta(minutes=5)


@dataclass(frozen=True)
class SliceImportResult:
    """Result of importing a single time slice of Bibliotheca events.

    :param events_handled: Number of events processed in this slice.
    :param slice_start: Start of the slice that was processed.
    :param slice_end: End of the slice that was processed.
    """

    events_handled: int
    slice_start: datetime
    slice_end: datetime


class BibliothecaEventImporter(LoggerMixin):
    """Imports near-real-time circulation events from the Bibliotheca API.

    Processes one time slice per invocation.  Callers are responsible for
    chaining slices until the collection is caught up.
    """

    def __init__(
        self,
        session: Session,
        collection: Collection,
        api: BibliothecaAPI | None = None,
    ) -> None:
        """
        :param session: Database session.
        :param collection: The Bibliotheca collection to import events for.
        :param api: Optional pre-constructed API instance; created from
            ``session`` and ``collection`` if not supplied.
        """
        self._session = session
        self._collection = collection
        self._api = api or BibliothecaAPI(session, collection)

    def get_start(self, cutoff: datetime) -> datetime:
        """Derive the start of the next slice from the stored ``Timestamp``.

        Falls back to ``cutoff - EVENT_IMPORT_OVERLAP`` when no prior run
        has been recorded, so the first run processes a small warm-up window
        rather than attempting to fetch the entire event history.

        :param cutoff: The upper bound of the import window (typically
            ``utc_now() - EVENT_IMPORT_OVERLAP``).
        :returns: The start datetime for the next import slice.
        """
        timestamp = Timestamp.lookup(
            self._session,
            EVENT_IMPORT_SERVICE_NAME,
            Timestamp.TASK_TYPE,
            self._collection,
        )
        if timestamp is None or timestamp.finish is None:
            return cutoff - EVENT_IMPORT_OVERLAP
        finish: datetime = timestamp.finish
        return finish - EVENT_IMPORT_OVERLAP

    def import_time_slice(
        self,
        start: datetime,
        cutoff: datetime,
        slice_size: timedelta = DEFAULT_SLICE_SIZE,
    ) -> SliceImportResult:
        """Import one slice of circulation events and stamp the ``Timestamp``.

        Fetches events for the window ``[start, slice_end]`` where
        ``slice_end = min(start + slice_size, cutoff)``, processes each
        event, then updates the stored ``Timestamp`` so that the next call
        to :meth:`get_start` resumes from ``slice_end``.

        :param start: Start of the slice to process.
        :param cutoff: Upper bound of the import window; the slice will not
            extend past this point.
        :param slice_size: Maximum duration of a single slice.  Defaults to
            :data:`DEFAULT_SLICE_SIZE` (5 minutes).
        :returns: A :class:`SliceImportResult` describing what was processed.
        """
        slice_end = min(start + slice_size, cutoff)
        events_handled = 0

        self.log.info(
            f"Bibliotheca event import: requesting events for "
            f"'{self._collection.name}' between "
            f"{start.strftime(_LOG_DATE_FORMAT)} and "
            f"{slice_end.strftime(_LOG_DATE_FORMAT)}."
        )

        for (
            bibliotheca_id,
            isbn,
            _foreign_patron_id,
            start_time,
            _end_time,
            internal_event_type,
        ) in self._api.get_events_between(start, slice_end):
            self._handle_event(bibliotheca_id, isbn, start_time, internal_event_type)
            events_handled += 1

        Timestamp.stamp(
            self._session,
            service=EVENT_IMPORT_SERVICE_NAME,
            service_type=Timestamp.TASK_TYPE,
            collection=self._collection,
            start=start,
            finish=slice_end,
            achievements=f"Events handled: {events_handled}.",
        )

        return SliceImportResult(
            events_handled=events_handled,
            slice_start=start,
            slice_end=slice_end,
        )

    def _handle_event(
        self,
        bibliotheca_id: str,
        isbn: str,
        start_time: datetime,
        internal_event_type: str,
    ) -> None:
        """Process a single Bibliotheca circulation event.

        Creates or updates the ``LicensePool``, links the ISBN identifier,
        adjusts availability based on the event delta, and queues a
        ``bibliographic_apply`` task when the title's metadata has changed
        (hash-based deduplication).

        :param bibliotheca_id: Bibliotheca's identifier for the item.
        :param isbn: ISBN of the item, used to create an equivalency link.
        :param start_time: Timestamp of the event.
        :param internal_event_type: Normalised circulation event type (e.g.
            ``CirculationEvent.DISTRIBUTOR_LICENSE_ADD``).
        """
        license_pool, _ = LicensePool.for_foreign_id(
            self._session,
            self._api.data_source,
            Identifier.BIBLIOTHECA_ID,
            bibliotheca_id,
            collection=self._collection,
        )

        # Fetch bibliographic metadata and queue an apply task only if the
        # content hash differs from what is already stored.
        for bibliographic in self._api.bibliographic_lookup(bibliotheca_id):
            if bibliographic.needs_apply(self._session):
                apply.bibliographic_apply.delay(
                    bibliographic,
                    collection_id=self._collection.id,
                    replace=ReplacementPolicy.from_license_source(),
                )

        bibliotheca_identifier = license_pool.identifier
        isbn_identifier, _ = Identifier.for_foreign_id(
            self._session, Identifier.ISBN, isbn
        )

        edition, _ = Edition.for_foreign_id(
            self._session,
            self._api.data_source,
            Identifier.BIBLIOTHECA_ID,
            bibliotheca_id,
        )

        bibliotheca_identifier.equivalent_to(
            self._api.data_source, isbn_identifier, strength=1
        )

        license_pool.update_availability_from_delta(internal_event_type, start_time, 1)

        self.log.info(
            "%s %s: %s",
            start_time.strftime(_LOG_DATE_FORMAT),
            edition.title or "[no title]",
            internal_event_type,
        )

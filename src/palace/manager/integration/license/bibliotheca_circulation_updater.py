"""Updater for Bibliotheca (3M Cloud) circulation availability data."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.orm import Session

from palace.util.datetime_helpers import utc_now
from palace.util.log import LoggerMixin

from palace.manager.celery.tasks import apply
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
from palace.manager.sqlalchemy.constants import DataSourceConstants
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool

CIRCULATION_UPDATE_SERVICE_NAME = "Bibliotheca Circulation Update"
CIRCULATION_UPDATE_BATCH_SIZE = 25


@dataclass(frozen=True)
class BatchUpdateResult:
    """Result of processing one batch of identifiers during a circulation sweep.

    :param records_handled: Number of identifiers processed in this batch.
    :param next_offset: The last identifier DB ID processed, used as the
        starting point for the next batch. ``None`` when the sweep is
        complete (batch was smaller than :data:`CIRCULATION_UPDATE_BATCH_SIZE`).
    """

    records_handled: int
    next_offset: int | None


class BibliothecaCirculationUpdater(LoggerMixin):
    """Updates circulation availability for all Bibliotheca identifiers in a collection.

    Each call to :meth:`update_batch` processes up to
    :data:`CIRCULATION_UPDATE_BATCH_SIZE` identifiers ordered by DB ID,
    queuing asynchronous metadata/availability updates for changed titles and
    zeroing out any titles no longer recognised by Bibliotheca.

    Progress is tracked via :attr:`~palace.manager.sqlalchemy.model.coverage.Timestamp.counter`
    (the last identifier ID seen).  When the final batch is smaller than the
    batch size, the counter resets to 0 and :attr:`~palace.manager.sqlalchemy.model.coverage.Timestamp.finish`
    is stamped so the next beat trigger restarts from the beginning.
    """

    def __init__(
        self,
        session: Session,
        collection: Collection,
        api: BibliothecaAPI | None = None,
    ) -> None:
        """
        :param session: Database session.
        :param collection: The Bibliotheca collection to update.
        :param api: Optional pre-constructed API instance; created from
            ``session`` and ``collection`` if not supplied.
        """
        self._session = session
        self._collection = collection
        self._api = api or BibliothecaAPI(session, collection)

    def get_offset(self) -> int:
        """Return the last identifier ID processed in a prior run.

        Reads :attr:`~palace.manager.sqlalchemy.model.coverage.Timestamp.counter`
        for this collection.  Returns ``0`` when no prior run has been recorded
        or when the counter was reset after a completed sweep, so the next run
        starts from the beginning of the collection.

        :returns: The offset (identifier DB ID) to start from.
        """
        ts = Timestamp.lookup(
            self._session,
            CIRCULATION_UPDATE_SERVICE_NAME,
            Timestamp.TASK_TYPE,
            self._collection,
        )
        if ts is None or ts.counter is None:
            return 0
        return int(ts.counter)

    def update_batch(self, offset: int = 0) -> BatchUpdateResult:
        """Process one batch of identifiers starting after ``offset``.

        Fetches up to :data:`CIRCULATION_UPDATE_BATCH_SIZE` identifiers with
        ``id > offset`` licensed through this collection, processes them via
        :meth:`_process_batch`, then updates the
        :attr:`~palace.manager.sqlalchemy.model.coverage.Timestamp.counter`:

        - **Full batch**: counter is set to the last identifier's DB ID so the
          next invocation continues where this one left off.
        - **Partial batch** (sweep complete): counter is reset to ``0`` and
          :attr:`~palace.manager.sqlalchemy.model.coverage.Timestamp.finish`
          is stamped with the current time.

        :param offset: DB ID of the last identifier processed in the previous
            batch.  ``0`` starts from the beginning.
        :returns: A :class:`BatchUpdateResult` whose ``next_offset`` is ``None``
            when the sweep is complete.
        """
        stmt = (
            select(Identifier)
            .join(Identifier.licensed_through)
            .filter(
                LicensePool.collection_id == self._collection.id,
                Identifier.id > offset,
            )
            .order_by(Identifier.id)
            .distinct()
            .limit(CIRCULATION_UPDATE_BATCH_SIZE)
        )
        # .unique() is required because Identifier has eager-loaded collection
        # relationships that cause SQLAlchemy to raise if it is omitted.
        # .distinct() (on the statement) ensures LIMIT is applied to distinct
        # rows at the database level so the page boundary is reliable regardless
        # of any join multiplicity.
        identifiers: list[Identifier] = list(self._session.scalars(stmt).unique().all())

        if identifiers:
            self._process_batch(identifiers)

        records_handled = len(identifiers)
        sweep_complete = records_handled < CIRCULATION_UPDATE_BATCH_SIZE

        if sweep_complete:
            Timestamp.stamp(
                self._session,
                service=CIRCULATION_UPDATE_SERVICE_NAME,
                service_type=Timestamp.TASK_TYPE,
                collection=self._collection,
                finish=utc_now(),
                counter=0,
            )
            return BatchUpdateResult(records_handled=records_handled, next_offset=None)
        else:
            last_id = identifiers[-1].id
            Timestamp.stamp(
                self._session,
                service=CIRCULATION_UPDATE_SERVICE_NAME,
                service_type=Timestamp.TASK_TYPE,
                collection=self._collection,
                counter=last_id,
            )
            return BatchUpdateResult(
                records_handled=records_handled, next_offset=last_id
            )

    def process_identifiers(self, identifiers: Sequence[Identifier]) -> None:
        """Process a caller-supplied list of identifiers without touching the Timestamp.

        Used by :meth:`~palace.manager.integration.license.bibliotheca.BibliothecaAPI.update_availability`
        and :class:`~palace.manager.scripts.availability.AvailabilityRefreshScript` for
        on-demand single-title refreshes.  Applies the same hash-based deduplication
        as :meth:`update_batch` so unchanged titles produce no database writes.

        Unlike the sweep (:meth:`update_batch`), changes are applied **synchronously**
        in the caller's session rather than queued as ``bibliographic_apply`` tasks, so
        the updated availability is visible as soon as this method returns.  Callers such
        as :meth:`~palace.manager.integration.license.bibliotheca.BibliothecaAPI.update_availability`
        rely on this — e.g. the circulation dispatcher reads ``LicensePool.licenses_available``
        immediately after requesting an availability refresh.

        :param identifiers: Identifiers to process.
        """
        self._process_batch(list(identifiers), synchronous=True)

    def _process_batch(
        self, identifiers: list[Identifier], *, synchronous: bool = False
    ) -> None:
        """Look up availability from Bibliotheca, apply changes, and zero out removed titles.

        For each :class:`~palace.manager.data_layer.bibliographic.BibliographicData`
        returned by the API, applies the change when
        :meth:`~palace.manager.data_layer.bibliographic.BibliographicData.needs_apply`
        returns ``True`` (hash-based deduplication).  When ``synchronous`` is ``False``
        (the sweep) the apply is queued as a ``bibliographic_apply`` Celery task; when
        ``True`` (on-demand refreshes) it is applied directly in this session so the
        result is immediately visible to the caller.

        For identifiers the API does not return (indicating removed or expired
        licenses), calls
        :meth:`~palace.manager.sqlalchemy.model.licensing.LicensePool.update_availability`
        with all-zero counts.

        :param identifiers: Identifiers to process.
        :param synchronous: When ``True``, apply changes in-band instead of queuing
            asynchronous ``bibliographic_apply`` tasks.
        """
        identifiers_by_bibliotheca_id: dict[str, Identifier] = {
            i.identifier: i for i in identifiers
        }
        bibliotheca_ids = set(identifiers_by_bibliotheca_id.keys())
        identifiers_not_mentioned: set[Identifier] = set(identifiers)

        for bibliographic in self._api.bibliographic_lookup(bibliotheca_ids):
            bibliotheca_id = (
                bibliographic.primary_identifier_data.identifier
                if bibliographic.primary_identifier_data
                else None
            )
            if bibliotheca_id and bibliotheca_id in identifiers_by_bibliotheca_id:
                identifiers_not_mentioned.discard(
                    identifiers_by_bibliotheca_id[bibliotheca_id]
                )

            if bibliographic.needs_apply(self._session):
                if synchronous:
                    # Apply in-band (mirrors the apply.bibliographic_apply task body)
                    # so the caller sees the updated availability in its own session.
                    edition, _ = bibliographic.edition(self._session)
                    bibliographic.apply(
                        self._session,
                        edition,
                        self._collection,
                        ReplacementPolicy.from_license_source(),
                        create_coverage_record=False,
                    )
                else:
                    apply.bibliographic_apply.delay(
                        bibliographic,
                        collection_id=self._collection.id,
                        replace=ReplacementPolicy.from_license_source(),
                    )

        now = utc_now()
        for identifier in identifiers_not_mentioned:
            pools = [
                lp
                for lp in identifier.licensed_through
                if lp.data_source.name == DataSourceConstants.BIBLIOTHECA
                and lp.collection == self._collection
            ]
            if not pools:
                continue
            [pool] = pools
            if pool.licenses_owned > 0:
                self.log.warning("Removing %s from circulation.", identifier.identifier)
            pool.update_availability(0, 0, 0, 0, as_of=now)

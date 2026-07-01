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
        on-demand single-title refreshes.  Reconciles the same way as
        :meth:`update_batch` — bibliographic metadata is hash-deduplicated and
        availability is compared against the pool's live columns — so a title whose
        metadata and availability are both unchanged produces no database writes.

        Unlike the sweep (:meth:`update_batch`), changes are applied **synchronously**
        in the caller's session rather than queued as ``bibliographic_apply`` /
        ``circulation_apply`` tasks, so the updated availability is visible as soon as
        this method returns.  Callers such
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

        Reconciles each returned record along two tracks (the same shape as the OPDS
        importer): bibliographic **metadata** is hash-deduplicated via
        :meth:`~palace.manager.data_layer.bibliographic.BibliographicData.needs_apply`,
        and otherwise **circulation** is gated on
        :meth:`~palace.manager.data_layer.circulation.CirculationData.needs_apply`.
        The circulation check is reliable for availability because
        :meth:`~palace.manager.data_layer.circulation.CirculationData.should_apply_to`
        compares the snapshot against the pool's *live* columns — the availability
        counts are excluded from the dedup hash, which they would otherwise drift from
        (the event importer and loan/hold operations mutate ``licenses_*`` via
        :meth:`~palace.manager.sqlalchemy.model.licensing.LicensePool.update_availability`
        without restamping the hash).

        When ``synchronous`` is ``False`` (the sweep) applies are queued as
        ``bibliographic_apply`` / ``circulation_apply`` Celery tasks; when ``True``
        (on-demand refreshes) they are applied directly in this session so the
        result is immediately visible to the caller.

        For identifiers the API does not return (indicating removed or expired
        licenses), zeroes out the ``LicensePool``'s availability.

        :param identifiers: Identifiers to process.
        :param synchronous: When ``True``, apply changes in-band instead of queuing
            asynchronous Celery tasks.
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
            identifier = (
                identifiers_by_bibliotheca_id.get(bibliotheca_id)
                if bibliotheca_id
                else None
            )
            if identifier is not None:
                identifiers_not_mentioned.discard(identifier)

            # Two-track reconciliation: apply bibliographic metadata when its hash
            # changed; otherwise apply circulation when CirculationData.needs_apply()
            # says so. The latter is reliable for availability because
            # CirculationData.should_apply_to() compares the pool's live columns (the
            # availability counts are excluded from the dedup hash, which they would
            # otherwise drift from -- the event importer and loan/hold operations
            # mutate licenses_* without restamping it). When metadata changed, its
            # embedded circulation rides along and is reconciled by
            # CirculationData.apply's own should_apply_to.
            circulation = bibliographic.circulation
            if bibliographic.needs_apply(self._session):
                if synchronous:
                    edition, _ = bibliographic.edition(self._session)
                    bibliographic.apply(
                        self._session,
                        edition,
                        self._collection,
                        ReplacementPolicy.from_license_source(),
                    )
                else:
                    apply.bibliographic_apply.delay(
                        bibliographic,
                        collection_id=self._collection.id,
                        replace=ReplacementPolicy.from_license_source(),
                    )
            elif circulation is not None and circulation.needs_apply(
                self._session, self._collection
            ):
                if synchronous:
                    circulation.apply(
                        self._session,
                        self._collection,
                        ReplacementPolicy.from_license_source(),
                    )
                else:
                    apply.circulation_apply.delay(
                        circulation,
                        collection_id=self._collection.id,
                        replace=ReplacementPolicy.from_license_source(),
                    )

        now = utc_now()
        for identifier in identifiers_not_mentioned:
            pool = self._license_pool_for(identifier)
            if pool is None:
                continue
            if pool.licenses_owned > 0:
                self.log.warning("Removing %s from circulation.", identifier.identifier)
            pool.update_availability(0, 0, 0, 0, as_of=now)

    def _license_pool_for(self, identifier: Identifier) -> LicensePool | None:
        """Return this collection's Bibliotheca ``LicensePool`` for ``identifier``, if any."""
        pools = [
            lp
            for lp in identifier.licensed_through
            if lp.data_source.name == DataSourceConstants.BIBLIOTHECA
            and lp.collection == self._collection
        ]
        return pools[0] if pools else None

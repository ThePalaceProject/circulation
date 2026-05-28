"""Importer for Bibliotheca (3M Cloud) MARC purchase records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from pymarc import Record
from sqlalchemy.orm import Session

from palace.util.datetime_helpers import datetime_utc
from palace.util.log import LoggerMixin

from palace.manager.celery.tasks import apply
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool

PURCHASE_RECORD_SERVICE_NAME = "Bibliotheca Purchase Record Importer"

# The importer starts from this date when no prior Timestamp exists.
# Earlier versions of this importer set the default start date to 2014-01-01.
# For the sake of consistency, we will preserve this default until there is a
# clear rationale for changing it.
DEFAULT_PURCHASE_RECORD_START_TIME = datetime_utc(2014, 1, 1)

_LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

# Maximum number of MARC records per API page (Bibliotheca hard limit).
_MARC_PAGE_SIZE = 50


@dataclass(frozen=True)
class DayImportResult:
    """Result of importing one page of Bibliotheca MARC purchase records.

    :param records_fetched: Number of MARC records received from the API in this page.
    :param day_start: Start of the day being processed (inclusive).
    :param day_end: End of the day window (exclusive).
        Equal to ``min(day_start + 1 day, cutoff)``.
    :param next_offset: Offset to pass to the next ``import_day`` call for
        the same day, or ``None`` when the page was smaller than the maximum
        (meaning the day is fully processed and the caller should advance to
        ``day_end``).
    """

    records_fetched: int
    day_start: datetime
    day_end: datetime
    next_offset: int | None


class BibliothecaPurchaseRecordImporter(LoggerMixin):
    """Imports Bibliotheca MARC purchase records one page at a time.

    Each call to :meth:`import_day` processes one API page (up to
    :data:`_MARC_PAGE_SIZE` records) for a given day.  Callers advance
    through pages of the same day (via ``next_offset``) and then advance
    to the next day (via ``day_end``) until the collection is caught up
    to the cutoff.
    """

    def __init__(
        self,
        session: Session,
        collection: Collection,
        api: BibliothecaAPI | None = None,
    ) -> None:
        """
        :param session: Database session.
        :param collection: The Bibliotheca collection to import records for.
        :param api: Optional pre-constructed API instance; created from
            ``session`` and ``collection`` if not supplied.
        """
        self._session = session
        self._collection = collection
        self._api = api or BibliothecaAPI(session, collection)

    def get_start(self) -> datetime:
        """Return the start of the next day to import from the stored ``Timestamp``.

        Falls back to :data:`DEFAULT_PURCHASE_RECORD_START_TIME` (2014-01-01) when no
        prior run has been recorded, so the first run begins a full historical
        backfill from the earliest possible purchase record date.

        :returns: The start datetime for the next import day.
        """
        timestamp = Timestamp.lookup(
            self._session,
            PURCHASE_RECORD_SERVICE_NAME,
            Timestamp.TASK_TYPE,
            self._collection,
        )
        if timestamp is None or timestamp.finish is None:
            return DEFAULT_PURCHASE_RECORD_START_TIME
        finish: datetime = timestamp.finish
        return finish

    def import_day(
        self, current_day: datetime, cutoff: datetime, offset: int = 1
    ) -> DayImportResult:
        """Import one page of MARC purchase records for a given day.

        Fetches up to :data:`_MARC_PAGE_SIZE` records from the window
        ``[current_day, day_end]`` starting at ``offset``, where
        ``day_end = min(current_day + 1 day, cutoff)``.

        The ``Timestamp`` is always updated after the page is processed:
        to ``current_day`` while the day is still in progress (so a restart
        after a crash resumes from the beginning of this day rather than an
        earlier day), and to ``day_end`` once the page is smaller than the
        maximum (signalling that the day is fully processed).

        :param current_day: Start of the day to process.
        :param cutoff: Upper bound of the import window; the day will not
            extend past this point.
        :param offset: 1-based record offset within the day's result set.
            Defaults to ``1`` (the first page).
        :returns: A :class:`DayImportResult` describing what was processed.
            Check :attr:`~DayImportResult.next_offset` to determine whether
            to re-queue for the same day or advance to the next.
        """
        day_end = min(current_day + timedelta(days=1), cutoff)

        self.log.info(
            f"Bibliotheca purchase record import: requesting MARC records for "
            f"'{self._collection.name}' between "
            f"{current_day.strftime(_LOG_DATE_FORMAT)} and "
            f"{day_end.strftime(_LOG_DATE_FORMAT)}, offset {offset}."
        )

        records = list(
            self._api.marc_request(current_day, day_end, offset, _MARC_PAGE_SIZE)
        )
        for record in records:
            self._process_record(record, current_day)

        records_fetched = len(records)
        day_complete = records_fetched < _MARC_PAGE_SIZE
        next_offset = None if day_complete else offset + _MARC_PAGE_SIZE

        # Always checkpoint: advance finish to day_end when the day is done,
        # or to current_day while still in progress, so a restart after a
        # crash resumes from this day rather than the previous one.
        Timestamp.stamp(
            self._session,
            service=PURCHASE_RECORD_SERVICE_NAME,
            service_type=Timestamp.TASK_TYPE,
            collection=self._collection,
            start=current_day,
            finish=day_end if day_complete else current_day,
            achievements=f"MARC records fetched: {records_fetched}.",
        )

        return DayImportResult(
            records_fetched=records_fetched,
            day_start=current_day,
            day_end=day_end,
            next_offset=next_offset,
        )

    def _process_record(self, record: Record, purchase_record_time: datetime) -> None:
        """Process a single Bibliotheca MARC purchase record.

        Extracts the Bibliotheca ID from MARC field ``001``, creates or finds
        the ``LicensePool``, then queues a ``bibliographic_apply`` task when
        the title's metadata has changed (hash-based deduplication).

        :param record: A pymarc ``Record`` representing one purchased title.
        :param purchase_record_time: Timestamp of the purchase record day.
        """
        control_numbers = [f for f in record.fields if f.tag == "001"]
        if not control_numbers:
            self.log.error(
                f"Ignoring MARC record with no Bibliotheca control number. {record.as_json()}"
            )
            return
        if len(control_numbers) > 1:
            self.log.error(
                f"Ignoring MARC record with multiple Bibliotheca control numbers. {record.as_json()}"
            )
            return

        bibliotheca_id = control_numbers[0].value()

        LicensePool.for_foreign_id(
            self._session,
            self._api.data_source,
            Identifier.BIBLIOTHECA_ID,
            bibliotheca_id,
            collection=self._collection,
        )

        for bibliographic in self._api.bibliographic_lookup(bibliotheca_id):
            if bibliographic.needs_apply(self._session):
                apply.bibliographic_apply.delay(
                    bibliographic,
                    collection_id=self._collection.id,
                    replace=ReplacementPolicy.from_license_source(),
                )

        self.log.info(
            f"{purchase_record_time.strftime(_LOG_DATE_FORMAT)}: processed purchase record for Bibliotheca ID {bibliotheca_id}"
        )

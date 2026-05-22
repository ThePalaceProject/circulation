"""Importer for Bibliotheca (3M Cloud) MARC purchase records."""

from __future__ import annotations

from collections.abc import Generator
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

PURCHASE_SERVICE_NAME = "Bibliotheca Purchase Monitor"

# The purchase monitor starts from this date when no prior Timestamp exists.
# Bibliotheca collections typically go back to 2014-01-01.
DEFAULT_PURCHASE_START_TIME = datetime_utc(2014, 1, 1)

_LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

# Maximum number of MARC records per API page (Bibliotheca hard limit).
_MARC_PAGE_SIZE = 50


@dataclass(frozen=True)
class DayImportResult:
    """Result of importing one day of Bibliotheca MARC purchase records.

    :param records_handled: Number of MARC records processed.
    :param day_start: Start of the day that was processed (inclusive).
    :param day_end: End of the window that was processed (exclusive).
        Equal to ``min(day_start + 1 day, cutoff)``.
    """

    records_handled: int
    day_start: datetime
    day_end: datetime


class BibliothecaPurchaseImporter(LoggerMixin):
    """Imports Bibliotheca MARC purchase records one day at a time.

    Processes one day per invocation.  Callers are responsible for
    chaining days until the collection is caught up to the cutoff.
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

        Falls back to :data:`DEFAULT_PURCHASE_START_TIME` (2014-01-01) when no
        prior run has been recorded, so the first run begins a full historical
        backfill from the earliest possible purchase date.

        :returns: The start datetime for the next import day.
        """
        timestamp = Timestamp.lookup(
            self._session,
            PURCHASE_SERVICE_NAME,
            Timestamp.MONITOR_TYPE,
            self._collection,
        )
        if timestamp is None or timestamp.finish is None:
            return DEFAULT_PURCHASE_START_TIME
        finish: datetime = timestamp.finish
        return finish

    def import_day(self, current_day: datetime, cutoff: datetime) -> DayImportResult:
        """Import all MARC purchase records for one day and stamp the ``Timestamp``.

        Fetches all paginated MARC records for the window
        ``[current_day, day_end]`` where ``day_end = min(current_day + 1 day,
        cutoff)``, processes each record, then updates the stored ``Timestamp``
        so that the next call to :meth:`get_start` resumes from ``day_end``.

        :param current_day: Start of the day to process.
        :param cutoff: Upper bound of the import window; the day will not
            extend past this point.
        :returns: A :class:`DayImportResult` describing what was processed.
        """
        day_end = min(current_day + timedelta(days=1), cutoff)
        records_handled = 0

        self.log.info(
            f"Bibliotheca purchase import: requesting MARC records for "
            f"'{self._collection.name}' between "
            f"{current_day.strftime(_LOG_DATE_FORMAT)} and "
            f"{day_end.strftime(_LOG_DATE_FORMAT)}."
        )

        for record in self._purchases(current_day, day_end):
            self._process_record(record, current_day)
            records_handled += 1

        Timestamp.stamp(
            self._session,
            service=PURCHASE_SERVICE_NAME,
            service_type=Timestamp.MONITOR_TYPE,
            collection=self._collection,
            start=current_day,
            finish=day_end,
            achievements=f"MARC records processed: {records_handled}.",
        )

        return DayImportResult(
            records_handled=records_handled,
            day_start=current_day,
            day_end=day_end,
        )

    def _purchases(self, start: datetime, end: datetime) -> Generator[Record]:
        """Paginate ``marc_request`` until a page smaller than the max is returned.

        :param start: Start of the window to request.
        :param end: End of the window to request.
        :yields: pymarc ``Record`` objects.
        """
        offset = 1  # Bibliotheca smallest allowed offset.
        records: list[Record] | None = None
        while records is None or len(records) >= _MARC_PAGE_SIZE:
            records = list(self._api.marc_request(start, end, offset, _MARC_PAGE_SIZE))
            yield from records
            offset += _MARC_PAGE_SIZE

    def _process_record(self, record: Record, purchase_time: datetime) -> None:
        """Process a single Bibliotheca MARC purchase record.

        Extracts the Bibliotheca ID from MARC field ``001``, creates or finds
        the ``LicensePool``, then queues a ``bibliographic_apply`` task when
        the title's metadata has changed (hash-based deduplication).

        :param record: A pymarc ``Record`` representing one purchased title.
        :param purchase_time: Timestamp of the purchase day.
        """
        control_numbers = [f for f in record.fields if f.tag == "001"]
        if not control_numbers:
            self.log.error(
                "Ignoring MARC record with no Bibliotheca control number. %s",
                record.as_json(),
            )
            return
        if len(control_numbers) > 1:
            self.log.error(
                "Ignoring MARC record with multiple Bibliotheca control numbers. %s",
                record.as_json(),
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
            "%s: processed purchase record for Bibliotheca ID %s",
            purchase_time.strftime(_LOG_DATE_FORMAT),
            bibliotheca_id,
        )

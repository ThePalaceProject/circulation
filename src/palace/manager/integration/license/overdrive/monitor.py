from __future__ import annotations

import datetime
import traceback
from collections.abc import Generator
from typing import Any

import dateutil
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import ObjectDeletedError, StaleDataError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from palace.manager.core.monitor import (
    CollectionMonitor,
    IdentifierSweepMonitor,
    TimelineMonitor,
    TimestampData,
)
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier


class OverdriveCirculationMonitor(CollectionMonitor, TimelineMonitor):
    """Maintain LicensePools for recently changed Overdrive titles. Create
    basic Editions for any new LicensePools that show up.
    """

    MAXIMUM_BOOK_RETRIES = 5
    SERVICE_NAME = "Overdrive Circulation Monitor"
    PROTOCOL = OverdriveAPI.label()
    OVERLAP = datetime.timedelta(minutes=1)

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        api_class: type[OverdriveAPI] = OverdriveAPI,
    ) -> None:
        """Constructor."""
        super().__init__(_db, collection)
        self.api = api_class(_db, collection)

    def recently_changed_ids(
        self, start: datetime.datetime, cutoff: datetime.datetime | None
    ) -> Generator[dict[str, str]]:
        return self.api.recently_changed_ids(start, cutoff)

    def catch_up_from(
        self,
        start: datetime.datetime,
        cutoff: datetime.datetime | None,
        progress: TimestampData,
    ) -> None:
        """Find Overdrive books that changed recently.

        :progress: A TimestampData representing the time previously
            covered by this Monitor.
        """
        overdrive_data_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        # Ask for changes between the last time covered by the Monitor
        # and the current time.
        total_books = 0
        for book in self.recently_changed_ids(start, cutoff):
            total_books += 1
            if not total_books % 100:
                self.log.info("%s books processed", total_books)
            if not book:
                continue

            book_changed = False
            try:
                book_changed = self.process_book(book, progress)
                self._db.commit()
            except Exception as e:
                progress.exception = "".join(traceback.format_exception(e))

            if self.should_stop(start, book, book_changed):
                break

        progress.achievements = "Books processed: %d." % total_books

    @retry(
        retry=(
            retry_if_exception_type(StaleDataError)
            | retry_if_exception_type(ObjectDeletedError)
        ),
        stop=stop_after_attempt(MAXIMUM_BOOK_RETRIES),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        reraise=True,
    )
    def process_book(self, book: dict[str, Any], progress: TimestampData) -> bool:
        # Attempt to create/update the book up to MAXIMUM_BOOK_RETRIES times.
        try:
            with self._db.begin_nested():
                _, _, is_changed = self.api.update_licensepool(book)
                book_changed = is_changed
        except Exception:
            self.log.exception("exception on update_licensepool: ")
            raise
        return book_changed

    def should_stop(
        self,
        start: datetime.datetime,
        api_description: dict[str, Any],
        is_changed: bool,
    ) -> bool | None:
        pass


class NewTitlesOverdriveCollectionMonitor(OverdriveCirculationMonitor):
    """Monitor the Overdrive collection for newly added titles.
    This catches any new titles that slipped through the cracks of the
    RecentOverdriveCollectionMonitor.  This monitor queries for all titles
    ordered by dateAdded in reverse chronological order.  However,
    according to Overdrive (see https://ebce-lyrasis.atlassian.net/browse/PP-1002),
    the dateAdded value is will not necessarily be in reverse chronological order.
    If the collection this is being search is a consortial account and a title that
    is present in a linked Advantage account has been added to the consortial collection
    the dateAdded value will reflect the date it was added to the Advantage account. However,
    it's position in the list will reflect the date it was added to the consortial account.
    So ugly, but Overdrive insists this is the expected behavior.

    To mitigate the possibility of missing new titles,  we can count the number of consecutive
    titles with a dateAdded that is out of scope.  If we exceed that threshold, only then do
    we stop processing.
    """

    SERVICE_NAME = "Overdrive New Title Monitor"
    OVERLAP = datetime.timedelta(days=7)
    DEFAULT_START_TIME = OverdriveCirculationMonitor.NEVER
    MAX_CONSECUTIVE_OUT_OF_SCOPE_DATES = 1000

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._consecutive_items_out_of_scope = 0

    def recently_changed_ids(
        self, start: datetime.datetime, cutoff: datetime.datetime | None
    ) -> Generator[dict[str, str]]:
        """Ignore the dates and return all IDs ordered by dateAdded in reverse chronological order."""
        return self.api.all_ids()

    def should_stop(
        self,
        start: datetime.datetime | None,
        api_description: dict[str, Any],
        is_changed: bool,
    ) -> bool | None:
        if not start or start is self.NEVER:
            # This monitor has never run before. It should ask about
            # every single book.
            return False

        # We should stop if this book was added before our start time.
        date_added = api_description.get("date_added")
        if not date_added:
            # We don't know when this book was added -- shouldn't happen.
            return False

        try:
            date_added = dateutil.parser.parse(date_added)
        except ValueError as e:
            # The date format is unparseable -- shouldn't happen.
            self.log.error("Got invalid date: %s", date_added)
            return False

        date_out_of_scope = date_added < start
        self.log.info(
            f"Date added: {date_added}, start time: {start}, date out of scope: {date_out_of_scope}"
        )

        if date_out_of_scope:
            self._consecutive_items_out_of_scope += 1
            if (
                self._consecutive_items_out_of_scope
                > self.MAX_CONSECUTIVE_OUT_OF_SCOPE_DATES
            ):
                self.log.info(
                    f"Max consecutive out of scope date threshold of {self.MAX_CONSECUTIVE_OUT_OF_SCOPE_DATES} "
                    f"breached! We should stop now."
                )
                return True

        else:
            # reset consecutive counter
            if self._consecutive_items_out_of_scope > 0:
                self.log.info(
                    f"We encountered a title that was added within our scope that followed a title that was out "
                    f"of scope. Resetting counter from {self._consecutive_items_out_of_scope} consecutive items "
                    f"back to zero."
                )
                self._consecutive_items_out_of_scope = 0

        return False


class OverdriveCollectionReaper(IdentifierSweepMonitor):
    """Check for books that are in the local collection but have left our
    Overdrive collection.
    """

    SERVICE_NAME = "Overdrive Collection Reaper"
    PROTOCOL = OverdriveAPI.label()
    DEFAULT_BATCH_SIZE = 10

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        api_class: type[OverdriveAPI] = OverdriveAPI,
    ) -> None:
        super().__init__(_db, collection)
        self.api = api_class(_db, collection)

    def process_item(self, identifier: Identifier) -> None:
        self.api.update_licensepool(identifier.identifier)


class RecentOverdriveCollectionMonitor(OverdriveCirculationMonitor):
    """Monitor recently changed books in the Overdrive collection."""

    SERVICE_NAME = "Reverse Chronological Overdrive Collection Monitor"

    # Report successful completion upon finding this number of
    # consecutive books in the Overdrive results whose LicensePools
    # haven't changed since last time. Overdrive results are not in
    # strict chronological order, but if you see 100 consecutive books
    # that haven't changed, you're probably done.
    MAXIMUM_CONSECUTIVE_UNCHANGED_BOOKS = 100

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.consecutive_unchanged_books = 0

    def should_stop(
        self,
        start: datetime.datetime,
        api_description: dict[str, Any],
        is_changed: bool,
    ) -> bool | None:
        if is_changed:
            self.consecutive_unchanged_books = 0
        else:
            self.consecutive_unchanged_books += 1
            if (
                self.consecutive_unchanged_books
                >= self.MAXIMUM_CONSECUTIVE_UNCHANGED_BOOKS
            ):
                # We're supposed to stop this run after finding a
                # run of books that have not changed, and we have
                # in fact seen that many consecutive unchanged
                # books.
                self.log.info(
                    "Stopping at %d unchanged books.", self.consecutive_unchanged_books
                )
                return True
        return False


class OverdriveFormatSweep(IdentifierSweepMonitor):
    """Check the current formats of every Overdrive book
    in our collection.
    """

    SERVICE_NAME = "Overdrive Format Sweep"
    DEFAULT_BATCH_SIZE = 10
    PROTOCOL = OverdriveAPI.label()

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        api_class: type[OverdriveAPI] = OverdriveAPI,
    ) -> None:
        super().__init__(_db, collection)
        self.api = api_class(_db, collection)

    def process_item(self, identifier: Identifier) -> None:
        pools = identifier.licensed_through
        for pool in pools:
            self.api.update_formats(pool)
            # if there are multiple pools they should all have the same formats
            # so we break after processing the first one
            break

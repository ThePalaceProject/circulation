from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from datetime import datetime
from typing import Any
from urllib.parse import urljoin, urlparse

from requests import Response
from sqlalchemy.orm import Session

from palace.manager.core.coverage import CoverageFailure
from palace.manager.core.monitor import CollectionMonitor, TimestampData
from palace.manager.integration.license.opds.base.importer import BaseOPDSImporter
from palace.manager.integration.license.opds.opds1.api import OPDSAPI
from palace.manager.integration.license.opds.opds1.settings import OPDSImporterSettings
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import CoverageRecord
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.util import base64
from palace.manager.util.datetime_helpers import to_utc
from palace.manager.util.http import HTTP, BadResponseException
from palace.manager.util.opds_writer import OPDSFeed


class OPDSImportMonitor(CollectionMonitor):
    """Periodically monitor a Collection's OPDS archive feed and import
    every title it mentions.
    """

    SERVICE_NAME = "OPDS Import Monitor"

    # The first time this Monitor is invoked we want to get the
    # entire OPDS feed.
    DEFAULT_START_TIME = CollectionMonitor.NEVER

    # The protocol this Monitor works with. Subclasses that
    # specialize OPDS import should override this.
    PROTOCOL = OPDSAPI.label()

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        import_class: type[BaseOPDSImporter[OPDSImporterSettings]],
        force_reimport: bool = False,
        **import_class_kwargs: Any,
    ) -> None:
        if not collection:
            raise ValueError(
                "OPDSImportMonitor can only be run in the context of a Collection."
            )

        if collection.protocol != self.PROTOCOL:
            raise ValueError(
                "Collection %s is configured for protocol %s, not %s."
                % (collection.name, collection.protocol, self.PROTOCOL)
            )

        self.force_reimport = force_reimport

        self.importer = import_class(_db, collection=collection, **import_class_kwargs)
        settings = self.importer.settings
        self.username = settings.username
        self.password = settings.password
        self.feed_url = settings.external_account_id

        self.custom_accept_header = settings.custom_accept_header
        self._max_retry_count = settings.max_retry_count

        parsed_url = urlparse(self.feed_url)
        self._feed_base_url = f"{parsed_url.scheme}://{parsed_url.hostname}{(':' + str(parsed_url.port)) if parsed_url.port else ''}/"
        super().__init__(_db, collection)

    def _get(self, url: str, headers: Mapping[str, str]) -> Response:
        """Make the sort of HTTP request that's normal for an OPDS feed.

        Long timeout, raise error on anything but 2xx or 3xx.
        """

        headers = self._update_headers(headers)
        if not url.startswith("http"):
            url = urljoin(self._feed_base_url, url)
        return HTTP.get_with_timeout(
            url,
            headers=headers,
            timeout=120,
            max_retry_count=self._max_retry_count,
            allowed_response_codes=["2xx", "3xx"],
        )

    def _get_accept_header(self) -> str:
        return ",".join(
            [
                OPDSFeed.ACQUISITION_FEED_TYPE,
                "application/atom+xml;q=0.9",
                "application/xml;q=0.8",
                "*/*;q=0.1",
            ]
        )

    def _update_headers(self, headers: Mapping[str, str] | None) -> dict[str, str]:
        headers = dict(headers) if headers else {}
        if self.username and self.password and not "Authorization" in headers:
            headers["Authorization"] = "Basic %s" % base64.b64encode(
                f"{self.username}:{self.password}"
            )

        if self.custom_accept_header:
            headers["Accept"] = self.custom_accept_header
        elif not "Accept" in headers:
            headers["Accept"] = self._get_accept_header()

        return headers

    def data_source(self, collection: Collection) -> DataSource:
        """Returns the data source name for the given collection.

        By default, this URL is stored as a setting on the collection, but
        subclasses may hard-code it.
        """
        return collection.data_source

    def feed_contains_new_data(self, feed: bytes | str) -> bool:
        """Does the given feed contain any entries that haven't been imported
        yet?
        """
        if self.force_reimport:
            # We don't even need to check. Always treat the feed as
            # though it contained new data.
            return True

        # For every item in the last page of the feed, check when that
        # item was last updated.
        last_update_dates = self.importer.extract_last_update_dates(feed)

        new_data = False
        for raw_identifier, remote_updated in last_update_dates:
            identifier = self.importer.parse_identifier(raw_identifier)
            if not identifier:
                # Maybe this is new, maybe not, but we can't associate
                # the information with an Identifier, so we can't do
                # anything about it.
                self.log.info(
                    f"Ignoring {raw_identifier} because unable to turn into an Identifier."
                )
                continue

            if self.identifier_needs_import(identifier, remote_updated):
                new_data = True
                break
        return new_data

    def identifier_needs_import(
        self, identifier: Identifier | None, last_updated_remote: datetime | None
    ) -> bool:
        """Does the remote side have new information about this Identifier?

        :param identifier: An Identifier.
        :param last_update_remote: The last time the remote side updated
            the OPDS entry for this Identifier.
        """
        if not identifier:
            return False

        record = CoverageRecord.lookup(
            identifier,
            self.importer.data_source,
            operation=CoverageRecord.IMPORT_OPERATION,
            collection=self.collection,
        )

        if not record:
            # We have no record of importing this Identifier. Import
            # it now.
            self.log.info(
                "Counting %s as new because it has no CoverageRecord.", identifier
            )
            return True

        # If there was a transient failure last time we tried to
        # import this book, try again regardless of whether the
        # feed has changed.
        if record.status == CoverageRecord.TRANSIENT_FAILURE:
            self.log.info(
                "Counting %s as new because previous attempt resulted in transient failure: %s",
                identifier,
                record.exception,
            )
            return True

        # If our last attempt was a success or a persistent
        # failure, we only want to import again if something
        # changed since then.

        if record.timestamp:
            # We've imported this entry before, so don't import it
            # again unless it's changed.

            if not last_updated_remote:
                # The remote isn't telling us whether the entry
                # has been updated. Import it again to be safe.
                self.log.info(
                    "Counting %s as new because remote has no information about when it was updated.",
                    identifier,
                )
                return True

            if to_utc(last_updated_remote) >= to_utc(record.timestamp):
                # This book has been updated.
                self.log.info(
                    "Counting %s as new because its coverage date is %s and remote has %s.",
                    identifier,
                    record.timestamp,
                    last_updated_remote,
                )
                return True
        return False

    def _verify_media_type(self, url: str, resp: Response) -> None:
        # Make sure we got an OPDS feed, and not an error page that was
        # sent with a 200 status code.
        media_type = resp.headers.get("content-type")
        if not media_type or not any(
            x in media_type for x in (OPDSFeed.ATOM_LIKE_TYPES)
        ):
            message = "Expected Atom feed, got %s" % media_type
            raise BadResponseException(url, message=message, response=resp)

    def follow_one_link(
        self, url: str, do_get: Callable[..., Response] | None = None
    ) -> tuple[list[str], bytes | None]:
        """Download a representation of a URL and extract the useful
        information.

        :return: A 2-tuple (next_links, feed). `next_links` is a list of
            additional links that need to be followed. `feed` is the content
            that needs to be imported.
        """
        self.log.info("Following next link: %s", url)
        get = do_get or self._get
        resp = get(url, headers={})
        feed = resp.content

        self._verify_media_type(url, resp)

        new_data = self.feed_contains_new_data(feed)

        if new_data:
            # There's something new on this page, so we need to check
            # the next page as well.
            next_links = self.importer.extract_next_links(feed)
            return next_links, feed
        else:
            # There's nothing new, so we don't need to import this
            # feed or check the next page.
            self.log.info("No new data.")
            return [], None

    def import_one_feed(
        self, feed: bytes | str
    ) -> tuple[list[Edition], dict[str, list[CoverageFailure]]]:
        """Import every book mentioned in an OPDS feed."""

        # Because we are importing into a Collection, we will immediately
        # mark a book as presentation-ready if possible.
        imported_editions, pools, works, failures = self.importer.import_from_feed(
            feed, feed_url=self.feed_url
        )

        # Create CoverageRecords for the successful imports.
        for edition in imported_editions:
            record = CoverageRecord.add_for(
                edition,
                self.importer.data_source,
                CoverageRecord.IMPORT_OPERATION,
                status=CoverageRecord.SUCCESS,
                collection=self.collection,
            )

        # Create CoverageRecords for the failures.
        for urn, failure_items in list(failures.items()):
            for failure_item in failure_items:
                failure_item.to_coverage_record(
                    operation=CoverageRecord.IMPORT_OPERATION
                )

        return imported_editions, failures

    def _get_feeds(self) -> Iterable[tuple[str, bytes]]:
        feeds = []
        queue = [self.feed_url]
        seen_links = set()

        # First, follow the feed's next links until we reach a page with
        # nothing new. If any link raises an exception, nothing will be imported.

        while queue:
            new_queue = []

            for link in queue:
                if link in seen_links:
                    continue
                next_links, feed = self.follow_one_link(link)
                new_queue.extend(next_links)
                if feed:
                    feeds.append((link, feed))
                seen_links.add(link)

            queue = new_queue

        # Start importing at the end. If something fails, it will be easier to
        # pick up where we left off.
        return reversed(feeds)

    def run_once(self, progress: TimestampData) -> TimestampData:
        feeds = self._get_feeds()
        total_imported = 0
        total_failures = 0

        for link, feed in feeds:
            self.log.info("Importing next feed: %s", link)
            imported_editions, failures = self.import_one_feed(feed)
            total_imported += len(imported_editions)
            total_failures += len(failures)
            self._db.commit()

        achievements = "Items imported: %d. Failures: %d." % (
            total_imported,
            total_failures,
        )

        return TimestampData(achievements=achievements)

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import feedparser
from requests import Response
from sqlalchemy.orm import Session

from palace.manager.core.coverage import CoverageFailure
from palace.manager.core.monitor import TimestampData
from palace.manager.integration.license.opds.for_distributors.api import (
    OPDSForDistributorsAPI,
)
from palace.manager.integration.license.opds.for_distributors.importer import (
    OPDSForDistributorsImporter,
)
from palace.manager.integration.license.opds.opds1.importer import OPDSImporter
from palace.manager.integration.license.opds.opds1.monitor import OPDSImportMonitor
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool


class OPDSForDistributorsImportMonitor(OPDSImportMonitor):
    """Monitor an OPDS feed that requires or allows authentication,
    such as Biblioboard or Plympton.
    """

    PROTOCOL = OPDSForDistributorsImporter.NAME
    SERVICE_NAME = "OPDS for Distributors Import Monitor"

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        import_class: type[OPDSImporter],
        **kwargs: Any,
    ) -> None:
        super().__init__(_db, collection, import_class, **kwargs)

        self.api = OPDSForDistributorsAPI(_db, collection)

    def _get(self, url: str, headers: Mapping[str, str]) -> Response:
        """Make a normal HTTP request for an OPDS feed, but add in an
        auth header with the credentials for the collection.
        """

        token = self.api._make_request._auth.token
        headers = dict(headers or {})
        auth_header = "Bearer %s" % token
        headers["Authorization"] = auth_header

        return super()._get(url, headers)


class OPDSForDistributorsReaperMonitor(OPDSForDistributorsImportMonitor):
    """This is an unusual import monitor that crawls the entire OPDS feed
    and keeps track of every identifier it sees, to find out if anything
    has been removed from the collection.
    """

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        import_class: type[OPDSImporter],
        **kwargs: Any,
    ) -> None:
        super().__init__(_db, collection, import_class, **kwargs)
        self.seen_identifiers: set[str] = set()

    def feed_contains_new_data(self, feed: bytes | str) -> bool:
        # Always return True so that the importer will crawl the
        # entire feed.
        return True

    def import_one_feed(
        self, feed: bytes | str
    ) -> tuple[list[Edition], dict[str, list[CoverageFailure]]]:
        # Collect all the identifiers in the feed.
        parsed_feed = feedparser.parse(feed)
        identifiers = [entry.get("id") for entry in parsed_feed.get("entries", [])]
        self.seen_identifiers.update(identifiers)
        return [], {}

    def run_once(self, progress: TimestampData) -> TimestampData:
        """Check to see if any identifiers we know about are no longer
        present on the remote. If there are any, remove them.

        :param progress: A TimestampData, ignored.
        """
        super().run_once(progress)

        # self.seen_identifiers is full of URNs. We need the values
        # that go in Identifier.identifier.
        identifiers, failures = Identifier.parse_urns(self._db, self.seen_identifiers)
        identifier_ids = [x.id for x in list(identifiers.values())]

        # At this point we've gone through the feed and collected all the identifiers.
        # If there's anything we didn't see, we know it's no longer available.
        qu = (
            self._db.query(LicensePool)
            .join(Identifier)
            .filter(LicensePool.collection_id == self.collection.id)
            .filter(~Identifier.id.in_(identifier_ids))
            .filter(LicensePool.licenses_available == LicensePool.UNLIMITED_ACCESS)
        )
        pools_reaped = qu.count()
        self.log.info(
            "Reaping %s license pools for collection %s."
            % (pools_reaped, self.collection.name)
        )

        for pool in qu:
            pool.unlimited_access = False

        self._db.commit()
        achievements = "License pools removed: %d." % pools_reaped
        return TimestampData(achievements=achievements)

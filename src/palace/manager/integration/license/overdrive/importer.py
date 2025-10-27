import asyncio
import datetime
from collections.abc import Set
from dataclasses import dataclass
from typing import Any

import dateutil
from sqlalchemy.orm import Session

from palace.manager.celery.tasks.apply import (
    ApplyBibliographicCallable,
    ApplyCirculationCallable,
)
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.integration.license.overdrive.api import (
    BookInfoEndpoint,
    OverdriveAPI,
)
from palace.manager.integration.license.overdrive.representation import (
    OverdriveRepresentationExtractor,
)
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.util import get_one_or_create
from palace.manager.util.datetime_helpers import datetime_utc
from palace.manager.util.log import LoggerMixin


@dataclass(frozen=True)
class FeedImportResult:
    current_page: BookInfoEndpoint
    next_page: BookInfoEndpoint | None = None
    processed_count: int = 0


class OverdriveImporter(LoggerMixin):
    DEFAULT_START_TIME = datetime_utc(1970, 1, 1)

    def __init__(
        self,
        db: Session,
        collection: Collection,
        registry: LicenseProvidersRegistry,
        import_all: bool = False,
        identifier_set: IdentifierSet | None = None,
        parent_identifier_set: IdentifierSet | None = None,
        api: OverdriveAPI | None = None,
    ) -> None:
        self._db = db
        self._collection = collection
        self._import_all = import_all
        self._identifier_set = identifier_set

        self._parent_identifiers: Set[str] | None = None
        if parent_identifier_set is not None:
            # create an in-memory set from the redis set to optimize existence checks for individiual identifiers.
            # I don't believe we need to worry about memory here: few redis identifier sets will likely exceed 200K
            # items which should be easily manageable given an identifier is 36 characters (36*200K = 7.2 MB). Most OD
            # collections are much  smaller in the 20-70K range.
            self._parent_identifiers = {
                x.identifier for x in parent_identifier_set.get()
            }

        if not registry.equivalent(collection.protocol, OverdriveAPI):
            raise PalaceValueError(
                f"Collection {collection.name} [id={collection.id} protocol={collection.protocol}] "
                f"is not an OverDrive collection."
            )

        self._api = (
            OverdriveAPI(_db=self._db, collection=self._collection)
            if api is None
            else api
        )

        self._extractor = OverdriveRepresentationExtractor(self._api)

    def get_timestamp(self) -> Timestamp:
        timestamp, _ = get_one_or_create(
            self._db,
            Timestamp,
            service="OverDrive Import",
            service_type=Timestamp.TASK_TYPE,
            collection=self._collection,
        )
        return timestamp

    def _get_start_time(self, timestamp: Timestamp) -> datetime.datetime:
        """Determine the start time for fetching new data."""
        if (
            self._import_all
            or self._identifier_set is not None
            or timestamp.start is None
        ):
            return self.DEFAULT_START_TIME
        return timestamp.start

    def _all_books_out_of_scope(
        self,
        modified_since: datetime.datetime | None,
        book_data: list[dict[str, Any]],
    ) -> bool:
        out_of_scope_count = 0

        for book in book_data:
            date_added = book.get("date_added")
            if not date_added:
                continue

            date_added = dateutil.parser.parse(date_added)
            if date_added < modified_since:
                out_of_scope_count += 1

        return out_of_scope_count == len(book_data)

    def import_collection(
        self,
        *,
        apply_bibliographic: ApplyBibliographicCallable,
        apply_circulation: ApplyCirculationCallable,
        modified_since: datetime.datetime,
        endpoint: BookInfoEndpoint | None = None,
    ) -> FeedImportResult:

        timestamp = self.get_timestamp()
        start_time = self._get_start_time(timestamp)

        identifiers = []
        policy = ReplacementPolicy(
            identifiers=False,
            subjects=True,
            contributions=True,
            formats=True,
            links=True,
        )

        self.log.info(
            f"Starting process of queuing items in collection {self._collection.name} (id={self._collection.id} "
            f"for import that have changed since {start_time}. "
        )

        if not endpoint:
            self.log.info(f"No endpoint provided, generating default endpoint.")
            endpoint = self._api.book_info_initial_endpoint(
                start=modified_since, page_size=100
            )

        with timestamp.recording():
            # Fetch metadata upfront if no parent identifier set is provided.  Practically speaking,
            # if there is no parent identifier set, then the collection being imported is a
            # main rather than an advantage collection.  We always fetch availabililty because we do not gain
            # much by trying to
            fetch_metadata = self._parent_identifiers is None
            book_data, next_endpoint = asyncio.run(
                self._api.fetch_book_info_list(
                    endpoint,
                    fetch_metadata=fetch_metadata,
                    fetch_availability=True,
                )
            )
            for book in book_data:
                identifier, _ = Identifier.for_foreign_id(
                    self._db,
                    foreign_id=book.get("id"),
                    foreign_identifier_type=Identifier.OVERDRIVE_ID,
                )

                assert identifier

                # We only need to look up metadata if we didn't already fetch it and it was not in the parent identifier
                # set.  Why? Because the existence of the parent identifier set implies that the parent collection
                # has already been imported which would have included all the metadata.
                if not fetch_metadata and (
                    not self._parent_identifiers
                    or identifier.identifier not in self._parent_identifiers
                ):
                    book["metadata"] = self._api.metadata_lookup(identifier=identifier)

                # we need to check that there is metadata because it is possible that we attempted to fetch it, but we
                # didn't get anything back from overdrive (ie from the book list fetch above).
                if book["metadata"]:
                    bibliographic = self._extractor.book_info_to_bibliographic(book)
                    assert bibliographic
                    if bibliographic.has_changed(self._db):
                        apply_bibliographic(
                            bibliographic,
                            collection_id=self._collection.id,
                            replace=policy,
                        )

                # availability needs to be checked/updated in all but a few instances so it is
                # probably not worth the compute time to save ourselves a handful of unnecessary updates.
                availability = book.get("availabilityV2", None)
                if availability:
                    circulation = self._extractor.book_info_to_circulation(availability)
                    assert circulation
                    if circulation.has_changed(
                        session=self._db, collection=self._collection
                    ):
                        apply_circulation(
                            circulation, collection_id=self._collection.id
                        )

                # add identifier for later counting.
                identifiers.append(identifier)

        achievements = [f"Total items queued for import:  {len(identifiers)}."]
        if (elapsed_time := timestamp.elapsed_seconds) is not None:
            achievements.append(f"Elapsed time: {elapsed_time:.2f} seconds.")

        if self._identifier_set is not None:
            self._identifier_set.add(*identifiers)

        timestamp.achievements = "\n".join(achievements)

        self.log.info(
            f"Finished import of {len(identifiers)} for collection {self._collection.name} (id={self._collection.id}. "
            f"{' '.join(achievements)}"
        )
        # if all books are out of scope, we don't need to fetch the next page.
        if not self._import_all and self._all_books_out_of_scope(
            modified_since, book_data
        ):
            next_endpoint = None
        return FeedImportResult(
            next_page=next_endpoint,
            current_page=endpoint,
            processed_count=len(identifiers),
        )

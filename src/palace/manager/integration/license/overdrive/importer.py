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
from palace.manager.data_layer.identifier import IdentifierData
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
from palace.manager.util.log import LoggerMixin


@dataclass(frozen=True)
class FeedImportResult:
    current_page: BookInfoEndpoint
    next_page: BookInfoEndpoint | None = None
    processed_count: int = 0


class OverdriveImporter(LoggerMixin):
    DEFAULT_PAGE_SIZE = 100

    def __init__(
        self,
        db: Session,
        collection: Collection,
        registry: LicenseProvidersRegistry,
        identifier_set: IdentifierSet | None = None,
        parent_identifier_set: IdentifierSet | None = None,
        api: OverdriveAPI | None = None,
    ) -> None:
        """Constructor for the OverdriveImporter class.

        :param db: The database session.
        :param collection: The collection to import.
        :param registry: The license providers registry.
        :param identifier_set: The identifier set to use for the import.
        :param parent_identifier_set: The parent identifier set to use for the import.
        :param api: The OverdriveAPI instance to use for the import.
        """
        self._db = db
        self._collection = collection
        self._identifier_set = identifier_set

        self._parent_identifiers: Set[IdentifierData] | None = None
        if parent_identifier_set is not None:
            # create an in-memory set from the redis set  to optimize existence checks for individual identifiers.
            # I don't believe we need to worry about memory here: few redis identifier sets will likely exceed 200K
            # items which should be easily manageable given an identifier is 36 characters (36*200K = 7.2 MB). Most OD
            # collections are much  smaller in the 20-70K range.

            self._parent_identifiers = parent_identifier_set.get()

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

    def _process_book(
        self,
        book: dict[str, Any],
        fetch_metadata: bool,
        policy: ReplacementPolicy,
        apply_bibliographic: ApplyBibliographicCallable,
        apply_circulation: ApplyCirculationCallable,
    ) -> tuple[Identifier, bool]:
        """Process a single book and return (identifier, changed).

        :param book: Book data dictionary from OverDrive API
        :param fetch_metadata: Whether metadata was already fetched
        :param policy: Replacement policy for bibliographic updates
        :param apply_bibliographic: Callback to apply bibliographic updates
        :param apply_circulation: Callback to apply circulation updates
        :return: Tuple of (identifier, changed) where changed is True if any data changed
        """

        # we may need to manipulate values in the book dictionary.  Therefore we make a copy for local changes
        # to avoid unnecessary side effects.
        book = book.copy()

        identifier, _ = Identifier.for_foreign_id(
            self._db,
            foreign_id=book.get("id"),
            foreign_identifier_type=Identifier.OVERDRIVE_ID,
        )

        # the identifier should never be null, because by default autocreate = True in for_foreign_id().
        # however mypy complains throughout without changing type hints or adding an asssertion.
        # An assertion is least verbose solution.
        assert identifier

        changed: bool = False

        # We only need to look up metadata if we didn't already fetch it and it was not in the parent identifier
        # set.  Why? Because the existence of the parent identifier set implies that the parent collection
        # has already been imported which would have included all the metadata.

        if not fetch_metadata and (
            not self._parent_identifiers
            or IdentifierData.from_identifier(identifier)
            not in self._parent_identifiers
        ):
            book["metadata"] = self._api.metadata_lookup(identifier=identifier)

        # we need to check that there is metadata because it is possible that we attempted to fetch it, but we
        # didn't get anything back from overdrive (ie from the book list fetch above) or we did not attempt to
        # fetch it because it was already processed by the parent collection.
        if book.get("metadata"):
            bibliographic = self._extractor.book_info_to_bibliographic(book)
            # The bibliographic should never be null here because there is a non-null entry for metadata in the
            # book dictionary.  Mypy complains without an assertion or type hints.
            assert bibliographic

            if bibliographic.has_changed(self._db):
                changed = True
                apply_bibliographic(
                    bibliographic,
                    collection_id=self._collection.id,
                    replace=policy,
                )

        # availability needs to be checked/updated in all but a few instances so it is
        # probably not worth the compute time to save ourselves a handful of unnecessary updates.
        availability = book.get("availabilityV2", None)
        if not availability:
            # This is a rare and probably transient case where the availabilityV2
            # was not retrieved due to a 404 from OD.
            self.log.warning(
                f"No availabilityV2 found for book {identifier}. book={book}.  This state can "
                f"arise when the OD returns a 404 for the availability url."
            )
        else:
            circulation = self._extractor.book_info_to_circulation(availability)
            # The circulation should never be null here because there is a non-null entry for availabilityV2 in the
            # book dictionary.  Mypy complains without an assertion or type hints.
            assert circulation

            if circulation.has_changed(session=self._db, collection=self._collection):
                changed = True
                apply_circulation(circulation, collection_id=self._collection.id)

        return identifier, changed

    def _all_books_out_of_scope(
        self,
        modified_since: datetime.datetime,
        book_data: list[dict[str, Any]],
    ) -> bool:
        """Check if all books in the book_data are out of scope in terms of the date they were added.

        This method is used to determine if we should continue to fetch the next page of books.
        Overdrive does not provide a way to retrieve books that were added or modified since a given date.
        They do however give us the "date_added" value for each book which we can use to determine
        if the book was added before the modified_since date.

        :param modified_since: The datetime to check if the books are out of scope.
        :param book_data: The book data to check if the books are out of scope.
        :return: True if all books are out of scope, False otherwise.
        """
        out_of_scope_count = 0

        for book in book_data:
            date_added = book.get("date_added", None)
            if not date_added:
                # this should not happen, but if it does, we'll assume the book is not out of scope.
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
        modified_since: datetime.datetime | None = None,
        endpoint: BookInfoEndpoint | None = None,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> FeedImportResult:
        """Import books from an OverDrive collection into the circulation manager.

        This method fetches book information from OverDrive's API and queues bibliographic
        and circulation data for processing. It implements several optimizations:

        1. **Metadata Fetching Strategy**:
           - For main collections (no parent_identifier_set): Fetches metadata upfront in bulk
           - For advantage collections (with parent_identifier_set): Fetches metadata lazily,
             skipping books that are already in the parent collection

        2. **Out-of-Scope Optimization**:
           - If all books in the current page were added before modified_since and there were no changes detected,
             stops pagination early to avoid processing old data
           - Can be disabled with import_all=True

        3. **Change Detection**:
           - Only applies bibliographic updates if metadata has changed
           - Always checks circulation data as availability changes frequently and applies changes only if changed.

        :param apply_bibliographic: Callback to apply bibliographic metadata updates (title, author, etc.)
        :param apply_circulation: Callback to apply circulation data updates (copies owned, available, etc.)
        :param modified_since: Only process books modified after this datetime. If None, processes all books.
        :param endpoint: OverDrive API endpoint to fetch from. If None, generates a default endpoint
                         starting from modified_since
        :param page_size: Number of items to fetch per page
        :return: FeedImportResult containing current_page (the endpoint that was processed),
                 next_page (the next endpoint to process, None if done or all books out of scope),
                 and processed_count (number of books processed in this call)

        .. note::
           Side Effects:
           - Creates/updates Identifier records in the database
           - Adds identifiers to self._identifier_set if provided
           - Records timing and achievement data in the Timestamp
           - Logs progress information
        """
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
            f"for import that have changed since {modified_since}. "
        )

        if not endpoint:
            self.log.info(f"No endpoint provided, generating default endpoint.")
            endpoint = self._api.book_info_initial_endpoint(
                start=modified_since, page_size=page_size
            )
            self.log.info(f"Generated endpoint: {endpoint}")
        else:
            self.log.info(f"Using provided endpoint: {endpoint}")
            self.log.info(
                f"Ignoring modified_since parameter (value='{modified_since}') since the endpoint "
                f"was provided."
            )

        timestamp = self.get_timestamp()
        changed_books_count = 0
        # Fetch metadata upfront if no parent identifier set is provided.  Practically speaking,
        # if there is no parent identifier set, then the collection being imported is a
        # main rather than an advantage collection.  We always fetch availability because we do not gain
        # much by trying to fetch it lazily.
        fetch_metadata = self._parent_identifiers is None
        book_data, next_endpoint = asyncio.run(
            self._api.fetch_book_info_list(
                endpoint,
                fetch_metadata=fetch_metadata,
                fetch_availability=True,
            )
        )
        for book in book_data:
            identifier, changed = self._process_book(
                book, fetch_metadata, policy, apply_bibliographic, apply_circulation
            )
            if changed:
                changed_books_count += 1
            identifiers.append(identifier)

        achievements = [f"Total items queued for import: {len(identifiers)}."]
        if (elapsed_time := timestamp.elapsed_seconds) is not None:
            achievements.append(f"Elapsed time: {elapsed_time:.2f} seconds.")

        if self._identifier_set is not None:
            self._identifier_set.add(*identifiers)

        timestamp.achievements = "\n".join(achievements)

        self.log.info(
            f"Finished import of {len(identifiers)} for collection {self._collection.name} (id={self._collection.id}). "
            f"{' '.join(achievements)}"
        )
        # if we are not in import all mode and all books are both out of scope and no books were changed, we can assume that
        # were are done importing and therefore we don't need to fetch the next page.
        if (
            modified_since is not None
            and changed_books_count == 0
            and self._all_books_out_of_scope(modified_since, book_data)
        ):
            next_endpoint = None

        return FeedImportResult(
            next_page=next_endpoint,
            current_page=endpoint,
            processed_count=len(identifiers),
        )

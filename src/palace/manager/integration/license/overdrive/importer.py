import asyncio
import datetime
from collections.abc import Set
from dataclasses import dataclass
from typing import Any

from sqlalchemy.orm import Session

from palace.util.exceptions import PalaceValueError
from palace.util.log import LoggerMixin

from palace.manager.celery.tasks.apply import (
    ApplyBibliographicCallable,
    ApplyCirculationCallable,
)
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.integration.license.overdrive.model import Availability
from palace.manager.integration.license.overdrive.representation import (
    OverdriveRepresentationExtractor,
)
from palace.manager.integration.license.overdrive.requests import BookInfoEndpoint
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.util import get_one_or_create


@dataclass(frozen=True)
class FeedImportResult:
    current_page: BookInfoEndpoint
    next_page: BookInfoEndpoint | None = None
    processed_count: int = 0


class OverdriveImporter(LoggerMixin):
    DEFAULT_PAGE_SIZE = 100

    #: The policy every import applies under. ``formats=True`` is the part that
    #: matters most: Overdrive's metadata record carries the full format list, so an
    #: apply built from it has to be allowed to remove a format Overdrive dropped.
    #: Tests assert against this rather than against a copy, so a change here cannot
    #: leave them passing on a policy production no longer uses.
    REPLACEMENT_POLICY = ReplacementPolicy(
        identifiers=False,
        subjects=True,
        contributions=True,
        formats=True,
        links=True,
    )

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

    @staticmethod
    def _bibliographic_matches_identifier(
        bibliographic: BibliographicData, identifier: Identifier
    ) -> bool:
        """Whether converted bibliographic data describes the expected title.

        The identifier comes from the product list, while the bibliographic data is
        converted from the separately fetched metadata record, so the two can in
        principle disagree -- for instance if Overdrive redirects a merged title to a
        different id. The comparison is case-insensitive, mirroring
        :meth:`BibliographicData._validate_primary_identifier`, because Overdrive is
        not consistent about the case of its ids. Only the identifier is compared;
        the type is always Identifier.OVERDRIVE_ID on both sides.

        :param bibliographic: Bibliographic data converted from the metadata record.
        :param identifier: The identifier taken from the product list.
        :return: True if the data can safely be applied to ``identifier``.
        """
        primary = bibliographic.primary_identifier_data
        if primary is None:
            return False
        return primary.identifier.lower() == identifier.identifier.lower()

    def _usable_bibliographic(
        self, book: dict[str, Any], identifier: Identifier
    ) -> BibliographicData | None:
        """Convert a book's metadata record, or return None if it is not usable.

        :param book: Book data dictionary, whose ``metadata`` key holds the record.
        :param identifier: The identifier the product list pointed us at.
        :return: The converted data, or None if there is no metadata, it could not be
            converted, or it describes a different title.
        """
        # It is possible that we attempted to fetch metadata but got nothing back from
        # Overdrive, or that we did not attempt to fetch it because the parent
        # collection already processed it.
        if not book.get("metadata"):
            return None

        bibliographic = self._extractor.book_info_to_bibliographic(book["metadata"])
        if bibliographic is None:
            # A non-empty metadata entry is not necessarily a usable record.
            # Overdrive reports an unrecognized identifier as an HTTP 200 whose body
            # is an error document (see OverdriveClientRequests.metadata), and such a
            # document passes the check above while carrying no "id" for the extractor
            # to key on. Report it as unusable; the caller still attempts the
            # circulation update, which is worth doing on its own.
            self.log.warning(
                f"Could not extract bibliographic data from Overdrive metadata for "
                f"{identifier}. Skipping bibliographic update."
            )
            return None

        if not self._bibliographic_matches_identifier(bibliographic, identifier):
            # The record we converted describes a different title than the one the
            # product list pointed us at. Applying it would raise PalaceValueError
            # deep inside the apply worker, where it is far harder to connect back
            # to the collection being imported, so stop here instead.
            self.log.warning(
                f"Overdrive metadata for {identifier} reports a different identifier "
                f"({bibliographic.primary_identifier_data}). Skipping bibliographic "
                f"update."
            )
            return None

        return bibliographic

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
        :param policy: Replacement policy for the apply. Passed to a bibliographic
            apply, and to a circulation apply whose formats came from the metadata
            record -- see the dispatch below for why the two differ.
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
            book["metadata"] = self._api.metadata_lookup(identifier=identifier).raw

        bibliographic = self._usable_bibliographic(book, identifier)

        # availability needs to be checked/updated in all but a few instances so it is
        # probably not worth the compute time to save ourselves a handful of unnecessary updates.
        circulation = None
        availability_data = book.get("availabilityV2", None)
        if not availability_data:
            # This is a rare and probably transient case where the availabilityV2
            # was not retrieved due to a 404 from OD.
            self.log.warning(
                f"No availabilityV2 found for book {identifier}. book={book}.  This state can "
                f"arise when the OD returns a 404 for the availability url."
            )
        else:
            availability = Availability.model_validate(availability_data)
            circulation = self._extractor.book_info_to_circulation(availability)
            # The circulation should never be null here because there is a non-null entry for availabilityV2 in the
            # book dictionary.  Mypy complains without an assertion or type hints.
            assert circulation

        # Overdrive is the only distributor whose formats and availability counts come
        # from two different documents, and converting them produced two separate
        # CirculationData objects. Fold them into one before anything is dispatched.
        #
        # This is not tidying. Each object stamps ``pool.updated_at`` with its own
        # ``as_of_timestamp`` on apply, and ``should_apply_to`` rejects any snapshot
        # strictly older than that stamp. Two objects built microseconds apart and
        # queued as two independent tasks can therefore arrive in either order, and if
        # the availability one lands first the formats one is discarded -- while the
        # edition's hash is still stamped, so no later import retries it. One object
        # has one timestamp and cannot race itself. OPDS, ODL and Boundless have always
        # worked this way; see ``opds/importer.py`` and ``boundless/importer.py``.
        formats_from_metadata = False
        if bibliographic is not None and bibliographic.circulation is not None:
            if circulation is None:
                # No availability document, so the formats-only snapshot the converter
                # built is all we have. Dispatch it as-is.
                circulation = bibliographic.circulation
            else:
                # Assignment re-runs CirculationData's normalising validator, which is
                # what we want; it also clears the memoised hash.
                circulation.formats = bibliographic.circulation.formats
                bibliographic.circulation = circulation
            formats_from_metadata = True

        # Dispatch exactly one task. ``BibliographicData.apply`` applies the circulation
        # data hanging off it, so queuing both would make two writers for one pool.
        if bibliographic is not None and bibliographic.needs_apply(self._db):
            changed = True
            apply_bibliographic(
                bibliographic,
                collection_id=self._collection.id,
                replace=policy,
            )
        elif circulation is not None and circulation.needs_apply(
            self._db, self._collection
        ):
            # The bibliographic data is unchanged but the circulation data is not --
            # an availability flip, or a change to the format list. Apply it on its own.
            #
            # Whether this apply may *remove* formats depends on where its format list
            # came from, and getting it wrong is damaging in both directions:
            #
            # * Formats read from the metadata record are the full, authoritative list,
            #   so a format Overdrive has dropped must be removed. Without the policy,
            #   ``replace.formats`` defaults to False, the stale delivery mechanism
            #   survives, and the apply still records the new hash -- so no later import
            #   retries the removal and patrons keep being offered a format Overdrive no
            #   longer has.
            # * An availability-only snapshot has no formats at all. The Advantage path
            #   skips the metadata lookup for any title the parent collection already
            #   imported, so passing the policy there would treat an empty list as
            #   authoritative and delete every delivery mechanism -- and because
            #   delivery mechanisms are keyed on data source and identifier rather than
            #   on collection, that would take the parent's titles with it.
            changed = True
            apply_circulation(
                circulation,
                collection_id=self._collection.id,
                replace=policy if formats_from_metadata else None,
            )

        return identifier, changed

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

        2. **Change Detection**:
           - Each book queues at most one apply. If its metadata changed, the
             bibliographic apply runs and carries the circulation data with it.
             Otherwise the circulation data is checked on its own, and applied if
             the availability or the format list has moved.
           - See :meth:`_process_book` for why the two halves have to travel on a
             single object rather than as two independent tasks.

        :param apply_bibliographic: Callback to apply bibliographic metadata updates (title, author, etc.)
        :param apply_circulation: Callback to apply circulation data updates (copies owned, available, etc.)
        :param modified_since: Only process books modified after this datetime. If None, processes all books.
        :param endpoint: OverDrive API endpoint to fetch from. If None, generates a default endpoint
                         starting from modified_since
        :param page_size: Number of items to fetch per page
        :return: FeedImportResult containing current_page (the endpoint that was processed),
                 next_page (the next endpoint to process, None if done),
                 and processed_count (number of books processed in this call)

        .. note::
           Side Effects:
           - Creates/updates Identifier records in the database
           - Adds identifiers to self._identifier_set if provided
           - Records timing and achievement data in the Timestamp
           - Logs progress information
        """
        identifiers = []
        policy = self.REPLACEMENT_POLICY

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
        # Fetch metadata upfront if no parent identifier set is provided.  Practically speaking,
        # if there is no parent identifier set, then the collection being imported is a
        # main rather than an advantage collection.  We always fetch availability because we do not gain
        # much by trying to fetch it lazily.
        fetch_metadata = self._parent_identifiers is None
        book_data, next_endpoint = asyncio.run(
            self._api.async_requests.fetch_book_info_list(
                endpoint,
                fetch_metadata=fetch_metadata,
                fetch_availability=True,
            )
        )
        for book in book_data:
            identifier, _ = self._process_book(
                book, fetch_metadata, policy, apply_bibliographic, apply_circulation
            )
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
        return FeedImportResult(
            next_page=next_endpoint,
            current_page=endpoint,
            processed_count=len(identifiers),
        )

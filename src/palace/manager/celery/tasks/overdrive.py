import datetime
from typing import Any, Literal, TypedDict, TypeGuard
from uuid import uuid4

from celery import chain, chord, group, shared_task
from sqlalchemy.orm import Session

from palace.util.datetime_helpers import utc_now

from palace.manager.celery.importer import (
    import_all as create_import_tasks,
    import_key,
    import_workflow_lock,
    reap_key,
    reap_workflow_lock,
    workflow_lock_guard,
)
from palace.manager.celery.task import Task
from palace.manager.celery.tasks import apply
from palace.manager.celery.tasks.identifiers import (
    available_identifiers_query,
    queue_unavailable_updates,
)
from palace.manager.celery.utils import load_from_id, signature_with, validate_not_none
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.license.overdrive.api import (
    BookInfoEndpoint,
    OverdriveAPI,
)
from palace.manager.integration.license.overdrive.importer import OverdriveImporter
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePoolStatus
from palace.manager.util.http.exception import (
    BadResponseException,
    RemoteIntegrationException,
    RequestTimedOut,
)

IMPORT_SKIPPED: str = "import_skipped"

# How far a completed crawl may fall short of Overdrive's reported totalItems before
# it is treated as incomplete. A crawl of a large collection runs for hours while
# titles are added and removed under it, so the count is expected to move a little;
# the floor keeps small collections from being held to an unreachable standard. The
# shortfall being allowed for is churn during the crawl, which does not scale with
# collection size, so the cap keeps the allowance below one page -- a crawl that lost
# a whole page must never be excused.
REAP_CRAWL_ALLOWANCE: float = 0.001
REAP_CRAWL_ALLOWANCE_FLOOR: int = 10
REAP_CRAWL_ALLOWANCE_CAP: int = 500

# How long the reaper's identifier sets outlive their creation. A crawl of the largest
# collection can span more than a day, and both halves of the comparison have to still
# be there when it finishes. The sets are keyed on the run's task id, so an abandoned
# run's keys cannot be picked up by the next pass -- they only take up space until they
# expire.
REAP_IDENTIFIER_SET_LIFETIME: datetime.timedelta = datetime.timedelta(hours=36)


class ImportSkippedPayload(TypedDict):
    """Payload returned when import is skipped (workflow lock already held)."""

    import_skipped: Literal[True]


class ImportRouterResult(TypedDict, total=False):
    """Result of import_result_router: chord_id when chord runs, import_skipped when skipped."""

    import_skipped: Literal[True]
    chord_id: str | None


@shared_task(
    queue=QueueNames.default,
    bind=True,
    max_retries=4,
    autoretry_for=(BadResponseException, RequestTimedOut),
    throws=(RemoteIntegrationException,),
    retry_backoff=60,
)
def import_collection(
    task: Task,
    collection_id: int,
    *,
    import_all: bool = False,
    page: str | None = None,
    modified_since: datetime.datetime | None = None,
    start_time: datetime.datetime | None = None,
    return_identifiers: bool = True,
    parent_identifiers: dict[str, Any] | None = None,
) -> IdentifierSet | ImportSkippedPayload | None:
    """
    Run an import for a single Overdrive collection.

    This task processes identifiers from the OverDrive API in a paginated
    fashion. When multiple pages are present, the task chains itself using task.replace()
    to process subsequent pages while maintaining the same modified_since timestamp
    and start_time across all pages.

    :param collection_id: The ID of the collection to import
    :param import_all: If True, import all titles regardless of whether they have changed.
        If False, import titles that have changed since the modified_since date.
    :param page: The "page" to be processed. The page param is a url represented as a string. A None value means the
        import will start from the beginning of the set.
    :param modified_since: Only process titles modified after this datetime. This field parameter is ignored if
        import_all is True. If import_all is False and modified_since is None, then only titles after the last import
        timestamp's start time will be imported. If there is no prior timestamp, all titles will be imported.
         Finally, if import_all is False and modified_since is not None, then
        only titles modified after modified_since date will be imported.
    :param start_time: The datetime when this import process began. Used to update
        the collection's timestamp only after all pages have been processed. If None,
        will be set to the current time on the first page.
    :param return_identifiers: A running set of identifiers that have been processed so far in this run.
    :param parent_identifiers: A running set of parent identifiers (if not a parent collection)
        that were processed before this run started. This value is a serialized representation of the
        parent_identifier IdentifierSet.
    :return: IdentifierSet when import completes and return_identifiers is True; None when
        return_identifiers is False or collection is marked for deletion; {IMPORT_SKIPPED: True}
        when the workflow lock is held and another import is already in progress.
    """
    redis = task.services.redis().client()
    registry = task.services.integration_registry().license_providers()

    if start_time is None:
        start_time = utc_now()

    with workflow_lock_guard(task, collection_id, label="OverDrive import") as proceed:
        if not proceed:
            return _import_skipped_payload()

        with task.transaction() as session:
            collection = load_from_id(session, Collection, collection_id)
            collection_name = collection.name

            identifier_set = (
                IdentifierSet(redis, import_key(collection.id, task.request.id))
                if return_identifiers
                else None
            )

            if collection.marked_for_deletion:
                task.log.warning(
                    f"This collection is marked for deletion. "
                    f"Skipping import of '{collection_name}'."
                )
                return identifier_set

            parent_identifier_set = (
                rehydrate_identifier_set(task, parent_identifiers)
                if parent_identifiers
                else None
            )

            importer = OverdriveImporter(
                db=session,
                collection=collection,
                registry=registry,
                identifier_set=identifier_set,
                parent_identifier_set=parent_identifier_set,
            )

            if modified_since is None:
                if import_all:
                    modified_since = None
                else:
                    timestamp = importer.get_timestamp()
                    modified_since = timestamp.start

            task.log.info(
                f"OverDrive import started: '{collection_name}' Modified since: {modified_since}, "
                f"page: {None if not page else page}"
            )

            endpoint = None if not page else BookInfoEndpoint(page)

            result = importer.import_collection(
                apply_bibliographic=apply.bibliographic_apply.delay,
                apply_circulation=apply.circulation_apply.delay,
                endpoint=endpoint,
                modified_since=modified_since,
            )

            task.log.info(
                f"OverDrive import page complete: '{collection_name}' Page: {result.current_page}. "
                f"Processed: {result.processed_count}. "
            )

            if identifier_set:
                task.log.info(
                    f"OverDrive collection import '{collection_name}': Total processed in run so far: {identifier_set.len()}"
                )

            if result.next_page is None:
                # We are done. We only update the timestamp once we have processed all pages.
                # To make sure that if we fail or are interrupted, we re-process any
                # titles we may have missed.
                timestamp = importer.get_timestamp()
                timestamp.start = start_time
                timestamp.finish = utc_now()
                task.log.info(
                    f"OverDrive import complete: '{collection_name}' Total time: {timestamp.elapsed}."
                )

        if result.next_page is not None:
            task.log.info(
                f"OverDrive import re-queueing: '{collection_name}' Next page: {result.next_page}."
            )
            # Serialize parent_identifier_set to dict for passing to next task
            serialized_parent_identifiers = (
                parent_identifier_set.__json__() if parent_identifier_set else None
            )
            raise task.replace(
                signature_with(
                    task,
                    parent_identifiers=serialized_parent_identifiers,
                    page=result.next_page.url,
                    # modified_since and start_time are resolved from None on the first
                    # page, so they must be carried forward explicitly rather than
                    # refilled from the original arguments.
                    modified_since=modified_since,
                    start_time=start_time,
                )
            )
        else:
            return identifier_set


@shared_task(
    queue=QueueNames.default,
    bind=True,
    max_retries=4,
    autoretry_for=(BadResponseException, RequestTimedOut),
    throws=(RemoteIntegrationException,),
    retry_backoff=60,
)
def import_collection_group(
    task: Task,
    collection_id: int,
    *,
    import_all: bool = False,
    modified_since: datetime.datetime | None = None,
    start_time: datetime.datetime | None = None,
) -> dict[str, Any] | ImportSkippedPayload:
    """Import an Overdrive collection and all its child (Advantage) collections.

    This task orchestrates the import of a parent Overdrive collection and chains
    the import of any associated Overdrive Advantage (child) collections. It uses
    Celery's chord pattern to run child imports in parallel after the parent completes,
    and then cleans up the shared identifier set.

    Workflow:
        1. Imports the parent collection
        2. Passes the parent's identifier set to child collections
        3. Imports all child collections in parallel (if any exist)
        4. Cleans up the shared identifier set after all imports complete

    :param collection_id: The ID of the parent collection to import
    :param import_all: If True, import all titles regardless of change status.
                       If False, only import changed titles based on modified_since.
    :param modified_since: Only process titles modified after this datetime.
                          See import_collection docstring for detailed behavior.
    :param start_time: The datetime when this import began. Used to update the
                      collection's timestamp. If None, uses current time.
    :return: Dictionary containing the chain_id for tracking the async import chain,
             or an import-skipped payload if another workflow is already in progress.
    """
    redis = task.services.redis().client()
    # Defense-in-depth: skip chain creation if a workflow is already running.
    # This prevents redundant chains from being queued when the workflow lock
    # expires between pages and a new Beat tick fires before import_collection
    # can re-acquire and enforce the first-page guard itself.
    if import_workflow_lock(redis, collection_id, str(uuid4())).locked():
        task.log.info(
            f"OverDrive import skipped for collection {collection_id}: "
            "another import is already in progress (skipping at group level)."
        )
        return _import_skipped_payload()

    result = chain(
        import_collection.s(
            collection_id=collection_id,
            import_all=import_all,
            page=None,
            parent_identifiers=None,
            return_identifiers=True,
            modified_since=modified_since,
            start_time=start_time,
        ),
        import_result_router.s(
            collection_id=collection_id,
            import_all=import_all,
            modified_since=modified_since,
        ),
    )()
    return {"chain_id": result.id}


def _is_import_skipped(
    result: IdentifierSet | dict[str, Any] | None,
) -> TypeGuard[dict[str, Any]]:
    """Type guard: True when result is the skip payload."""
    return isinstance(result, dict) and result.get(IMPORT_SKIPPED) is True


def _import_skipped_payload() -> ImportSkippedPayload:
    """Build the skip payload for return values."""
    return {"import_skipped": True}


@shared_task(queue=QueueNames.default, bind=True)
def import_result_router(
    task: Task,
    import_result: IdentifierSet | dict[str, Any] | None,
    collection_id: int,
    import_all: bool,
    modified_since: datetime.datetime | None,
) -> ImportRouterResult:
    """Route import result to child imports or short-circuit when skipped.

    This task receives the result of import_collection and either invokes the
    child-import chord (when the import ran) or returns early (when the import
    was skipped due to another import already in progress).

    :param import_result: Result from import_collection. Either an IdentifierSet
        (or its serialized form when passed through Celery), ImportSkippedPayload
        when skipped, or None when the import returned no identifier set.
    :param collection_id: The parent collection ID.
    :param import_all: Whether to import all titles in children.
    :param modified_since: Only import titles modified after this datetime.
    :return: {"chord_id": "..."} when chord is invoked, {IMPORT_SKIPPED: True}
        when skipped, or {"chord_id": None} when import_result is None.
    """
    if _is_import_skipped(import_result):
        task.log.info(
            f"OverDrive import skipped for collection {collection_id}: "
            "skipping child imports (another import already in progress)."
        )
        skip_result: ImportRouterResult = {"import_skipped": True}
        return skip_result

    if import_result is None:
        task.log.warning(
            f"OverDrive import for collection {collection_id}: no identifier set "
            "returned; skipping child imports."
        )
        return {"chord_id": None}

    identifier_set_info = (
        import_result.__json__()
        if isinstance(import_result, IdentifierSet)
        else import_result
    )
    async_res = import_children_and_cleanup_chord.apply_async(
        args=[identifier_set_info, collection_id, import_all, modified_since],
    )
    return {"chord_id": async_res.id}


def rehydrate_identifier_set(
    task: Task, identifier_set_info: dict[str, Any]
) -> IdentifierSet:
    """Reconstruct an IdentifierSet from its serialized representation.

    This helper function takes a dictionary containing identifier set metadata
    (specifically the Redis key) and recreates the IdentifierSet object that
    can be used to access the data in Redis.

    :param task: The Celery task instance (provides access to Redis client)
    :param identifier_set_info: Dictionary containing the identifier set's key
                                Format: {"key": ["key", "parts"]}
    :return: Reconstructed IdentifierSet connected to Redis
    """
    return IdentifierSet(task.services.redis().client(), identifier_set_info["key"])


@shared_task(
    queue=QueueNames.default,
    bind=True,
    max_retries=4,
    autoretry_for=(BadResponseException, RequestTimedOut),
    throws=(RemoteIntegrationException,),
    retry_backoff=60,
)
def import_children_and_cleanup_chord(
    task: Task,
    identifier_set_info: dict[str, Any],
    collection_id: int,
    import_all: bool,
    modified_since: datetime.datetime,
) -> dict[str, Any]:
    """Import child (Advantage) collections and clean up the parent identifier set.

    This task is called as the callback/link after a parent collection import completes.
    It receives the parent collection's identifier set and uses a Celery chord to:

    1. Import all child Overdrive Advantage collections in parallel, passing the
       parent's identifier set to optimize metadata fetching (children skip books
       already imported by the parent)
    2. After all child imports complete, remove the shared identifier set from Redis

    The chord pattern ensures the cleanup (step 2) only runs after all child imports
    have finished, preventing premature deletion of the shared identifier set.

    :param identifier_set_info: Serialized parent identifier set info from the parent import.
                                Format: {"key": ["redis", "key", "parts"]}
    :param collection_id: The ID of the parent collection whose children to import
    :param import_all: If True, import all titles in children regardless of change status.
                      If False, only import changed titles.
    :param modified_since: Only process titles modified after this datetime in child collections
    :return: Dictionary containing the chord ID for tracking: {"chord_id": "..."}

    .. note::
       If the parent collection has no children, the chord will still be created
       but with an empty group, and cleanup will proceed normally.
    """
    with task.session() as session:
        collection = load_from_id(session, Collection, collection_id)
        identifier_set = rehydrate_identifier_set(task, identifier_set_info)
        header = group(
            [
                import_collection.si(
                    collection_id=c.id,
                    page=None,
                    import_all=import_all,
                    modified_since=modified_since,
                    parent_identifiers=identifier_set,
                )
                for c in collection.children
            ]
        )
        async_res = chord(
            header=header,
            body=remove_identifier_set.si(identifier_set_info=identifier_set_info),
        ).apply_async()
        return {"chord_id": async_res.id}


@shared_task(
    queue=QueueNames.default,
    bind=True,
    max_retries=4,
    autoretry_for=(BadResponseException, RequestTimedOut),
    throws=(RemoteIntegrationException,),
    retry_backoff=60,
)
def remove_identifier_set(task: Task, identifier_set_info: dict[str, Any]) -> None:
    """Clean up a temporary identifier set from Redis after import completes.

    This task is used as the callback body of the chord in import_children_and_cleanup_chord.
    It deletes the temporary Redis set used to share identifiers between
    parent and child collection imports.  If the set doesn't exist, the operation will
    still succeed.


    :param identifier_set_info: Serialized identifier set info.
                                Format: {"key": ["redis", "key", "parts"]}
    """
    identifier_set = rehydrate_identifier_set(task, identifier_set_info)
    if not identifier_set.exists():
        task.log.warning(
            f"Identifier set (key={identifier_set._key}) does not exist in Redis. Skipping cleanup."
        )
    else:
        identifier_set.delete()


@shared_task(queue=QueueNames.default, bind=True)
def import_all_collections(task: Task, *, import_all: bool = False) -> None:
    """
    A shared task that loops through all OverDrive parent collections and kick off an
    import task for each.
    """
    with task.session() as session:
        registry = task.services.integration_registry().license_providers()
        collection_query = Collection.select_by_protocol(
            OverdriveAPI, registry=registry
        ).where(Collection.parent == None)
        create_import_tasks(
            session.scalars(collection_query).all(),
            import_collection_group.s(
                import_all=import_all,
            ),
            task.log,
        )


@shared_task(queue=QueueNames.default, bind=True)
def reap_all_collections(task: Task) -> None:
    """
    Queue a reap task for every Overdrive collection.

    Includes both parent collections and Advantage (child) collections, since titles
    can be removed from either independently. Each collection is reaped against its
    own collection token, which is what keeps parent and Advantage collections
    consistent: an Advantage token lists only the titles that account owns, exactly
    the titles the importer wrote into that collection.
    """
    with task.session() as session:
        registry = task.services.integration_registry().license_providers()
        collection_query = Collection.select_by_protocol(
            OverdriveAPI, registry=registry
        )
        collections = session.scalars(collection_query).all()
    for collection in collections:
        OverdriveAPI.reap_task(collection.id).apply_async()


@shared_task(
    queue=QueueNames.default,
    bind=True,
    max_retries=4,
    autoretry_for=(BadResponseException, RequestTimedOut),
    throws=(RemoteIntegrationException,),
    retry_backoff=60,
)
def reap_collection(
    task: Task,
    collection_id: int,
    *,
    offset: int = 0,
    total_items: int | None = None,
    previous_offset: int | None = None,
    batch_size: int | None = None,
) -> IdentifierSet | None:
    """
    Enumerate the titles an Overdrive collection currently lists.

    Walks the collection's product list one page at a time, re-queuing itself via
    ``task.replace()`` until the crawl is complete, and collects the identifiers into
    a Redis set. Only ids and ownership are read -- no metadata or availability
    lookups -- so a whole collection costs a handful of requests rather than one per
    title. See :meth:`OverdriveAPI.next_product_offset` for how the crawl is paged.

    Titles are removed from a collection in two ways, and this handles them
    differently:

    * Overdrive keeps weeded and expired titles in the product list, flagged
      ``isOwnedByCollections: false``. That is direct evidence, so those titles are
      marked as each page arrives, whether or not the crawl goes on to complete.
    * A title can also leave the product list entirely. That can only be detected by
      its *absence*, which is what the returned set is for: it is the "active" half of
      :func:`~palace.manager.celery.tasks.identifiers.create_mark_unavailable_chord`,
      and identifiers held locally but missing from it are marked exhausted.

    Because the second half acts on absence, a partial crawl would mark live titles as
    gone. The crawl is therefore checked against Overdrive's ``totalItems`` before the
    set is handed on, and ``None`` is returned if it cannot be shown to be complete --
    which the chord body treats as a failed run and aborts. Titles flagged unowned are
    still marked in that case, since nothing about them was inferred. Concurrency is
    governed by :func:`~palace.manager.celery.importer.workflow_lock_guard`.

    :param collection_id: The ID of the Overdrive collection to reap.
    :param offset: Where in the product list to fetch the next page from. A crawl
        starts at 0 and walks backwards from the end; see
        :meth:`OverdriveAPI.next_product_offset`.
    :param total_items: Overdrive's collection size, as reported by the crawl's first
        page and carried across the remaining ones.
    :param previous_offset: Where the previous page of the backwards walk started, so
        this one can be shown to reach it. None on the first page of a walk, which has
        nothing above it to join up with.
    :param batch_size: Only ever set on a page queued by the previous per-title
        reaper, whose ``offset`` meant something else entirely. Accepted so those
        messages are discarded rather than failing to bind after a deploy; remove it
        once no such message can still be in flight.
    :return: The identifiers the collection currently lists, or None if the crawl was
        skipped, aborted, or could not be verified as complete.
    """
    if batch_size is not None:
        # A page left over from the per-title reaper, whose `offset` was the last
        # Identifier.id it processed. Running it as a product-list offset would crawl
        # from an arbitrary point, so the run is dropped and the next scheduled one
        # starts clean.
        task.log.info(
            f"Discarding a reap page queued by the previous reaper implementation "
            f"for collection {collection_id}."
        )
        return None

    with workflow_lock_guard(
        task,
        collection_id,
        label="Overdrive reaper",
        lock_factory=reap_workflow_lock,
    ) as proceed:
        if not proceed:
            return None

        redis = task.services.redis().client()
        identifier_set = IdentifierSet(
            redis,
            reap_key(collection_id, task.request.id),
            # The crawl outlives the default expiry on a large collection, and its
            # earlier pages have to still be there when the last one finishes. Matches
            # the lifetime given to the set it will be compared against.
            expire_time=REAP_IDENTIFIER_SET_LIFETIME,
        )

        # Committed rather than read-only: resolving the collection token refreshes
        # the cached library document, and a crawl that discarded that write would
        # re-fetch it from Overdrive on every page.
        with task.transaction() as session:
            collection = load_from_id(session, Collection, collection_id)
            collection_name = collection.name

            if collection.marked_for_deletion:
                task.log.warning(
                    f"This collection is marked for deletion. "
                    f"Skipping reap of '{collection_name}'."
                )
                identifier_set.delete()
                return None

            api = OverdriveAPI(session, collection)
            result = api.fetch_product_page(api.product_page_endpoint(offset))
            next_offset = api.next_product_offset(result, offset)
            marked = _mark_unowned_identifiers(
                task, session, collection, result.unowned
            )

        identifier_set.add(*result.listed)
        if total_items is None:
            total_items = result.total_items
        if not result.pageable:
            # The crawl stopped somewhere unknown rather than at the start of the
            # list, so how much it covered is not a question the allowance for churn
            # can answer.
            task.log.error(
                f"Overdrive reaper aborting for collection '{collection_name}': the "
                f"page at offset {offset} carries no usable totalItems or page size, "
                f"so the crawl cannot be continued or verified."
            )
            identifier_set.delete()
            return None

        if (
            previous_offset is not None
            and offset + len(result.listed) < previous_offset
        ):
            # The walk steps back by a page size, but a page is free to return fewer
            # titles than that. When one does, the step overshoots and leaves a strip
            # of the list uncrawled -- which reaches the completeness check as a
            # shortfall indistinguishable from titles removed mid-crawl. Pages are
            # only legitimately short at the end of the list, so every other page is
            # required to reach the one before it.
            task.log.error(
                f"Overdrive reaper aborting for collection '{collection_name}': the "
                f"page at offset {offset} returned {len(result.listed)} titles and so "
                f"stops short of the region already crawled at {previous_offset}. "
                f"Refusing to reap across a gap."
            )
            identifier_set.delete()
            return None
        if marked:
            task.log.info(
                f"Overdrive reaper marked {marked} title(s) Overdrive no longer owns "
                f"in collection '{collection_name}'."
            )

        if next_offset is not None:
            task.log.info(
                f"Overdrive reaper crawling '{collection_name}': "
                f"{identifier_set.len()} of {total_items} titles seen."
            )
            raise task.replace(
                signature_with(
                    task,
                    offset=next_offset,
                    total_items=total_items,
                    # The first page of a walk starts at 0 and is followed by a jump
                    # to the end of the list, so the page after it has nothing to
                    # join up with.
                    previous_offset=offset or None,
                )
            )

        crawled = identifier_set.len()
        if not _crawl_is_complete(
            task, collection_name, crawled, total_items, single_page=offset == 0
        ):
            identifier_set.delete()
            return None

        task.log.info(
            f"Overdrive reaper crawl complete for collection '{collection_name}': "
            f"{crawled} titles listed of {total_items} reported."
        )
        return identifier_set


def _mark_unowned_identifiers(
    task: Task,
    session: Session,
    collection: Collection,
    unowned: tuple[IdentifierData, ...],
) -> int:
    """Mark the titles on a crawled page that Overdrive reports it no longer owns.

    Overdrive keeps weeded and expired titles in the product list rather than dropping
    them, so this is a statement that a title is gone rather than an inference from its
    absence -- which is why it does not wait on the crawl being shown complete.

    Only titles we still record as available are marked, so the titles Overdrive keeps
    listing as unowned do not generate an apply task on every pass.
    """
    if not unowned:
        return 0

    identifiers = session.scalars(
        available_identifiers_query(collection.id).where(
            Identifier.type == Identifier.OVERDRIVE_ID,
            Identifier.identifier.in_(
                [identifier.identifier for identifier in unowned]
            ),
        )
    ).all()
    if not identifiers:
        return 0

    data_source = validate_not_none(
        collection.data_source,
        message="Collection has no data source! (data_source is None).",
    )
    return queue_unavailable_updates(
        (IdentifierData.from_identifier(identifier) for identifier in identifiers),
        collection_id=collection.id,
        collection_name=collection.name,
        data_source_name=data_source.name,
        # Overdrive drops titles from a collection when a lease ends or a purchase is
        # returned, not only when a title is withdrawn, so this means "no copies here
        # now" rather than "revoke outstanding loans and holds".
        status=LicensePoolStatus.EXHAUSTED,
        log=task.log,
    )


def _crawl_is_complete(
    task: Task,
    collection_name: str,
    crawled: int,
    total_items: int | None,
    *,
    single_page: bool,
) -> bool:
    """Decide whether a finished crawl covered the whole collection.

    ``crawled`` counts *distinct* identifiers, so a title returned twice cannot paper
    over one that was missed.

    A crawl runs for hours against a collection that keeps changing underneath it, so a
    small discrepancy against ``totalItems`` is expected -- titles added or removed
    mid-crawl move the count. A large shortfall means pages were missed, and reaping on
    that would mark live titles as gone.

    :param single_page: Whether the whole collection arrived in one response. There is
        no window for churn in that case -- the products and the count were produced
        together -- so they are required to agree exactly.
    """
    if total_items is None:
        task.log.error(
            f"Overdrive reaper aborting for collection '{collection_name}': "
            f"Overdrive did not report totalItems, so the crawl cannot be verified."
        )
        return False

    allowance = (
        0
        if single_page
        else min(
            REAP_CRAWL_ALLOWANCE_CAP,
            max(REAP_CRAWL_ALLOWANCE_FLOOR, int(total_items * REAP_CRAWL_ALLOWANCE)),
        )
    )
    if crawled + allowance < total_items:
        task.log.error(
            f"Overdrive reaper aborting for collection '{collection_name}': crawl saw "
            f"{crawled} titles but Overdrive reports {total_items} "
            f"(allowance {allowance}). Refusing to reap on an incomplete crawl."
        )
        return False

    return True

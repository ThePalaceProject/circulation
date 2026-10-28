import datetime
from collections import defaultdict
from collections.abc import Sequence
from typing import Any, Literal, NamedTuple, TypedDict, TypeGuard
from uuid import uuid4

from celery import chain, chord, group, shared_task
from sqlalchemy import Boolean, and_, exists, func, or_, select
from sqlalchemy.orm import Session
from sqlalchemy.sql import ColumnElement, Select

from palace.util.datetime_helpers import utc_now

from palace.manager.celery.importer import (
    backfill_workflow_lock,
    import_all as create_import_tasks,
    import_key,
    import_workflow_lock,
    reap_workflow_lock,
    workflow_lock_guard,
)
from palace.manager.celery.task import Task
from palace.manager.celery.tasks import apply
from palace.manager.celery.utils import load_from_id, signature_with
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.integration.license.overdrive.importer import OverdriveImporter
from palace.manager.integration.license.overdrive.requests import BookInfoEndpoint
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.contributor import Contribution
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.http.exception import (
    BadResponseException,
    RemoteIntegrationException,
    RequestTimedOut,
)

IMPORT_SKIPPED: str = "import_skipped"


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
    can be removed from either independently.
    """
    with task.session() as session:
        registry = task.services.integration_registry().license_providers()
        collection_query = Collection.select_by_protocol(
            OverdriveAPI, registry=registry
        )
        collections = session.scalars(collection_query).all()
    for collection in collections:
        reap_collection.delay(collection.id)


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
    batch_size: int = 50,
) -> None:
    """
    Check for books that are in the local collection but have left our Overdrive collection.

    Processes identifiers in batches, re-queuing itself via task.replace() until all
    identifiers have been checked. Concurrency is governed by
    :func:`~palace.manager.celery.importer.workflow_lock_guard`.

    :param collection_id: The ID of the Overdrive collection to reap.
    :param offset: The last Identifier.id processed; used to resume across batches.
    :param batch_size: Number of identifiers to process per batch.
    """
    with workflow_lock_guard(
        task,
        collection_id,
        label="Overdrive reaper",
        lock_factory=reap_workflow_lock,
    ) as proceed:
        if not proceed:
            return

        new_offset = 0
        processed_count = 0
        collection_name = None

        with task.transaction() as session:
            collection = load_from_id(session, Collection, collection_id)
            collection_name = collection.name

            identifiers = (
                session.execute(
                    select(Identifier)
                    .join(Identifier.licensed_through)
                    .where(
                        LicensePool.collection_id == collection_id,
                        Identifier.id > offset,
                    )
                    .order_by(Identifier.id)
                    .limit(batch_size)
                )
                .unique()
                .scalars()
                .all()
            )

            if not identifiers:
                task.log.info(
                    f"Overdrive reaper complete for collection '{collection_name}'."
                )
                return

            api = OverdriveAPI(session, collection)
            for identifier in identifiers:
                api.update_licensepool(identifier.identifier)

            new_offset = identifiers[-1].id
            processed_count = len(identifiers)

        task.log.info(
            f"Overdrive reaper: processed {processed_count} identifiers for "
            f"collection '{collection_name}' (offset: {offset} -> {new_offset})."
        )

        if processed_count == batch_size:
            raise task.replace(
                signature_with(
                    task,
                    offset=new_offset,
                )
            )


# How many titles a single backfill batch processes before the task re-queues
# itself. Each title costs one blocking Overdrive metadata request, so batches are
# kept small to keep any one task short.
BACKFILL_BATCH_SIZE: int = 50


class BackfillClauses(NamedTuple):
    """The three damage symptoms, kept separate so they can be counted apart.

    A dry run reports how many pools each one selects, which is the only way to
    find out whether they are each earning their place or whether one of them
    already covers the others.
    """

    unopenable: ColumnElement[Boolean]
    """No available delivery mechanism: nothing a patron could open."""

    unsearchable: ColumnElement[Boolean]
    """No work, or one that never became presentation-ready: cannot appear in search."""

    incomplete: ColumnElement[Boolean]
    """A presentation edition missing a publisher or any contributions."""


def _backfill_clauses() -> BackfillClauses:
    """Build the filter identifying pools left damaged by the old Overdrive importer.

    Until it was fixed, the importer converted the product-list summary rather than
    the fetched metadata record. The summary carries no ``creators``, ``publisher``,
    ``publishDate`` or ``languages``, and its ``formats`` entries carry only an id and
    a name, so the resulting records lost their author, publisher, publication date,
    subjects and per-format identifiers. Separately, a title's formats could not
    trigger a re-apply, so a book that gained a usable format after it was first
    imported never picked it up.

    Three symptoms follow from that, and a pool showing any of them is worth
    refreshing. They are combined with OR rather than queried separately so that a
    pool showing more than one symptom is still visited, and paid for, exactly once.

    :return: The three clauses, named. Each is suitable for a ``LicensePool`` query
        that has been outer-joined to ``Work`` and to the presentation ``Edition``.
        :func:`_needs_backfill` ORs them together; the dry run counts them apart.
    """
    # (a) Nothing a patron could actually open.
    no_delivery_mechanism = ~exists().where(
        and_(
            LicensePoolDeliveryMechanism.data_source_id == LicensePool.data_source_id,
            LicensePoolDeliveryMechanism.identifier_id == LicensePool.identifier_id,
            LicensePoolDeliveryMechanism.available == True,  # noqa: E712
        )
    )

    # (b) No work at all, or one that never became presentation ready, so the title
    #     cannot surface in the search index.
    no_presentation_ready_work = or_(
        LicensePool.work_id.is_(None),
        Work.presentation_ready.is_(False),
    )

    # (c) A presentation edition missing fields the summary never carried.
    #
    #     Be precise about what this detects. A summary-derived apply never
    #     *erases* a publisher or a contribution: _update_basic_fields skips None
    #     and empty values, and update_contributions collects the existing
    #     contributors only when the incoming data has contributors of its own, so
    #     an empty incoming list deletes nothing. These columns being empty
    #     therefore means no import ever supplied them -- not that a bad import
    #     overwrote them.
    #
    #     The consequence is a blind spot: a title imported correctly before the
    #     regression and then re-imported by the broken importer keeps its
    #     publisher and contributors, so it does not match here. Clause (a) is
    #     what catches that case. This clause is cheap belt-and-braces, not the
    #     primary signal -- on the one production copy measured it selected no
    #     pool that (a) had not already selected.
    degraded_bibliographic_data = or_(
        LicensePool.presentation_edition_id.is_(None),
        Edition.publisher.is_(None),
        ~exists().where(Contribution.edition_id == Edition.id),
    )

    return BackfillClauses(
        unopenable=no_delivery_mechanism,
        unsearchable=no_presentation_ready_work,
        incomplete=degraded_bibliographic_data,
    )


def looks_like_a_magazine() -> ColumnElement[Boolean]:
    """Identify titles that are almost certainly magazine issues.

    Overdrive serves magazines under a format id we do not map, so converting one
    produces no formats and the title ends up with no delivery mechanism. Re-fetching
    its metadata cannot change that, so a repair pass over magazines spends one
    blocking Overdrive request per title to leave it exactly as it was -- and rewrites
    its hashes for nothing.

    We have no magazine support and no periodical records: every Overdrive edition is
    filed as a book, an audiobook or a video. So the only signal available is the
    shape of the record. A magazine issue carries its own publication as its series,
    and on the production copy measured that held for 8,233 of the 8,242 titles with
    no delivery mechanism, against 20 of the 626 that had one.

    It is a guess, not a fact, which is why it sits behind a flag rather than in the
    default filter.

    :return: A clause for a ``LicensePool`` query outer-joined to the presentation
        ``Edition``. Null-safe: a pool with no presentation edition is not a magazine.
    """
    return func.coalesce(
        and_(
            Edition.series.is_not(None),
            func.btrim(Edition.series) == func.btrim(Edition.title),
        ),
        False,
        type_=Boolean,
    )


def _needs_backfill() -> ColumnElement[Boolean]:
    """Any of the three symptoms is reason enough to refresh a pool.

    ORed rather than queried separately so a pool showing more than one symptom is
    visited, and paid for, exactly once.
    """
    return or_(*_backfill_clauses())


def backfill_query(
    collections: int | Sequence[int],
    imported_since: datetime.datetime | None = None,
    *,
    clause: ColumnElement[Boolean] | None = None,
    include_suspected_magazines: bool = False,
) -> Select:
    """Build the query selecting a collection's damaged pools.

    Shared by :func:`backfill_collection` and :func:`count_backfill_candidates` so a
    dry run can never report a different set than the run it is estimating.

    :param collections: The Overdrive collection to search, or several of them.
    :param imported_since: Restrict to titles whose presentation edition was last
        written at or after this time -- that is, titles imported by the damaged
        code. A pool with no presentation edition has no such time and is therefore
        excluded when this is given.
    :param clause: Use this damage filter instead of all three symptoms ORed
        together. A dry run passes individual clauses here to report what each one
        selects on its own.
    :param include_suspected_magazines: Keep titles whose series name is their own
        title. They are left out by default, because we cannot make one openable. See
        :func:`looks_like_a_magazine` for why identifying them is a guess.
    :return: A ``select()`` over ``LicensePool``, unordered and unlimited.
    """
    query = (
        select(LicensePool)
        .outerjoin(Work, LicensePool.work_id == Work.id)
        .outerjoin(Edition, LicensePool.presentation_edition_id == Edition.id)
        .where(
            (
                LicensePool.collection_id == collections
                if isinstance(collections, int)
                else LicensePool.collection_id.in_(collections)
            ),
            _needs_backfill() if clause is None else clause,
        )
    )
    if imported_since is not None:
        query = query.where(Edition.updated_at >= imported_since)
    if not include_suspected_magazines:
        query = query.where(looks_like_a_magazine().is_(False))
    return query


def count_backfill_candidates(
    session: Session,
    collections: int | Sequence[int],
    imported_since: datetime.datetime | None = None,
    *,
    clause: ColumnElement[Boolean] | None = None,
    include_suspected_magazines: bool = False,
) -> int:
    """Count the damaged pools a backfill would select, without contacting Overdrive.

    Pools, not titles: a title held by more than one collection is counted once per
    collection. :func:`count_backfill_titles` is the one that answers how many
    Overdrive requests a run actually costs.

    :param session: An active database session.
    :param collections: The Overdrive collection to search, or several of them.
    :param imported_since: See :func:`backfill_query`.
    :param clause: See :func:`backfill_query`. A dry run passes one symptom at a time
        here to report what each selects on its own.
    :param include_suspected_magazines: See :func:`backfill_query`.
    :return: The number of damaged pools.
    """
    count = session.scalar(
        select(func.count()).select_from(
            backfill_query(
                collections,
                imported_since,
                clause=clause,
                include_suspected_magazines=include_suspected_magazines,
            ).subquery()
        )
    )
    return count or 0


def count_backfill_titles(
    session: Session,
    collections: int | Sequence[int],
    imported_since: datetime.datetime | None = None,
    *,
    include_suspected_magazines: bool = False,
) -> int:
    """Count the distinct titles a backfill would fetch, not the pools it would select.

    These differ whenever one title sits in more than one collection, which is normal:
    a parent and its Advantage children share titles, and so do two libraries' separate
    collections on the same manager. All three damage symptoms are keyed on the title
    rather than the collection -- delivery mechanisms belong to a book and a
    distributor, and pools sharing an identifier share a work and an edition -- so
    repairing a title through one collection stops every other collection from
    selecting it.

    Summing the per-collection counts therefore overstates what a run costs. This is
    the number of blocking Overdrive requests it will actually make.

    :param session: An active database session.
    :param collections: The Overdrive collection to search, or several of them.
    :param imported_since: See :func:`backfill_query`.
    :param include_suspected_magazines: See :func:`backfill_query`.
    :return: The number of distinct damaged titles.
    """
    damaged = backfill_query(
        collections,
        imported_since,
        include_suspected_magazines=include_suspected_magazines,
    ).subquery()
    count = session.scalar(select(func.count(func.distinct(damaged.c.identifier_id))))
    return count or 0


@shared_task(
    queue=QueueNames.default,
    bind=True,
    max_retries=4,
    autoretry_for=(BadResponseException, RequestTimedOut),
    throws=(RemoteIntegrationException,),
    retry_backoff=60,
)
def backfill_collection(
    task: Task,
    collection_id: int,
    *,
    offset: int = 0,
    batch_size: int = BACKFILL_BATCH_SIZE,
    max_titles: int | None = None,
    imported_since: datetime.datetime | None = None,
    include_suspected_magazines: bool = False,
) -> None:
    """Re-fetch Overdrive metadata for the titles the old importer damaged.

    Walks the collection's damaged pools in batches of ``batch_size``, re-queuing
    itself via ``task.replace()`` until none are left. Concurrency is governed by
    :func:`~palace.manager.celery.importer.workflow_lock_guard`.

    This is a repair pass, not an import: it is driven by what our own database looks
    like rather than by what Overdrive reports as changed, because the affected titles
    are wrong for reasons Overdrive knows nothing about and will never report as
    modified. See :func:`_needs_backfill` for what counts as damaged.

    .. warning::
       Each title costs one blocking Overdrive metadata request, the same shape as the
       reaper, which is known to monopolise the default queue. Pace runs accordingly.

    :param collection_id: The ID of the Overdrive collection to back-fill.
    :param offset: The last LicensePool.id processed; used to resume across batches.
    :param batch_size: Number of pools to process per batch.
    :param max_titles: Stop after this many titles. Counts down across batches, so it
        caps the whole run rather than each batch. Useful for measuring the real
        per-title cost on a sample before committing a whole collection.
    :param imported_since: Only repair titles whose presentation edition was last
        written at or after this time. The damage was done by a known release, so
        bounding by when *we* last wrote a record isolates what that release touched.
        See :func:`backfill_query` for how a pool with no presentation edition is
        treated.
    :param include_suspected_magazines: Repair titles whose series name is their own
        title too. They are skipped by default: we cannot make a magazine openable, so
        repairing one spends a blocking request and rewrites both of its hashes to
        leave it exactly as damaged as it was. See :func:`looks_like_a_magazine`.
    """
    with workflow_lock_guard(
        task,
        collection_id,
        label="Overdrive backfill",
        lock_factory=backfill_workflow_lock,
    ) as proceed:
        if not proceed:
            return

        # A session rather than a transaction, because each title is committed on its
        # own below.
        with task.session() as session:
            collection = load_from_id(session, Collection, collection_id)
            collection_name = collection.name

            # A capped run must not fetch more than it is allowed to process; each
            # pool returned costs one blocking Overdrive request.
            limit = batch_size if max_titles is None else min(batch_size, max_titles)
            pools = (
                session.execute(
                    backfill_query(
                        collection_id,
                        imported_since,
                        include_suspected_magazines=include_suspected_magazines,
                    )
                    .where(LicensePool.id > offset)
                    .order_by(LicensePool.id)
                    .limit(limit)
                )
                .unique()
                .scalars()
                .all()
            )

            if not pools:
                task.log.info(
                    f"Overdrive backfill complete for collection '{collection_name}'."
                )
                return

            api = OverdriveAPI(session, collection)
            new_offset = offset
            processed_count = 0
            for pool in pools:
                # Read the id before the commit below expires the instance.
                pool_id = pool.id
                # force=True: these records are wrong in ways the stored fingerprint
                # cannot see, so an unforced apply could decide there is nothing to do.
                api.ensure_bibliographic_coverage(pool, force=True)
                # Commit each title on its own, so a failure part way through a batch
                # keeps the repairs already paid for. A retry restarts at the same
                # offset, but the committed titles no longer match the damage query,
                # so they are skipped rather than fetched again.
                session.commit()
                new_offset = pool_id
                processed_count += 1

        task.log.info(
            f"Overdrive backfill: processed {processed_count} pools for collection "
            f"'{collection_name}' (offset: {offset} -> {new_offset})."
        )

        remaining = None if max_titles is None else max_titles - processed_count
        if remaining is not None and remaining <= 0:
            task.log.info(
                f"Overdrive backfill stopping for collection '{collection_name}': "
                f"reached the {max_titles}-title cap."
            )
            return

        # A short batch means the query is exhausted, so there is nothing left to do.
        if processed_count == limit:
            raise task.replace(
                signature_with(
                    task,
                    offset=new_offset,
                    max_titles=remaining,
                )
            )


@shared_task(queue=QueueNames.default, bind=True)
def backfill_all_collections(
    task: Task,
    *,
    max_titles: int | None = None,
    imported_since: datetime.datetime | None = None,
    include_suspected_magazines: bool = False,
) -> None:
    """Queue a backfill for every Overdrive collection.

    Includes both parent collections and Advantage (child) collections, since a
    damaged title can sit in either. Every collection runs one after another, never
    two at once; see the comment below for why.

    This is deliberately not on a schedule. A backfill is a one-off repair whose cost
    scales with the number of damaged titles, so it is kicked off by hand once the
    queues can absorb it.

    :param max_titles: Passed to each collection's task, so it caps **per collection**
        rather than across the whole fan-out.
    :param imported_since: Passed through unchanged; see :func:`backfill_collection`.
    :param include_suspected_magazines: Passed through unchanged; see
        :func:`backfill_collection`.
    """
    with task.session() as session:
        registry = task.services.integration_registry().license_providers()
        collection_query = Collection.select_by_protocol(
            OverdriveAPI, registry=registry
        )
        collections = session.scalars(collection_query).all()

        # Run the collections one after another rather than all at once. Any two of
        # them can hold the same title -- a parent and its Advantage children do, and
        # so do two libraries' separate collections on the same manager -- and a
        # shared title brings one Edition and one set of delivery mechanisms with it,
        # because those are keyed on data source and identifier rather than on
        # collection. Two collections running together therefore means two workers
        # paying for the same metadata request and writing the same rows. Each batch
        # holds its transaction open across fifty requests, so one worker ends up
        # blocked on the other's row locks, and two shared titles reached in opposite
        # id order can deadlock outright -- which raises an error the task does not
        # retry, stopping the rest of that run. Running in sequence also does less
        # work: once one collection has repaired the shared rows, the next one's
        # query no longer selects them.
        #
        # A parent still goes immediately before its own children, since that is
        # where the sharing is densest and so where skipping repaired titles saves
        # the most.
        children: dict[int, list[int]] = defaultdict(list)
        roots = []
        for collection in collections:
            if collection.parent_id is None:
                roots.append(collection.id)
            else:
                children[collection.parent_id].append(collection.id)

        ordered = [id for root in roots for id in (root, *children.pop(root, []))]
        # A child whose parent is not itself an Overdrive collection would other-
        # wise never be queued.
        ordered.extend(orphan for ids in children.values() for orphan in ids)

    if ordered:
        chain(
            *(
                backfill_collection.si(
                    collection_id,
                    max_titles=max_titles,
                    imported_since=imported_since,
                    include_suspected_magazines=include_suspected_magazines,
                )
                for collection_id in ordered
            )
        ).delay()

    task.log.info(
        f"Queued {len(ordered)} Overdrive collection(s) for backfill, "
        f"to run one after another."
    )

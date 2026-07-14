import datetime
import uuid
from contextlib import ExitStack
from tempfile import TemporaryFile
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError
from celery import shared_task
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from palace.util.datetime_helpers import utc_now

from palace.manager.celery.task import Task
from palace.manager.celery.utils import signature_with
from palace.manager.integration.catalog.marc.exporter import (
    MARC_EPOCH,
    LibraryInfo,
    MarcExporter,
)
from palace.manager.integration.catalog.marc.uploader import (
    MarcUploadManager,
    UploadContext,
)
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.lock import RedisLock
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.identifier import (
    Identifier,
    RecursiveEquivalencyCache,
)
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.marcfile import MarcFile
from palace.manager.sqlalchemy.util import create, get_one

# LibraryInfo fields that change between runs without a configuration change.
_VOLATILE_LIBRARY_INFO_FIELDS = frozenset({"last_updated", "needs_update"})

# LibraryInfo tuple fields compared order-independently.
# TODO: The `filtered_audiences` and `filtered_genres` library settings are
#  compared below, but changing them does not yet queue a MARC export reset.
_ORDER_INSENSITIVE_LIBRARY_INFO_FIELDS = frozenset(
    {"web_client_urls", "filtered_audiences", "filtered_genres"}
)


@shared_task(queue=QueueNames.default, bind=True)
def marc_export(task: Task, force: bool = False) -> None:
    """
    Export MARC records for all collections with the `export_marc_records` flag set to True, whose libraries
    have a MARC exporter integration enabled.
    """

    with task.session() as session:
        registry = task.services.integration_registry.catalog_services()
        start_time = utc_now()
        collections = MarcExporter.enabled_collections(session, registry)
        for collection in collections:
            libraries_info = MarcExporter.enabled_libraries(
                session, registry, collection.id
            )
            needs_update = any(info.needs_update for info in libraries_info) or force

            if not needs_update:
                task.log.info(
                    f"Skipping collection {collection.name} ({collection.id}) because it has been updated recently."
                )
                continue

            if not MarcExporter.query_works(
                session,
                collection.id,
                batch_size=1,
            ):
                task.log.info(
                    f"Skipping collection {collection.name} ({collection.id}) because it has no works."
                )
                continue

            task.log.info(
                f"Generating MARC records for collection {collection.name} ({collection.id})."
            )

            marc_export_collection.delay(
                collection_id=collection.id,
                collection_name=collection.name,
                start_time=start_time,
                libraries=libraries_info,
            )

            needs_delta = [l for l in libraries_info if l.last_updated]
            if needs_delta:
                min_last_updated = min(
                    [l.last_updated for l in libraries_info if l.last_updated]
                )
                if not MarcExporter.query_works(
                    session,
                    collection.id,
                    batch_size=1,
                    last_updated=min_last_updated,
                ):
                    task.log.info(
                        f"Skipping delta for collection {collection.name} ({collection.id}) "
                        f"because no works have been updated."
                    )
                else:
                    marc_export_collection.delay(
                        collection_id=collection.id,
                        collection_name=collection.name,
                        start_time=start_time,
                        libraries=needs_delta,
                        delta=True,
                    )


def marc_export_collection_lock(
    client: Redis, collection_id: int, delta: bool = False
) -> RedisLock:
    return RedisLock(
        client,
        ["MarcUpload", Collection.redis_key_from_id(collection_id), f"Delta::{delta}"],
        lock_timeout=datetime.timedelta(minutes=20),
    )


def _export_settings_changed(snapshot: LibraryInfo, current: LibraryInfo) -> bool:
    """Whether a library's export-affecting settings differ between two LibraryInfo.

    Ignores the fields that may change between runs without affecting the export
    and compares the tuple fields order-independently.
    """

    def comparable(info: LibraryInfo) -> dict[str, Any]:
        dumped = info.model_dump(exclude=set(_VOLATILE_LIBRARY_INFO_FIELDS))
        for field in _ORDER_INSENSITIVE_LIBRARY_INFO_FIELDS:
            dumped[field] = sorted(dumped[field])
        return dumped

    return comparable(snapshot) != comparable(current)


def _export_config_changed(
    session: Session,
    collection_id: int,
    snapshot: LibraryInfo,
    current: LibraryInfo | None,
) -> bool:
    """Whether a library's MARC configuration changed while an export was in flight.

    True when the library is no longer MARC-enabled (current is None), when its
    settings no longer match the ones from export generation time, or when a concurrent
    `marc_export_reset` deleted the library's MarcFile records. The settings check also
    covers a reset that found no records to delete during a first run.
    """
    if current is None:
        return True
    if _export_settings_changed(snapshot, current):
        return True
    records_gone: bool = (
        snapshot.last_updated is not None
        and session.execute(
            select(func.count(MarcFile.id)).where(
                MarcFile.library_id == snapshot.library_id,
                MarcFile.collection_id == collection_id,
            )
        ).scalar_one()
        == 0
    )
    return records_gone


def _finalize_uploads(
    task: Task,
    session: Session,
    uploads: dict[LibraryInfo, MarcUploadManager],
    collection_id: int,
    start_time: datetime.datetime,
    delta: bool,
) -> tuple[list[LibraryInfo], list[LibraryInfo]]:
    """Complete the uploads and create their MarcFile records.

    Uploads for libraries whose configuration changed while the export was in
    flight are discarded instead. Returns two lists: the current LibraryInfo
    for the discarded libraries, so the caller can re-queue a fresh full export
    for them, and the snapshot LibraryInfo for the libraries whose records were
    created, so the caller can re-verify them after the commit.
    """
    registry = task.services.integration_registry.catalog_services()
    current_infos = {
        info.library_id: info
        for info in MarcExporter.enabled_libraries(session, registry, collection_id)
    }
    drifted_libraries: list[LibraryInfo] = []
    recorded_libraries: list[LibraryInfo] = []
    for library, upload in uploads.items():
        # Recording an upload after the library's configuration changed would
        # leave stale content behind, so discard it and let the caller re-queue
        # a fresh full export. A change landing between this check and the
        # commit is caught by _verify_uploads_after_commit.
        current = current_infos.get(library.library_id)
        if _export_config_changed(session, collection_id, library, current):
            task.log.warning(
                f"MARC export configuration for library "
                f"{library.library_short_name} ({library.library_id}) "
                f"changed while this export was in flight. "
                f"Discarding '{upload.context.s3_key}'."
            )
            upload.abort()
            # A library that is no longer MARC-enabled is discarded without a re-run.
            if current is not None:
                drifted_libraries.append(current)
            continue

        if upload.complete():
            recorded_libraries.append(library)
            create(
                session,
                MarcFile,
                id=upload.context.upload_uuid,
                library_id=library.library_id,
                collection_id=collection_id,
                created=start_time,
                key=upload.context.s3_key,
                since=library.last_updated if delta else None,
            )
            # For first-run (or reset) libraries in a full export, create a
            # full-content delta record pointing at the same S3 object.
            # Delta-only consumers will receive the complete dataset without
            # a second upload.
            if not delta and library.last_updated is None:
                create(
                    session,
                    MarcFile,
                    library_id=library.library_id,
                    collection_id=collection_id,
                    created=start_time,
                    key=upload.context.s3_key,
                    since=MARC_EPOCH,
                )
            task.log.info(f"Completed upload for '{upload.context.s3_key}'")
        else:
            task.log.warning(
                f"No upload for '{upload.context.s3_key}', "
                f"because there were no records."
            )
    return drifted_libraries, recorded_libraries


def _verify_uploads_after_commit(
    task: Task,
    collection_id: int,
    recorded_libraries: list[LibraryInfo],
) -> None:
    """Detect a configuration change that landed between the finalize check and
    the export's commit, and then queue a reset for any library affected.

    Every reset trigger also changes the library's LibraryInfo, so comparing
    settings after the commit closes the remaining race window. That is, a change
    committed before this check is detected here, and a change committed after
    it queues a reset task that will see (and delete) the records this export
    just committed.
    """
    if not recorded_libraries:
        return

    with task.session() as session:
        registry = task.services.integration_registry.catalog_services()
        current_infos = {
            info.library_id: info
            for info in MarcExporter.enabled_libraries(session, registry, collection_id)
        }

    for library in recorded_libraries:
        current = current_infos.get(library.library_id)
        if current is None or _export_settings_changed(library, current):
            task.log.warning(
                f"MARC export configuration for library "
                f"{library.library_short_name} ({library.library_id}) changed "
                f"while the export was being recorded. Queueing a reset to "
                f"discard the stale export."
            )
            marc_export_reset.delay(library.library_id)


@shared_task(queue=QueueNames.default, bind=True)
def marc_export_collection(
    task: Task,
    collection_id: int,
    collection_name: str,
    start_time: datetime.datetime,
    libraries: list[LibraryInfo],
    context: list[tuple[int, UploadContext]] | None = None,
    last_work_id: int | None = None,
    batch_size: int = 1000,
    delta: bool = False,
) -> None:
    """
    Export MARC records for a single collection.

    This task is designed to be re-queued until all works in the collection have been processed,
    this can take some time, however each individual task should complete quickly, so that it
    doesn't block other tasks from running.
    """

    base_url = task.services.config.sitewide.base_url()
    storage_service = task.services.storage.public()
    if context is None:
        context = []
    context_dict = dict(context)

    with marc_export_collection_lock(
        task.services.redis.client(), collection_id, delta
    ).lock():
        with ExitStack() as stack, task.transaction() as session:
            files = {
                library: stack.enter_context(TemporaryFile()) for library in libraries
            }
            uploads: dict[LibraryInfo, MarcUploadManager] = {
                library: stack.enter_context(
                    MarcUploadManager(
                        storage_service,
                        collection_name,
                        library.library_short_name,
                        start_time,
                        library.last_updated if delta else None,
                        context_dict.get(library.library_id),
                    )
                )
                for library in libraries
            }

            min_last_updated = (
                min([l.last_updated for l in libraries if l.last_updated])
                if delta
                else None
            )

            no_more_works = False
            while not all(
                [
                    file.tell() > storage_service.MINIMUM_MULTIPART_UPLOAD_SIZE
                    for file in files.values()
                ]
            ):
                works = MarcExporter.query_works(
                    session,
                    collection_id,
                    batch_size=batch_size,
                    work_id_offset=last_work_id,
                    last_updated=min_last_updated,
                )
                if not works:
                    no_more_works = True
                    break

                # Set this for the next iteration
                last_work_id = works[-1].id

                works_with_pools = [
                    (work, pool)
                    for work in works
                    if (pool := work.active_license_pool()) is not None
                ]

                # Find ISBN for any work that needs it
                isbns = RecursiveEquivalencyCache.equivalent_identifiers(
                    session,
                    {pool.identifier for work, pool in works_with_pools},
                    Identifier.ISBN,
                )

                for work, pool in works_with_pools:
                    isbn_identifier = isbns.get(pool.identifier)
                    records = MarcExporter.process_work(
                        work, pool, isbn_identifier, libraries, base_url, delta
                    )
                    for library, record in records.items():
                        files[library].write(record)

            # Upload part to s3, if there is anything to upload
            for library, tmp_file in files.items():
                upload = uploads[library]
                if not upload.upload_part(tmp_file):
                    task.log.warning(
                        f"No data to upload to s3 '{upload.context.s3_key}'."
                    )

            if no_more_works:
                # Task is complete. Finalize the s3 uploads and create MarcFile records in DB.
                drifted_libraries, recorded_libraries = _finalize_uploads(
                    task, session, uploads, collection_id, start_time, delta
                )
                task.log.info(
                    f"Finished generating MARC records for collection '{collection_name}' ({collection_id}) "
                    f"in {(utc_now() - start_time).seconds} seconds."
                )

        if no_more_works:
            # The transaction has committed. Re-check the recorded libraries so
            # that a configuration change landing between the finalize check and
            # the commit cannot leave a stale export standing.
            _verify_uploads_after_commit(task, collection_id, recorded_libraries)

    if no_more_works:
        if drifted_libraries:
            # Re-run a full export for the libraries whose configuration changed
            # while this export was in flight, so they get fresh content with
            # their new settings right away rather than at the next scheduled run.
            raise task.replace(
                marc_export_collection.s(
                    collection_id=collection_id,
                    collection_name=collection_name,
                    start_time=utc_now(),
                    libraries=drifted_libraries,
                    batch_size=batch_size,
                )
            )
        return

    # This task is complete, but there are more works waiting to be exported. So we requeue ourselves
    # to process the next batch.
    raise task.replace(
        signature_with(
            task,
            # We pass context as a list of tuples instead of a dict, because the json serializer
            # converts all dict keys to strings, so we lose the integer keys. We convert it back
            # to a dict on the other side.
            # See the note here: https://docs.celeryq.dev/en/stable/userguide/calling.html#serializers
            context=[
                (library.library_id, upload.context)
                for library, upload in uploads.items()
            ],
            # last_work_id advances each batch, so carry the updated value forward.
            last_work_id=last_work_id,
        )
    )


def _marc_file_key_ref_count(
    session: Session, key: str, exclude_id: uuid.UUID | None = None
) -> int:
    """Count the MarcFile records referencing the given S3 key.

    A first-run full export and its paired full-content delta share an S3 key,
    so the object must only be deleted when the last record referencing it goes.
    """
    stmt = select(func.count(MarcFile.id)).where(MarcFile.key == key)
    if exclude_id is not None:
        stmt = stmt.where(MarcFile.id != exclude_id)
    count: int = session.execute(stmt).scalar_one()
    return count


@shared_task(queue=QueueNames.default, bind=True)
def marc_export_cleanup(
    task: Task,
    batch_size: int = 20,
) -> None:
    """
    Cleanup old MARC exports that are outdated or no longer needed.

    The S3 object is deleted before the MarcFile record, so a failure leaves the
    record in place to be retried by the next scheduled cleanup run.
    """
    storage_service = task.services.storage.public()
    registry = task.services.integration_registry.catalog_services()
    with task.session() as session:
        for count, file_record in enumerate(
            MarcExporter.files_for_cleanup(session, registry)
        ):
            if count >= batch_size:
                # Requeue ourselves after deleting `batch_size` files to avoid blocking the worker for too long.
                raise task.replace(marc_export_cleanup.s())

            task.log.info(f"Deleting MARC export {file_record.key} ({file_record.id}).")
            if (
                _marc_file_key_ref_count(
                    session, file_record.key, exclude_id=file_record.id
                )
                == 0
            ):
                storage_service.delete(file_record.key)
            session.delete(file_record)
            session.commit()


@shared_task(queue=QueueNames.default, bind=True)
def marc_export_reset(task: Task, library_id: int) -> None:
    """
    Reset MARC export for a library so that the next export run behaves like a
    first run, producing both a new full export and a full-content delta.
    Deletes the MarcFile records and associated S3 objects for the library's
    MARC-enabled collections.

    The records are deleted in a single transaction, so a failure cannot leave the
    reset half-applied. The S3 objects are then deleted best-effort: a failed
    deletion orphans that object and logs an error, but allows the reset to continue.

    Repeated configuration changes can queue duplicate tasks. We don't guard
    against that since the task is idempotent, even under concurrent duplicates:
    the bulk delete matches no rows the other task already removed, and the S3
    deletions tolerate missing objects.
    """
    storage_service = task.services.storage.public()
    with task.session() as session:
        library = get_one(session, Library, id=library_id)
        if library is None:
            task.log.warning(
                f"Library {library_id} not found. Skipping MARC export reset."
            )
            return

        keys = MarcExporter.delete_files_for_reset(session, library)
        session.commit()
        task.log.info(
            f"Reset MARC export for library {library_id}. "
            f"Deleted records for {len(keys)} file(s)."
        )

        # Skip keys that other MarcFile records still reference.
        still_referenced = set(
            session.scalars(
                select(MarcFile.key).where(MarcFile.key.in_(keys)).distinct()
            )
        )
        for key in keys - still_referenced:
            try:
                storage_service.delete(key)
            except (BotoCoreError, ClientError):
                task.log.exception(f"Failed to delete orphaned MARC export '{key}'.")

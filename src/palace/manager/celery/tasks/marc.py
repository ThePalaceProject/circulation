import datetime
from contextlib import ExitStack
from tempfile import TemporaryFile

from botocore.exceptions import BotoCoreError, ClientError
from celery import shared_task
from sqlalchemy import func, select

from palace.util.datetime_helpers import utc_now

from palace.manager.celery.task import Task
from palace.manager.celery.utils import signature_with
from palace.manager.integration.catalog.marc.exporter import LibraryInfo, MarcExporter
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

# Sentinel timestamp used as `since` on a full-content delta MarcFile record.
# When a library's settings reset causes a fresh full export, a paired delta
# record with this timestamp is created so delta-only consumers also receive
# the complete dataset.
MARC_EPOCH = datetime.datetime(1900, 1, 1, tzinfo=datetime.UTC)


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
                for library, upload in uploads.items():
                    if upload.complete():
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

                task.log.info(
                    f"Finished generating MARC records for collection '{collection_name}' ({collection_id}) "
                    f"in {(utc_now() - start_time).seconds} seconds."
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
            # Only delete the S3 object when no other MarcFile records share the key.
            # A first-run full export and its paired full-content delta share an S3 key.
            other_refs = session.execute(
                select(func.count(MarcFile.id)).where(
                    MarcFile.key == file_record.key,
                    MarcFile.id != file_record.id,
                )
            ).scalar_one()
            if other_refs == 0:
                storage_service.delete(file_record.key)
            session.delete(file_record)
            session.commit()


@shared_task(queue=QueueNames.default, bind=True)
def marc_export_reset(task: Task, library_id: int) -> None:
    """
    Reset MARC export for a library, so that the next export run behaves like a
    first run, producing both a new full export and a full-content delta.

    This is triggered when a library's MARC export configuration changes, to make
    sure that consumers relying only on deltas receive a complete refresh with the
    updated settings. It deletes the MarcFile records for the library's
    MARC-enabled collections, along with the corresponding S3 objects.

    The records are deleted in a single transaction, so a failure cannot leave the
    reset half-applied. The S3 objects are then deleted best-effort: a failed
    deletion orphans that object (with an error logged), but never compromises
    the reset itself.

    Repeated configuration changes can queue duplicate tasks. We don't guard
    against that: the task is idempotent (a duplicate finds no records and is a
    no-op), so a lock would only add a failure mode without preventing any harm.
    """
    storage_service = task.services.storage.public()
    with task.session() as session:
        library = get_one(session, Library, id=library_id)
        if library is None:
            task.log.warning(
                f"Library {library_id} not found. Skipping MARC export reset."
            )
            return

        keys: set[str] = set()
        for file_record in MarcExporter.files_for_reset(session, library):
            task.log.info(f"Deleting MARC export {file_record.key} ({file_record.id}).")
            keys.add(file_record.key)
            session.delete(file_record)
        session.commit()

        for key in keys:
            # Skip keys that other MarcFile records still reference.
            other_refs = session.execute(
                select(func.count(MarcFile.id)).where(MarcFile.key == key)
            ).scalar_one()
            if other_refs > 0:
                continue
            try:
                storage_service.delete(key)
            except (BotoCoreError, ClientError):
                task.log.exception(f"Failed to delete orphaned MARC export '{key}'.")

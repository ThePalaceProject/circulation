import datetime
from contextlib import ExitStack
from tempfile import TemporaryFile
from typing import Any

from celery import shared_task
from pydantic import TypeAdapter

from palace.manager.celery.task import Task
from palace.manager.marc.exporter import LibraryInfo, MarcExporter
from palace.manager.marc.uploader import MarcUploadManager, UploadContext
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.lock import RedisLock
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.marcfile import MarcFile
from palace.manager.sqlalchemy.util import create
from palace.manager.util.datetime_helpers import utc_now


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
                libraries=[l.model_dump() for l in libraries_info],
            )

            needs_delta = [l.model_dump() for l in libraries_info if l.last_updated]
            if needs_delta:
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
    libraries: list[dict[str, Any]],
    context: dict[int, dict[str, Any]] | None = None,
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

    # Parse data into pydantic models
    libraries_info = TypeAdapter(list[LibraryInfo]).validate_python(libraries)
    context_parsed = TypeAdapter(dict[int, UploadContext]).validate_python(
        context or {}
    )

    lock = marc_export_collection_lock(
        task.services.redis.client(), collection_id, delta
    )

    with lock.lock() as locked:
        if not locked:
            task.log.info(
                f"Skipping collection {collection_id} because another task is already processing it."
            )
            return

        with ExitStack() as stack, task.transaction() as session:
            files = {
                library: stack.enter_context(TemporaryFile())
                for library in libraries_info
            }
            uploads: dict[LibraryInfo, MarcUploadManager] = {
                library: stack.enter_context(
                    MarcUploadManager(
                        storage_service,
                        collection_name,
                        library.library_short_name,
                        start_time,
                        library.last_updated if delta else None,
                        context_parsed.get(library.library_id),
                    )
                )
                for library in libraries_info
            }

            min_last_updated = (
                min([l.last_updated for l in libraries_info if l.last_updated])
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
                isbns = MarcExporter.query_isbn_identifiers(
                    session,
                    {pool.identifier for work, pool in works_with_pools},
                )

                for work, pool in works_with_pools:
                    isbn_identifier = isbns.get(pool.identifier)
                    records = MarcExporter.process_work(
                        work, pool, isbn_identifier, libraries_info, base_url, delta
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
        marc_export_collection.s(
            collection_id=collection_id,
            collection_name=collection_name,
            start_time=start_time,
            libraries=[l.model_dump() for l in libraries_info],
            context={
                l.library_id: uploads[l].context.model_dump() for l in libraries_info
            },
            last_work_id=last_work_id,
            batch_size=batch_size,
            delta=delta,
        )
    )


@shared_task(queue=QueueNames.default, bind=True)
def marc_export_cleanup(
    task: Task,
    batch_size: int = 20,
) -> None:
    """
    Cleanup old MARC exports that are outdated or no longer needed.
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
            storage_service.delete(file_record.key)
            session.delete(file_record)
            session.commit()

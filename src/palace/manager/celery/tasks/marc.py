import datetime
from typing import Any

from celery import shared_task

from palace.manager.celery.task import Task
from palace.manager.marc.exporter import LibraryInfo, MarcExporter
from palace.manager.marc.uploader import MarcUploadManager
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.marc import MarcFileUploadSession
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
            # Collection.id should never be able to be None here, but mypy doesn't know that.
            # So we assert it for mypy's benefit.
            assert collection.id is not None
            lock = MarcFileUploadSession(task.services.redis.client(), collection.id)
            with lock.lock() as acquired:
                if not acquired:
                    task.log.info(
                        f"Skipping collection {collection.name} ({collection.id}) because another task holds its lock."
                    )
                    continue

                libraries_info = MarcExporter.enabled_libraries(
                    session, registry, collection.id
                )
                needs_update = (
                    any(info.needs_update for info in libraries_info) or force
                )

                if not needs_update:
                    task.log.info(
                        f"Skipping collection {collection.name} ({collection.id}) because it has been updated recently."
                    )
                    continue

                works = MarcExporter.query_works(
                    session,
                    collection.id,
                    work_id_offset=0,
                    batch_size=1,
                )
                if not works:
                    task.log.info(
                        f"Skipping collection {collection.name} ({collection.id}) because it has no works."
                    )
                    continue

                task.log.info(
                    f"Generating MARC records for collection {collection.name} ({collection.id})."
                )
                marc_export_collection.delay(
                    collection_id=collection.id,
                    start_time=start_time,
                    libraries=[l.dict() for l in libraries_info],
                )


@shared_task(queue=QueueNames.default, bind=True)
def marc_export_collection(
    task: Task,
    collection_id: int,
    start_time: datetime.datetime,
    libraries: list[dict[str, Any]],
    batch_size: int = 500,
    last_work_id: int | None = None,
    update_number: int = 0,
) -> None:
    """
    Export MARC records for a single collection.

    This task is designed to be re-queued until all works in the collection have been processed,
    this can take some time, however each individual task should complete quickly, so that it
    doesn't block other tasks from running.
    """

    base_url = task.services.config.sitewide.base_url()
    storage_service = task.services.storage.public()
    libraries_info = [LibraryInfo.parse_obj(l) for l in libraries]
    upload_manager = MarcUploadManager(
        storage_service,
        MarcFileUploadSession(
            task.services.redis.client(), collection_id, update_number
        ),
    )
    with upload_manager.begin():
        if not upload_manager.locked:
            task.log.info(
                f"Skipping collection {collection_id} because another task is already processing it."
            )
            return

        with task.session() as session:
            works = MarcExporter.query_works(
                session,
                collection_id,
                work_id_offset=last_work_id,
                batch_size=batch_size,
            )
            for work in works:
                MarcExporter.process_work(
                    work, libraries_info, base_url, upload_manager=upload_manager
                )

        # Sync the upload_manager to ensure that all the data is written to storage.
        upload_manager.sync()

        if len(works) == batch_size:
            # This task is complete, but there are more works waiting to be exported. So we requeue ourselves
            # to process the next batch.
            raise task.replace(
                marc_export_collection.s(
                    collection_id=collection_id,
                    start_time=start_time,
                    libraries=[l.dict() for l in libraries_info],
                    batch_size=batch_size,
                    last_work_id=works[-1].id,
                    update_number=upload_manager.update_number,
                )
            )

        # If we got here, we have finished generating MARC records. Cleanup and exit.
        with task.transaction() as session:
            collection = MarcExporter.collection(session, collection_id)
            collection_name = collection.name if collection else "unknown"
            completed_uploads = upload_manager.complete()
            MarcExporter.create_marc_upload_records(
                session, start_time, collection_id, libraries_info, completed_uploads
            )
            upload_manager.remove_session()
        task.log.info(
            f"Finished generating MARC records for collection '{collection_name}' ({collection_id})."
        )

from __future__ import annotations

import datetime
from collections.abc import Generator, Iterable, Sequence
from uuid import UUID, uuid4

import pytz
from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.orm import Session, aliased

from palace.manager.integration.base import HasLibraryIntegrationConfiguration
from palace.manager.integration.goals import Goals
from palace.manager.marc.annotator import Annotator
from palace.manager.marc.settings import (
    MarcExporterLibrarySettings,
    MarcExporterSettings,
)
from palace.manager.marc.uploader import MarcUploadManager
from palace.manager.service.integration_registry.catalog_services import (
    CatalogServicesRegistry,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.discovery_service_registration import (
    DiscoveryServiceRegistration,
)
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.marcfile import MarcFile
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import create
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.log import LoggerMixin
from palace.manager.util.uuid import uuid_encode


class LibraryInfo(BaseModel):
    library_id: int
    library_short_name: str
    last_updated: datetime.datetime | None = None
    needs_update: bool
    organization_code: str | None = None
    include_summary: bool
    include_genres: bool
    web_client_urls: tuple[str, ...]

    s3_key_full_uuid: str
    s3_key_full: str

    s3_key_delta_uuid: str
    s3_key_delta: str | None = None
    model_config = ConfigDict(frozen=True)


class MarcExporter(
    HasLibraryIntegrationConfiguration[
        MarcExporterSettings, MarcExporterLibrarySettings
    ],
    LoggerMixin,
):
    """
    This class provides the logic for exporting MARC records for a collection to S3.
    """

    @classmethod
    def label(cls) -> str:
        return "MARC Export"

    @classmethod
    def description(cls) -> str:
        return (
            "Export metadata into MARC files that can be imported into an ILS manually."
        )

    @classmethod
    def settings_class(cls) -> type[MarcExporterSettings]:
        return MarcExporterSettings

    @classmethod
    def library_settings_class(cls) -> type[MarcExporterLibrarySettings]:
        return MarcExporterLibrarySettings

    @staticmethod
    def _s3_key(
        library: Library,
        collection: Collection,
        creation_time: datetime.datetime,
        uuid: UUID,
        since_time: datetime.datetime | None = None,
    ) -> str:
        """The path to the hosted MARC file for the given library, collection,
        and date range."""

        def date_to_string(date: datetime.datetime) -> str:
            return date.astimezone(pytz.UTC).strftime("%Y-%m-%d")

        root = "marc"
        short_name = str(library.short_name)
        creation = date_to_string(creation_time)

        if since_time:
            file_type = f"delta.{date_to_string(since_time)}.{creation}"
        else:
            file_type = f"full.{creation}"

        uuid_encoded = uuid_encode(uuid)
        collection_name = collection.name.replace(" ", "_")
        filename = f"{collection_name}.{file_type}.{uuid_encoded}.mrc"
        parts = [root, short_name, filename]
        return "/".join(parts)

    @staticmethod
    def _needs_update(
        last_updated_time: datetime.datetime | None, update_frequency: int
    ) -> bool:
        return not last_updated_time or (
            last_updated_time.date()
            <= (utc_now() - datetime.timedelta(days=update_frequency)).date()
        )

    @staticmethod
    def _web_client_urls(
        session: Session, library: Library, url: str | None = None
    ) -> tuple[str, ...]:
        """Find web client URLs configured by the registry for this library."""
        urls = [
            s.web_client
            for s in session.execute(
                select(DiscoveryServiceRegistration.web_client).where(
                    DiscoveryServiceRegistration.library == library,
                    DiscoveryServiceRegistration.web_client != None,
                )
            ).all()
        ]

        if url:
            urls.append(url)

        return tuple(urls)

    @classmethod
    def _enabled_collections_and_libraries(
        cls,
        session: Session,
        registry: CatalogServicesRegistry,
        collection_id: int | None = None,
    ) -> set[tuple[Collection, IntegrationLibraryConfiguration]]:
        collection_integration_configuration = aliased(IntegrationConfiguration)
        collection_integration_library_configuration = aliased(
            IntegrationLibraryConfiguration
        )
        library_integration_library_configuration = aliased(
            IntegrationLibraryConfiguration,
            name="library_integration_library_configuration",
        )
        library_integration_configuration = aliased(IntegrationConfiguration)

        protocols = registry.get_protocols(cls)

        collection_query = (
            select(Collection, library_integration_library_configuration)
            .select_from(Collection)
            .join(collection_integration_configuration)
            .join(collection_integration_library_configuration)
            .join(Library)
            .join(library_integration_library_configuration)
            .join(library_integration_configuration)
            .where(
                Collection.export_marc_records == True,
                library_integration_configuration.goal == Goals.CATALOG_GOAL,
                library_integration_configuration.protocol.in_(protocols),
            )
        )
        if collection_id is not None:
            collection_query = collection_query.where(Collection.id == collection_id)
        return {
            (r.Collection, r.library_integration_library_configuration)
            for r in session.execute(collection_query)
        }

    @staticmethod
    def _last_updated(
        session: Session, library: Library, collection: Collection
    ) -> datetime.datetime | None:
        """Find the most recent MarcFile creation time."""
        last_updated_file = session.execute(
            select(MarcFile.created)
            .where(
                MarcFile.library == library,
                MarcFile.collection == collection,
            )
            .order_by(MarcFile.created.desc())
        ).first()

        return last_updated_file.created if last_updated_file else None

    @classmethod
    def enabled_collections(
        cls, session: Session, registry: CatalogServicesRegistry
    ) -> set[Collection]:
        return {c for c, _ in cls._enabled_collections_and_libraries(session, registry)}

    @classmethod
    def enabled_libraries(
        cls, session: Session, registry: CatalogServicesRegistry, collection_id: int
    ) -> Sequence[LibraryInfo]:
        library_info = []
        creation_time = utc_now()
        for collection, library_integration in cls._enabled_collections_and_libraries(
            session, registry, collection_id
        ):
            library = library_integration.library
            library_id = library.id
            library_short_name = library.short_name
            if library_id is None or library_short_name is None:
                cls.logger().warning(
                    f"Library {library} is missing an ID or short name."
                )
                continue
            last_updated_time = cls._last_updated(session, library, collection)
            update_frequency = cls.settings_load(
                library_integration.parent
            ).update_frequency
            library_settings = cls.library_settings_load(library_integration)
            needs_update = cls._needs_update(last_updated_time, update_frequency)
            web_client_urls = cls._web_client_urls(
                session, library, library_settings.web_client_url
            )
            s3_key_full_uuid = uuid4()
            s3_key_full = cls._s3_key(
                library,
                collection,
                creation_time,
                s3_key_full_uuid,
            )
            s3_key_delta_uuid = uuid4()
            s3_key_delta = (
                cls._s3_key(
                    library,
                    collection,
                    creation_time,
                    s3_key_delta_uuid,
                    since_time=last_updated_time,
                )
                if last_updated_time
                else None
            )
            library_info.append(
                LibraryInfo(
                    library_id=library_id,
                    library_short_name=library_short_name,
                    last_updated=last_updated_time,
                    needs_update=needs_update,
                    organization_code=library_settings.organization_code,
                    include_summary=library_settings.include_summary,
                    include_genres=library_settings.include_genres,
                    web_client_urls=web_client_urls,
                    s3_key_full_uuid=str(s3_key_full_uuid),
                    s3_key_full=s3_key_full,
                    s3_key_delta_uuid=str(s3_key_delta_uuid),
                    s3_key_delta=s3_key_delta,
                )
            )
        library_info.sort(key=lambda info: info.library_id)
        return library_info

    @staticmethod
    def query_works(
        session: Session,
        collection_id: int,
        work_id_offset: int | None,
        batch_size: int,
    ) -> list[Work]:
        query = (
            select(Work)
            .join(LicensePool)
            .where(
                LicensePool.collection_id == collection_id,
            )
            .limit(batch_size)
            .order_by(Work.id.asc())
        )

        if work_id_offset is not None:
            query = query.where(Work.id > work_id_offset)

        return session.execute(query).scalars().unique().all()

    @staticmethod
    def collection(session: Session, collection_id: int) -> Collection | None:
        return session.execute(
            select(Collection).where(Collection.id == collection_id)
        ).scalar_one_or_none()

    @classmethod
    def process_work(
        cls,
        work: Work,
        libraries_info: Iterable[LibraryInfo],
        base_url: str,
        *,
        upload_manager: MarcUploadManager,
        annotator: type[Annotator] = Annotator,
    ) -> None:
        pool = work.active_license_pool()
        if pool is None:
            return
        base_record = annotator.marc_record(work, pool)

        for library_info in libraries_info:
            library_record = annotator.library_marc_record(
                base_record,
                pool.identifier,
                base_url,
                library_info.library_short_name,
                library_info.web_client_urls,
                library_info.organization_code,
                library_info.include_summary,
                library_info.include_genres,
            )

            upload_manager.add_record(
                library_info.s3_key_full,
                library_record.as_marc(),
            )

            if (
                library_info.last_updated
                and library_info.s3_key_delta
                and work.last_update_time
                and work.last_update_time > library_info.last_updated
            ):
                upload_manager.add_record(
                    library_info.s3_key_delta,
                    annotator.set_revised(library_record).as_marc(),
                )

    @staticmethod
    def create_marc_upload_records(
        session: Session,
        start_time: datetime.datetime,
        collection_id: int,
        libraries_info: Iterable[LibraryInfo],
        uploaded_keys: set[str],
    ) -> None:
        for library_info in libraries_info:
            if library_info.s3_key_full in uploaded_keys:
                create(
                    session,
                    MarcFile,
                    id=library_info.s3_key_full_uuid,
                    library_id=library_info.library_id,
                    collection_id=collection_id,
                    created=start_time,
                    key=library_info.s3_key_full,
                )
            if library_info.s3_key_delta and library_info.s3_key_delta in uploaded_keys:
                create(
                    session,
                    MarcFile,
                    id=library_info.s3_key_delta_uuid,
                    library_id=library_info.library_id,
                    collection_id=collection_id,
                    created=start_time,
                    since=library_info.last_updated,
                    key=library_info.s3_key_delta,
                )

    @staticmethod
    def files_for_cleanup(
        session: Session, registry: CatalogServicesRegistry
    ) -> Generator[MarcFile, None, None]:
        # Files for collections or libraries that have had exports disabled.
        existing = {
            (row.collection_id, row.library_id)
            for row in session.execute(
                select(MarcFile.collection_id, MarcFile.library_id).distinct()
            ).all()
        }
        enabled = {
            (collection.id, integration.library_id)
            for collection, integration in MarcExporter._enabled_collections_and_libraries(
                session, registry
            )
        }

        for collection_id, library_id in existing - enabled:
            yield from session.execute(
                select(MarcFile).where(
                    MarcFile.library_id == library_id,
                    MarcFile.collection_id == collection_id,
                )
            ).scalars()

        # Outdated exports
        for collection_id, library_id in existing:
            # Only keep the most recent full export for each library/collection pair.
            yield from session.execute(
                select(MarcFile)
                .where(
                    MarcFile.library_id == library_id,
                    MarcFile.collection_id == collection_id,
                    MarcFile.since == None,
                )
                .order_by(MarcFile.created.desc())
                .offset(1)
            ).scalars()

            # Keep the most recent 12 delta exports for each library/collection pair.
            yield from session.execute(
                select(MarcFile)
                .where(
                    MarcFile.library_id == library_id,
                    MarcFile.collection_id == collection_id,
                    MarcFile.since != None,
                )
                .order_by(MarcFile.created.desc())
                .offset(12)
            ).scalars()

from __future__ import annotations

import datetime
from collections.abc import Generator, Iterable, Sequence

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select
from sqlalchemy.orm import Session, aliased, raiseload, selectinload

from palace.manager.integration.base import HasLibraryIntegrationConfiguration
from palace.manager.integration.catalog.marc.annotator import Annotator
from palace.manager.integration.catalog.marc.settings import (
    MarcExporterLibrarySettings,
    MarcExporterSettings,
)
from palace.manager.service.integration_registry.catalog_services import (
    CatalogServicesRegistry,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.contributor import Contribution
from palace.manager.sqlalchemy.model.discovery_service_registration import (
    DiscoveryServiceRegistration,
)
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.marcfile import MarcFile
from palace.manager.sqlalchemy.model.work import Work, WorkGenre
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.log import LoggerMixin


class LibraryInfo(BaseModel):
    library_id: int
    library_short_name: str
    last_updated: datetime.datetime | None = None
    needs_update: bool
    organization_code: str | None = None
    include_summary: bool
    include_genres: bool
    web_client_urls: tuple[str, ...]
    filtered_audiences: tuple[str, ...] = ()
    filtered_genres: tuple[str, ...] = ()

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

        library_integration_configuration_query = registry.configurations_query(cls)

        collection_query = (
            select(Collection, library_integration_library_configuration)
            .select_from(Collection)
            .join(collection_integration_configuration)
            .join(collection_integration_library_configuration)
            .join(Library)
            .join(library_integration_library_configuration)
            .join(
                library_integration_configuration_query.subquery(
                    name="library_integration_configuration"
                )
            )
            .where(
                Collection.export_marc_records == True,
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
        cls,
        session: Session,
        registry: CatalogServicesRegistry,
        collection_id: int | None,
    ) -> Sequence[LibraryInfo]:
        if collection_id is None:
            return []

        library_info = []
        for collection, library_integration in cls._enabled_collections_and_libraries(
            session, registry, collection_id
        ):
            library = library_integration.library
            library_id = library.id
            library_short_name = library.short_name
            last_updated_time = cls._last_updated(session, library, collection)
            update_frequency = cls.settings_load(
                library_integration.parent
            ).update_frequency
            marc_settings = cls.library_settings_load(library_integration)
            needs_update = cls._needs_update(last_updated_time, update_frequency)
            web_client_urls = cls._web_client_urls(
                session, library, marc_settings.web_client_url
            )
            # Get library content filtering settings
            library_settings = library.settings
            library_info.append(
                LibraryInfo(
                    library_id=library_id,
                    library_short_name=library_short_name,
                    last_updated=last_updated_time,
                    needs_update=needs_update,
                    organization_code=marc_settings.organization_code,
                    include_summary=marc_settings.include_summary,
                    include_genres=marc_settings.include_genres,
                    web_client_urls=web_client_urls,
                    filtered_audiences=tuple(library_settings.filtered_audiences),
                    filtered_genres=tuple(library_settings.filtered_genres),
                )
            )
        library_info.sort(key=lambda info: info.library_id)
        return library_info

    @staticmethod
    def query_works(
        session: Session,
        collection_id: int | None,
        batch_size: int,
        work_id_offset: int | None = None,
        last_updated: datetime.datetime | None = None,
    ) -> list[Work]:
        if collection_id is None:
            return []

        query = (
            select(Work)
            .join(LicensePool)
            .where(
                LicensePool.collection_id == collection_id,
            )
            .limit(batch_size)
            .order_by(Work.id.asc())
            .options(
                # We set loader options on all the collection properties
                # needed to generate the MARC records, so that we don't end
                # up doing queries for each work.
                selectinload(Work.license_pools).options(
                    selectinload(LicensePool.identifier),
                    selectinload(LicensePool.presentation_edition).options(
                        selectinload(Edition.contributions).options(
                            selectinload(Contribution.contributor)
                        )
                    ),
                    selectinload(LicensePool.available_delivery_mechanisms).options(
                        selectinload(LicensePoolDeliveryMechanism.delivery_mechanism)
                    ),
                    selectinload(LicensePool.data_source),
                ),
                selectinload(Work.work_genres).options(selectinload(WorkGenre.genre)),
                # We set raiseload on all the other properties, so we quickly know if
                # a change causes us to start having to issue queries to get a property.
                # This will raise a InvalidRequestError, that should fail our tests, so
                # we know to add the new required properties to this function.
                raiseload("*"),
            )
        )

        if last_updated:
            query = query.where(Work.last_update_time > last_updated)

        if work_id_offset:
            query = query.where(Work.id > work_id_offset)

        return session.execute(query).scalars().all()

    @staticmethod
    def collection(session: Session, collection_id: int) -> Collection | None:
        return session.execute(
            select(Collection).where(Collection.id == collection_id)
        ).scalar_one_or_none()

    @staticmethod
    def process_work(
        work: Work,
        license_pool: LicensePool,
        isbn_identifier: Identifier | None,
        libraries_info: Iterable[LibraryInfo],
        base_url: str,
        delta: bool,
        *,
        annotator: type[Annotator] = Annotator,
    ) -> dict[LibraryInfo, bytes]:
        base_record = annotator.marc_record(work, isbn_identifier, license_pool)
        return {
            library_info: annotator.library_marc_record(
                base_record,
                license_pool.identifier,
                base_url,
                library_info.library_short_name,
                library_info.web_client_urls,
                library_info.organization_code,
                library_info.include_summary,
                library_info.include_genres,
                delta,
            ).as_marc()
            for library_info in libraries_info
            if not work.is_filtered_by(
                library_info.filtered_audiences, library_info.filtered_genres
            )
            and (
                not delta
                or (
                    work.last_update_time
                    and library_info.last_updated
                    and work.last_update_time > library_info.last_updated
                )
            )
        }

    @staticmethod
    def files_for_cleanup(
        session: Session, registry: CatalogServicesRegistry
    ) -> Generator[MarcFile]:
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

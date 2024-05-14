from __future__ import annotations

import argparse
import datetime
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.core.marc import Annotator as MarcAnnotator
from palace.manager.core.marc import (
    MARCExporter,
    MarcExporterLibrarySettings,
    MarcExporterSettings,
)
from palace.manager.integration.goals import Goals
from palace.manager.scripts.input import LibraryInputScript
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.discovery_service_registration import (
    DiscoveryServiceRegistration,
)
from palace.manager.sqlalchemy.model.integration import (
    IntegrationConfiguration,
    IntegrationLibraryConfiguration,
)
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.marcfile import MarcFile
from palace.manager.util.datetime_helpers import utc_now


class CacheMARCFiles(LibraryInputScript):
    """Generate and cache MARC files for each input library."""

    name = "Cache MARC files"

    @classmethod
    def arg_parser(cls, _db: Session) -> argparse.ArgumentParser:  # type: ignore[override]
        parser = super().arg_parser(_db)
        parser.add_argument(
            "--force",
            help="Generate new MARC files even if MARC files have already been generated recently enough",
            dest="force",
            action="store_true",
        )
        return parser

    def __init__(
        self,
        _db: Session | None = None,
        cmd_args: Sequence[str] | None = None,
        exporter: MARCExporter | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(_db, *args, **kwargs)
        self.force = False
        self.parse_args(cmd_args)
        self.storage_service = self.services.storage.public()
        self.base_url = self.services.config.sitewide.base_url()
        if self.base_url is None:
            raise CannotLoadConfiguration(
                f"Missing required environment variable: PALACE_BASE_URL."
            )

        self.exporter = exporter or MARCExporter(self._db, self.storage_service)

    def parse_args(self, cmd_args: Sequence[str] | None = None) -> argparse.Namespace:
        parser = self.arg_parser(self._db)
        parsed = parser.parse_args(cmd_args)
        self.force = parsed.force
        return parsed

    def settings(
        self, library: Library
    ) -> tuple[MarcExporterSettings, MarcExporterLibrarySettings]:
        integration_query = (
            select(IntegrationLibraryConfiguration)
            .join(IntegrationConfiguration)
            .where(
                IntegrationConfiguration.goal == Goals.CATALOG_GOAL,
                IntegrationConfiguration.protocol == MARCExporter.__name__,
                IntegrationLibraryConfiguration.library == library,
            )
        )
        integration = self._db.execute(integration_query).scalar_one()

        library_settings = MARCExporter.library_settings_load(integration)
        settings = MARCExporter.settings_load(integration.parent)

        return settings, library_settings

    def process_libraries(self, libraries: Sequence[Library]) -> None:
        if not self.storage_service:
            self.log.info("No storage service was found.")
            return

        super().process_libraries(libraries)

    def get_collections(self, library: Library) -> Sequence[Collection]:
        return self._db.scalars(
            select(Collection).where(
                Collection.libraries.contains(library),
                Collection.export_marc_records == True,
            )
        ).all()

    def get_web_client_urls(
        self, library: Library, url: str | None = None
    ) -> list[str]:
        """Find web client URLs configured by the registry for this library."""
        urls = [
            s.web_client
            for s in self._db.execute(
                select(DiscoveryServiceRegistration.web_client).where(
                    DiscoveryServiceRegistration.library == library,
                    DiscoveryServiceRegistration.web_client != None,
                )
            ).all()
        ]

        if url:
            urls.append(url)

        return urls

    def process_library(
        self, library: Library, annotator_cls: type[MarcAnnotator] = MarcAnnotator
    ) -> None:
        try:
            settings, library_settings = self.settings(library)
        except NoResultFound:
            return

        self.log.info("Processing library %s" % library.name)

        update_frequency = int(settings.update_frequency)

        # Find the collections for this library.
        collections = self.get_collections(library)

        # Find web client URLs configured by the registry for this library.
        web_client_urls = self.get_web_client_urls(
            library, library_settings.web_client_url
        )

        annotator = annotator_cls(
            self.base_url,
            library.short_name or "",
            web_client_urls,
            library_settings.organization_code,
            library_settings.include_summary,
            library_settings.include_genres,
        )

        # We set the creation time to be the start of the batch. Any updates that happen during the batch will be
        # included in the next batch.
        creation_time = utc_now()

        for collection in collections:
            self.process_collection(
                library,
                collection,
                annotator,
                update_frequency,
                creation_time,
            )

    def last_updated(
        self, library: Library, collection: Collection
    ) -> datetime.datetime | None:
        """Find the most recent MarcFile creation time."""
        last_updated_file = self._db.execute(
            select(MarcFile.created)
            .where(
                MarcFile.library == library,
                MarcFile.collection == collection,
            )
            .order_by(MarcFile.created.desc())
        ).first()

        return last_updated_file.created if last_updated_file else None

    def process_collection(
        self,
        library: Library,
        collection: Collection,
        annotator: MarcAnnotator,
        update_frequency: int,
        creation_time: datetime.datetime,
    ) -> None:
        last_update = self.last_updated(library, collection)

        if (
            not self.force
            and last_update
            and (last_update > creation_time - timedelta(days=update_frequency))
        ):
            self.log.info(
                f"Skipping collection {collection.name} because last update was less than {update_frequency} days ago"
            )
            return

        # First update the file with ALL the records.
        self.exporter.records(
            library, collection, annotator, creation_time=creation_time
        )

        # Then create a new file with changes since the last update.
        if last_update:
            self.exporter.records(
                library,
                collection,
                annotator,
                creation_time=creation_time,
                since_time=last_update,
            )

        self._db.commit()
        self.log.info("Processed collection %s" % collection.name)

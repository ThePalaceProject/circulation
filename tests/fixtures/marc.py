import datetime
from collections.abc import Sequence
from typing import Literal

import pytest

from palace.manager.integration.goals import Goals
from palace.manager.marc.exporter import LibraryInfo, MarcExporter
from palace.manager.marc.settings import MarcExporterLibrarySettings
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.marcfile import MarcFile
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import create
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


class MarcExporterFixture:
    def __init__(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ):
        self._db = db
        self._services_fixture = services_fixture

        self.registry = (
            services_fixture.services.integration_registry.catalog_services()
        )
        self.session = db.session

        self.library1 = db.default_library()
        self.library1.short_name = "library1"
        self.library2 = db.library(short_name="library2")

        self.collection1 = db.collection(name="collection1")
        self.collection2 = db.collection()
        self.collection3 = db.collection()

        self.collection1.associated_libraries = [self.library1, self.library2]
        self.collection2.associated_libraries = [self.library1]
        self.collection3.associated_libraries = [self.library2]

    def integration(self) -> IntegrationConfiguration:
        return self._db.integration_configuration(
            MarcExporter, Goals.CATALOG_GOAL, name="MARC Exporter"
        )

    def work(self, collection: Collection | None = None) -> Work:
        collection = collection or self.collection1
        edition = self._db.edition()
        self._db.licensepool(edition, collection=collection)
        work = self._db.work(presentation_edition=edition)
        # We set the works last updated time to 1 day ago, so we know this work
        # will only be included in delta exports covering a time range before
        # 1 day ago. This lets us easily test works being included / excluded
        # based on their `last_update_time`.
        work.last_update_time = utc_now() - datetime.timedelta(days=1)
        return work

    def works(self, collection: Collection | None = None) -> list[Work]:
        return [self.work(collection) for _ in range(5)]

    def configure_export(self) -> None:
        marc_integration = self.integration()
        self._db.integration_library_configuration(
            marc_integration,
            self.library1,
            MarcExporterLibrarySettings(organization_code="library1-org"),
        )
        self._db.integration_library_configuration(
            marc_integration,
            self.library2,
            MarcExporterLibrarySettings(organization_code="library2-org"),
        )

        self.collection1.export_marc_records = True
        self.collection2.export_marc_records = True
        self.collection3.export_marc_records = True

    def enabled_libraries(
        self, collection: Collection | None = None
    ) -> Sequence[LibraryInfo]:
        collection = collection or self.collection1
        assert collection.id is not None
        return MarcExporter.enabled_libraries(
            self.session, self.registry, collection_id=collection.id
        )

    def marc_file(
        self,
        *,
        key: str | None = None,
        collection: Collection | None | Literal[False] = False,
        library: Library | None | Literal[False] = False,
        created: datetime.datetime | None = None,
        since: datetime.datetime | None = None
    ) -> MarcFile:
        collection = collection if collection is not False else self.collection1
        library = library if library is not False else self.library1
        key = key or self._db.fresh_str()
        created = created or utc_now()
        marc_file, _ = create(
            self.session,
            MarcFile,
            library=library,
            collection=collection,
            key=key,
            created=created,
            since=since,
        )
        return marc_file


@pytest.fixture
def marc_exporter_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
) -> MarcExporterFixture:
    return MarcExporterFixture(db, services_fixture)

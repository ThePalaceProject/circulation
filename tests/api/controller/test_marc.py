from __future__ import annotations

import datetime
from unittest.mock import MagicMock

import pytest
from flask import Response

from api.controller.marc import MARCRecordController
from core.integration.goals import Goals
from core.marc import MARCExporter
from core.model import Collection, Library, MarcFile, create
from core.service.storage.s3 import S3Service
from core.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class MARCRecordControllerFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.mock_s3_service = MagicMock(spec=S3Service)
        self.mock_s3_service.generate_url = lambda x: "http://s3.url/" + x
        self.controller = MARCRecordController(self.mock_s3_service)
        self.library = db.default_library()
        self.collection = db.default_collection()
        self.collection.export_marc_records = True

        # stub out the library function to return the default library,
        # since we don't have a request context
        self.controller.library = lambda: self.library

    def integration(self, library: Library | None = None):
        library = library or self.library
        return self.db.integration_configuration(
            MARCExporter.__name__,
            Goals.CATALOG_GOAL,
            libraries=[library],
        )

    def file(
        self,
        library: Library | None = None,
        collection: Collection | None = None,
        key: str | None = None,
        created: datetime.datetime | None = None,
        since: datetime.datetime | None = None,
    ):
        key = key or self.db.fresh_str()
        created = created or utc_now()
        library = library or self.library
        collection = collection or self.collection

        return create(
            self.db.session,
            MarcFile,
            library=library,
            collection=collection,
            created=created,
            since=since,
            key=key,
        )

    def get_response_html(self, response: Response) -> str:
        assert response.status_code == 200
        assert response.content_type == "text/html"
        html = response.get_data(as_text=True)
        assert ("Download MARC files for %s" % self.library.name) in html
        return html


@pytest.fixture
def marc_record_controller_fixture(
    db: DatabaseTransactionFixture,
) -> MARCRecordControllerFixture:
    return MARCRecordControllerFixture(db)


class TestMARCRecordController:
    def test_has_integration(
        self, marc_record_controller_fixture: MARCRecordControllerFixture
    ):
        # No integration is configured.
        assert not marc_record_controller_fixture.controller.has_integration(
            marc_record_controller_fixture.db.session,
            marc_record_controller_fixture.library,
        )

        # An integration is configured, but not for this library.
        other_library = marc_record_controller_fixture.db.library()
        marc_record_controller_fixture.integration(library=other_library)
        assert not marc_record_controller_fixture.controller.has_integration(
            marc_record_controller_fixture.db.session,
            marc_record_controller_fixture.library,
        )

        # An integration is configured for this library.
        marc_record_controller_fixture.integration()
        assert marc_record_controller_fixture.controller.has_integration(
            marc_record_controller_fixture.db.session,
            marc_record_controller_fixture.library,
        )

    def test_get_files_no_files(
        self, marc_record_controller_fixture: MARCRecordControllerFixture
    ):
        assert (
            marc_record_controller_fixture.controller.get_files(
                marc_record_controller_fixture.db.session,
                marc_record_controller_fixture.library,
            )
            == {}
        )

    def test_get_files_one_collection(
        self, marc_record_controller_fixture: MARCRecordControllerFixture
    ):
        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)
        last_week = now - datetime.timedelta(days=7)

        # Only a single full file is given, the most recent one. Even
        # though there are older full files, they are ignored.
        marc_record_controller_fixture.file(created=now)
        marc_record_controller_fixture.file(created=yesterday)

        # There are multiple delta files, and they are all returned.
        marc_record_controller_fixture.file(created=now, since=yesterday)
        marc_record_controller_fixture.file(created=last_week, since=yesterday)

        files = marc_record_controller_fixture.controller.get_files(
            marc_record_controller_fixture.db.session,
            marc_record_controller_fixture.library,
        )

        assert len(files) == 1
        assert files["Default Collection"].full is not None
        assert files["Default Collection"].full.created == now

        assert len(files["Default Collection"].deltas) == 2

        # The delta files are sorted by their created date, so the latest
        # delta file is first.
        [delta_now, delta_last_week] = files["Default Collection"].deltas
        assert delta_now.created == now
        assert delta_now.since == yesterday
        assert delta_last_week.created == last_week
        assert delta_last_week.since == yesterday

    def test_get_files_collection_removed_from_library(
        self, marc_record_controller_fixture: MARCRecordControllerFixture
    ):
        marc_record_controller_fixture.file(created=utc_now())
        files = marc_record_controller_fixture.controller.get_files(
            marc_record_controller_fixture.db.session,
            marc_record_controller_fixture.library,
        )
        assert len(files) == 1

        # The collection is removed from the library, so it's not returned.
        marc_record_controller_fixture.collection.libraries = []

        files = marc_record_controller_fixture.controller.get_files(
            marc_record_controller_fixture.db.session,
            marc_record_controller_fixture.library,
        )
        assert len(files) == 0

    def test_get_files_multiple_collections(
        self, marc_record_controller_fixture: MARCRecordControllerFixture
    ):
        db = marc_record_controller_fixture.db
        now = utc_now()
        last_week = now - datetime.timedelta(days=7)

        # Add a full file to the default collection.
        collection_1 = marc_record_controller_fixture.collection
        marc_record_controller_fixture.file(collection=collection_1, created=last_week)

        # Create a second collection, with a full file and a delta.
        collection_2 = db.collection(name="Second Collection")
        collection_2.export_marc_records = True
        collection_2.libraries = [marc_record_controller_fixture.library]
        marc_record_controller_fixture.file(collection=collection_2, created=now)
        marc_record_controller_fixture.file(
            collection=collection_2, created=now, since=last_week
        )

        # Create a third collection that doesn't export MARC records.
        collection_3 = db.collection()
        collection_3.export_marc_records = False
        collection_3.libraries = [marc_record_controller_fixture.library]
        marc_record_controller_fixture.file(collection=collection_3, created=now)

        # Create a fourth collection that doesn't belong to the library.
        collection_4 = db.collection()
        collection_4.export_marc_records = True
        collection_4.libraries = []
        marc_record_controller_fixture.file(collection=collection_4, created=now)

        files = marc_record_controller_fixture.controller.get_files(
            db.session,
            marc_record_controller_fixture.library,
        )

        assert len(files) == 2

        # The returned collections are sorted by name.
        assert list(files.keys()) == [collection_1.name, collection_2.name]

        [collection_1_result, collection_2_result] = files.values()

        assert collection_1_result.full is not None
        assert collection_1_result.full.created == last_week
        assert len(collection_1_result.deltas) == 0

        assert collection_2_result.full is not None
        assert collection_2_result.full.created == now
        assert len(collection_2_result.deltas) == 1

    def test_download_page_with_full_and_delta(
        self, marc_record_controller_fixture: MARCRecordControllerFixture
    ):
        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)
        last_week = now - datetime.timedelta(days=7)

        marc_record_controller_fixture.integration()
        marc_record_controller_fixture.file(key="full", created=now)
        marc_record_controller_fixture.file(key="old_full", created=yesterday)
        marc_record_controller_fixture.file(key="delta_1", created=now, since=yesterday)
        marc_record_controller_fixture.file(
            key="delta_2", created=yesterday, since=last_week
        )

        response = marc_record_controller_fixture.controller.download_page()
        html = marc_record_controller_fixture.get_response_html(response)

        assert (
            '<a href="http://s3.url/full">Full file - last updated %s</a>'
            % now.strftime("%B %-d, %Y")
            in html
        )
        assert '<a href="http://s3.url/old_full">' not in html
        assert "<h4>Update-only files</h4>" in html
        assert (
            '<a href="http://s3.url/delta_1">Updates from %s to %s</a>'
            % (yesterday.strftime("%B %-d, %Y"), now.strftime("%B %-d, %Y"))
            in html
        )
        assert (
            '<a href="http://s3.url/delta_2">Updates from %s to %s</a>'
            % (last_week.strftime("%B %-d, %Y"), yesterday.strftime("%B %-d, %Y"))
            in html
        )

    def test_download_page_with_exporter_but_no_collection(
        self, marc_record_controller_fixture: MARCRecordControllerFixture
    ):
        marc_record_controller_fixture.integration()
        marc_record_controller_fixture.collection.export_marc_records = False

        response = marc_record_controller_fixture.controller.download_page()
        html = marc_record_controller_fixture.get_response_html(response)
        assert "No collections are configured to export MARC records" in html

    def test_download_page_with_exporter_but_no_files(
        self, marc_record_controller_fixture: MARCRecordControllerFixture
    ):
        marc_record_controller_fixture.integration()

        response = marc_record_controller_fixture.controller.download_page()
        html = marc_record_controller_fixture.get_response_html(response)
        assert "MARC files aren't ready" in html

    def test_download_page_no_exporter(
        self, marc_record_controller_fixture: MARCRecordControllerFixture
    ):
        response = marc_record_controller_fixture.controller.download_page()
        html = marc_record_controller_fixture.get_response_html(response)
        assert "No MARC exporter is currently configured" in html

    def test_download_page_no_storage_service(
        self, marc_record_controller_fixture: MARCRecordControllerFixture
    ):
        marc_record_controller_fixture.integration()
        controller = marc_record_controller_fixture.controller
        controller.storage_service = None

        response = controller.download_page()
        html = marc_record_controller_fixture.get_response_html(response)
        assert "No storage service is currently configured" in html

import datetime
from unittest.mock import MagicMock

from api.controller import MARCRecordController
from core.integration.goals import Goals
from core.marc import MARCExporter
from core.model import MarcFile, create
from core.service.storage.s3 import S3Service
from core.util.datetime_helpers import utc_now
from tests.fixtures.api_controller import CirculationControllerFixture


class TestMARCRecordController:
    def test_download_page_with_exporter_and_files(
        self, circulation_fixture: CirculationControllerFixture
    ):
        db = circulation_fixture.db

        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)

        library = db.default_library()
        collection = db.default_collection()
        collection.export_marc_records = True

        db.integration_configuration(
            MARCExporter.__name__,
            Goals.CATALOG_GOAL,
            libraries=[library],
        )

        cache1, ignore = create(
            db.session,
            MarcFile,
            library=library,
            collection=collection,
            created=now,
            key="cache1",
        )

        cache2, ignore = create(
            db.session,
            MarcFile,
            library=library,
            collection=collection,
            created=yesterday,
            key="cache2",
        )

        cache3, ignore = create(
            db.session,
            MarcFile,
            library=library,
            collection=collection,
            created=now,
            since=yesterday,
            key="cache3",
        )

        mock_s3_service = MagicMock(spec=S3Service)
        mock_s3_service.generate_url = lambda x: x
        marc_records = MARCRecordController(
            circulation_fixture.manager, mock_s3_service
        )

        with circulation_fixture.request_context_with_library("/"):
            response = marc_records.download_page()
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert ("Download MARC files for %s" % library.name) in html

            assert f"<h3>{collection.name}</h3>" in html
            assert (
                '<a href="cache1">Full file - last updated %s</a>'
                % now.strftime("%B %-d, %Y")
                in html
            )
            assert "<h4>Update-only files</h4>" in html
            assert (
                '<a href="cache3">Updates from %s to %s</a>'
                % (yesterday.strftime("%B %-d, %Y"), now.strftime("%B %-d, %Y"))
                in html
            )

    def test_download_page_with_exporter_but_no_files(
        self, circulation_fixture: CirculationControllerFixture
    ):
        db = circulation_fixture.db

        library = db.default_library()

        db.integration_configuration(
            MARCExporter.__name__,
            Goals.CATALOG_GOAL,
            libraries=[library],
        )

        with circulation_fixture.request_context_with_library("/"):
            response = circulation_fixture.manager.marc_records.download_page()
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert ("Download MARC files for %s" % library.name) in html
            assert "MARC files aren't ready" in html

    def test_download_page_no_exporter(
        self, circulation_fixture: CirculationControllerFixture
    ):
        db = circulation_fixture.db
        library = db.default_library()

        with circulation_fixture.request_context_with_library("/"):
            response = circulation_fixture.manager.marc_records.download_page()
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert ("Download MARC files for %s" % library.name) in html
            assert "No MARC exporter is currently configured" in html

    def test_download_page_no_storage_service(
        self, circulation_fixture: CirculationControllerFixture
    ):
        db = circulation_fixture.db
        library = db.default_library()

        db.integration_configuration(
            MARCExporter.__name__,
            Goals.CATALOG_GOAL,
            libraries=[library],
        )

        marc_records = MARCRecordController(circulation_fixture.manager, None)

        with circulation_fixture.request_context_with_library("/"):
            response = marc_records.download_page()
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert ("Download MARC files for %s" % library.name) in html
            assert "No storage service is currently configured" in html

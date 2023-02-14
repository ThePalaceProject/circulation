import datetime

from core.model import CachedMARCFile, ExternalIntegration, Representation, create
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
        lane = db.lane(display_name="Test Lane")

        exporter = db.external_integration(
            ExternalIntegration.MARC_EXPORT,
            ExternalIntegration.CATALOG_GOAL,
            libraries=[db.default_library()],
        )

        rep1, ignore = create(
            db.session,
            Representation,
            url="http://mirror1",
            mirror_url="http://mirror1",
            media_type=Representation.MARC_MEDIA_TYPE,
            mirrored_at=now,
        )
        cache1, ignore = create(
            db.session,
            CachedMARCFile,
            library=db.default_library(),
            lane=None,
            representation=rep1,
            end_time=now,
        )

        rep2, ignore = create(
            db.session,
            Representation,
            url="http://mirror2",
            mirror_url="http://mirror2",
            media_type=Representation.MARC_MEDIA_TYPE,
            mirrored_at=yesterday,
        )
        cache2, ignore = create(
            db.session,
            CachedMARCFile,
            library=db.default_library(),
            lane=lane,
            representation=rep2,
            end_time=yesterday,
        )

        rep3, ignore = create(
            db.session,
            Representation,
            url="http://mirror3",
            mirror_url="http://mirror3",
            media_type=Representation.MARC_MEDIA_TYPE,
            mirrored_at=now,
        )
        cache3, ignore = create(
            db.session,
            CachedMARCFile,
            library=db.default_library(),
            lane=None,
            representation=rep3,
            end_time=now,
            start_time=yesterday,
        )

        with circulation_fixture.request_context_with_library("/"):
            response = circulation_fixture.manager.marc_records.download_page()
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert ("Download MARC files for %s" % library.name) in html

            assert "<h3>All Books</h3>" in html
            assert (
                '<a href="http://mirror1">Full file - last updated %s</a>'
                % now.strftime("%B %-d, %Y")
                in html
            )
            assert "<h4>Update-only files</h4>" in html
            assert (
                '<a href="http://mirror3">Updates from %s to %s</a>'
                % (yesterday.strftime("%B %-d, %Y"), now.strftime("%B %-d, %Y"))
                in html
            )

            assert "<h3>Test Lane</h3>" in html
            assert (
                '<a href="http://mirror2">Full file - last updated %s</a>'
                % yesterday.strftime("%B %-d, %Y")
                in html
            )

    def test_download_page_with_exporter_but_no_files(
        self, circulation_fixture: CirculationControllerFixture
    ):
        db = circulation_fixture.db

        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)

        library = db.default_library()

        exporter = db.external_integration(
            ExternalIntegration.MARC_EXPORT,
            ExternalIntegration.CATALOG_GOAL,
            libraries=[db.default_library()],
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
            assert ("No MARC exporter is currently configured") in html

        # If the exporter was deleted after some MARC files were cached,
        # they will still be available to download.
        now = utc_now()
        rep, ignore = create(
            db.session,
            Representation,
            url="http://mirror1",
            mirror_url="http://mirror1",
            media_type=Representation.MARC_MEDIA_TYPE,
            mirrored_at=now,
        )
        cache, ignore = create(
            db.session,
            CachedMARCFile,
            library=db.default_library(),
            lane=None,
            representation=rep,
            end_time=now,
        )

        with circulation_fixture.request_context_with_library("/"):
            response = circulation_fixture.manager.marc_records.download_page()
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert ("Download MARC files for %s" % library.name) in html
            assert "No MARC exporter is currently configured" in html
            assert "<h3>All Books</h3>" in html
            assert (
                '<a href="http://mirror1">Full file - last updated %s</a>'
                % now.strftime("%B %-d, %Y")
                in html
            )

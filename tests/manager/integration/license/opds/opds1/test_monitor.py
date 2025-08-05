from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests_mock

from palace.manager.integration.license.opds.opds1.api import OPDSAPI
from palace.manager.integration.license.opds.opds1.importer import OPDSImporter
from palace.manager.integration.license.opds.opds1.monitor import OPDSImportMonitor
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.sqlalchemy.model.coverage import CoverageRecord
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.resource import Hyperlink
from palace.manager.util import http
from palace.manager.util.datetime_helpers import datetime_utc
from palace.manager.util.http import HTTP, BadResponseException
from palace.manager.util.opds_writer import AtomFeed, OPDSFeed
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.http import MockHttpClientFixture
from tests.manager.integration.license.opds.opds1.conftest import OPDSImporterFixture
from tests.manager.integration.license.opds.opds1.test_importer import (
    DoomedOPDSImporter,
)
from tests.mocks.mock import MockRequestsResponse


class TestOPDSImportMonitor:
    def test_constructor(self, db: DatabaseTransactionFixture):
        session = db.session

        with pytest.raises(ValueError) as excinfo:
            OPDSImportMonitor(session, None, OPDSImporter)  # type: ignore[arg-type]
        assert (
            "OPDSImportMonitor can only be run in the context of a Collection."
            in str(excinfo.value)
        )
        c1 = db.collection(protocol=OverdriveAPI)
        with pytest.raises(ValueError) as excinfo:
            OPDSImportMonitor(session, c1, OPDSImporter)
        assert (
            f"Collection {c1.name} is configured for protocol Overdrive, not OPDS Import."
            in str(excinfo.value)
        )

        c2 = db.collection(
            protocol=OPDSAPI,
            settings=db.opds_settings(
                external_account_id="https://opds.import.com/feed?size=100",
            ),
        )
        monitor = OPDSImportMonitor(session, c2, OPDSImporter)
        assert monitor._feed_base_url == "https://opds.import.com/"

    def test_get(
        self,
        db: DatabaseTransactionFixture,
    ):
        session = db.session

        ## Test whether relative urls work
        collection = db.collection(
            settings=db.opds_settings(
                external_account_id="https://opds.import.com:9999/feed",
            ),
        )
        monitor = OPDSImportMonitor(session, collection, OPDSImporter)

        with patch.object(HTTP, "get_with_timeout") as mock_get:
            monitor._get("/absolute/path", {})
            assert mock_get.call_args[0] == (
                "https://opds.import.com:9999/absolute/path",
            )

            mock_get.reset_mock()
            monitor._get("relative/path", {})
            assert mock_get.call_args[0] == (
                "https://opds.import.com:9999/relative/path",
            )

    def test_hook_methods(self, db: DatabaseTransactionFixture):
        """By default, the OPDS URL and data source used by the importer
        come from the collection configuration.
        """
        collection = db.collection()
        monitor = OPDSImportMonitor(
            db.session,
            collection,
            import_class=OPDSImporter,
        )

        assert collection.data_source == monitor.data_source(collection)

    def test_feed_contains_new_data(
        self,
        opds_importer_fixture: OPDSImporterFixture,
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        feed = data.content_server_mini_feed

        class MockOPDSImportMonitor(OPDSImportMonitor):
            def _get(self, url, headers):
                return MockRequestsResponse(
                    200, {"content-type": AtomFeed.ATOM_TYPE}, feed
                )

        data_source_name = "OPDS"
        collection = db.collection(
            protocol=OPDSAPI,
            settings=db.opds_settings(data_source=data_source_name),
        )
        monitor = OPDSImportMonitor(
            session,
            collection,
            import_class=OPDSImporter,
        )
        timestamp = monitor.timestamp()

        # Nothing has been imported yet, so all data is new.
        assert monitor.feed_contains_new_data(feed) is True
        assert timestamp.start is None

        # Now import the editions.
        monitor = MockOPDSImportMonitor(
            session,
            collection=collection,
            import_class=OPDSImporter,
        )
        monitor.run()

        # Editions have been imported.
        assert 2 == session.query(Edition).count()

        # The timestamp has been updated, although unlike most
        # Monitors the timestamp is purely informational.
        assert timestamp.finish is not None

        editions = session.query(Edition).all()
        data_source = DataSource.lookup(session, data_source_name)

        # If there are CoverageRecords that record work are after the updated
        # dates, there's nothing new.
        record, ignore = CoverageRecord.add_for(
            editions[0],
            data_source,
            CoverageRecord.IMPORT_OPERATION,
            collection=collection,
        )
        record.timestamp = datetime_utc(2016, 1, 1, 1, 1, 1)

        record2, ignore = CoverageRecord.add_for(
            editions[1],
            data_source,
            CoverageRecord.IMPORT_OPERATION,
            collection=collection,
        )
        record2.timestamp = datetime_utc(2016, 1, 1, 1, 1, 1)

        assert monitor.feed_contains_new_data(feed) is False

        # If the monitor is set up to force reimport, it doesn't
        # matter that there's nothing new--we act as though there is.
        monitor.force_reimport = True
        assert monitor.feed_contains_new_data(feed) is True
        monitor.force_reimport = False

        # If an entry was updated after the date given in that entry's
        # CoverageRecord, there's new data.
        record2.timestamp = datetime_utc(1970, 1, 1, 1, 1, 1)
        assert monitor.feed_contains_new_data(feed) is True

        # If a CoverageRecord is a transient failure, we try again
        # regardless of whether it's been updated.
        for r in [record, record2]:
            r.timestamp = datetime_utc(2016, 1, 1, 1, 1, 1)
            r.exception = "Failure!"
            r.status = CoverageRecord.TRANSIENT_FAILURE
        assert monitor.feed_contains_new_data(feed) is True

        # If a CoverageRecord is a persistent failure, we don't try again...
        for r in [record, record2]:
            r.status = CoverageRecord.PERSISTENT_FAILURE
        assert monitor.feed_contains_new_data(feed) is False

        # ...unless the feed updates.
        record.timestamp = datetime_utc(1970, 1, 1, 1, 1, 1)
        assert monitor.feed_contains_new_data(feed) is True

    def test_follow_one_link(
        self,
        opds_importer_fixture: OPDSImporterFixture,
        http_client: MockHttpClientFixture,
    ):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )
        data_source_name = "OPDS"
        collection = db.collection(
            protocol=OPDSAPI,
            settings=db.opds_settings(data_source=data_source_name),
        )
        monitor = OPDSImportMonitor(
            session,
            collection=collection,
            import_class=OPDSImporter,
        )
        feed = data.content_server_mini_feed

        # If there's new data, follow_one_link extracts the next links.
        def follow():
            return monitor.follow_one_link("http://url", do_get=http_client.do_get)

        http_client.queue_response(200, OPDSFeed.ACQUISITION_FEED_TYPE, content=feed)
        next_links, content = follow()
        assert 1 == len(next_links)
        assert "http://localhost:5000/?after=327&size=100" == next_links[0]

        assert feed.encode("utf-8") == content

        # Now import the editions and add coverage records.
        monitor.importer.import_from_feed(feed)
        assert 2 == session.query(Edition).count()

        editions = session.query(Edition).all()
        data_source = DataSource.lookup(session, data_source_name)

        for edition in editions:
            record, ignore = CoverageRecord.add_for(
                edition,
                data_source,
                CoverageRecord.IMPORT_OPERATION,
                collection=collection,
            )
            record.timestamp = datetime_utc(2016, 1, 1, 1, 1, 1)

        # If there's no new data, follow_one_link returns no next
        # links and no content.
        #
        # Note that this works even when the media type is imprecisely
        # specified as Atom or bare XML.
        for imprecise_media_type in OPDSFeed.ATOM_LIKE_TYPES:
            http_client.queue_response(200, imprecise_media_type, content=feed)
            next_links, content = follow()
            assert 0 == len(next_links)
            assert None == content

        http_client.queue_response(200, AtomFeed.ATOM_TYPE, content=feed)
        next_links, content = follow()
        assert 0 == len(next_links)
        assert None == content

        # If the media type is missing or is not an Atom feed,
        # an exception is raised.
        http_client.queue_response(200, None, content=feed)
        with pytest.raises(BadResponseException) as excinfo:
            follow()
        assert "Expected Atom feed, got None" in str(excinfo.value)

        http_client.queue_response(200, "not/atom", content=feed)
        with pytest.raises(BadResponseException) as excinfo:
            follow()
        assert "Expected Atom feed, got not/atom" in str(excinfo.value)

    def test_import_one_feed(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )
        # Check coverage records are created.
        data_source_name = "OPDS"
        collection = db.collection(
            settings=db.opds_settings(
                external_account_id="http://root-url/index.xml",
                data_source=data_source_name,
            ),
        )
        monitor = OPDSImportMonitor(
            session,
            collection=collection,
            import_class=DoomedOPDSImporter,
        )
        data_source = DataSource.lookup(session, data_source_name)

        feed = data.content_server_mini_feed

        imported, failures = monitor.import_one_feed(feed)

        editions = session.query(Edition).all()

        # One edition has been imported
        assert 1 == len(editions)
        [edition] = editions

        # The return value of import_one_feed includes the imported
        # editions.
        assert [edition] == imported

        # That edition has a CoverageRecord.
        record = CoverageRecord.lookup(
            editions[0].primary_identifier,
            data_source,
            operation=CoverageRecord.IMPORT_OPERATION,
            collection=collection,
        )
        assert CoverageRecord.SUCCESS == record.status
        assert record.exception is None

        # The edition's primary identifier has some cover links whose
        # relative URL have been resolved relative to the Collection's
        # external_account_id.
        covers = {
            x.resource.url
            for x in editions[0].primary_identifier.links
            if x.rel == Hyperlink.IMAGE
        }
        assert covers == {
            "http://root-url/broken-cover-image",
            "http://root-url/working-cover-image",
        }

        # The 202 status message in the feed caused a transient failure.
        # The exception caused a persistent failure.

        coverage_records = session.query(CoverageRecord).filter(
            CoverageRecord.operation == CoverageRecord.IMPORT_OPERATION,
            CoverageRecord.status != CoverageRecord.SUCCESS,
        )
        assert sorted(
            [CoverageRecord.TRANSIENT_FAILURE, CoverageRecord.PERSISTENT_FAILURE]
        ) == sorted(x.status for x in coverage_records)

        identifier, ignore = Identifier.parse_urn(
            session, "urn:librarysimplified.org/terms/id/Gutenberg%20ID/10441"
        )
        failure = CoverageRecord.lookup(
            identifier,
            data_source,
            operation=CoverageRecord.IMPORT_OPERATION,
            collection=collection,
        )
        assert "Utter failure!" in failure.exception

        # Both failures were reported in the return value from
        # import_one_feed
        assert 2 == len(failures)

    def test_run_once(self, db: DatabaseTransactionFixture):
        class MockOPDSImportMonitor(OPDSImportMonitor):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.responses = []
                self.imports = []

            def queue_response(self, response):
                self.responses.append(response)

            def follow_one_link(self, link, cutoff_date=None, do_get=None):
                return self.responses.pop()

            def import_one_feed(self, feed):
                # Simulate two successes and one failure on every page.
                self.imports.append(feed)
                return [object(), object()], {"identifier": "Failure"}

        collection = db.collection()

        monitor = MockOPDSImportMonitor(
            db.session,
            collection=collection,
            import_class=OPDSImporter,
        )

        monitor.queue_response([[], "last page"])
        monitor.queue_response([["second next link"], "second page"])
        monitor.queue_response([["next link"], "first page"])

        progress = monitor.run_once(MagicMock())

        # Feeds are imported in reverse order
        assert ["last page", "second page", "first page"] == monitor.imports

        # Every page of the import had two successes and one failure.
        assert "Items imported: 6. Failures: 3." == progress.achievements

        # The TimestampData returned by run_once does not include any
        # timing information; that's provided by run().
        assert progress.start is None
        assert progress.finish is None

    def test_update_headers(self, db: DatabaseTransactionFixture):
        collection = db.collection()

        # Test the _update_headers helper method.
        monitor = OPDSImportMonitor(
            db.session,
            collection=collection,
            import_class=OPDSImporter,
        )

        # _update_headers return a new dictionary. An Accept header will be setted
        # using the value of custom_accept_header. If the value is not set a
        # default value will be used.
        headers = {"Some other": "header"}
        new_headers = monitor._update_headers(headers)
        assert ["Some other"] == list(headers.keys())
        assert ["Accept", "Some other"] == sorted(list(new_headers.keys()))

        # If a custom_accept_header exist, will be used instead a default value
        new_headers = monitor._update_headers(headers)
        old_value = new_headers["Accept"]
        target_value = old_value + "more characters"
        monitor.custom_accept_header = target_value
        new_headers = monitor._update_headers(headers)
        assert new_headers["Accept"] == target_value
        assert old_value != target_value

        # If the monitor has a username and password, an Authorization
        # header using HTTP Basic Authentication is also added.
        monitor.username = "a user"
        monitor.password = "a password"
        headers = {}
        new_headers = monitor._update_headers(headers)
        assert new_headers["Authorization"].startswith("Basic")

        # However, if the Authorization and/or Accept headers have been
        # filled in by some other piece of code, _update_headers does
        # not touch them.
        expect = dict(Accept="text/html", Authorization="Bearer abc")
        headers = dict(expect)
        new_headers = monitor._update_headers(headers)
        assert headers == expect

    def test_retry(self, opds_importer_fixture: OPDSImporterFixture):
        data, db, session = (
            opds_importer_fixture,
            opds_importer_fixture.db,
            opds_importer_fixture.db.session,
        )

        retry_count = 15
        feed = data.content_server_mini_feed
        feed_url = "https://example.com/feed.opds"

        collection = db.collection(
            settings=db.opds_settings(
                external_account_id=feed_url,
                max_retry_count=retry_count,
            ),
        )

        # The importer takes its retry count from the collection settings.
        monitor = OPDSImportMonitor(
            session,
            collection=collection,
            import_class=OPDSImporter,
        )

        # We mock Retry class to ensure that the correct retry count had been passed.
        with patch.object(http, "Retry") as retry_constructor_mock:
            with requests_mock.Mocker() as request_mock:
                request_mock.get(
                    feed_url,
                    text=feed,
                    status_code=200,
                    headers={"content-type": OPDSFeed.ACQUISITION_FEED_TYPE},
                )

                monitor.follow_one_link(feed_url)

                # Ensure that the correct retry count had been passed.
                retry_constructor_mock.assert_called_once_with(
                    total=retry_count,
                    status_forcelist=[429, 500, 502, 503, 504],
                    backoff_factor=1.0,
                )

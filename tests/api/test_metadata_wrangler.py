"""Tests of the Monitors and CoverageProviders associated with the metadata
wrangler.
"""

import datetime
from typing import Any

import feedparser
import pytest

from api.metadata_wrangler import (
    BaseMetadataWranglerCoverageProvider,
    MetadataUploadCoverageProvider,
    MetadataWranglerCollectionReaper,
    MetadataWranglerCollectionRegistrar,
    MWAuxiliaryMetadataMonitor,
    MWCollectionUpdateMonitor,
)
from core.config import CannotLoadConfiguration
from core.coverage import CoverageFailure
from core.model import (
    Collection,
    CoverageRecord,
    DataSource,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    LicensePool,
    Timestamp,
)
from core.opds_import import MetadataWranglerOPDSLookup, MockMetadataWranglerOPDSLookup
from core.testing import AlwaysSuccessfulCoverageProvider, MockRequestsResponse
from core.util.datetime_helpers import datetime_utc, utc_now
from core.util.http import BadResponseException
from core.util.opds_writer import OPDSFeed

from ..fixtures.api_opds_files import OPDSAPIFilesFixture
from ..fixtures.database import DatabaseTransactionFixture


class InstrumentedMWCollectionUpdateMonitor(MWCollectionUpdateMonitor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.imports = []

    def import_one_feed(self, timestamp, url):
        self.imports.append((timestamp, url))
        return super().import_one_feed(timestamp, url)


class MonitorFixture:
    def __init__(self, db: DatabaseTransactionFixture, files: OPDSAPIFilesFixture):
        self.db = db
        self.files = files
        self.db.external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            ExternalIntegration.METADATA_GOAL,
            username="abc",
            password="def",
            url=self.db.fresh_str(),
        )
        self.collection = self.db.collection(
            protocol=ExternalIntegration.BIBLIOTHECA, external_account_id="lib"
        )
        self.lookup = MockMetadataWranglerOPDSLookup.from_config(
            self.db.session, self.collection
        )
        self.monitor = InstrumentedMWCollectionUpdateMonitor(
            self.db.session, self.collection, self.lookup
        )

    @property
    def ts(self):
        """Make the timestamp used by run() when calling run_once().
        This makes it easier to test run_once() in isolation.
        """
        return self.monitor.timestamp().to_data()


@pytest.fixture(scope="function")
def monitor_fixture(
    db: DatabaseTransactionFixture, api_opds_files_fixture: OPDSAPIFilesFixture
) -> MonitorFixture:
    return MonitorFixture(db, api_opds_files_fixture)


class TestMWCollectionUpdateMonitor:
    def test_monitor_requires_authentication(self, monitor_fixture: MonitorFixture):
        class Mock:
            authenticated = False

        monitor_fixture.monitor.lookup = Mock()
        with pytest.raises(Exception) as excinfo:
            monitor_fixture.monitor.run_once(monitor_fixture.ts)
        assert "no authentication credentials" in str(excinfo.value)

    def test_import_one_feed(self, monitor_fixture: MonitorFixture):
        data = monitor_fixture.files.sample_data("metadata_updates_response.opds")
        monitor_fixture.lookup.queue_response(
            200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        next_links, editions, timestamp = monitor_fixture.monitor.import_one_feed(
            None, None
        )

        # The 'next' links found in the OPDS feed are returned.
        assert ["http://next-link/"] == next_links

        # Insofar as is possible, all <entry> tags are converted into
        # Editions.
        assert ["9781594632556"] == [x.primary_identifier.identifier for x in editions]

        # The earliest time found in the OPDS feed is returned as a
        # candidate for the Monitor's timestamp.
        assert datetime_utc(2016, 9, 20, 19, 37, 2) == timestamp

    def test_empty_feed_stops_import(self, monitor_fixture: MonitorFixture):
        # We don't follow the 'next' link of an empty feed.
        data = monitor_fixture.files.sample_data("metadata_updates_empty_response.opds")
        monitor_fixture.lookup.queue_response(
            200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        new_timestamp = monitor_fixture.monitor.run()

        # We could have followed the 'next' link, but we chose not to.
        assert [(None, None)] == monitor_fixture.monitor.imports
        assert 1 == len(monitor_fixture.lookup.requests)

        # Since there were no <entry> tags, the timestamp's finish
        # date was set to the <updated> date of the feed itself, minus
        # one day (to avoid race conditions).
        assert (
            datetime_utc(2016, 9, 19, 19, 37, 10)
            == monitor_fixture.monitor.timestamp().finish
        )

    def test_run_once(self, monitor_fixture: MonitorFixture):
        db = monitor_fixture.db

        # Setup authentication and Metadata Wrangler details.
        lp = db.licensepool(
            None,
            data_source_name=DataSource.BIBLIOTHECA,
            collection=monitor_fixture.collection,
        )
        lp.identifier.type = Identifier.BIBLIOTHECA_ID
        isbn = Identifier.parse_urn(db.session, "urn:isbn:9781594632556")[0]
        lp.identifier.equivalent_to(
            DataSource.lookup(db.session, DataSource.BIBLIOTHECA), isbn, 1
        )
        assert [] == lp.identifier.links
        assert [] == lp.identifier.measurements

        # Queue some data to be found.
        responses = (
            "metadata_updates_response.opds",
            "metadata_updates_empty_response.opds",
        )
        for filename in responses:
            data = monitor_fixture.files.sample_data(filename)
            monitor_fixture.lookup.queue_response(
                200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, data
            )

        timestamp = monitor_fixture.ts
        new_timestamp = monitor_fixture.monitor.run_once(timestamp)

        # We have a new value to use for the Monitor's timestamp -- the
        # earliest date seen in the last OPDS feed that contained
        # any entries.
        assert datetime_utc(2016, 9, 20, 19, 37, 2) == new_timestamp.finish
        assert "Editions processed: 1" == new_timestamp.achievements

        # Normally run_once() doesn't update the monitor's timestamp,
        # but this implementation does, so that work isn't redone if
        # run_once() crashes or the monitor is killed.
        assert new_timestamp.finish == monitor_fixture.monitor.timestamp().finish

        # The original Identifier has information from the
        # mock Metadata Wrangler.
        mw_source = DataSource.lookup(db.session, DataSource.METADATA_WRANGLER)
        assert 3 == len(lp.identifier.links)
        [quality] = lp.identifier.measurements
        assert mw_source == quality.data_source

        # Check the URLs we processed.
        url1, url2 = (x[0] for x in monitor_fixture.lookup.requests)

        # The first URL processed was the default one for the
        # MetadataWranglerOPDSLookup.
        assert (
            monitor_fixture.lookup.get_collection_url(
                monitor_fixture.lookup.UPDATES_ENDPOINT
            )
            == url1
        )

        # The second URL processed was whatever we saw in the 'next' link.
        assert "http://next-link/" == url2

        # Since that URL didn't contain any new imports, we didn't process
        # its 'next' link, http://another-next-link/.

    def test_no_changes_means_no_timestamp_update(
        self, monitor_fixture: MonitorFixture
    ):
        before = utc_now()
        monitor_fixture.monitor.timestamp().finish = before

        # We're going to ask the metadata wrangler for updates, but
        # there will be none -- not even a feed-level update
        data = monitor_fixture.files.sample_data(
            "metadata_updates_empty_response_no_feed_timestamp.opds"
        )
        monitor_fixture.lookup.queue_response(
            200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )
        new_timestamp = monitor_fixture.monitor.run_once(monitor_fixture.ts)

        # run_once() returned a TimestampData referencing the original
        # timestamp, and the Timestamp object was not updated.
        assert before == new_timestamp.finish
        assert before == monitor_fixture.monitor.timestamp().finish

        # If timestamp.finish is None before the update is run, and
        # there are no updates, the timestamp will be set
        # to None.
        monitor_fixture.monitor.timestamp().finish = None
        monitor_fixture.lookup.queue_response(
            200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )
        new_timestamp = monitor_fixture.monitor.run_once(monitor_fixture.ts)
        assert Timestamp.CLEAR_VALUE == new_timestamp.finish

    def test_no_import_loop(self, monitor_fixture: MonitorFixture):
        # We stop processing a feed's 'next' link if it links to a URL we've
        # already seen.

        data = monitor_fixture.files.sample_data("metadata_updates_response.opds")
        monitor_fixture.lookup.queue_response(
            200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )
        data = data.replace(b"http://next-link/", b"http://different-link/")
        monitor_fixture.lookup.queue_response(
            200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        # This introduces a loop.
        data = data.replace(b"http://next-link/", b"http://next-link/")
        monitor_fixture.lookup.queue_response(
            200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )
        new_timestamp = monitor_fixture.monitor.run_once(monitor_fixture.ts)

        # Even though all these pages had the same content, we kept
        # processing them until we encountered a 'next' link we had
        # seen before; then we stopped.
        first, second, third = monitor_fixture.monitor.imports
        assert (None, None) == first
        assert (None, "http://next-link/") == second
        assert (None, "http://different-link/") == third

        assert datetime_utc(2016, 9, 20, 19, 37, 2) == new_timestamp.finish

    def test_get_response(self, monitor_fixture: MonitorFixture):
        db = monitor_fixture.db

        class Mock(MockMetadataWranglerOPDSLookup):
            def __init__(self):
                self.last_timestamp = None
                self.urls = []

            def updates(self, timestamp):
                self.last_timestamp = timestamp
                return MockRequestsResponse(
                    200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}
                )

            def _get(self, _url):
                self.urls.append(_url)
                return MockRequestsResponse(
                    200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}
                )

        # If you pass in None for the URL, it passes the timestamp into
        # updates()
        lookup = Mock()
        monitor = MWCollectionUpdateMonitor(
            db.session, monitor_fixture.collection, lookup
        )
        timestamp = object()
        response = monitor.get_response(timestamp=timestamp, url=None)
        assert 200 == response.status_code
        assert timestamp == lookup.last_timestamp
        assert [] == lookup.urls

        # If you pass in a URL, the timestamp is ignored and
        # the URL is passed into _get().
        lookup = Mock()
        monitor = MWCollectionUpdateMonitor(
            db.session, monitor_fixture.collection, lookup
        )
        response = monitor.get_response(timestamp=None, url="http://now used/")
        assert 200 == response.status_code
        assert None == lookup.last_timestamp
        assert ["http://now used/"] == lookup.urls


class AuxMonitorFixture:
    def __init__(
        self,
        db: DatabaseTransactionFixture,
        api_opds_files_fixture: OPDSAPIFilesFixture,
    ):
        self.db = db
        self.files = api_opds_files_fixture
        self.db.external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            ExternalIntegration.METADATA_GOAL,
            username="abc",
            password="def",
            url=self.db.fresh_url(),
        )
        self.collection = self.db.collection(
            protocol=ExternalIntegration.OVERDRIVE, external_account_id="lib"
        )
        self.lookup = MockMetadataWranglerOPDSLookup.from_config(
            self.db.session, self.collection
        )
        provider = AlwaysSuccessfulCoverageProvider(self.db.session)
        self.monitor = MWAuxiliaryMetadataMonitor(
            self.db.session, self.collection, lookup=self.lookup, provider=provider
        )

    @property
    def ts(self):
        """Make the timestamp used by run() when calling run_once().
        This makes it easier to test run_once() in isolation.
        """
        return self.monitor.timestamp().to_data()


@pytest.fixture(scope="function")
def aux_monitor_fixture(
    db: DatabaseTransactionFixture, api_opds_files_fixture: OPDSAPIFilesFixture
) -> AuxMonitorFixture:
    return AuxMonitorFixture(db, api_opds_files_fixture)


class TestMWAuxiliaryMetadataMonitor:
    def test_monitor_requires_authentication(
        self, aux_monitor_fixture: AuxMonitorFixture
    ):
        db = aux_monitor_fixture.db

        class Mock:
            authenticated = False

        aux_monitor_fixture.monitor.lookup = Mock()
        with pytest.raises(Exception) as excinfo:
            aux_monitor_fixture.monitor.run_once(aux_monitor_fixture.ts)
        assert "no authentication credentials" in str(excinfo.value)

    @staticmethod
    def prep_feed_identifiers(aux_monitor_fixture: AuxMonitorFixture):
        db = aux_monitor_fixture.db
        ignored = db.identifier()

        # Create an Overdrive ID to match the one in the feed.
        overdrive = db.identifier(
            identifier_type=Identifier.OVERDRIVE_ID,
            foreign_id="4981c34f-d518-48ff-9659-2601b2b9bdc1",
        )

        # Create an ISBN to match the one in the feed.
        isbn = db.identifier(
            identifier_type=Identifier.ISBN, foreign_id="9781602835740"
        )

        # Create a Axis 360 ID equivalent to the other ISBN in the feed.
        axis_360 = db.identifier(
            identifier_type=Identifier.AXIS_360_ID, foreign_id="fake"
        )
        axis_360_isbn = db.identifier(
            identifier_type=Identifier.ISBN, foreign_id="9781569478295"
        )
        axis_source = DataSource.lookup(db.session, DataSource.AXIS_360)
        axis_360.equivalent_to(axis_source, axis_360_isbn, 1)
        db.session.commit()

        # Put all the identifiers in the collection.
        for identifier in [overdrive, isbn, axis_360]:
            db.edition(
                data_source_name=axis_source.name,
                with_license_pool=True,
                identifier_type=identifier.type,
                identifier_id=identifier.identifier,
                collection=aux_monitor_fixture.collection,
            )

        return overdrive, isbn, axis_360

    def test_get_identifiers(self, aux_monitor_fixture: AuxMonitorFixture):
        overdrive, isbn, axis_360 = self.prep_feed_identifiers(aux_monitor_fixture)
        data = aux_monitor_fixture.files.sample_data(
            "metadata_data_needed_response.opds"
        )
        aux_monitor_fixture.lookup.queue_response(
            200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )
        identifiers, next_links = aux_monitor_fixture.monitor.get_identifiers()

        # The expected identifiers are returned, including the mapped axis_360
        # identifier.
        assert sorted([overdrive, axis_360, isbn]) == sorted(identifiers)
        assert ["http://next-link"] == next_links

    def test_run_once(self, aux_monitor_fixture: AuxMonitorFixture):
        db = aux_monitor_fixture.db
        overdrive, isbn, axis_360 = self.prep_feed_identifiers(aux_monitor_fixture)

        # Give one of the identifiers a full work.
        db.work(presentation_edition=overdrive.primarily_identifies[0])
        # And another identifier a work without entries.
        w = db.work(presentation_edition=isbn.primarily_identifies[0])
        w.simple_opds_entry = w.verbose_opds_entry = None

        # Queue some response feeds.
        feed1 = aux_monitor_fixture.files.sample_data(
            "metadata_data_needed_response.opds"
        )
        feed2 = aux_monitor_fixture.files.sample_data(
            "metadata_data_needed_empty_response.opds"
        )
        for feed in [feed1, feed2]:
            aux_monitor_fixture.lookup.queue_response(
                200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, feed
            )

        progress = aux_monitor_fixture.monitor.run_once(aux_monitor_fixture.ts)

        # Only the identifier with a work has been given coverage.
        assert "Identifiers processed: 1" == progress.achievements

        # The TimestampData returned by run_once() does not include
        # any timing information -- that will be applied by run().
        assert None == progress.start
        assert None == progress.finish

        record = CoverageRecord.lookup(
            overdrive,
            aux_monitor_fixture.monitor.provider.data_source,
            operation=aux_monitor_fixture.monitor.provider.operation,
        )
        assert record

        for identifier in [axis_360, isbn]:
            record = CoverageRecord.lookup(
                identifier,
                aux_monitor_fixture.monitor.provider.data_source,
                operation=aux_monitor_fixture.monitor.provider.operation,
            )
            assert None == record


class MetadataWranglerCoverageFixture:

    db: DatabaseTransactionFixture
    integration: ExternalIntegration
    source: DataSource
    collection: Collection
    lookup: MetadataWranglerOPDSLookup
    provider: Any
    lookup_client: Any
    files: OPDSAPIFilesFixture

    def __init__(
        self,
        db: DatabaseTransactionFixture,
        api_opds_files_fixture: OPDSAPIFilesFixture,
    ):
        self.db = db
        self.files = api_opds_files_fixture
        self.integration = self.db.external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL,
            url=self.db.fresh_url(),
            username="abc",
            password="def",
        )
        self.source = DataSource.lookup(self.db.session, DataSource.METADATA_WRANGLER)
        self.collection = self.db.collection(
            protocol=ExternalIntegration.BIBLIOTHECA, external_account_id="lib"
        )

    def create_provider(self, cls, **kwargs):
        self.lookup = MockMetadataWranglerOPDSLookup.from_config(
            self.db.session, self.collection
        )
        self.provider = cls(self.collection, self.lookup, **kwargs)
        self.lookup_client = self.provider.lookup_client
        return self.provider

    def opds_feed_identifiers(self):
        """Creates three Identifiers to use for testing with sample OPDS files."""

        # An identifier directly represented in the OPDS response.
        valid_id = self.db.identifier(foreign_id="2020110")

        # An identifier mapped to an identifier represented in the OPDS
        # response.
        source = DataSource.lookup(self.db.session, DataSource.AXIS_360)
        mapped_id = self.db.identifier(
            identifier_type=Identifier.AXIS_360_ID, foreign_id="0015187876"
        )
        equivalent_id = self.db.identifier(
            identifier_type=Identifier.ISBN, foreign_id="9781936460236"
        )
        mapped_id.equivalent_to(source, equivalent_id, 1)

        # An identifier that's not represented in the OPDS response.
        lost_id = self.db.identifier()
        return valid_id, mapped_id, lost_id


@pytest.fixture(scope="function")
def wrangler_coverage_fixture(
    db: DatabaseTransactionFixture, api_opds_files_fixture: OPDSAPIFilesFixture
) -> MetadataWranglerCoverageFixture:
    return MetadataWranglerCoverageFixture(db, api_opds_files_fixture)


class TestBaseMetadataWranglerCoverageProvider:
    class Mock(BaseMetadataWranglerCoverageProvider):
        SERVICE_NAME = "Mock"
        DATA_SOURCE_NAME = DataSource.OVERDRIVE

    @classmethod
    @pytest.fixture(scope="function")
    def mock_coverage_fixture(
        cls, wrangler_coverage_fixture: MetadataWranglerCoverageFixture
    ) -> MetadataWranglerCoverageFixture:
        wrangler_coverage_fixture.create_provider(
            TestBaseMetadataWranglerCoverageProvider.Mock
        )
        return wrangler_coverage_fixture

    def test_must_be_authenticated(
        self, wrangler_coverage_fixture: MetadataWranglerCoverageFixture
    ):
        """CannotLoadConfiguration is raised if you try to create a
        metadata wrangler coverage provider that can't authenticate
        with the metadata wrangler.
        """
        wrangler_coverage_fixture.create_provider(self.Mock)

        class UnauthenticatedLookupClient:
            authenticated = False

        with pytest.raises(CannotLoadConfiguration) as excinfo:
            self.Mock(
                wrangler_coverage_fixture.collection, UnauthenticatedLookupClient()
            )
        assert (
            "Authentication for the Palace Collection Manager Metadata Wrangler "
            in str(excinfo.value)
        )

    def test_input_identifier_types(self):
        """Verify all the different types of identifiers we send
        to the metadata wrangler.
        """
        assert {
            Identifier.OVERDRIVE_ID,
            Identifier.BIBLIOTHECA_ID,
            Identifier.AXIS_360_ID,
            Identifier.URI,
        } == set(BaseMetadataWranglerCoverageProvider.INPUT_IDENTIFIER_TYPES)

    def test_create_identifier_mapping(
        self, wrangler_coverage_fixture: MetadataWranglerCoverageFixture
    ):
        db = wrangler_coverage_fixture.db
        wrangler_coverage_fixture.create_provider(self.Mock)

        # Most identifiers map to themselves.
        overdrive = db.identifier(Identifier.OVERDRIVE_ID)

        # But Axis 360 and 3M identifiers map to equivalent ISBNs.
        axis = db.identifier(Identifier.AXIS_360_ID)
        threem = db.identifier(Identifier.THREEM_ID)
        isbn_axis = db.identifier(Identifier.ISBN)
        isbn_threem = db.identifier(Identifier.ISBN)

        who_says = DataSource.lookup(db.session, DataSource.AXIS_360)

        axis.equivalent_to(who_says, isbn_axis, 1)
        threem.equivalent_to(who_says, isbn_threem, 1)

        mapping = wrangler_coverage_fixture.provider.create_identifier_mapping(
            [overdrive, axis, threem]
        )
        assert overdrive == mapping[overdrive]
        assert axis == mapping[isbn_axis]
        assert threem == mapping[isbn_threem]

    def test_coverage_records_for_unhandled_items_include_collection(
        self, wrangler_coverage_fixture: MetadataWranglerCoverageFixture
    ):
        db = wrangler_coverage_fixture.db
        wrangler_coverage_fixture.create_provider(self.Mock)

        # NOTE: This could be made redundant by adding test coverage to
        # CoverageProvider.process_batch_and_handle_results in core.
        data = wrangler_coverage_fixture.files.sample_data(
            "metadata_sync_response.opds"
        )
        wrangler_coverage_fixture.lookup_client.queue_response(
            200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        identifier = db.identifier()
        wrangler_coverage_fixture.provider.process_batch_and_handle_results(
            [identifier]
        )
        [record] = identifier.coverage_records
        assert CoverageRecord.TRANSIENT_FAILURE == record.status
        assert wrangler_coverage_fixture.provider.data_source == record.data_source
        assert wrangler_coverage_fixture.provider.operation == record.operation
        assert wrangler_coverage_fixture.provider.collection == record.collection


@pytest.fixture(scope="function")
def collection_registrar_fixture(
    wrangler_coverage_fixture: MetadataWranglerCoverageFixture,
) -> MetadataWranglerCoverageFixture:
    wrangler_coverage_fixture.create_provider(MetadataWranglerCollectionRegistrar)
    return wrangler_coverage_fixture


class TestMetadataWranglerCollectionRegistrar:
    def test_constants(
        self, collection_registrar_fixture: MetadataWranglerCoverageFixture
    ):
        # This CoverageProvider runs Identifiers through the 'lookup'
        # endpoint and marks success with CoverageRecords that have
        # the IMPORT_OPERATION operation.
        assert (
            collection_registrar_fixture.provider.lookup_client.lookup
            == collection_registrar_fixture.provider.api_method
        )
        assert (
            CoverageRecord.IMPORT_OPERATION
            == MetadataWranglerCollectionRegistrar.OPERATION
        )

    def test_process_batch(
        self, collection_registrar_fixture: MetadataWranglerCoverageFixture
    ):
        """End-to-end test of the registrar's process_batch() implementation."""
        data = collection_registrar_fixture.files.sample_data(
            "metadata_sync_response.opds"
        )
        collection_registrar_fixture.lookup_client.queue_response(
            200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        (
            valid_id,
            mapped_id,
            lost_id,
        ) = collection_registrar_fixture.opds_feed_identifiers()
        results = collection_registrar_fixture.provider.process_batch(
            [valid_id, mapped_id, lost_id]
        )

        # The Identifier that resulted in a 200 message was returned.
        #
        # The Identifier that resulted in a 201 message was returned.
        #
        # The Identifier that was ignored by the server was not
        # returned.
        #
        # The Identifier that was not requested but was sent back by
        # the server anyway was ignored.
        assert sorted([valid_id, mapped_id]) == sorted(results)

    def test_process_batch_errors(
        self, collection_registrar_fixture: MetadataWranglerCoverageFixture
    ):
        """When errors are raised during batch processing, an exception is
        raised and no CoverageRecords are created.
        """
        db = collection_registrar_fixture.db

        # This happens if the 'server' sends data with the wrong media
        # type.
        collection_registrar_fixture.lookup_client.queue_response(
            200, {"content-type": "json/application"}, '{ "title": "It broke." }'
        )

        id1 = db.identifier()
        id2 = db.identifier()
        with pytest.raises(BadResponseException) as excinfo:
            collection_registrar_fixture.provider.process_batch([id1, id2])
        assert "Wrong media type" in str(excinfo.value)
        assert [] == id1.coverage_records
        assert [] == id2.coverage_records

        # Of if the 'server' sends an error response code.
        collection_registrar_fixture.lookup_client.queue_response(
            500,
            {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE},
            "Internal Server Error",
        )
        with pytest.raises(BadResponseException) as excinfo:
            collection_registrar_fixture.provider.process_batch([id1, id2])
        assert "Got status code 500" in str(excinfo.value)
        assert [] == id1.coverage_records
        assert [] == id2.coverage_records

        # If a message comes back with an unexpected status, a
        # CoverageFailure is created.
        data = collection_registrar_fixture.files.sample_data(
            "unknown_message_status_code.opds"
        )
        valid_id = collection_registrar_fixture.opds_feed_identifiers()[0]
        collection_registrar_fixture.lookup_client.queue_response(
            200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )
        [result] = collection_registrar_fixture.provider.process_batch([valid_id])
        assert True == isinstance(result, CoverageFailure)
        assert valid_id == result.obj
        assert "418: Mad Hatter" == result.exception

        # The OPDS importer didn't know which Collection to associate
        # with this CoverageFailure, but the CoverageProvider does,
        # and it set .collection appropriately.
        assert collection_registrar_fixture.provider.collection == result.collection

    def test_items_that_need_coverage_excludes_unavailable_items(
        self, collection_registrar_fixture: MetadataWranglerCoverageFixture
    ):
        """A LicensePool that's not actually available doesn't need coverage."""
        db = collection_registrar_fixture.db
        edition, pool = db.edition(
            with_license_pool=True,
            collection=collection_registrar_fixture.collection,
            identifier_type=Identifier.BIBLIOTHECA_ID,
        )
        pool.licenses_owned = 0
        assert (
            0
            == collection_registrar_fixture.provider.items_that_need_coverage().count()
        )

        # Open-access titles _do_ need coverage.
        pool.open_access = True
        assert [
            pool.identifier
        ] == collection_registrar_fixture.provider.items_that_need_coverage().all()

    def test_items_that_need_coverage_removes_reap_records_for_relicensed_items(
        self, collection_registrar_fixture: MetadataWranglerCoverageFixture
    ):
        """A LicensePool that's not actually available doesn't need coverage."""
        db = collection_registrar_fixture.db
        edition, pool = db.edition(
            with_license_pool=True,
            collection=collection_registrar_fixture.collection,
            identifier_type=Identifier.BIBLIOTHECA_ID,
        )

        identifier = pool.identifier
        original_coverage_records = list(identifier.coverage_records)

        # This identifier was reaped...
        cr = db.coverage_record(
            pool.identifier,
            collection_registrar_fixture.provider.data_source,
            operation=CoverageRecord.REAP_OPERATION,
            collection=collection_registrar_fixture.collection,
        )
        assert set(original_coverage_records + [cr]) == set(identifier.coverage_records)

        # ... but then it was relicensed.
        pool.licenses_owned = 10

        assert [
            identifier
        ] == collection_registrar_fixture.provider.items_that_need_coverage().all()

        # The now-inaccurate REAP record has been removed.
        assert original_coverage_records == identifier.coverage_records

    def test_identifier_covered_in_one_collection_not_covered_in_another(
        self, collection_registrar_fixture: MetadataWranglerCoverageFixture
    ):
        db = collection_registrar_fixture.db
        edition, pool = db.edition(
            with_license_pool=True,
            collection=collection_registrar_fixture.collection,
            identifier_type=Identifier.BIBLIOTHECA_ID,
        )

        identifier = pool.identifier
        other_collection = db.collection()

        # This Identifier needs coverage.
        qu = collection_registrar_fixture.provider.items_that_need_coverage()
        assert [identifier] == qu.all()

        # Adding coverage for an irrelevant collection won't fix that.
        cr = db.coverage_record(
            pool.identifier,
            collection_registrar_fixture.provider.data_source,
            operation=collection_registrar_fixture.provider.OPERATION,
            collection=other_collection,
        )
        assert [identifier] == qu.all()

        # Adding coverage for the relevant collection will.
        cr = db.coverage_record(
            pool.identifier,
            collection_registrar_fixture.provider.data_source,
            operation=collection_registrar_fixture.provider.OPERATION,
            collection=collection_registrar_fixture.provider.collection,
        )
        assert [] == qu.all()

    def test_identifier_reaped_from_one_collection_covered_in_another(
        self, collection_registrar_fixture: MetadataWranglerCoverageFixture
    ):
        """An Identifier can be reaped from one collection but still
        need coverage in another.
        """
        db = collection_registrar_fixture.db
        edition, pool = db.edition(
            with_license_pool=True,
            collection=collection_registrar_fixture.collection,
            identifier_type=Identifier.BIBLIOTHECA_ID,
        )

        identifier = pool.identifier
        other_collection = db.collection()

        # This identifier was reaped from other_collection, but not
        # from self.provider.collection.
        cr = db.coverage_record(
            pool.identifier,
            collection_registrar_fixture.provider.data_source,
            operation=CoverageRecord.REAP_OPERATION,
            collection=other_collection,
        )

        # It still needs to be covered in self.provider.collection.
        assert [
            identifier
        ] == collection_registrar_fixture.provider.items_that_need_coverage().all()

    def test_items_that_need_coverage_respects_cutoff(
        self, collection_registrar_fixture: MetadataWranglerCoverageFixture
    ):
        """Verify that this coverage provider respects the cutoff_time
        argument.
        """

        db = collection_registrar_fixture.db
        edition, pool = db.edition(
            with_license_pool=True,
            collection=collection_registrar_fixture.collection,
            identifier_type=Identifier.BIBLIOTHECA_ID,
        )
        cr = db.coverage_record(
            pool.identifier,
            collection_registrar_fixture.provider.data_source,
            operation=collection_registrar_fixture.provider.OPERATION,
            collection=collection_registrar_fixture.collection,
        )

        # We have a coverage record already, so this book doesn't show
        # up in items_that_need_coverage
        items = collection_registrar_fixture.provider.items_that_need_coverage().all()
        assert [] == items

        # But if we send a cutoff_time that's later than the time
        # associated with the coverage record...
        one_hour_from_now = utc_now() + datetime.timedelta(seconds=3600)
        provider_with_cutoff = collection_registrar_fixture.create_provider(
            cls=MetadataWranglerCollectionRegistrar, cutoff_time=one_hour_from_now
        )

        # The book starts showing up in items_that_need_coverage.
        assert [
            pool.identifier
        ] == provider_with_cutoff.items_that_need_coverage().all()

    def test_items_that_need_coverage_respects_count_as_covered(
        self, collection_registrar_fixture: MetadataWranglerCoverageFixture
    ):
        db = collection_registrar_fixture.db

        # Here's a coverage record with a transient failure.
        edition, pool = db.edition(
            with_license_pool=True,
            collection=collection_registrar_fixture.collection,
            identifier_type=Identifier.OVERDRIVE_ID,
        )
        cr = db.coverage_record(
            pool.identifier,
            collection_registrar_fixture.provider.data_source,
            operation=collection_registrar_fixture.provider.operation,
            status=CoverageRecord.TRANSIENT_FAILURE,
            collection=collection_registrar_fixture.collection,
        )

        # Ordinarily, a transient failure does not count as coverage.
        [
            needs_coverage
        ] = collection_registrar_fixture.provider.items_that_need_coverage().all()
        assert needs_coverage == pool.identifier

        # But if we say that transient failure counts as coverage, it
        # does count.
        assert (
            []
            == collection_registrar_fixture.provider.items_that_need_coverage(
                count_as_covered=CoverageRecord.TRANSIENT_FAILURE
            ).all()
        )

    def test_isbn_covers_are_imported_from_mapped_identifiers(
        self, collection_registrar_fixture: MetadataWranglerCoverageFixture
    ):
        db = collection_registrar_fixture.db

        # Now that we pass ISBN equivalents instead of Bibliotheca identifiers
        # to the Metadata Wrangler, they're not getting covers. Let's confirm
        # that the problem isn't on the Circulation Manager import side of things.

        # Create a Bibliotheca identifier with a license pool.
        source = DataSource.lookup(db.session, DataSource.BIBLIOTHECA)
        identifier = db.identifier(identifier_type=Identifier.BIBLIOTHECA_ID)
        LicensePool.for_foreign_id(
            db.session,
            source,
            identifier.type,
            identifier.identifier,
            collection=collection_registrar_fixture.provider.collection,
        )

        # Create an ISBN and set it equivalent.
        isbn = db.identifier(identifier_type=Identifier.ISBN)
        isbn.identifier = "9781594632556"
        identifier.equivalent_to(source, isbn, 1)

        opds = collection_registrar_fixture.files.sample_data(
            "metadata_isbn_response.opds"
        )
        collection_registrar_fixture.provider.lookup_client.queue_response(
            200,
            {
                "content-type": "application/atom+xml;profile=opds-catalog;kind=acquisition"
            },
            opds,
        )

        result = collection_registrar_fixture.provider.process_item(identifier)
        # The lookup is successful
        assert result == identifier
        # The appropriate cover links are transferred.
        identifier_uris = [
            l.resource.url
            for l in identifier.links
            if l.rel in [Hyperlink.IMAGE, Hyperlink.THUMBNAIL_IMAGE]
        ]
        expected = [
            "http://book-covers.nypl.org/Content%20Cafe/ISBN/9781594632556/cover.jpg",
            "http://book-covers.nypl.org/scaled/300/Content%20Cafe/ISBN/9781594632556/cover.jpg",
        ]

        assert sorted(identifier_uris) == sorted(expected)

        # The ISBN doesn't get any information.
        assert isbn.links == []


class MetadataWranglerCollectionManagerFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.integration = self.db.external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL,
            url=self.db.fresh_url(),
            username="abc",
            password="def",
        )
        self.source = DataSource.lookup(db.session, DataSource.METADATA_WRANGLER)
        self.collection = self.db.collection(
            protocol=ExternalIntegration.BIBLIOTHECA, external_account_id="lib"
        )
        self.lookup = MockMetadataWranglerOPDSLookup.from_config(
            db.session, collection=self.collection
        )


@pytest.fixture(scope="function")
def collection_reaper_fixture(
    wrangler_coverage_fixture: MetadataWranglerCoverageFixture,
) -> MetadataWranglerCoverageFixture:
    wrangler_coverage_fixture.create_provider(MetadataWranglerCollectionReaper)
    return wrangler_coverage_fixture


class TestMetadataWranglerCollectionReaper:
    def test_constants(
        self, collection_reaper_fixture: MetadataWranglerCoverageFixture
    ):
        # This CoverageProvider runs Identifiers through the 'remove'
        # endpoint and marks success with CoverageRecords that have
        # the REAP_OPERATION operation.
        assert (
            CoverageRecord.REAP_OPERATION == MetadataWranglerCollectionReaper.OPERATION
        )
        assert (
            collection_reaper_fixture.provider.lookup_client.remove
            == collection_reaper_fixture.provider.api_method
        )

    def test_items_that_need_coverage(
        self, collection_reaper_fixture: MetadataWranglerCoverageFixture
    ):
        """The reaper only returns identifiers with no-longer-licensed
        license_pools that have been synced with the Metadata
        Wrangler.
        """
        db = collection_reaper_fixture.db
        # Create an item that was imported into the Wrangler-side
        # collection but no longer has any owned licenses
        covered_unlicensed_lp = db.licensepool(
            None,
            open_access=False,
            set_edition_as_presentation=True,
            collection=collection_reaper_fixture.collection,
        )
        covered_unlicensed_lp.update_availability(0, 0, 0, 0)
        cr = db.coverage_record(
            covered_unlicensed_lp.presentation_edition,
            collection_reaper_fixture.source,
            operation=CoverageRecord.IMPORT_OPERATION,
            collection=collection_reaper_fixture.provider.collection,
        )

        # Create an unsynced item that doesn't have any licenses
        uncovered_unlicensed_lp = db.licensepool(None, open_access=False)
        uncovered_unlicensed_lp.update_availability(0, 0, 0, 0)

        # And an unsynced item that has licenses.
        licensed_lp = db.licensepool(None, open_access=False)

        # Create an open access license pool
        open_access_lp = db.licensepool(None)

        items = collection_reaper_fixture.provider.items_that_need_coverage().all()
        assert 1 == len(items)

        # Items that are licensed are ignored.
        assert licensed_lp.identifier not in items

        # Items with open access license pools are ignored.
        assert open_access_lp.identifier not in items

        # Items that haven't been synced with the Metadata Wrangler are
        # ignored, even if they don't have licenses.
        assert uncovered_unlicensed_lp.identifier not in items

        # Only synced items without owned licenses are returned.
        assert [covered_unlicensed_lp.identifier] == items

        # Items that had unsuccessful syncs are not returned.
        cr.status = CoverageRecord.TRANSIENT_FAILURE
        assert [] == collection_reaper_fixture.provider.items_that_need_coverage().all()

    def test_process_batch(
        self, collection_reaper_fixture: MetadataWranglerCoverageFixture
    ):
        data = collection_reaper_fixture.files.sample_data(
            "metadata_reaper_response.opds"
        )
        collection_reaper_fixture.lookup_client.queue_response(
            200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, data
        )

        valid_id, mapped_id, lost_id = collection_reaper_fixture.opds_feed_identifiers()
        results = collection_reaper_fixture.provider.process_batch(
            [valid_id, mapped_id, lost_id]
        )

        # The valid_id and mapped_id were handled successfully.
        # The server ignored lost_id, so nothing happened to it,
        # and the server sent a fourth ID we didn't ask for,
        # which we ignored.
        assert sorted(results) == sorted([valid_id, mapped_id])

    def test_finalize_batch(
        self, collection_reaper_fixture: MetadataWranglerCoverageFixture
    ):
        db = collection_reaper_fixture.db

        # Metadata Wrangler sync coverage records are deleted from the db
        # when the the batch is finalized if the item has been reaped.

        # Create an identifier that has been imported and one that's
        # been reaped.
        sync_cr = db.coverage_record(
            db.edition(),
            collection_reaper_fixture.source,
            operation=CoverageRecord.IMPORT_OPERATION,
            collection=collection_reaper_fixture.provider.collection,
        )
        reaped_cr = db.coverage_record(
            db.edition(),
            collection_reaper_fixture.source,
            operation=CoverageRecord.REAP_OPERATION,
            collection=collection_reaper_fixture.provider.collection,
        )

        # Create coverage records for an Identifier that has been both synced
        # and reaped.
        doubly_covered = db.edition()
        doubly_sync_record = db.coverage_record(
            doubly_covered,
            collection_reaper_fixture.source,
            operation=CoverageRecord.IMPORT_OPERATION,
            collection=collection_reaper_fixture.provider.collection,
        )
        doubly_reap_record = db.coverage_record(
            doubly_covered,
            collection_reaper_fixture.source,
            operation=CoverageRecord.REAP_OPERATION,
            collection=collection_reaper_fixture.provider.collection,
        )

        collection_reaper_fixture.provider.finalize_batch()
        remaining_records = db.session.query(CoverageRecord).all()

        # The syncing record has been deleted from the database
        assert doubly_sync_record not in remaining_records
        assert sorted(
            [sync_cr, reaped_cr, doubly_reap_record], key=lambda x: x.id
        ) == sorted(remaining_records, key=lambda x: x.id)


class MetadataUploadCoverageFixture:
    def create_provider(self, **kwargs):
        upload_client = MockMetadataWranglerOPDSLookup.from_config(
            self.db.session, self.collection
        )
        return MetadataUploadCoverageProvider(self.collection, upload_client, **kwargs)

    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.integration = self.db.external_integration(
            ExternalIntegration.METADATA_WRANGLER,
            goal=ExternalIntegration.METADATA_GOAL,
            url=self.db.fresh_url(),
            username="abc",
            password="def",
        )
        self.source = DataSource.lookup(db.session, DataSource.METADATA_WRANGLER)
        self.collection = self.db.collection(
            protocol=ExternalIntegration.BIBLIOTHECA, external_account_id="lib"
        )
        self.provider = self.create_provider()


@pytest.fixture(scope="function")
def metadata_upload_coverage_fixture(
    db: DatabaseTransactionFixture,
) -> MetadataUploadCoverageFixture:
    return MetadataUploadCoverageFixture(db)


class TestMetadataUploadCoverageProvider:
    def test_items_that_need_coverage_only_finds_transient_failures(
        self, metadata_upload_coverage_fixture: MetadataUploadCoverageFixture
    ):
        """Verify that this coverage provider only covers items that have
        transient failure CoverageRecords.
        """
        db = metadata_upload_coverage_fixture.db

        edition, pool = db.edition(
            with_license_pool=True,
            collection=metadata_upload_coverage_fixture.collection,
            identifier_type=Identifier.BIBLIOTHECA_ID,
        )
        # We don't have a CoverageRecord yet, so the book doesn't show up.
        items = (
            metadata_upload_coverage_fixture.provider.items_that_need_coverage().all()
        )
        assert [] == items

        cr = db.coverage_record(
            pool.identifier,
            metadata_upload_coverage_fixture.provider.data_source,
            operation=metadata_upload_coverage_fixture.provider.OPERATION,
            collection=metadata_upload_coverage_fixture.collection,
        )

        # With a successful or persistent failure CoverageRecord, it still doesn't show up.
        cr.status = CoverageRecord.SUCCESS
        items = (
            metadata_upload_coverage_fixture.provider.items_that_need_coverage().all()
        )
        assert [] == items

        cr.status = CoverageRecord.PERSISTENT_FAILURE
        items = (
            metadata_upload_coverage_fixture.provider.items_that_need_coverage().all()
        )
        assert [] == items

        # But with a transient failure record it does.
        cr.status = CoverageRecord.TRANSIENT_FAILURE
        items = (
            metadata_upload_coverage_fixture.provider.items_that_need_coverage().all()
        )
        assert [edition.primary_identifier] == items

    def test_process_batch_uploads_metadata(
        self, metadata_upload_coverage_fixture: MetadataUploadCoverageFixture
    ):
        db = metadata_upload_coverage_fixture.db

        class MockMetadataClient:
            metadata_feed = None
            authenticated = True

            def canonicalize_author_name(self, identifier, working_display_name):
                return working_display_name

            def add_with_metadata(self, feed):
                self.metadata_feed = feed

        metadata_client = MockMetadataClient()

        provider = MetadataUploadCoverageProvider(
            metadata_upload_coverage_fixture.collection, metadata_client
        )

        edition, pool = db.edition(
            with_license_pool=True,
            collection=metadata_upload_coverage_fixture.collection,
            identifier_type=Identifier.BIBLIOTHECA_ID,
        )
        work = pool.calculate_work()

        # This identifier has no Work.
        no_work = db.identifier()

        results = provider.process_batch([pool.identifier, no_work])

        # An OPDS feed of metadata was sent to the metadata wrangler.
        assert metadata_client.metadata_feed != None
        feed = feedparser.parse(str(metadata_client.metadata_feed))
        urns = [entry.get("id") for entry in feed.get("entries", [])]
        # Only the identifier work a work ends up in the feed.
        assert [pool.identifier.urn] == urns

        # There are two results: the identifier with a work and a CoverageFailure.
        assert 2 == len(results)
        assert pool.identifier in results
        [failure] = [r for r in results if isinstance(r, CoverageFailure)]
        assert no_work == failure.obj

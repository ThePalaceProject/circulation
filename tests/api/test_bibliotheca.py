from __future__ import annotations

import json
import random
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Optional,
    Protocol,
    runtime_checkable,
)
from unittest import mock
from unittest.mock import MagicMock

import pytest
from pymarc import parse_xml_to_array
from pymarc.record import Record  # type: ignore

from api.bibliotheca import (
    BibliothecaAPI,
    BibliothecaBibliographicCoverageProvider,
    BibliothecaCirculationSweep,
    BibliothecaEventMonitor,
    BibliothecaParser,
    BibliothecaPurchaseMonitor,
    CheckoutResponseParser,
    ErrorParser,
    EventParser,
    ItemListParser,
    PatronCirculationParser,
)
from api.circulation import CirculationAPI, FulfillmentInfo, HoldInfo, LoanInfo
from api.circulation_exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    CannotHold,
    CurrentlyAvailable,
    NoAvailableCopies,
    NoLicenses,
    NotCheckedOut,
    NotOnHold,
    PatronHoldLimitReached,
    PatronLoanLimitReached,
    RemoteInitiatedServerError,
)
from api.web_publication_manifest import FindawayManifest
from core.metadata_layer import ReplacementPolicy, TimestampData
from core.mock_analytics_provider import MockAnalyticsProvider
from core.model import (
    CirculationEvent,
    Contributor,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    LicensePool,
    Measurement,
    Representation,
    Subject,
    Timestamp,
    Work,
    WorkCoverageRecord,
    create,
)
from core.scripts import RunCollectionCoverageProviderScript
from core.util.datetime_helpers import datetime_utc, utc_now
from core.util.http import BadResponseException
from core.util.problem_detail import ProblemDetail
from core.util.web_publication_manifest import AudiobookManifest
from tests.api.mockapi.bibliotheca import MockBibliothecaAPI

if TYPE_CHECKING:
    from tests.fixtures.api_bibliotheca_files import BibliothecaFilesFixture
    from tests.fixtures.authenticator import AuthProviderFixture
    from tests.fixtures.database import DatabaseTransactionFixture
    from tests.fixtures.time import Time


class BibliothecaAPITestFixture:
    def __init__(self, db: DatabaseTransactionFixture, files: BibliothecaFilesFixture):
        self.files = files
        self.db = db
        self.collection = MockBibliothecaAPI.mock_collection(db.session)
        self.api = MockBibliothecaAPI(db.session, self.collection)


@pytest.fixture(scope="function")
def bibliotheca_fixture(
    db: DatabaseTransactionFixture,
    api_bibliotheca_files_fixture: BibliothecaFilesFixture,
) -> BibliothecaAPITestFixture:
    return BibliothecaAPITestFixture(db, api_bibliotheca_files_fixture)


class TestBibliothecaAPI:
    def test_external_integration(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        assert (
            bibliotheca_fixture.collection.external_integration
            == bibliotheca_fixture.api.external_integration(object())
        )

    def test__run_self_tests(
        self,
        bibliotheca_fixture: BibliothecaAPITestFixture,
        create_simple_auth_integration: Callable[..., AuthProviderFixture],
    ):
        db = bibliotheca_fixture.db
        # Verify that BibliothecaAPI._run_self_tests() calls the right
        # methods.

        class Mock(MockBibliothecaAPI):
            "Mock every method used by BibliothecaAPI._run_self_tests."

            # First we will count the circulation events that happened in the
            # last five minutes.
            def get_events_between(self, start, finish):
                self.get_events_between_called_with = (start, finish)
                return [1, 2, 3]

            # Then we will count the loans and holds for the default
            # patron.
            def patron_activity(self, patron, pin):
                self.patron_activity_called_with = (patron, pin)
                return ["loan", "hold"]

        # Now let's make sure two Libraries have access to this
        # Collection -- one library with a default patron and one
        # without.
        no_default_patron = db.library()
        bibliotheca_fixture.collection.libraries.append(no_default_patron)

        with_default_patron = db.default_library()
        create_simple_auth_integration(with_default_patron)

        # Now that everything is set up, run the self-test.
        api = Mock(db.session, bibliotheca_fixture.collection)
        now = utc_now()
        [no_patron_credential, recent_circulation_events, patron_activity] = sorted(
            api._run_self_tests(db.session), key=lambda x: x.name
        )

        assert (
            "Acquiring test patron credentials for library %s" % no_default_patron.name
            == no_patron_credential.name
        )
        assert False == no_patron_credential.success
        assert "Library has no test patron configured." == str(
            no_patron_credential.exception
        )

        assert (
            "Asking for circulation events for the last five minutes"
            == recent_circulation_events.name
        )
        assert True == recent_circulation_events.success
        assert "Found 3 event(s)" == recent_circulation_events.result
        start, end = api.get_events_between_called_with
        assert 5 * 60 == (end - start).total_seconds()
        assert (end - now).total_seconds() < 2

        assert (
            "Checking activity for test patron for library %s"
            % with_default_patron.name
            == patron_activity.name
        )
        assert "Found 2 loans/holds" == patron_activity.result
        patron, pin = api.patron_activity_called_with
        assert "username1" == patron.authorization_identifier
        assert "password1" == pin

    def test_full_path(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        id = bibliotheca_fixture.api.library_id
        assert "/cirrus/library/%s/foo" % id == bibliotheca_fixture.api.full_path("foo")
        assert "/cirrus/library/%s/foo" % id == bibliotheca_fixture.api.full_path(
            "/foo"
        )
        assert "/cirrus/library/%s/foo" % id == bibliotheca_fixture.api.full_path(
            "/cirrus/library/%s/foo" % id
        )

    def test_full_url(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        id = bibliotheca_fixture.api.library_id
        assert (
            "http://bibliotheca.test/cirrus/library/%s/foo" % id
            == bibliotheca_fixture.api.full_url("foo")
        )
        assert (
            "http://bibliotheca.test/cirrus/library/%s/foo" % id
            == bibliotheca_fixture.api.full_url("/foo")
        )

    def test_request_signing(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        # Confirm a known correct result for the Bibliotheca request signing
        # algorithm.

        bibliotheca_fixture.api.queue_response(200)
        response = bibliotheca_fixture.api.request("some_url")
        [request] = bibliotheca_fixture.api.requests
        headers = request[-1]["headers"]
        assert "Fri, 01 Jan 2016 00:00:00 GMT" == headers["3mcl-Datetime"]
        assert "2.0" == headers["3mcl-Version"]
        expect = "3MCLAUTH a:HZHNGfn6WVceakGrwXaJQ9zIY0Ai5opGct38j9/bHrE="
        assert expect == headers["3mcl-Authorization"]

        # Tweak one of the variables that go into the signature, and
        # the signature changes.
        bibliotheca_fixture.api.library_id = bibliotheca_fixture.api.library_id + "1"
        bibliotheca_fixture.api.queue_response(200)
        response = bibliotheca_fixture.api.request("some_url")
        request = bibliotheca_fixture.api.requests[-1]
        headers = request[-1]["headers"]
        assert headers["3mcl-Authorization"] != expect

    def test_replacement_policy(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        db = bibliotheca_fixture.db
        mock_analytics = object()
        policy = bibliotheca_fixture.api.replacement_policy(
            db.session, analytics=mock_analytics
        )
        assert isinstance(policy, ReplacementPolicy)
        assert mock_analytics == policy.analytics

    def test_bibliographic_lookup_request(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        bibliotheca_fixture.api.queue_response(200, content="some data")
        response = bibliotheca_fixture.api.bibliographic_lookup_request(["id1", "id2"])
        [request] = bibliotheca_fixture.api.requests
        url = request[1]

        # The request URL is the /items endpoint with the IDs concatenated.
        assert url == bibliotheca_fixture.api.full_url("items") + "/id1,id2"

        # The response string is returned directly.
        assert b"some data" == response

    def test_bibliographic_lookup(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        db = bibliotheca_fixture.db

        class MockItemListParser:
            def parse(self, data):
                self.parse_called_with = data
                yield "item1"
                yield "item2"

        class Mock(MockBibliothecaAPI):
            """Mock the functionality used by bibliographic_lookup_request."""

            def __init__(self):
                self.item_list_parser = MockItemListParser()

            def bibliographic_lookup_request(self, identifier_strings):
                self.bibliographic_lookup_request_called_with = identifier_strings
                return "parse me"

        api = Mock()

        identifier = db.identifier()
        # We can pass in a list of identifier strings, a list of
        # Identifier objects, or a single example of each:
        for identifier, identifier_string in (
            ("id1", "id1"),
            (identifier, identifier.identifier),
        ):
            for identifier_list in ([identifier], identifier):
                api.item_list_parser.parse_called_with = None

                results = list(api.bibliographic_lookup(identifier_list))

                # A list of identifier strings is passed into
                # bibliographic_lookup_request().
                assert [
                    identifier_string
                ] == api.bibliographic_lookup_request_called_with

                # The response content is passed into parse()
                assert "parse me" == api.item_list_parser.parse_called_with

                # The results of parse() are yielded.
                assert ["item1", "item2"] == results

    def test_bad_response_raises_exception(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        db = bibliotheca_fixture.db
        bibliotheca_fixture.api.queue_response(500, content="oops")
        identifier = db.identifier()
        with pytest.raises(BadResponseException) as excinfo:
            bibliotheca_fixture.api.bibliographic_lookup(identifier)
        assert "Got status code 500" in str(excinfo.value)

    def test_put_request(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        # This is a basic test to make sure the method calls line up
        # right--there are more thorough tests in the circulation
        # manager, which actually uses this functionality.

        bibliotheca_fixture.api.queue_response(200, content="ok, you put something")
        response = bibliotheca_fixture.api.request(
            "checkout", "put this!", method="PUT"
        )

        # The PUT request went through to the correct URL and the right
        # payload was sent.
        [[method, url, args, kwargs]] = bibliotheca_fixture.api.requests
        assert "PUT" == method
        assert bibliotheca_fixture.api.full_url("checkout") == url
        assert "put this!" == kwargs["data"]

        # The response is what we'd expect.
        assert 200 == response.status_code
        assert b"ok, you put something" == response.content

    def test_get_events_between_success(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        data = bibliotheca_fixture.files.sample_data("empty_end_date_event.xml")
        bibliotheca_fixture.api.queue_response(200, content=data)
        now = utc_now()
        an_hour_ago = now - timedelta(minutes=3600)
        response = bibliotheca_fixture.api.get_events_between(an_hour_ago, now)
        [event] = list(response)
        assert "d5rf89" == event[0]

    def test_get_events_between_failure(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        bibliotheca_fixture.api.queue_response(500)
        now = utc_now()
        an_hour_ago = now - timedelta(minutes=3600)
        pytest.raises(
            BadResponseException,
            bibliotheca_fixture.api.get_events_between,
            an_hour_ago,
            now,
        )

    def test_update_availability(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        db = bibliotheca_fixture.db
        # Test the Bibliotheca implementation of the update_availability
        # method defined by the CirculationAPI interface.

        # Create an analytics integration so we can make sure
        # events are tracked.
        integration, ignore = create(
            db.session,
            ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="core.local_analytics_provider",
        )

        # Create a LicensePool that needs updating.
        edition, pool = db.edition(
            identifier_type=Identifier.THREEM_ID,
            data_source_name=DataSource.THREEM,
            with_license_pool=True,
            collection=bibliotheca_fixture.collection,
        )

        # We have never checked the circulation information for this
        # LicensePool. Put some random junk in the pool to verify
        # that it gets changed.
        pool.licenses_owned = 10
        pool.licenses_available = 5
        pool.patrons_in_hold_queue = 3
        assert None == pool.last_checked

        # We do have a Work hanging around, but things are about to
        # change for it.
        work, is_new = pool.calculate_work()
        assert any(
            x
            for x in work.coverage_records
            if x.operation == WorkCoverageRecord.CLASSIFY_OPERATION
        )

        # Prepare availability information.
        data = bibliotheca_fixture.files.sample_data("item_metadata_single.xml")
        # Change the ID in the test data so it looks like it's talking
        # about the LicensePool we just created.
        data = data.replace(b"ddf4gr9", pool.identifier.identifier.encode("utf8"))

        # Update availability using that data.
        bibliotheca_fixture.api.queue_response(200, content=data)
        bibliotheca_fixture.api.update_availability(pool)

        # The availability information has been updated, as has the
        # date the availability information was last checked.
        assert 1 == pool.licenses_owned
        assert 1 == pool.licenses_available
        assert 0 == pool.patrons_in_hold_queue

        circulation_events = (
            db.session.query(CirculationEvent)
            .join(LicensePool)
            .filter(LicensePool.id == pool.id)
        )
        # No more Distributor events
        assert 0 == circulation_events.count()

        old_last_checked = pool.last_checked
        assert old_last_checked is not None

        # The work's CLASSIFY_OPERATION coverage record has been
        # removed. In the near future its coverage will be
        # recalculated to accommodate the new metadata.
        assert any(
            x
            for x in work.coverage_records
            if x.operation == WorkCoverageRecord.CLASSIFY_OPERATION
        )

        # Now let's try update_availability again, with a file that
        # makes it look like the book has been removed from the
        # collection.
        data = bibliotheca_fixture.files.sample_data("empty_item_bibliographic.xml")
        bibliotheca_fixture.api.queue_response(200, content=data)

        bibliotheca_fixture.api.update_availability(pool)

        assert 0 == pool.licenses_owned
        assert 0 == pool.licenses_available
        assert 0 == pool.patrons_in_hold_queue

        assert pool.last_checked is not old_last_checked

        circulation_events = (
            db.session.query(CirculationEvent)
            .join(LicensePool)
            .filter(LicensePool.id == pool.id)
        )
        # No more DISTRIBUTOR events
        assert 0 == circulation_events.count()

    def test_marc_request(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        # A request for MARC records between two dates makes an API
        # call and yields a sequence of pymarc Record objects.
        start = datetime_utc(2012, 1, 2, 3, 4, 5)
        end = datetime_utc(2014, 5, 6, 7, 8, 9)
        bibliotheca_fixture.api.queue_response(
            200, content=bibliotheca_fixture.files.sample_data("marc_records_two.xml")
        )
        records = [x for x in bibliotheca_fixture.api.marc_request(start, end, 10, 20)]
        [(method, url, body, headers)] = bibliotheca_fixture.api.requests

        # A GET request was sent to the expected endpoint
        assert method == "GET"
        for expect in (
            "/data/marc?" "startdate=2012-01-02T03:04:05",
            "enddate=2014-05-06T07:08:09",
            "offset=10",
            "limit=20",
        ):
            assert expect in url

        # The queued response was converted into pymarc Record objects.
        assert all(isinstance(x, Record) for x in records)
        assert ["Siege and Storm", "Red Island House A Novel/"] == [
            x.title() for x in records
        ]

        # If the API returns an error, an appropriate exception is raised.
        bibliotheca_fixture.api.queue_response(
            404, content=bibliotheca_fixture.files.sample_data("error_unknown.xml")
        )
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            [x for x in bibliotheca_fixture.api.marc_request(start, end, 10, 20)]

    def test_sync_bookshelf(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        db = bibliotheca_fixture.db
        patron = db.patron()
        circulation = CirculationAPI(
            db.session,
            db.default_library(),
            api_map={bibliotheca_fixture.collection.protocol: MockBibliothecaAPI},
        )

        api = circulation.api_for_collection[bibliotheca_fixture.collection.id]
        api.queue_response(
            200, content=bibliotheca_fixture.files.sample_data("checkouts.xml")
        )
        circulation.sync_bookshelf(patron, "dummy pin")

        # The patron should have two loans and two holds.
        l1, l2 = patron.loans
        h1, h2 = patron.holds

        assert datetime_utc(2015, 3, 20, 18, 50, 22) == l1.start
        assert datetime_utc(2015, 4, 10, 18, 50, 22) == l1.end

        assert datetime_utc(2015, 3, 13, 13, 38, 19) == l2.start
        assert datetime_utc(2015, 4, 3, 13, 38, 19) == l2.end

        # The patron is fourth in line. The end date is an estimate
        # of when the hold will be available to check out.
        assert datetime_utc(2015, 3, 24, 15, 6, 56) == h1.start
        assert datetime_utc(2015, 3, 24, 15, 7, 51) == h1.end
        assert 4 == h1.position

        # The hold has an end date. It's time for the patron to decide
        # whether or not to check out this book.
        assert datetime_utc(2015, 5, 25, 17, 5, 34) == h2.start
        assert datetime_utc(2015, 5, 27, 17, 5, 34) == h2.end
        assert 0 == h2.position

    def test_place_hold(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        db = bibliotheca_fixture.db
        patron = db.patron()
        edition, pool = db.edition(with_license_pool=True)
        bibliotheca_fixture.api.queue_response(
            200, content=bibliotheca_fixture.files.sample_data("successful_hold.xml")
        )
        response = bibliotheca_fixture.api.place_hold(patron, "pin", pool)
        assert pool.identifier.type == response.identifier_type
        assert pool.identifier.identifier == response.identifier

    def test_place_hold_fails_if_exceeded_hold_limit(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        db = bibliotheca_fixture.db
        patron = db.patron()
        edition, pool = db.edition(with_license_pool=True)
        bibliotheca_fixture.api.queue_response(
            400,
            content=bibliotheca_fixture.files.sample_data(
                "error_exceeded_hold_limit.xml"
            ),
        )
        pytest.raises(
            PatronHoldLimitReached,
            bibliotheca_fixture.api.place_hold,
            patron,
            "pin",
            pool,
        )

    def test_get_audio_fulfillment_file(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        """Verify that get_audio_fulfillment_file sends the
        request we expect.
        """
        bibliotheca_fixture.api.queue_response(200, content="A license")
        response = bibliotheca_fixture.api.get_audio_fulfillment_file(
            "patron id", "bib id"
        )

        [[method, url, args, kwargs]] = bibliotheca_fixture.api.requests
        assert "POST" == method
        assert url.endswith("GetItemAudioFulfillment")
        assert (
            "<AudioFulfillmentRequest><ItemId>bib id</ItemId><PatronId>patron id</PatronId></AudioFulfillmentRequest>"
            == kwargs["data"]
        )

        assert 200 == response.status_code
        assert b"A license" == response.content

    def test_fulfill(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        db = bibliotheca_fixture.db
        patron = db.patron()

        # This miracle book is available either as an audiobook or as
        # an EPUB.
        work = db.work(data_source_name=DataSource.BIBLIOTHECA, with_license_pool=True)
        [pool] = work.license_pools

        # Let's fulfill the EPUB first.
        bibliotheca_fixture.api.queue_response(
            200,
            headers={"Content-Type": "presumably/an-acsm"},
            content="this is an ACSM",
        )
        fulfillment = bibliotheca_fixture.api.fulfill(
            patron, "password", pool, internal_format="ePub"
        )
        assert isinstance(fulfillment, FulfillmentInfo)
        assert b"this is an ACSM" == fulfillment.content
        assert pool.identifier.identifier == fulfillment.identifier
        assert pool.identifier.type == fulfillment.identifier_type
        assert pool.data_source.name == fulfillment.data_source_name

        # The media type reported by the server is passed through.
        assert "presumably/an-acsm" == fulfillment.content_type

        # Now let's try the audio version.
        license = bibliotheca_fixture.files.sample_data(
            "sample_findaway_audiobook_license.json"
        )
        bibliotheca_fixture.api.queue_response(
            200, headers={"Content-Type": "application/json"}, content=license
        )
        fulfillment = bibliotheca_fixture.api.fulfill(
            patron, "password", pool, internal_format="MP3"
        )
        assert isinstance(fulfillment, FulfillmentInfo)

        # Here, the media type reported by the server is not passed
        # through; it's replaced by a more specific media type
        assert DeliveryMechanism.FINDAWAY_DRM == fulfillment.content_type

        # The document sent by the 'Findaway' server has been
        # converted into a web publication manifest.
        manifest = json.loads(fulfillment.content)

        # The conversion process is tested more fully in
        # test_findaway_license_to_webpub_manifest. This just verifies
        # that the manifest contains information from the 'Findaway'
        # document as well as information from the Work.
        metadata = manifest["metadata"]
        assert (
            "abcdef01234789abcdef0123" == metadata["encrypted"]["findaway:checkoutId"]
        )
        assert work.title == metadata["title"]

        # Now let's see what happens to fulfillment when 'Findaway' or
        # 'Bibliotheca' sends bad information.
        bad_media_type = "application/error+json"
        bad_content = b"This is not my beautiful license document!"
        bibliotheca_fixture.api.queue_response(
            200, headers={"Content-Type": bad_media_type}, content=bad_content
        )
        fulfillment = bibliotheca_fixture.api.fulfill(
            patron, "password", pool, internal_format="MP3"
        )
        assert isinstance(fulfillment, FulfillmentInfo)

        # The (apparently) bad document is just passed on to the
        # client as part of the FulfillmentInfo, in the hopes that the
        # client will know what to do with it.
        assert bad_media_type == fulfillment.content_type
        assert bad_content == fulfillment.content

    def test_findaway_license_to_webpub_manifest(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        db = bibliotheca_fixture.db
        work = db.work(with_license_pool=True)
        [pool] = work.license_pools
        document = bibliotheca_fixture.files.sample_data(
            "sample_findaway_audiobook_license.json"
        )

        # Randomly scramble the Findaway manifest to make sure it gets
        # properly sorted when converted to a Webpub-like manifest.
        document = json.loads(document)
        document["items"].sort(key=lambda x: random.random())  # type: ignore
        document = json.dumps(document)  # type: ignore

        m = BibliothecaAPI.findaway_license_to_webpub_manifest
        media_type, manifest = m(pool, document)
        assert DeliveryMechanism.FINDAWAY_DRM == media_type
        manifest = json.loads(manifest)

        # We use the default context for Web Publication Manifest
        # files, but we also define an extension context called
        # 'findaway', which lets us include terms coined by Findaway
        # in a normal Web Publication Manifest document.
        context = manifest["@context"]
        default, findaway = context
        assert AudiobookManifest.DEFAULT_CONTEXT == default
        assert {"findaway": FindawayManifest.FINDAWAY_EXTENSION_CONTEXT} == findaway

        metadata = manifest["metadata"]

        # Information about the book has been added to metadata.
        # (This is tested more fully in
        # core/tests/util/test_util_web_publication_manifest.py.)
        assert work.title == metadata["title"]
        assert pool.identifier.urn == metadata["identifier"]
        assert "en" == metadata["language"]

        # Information about the license has been added to an 'encrypted'
        # object within metadata.
        encrypted = metadata["encrypted"]
        assert (
            "http://librarysimplified.org/terms/drm/scheme/FAE" == encrypted["scheme"]
        )
        assert "abcdef01234789abcdef0123" == encrypted["findaway:checkoutId"]
        assert "1234567890987654321ababa" == encrypted["findaway:licenseId"]
        assert "3M" == encrypted["findaway:accountId"]
        assert "123456" == encrypted["findaway:fulfillmentId"]
        assert (
            "aaaaaaaa-4444-cccc-dddd-666666666666" == encrypted["findaway:sessionKey"]
        )

        # Every entry in the license document's 'items' list has
        # become a readingOrder item in the manifest.
        reading_order = manifest["readingOrder"]
        assert 79 == len(reading_order)

        # The duration of each readingOrder item has been converted to
        # seconds.
        first = reading_order[0]
        assert 16.201 == first["duration"]
        assert "Track 1" == first["title"]

        # There is no 'href' value for the readingOrder items because the
        # files must be obtained through the Findaway SDK rather than
        # through regular HTTP requests.
        #
        # Since this is a relatively small book, it only has one part,
        # part #0. Within that part, the items have been sorted by
        # their sequence.
        for i, item in enumerate(reading_order):
            assert None == item.get("href", None)
            assert Representation.MP3_MEDIA_TYPE == item["type"]
            assert 0 == item["findaway:part"]
            assert i + 1 == item["findaway:sequence"]

        # The total duration, in seconds, has been added to metadata.
        assert 28371 == int(metadata["duration"])


class TestBibliothecaCirculationSweep:
    def test_circulation_sweep_discovers_work(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        db = bibliotheca_fixture.db
        # Test what happens when BibliothecaCirculationSweep discovers a new
        # work.

        # Create an analytics integration so we can make sure
        # events are tracked.
        integration, ignore = create(
            db.session,
            ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="core.local_analytics_provider",
        )

        # We know about an identifier, but nothing else.
        identifier = db.identifier(
            identifier_type=Identifier.BIBLIOTHECA_ID, foreign_id="ddf4gr9"
        )

        # We're about to get information about that identifier from
        # the API.
        data = bibliotheca_fixture.files.sample_data("item_metadata_single.xml")

        # Update availability using that data.
        bibliotheca_fixture.api.queue_response(200, content=data)
        monitor = BibliothecaCirculationSweep(
            db.session,
            bibliotheca_fixture.collection,
            api_class=bibliotheca_fixture.api,
        )
        monitor.process_items([identifier])

        # Validate that the HTTP request went to the /items endpoint.
        request = bibliotheca_fixture.api.requests.pop()
        url = request[1]
        assert (
            url
            == bibliotheca_fixture.api.full_url("items") + "/" + identifier.identifier
        )

        # A LicensePool has been created for the previously mysterious
        # identifier.
        [pool] = identifier.licensed_through
        assert bibliotheca_fixture.collection == pool.collection
        assert False == pool.open_access

        # Three circulation events were created for this license pool,
        # marking the creation of the license pool, the addition of
        # licenses owned, and the making of those licenses available.
        circulation_events = (
            db.session.query(CirculationEvent)
            .join(LicensePool)
            .filter(LicensePool.id == pool.id)
        )

        # DISTRIBUTOR EVENTS have been removed entirely
        assert 0 == circulation_events.count()


# Tests of the various parser classes.
#


class TestBibliothecaParser:
    def test_parse_date(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        parser = BibliothecaParser()
        v = parser.parse_date("2016-01-02T12:34:56")
        assert datetime_utc(2016, 1, 2, 12, 34, 56) == v

        assert None == parser.parse_date(None)
        assert None == parser.parse_date("Some weird value")


class TestEventParser:
    def test_parse_empty_list(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        data = bibliotheca_fixture.files.sample_data("empty_event_batch.xml")

        # By default, we consider an empty batch of events not
        # as an error.
        events = list(EventParser().process_all(data))
        assert [] == events

        # But if we consider not having events for a certain time
        # period, then an exception should be raised.
        no_events_error = True
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            list(EventParser().process_all(data, no_events_error))
        assert (
            "No events returned from server. This may not be an error, but treating it as one to be safe."
            in str(excinfo.value)
        )

    def test_parse_empty_end_date_event(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        data = bibliotheca_fixture.files.sample_data("empty_end_date_event.xml")
        [event] = list(EventParser().process_all(data))
        (threem_id, isbn, patron_id, start_time, end_time, internal_event_type) = event
        assert "d5rf89" == threem_id
        assert "9781101190623" == isbn
        assert None == patron_id
        assert datetime_utc(2016, 4, 28, 11, 4, 6) == start_time
        assert None == end_time
        assert "distributor_license_add" == internal_event_type


class TestPatronCirculationParser:
    def test_parse(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        data = bibliotheca_fixture.files.sample_data("checkouts.xml")
        collection = bibliotheca_fixture.collection
        loans_and_holds = PatronCirculationParser(collection).process_all(data)
        loans = [x for x in loans_and_holds if isinstance(x, LoanInfo)]
        holds = [x for x in loans_and_holds if isinstance(x, HoldInfo)]
        assert 2 == len(loans)
        assert 2 == len(holds)
        [l1, l2] = sorted(loans, key=lambda x: x.identifier)
        assert "1ad589" == l1.identifier
        assert "cgaxr9" == l2.identifier
        expect_loan_start = datetime_utc(2015, 3, 20, 18, 50, 22)
        expect_loan_end = datetime_utc(2015, 4, 10, 18, 50, 22)
        assert expect_loan_start == l1.start_date
        assert expect_loan_end == l1.end_date

        [h1, h2] = sorted(holds, key=lambda x: x.identifier)

        # This is the book on reserve.
        assert collection.id == h1.collection_id
        assert DataSource.BIBLIOTHECA == h1.data_source_name
        assert "9wd8" == h1.identifier
        expect_hold_start = datetime_utc(2015, 5, 25, 17, 5, 34)
        expect_hold_end = datetime_utc(2015, 5, 27, 17, 5, 34)
        assert expect_hold_start == h1.start_date
        assert expect_hold_end == h1.end_date
        assert 0 == h1.hold_position

        # This is the book on hold.
        assert "d4o8r9" == h2.identifier
        assert collection.id == h2.collection_id
        assert DataSource.BIBLIOTHECA == h2.data_source_name
        expect_hold_start = datetime_utc(2015, 3, 24, 15, 6, 56)
        expect_hold_end = datetime_utc(2015, 3, 24, 15, 7, 51)
        assert expect_hold_start == h2.start_date
        assert expect_hold_end == h2.end_date
        assert 4 == h2.hold_position


class TestCheckoutResponseParser:
    def test_parse(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        data = bibliotheca_fixture.files.sample_data("successful_checkout.xml")
        due_date = CheckoutResponseParser().process_all(data)
        assert datetime_utc(2015, 4, 16, 0, 32, 36) == due_date


class TestErrorParser:

    BIBLIOTHECA_ERROR_RESPONSE_BODY_TEMPLATE = (
        '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
        "<Code>Gen-001</Code><Message>"
        "{message}"
        "</Message></Error>"
    )

    @runtime_checkable
    class CirculationExceptionWithProblemDetail(Protocol):
        status_code: ClassVar[int]

        def as_problem_detail_document(self, debug=False) -> ProblemDetail:
            ...

    @pytest.mark.parametrize(
        "incoming_message, error_class, error_code, problem_detail_title, problem_detail_code",
        [
            (
                "Patron cannot loan more than 12 documents",
                PatronLoanLimitReached,
                500,
                "Loan limit reached.",
                403,
            ),
            (
                "Patron cannot have more than 15 holds",
                PatronHoldLimitReached,
                500,
                "Limit reached.",
                403,
            ),
            (
                "the patron document status was CAN_WISH and not one of CAN_LOAN,RESERVATION",
                NoLicenses,
                404,
                "No licenses.",
                404,
            ),
            (
                "the patron document status was CAN_HOLD and not one of CAN_LOAN,RESERVATION",
                NoAvailableCopies,
                400,
                None,
                None,
            ),
            (
                "the patron document status was LOAN and not one of CAN_LOAN,RESERVATION",
                AlreadyCheckedOut,
                400,
                None,
                None,
            ),
            (
                "The patron has no eBooks checked out",
                NotCheckedOut,
                400,
                None,
                None,
            ),
            (
                "the patron document status was CAN_LOAN and not one of CAN_HOLD",
                CurrentlyAvailable,
                400,
                None,
                None,
            ),
            (
                "the patron document status was HOLD and not one of CAN_HOLD",
                AlreadyOnHold,
                400,
                None,
                None,
            ),
            (
                "The patron does not have the book on hold",
                NotOnHold,
                400,
                None,
                None,
            ),
            # This is such a weird case we don't have a special exception for it.
            (
                "the patron document status was LOAN and not one of CAN_HOLD",
                CannotHold,
                500,
                None,
                None,
            ),
        ],
    )
    def test_exception(
        self,
        incoming_message: str,
        error_class: Any,
        error_code: int,
        problem_detail_title: str,
        problem_detail_code: int,
    ):
        document = self.BIBLIOTHECA_ERROR_RESPONSE_BODY_TEMPLATE.format(
            message=incoming_message
        )
        error = ErrorParser().process_all(document)
        assert isinstance(error, error_class)
        assert incoming_message == str(error)
        assert error_code == error.status_code

        if isinstance(error, self.CirculationExceptionWithProblemDetail):
            problem = error.as_problem_detail_document()
            assert problem_detail_code == problem.status_code
            assert problem_detail_title == problem.title

    @pytest.mark.parametrize(
        "incoming_message, incoming_message_from_file, error_string",
        [
            (
                # Simulate the message we get when the server goes down.
                "The server has encountered an error",
                None,
                "The server has encountered an error",
            ),
            (
                # Simulate the message we get when the server gives a vague error.
                None,
                "error_unknown.xml",
                "Unknown error",
            ),
            (
                # Simulate the message we get when the error message is
                # 'Authentication failed' but our authentication information is
                # set up correctly.
                None,
                "error_authentication_failed.xml",
                "Authentication failed",
            ),
            (
                """<weird>This error does not follow the standard set out by Bibliotheca.</weird>""",
                None,
                "Unknown error",
            ),
            (
                # Empty error message
                """<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><Message/></Error>""",
                None,
                "Unknown error",
            ),
        ],
    )
    def test_remote_initiated_server_error(
        self,
        incoming_message: Optional[str],
        incoming_message_from_file: Optional[str],
        error_string: str,
        api_bibliotheca_files_fixture: BibliothecaFilesFixture,
    ):
        if incoming_message_from_file:
            incoming_message = api_bibliotheca_files_fixture.files().sample_text(
                incoming_message_from_file
            )
        error = ErrorParser().process_all(incoming_message)
        problem = error.as_problem_detail_document()

        assert isinstance(error, RemoteInitiatedServerError)
        assert BibliothecaAPI.SERVICE_NAME == error.service_name
        assert 502 == error.status_code
        assert error_string == str(error)

        assert 502 == problem.status_code
        assert "Integration error communicating with Bibliotheca" == problem.detail
        assert "Third-party service failed." == problem.title


class TestBibliothecaEventParser:

    # Sample event feed to test out the parser.
    TWO_EVENTS = """<LibraryEventBatch xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <PublishId>1b0d6667-a10e-424a-9f73-fb6f6d41308e</PublishId>
  <PublishDateTimeInUTC>2014-04-14T13:59:05.6920303Z</PublishDateTimeInUTC>
  <LastEventDateTimeInUTC>2014-04-03T00:00:34</LastEventDateTimeInUTC>
  <Events>
    <CloudLibraryEvent>
      <LibraryId>test-library</LibraryId>
      <EventId>event-1</EventId>
      <EventType>CHECKIN</EventType>
      <EventStartDateTimeInUTC>2014-04-03T00:00:23</EventStartDateTimeInUTC>
      <EventEndDateTimeInUTC>2014-04-03T00:00:23</EventEndDateTimeInUTC>
      <ItemId>theitem1</ItemId>
      <ISBN>900isbn1</ISBN>
      <PatronId>patronid1</PatronId>
      <EventPublishDateTimeInUTC>2014-04-14T13:59:05</EventPublishDateTimeInUTC>
    </CloudLibraryEvent>
    <CloudLibraryEvent>
      <LibraryId>test-library</LibraryId>
      <EventId>event-2</EventId>
      <EventType>CHECKOUT</EventType>
      <EventStartDateTimeInUTC>2014-04-03T00:00:34</EventStartDateTimeInUTC>
      <EventEndDateTimeInUTC>2014-04-02T23:57:37</EventEndDateTimeInUTC>
      <ItemId>theitem2</ItemId>
      <ISBN>900isbn2</ISBN>
      <PatronId>patronid2</PatronId>
      <EventPublishDateTimeInUTC>2014-04-14T13:59:05</EventPublishDateTimeInUTC>
    </CloudLibraryEvent>
  </Events>
</LibraryEventBatch>
"""

    def test_parse_event_batch(self):
        # Parsing the XML gives us two events.
        event1, event2 = EventParser().process_all(self.TWO_EVENTS)

        (threem_id, isbn, patron_id, start_time, end_time, internal_event_type) = event1

        assert "theitem1" == threem_id
        assert "900isbn1" == isbn
        assert "patronid1" == patron_id
        assert CirculationEvent.DISTRIBUTOR_CHECKIN == internal_event_type
        assert start_time == end_time

        (threem_id, isbn, patron_id, start_time, end_time, internal_event_type) = event2
        assert "theitem2" == threem_id
        assert "900isbn2" == isbn
        assert "patronid2" == patron_id
        assert CirculationEvent.DISTRIBUTOR_CHECKOUT == internal_event_type

        # Verify that start and end time were parsed correctly.
        correct_start = datetime_utc(2014, 4, 3, 0, 0, 34)
        correct_end = datetime_utc(2014, 4, 2, 23, 57, 37)
        assert correct_start == start_time
        assert correct_end == end_time


class TestBibliothecaPurchaseMonitor:
    @pytest.fixture()
    def default_monitor(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        return BibliothecaPurchaseMonitor(
            bibliotheca_fixture.db.session,
            bibliotheca_fixture.collection,
            api_class=MockBibliothecaAPI,
            analytics=MockAnalyticsProvider(),
        )

    @pytest.fixture()
    def initialized_monitor(self, db: DatabaseTransactionFixture):
        collection = MockBibliothecaAPI.mock_collection(
            db.session, name="Initialized Purchase Monitor Collection"
        )
        monitor = BibliothecaPurchaseMonitor(
            db.session, collection, api_class=MockBibliothecaAPI
        )
        Timestamp.stamp(
            db.session,
            service=monitor.service_name,
            service_type=Timestamp.MONITOR_TYPE,
            collection=collection,
        )
        return monitor

    @pytest.mark.parametrize(
        "specified_default_start, expected_default_start",
        [
            ("2011", datetime_utc(year=2011, month=1, day=1)),
            ("2011-10", datetime_utc(year=2011, month=10, day=1)),
            ("2011-10-05", datetime_utc(year=2011, month=10, day=5)),
            ("2011-10-05T15", datetime_utc(year=2011, month=10, day=5, hour=15)),
            (
                "2011-10-05T15:27",
                datetime_utc(year=2011, month=10, day=5, hour=15, minute=27),
            ),
            (
                "2011-10-05T15:27:33",
                datetime_utc(year=2011, month=10, day=5, hour=15, minute=27, second=33),
            ),
            (
                "2011-10-05 15:27:33",
                datetime_utc(year=2011, month=10, day=5, hour=15, minute=27, second=33),
            ),
            (
                "2011-10-05T15:27:33.123456",
                datetime_utc(
                    year=2011,
                    month=10,
                    day=5,
                    hour=15,
                    minute=27,
                    second=33,
                    microsecond=123456,
                ),
            ),
            (
                datetime_utc(year=2011, month=10, day=5, hour=15, minute=27),
                datetime_utc(year=2011, month=10, day=5, hour=15, minute=27),
            ),
            (None, None),
        ],
    )
    def test_optional_iso_date_valid_dates(
        self, specified_default_start, expected_default_start, default_monitor
    ):
        # ISO 8601 strings, `datetime`s, or None are valid.
        actual_default_start = default_monitor._optional_iso_date(
            specified_default_start
        )
        if expected_default_start is not None:
            assert isinstance(actual_default_start, datetime)
        assert actual_default_start == expected_default_start

    def test_monitor_intrinsic_start_time(
        self,
        default_monitor,
        initialized_monitor,
        bibliotheca_fixture: BibliothecaAPITestFixture,
    ):
        db = bibliotheca_fixture.db
        # No `default_start` time is specified for either `default_monitor` or
        # `initialized_monitor`, so each monitor's `default_start_time` should
        # match the monitor class's intrinsic start time.
        for monitor in [default_monitor, initialized_monitor]:
            expected_intrinsic_start = BibliothecaPurchaseMonitor.DEFAULT_START_TIME
            intrinsic_start = monitor._intrinsic_start_time(db.session)
            assert isinstance(intrinsic_start, datetime)
            assert intrinsic_start == expected_intrinsic_start
            assert intrinsic_start == monitor.default_start_time

    @pytest.mark.parametrize(
        "specified_default_start, override_timestamp, expected_start",
        [
            (
                "2011-10-05T15:27",
                False,
                datetime_utc(year=2011, month=10, day=5, hour=15, minute=27),
            ),
            (
                "2011-10-05T15:27:33",
                False,
                datetime_utc(year=2011, month=10, day=5, hour=15, minute=27, second=33),
            ),
            (None, False, None),
            (None, True, None),
            (
                "2011-10-05T15:27",
                True,
                datetime_utc(year=2011, month=10, day=5, hour=15, minute=27),
            ),
            (
                "2011-10-05T15:27:33",
                True,
                datetime_utc(year=2011, month=10, day=5, hour=15, minute=27, second=33),
            ),
        ],
    )
    def test_specified_start_trumps_intrinsic_default_start(
        self,
        specified_default_start,
        override_timestamp,
        expected_start,
        bibliotheca_fixture: BibliothecaAPITestFixture,
    ):
        db = bibliotheca_fixture.db
        # When a valid `default_start` parameter is specified, it -- not the monitor's
        # intrinsic default -- will always become the monitor's `default_start_time`.
        monitor = BibliothecaPurchaseMonitor(
            db.session,
            bibliotheca_fixture.collection,
            api_class=MockBibliothecaAPI,
            default_start=specified_default_start,
            override_timestamp=override_timestamp,
        )
        monitor_intrinsic_default = monitor._intrinsic_start_time(db.session)
        assert isinstance(monitor.default_start_time, datetime)
        assert isinstance(monitor_intrinsic_default, datetime)
        if specified_default_start:
            assert monitor.default_start_time == expected_start
        else:
            assert (
                abs(
                    (
                        monitor_intrinsic_default - monitor.default_start_time
                    ).total_seconds()
                )
                <= 1
            )

        # If no `default_date` specified, then `override_timestamp` must be false.
        if not specified_default_start:
            assert monitor.override_timestamp is False

        # For an uninitialized monitor (no timestamp), the monitor's `default_start_time`,
        # whether from a specified `default_start` or the monitor's intrinsic start time,
        # will be the actual start time. The cut-off will be roughly the current time, in
        # either case.
        expected_cutoff = utc_now()
        with mock.patch.object(
            monitor, "catch_up_from", return_value=None
        ) as catch_up_from:
            monitor.run()
            actual_start, actual_cutoff, progress = catch_up_from.call_args[0]
        assert abs((expected_cutoff - actual_cutoff).total_seconds()) <= 1
        assert actual_cutoff == progress.finish
        assert actual_start == monitor.default_start_time
        assert progress.start == monitor.default_start_time

    @pytest.mark.parametrize(
        "specified_default_start, override_timestamp, expected_start",
        [
            (
                "2011-10-05T15:27",
                False,
                datetime_utc(year=2011, month=10, day=5, hour=15, minute=27),
            ),
            (
                "2011-10-05T15:27:33",
                False,
                datetime_utc(year=2011, month=10, day=5, hour=15, minute=27, second=33),
            ),
            (None, False, None),
            (None, True, None),
            (
                "2011-10-05T15:27",
                True,
                datetime_utc(year=2011, month=10, day=5, hour=15, minute=27),
            ),
            (
                "2011-10-05T15:27:33",
                True,
                datetime_utc(year=2011, month=10, day=5, hour=15, minute=27, second=33),
            ),
        ],
    )
    def test_specified_start_can_override_timestamp(
        self,
        specified_default_start,
        override_timestamp,
        expected_start,
        bibliotheca_fixture: BibliothecaAPITestFixture,
    ):
        monitor = BibliothecaPurchaseMonitor(
            bibliotheca_fixture.db.session,
            bibliotheca_fixture.collection,
            api_class=MockBibliothecaAPI,
            default_start=specified_default_start,
            override_timestamp=override_timestamp,
        )
        # For an initialized monitor, the `default_start_time` will be derived from
        # `timestamp.finish`, unless overridden by a specified `default_start` when
        # `override_timestamp` is specified as True.
        ts = Timestamp.stamp(
            bibliotheca_fixture.db.session,
            service=monitor.service_name,
            service_type=Timestamp.MONITOR_TYPE,
            collection=monitor.collection,
        )
        start_time_from_ts = ts.finish - BibliothecaPurchaseMonitor.OVERLAP
        expected_actual_start_time = (
            expected_start if monitor.override_timestamp else start_time_from_ts
        )
        expected_cutoff = utc_now()
        with mock.patch.object(
            monitor, "catch_up_from", return_value=None
        ) as catch_up_from:
            monitor.run()
            actual_start, actual_cutoff, progress = catch_up_from.call_args[0]
        assert abs((expected_cutoff - actual_cutoff).total_seconds()) <= 1
        assert actual_cutoff == progress.finish
        assert actual_start == expected_actual_start_time
        assert progress.start == expected_actual_start_time

    @pytest.mark.parametrize("input", [("invalid"), ("2020/10"), (["2020-10-05"])])
    def test_optional_iso_date_invalid_dates(self, input, default_monitor):
        with pytest.raises(ValueError) as excinfo:
            default_monitor._optional_iso_date(input)

    def test_catch_up_from(self, default_monitor):
        # catch_up_from() slices up its given timespan, calls
        # purchases() to find purchases for each slice, processes each
        # purchase using process_record(), and sets a checkpoint for each
        # slice that is unambiguously in the past.
        today = utc_now().date()

        # _checkpoint() will be called after processing this slice
        # because it's a full slice that ends before today.
        full_slice = [datetime_utc(2014, 1, 1), datetime_utc(2014, 1, 2), True]

        # _checkpoint() is not called after processing this slice
        # because it's not a full slice.
        incomplete_slice = [datetime_utc(2015, 1, 1), datetime_utc(2015, 1, 2), False]

        # _checkpoint() is not called after processing this slice,
        # even though it's supposedly complete, because today isn't
        # over yet.
        today_slice = [today - timedelta(days=1), today, True]

        # _checkpoint() is not called after processing this slice
        # because it doesn't end in the past.
        future_slice = [today + timedelta(days=1), today + timedelta(days=2), True]

        default_monitor.slice_timespan = MagicMock(
            return_value=[full_slice, incomplete_slice, today_slice, future_slice]
        )
        default_monitor.purchases = MagicMock(return_value=["A record"])
        default_monitor.process_record = MagicMock()
        default_monitor._checkpoint = MagicMock()

        # Execute.
        progress = TimestampData()
        start = datetime_utc(2019, 1, 1)
        cutoff = datetime_utc(2020, 1, 1)
        default_monitor.catch_up_from(start, cutoff, progress)

        # slice_timespan was called once.
        default_monitor.slice_timespan.assert_called_once_with(
            start, cutoff, timedelta(days=1)
        )

        # purchases() was called on each slice it returned.
        default_monitor.purchases.assert_has_calls(
            [
                mock.call(*x[:2])
                for x in (full_slice, incomplete_slice, today_slice, future_slice)
            ]
        )

        # Each purchases() call returned a single record, which was
        # passed into process_record along with the start date of the
        # current slice.
        default_monitor.process_record.assert_has_calls(
            [
                mock.call("A record", x[0])
                for x in [full_slice, incomplete_slice, today_slice, future_slice]
            ]
        )

        # TimestampData.achievements was set to the total number of
        # records processed.
        assert progress.achievements == "MARC records processed: 4"

        # Only one of our contrived time slices -- the first one --
        # was a full slice that ended before the current
        # date. _checkpoint was called on that slice, and only that
        # slice.
        default_monitor._checkpoint.assert_called_once_with(
            progress, start, full_slice[0], "MARC records processed: 1"
        )

    def test__checkpoint(self, default_monitor):
        # The _checkpoint method allows the BibliothecaPurchaseMonitor
        # to preserve its progress in case of a crash.

        # The Timestamp for the default monitor shows that it has
        # a start date but it's never successfully completed.
        timestamp_obj = default_monitor.timestamp()
        assert timestamp_obj.achievements is None
        assert timestamp_obj.start == BibliothecaPurchaseMonitor.DEFAULT_START_TIME
        assert timestamp_obj.finish is None

        timestamp_data = TimestampData()
        finish = datetime_utc(2020, 1, 1)
        achievements = "Some achievements"

        default_monitor._checkpoint(
            timestamp_data, timestamp_obj.start, finish, achievements
        )

        # Calling _checkpoint creates the impression that the monitor
        # completed at the checkpoint, even though in point of fact
        # it's still running.
        timestamp_obj = default_monitor.timestamp()
        assert timestamp_obj.achievements == achievements
        assert timestamp_obj.start == BibliothecaPurchaseMonitor.DEFAULT_START_TIME
        assert timestamp_obj.finish == finish

    def test_purchases(self, default_monitor):
        # The purchases() method calls marc_request repeatedly, handling
        # pagination.

        # Mock three pages that contain 50, 50, and 49 items.
        default_monitor.api.marc_request = MagicMock(
            side_effect=[[1] * 50, [2] * 50, [3] * 49]
        )
        start = datetime_utc(2020, 1, 1)
        end = datetime_utc(2020, 1, 2)
        records = [x for x in default_monitor.purchases(start, end)]

        # marc_request was called repeatedly with increasing offsets
        # until it returned fewer than 50 results.
        default_monitor.api.marc_request.assert_has_calls(
            [mock.call(start, end, offset, 50) for offset in (1, 51, 101)]
        )

        # Every "record" it returned was yielded as part of a single
        # stream.
        assert ([1] * 50) + ([2] * 50) + ([3] * 49) == records

    def test_process_record(
        self, default_monitor, caplog, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        # process_record may create a LicensePool, trigger the
        # bibliographic coverage provider, and/or issue a "license
        # added" analytics event, based on the identifier found in a
        # MARC record.
        purchase_time = utc_now()
        analytics = MockAnalyticsProvider()
        default_monitor.analytics = analytics
        ensure_coverage = MagicMock()
        default_monitor.bibliographic_coverage_provider.ensure_coverage = (
            ensure_coverage
        )

        # Try some cases that won't happen in real life.
        multiple_control_numbers = b"""<?xml version="1.0" encoding="UTF-8" ?><marc:collection xmlns:marc="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd"><marc:record><marc:leader>01034nam a22002413a 4500</marc:leader><marc:controlfield tag="001">ehasb89</marc:controlfield><marc:controlfield tag="001">abcde</marc:controlfield></marc:record></marc:collection>"""
        no_control_number = b"""<?xml version="1.0" encoding="UTF-8" ?><marc:collection xmlns:marc="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd"><marc:record><marc:leader>01034nam a22002413a 4500</marc:leader></marc:record></marc:collection>"""
        for bad_record, expect_error in (
            (
                multiple_control_numbers,
                "Ignoring MARC record with multiple Bibliotheca control numbers.",
            ),
            (
                no_control_number,
                "Ignoring MARC record with no Bibliotheca control number.",
            ),
        ):
            [marc] = parse_xml_to_array(BytesIO(bad_record))
            assert default_monitor.process_record(marc, purchase_time) is None
            assert expect_error in caplog.messages[-1]

        # Now, try the two real cases.
        [ehasb89, oock89] = parse_xml_to_array(
            StringIO(
                bibliotheca_fixture.files.sample_data("marc_records_two.xml").decode(
                    "utf8"
                )
            )
        )

        # If the book is new to this collection, it's run through
        # BibliothecaBibliographicCoverageProvider.ensure_coverage to
        # give it initial bibliographic and circulation data.
        pool = default_monitor.process_record(ehasb89, purchase_time)
        assert pool.identifier.identifier == "ehasb89"
        assert pool.identifier.type == Identifier.BIBLIOTHECA_ID
        assert pool.data_source.name == DataSource.BIBLIOTHECA
        assert bibliotheca_fixture.collection == pool.collection
        ensure_coverage.assert_called_once_with(pool.identifier, force=True)

        # An analytics event is issued to mark the time at which the
        # book was first purchased.
        # No more DISTRIBUTOR events
        assert analytics.count == 0

        # If the book is already in this collection, ensure_coverage
        # is not called.
        pool, ignore = LicensePool.for_foreign_id(
            bibliotheca_fixture.db.session,
            DataSource.BIBLIOTHECA,
            Identifier.BIBLIOTHECA_ID,
            "3oock89",
            collection=bibliotheca_fixture.collection,
        )
        pool2 = default_monitor.process_record(oock89, purchase_time)
        assert pool == pool2
        assert ensure_coverage.call_count == 1  # i.e. was not called again.

        # But an analytics event is still issued to mark the purchase.
        # No more DISTRIBUTOR events
        assert analytics.count == 0

    def test_end_to_end(
        self, default_monitor, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        # Limited end-to-end test of the BibliothecaPurchaseMonitor.

        # Set the default start time to one minute in the past, so the
        # monitor doesn't feel the need to make more than one call to
        # the MARC endpoint.
        default_monitor.override_timestamp = True
        start_time = utc_now() - timedelta(minutes=1)
        default_monitor.default_start_time = start_time

        # There will be two calls to the mock API: one to the MARC
        # endpoint, which will tell us about the purchase of a single
        # book, and one to the metadata endpoint for information about
        # that book.
        api = default_monitor.api
        api.queue_response(
            200, content=bibliotheca_fixture.files.sample_data("marc_records_one.xml")
        )
        api.queue_response(
            200,
            content=bibliotheca_fixture.files.sample_data("item_metadata_single.xml"),
        )
        default_monitor.run()

        # One book was created.
        work = bibliotheca_fixture.db.session.query(Work).one()

        # Bibliographic information came from the coverage provider,
        # not from our fake MARC record (which is actually for a
        # different book).
        assert work.title == "The Incense Game"

        # Licensing information was also taken from the coverage
        # provider.
        [lp] = work.license_pools
        assert lp.identifier.identifier == "ddf4gr9"
        assert default_monitor.collection == lp.collection
        assert lp.licenses_owned == 1
        assert lp.licenses_available == 1

        # An analytics event was issued to commemorate the addition of
        # the book to the collection.
        # No more DISTRIBUTOR events
        assert default_monitor.analytics.count == 0

        # The timestamp has been updated; the next time the monitor
        # runs it will ask for purchases that haven't happened yet.
        default_monitor.override_timestamp = False
        timestamp = default_monitor.timestamp()
        assert timestamp.achievements == "MARC records processed: 1"
        assert timestamp.finish > start_time


class TestBibliothecaEventMonitor:
    @pytest.fixture()
    def default_monitor(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        return BibliothecaEventMonitor(
            bibliotheca_fixture.db.session,
            bibliotheca_fixture.collection,
            api_class=MockBibliothecaAPI,
        )

    @pytest.fixture()
    def initialized_monitor(self, db: DatabaseTransactionFixture):
        collection = MockBibliothecaAPI.mock_collection(
            db.session, name="Initialized Monitor Collection"
        )
        monitor = BibliothecaEventMonitor(
            db.session, collection, api_class=MockBibliothecaAPI
        )
        Timestamp.stamp(
            db.session,
            service=monitor.service_name,
            service_type=Timestamp.MONITOR_TYPE,
            collection=collection,
        )
        return monitor

    def test_run_once(
        self,
        bibliotheca_fixture: BibliothecaAPITestFixture,
        time_fixture: Time,
        db: DatabaseTransactionFixture,
    ):
        # run_once() slices the time between its start date
        # and the current time into five-minute intervals, and asks for
        # data about one interval at a time.

        now = utc_now()
        one_hour_ago = now - timedelta(hours=1)
        two_hours_ago = now - timedelta(hours=2)

        # Simulate that this script last ran 24 hours ago
        before_timestamp = TimestampData(start=two_hours_ago, finish=one_hour_ago)

        api = MockBibliothecaAPI(db.session, bibliotheca_fixture.collection)
        api.queue_response(
            200,
            content=bibliotheca_fixture.files.sample_data("item_metadata_single.xml"),
        )
        # Setting up making requests in 5-minute intervals in the hour slice.
        for i in range(1, 15):
            api.queue_response(
                200,
                content=bibliotheca_fixture.files.sample_data(
                    "empty_end_date_event.xml"
                ),
            )

        monitor = BibliothecaEventMonitor(
            db.session, bibliotheca_fixture.collection, api_class=api
        )

        after_timestamp = monitor.run_once(before_timestamp)
        # Fifteen requests were made to the API:
        #
        # 1. Looking up detailed information about the single book
        #    whose event we found.
        #
        # 2. Retrieving the 'slices' of events between 2 hours ago and
        #    1 hour ago in 5 minute intervals.
        assert 15 == len(api.requests)

        # There is no second 'detailed information' lookup because both events
        # relate to the same book.

        # A LicensePool was created for the identifier referred to
        # in empty_end_date_event.xml.
        [pool] = bibliotheca_fixture.collection.licensepools
        assert "d5rf89" == pool.identifier.identifier

        # But since the metadata retrieved in the follow-up request
        # was for a different book, no Work and no Edition have been
        # created. (See test_handle_event for what happens when the
        # API cooperates.)
        assert None == pool.work
        assert None == pool.presentation_edition

        # The timeframe covered by that run starts a little before the
        # 'finish' date associated with the old timestamp, and ends
        # around the time run_once() was called.
        #
        # The events we found were both from 2016, but that's not
        # considered when setting the timestamp.
        assert one_hour_ago - monitor.OVERLAP == after_timestamp.start
        time_fixture.time_eq(after_timestamp.finish, now)
        # The timestamp's achivements have been updated.
        assert "Events handled: 13." == after_timestamp.achievements

        # In earlier versions, the progress timestamp's `counter`
        # property was manipulated to put the monitor in different
        # states that would improve its reliability in different
        # failure scenarios. With the addition of the
        # BibliothecaPurchaseMonitor, the reliability of
        # BibliothecaEventMonitor became much less important, so the
        # complex code has been removed.
        assert None == after_timestamp.counter

        # To prove this, run the monitor again, catching up between
        # after_timestamp.start (the current time, minus 5 minutes and
        # a little bit), and the current time.
        #
        # This is going to result in two more API calls, one for the
        # "5 minutes" and one for the "little bit".
        api.queue_response(
            200, content=bibliotheca_fixture.files.sample_data("empty_event_batch.xml")
        )
        api.queue_response(
            200, content=bibliotheca_fixture.files.sample_data("empty_event_batch.xml")
        )
        monitor.run_once(after_timestamp)

        # Two more requests were made, but no events were found for the
        # corresponding time slices, so nothing happened.
        #
        # Previously the lack of any events would have been treated as
        # an error.
        assert 17 == len(api.requests)
        assert "Events handled: 0." == after_timestamp.achievements

    def test_handle_event(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        db = bibliotheca_fixture.db
        api = MockBibliothecaAPI(db.session, bibliotheca_fixture.collection)
        api.queue_response(
            200,
            content=bibliotheca_fixture.files.sample_data("item_metadata_single.xml"),
        )
        analytics = MockAnalyticsProvider()
        monitor = BibliothecaEventMonitor(
            db.session,
            bibliotheca_fixture.collection,
            api_class=api,
            analytics=analytics,
        )

        now = utc_now()
        monitor.handle_event(
            "ddf4gr9",
            "9781250015280",
            None,
            now,
            None,
            CirculationEvent.DISTRIBUTOR_LICENSE_ADD,
        )

        # The collection now has a LicensePool corresponding to the book
        # we just loaded.
        [pool] = bibliotheca_fixture.collection.licensepools
        assert "ddf4gr9" == pool.identifier.identifier

        # The book has a presentation-ready work and we know its
        # bibliographic metadata.
        assert True == pool.work.presentation_ready
        assert "The Incense Game" == pool.work.title

        # The LicensePool's circulation information has been changed
        # to reflect what we know about the book -- that we have one
        # license which (as of the instant the event happened) is
        # available.
        assert 1 == pool.licenses_owned
        assert 1 == pool.licenses_available

        # Three analytics events were collected: one for the license
        # add event itself, one for the 'checkin' that made the new
        # license available, and a redundant 'license add' event which
        # was registered with analytics but which did not affect the
        # counts.
        #
        # In earlier versions a fourth analytics event would have been
        # issued, for the creation of a new LicensePool, but that is now
        # solely the job of the BibliothecaPurchasMonitor.
        #
        # No more DISTRIBUTOR events anymore
        assert 0 == analytics.count


class TestBibliothecaPurchaseMonitorWhenMultipleCollections:
    def test_multiple_service_type_timestamps_with_start_date(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        db = bibliotheca_fixture.db
        # Start with multiple collections that have timestamps
        # because they've run before.
        collections = [
            MockBibliothecaAPI.mock_collection(db.session, name="Collection 1"),
            MockBibliothecaAPI.mock_collection(db.session, name="Collection 2"),
        ]
        for c in collections:
            Timestamp.stamp(
                db.session,
                service=BibliothecaPurchaseMonitor.SERVICE_NAME,
                service_type=Timestamp.MONITOR_TYPE,
                collection=c,
            )
        # Instantiate the associated monitors with a start date.
        monitors = [
            BibliothecaPurchaseMonitor(
                db.session, c, api_class=BibliothecaAPI, default_start="2011-02-03"
            )
            for c in collections
        ]
        assert len(monitors) == len(collections)
        # Ensure that we get monitors and not an exception.
        for m in monitors:
            assert isinstance(m, BibliothecaPurchaseMonitor)


class TestItemListParser:
    def test_contributors_for_string(cls):
        authors = list(
            ItemListParser.contributors_from_string(
                "Walsh, Jill Paton; Sayers, Dorothy L."
            )
        )
        assert [x.sort_name for x in authors] == [
            "Walsh, Jill Paton",
            "Sayers, Dorothy L.",
        ]
        assert [x.roles for x in authors] == [
            [Contributor.AUTHOR_ROLE],
            [Contributor.AUTHOR_ROLE],
        ]

        # Parentheticals are stripped.
        [author] = ItemListParser.contributors_from_string(
            "Baum, Frank L. (Frank Lyell)"
        )
        assert "Baum, Frank L." == author.sort_name

        # Contributors may have two levels of entity reference escaping,
        # one of which will have already been handled by the initial parse.
        # So, we'll test zero and one escapings here.
        authors = list(
            ItemListParser.contributors_from_string(
                "Raji Codell, Esmé; Raji Codell, Esm&#233;"
            )
        )
        author_names = [a.sort_name for a in authors]
        assert len(authors) == 2
        assert len(set(author_names)) == 1
        assert all("Raji Codell, Esmé" == name for name in author_names)

        # It's possible to specify some role other than AUTHOR_ROLE.
        narrators = list(
            ItemListParser.contributors_from_string(
                "Callow, Simon; Mann, Bruce; Hagon, Garrick", Contributor.NARRATOR_ROLE
            )
        )
        for narrator in narrators:
            assert [Contributor.NARRATOR_ROLE] == narrator.roles
        assert ["Callow, Simon", "Mann, Bruce", "Hagon, Garrick"] == [
            narrator.sort_name for narrator in narrators
        ]

    def test_parse_genre_string(self):
        def f(genre_string):
            genres = ItemListParser.parse_genre_string(genre_string)
            assert all([x.type == Subject.BISAC for x in genres])
            return [x.name for x in genres]

        assert ["Children's Health", "Health"] == f("Children&amp;#39;s Health,Health,")

        assert [
            "Action & Adventure",
            "Science Fiction",
            "Fantasy",
            "Magic",
            "Renaissance",
        ] == f(
            "Action &amp;amp; Adventure,Science Fiction, Fantasy, Magic,Renaissance,"
        )

    def test_item_list(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        data = bibliotheca_fixture.files.sample_data("item_metadata_list_mini.xml")
        data_parsed = list(ItemListParser().parse(data))

        # There should be 2 items in the list.
        assert 2 == len(data_parsed)

        cooked = data_parsed[0]

        assert "The Incense Game" == cooked.title
        assert "A Novel of Feudal Japan" == cooked.subtitle
        assert Edition.BOOK_MEDIUM == cooked.medium
        assert "eng" == cooked.language
        assert "St. Martin's Press" == cooked.publisher
        assert datetime_utc(year=2012, month=9, day=17) == cooked.published

        primary = cooked.primary_identifier
        assert "ddf4gr9" == primary.identifier
        assert Identifier.THREEM_ID == primary.type

        identifiers = sorted(cooked.identifiers, key=lambda x: x.identifier)
        assert ["9781250015280", "9781250031112", "ddf4gr9"] == [
            x.identifier for x in identifiers
        ]

        [author] = cooked.contributors
        assert "Rowland, Laura Joh" == author.sort_name
        assert [Contributor.AUTHOR_ROLE] == author.roles

        subjects = [x.name for x in cooked.subjects]
        assert ["Children's Health", "Mystery & Detective"] == sorted(subjects)

        [pages] = cooked.measurements
        assert Measurement.PAGE_COUNT == pages.quantity_measured
        assert 304 == pages.value

        [alternate, image, description] = sorted(cooked.links, key=lambda x: x.rel)
        assert "alternate" == alternate.rel
        assert alternate.href.startswith("http://ebook.3m.com/library")

        # We have a full-size image...
        assert Hyperlink.IMAGE == image.rel
        assert Representation.JPEG_MEDIA_TYPE == image.media_type
        assert image.href.startswith("http://ebook.3m.com/delivery")
        assert "documentID=ddf4gr9" in image.href
        assert "&size=NORMAL" not in image.href

        # ... and a thumbnail, which we obtained by adding an argument
        # to the main image URL.
        thumbnail = image.thumbnail
        assert Hyperlink.THUMBNAIL_IMAGE == thumbnail.rel
        assert Representation.JPEG_MEDIA_TYPE == thumbnail.media_type
        assert thumbnail.href == image.href + "&size=NORMAL"

        # We have a description.
        assert Hyperlink.DESCRIPTION == description.rel
        assert description.content.startswith("<b>Winner")

    def test_multiple_contributor_roles(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        data = bibliotheca_fixture.files.sample_data("item_metadata_audio.xml")
        [parsed_data] = list(ItemListParser().parse(data))
        names_and_roles = []
        for c in parsed_data.contributors:
            [role] = c.roles
            names_and_roles.append((c.sort_name, role))

        # We found one author and three narrators.
        assert sorted(
            [
                ("Riggs, Ransom", "Author"),
                ("Callow, Simon", "Narrator"),
                ("Mann, Bruce", "Narrator"),
                ("Hagon, Garrick", "Narrator"),
            ]
        ) == sorted(names_and_roles)


class TestBibliographicCoverageProvider(TestBibliothecaAPI):

    """Test the code that looks up bibliographic information from Bibliotheca."""

    def test_script_instantiation(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        """Test that RunCollectionCoverageProviderScript can instantiate
        this coverage provider.
        """
        script = RunCollectionCoverageProviderScript(
            BibliothecaBibliographicCoverageProvider,
            bibliotheca_fixture.db.session,
            api_class=MockBibliothecaAPI,
        )
        [provider] = script.providers
        assert isinstance(provider, BibliothecaBibliographicCoverageProvider)
        assert isinstance(provider.api, MockBibliothecaAPI)

    def test_process_item_creates_presentation_ready_work(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        db = bibliotheca_fixture.db
        # Test the normal workflow where we ask Bibliotheca for data,
        # Bibliotheca provides it, and we create a presentation-ready work.
        identifier = db.identifier(identifier_type=Identifier.BIBLIOTHECA_ID)
        identifier.identifier = "ddf4gr9"

        # This book has no LicensePools.
        assert [] == identifier.licensed_through

        # Run it through the BibliothecaBibliographicCoverageProvider
        provider = BibliothecaBibliographicCoverageProvider(
            bibliotheca_fixture.collection, api_class=MockBibliothecaAPI
        )
        data = bibliotheca_fixture.files.sample_data("item_metadata_single.xml")

        # We can't use bibliotheca_fixture.api because that's not the same object
        # as the one created by the coverage provider.
        provider.api.queue_response(200, content=data)

        [result] = provider.process_batch([identifier])
        assert identifier == result

        # A LicensePool was created and populated with format and availability
        # information.
        [pool] = identifier.licensed_through
        assert 1 == pool.licenses_owned
        assert 1 == pool.licenses_available
        [lpdm] = pool.delivery_mechanisms
        assert (
            "application/epub+zip (application/vnd.adobe.adept+xml)"
            == lpdm.delivery_mechanism.name
        )

        # A Work was created and made presentation ready.
        assert "The Incense Game" == pool.work.title
        assert True == pool.work.presentation_ready

    def test_internal_formats(self):

        m = ItemListParser.internal_formats

        def _check_format(input, expect_medium, expect_format, expect_drm):
            medium, formats = m(input)
            assert medium == expect_medium
            [format] = formats
            assert expect_format == format.content_type
            assert expect_drm == format.drm_scheme

        rep = Representation
        adobe = DeliveryMechanism.ADOBE_DRM
        findaway = DeliveryMechanism.FINDAWAY_DRM
        book = Edition.BOOK_MEDIUM

        # Verify that we handle the known strings from Bibliotheca
        # appropriately.
        _check_format("EPUB", book, rep.EPUB_MEDIA_TYPE, adobe)
        _check_format("EPUB3", book, rep.EPUB_MEDIA_TYPE, adobe)
        _check_format("PDF", book, rep.PDF_MEDIA_TYPE, adobe)
        _check_format("MP3", Edition.AUDIO_MEDIUM, None, findaway)

        # Now Try a string we don't recognize from Bibliotheca.
        medium, formats = m("Unknown")

        # We assume it's a book.
        assert Edition.BOOK_MEDIUM == medium

        # But we don't know which format.
        assert [] == formats

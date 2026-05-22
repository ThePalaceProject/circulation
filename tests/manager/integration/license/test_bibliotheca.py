from __future__ import annotations

import json
import random
from datetime import date, timedelta
from typing import TYPE_CHECKING, cast
from unittest.mock import create_autospec

import pytest
from pymarc.record import Record

from palace.util.datetime_helpers import datetime_utc, utc_now

from palace.manager.api.circulation.data import HoldInfo, LoanInfo
from palace.manager.api.circulation.dispatcher import CirculationApiDispatcher
from palace.manager.api.circulation.exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    CannotHold,
    CirculationException,
    CurrentlyAvailable,
    NoAvailableCopies,
    NoLicenses,
    NotCheckedOut,
    NotOnHold,
    PatronHoldLimitReached,
    PatronLoanLimitReached,
    RemoteInitiatedServerError,
)
from palace.manager.api.circulation.fulfillment import Fulfillment
from palace.manager.api.web_publication_manifest import FindawayManifest
from palace.manager.integration.license.bibliotheca import (
    BibliothecaAPI,
    BibliothecaBibliographicCoverageProvider,
    BibliothecaCirculationSweep,
    BibliothecaParser,
    CheckoutResponseParser,
    ErrorParser,
    EventParser,
    ItemListParser,
    PatronCirculationParser,
)
from palace.manager.scripts.coverage_provider import RunCollectionCoverageProviderScript
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePoolDeliveryMechanism,
    LicensePoolStatus,
)
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.util.http.exception import (
    BadResponseException,
    RemoteIntegrationException,
)
from palace.manager.util.web_publication_manifest import AudiobookManifest
from tests.mocks.bibliotheca import MockBibliothecaAPI

if TYPE_CHECKING:
    from tests.fixtures.database import DatabaseTransactionFixture
    from tests.fixtures.files import BibliothecaFilesFixture


class BibliothecaAPITestFixture:
    def __init__(
        self,
        db: DatabaseTransactionFixture,
        files: BibliothecaFilesFixture,
    ):
        self.files = files
        self.db = db
        self.collection = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library()
        )
        self.api = MockBibliothecaAPI(db.session, self.collection)


@pytest.fixture(scope="function")
def bibliotheca_fixture(
    db: DatabaseTransactionFixture,
    bibliotheca_files_fixture: BibliothecaFilesFixture,
) -> BibliothecaAPITestFixture:
    return BibliothecaAPITestFixture(db, bibliotheca_files_fixture)


class TestBibliothecaAPI:
    def test__run_self_tests(
        self,
        bibliotheca_fixture: BibliothecaAPITestFixture,
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
        bibliotheca_fixture.collection.associated_libraries.append(no_default_patron)

        with_default_patron = db.default_library()
        db.simple_auth_integration(with_default_patron)

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
            "https://partner.yourcloudlibrary.com/cirrus/library/%s/foo" % id
            == bibliotheca_fixture.api.full_url("foo")
        )
        assert (
            "https://partner.yourcloudlibrary.com/cirrus/library/%s/foo" % id
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
            def process_all(self, data):
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
                api.item_list_parser.parse_called_with = None  # type: ignore[attr-defined]

                results = list(api.bibliographic_lookup(identifier_list))

                # A list of identifier strings is passed into
                # bibliographic_lookup_request().
                assert [
                    identifier_string
                ] == api.bibliographic_lookup_request_called_with

                # The response content is passed into parse()
                assert "parse me" == api.item_list_parser.parse_called_with  # type: ignore[attr-defined]

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

        # Create a LicensePool that needs updating.
        edition, pool = db.edition(
            identifier_type=Identifier.BIBLIOTHECA_ID,
            data_source_name=DataSource.BIBLIOTHECA,
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

        old_last_checked = pool.last_checked
        assert old_last_checked is not None

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
            x.title for x in records
        ]

        # If the API returns an error, an appropriate exception is raised.
        bibliotheca_fixture.api.queue_response(
            404, content=bibliotheca_fixture.files.sample_data("error_unknown.xml")
        )
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            [x for x in bibliotheca_fixture.api.marc_request(start, end, 10, 20)]

    def test_sync_patron_activity(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        db = bibliotheca_fixture.db
        patron = db.patron()
        circulation = CirculationApiDispatcher(
            db.session,
            db.default_library(),
            {bibliotheca_fixture.collection.id: bibliotheca_fixture.api},
        )

        bibliotheca_fixture.api.queue_response(
            200, content=bibliotheca_fixture.files.sample_data("checkouts.xml")
        )

        bibliotheca_fixture.api.sync_patron_activity(patron, "dummy pin")

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

        # Test the case where we get bad data in response
        bibliotheca_fixture.api.queue_response(200, content="")
        with pytest.raises(
            RemoteIntegrationException, match="Unable to parse response XML"
        ):
            bibliotheca_fixture.api.sync_patron_activity(patron, "dummy pin")

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
        delivery_mechanism = create_autospec(LicensePoolDeliveryMechanism)
        delivery_mechanism.delivery_mechanism.drm_scheme = DeliveryMechanism.ADOBE_DRM
        fulfillment = bibliotheca_fixture.api.fulfill(
            patron, "password", pool, delivery_mechanism=delivery_mechanism
        )
        assert isinstance(fulfillment, Fulfillment)
        assert b"this is an ACSM" == fulfillment.content

        # The media type reported by the server is passed through.
        assert "presumably/an-acsm" == fulfillment.content_type

        # Now let's try the audio version.
        license = bibliotheca_fixture.files.sample_data(
            "sample_findaway_audiobook_license.json"
        )
        bibliotheca_fixture.api.queue_response(
            200, headers={"Content-Type": "application/json"}, content=license
        )
        delivery_mechanism.delivery_mechanism.drm_scheme = (
            DeliveryMechanism.FINDAWAY_DRM
        )
        fulfillment = bibliotheca_fixture.api.fulfill(
            patron, "password", pool, delivery_mechanism=delivery_mechanism
        )
        assert isinstance(fulfillment, Fulfillment)

        # Here, the media type reported by the server is not passed
        # through; it's replaced by a more specific media type
        assert DeliveryMechanism.FINDAWAY_DRM == fulfillment.content_type

        # The document sent by the 'Findaway' server has been
        # converted into a web publication manifest.
        assert fulfillment.content is not None
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
            patron, "password", pool, delivery_mechanism=delivery_mechanism
        )
        assert isinstance(fulfillment, Fulfillment)

        # The (apparently) bad document is just passed on to the
        # client as part of the Fulfillment, in the hopes that the
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
        document["items"].sort(key=lambda x: random.random())
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


# Tests of the various parser classes.
#


class TestBibliothecaParser:
    def test_parse_date(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        v = BibliothecaParser.parse_date("2016-01-02T12:34:56")
        assert v == datetime_utc(2016, 1, 2, 12, 34, 56)

        assert BibliothecaParser.parse_date(None) is None
        assert BibliothecaParser.parse_date("Some weird value") is None


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
        loans_and_holds = list(PatronCirculationParser(collection).process_all(data))
        loans = [x for x in loans_and_holds if isinstance(x, LoanInfo)]
        holds = [x for x in loans_and_holds if isinstance(x, HoldInfo)]
        assert 2 == len(loans)
        assert 2 == len(holds)
        [l1, l2] = sorted(loans, key=lambda x: str(x.identifier))
        assert "1ad589" == l1.identifier
        assert "cgaxr9" == l2.identifier
        expect_loan_start = datetime_utc(2015, 3, 20, 18, 50, 22)
        expect_loan_end = datetime_utc(2015, 4, 10, 18, 50, 22)
        assert expect_loan_start == l1.start_date
        assert expect_loan_end == l1.end_date

        [h1, h2] = sorted(holds, key=lambda x: str(x.identifier))

        # This is the book on reserve.
        assert collection.id == h1.collection_id
        assert "9wd8" == h1.identifier
        expect_hold_start = datetime_utc(2015, 5, 25, 17, 5, 34)
        expect_hold_end = datetime_utc(2015, 5, 27, 17, 5, 34)
        assert expect_hold_start == h1.start_date
        assert expect_hold_end == h1.end_date
        assert 0 == h1.hold_position

        # This is the book on hold.
        assert "d4o8r9" == h2.identifier
        assert collection.id == h2.collection_id
        expect_hold_start = datetime_utc(2015, 3, 24, 15, 6, 56)
        expect_hold_end = datetime_utc(2015, 3, 24, 15, 7, 51)
        assert expect_hold_start == h2.start_date
        assert expect_hold_end == h2.end_date
        assert 4 == h2.hold_position


class TestCheckoutResponseParser:
    def test_parse(self, bibliotheca_fixture: BibliothecaAPITestFixture):
        data = bibliotheca_fixture.files.sample_data("successful_checkout.xml")
        due_date = CheckoutResponseParser().process_first(data)
        assert datetime_utc(2015, 4, 16, 0, 32, 36) == due_date


class TestErrorParser:
    BIBLIOTHECA_ERROR_RESPONSE_BODY_TEMPLATE = (
        '<Error xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
        "<Code>Gen-001</Code><Message>"
        "{message}"
        "</Message></Error>"
    )

    @pytest.mark.parametrize(
        "incoming_message, error_class, message, debug_message",
        [
            (
                "Patron cannot loan more than 12 documents",
                PatronLoanLimitReached,
                "Patron cannot loan more than 12 documents",
                None,
            ),
            (
                "Patron cannot have more than 15 holds",
                PatronHoldLimitReached,
                "Patron cannot have more than 15 holds",
                None,
            ),
            (
                "the patron document status was CAN_WISH and not one of CAN_LOAN,RESERVATION",
                NoLicenses,
                "The library currently has no licenses for this book.",
                "the patron document status was CAN_WISH and not one of CAN_LOAN,RESERVATION",
            ),
            (
                "the patron document status was CAN_HOLD and not one of CAN_LOAN,RESERVATION",
                NoAvailableCopies,
                "No copies available to check out.",
                "the patron document status was CAN_HOLD and not one of CAN_LOAN,RESERVATION",
            ),
            (
                "the patron document status was LOAN and not one of CAN_LOAN,RESERVATION",
                AlreadyCheckedOut,
                "You already have this book checked out.",
                "the patron document status was LOAN and not one of CAN_LOAN,RESERVATION",
            ),
            (
                "The patron has no eBooks checked out",
                NotCheckedOut,
                "The patron has no eBooks checked out",
                None,
            ),
            (
                "the patron document status was CAN_LOAN and not one of CAN_HOLD",
                CurrentlyAvailable,
                "Cannot place a hold on an available title.",
                "the patron document status was CAN_LOAN and not one of CAN_HOLD",
            ),
            (
                "the patron document status was HOLD and not one of CAN_HOLD",
                AlreadyOnHold,
                "You already have this book on hold.",
                "the patron document status was HOLD and not one of CAN_HOLD",
            ),
            (
                "The patron does not have the book on hold",
                NotOnHold,
                "The patron does not have the book on hold",
                None,
            ),
            # This is such a weird case we don't have a special exception for it.
            (
                "the patron document status was LOAN and not one of CAN_HOLD",
                CannotHold,
                "Could not place hold (reason unknown).",
                "the patron document status was LOAN and not one of CAN_HOLD",
            ),
        ],
    )
    def test_exception(
        self,
        incoming_message: str,
        error_class: type[CirculationException],
        message: str,
        debug_message: str | None,
    ):
        document = self.BIBLIOTHECA_ERROR_RESPONSE_BODY_TEMPLATE.format(
            message=incoming_message
        )
        error = ErrorParser().process_first(document)
        assert error.__class__ is error_class
        assert error.problem_detail.detail == message
        assert error.problem_detail.debug_message == debug_message

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
                # Simulate an unexpected response, which is not a unicode string.
                b"Beep boop bytes",
                None,
                "Beep boop bytes",
            ),
            (
                # Simulate an unexpected response, which cannot be decoded as a string.
                b"\xde\xad\xbe\xef",
                None,
                "Unreadable error message (Unicode decode error).",
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
        incoming_message: str | bytes | None,
        incoming_message_from_file: str | None,
        error_string: str,
        bibliotheca_files_fixture: BibliothecaFilesFixture,
    ):
        if incoming_message_from_file:
            incoming_message = bibliotheca_files_fixture.sample_text(
                incoming_message_from_file
            )
        assert incoming_message is not None
        error = ErrorParser().process_first(incoming_message)
        assert isinstance(error, RemoteInitiatedServerError)

        assert BibliothecaAPI.SERVICE_NAME == error.service_name
        assert error_string == str(error)

        problem = error.problem_detail
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
            (Contributor.Role.AUTHOR,),
            (Contributor.Role.AUTHOR,),
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
                "Callow, Simon; Mann, Bruce; Hagon, Garrick", Contributor.Role.NARRATOR
            )
        )
        for narrator in narrators:
            assert (Contributor.Role.NARRATOR,) == narrator.roles
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
        data_parsed = list(ItemListParser().process_all(data))

        # There should be 2 items in the list.
        assert 2 == len(data_parsed)

        cooked = data_parsed[0]

        assert "The Incense Game" == cooked.title
        assert "A Novel of Feudal Japan" == cooked.subtitle
        assert Edition.BOOK_MEDIUM == cooked.medium
        assert "eng" == cooked.language
        assert "St. Martin's Press" == cooked.publisher
        assert date(year=2012, month=9, day=17) == cooked.published

        primary = cooked.primary_identifier_data
        assert "ddf4gr9" == primary.identifier
        assert Identifier.BIBLIOTHECA_ID == primary.type

        identifiers = sorted(cooked.identifiers, key=lambda x: x.identifier)
        assert ["9781250015280", "9781250031112", "ddf4gr9"] == [
            x.identifier for x in identifiers
        ]

        [author] = cooked.contributors
        assert "Rowland, Laura Joh" == author.sort_name
        assert (Contributor.Role.AUTHOR,) == author.roles

        subjects = [x.name for x in cooked.subjects if x.name is not None]
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
        assert image.href is not None
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
        assert isinstance(description.content, str)
        assert description.content.startswith("<b>Winner")

    def test_multiple_contributor_roles(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        data = bibliotheca_fixture.files.sample_data("item_metadata_audio.xml")
        [parsed_data] = list(ItemListParser().process_all(data))
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

    def test_circulation_data_status(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        """Test that CirculationData from ItemListParser has correct status."""
        data = bibliotheca_fixture.files.sample_data("item_metadata_list_mini.xml")
        data_parsed = list(ItemListParser().process_all(data))

        # Check the first book's circulation data
        bibliographic1 = data_parsed[0]
        circulation1 = bibliographic1.circulation

        # This book has 1 license, so status should be ACTIVE
        assert circulation1.licenses_owned == 1
        assert circulation1.licenses_available == 1
        assert circulation1.status == LicensePoolStatus.ACTIVE

        # Check the second book's circulation data
        bibliographic2 = data_parsed[1]
        circulation2 = bibliographic2.circulation

        # This book also has licenses, so status should be ACTIVE
        assert circulation2.licenses_owned == 1
        assert circulation2.status == LicensePoolStatus.ACTIVE

    def test_circulation_data_status_exhausted(
        self, bibliotheca_fixture: BibliothecaAPITestFixture
    ):
        """Test that CirculationData has EXHAUSTED status when licenses_owned is 0."""
        data = bibliotheca_fixture.files.sample_data("item_metadata_list_mini.xml")
        # Replace TotalCopies with 0 to test EXHAUSTED status
        data = data.replace(
            b"<TotalCopies>1</TotalCopies>", b"<TotalCopies>0</TotalCopies>"
        )

        data_parsed = list(ItemListParser().process_all(data))

        # Both books should have EXHAUSTED status
        for bibliographic in data_parsed:
            circulation = bibliographic.circulation
            assert circulation.licenses_owned == 0
            assert circulation.status == LicensePoolStatus.EXHAUSTED


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
        api = cast(MockBibliothecaAPI, provider.api)
        data = bibliotheca_fixture.files.sample_data("item_metadata_single.xml")

        # We can't use bibliotheca_fixture.api because that's not the same object
        # as the one created by the coverage provider.
        api.queue_response(200, content=data)
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

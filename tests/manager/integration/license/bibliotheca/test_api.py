from __future__ import annotations

import json
import random
from datetime import timedelta
from unittest.mock import create_autospec, patch

import pytest
from pymarc.record import Record

from palace.util.datetime_helpers import datetime_utc, utc_now

from palace.manager.api.circulation.dispatcher import CirculationApiDispatcher
from palace.manager.api.circulation.exceptions import (
    PatronHoldLimitReached,
    RemoteInitiatedServerError,
)
from palace.manager.api.circulation.fulfillment import Fulfillment
from palace.manager.api.web_publication_manifest import FindawayManifest
from palace.manager.celery.tasks import apply
from palace.manager.integration.license.bibliotheca.api import BibliothecaAPI
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.resource import Representation
from palace.manager.util.http.exception import (
    BadResponseException,
    RemoteIntegrationException,
)
from palace.manager.util.web_publication_manifest import AudiobookManifest
from tests.manager.integration.license.bibliotheca.conftest import (
    BibliothecaAPITestFixture,
)
from tests.mocks.bibliotheca import MockBibliothecaAPI


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

    @pytest.mark.parametrize(
        "content",
        [
            pytest.param(b"", id="empty"),
            pytest.param(b"   \n  ", id="whitespace"),
        ],
    )
    def test_bibliographic_lookup_empty_response_raises(
        self,
        content: bytes,
        bibliotheca_fixture: BibliothecaAPITestFixture,
    ):
        # Bibliotheca occasionally returns an empty (or whitespace-only) HTTP
        # 200 body. An empty document cannot be parsed as XML, and treating it
        # as "no items returned" would make the circulation updater zero out
        # every requested identifier. So bibliographic_lookup raises a transient
        # RemoteInitiatedServerError instead of returning an empty list or
        # letting lxml's "Document is empty" error propagate.
        db = bibliotheca_fixture.db
        bibliotheca_fixture.api.queue_response(200, content=content)
        identifier = db.identifier()
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            bibliotheca_fixture.api.bibliographic_lookup(identifier)
        assert "empty response body" in str(excinfo.value)
        assert excinfo.value.service_name == bibliotheca_fixture.api.SERVICE_NAME

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
        # The new implementation delegates to BibliothecaCirculationUpdater, which on
        # this on-demand path applies changes synchronously (so the refreshed
        # availability is visible to the caller immediately) and zeros out any titles
        # that Bibliotheca no longer recognises.

        # Create a LicensePool that needs updating.
        edition, pool = db.edition(
            identifier_type=Identifier.BIBLIOTHECA_ID,
            data_source_name=DataSource.BIBLIOTHECA,
            with_license_pool=True,
            collection=bibliotheca_fixture.collection,
        )

        # Put some junk in the pool to verify that the zero-out path works.
        pool.licenses_owned = 10
        pool.licenses_available = 5
        pool.patrons_in_hold_queue = 3
        assert None == pool.last_checked

        # Prepare availability information.
        data = bibliotheca_fixture.files.sample_data("item_metadata_single.xml")
        # Change the ID in the test data so it looks like it's talking
        # about the LicensePool we just created.
        data = data.replace(b"ddf4gr9", pool.identifier.identifier.encode("utf8"))

        # When Bibliotheca returns data for the identifier, the change is applied
        # synchronously in the caller's session rather than queued as an async
        # bibliographic_apply task.
        bibliotheca_fixture.api.queue_response(200, content=data)

        with patch.object(apply, "bibliographic_apply") as mock_apply:
            bibliotheca_fixture.api.update_availability(pool)

        # No async task was queued; the apply happened in-band.
        mock_apply.delay.assert_not_called()

        # The pool immediately reflects the availability reported by Bibliotheca
        # (TotalCopies=1, AvailableCopies=1, OnHoldCount=0 in the sample data).
        assert 1 == pool.licenses_owned
        assert 1 == pool.licenses_available
        assert 0 == pool.patrons_in_hold_queue
        assert pool.last_checked is not None

        # Now test the zero-out path: API returns no data for the identifier.
        data = bibliotheca_fixture.files.sample_data("empty_item_bibliographic.xml")
        bibliotheca_fixture.api.queue_response(200, content=data)

        with patch.object(apply, "bibliographic_apply"):
            bibliotheca_fixture.api.update_availability(pool)

        # The pool should be zeroed out because Bibliotheca didn't mention it.
        assert 0 == pool.licenses_owned
        assert 0 == pool.licenses_available
        assert 0 == pool.patrons_in_hold_queue

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

    @pytest.mark.parametrize(
        "content",
        [
            pytest.param(b"", id="empty"),
            pytest.param(b"   \n  ", id="whitespace"),
        ],
    )
    def test_marc_request_empty_response(
        self,
        content: bytes,
        bibliotheca_fixture: BibliothecaAPITestFixture,
    ):
        # Bibliotheca sometimes returns an empty 200 response for a window
        # with no purchase records. This must yield no records rather than
        # raising the SAXException("no element found") that pymarc would
        # otherwise raise on an empty document.
        start = datetime_utc(2012, 1, 2, 3, 4, 5)
        end = datetime_utc(2014, 5, 6, 7, 8, 9)
        bibliotheca_fixture.api.queue_response(200, content=content)
        records = [x for x in bibliotheca_fixture.api.marc_request(start, end, 10, 20)]
        assert records == []

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

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Callable

import pytest

from api.circulation import CirculationAPI
from api.circulation_exceptions import *
from api.odilo import (
    OdiloAPI,
    OdiloBibliographicCoverageProvider,
    OdiloCirculationMonitor,
    OdiloRepresentationExtractor,
)
from core.metadata_layer import TimestampData
from core.model import (
    Classification,
    Contributor,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    Representation,
)
from core.util.datetime_helpers import datetime_utc, utc_now
from core.util.http import BadResponseException
from tests.api.mockapi.odilo import MockOdiloAPI
from tests.core.mock import MockRequestsResponse

if TYPE_CHECKING:
    from ..fixtures.api_odilo_files import OdiloFilesFixture
    from ..fixtures.authenticator import AuthProviderFixture
    from ..fixtures.database import DatabaseTransactionFixture


class OdiloFixture:
    PIN = "c4ca4238a0b923820dcc509a6f75849b"
    RECORD_ID = "00010982"

    def sample_data(self, filename):
        return self.files.sample_data(filename)

    def sample_json(self, filename):
        data = self.sample_data(filename)
        return data, json.loads(data)

    def error_message(self, error_code, message=None, token=None):
        """Create a JSON document that simulates the message served by
        Odilo given a certain error condition.
        """
        message = message or self.db.fresh_str()
        token = token or self.db.fresh_str()
        data = dict(errorCode=error_code, message=message, token=token)
        return json.dumps(data)

    def __init__(self, db: DatabaseTransactionFixture, files: OdiloFilesFixture):
        library = db.default_library()
        self.files = files
        self.db = db
        self.patron = db.patron()
        self.patron.authorization_identifier = "0001000265"
        self.collection = MockOdiloAPI.mock_collection(db.session)
        self.circulation = CirculationAPI(
            db.session, library, api_map={ExternalIntegration.ODILO: MockOdiloAPI}
        )
        self.api = self.circulation.api_for_collection[self.collection.id]
        self.edition, self.licensepool = db.edition(
            data_source_name=DataSource.ODILO,
            identifier_type=Identifier.ODILO_ID,
            collection=self.collection,
            identifier_id=self.RECORD_ID,
            with_license_pool=True,
        )


@pytest.fixture(scope="function")
def odilo(
    db: DatabaseTransactionFixture, api_odilo_files_fixture: OdiloFilesFixture
) -> OdiloFixture:
    return OdiloFixture(db, api_odilo_files_fixture)


class TestOdiloAPI:
    def test_token_post_success(self, odilo: OdiloFixture):
        odilo.api.queue_response(200, content="some content")
        response = odilo.api.token_post(odilo.db.fresh_url(), "the payload")
        assert 200 == response.status_code, (
            "Status code != 200 --> %i" % response.status_code
        )
        assert odilo.api.access_token_response.content == response.content
        odilo.api.log.info("Test token post success ok!")

    def test_get_success(self, odilo: OdiloFixture):
        odilo.api.queue_response(200, content="some content")
        status_code, headers, content = odilo.api.get(odilo.db.fresh_url(), {})
        assert 200 == status_code
        assert b"some content" == content
        odilo.api.log.info("Test get success ok!")

    def test_401_on_get_refreshes_bearer_token(self, odilo: OdiloFixture):
        assert "bearer token" == odilo.api.token

        # We try to GET and receive a 401.
        odilo.api.queue_response(401)

        # We refresh the bearer token. (This happens in
        # MockOdiloAPI.token_post, so we don't mock the response
        # in the normal way.)
        odilo.api.access_token_response = odilo.api.mock_access_token_response(
            "new bearer token"
        )

        # Then we retry the GET and it succeeds this time.
        odilo.api.queue_response(200, content="at last, the content")
        status_code, headers, content = odilo.api.get(odilo.db.fresh_url(), {})

        assert 200 == status_code
        assert b"at last, the content" == content

        # The bearer token has been updated.
        assert "new bearer token" == odilo.api.token

        odilo.api.log.info("Test 401 on get refreshes bearer token ok!")

    def test_credential_refresh_success(self, odilo: OdiloFixture):
        """Verify the process of refreshing the Odilo bearer token."""
        credential = odilo.api.credential_object(lambda x: x)
        assert "bearer token" == credential.credential
        assert odilo.api.token == credential.credential

        odilo.api.access_token_response = odilo.api.mock_access_token_response(
            "new bearer token"
        )
        odilo.api.refresh_creds(credential)
        assert "new bearer token" == credential.credential
        assert odilo.api.token == credential.credential

        # By default, the access token's 'expiresIn' value is -1,
        # indicating that the token will never expire.
        #
        # To reflect this fact, credential.expires is set to None.
        assert None == credential.expires

        # But a token may specify a specific expiration time,
        # which is used to set a future value for credential.expires.
        odilo.api.access_token_response = odilo.api.mock_access_token_response(
            "new bearer token 2", 1000
        )
        odilo.api.refresh_creds(credential)
        assert "new bearer token 2" == credential.credential
        assert odilo.api.token == credential.credential
        assert credential.expires > utc_now()

    def test_credential_refresh_failure(self, odilo: OdiloFixture):
        """Verify that a useful error message results when the Odilo bearer
        token cannot be refreshed, since this is the most likely point
        of failure on a new setup.
        """
        odilo.api.access_token_response = MockRequestsResponse(
            200, {"Content-Type": "text/html"}, "Hi, this is the website, not the API."
        )
        credential = odilo.api.credential_object(lambda x: x)
        with pytest.raises(BadResponseException) as excinfo:
            odilo.api.refresh_creds(credential)
        assert "Bad response from " in str(excinfo.value)
        assert (
            "may not be the right base URL. Response document was: 'Hi, this is the website, not the API.'"
            in str(excinfo.value)
        )

        # Also test a 400 response code.
        odilo.api.access_token_response = MockRequestsResponse(
            400,
            {"Content-Type": "application/json"},
            json.dumps(dict(errors=[dict(description="Oops")])),
        )
        with pytest.raises(BadResponseException) as excinfo:
            odilo.api.refresh_creds(credential)
        assert "Bad response from" in str(excinfo.value)
        assert "Oops" in str(excinfo.value)

        # If there's a 400 response but no error information,
        # the generic error message is used.
        odilo.api.access_token_response = MockRequestsResponse(
            400, {"Content-Type": "application/json"}, json.dumps(dict())
        )
        with pytest.raises(BadResponseException) as excinfo:
            odilo.api.refresh_creds(credential)
        assert "Bad response from" in str(excinfo.value)
        assert "may not be the right base URL." in str(excinfo.value)

    def test_401_after_token_refresh_raises_error(self, odilo: OdiloFixture):
        assert "bearer token" == odilo.api.token

        # We try to GET and receive a 401.
        odilo.api.queue_response(401)

        # We refresh the bearer token.
        odilo.api.access_token_response = odilo.api.mock_access_token_response(
            "new bearer token"
        )

        # Then we retry the GET but we get another 401.
        odilo.api.queue_response(401)

        # That raises a BadResponseException
        with pytest.raises(BadResponseException) as excinfo:
            odilo.api.get(odilo.db.fresh_url(), {})
        assert "Something's wrong with the Odilo OAuth Bearer Token!" in str(
            excinfo.value
        )

        # The bearer token has been updated.
        assert "new bearer token" == odilo.api.token

    def test_external_integration(self, odilo: OdiloFixture):
        assert odilo.collection.external_integration == odilo.api.external_integration(
            odilo.db.session
        )

    def test__run_self_tests(
        self,
        odilo: OdiloFixture,
        create_simple_auth_integration: Callable[..., AuthProviderFixture],
    ):
        """Verify that OdiloAPI._run_self_tests() calls the right
        methods.
        """

        class Mock(MockOdiloAPI):
            "Mock every method used by OdiloAPI._run_self_tests."

            def __init__(self, _db, collection):
                """Stop the default constructor from running."""
                self._db = _db
                self.collection_id = collection.id

            # First we will call check_creds() to get a fresh credential.
            mock_credential = object()

            def check_creds(self, force_refresh=False):
                self.check_creds_called_with = force_refresh
                return self.mock_credential

            # Finally, for every library associated with this
            # collection, we'll call get_patron_checkouts() using
            # the credentials of that library's test patron.
            mock_patron_checkouts = object()
            get_patron_checkouts_called_with = []

            def get_patron_checkouts(self, patron, pin):
                self.get_patron_checkouts_called_with.append((patron, pin))
                return self.mock_patron_checkouts

        # Now let's make sure two Libraries have access to this
        # Collection -- one library with a default patron and one
        # without.
        no_default_patron = odilo.db.library(name="no patron")
        odilo.collection.libraries.append(no_default_patron)

        with_default_patron = odilo.db.default_library()
        create_simple_auth_integration(with_default_patron)

        # Now that everything is set up, run the self-test.
        api = Mock(odilo.db.session, odilo.collection)
        results = sorted(api._run_self_tests(odilo.db.session), key=lambda x: x.name)
        loans_failure, sitewide, loans_success = results

        # Make sure all three tests were run and got the expected result.
        #

        # We got a sitewide access token.
        assert "Obtaining a sitewide access token" == sitewide.name
        assert True == sitewide.success
        assert api.mock_credential == sitewide.result
        assert True == api.check_creds_called_with

        # We got the default patron's checkouts for the library that had
        # a default patron configured.
        assert (
            "Viewing the active loans for the test patron for library %s"
            % with_default_patron.name
            == loans_success.name
        )
        assert True == loans_success.success
        # get_patron_checkouts was only called once.
        [(patron, pin)] = api.get_patron_checkouts_called_with
        assert "username1" == patron.authorization_identifier
        assert "password1" == pin
        assert api.mock_patron_checkouts == loans_success.result

        # We couldn't get a patron access token for the other library.
        assert (
            "Acquiring test patron credentials for library %s" % no_default_patron.name
            == loans_failure.name
        )
        assert False == loans_failure.success
        assert "Library has no test patron configured." == str(loans_failure.exception)

    def test_run_self_tests_short_circuit(self, odilo: OdiloFixture):
        """If OdiloAPI.check_creds can't get credentials, the rest of
        the self-tests aren't even run.

        This probably doesn't matter much, because if check_creds doesn't
        work we won't be able to instantiate the OdiloAPI class.
        """

        def explode(*args, **kwargs):
            raise Exception("Failure!")

        odilo.api.check_creds = explode

        # Only one test will be run.
        [check_creds] = odilo.api._run_self_tests(odilo.db.session)
        assert "Failure!" == str(check_creds.exception)


class TestOdiloCirculationAPI:
    #################
    # General tests
    #################

    # Test 404 Not Found --> patron not found --> 'patronNotFound'
    def test_01_patron_not_found(self, odilo: OdiloFixture):
        patron_not_found_data, patron_not_found_json = odilo.sample_json(
            "error_patron_not_found.json"
        )
        odilo.api.queue_response(404, content=patron_not_found_json)

        patron = odilo.db.patron()
        patron.authorization_identifier = "no such patron"
        pytest.raises(
            PatronNotFoundOnRemote,  # type: ignore
            odilo.api.checkout,
            patron,
            odilo.PIN,
            odilo.licensepool,
            "ACSM_EPUB",
        )
        odilo.api.log.info("Test patron not found ok!")

    # Test 404 Not Found --> record not found --> 'ERROR_DATA_NOT_FOUND'
    def test_02_data_not_found(self, odilo: OdiloFixture):
        data_not_found_data, data_not_found_json = odilo.sample_json(
            "error_data_not_found.json"
        )
        odilo.api.queue_response(404, content=data_not_found_json)

        odilo.licensepool.identifier.identifier = "12345678"
        pytest.raises(
            NotFoundOnRemote,  # type: ignore
            odilo.api.checkout,
            odilo.patron,
            odilo.PIN,
            odilo.licensepool,
            "ACSM_EPUB",
        )
        odilo.api.log.info("Test resource not found on remote ok!")

    def test_make_absolute_url(self, odilo: OdiloFixture):

        # A relative URL is made absolute using the API's base URL.
        relative = "/relative-url"
        absolute = odilo.api._make_absolute_url(relative)
        assert absolute == odilo.api.library_api_base_url.decode("utf-8") + relative

        # An absolute URL is not modified.
        for protocol in ("http", "https"):
            already_absolute = "%s://example.com/" % protocol
            assert already_absolute == odilo.api._make_absolute_url(already_absolute)

    #################
    # Checkout tests
    #################

    # Test 400 Bad Request --> Invalid format for that resource
    def test_11_checkout_fake_format(self, odilo: OdiloFixture):
        odilo.api.queue_response(400, content="")
        pytest.raises(
            NoAcceptableFormat,  # type: ignore
            odilo.api.checkout,
            odilo.patron,
            odilo.PIN,
            odilo.licensepool,
            "FAKE_FORMAT",
        )
        odilo.api.log.info("Test invalid format for resource ok!")

    def test_12_checkout_acsm_epub(self, odilo: OdiloFixture):
        checkout_data, checkout_json = odilo.sample_json("checkout_acsm_epub_ok.json")
        odilo.api.queue_response(200, content=checkout_json)
        self.perform_and_validate_checkout("ACSM_EPUB", odilo)

    def test_13_checkout_acsm_pdf(self, odilo: OdiloFixture):
        checkout_data, checkout_json = odilo.sample_json("checkout_acsm_pdf_ok.json")
        odilo.api.queue_response(200, content=checkout_json)
        self.perform_and_validate_checkout("ACSM_PDF", odilo)

    def test_14_checkout_ebook_streaming(self, odilo: OdiloFixture):
        checkout_data, checkout_json = odilo.sample_json(
            "checkout_ebook_streaming_ok.json"
        )
        odilo.api.queue_response(200, content=checkout_json)
        self.perform_and_validate_checkout("EBOOK_STREAMING", odilo)

    def test_mechanism_set_on_borrow(self, odilo: OdiloFixture):
        """The delivery mechanism for an Odilo title is set on checkout."""
        assert OdiloAPI.SET_DELIVERY_MECHANISM_AT == OdiloAPI.BORROW_STEP

    def perform_and_validate_checkout(self, internal_format, odilo: OdiloFixture):
        loan_info = odilo.api.checkout(
            odilo.patron, odilo.PIN, odilo.licensepool, internal_format
        )
        assert loan_info, "LoanInfo null --> checkout failed!"
        odilo.api.log.info("Loan ok: %s" % loan_info.identifier)

    #################
    # Fulfill tests
    #################

    def test_21_fulfill_acsm_epub(self, odilo: OdiloFixture):
        checkout_data, checkout_json = odilo.sample_json("patron_checkouts.json")
        odilo.api.queue_response(200, content=checkout_json)

        acsm_data = odilo.sample_data("fulfill_ok_acsm_epub.acsm")
        odilo.api.queue_response(200, content=acsm_data)

        fulfillment_info = self.fulfill("ACSM_EPUB", odilo)
        assert fulfillment_info.content_type[0] == Representation.EPUB_MEDIA_TYPE
        assert fulfillment_info.content_type[1] == DeliveryMechanism.ADOBE_DRM

    def test_22_fulfill_acsm_pdf(self, odilo: OdiloFixture):
        checkout_data, checkout_json = odilo.sample_json("patron_checkouts.json")
        odilo.api.queue_response(200, content=checkout_json)

        acsm_data = odilo.sample_data("fulfill_ok_acsm_pdf.acsm")
        odilo.api.queue_response(200, content=acsm_data)

        fulfillment_info = self.fulfill("ACSM_PDF", odilo)
        assert fulfillment_info.content_type[0] == Representation.PDF_MEDIA_TYPE
        assert fulfillment_info.content_type[1] == DeliveryMechanism.ADOBE_DRM

    def test_23_fulfill_ebook_streaming(self, odilo: OdiloFixture):
        checkout_data, checkout_json = odilo.sample_json("patron_checkouts.json")
        odilo.api.queue_response(200, content=checkout_json)

        odilo.licensepool.identifier.identifier = "00011055"
        fulfillment_info = self.fulfill("EBOOK_STREAMING", odilo)
        assert fulfillment_info.content_type[0] == Representation.TEXT_HTML_MEDIA_TYPE
        assert (
            fulfillment_info.content_type[1]
            == DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE
        )

    def fulfill(self, internal_format, odilo: OdiloFixture):
        fulfillment_info = odilo.api.fulfill(
            odilo.patron, odilo.PIN, odilo.licensepool, internal_format
        )
        assert fulfillment_info, "Cannot Fulfill !!"

        if fulfillment_info.content_link:
            odilo.api.log.info("Fulfill link: %s" % fulfillment_info.content_link)
        if fulfillment_info.content:
            odilo.api.log.info("Fulfill content: %s" % fulfillment_info.content)

        return fulfillment_info

    #################
    # Hold tests
    #################

    def test_31_already_on_hold(self, odilo: OdiloFixture):
        already_on_hold_data, already_on_hold_json = odilo.sample_json(
            "error_hold_already_in_hold.json"
        )
        odilo.api.queue_response(403, content=already_on_hold_json)

        pytest.raises(
            AlreadyOnHold,  # type: ignore
            odilo.api.place_hold,
            odilo.patron,
            odilo.PIN,
            odilo.licensepool,
            "ejcepas@odilotid.es",
        )

        odilo.api.log.info("Test hold already on hold ok!")

    def test_32_place_hold(self, odilo: OdiloFixture):
        hold_ok_data, hold_ok_json = odilo.sample_json("place_hold_ok.json")
        odilo.api.queue_response(200, content=hold_ok_json)

        hold_info = odilo.api.place_hold(
            odilo.patron, odilo.PIN, odilo.licensepool, "ejcepas@odilotid.es"
        )
        assert hold_info, "HoldInfo null --> place hold failed!"
        odilo.api.log.info("Hold ok: %s" % hold_info.identifier)

    #################
    # Patron Activity tests
    #################

    def test_41_patron_activity_invalid_patron(self, odilo: OdiloFixture):
        patron_not_found_data, patron_not_found_json = odilo.sample_json(
            "error_patron_not_found.json"
        )
        odilo.api.queue_response(404, content=patron_not_found_json)

        pytest.raises(
            PatronNotFoundOnRemote, odilo.api.patron_activity, odilo.patron, odilo.PIN  # type: ignore
        )

        odilo.api.log.info("Test patron activity --> invalid patron ok!")

    def test_42_patron_activity(self, odilo: OdiloFixture):
        patron_checkouts_data, patron_checkouts_json = odilo.sample_json(
            "patron_checkouts.json"
        )
        patron_holds_data, patron_holds_json = odilo.sample_json("patron_holds.json")
        odilo.api.queue_response(200, content=patron_checkouts_json)
        odilo.api.queue_response(200, content=patron_holds_json)

        loans_and_holds = odilo.api.patron_activity(odilo.patron, odilo.PIN)
        assert loans_and_holds
        assert 12 == len(loans_and_holds)
        odilo.api.log.info("Test patron activity ok !!")

    #################
    # Checkin tests
    #################

    def test_51_checkin_patron_not_found(self, odilo: OdiloFixture):
        patron_not_found_data, patron_not_found_json = odilo.sample_json(
            "error_patron_not_found.json"
        )
        odilo.api.queue_response(404, content=patron_not_found_json)

        pytest.raises(
            PatronNotFoundOnRemote,  # type: ignore
            odilo.api.checkin,
            odilo.patron,
            odilo.PIN,
            odilo.licensepool,
        )

        odilo.api.log.info("Test checkin --> invalid patron ok!")

    def test_52_checkin_checkout_not_found(self, odilo: OdiloFixture):
        checkout_not_found_data, checkout_not_found_json = odilo.sample_json(
            "error_checkout_not_found.json"
        )
        odilo.api.queue_response(404, content=checkout_not_found_json)

        pytest.raises(
            NotCheckedOut, odilo.api.checkin, odilo.patron, odilo.PIN, odilo.licensepool  # type: ignore
        )

        odilo.api.log.info("Test checkin --> invalid checkout ok!")

    def test_53_checkin(self, odilo: OdiloFixture):
        checkout_data, checkout_json = odilo.sample_json("patron_checkouts.json")
        odilo.api.queue_response(200, content=checkout_json)

        checkin_data, checkin_json = odilo.sample_json("checkin_ok.json")
        odilo.api.queue_response(200, content=checkin_json)

        response = odilo.api.checkin(odilo.patron, odilo.PIN, odilo.licensepool)
        assert response.status_code == 200, (
            "Response code != 200, cannot perform checkin for record: "
            + odilo.licensepool.identifier.identifier
            + " patron: "
            + odilo.patron.authorization_identifier
        )

        checkout_returned = response.json()

        assert checkout_returned
        assert "4318" == checkout_returned["id"]
        odilo.api.log.info("Checkout returned: %s" % checkout_returned["id"])

    #################
    # Patron Activity tests
    #################

    def test_61_return_hold_patron_not_found(self, odilo: OdiloFixture):
        patron_not_found_data, patron_not_found_json = odilo.sample_json(
            "error_patron_not_found.json"
        )
        odilo.api.queue_response(404, content=patron_not_found_json)

        pytest.raises(
            PatronNotFoundOnRemote,  # type: ignore
            odilo.api.release_hold,
            odilo.patron,
            odilo.PIN,
            odilo.licensepool,
        )

        odilo.api.log.info("Test release hold --> invalid patron ok!")

    def test_62_return_hold_not_found(self, odilo: OdiloFixture):
        holds_data, holds_json = odilo.sample_json("patron_holds.json")
        odilo.api.queue_response(200, content=holds_json)

        checkin_data, checkin_json = odilo.sample_json("error_hold_not_found.json")
        odilo.api.queue_response(404, content=checkin_json)

        response = odilo.api.release_hold(odilo.patron, odilo.PIN, odilo.licensepool)
        assert response == True, (
            "Cannot release hold, response false "
            + odilo.licensepool.identifier.identifier
            + " patron: "
            + odilo.patron.authorization_identifier
        )

        odilo.api.log.info(
            "Hold returned: %s" % odilo.licensepool.identifier.identifier
        )

    def test_63_return_hold(self, odilo: OdiloFixture):
        holds_data, holds_json = odilo.sample_json("patron_holds.json")
        odilo.api.queue_response(200, content=holds_json)

        release_hold_ok_data, release_hold_ok_json = odilo.sample_json(
            "release_hold_ok.json"
        )
        odilo.api.queue_response(200, content=release_hold_ok_json)

        response = odilo.api.release_hold(odilo.patron, odilo.PIN, odilo.licensepool)
        assert response == True, (
            "Cannot release hold, response false "
            + odilo.licensepool.identifier.identifier
            + " patron: "
            + odilo.patron.authorization_identifier
        )

        odilo.api.log.info(
            "Hold returned: %s" % odilo.licensepool.identifier.identifier
        )


class TestOdiloDiscoveryAPI:
    def test_run(self, odilo: OdiloFixture):
        """Verify that running the OdiloCirculationMonitor calls all_ids()."""

        class Mock(OdiloCirculationMonitor):
            def all_ids(self, modification_date=None):
                self.called_with = modification_date
                return 30, 15

        # The first time run() is called, all_ids is called with
        # a modification_date of None.
        monitor = Mock(odilo.db.session, odilo.collection, api_class=MockOdiloAPI)
        monitor.run()
        assert None == monitor.called_with
        progress = monitor.timestamp()
        completed = progress.finish

        # The return value of all_ids() is used to populate the
        # achievements field.
        assert "Updated records: 30. New records: 15." == progress.achievements

        # The second time run() is called, all_ids() is called with a
        # modification date five minutes earlier than the completion
        # of the last run.
        monitor.run()
        expect = completed - monitor.OVERLAP
        assert (expect - monitor.called_with).total_seconds() < 2

    def test_all_ids_with_date(self, odilo: OdiloFixture):
        # TODO: This tests that all_ids doesn't crash when you pass in
        # a date. It doesn't test anything about all_ids except the
        # return value.
        monitor = OdiloCirculationMonitor(
            odilo.db.session, odilo.collection, api_class=MockOdiloAPI
        )
        assert monitor, "Monitor null !!"
        assert ExternalIntegration.ODILO == monitor.protocol, "Wat??"

        records_metadata_data, records_metadata_json = odilo.sample_json(
            "records_metadata.json"
        )
        monitor.api.queue_response(200, content=records_metadata_data)

        availability_data = odilo.sample_data("record_availability.json")
        for record in records_metadata_json:
            monitor.api.queue_response(200, content=availability_data)

        monitor.api.queue_response(200, content="[]")  # No more resources retrieved

        timestamp = TimestampData(start=datetime_utc(2017, 9, 1))
        updated, new = monitor.all_ids(None)
        assert 10 == updated
        assert 10 == new

        odilo.api.log.info("Odilo circulation monitor with date finished ok!!")

    def test_all_ids_without_date(self, odilo: OdiloFixture):
        # TODO: This tests that all_ids doesn't crash when you pass in
        # an empty date. It doesn't test anything about all_ids except the
        # return value.

        monitor = OdiloCirculationMonitor(
            odilo.db.session, odilo.collection, api_class=MockOdiloAPI
        )
        assert monitor, "Monitor null !!"
        assert ExternalIntegration.ODILO == monitor.protocol, "Wat??"

        records_metadata_data, records_metadata_json = odilo.sample_json(
            "records_metadata.json"
        )
        monitor.api.queue_response(200, content=records_metadata_data)

        availability_data = odilo.sample_data("record_availability.json")
        for record in records_metadata_json:
            monitor.api.queue_response(200, content=availability_data)

        monitor.api.queue_response(200, content="[]")  # No more resources retrieved

        updated, new = monitor.all_ids(datetime_utc(2017, 9, 1))
        assert 10 == updated
        assert 10 == new

        odilo.api.log.info("Odilo circulation monitor without date finished ok!!")


class OdiloCoverageFixture(OdiloFixture):
    def __init__(self, db: DatabaseTransactionFixture, files: OdiloFilesFixture):
        super().__init__(db, files)
        self.provider = OdiloBibliographicCoverageProvider(
            self.collection, api_class=MockOdiloAPI
        )
        self.api = self.provider.api


@pytest.fixture(scope="function")
def odilo_coverage(
    db: DatabaseTransactionFixture, api_odilo_files_fixture: OdiloFilesFixture
) -> OdiloCoverageFixture:
    return OdiloCoverageFixture(db, api_odilo_files_fixture)


class TestOdiloBibliographicCoverageProvider:
    def test_process_item(self, odilo_coverage: OdiloCoverageFixture):
        record_metadata, record_metadata_json = odilo_coverage.sample_json(
            "odilo_metadata.json"
        )
        odilo_coverage.api.queue_response(200, content=record_metadata_json)
        availability, availability_json = odilo_coverage.sample_json(
            "odilo_availability.json"
        )
        odilo_coverage.api.queue_response(200, content=availability)

        identifier, made_new = odilo_coverage.provider.process_item("00010982")

        # Check that the Identifier returned has the right .type and .identifier.
        assert identifier, "Problem while testing process item !!!"
        assert identifier.type == Identifier.ODILO_ID
        assert identifier.identifier == "00010982"

        # Check that metadata and availability information were imported properly
        [pool] = identifier.licensed_through
        assert "Busy Brownies" == pool.work.title

        assert 2 == pool.licenses_owned
        assert 1 == pool.licenses_available
        assert 2 == pool.patrons_in_hold_queue
        assert 1 == pool.licenses_reserved

        names = [x.delivery_mechanism.name for x in pool.delivery_mechanisms]
        assert sorted(
            [
                Representation.EPUB_MEDIA_TYPE
                + " ("
                + DeliveryMechanism.ADOBE_DRM
                + ")",
                Representation.TEXT_HTML_MEDIA_TYPE
                + " ("
                + DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE
                + ")",
            ]
        ) == sorted(names)

        # Check that handle_success was called --> A Work was created and made presentation ready.
        assert True == pool.work.presentation_ready

        odilo_coverage.api.log.info("Testing process item finished ok !!")

    def test_process_inactive_item(self, odilo_coverage: OdiloCoverageFixture):
        record_metadata, record_metadata_json = odilo_coverage.sample_json(
            "odilo_metadata_inactive.json"
        )
        odilo_coverage.api.queue_response(200, content=record_metadata_json)
        availability, availability_json = odilo_coverage.sample_json(
            "odilo_availability_inactive.json"
        )
        odilo_coverage.api.queue_response(200, content=availability)

        identifier, made_new = odilo_coverage.provider.process_item("00011135")

        # Check that the Identifier returned has the right .type and .identifier.
        assert identifier, "Problem while testing process inactive item !!!"
        assert identifier.type == Identifier.ODILO_ID
        assert identifier.identifier == "00011135"

        [pool] = identifier.licensed_through
        assert (
            "!Tention A Story of Boy-Life during the Peninsular War" == pool.work.title
        )

        # Check work not available
        assert 0 == pool.licenses_owned
        assert 0 == pool.licenses_available

        assert True == pool.work.presentation_ready

        odilo_coverage.api.log.info("Testing process item inactive finished ok !!")


class TestOdiloRepresentationExtractor:
    def test_book_info_with_metadata(self, odilo: OdiloFixture):
        # Tests that can convert an odilo json block into a Metadata object.

        raw, book_json = odilo.sample_json("odilo_metadata.json")
        raw, availability = odilo.sample_json("odilo_availability.json")
        metadata, active = OdiloRepresentationExtractor.record_info_to_metadata(
            book_json, availability
        )

        assert "Busy Brownies" == metadata.title
        assert (
            " (The Classic Fantasy Literature of Elves for Children)"
            == metadata.subtitle
        )
        assert "eng" == metadata.language
        assert Edition.BOOK_MEDIUM == metadata.medium
        assert (
            "The Classic Fantasy Literature for Children written in 1896 retold for Elves adventure."
            == metadata.series
        )
        assert "1" == metadata.series_position
        assert "ANBOCO" == metadata.publisher
        assert 2013 == metadata.published.year
        assert 2 == metadata.published.month
        assert 2 == metadata.published.day
        assert 2017 == metadata.data_source_last_updated.year
        assert 3 == metadata.data_source_last_updated.month
        assert 10 == metadata.data_source_last_updated.day
        # Related IDs.
        assert (Identifier.ODILO_ID, "00010982") == (
            metadata.primary_identifier.type,
            metadata.primary_identifier.identifier,
        )
        ids = [(x.type, x.identifier) for x in metadata.identifiers]
        assert [
            (Identifier.ISBN, "9783736418837"),
            (Identifier.ODILO_ID, "00010982"),
        ] == sorted(ids)

        subjects = sorted(metadata.subjects, key=lambda x: x.identifier)
        weight = Classification.TRUSTED_DISTRIBUTOR_WEIGHT
        assert [
            ("Children", "tag", weight),
            ("Classics", "tag", weight),
            ("FIC004000", "BISAC", weight),
            ("Fantasy", "tag", weight),
            ("K-12", "Grade level", weight),
            ("LIT009000", "BISAC", weight),
            ("YAF019020", "BISAC", weight),
        ] == [(x.identifier, x.type, x.weight) for x in subjects]

        [author] = metadata.contributors
        assert "Veale, E." == author.sort_name
        assert "E. Veale" == author.display_name
        assert [Contributor.AUTHOR_ROLE] == author.roles

        # Available formats.
        [acsm_epub, ebook_streaming] = sorted(
            metadata.circulation.formats, key=lambda x: x.content_type
        )
        assert Representation.EPUB_MEDIA_TYPE == acsm_epub.content_type
        assert DeliveryMechanism.ADOBE_DRM == acsm_epub.drm_scheme

        assert Representation.TEXT_HTML_MEDIA_TYPE == ebook_streaming.content_type
        assert (
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE == ebook_streaming.drm_scheme
        )

        # Links to various resources.
        image, thumbnail, description = sorted(metadata.links, key=lambda x: x.rel)

        assert Hyperlink.IMAGE == image.rel
        assert (
            "http://pruebasotk.odilotk.es/public/OdiloPlace_eduDistUS/pg54159.jpg"
            == image.href
        )

        assert Hyperlink.THUMBNAIL_IMAGE == thumbnail.rel
        assert (
            "http://pruebasotk.odilotk.es/public/OdiloPlace_eduDistUS/pg54159_225x318.jpg"
            == thumbnail.href
        )

        assert Hyperlink.DESCRIPTION == description.rel
        assert description.content.startswith(
            "All the <b>Brownies</b> had promised to help, and when a Brownie undertakes a thing he works as busily"
        )

        circulation = metadata.circulation
        assert 2 == circulation.licenses_owned
        assert 1 == circulation.licenses_available
        assert 2 == circulation.patrons_in_hold_queue
        assert 1 == circulation.licenses_reserved

        odilo.api.log.info("Testing book info with metadata finished ok !!")

    def test_book_info_missing_metadata(self, odilo: OdiloFixture):
        # Verify that we properly handle missing metadata from Odilo.
        raw, book_json = odilo.sample_json("odilo_metadata.json")

        # This was seen in real data.
        book_json["series"] = " "
        book_json["seriesPosition"] = " "

        metadata, active = OdiloRepresentationExtractor.record_info_to_metadata(
            book_json, {}
        )
        assert None == metadata.series
        assert None == metadata.series_position

    def test_default_language_spanish(self, odilo: OdiloFixture):
        """Since Odilo primarily distributes Spanish-language titles, if a
        title comes in with no specified language, we assume it's
        Spanish.
        """
        raw, book_json = odilo.sample_json("odilo_metadata.json")
        raw, availability = odilo.sample_json("odilo_availability.json")
        del book_json["language"]
        metadata, active = OdiloRepresentationExtractor.record_info_to_metadata(
            book_json, availability
        )
        assert "spa" == metadata.language

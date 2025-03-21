from __future__ import annotations

import json
import os
import random
from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock, create_autospec, patch

import pytest
from requests import Response

from palace.manager.api.circulation import (
    FetchFulfillment,
    Fulfillment,
    HoldInfo,
    LoanInfo,
    RedirectFulfillment,
)
from palace.manager.api.circulation_exceptions import (
    AlreadyOnHold,
    CannotFulfill,
    CannotHold,
    CannotLoan,
    CannotRenew,
    FormatNotAvailable,
    FulfilledOnIncompatiblePlatform,
    NoAcceptableFormat,
    NoAvailableCopies,
    PatronAuthorizationFailedException,
    PatronHoldLimitReached,
    PatronLoanLimitReached,
)
from palace.manager.api.config import Configuration
from palace.manager.api.overdrive.api import OverdriveAPI
from palace.manager.api.overdrive.constants import OverdriveConstants
from palace.manager.api.overdrive.fulfillment import OverdriveManifestFulfillment
from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.core.exceptions import BasePalaceException
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.resource import Representation
from palace.manager.util import base64
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from palace.manager.util.http import BadResponseException
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture
from tests.fixtures.overdrive import OverdriveAPIFixture
from tests.fixtures.webserver import MockAPIServer, MockAPIServerResponse
from tests.mocks.mock import MockHTTPClient, MockRequestsResponse
from tests.mocks.overdrive import MockOverdriveAPI


class TestOverdriveAPI:
    def test_patron_activity_exception_collection_none(
        self,
        overdrive_api_fixture: OverdriveAPIFixture,
        db: DatabaseTransactionFixture,
    ):
        api = OverdriveAPI(db.session, overdrive_api_fixture.collection)
        db.session.delete(overdrive_api_fixture.collection)
        patron = db.patron()
        with pytest.raises(BasePalaceException) as excinfo:
            api.sync_patron_activity(patron, "pin")
        assert "No collection available for Overdrive patron activity." in str(
            excinfo.value
        )

    def test_errors_not_retried(
        self,
        overdrive_api_fixture: OverdriveAPIFixture,
        mock_web_server: MockAPIServer,
    ):
        session = overdrive_api_fixture.db.session
        library = overdrive_api_fixture.db.default_library()
        collection = MockOverdriveAPI.mock_collection(session, library)

        # Enqueue a response for the request that the server will make for a token.
        _r = MockAPIServerResponse()
        _r.status_code = 200
        _r.set_content(
            b"""{
            "access_token": "x",
            "expires_in": 23
        }
        """
        )
        mock_web_server.enqueue_response("POST", "/oauth/token", _r)

        api = OverdriveAPI(session, collection)
        api._hosts["oauth_host"] = mock_web_server.url("/oauth")

        # Try a get() call for each error code
        for code in [404]:
            _r = MockAPIServerResponse()
            _r.status_code = code
            mock_web_server.enqueue_response("GET", "/a/b/c", _r)
            _status, _, _ = api.get(mock_web_server.url("/a/b/c"))
            assert _status == code

        for code in [400, 403, 500, 501, 502, 503]:
            _r = MockAPIServerResponse()
            _r.status_code = code

            # The default is to retry 5 times, so enqueue 5 responses.
            for i in range(0, 6):
                mock_web_server.enqueue_response("GET", "/a/b/c", _r)
            try:
                api.get(mock_web_server.url("/a/b/c"))
            except BadResponseException:
                pass

        # Exactly one request was made for each error code, plus one for a token
        assert len(mock_web_server.requests()) == 8

    def test_constructor_makes_no_requests(
        self,
        overdrive_api_fixture: OverdriveAPIFixture,
    ):
        session = overdrive_api_fixture.db.session
        library = overdrive_api_fixture.db.default_library()
        # Invoking the OverdriveAPI constructor does not, by itself,
        # make any HTTP requests.
        collection = MockOverdriveAPI.mock_collection(session, library)

        exception_message = "This is a unit test, you can't make HTTP requests!"
        with (
            patch.object(
                OverdriveAPI, "_do_get", side_effect=Exception(exception_message)
            ),
            patch.object(
                OverdriveAPI, "_do_post", side_effect=Exception(exception_message)
            ),
        ):
            # Make sure that the constructor doesn't make any requests.
            api = OverdriveAPI(session, collection)

            # Attempting to access ._client_oauth_token or .collection_token _will_
            # try to make an HTTP request.
            with pytest.raises(Exception, match=exception_message):
                api.collection_token

            with pytest.raises(Exception, match=exception_message):
                api._client_oauth_token

    def test_ils_name(self, overdrive_api_fixture: OverdriveAPIFixture):
        fixture = overdrive_api_fixture
        transaction = overdrive_api_fixture.db

        """The 'ils_name' setting (defined in
        MockOverdriveAPI.mock_collection) is available through
        OverdriveAPI.ils_name().
        """
        assert "e" == fixture.api.ils_name(transaction.default_library())

        # The value must be explicitly set for a given library, or
        # else the default will be used.
        l2 = transaction.library()
        assert "default" == fixture.api.ils_name(l2)

    def test_hosts(self, overdrive_api_fixture: OverdriveAPIFixture):
        fixture = overdrive_api_fixture
        db = fixture.db
        # By default, OverdriveAPI is initialized with the production
        # set of hostnames.
        assert (
            fixture.api.hosts()
            == OverdriveAPI.HOSTS[OverdriveConstants.PRODUCTION_SERVERS]
        )

        collection = db.collection(
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(
                overdrive_server_nickname=OverdriveConstants.TESTING_SERVERS
            ),
        )
        testing = OverdriveAPI(db.session, collection)
        assert testing.hosts() == OverdriveAPI.HOSTS[OverdriveConstants.TESTING_SERVERS]

        # If the setting doesn't make sense, we default to production
        # hostnames.
        collection = db.collection(
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(overdrive_server_nickname="nonsensical"),
        )
        bad = OverdriveAPI(db.session, collection)
        assert bad.hosts() == OverdriveAPI.HOSTS[OverdriveConstants.PRODUCTION_SERVERS]

    def test_endpoint(self, overdrive_api_fixture: OverdriveAPIFixture):
        fixture = overdrive_api_fixture

        # The .endpoint() method performs string interpolation, including
        # the names of servers.
        template = (
            "%(host)s %(patron_host)s %(oauth_host)s %(oauth_patron_host)s %(extra)s"
        )
        result = fixture.api.endpoint(template, extra="val")

        # The host names and the 'extra' argument have been used to
        # fill in the string interpolations.
        expect_args = dict(fixture.api.hosts())
        expect_args["extra"] = "val"
        assert result == template % expect_args

        # The string has been completely interpolated.
        assert "%" not in result

        # Once interpolation has happened, doing it again has no effect.
        assert result == fixture.api.endpoint(result, extra="something else")

        # This is important because an interpolated URL may superficially
        # appear to contain extra formatting characters.
        assert result + "%3A" == fixture.api.endpoint(
            result + "%3A", extra="something else"
        )

    def test__collection_context_basic_auth_header(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        fixture = overdrive_api_fixture

        # Verify that the Authorization header needed to get an access
        # token for a given collection is encoded properly.
        assert fixture.api._collection_context_basic_auth_header == "Basic YTpi"
        assert (
            fixture.api._collection_context_basic_auth_header
            == "Basic "
            + base64.standard_b64encode(
                f"{fixture.api.client_key()}:{fixture.api.client_secret()}"
            )
        )

    def test_get_success(self, overdrive_api_fixture: OverdriveAPIFixture):
        fixture = overdrive_api_fixture
        transaction = fixture.db

        fixture.api.queue_response(200, content="some content")
        status_code, headers, content = fixture.api.get(transaction.fresh_url(), {})
        assert 200 == status_code
        assert b"some content" == content

    def test_failure_to_get_library_is_fatal(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        fixture = overdrive_api_fixture

        fixture.api.queue_response(500)
        with pytest.raises(BadResponseException) as excinfo:
            fixture.api.get_library()
        assert "Got status code 500" in str(excinfo.value)

    def test_error_getting_library(self, overdrive_api_fixture: OverdriveAPIFixture):
        fixture = overdrive_api_fixture
        session = fixture.db.session

        class MisconfiguredOverdriveAPI(MockOverdriveAPI):
            """This Overdrive client has valid credentials but the library
            can't be found -- probably because the library ID is wrong."""

            def get_library(self):
                return {
                    "errorCode": "Some error",
                    "message": "Some message.",
                    "token": "abc-def-ghi",
                }

        # Just instantiating the API doesn't cause this error.
        api = MisconfiguredOverdriveAPI(session, fixture.collection)
        api._collection_token = None

        # But trying to access the collection token will cause it.
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            api.collection_token
        assert (
            "Overdrive credentials are valid but could not fetch library: Some message."
            in str(excinfo.value)
        )

    def test_401_on_get_refreshes_bearer_token(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        fixture = overdrive_api_fixture
        transaction = fixture.db

        # We have a token.
        assert "bearer token" == fixture.api._client_oauth_token

        # But then we try to GET, and receive a 401.
        fixture.api.queue_response(401)

        # We refresh the bearer token. (This is special cased in MockOverdriveAPI
        # so we don't mock the response in the normal way.)
        fixture.api.access_token_response = fixture.api.mock_access_token_response(
            "new bearer token"
        )

        # Then we retry the GET and it succeeds this time.
        fixture.api.queue_response(200, content="at last, the content")

        status_code, headers, content = fixture.api.get(transaction.fresh_url(), {})

        assert 200 == status_code
        assert b"at last, the content" == content

        # The bearer token has been updated.
        assert "new bearer token" == fixture.api._client_oauth_token

    def test__client_oauth_token(self, overdrive_api_fixture: OverdriveAPIFixture):
        """Verify the process of refreshing the Overdrive bearer token."""
        api = overdrive_api_fixture.api

        # Initially the token is None
        assert len(api.access_token_requests) == 0

        # Accessing the token triggers a refresh
        assert api._client_oauth_token == "bearer token"
        assert len(api.access_token_requests) == 1

        # Mock the token response
        api.access_token_response = api.mock_access_token_response("new bearer token")

        # Accessing the token again won't refresh, because the old token is still valid
        assert api._client_oauth_token == "bearer token"
        assert len(api.access_token_requests) == 1

        # However if the token expires we will get a new one
        assert api._cached_client_oauth_token is not None
        api._cached_client_oauth_token = api._cached_client_oauth_token._replace(
            expires=utc_now() - timedelta(seconds=1)
        )

        assert api._client_oauth_token == "new bearer token"
        assert len(api.access_token_requests) == 2

    def test_401_after__refresh_client_oauth_token_raises_error(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        fixture = overdrive_api_fixture
        api = fixture.api

        # Our initial token value is "bearer token".
        assert api._client_oauth_token == "bearer token"

        # We try to GET and receive a 401.
        api.queue_response(401)

        # We refresh the bearer token.
        api.access_token_response = api.mock_access_token_response("new bearer token")

        # Then we retry the GET but we get another 401.
        api.queue_response(401)

        # That raises a BadResponseException
        with pytest.raises(
            BadResponseException,
            match="Bad response from .*: Something's wrong with the Overdrive OAuth Bearer Token",
        ):
            api.get_library()

        # We refreshed the token in the process.
        assert fixture.api._client_oauth_token == "new bearer token"

        # We made two requests
        assert len(api.requests) == 2

    def test_401_during__refresh_client_oauth_token_raises_error(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        fixture = overdrive_api_fixture

        """If we fail to refresh the OAuth bearer token, an exception is
        raised.
        """
        fixture.api.access_token_response = MockRequestsResponse(401, {}, "")
        with pytest.raises(
            BadResponseException,
            match="Got status code 401 .* can only continue on: 200.",
        ):
            fixture.api._refresh_client_oauth_token()

    def test_advantage_differences(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db
        session = db.session

        # Test the differences between Advantage collections and
        # regular Overdrive collections.

        # Here's a regular Overdrive collection.
        main = db.collection(
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(
                external_account_id="1",
                overdrive_client_key="user",
                overdrive_client_secret="password",
                overdrive_website_id="100",
                ils_name="default",
            ),
        )

        # Here's an Overdrive API client for that collection.
        overdrive_main = MockOverdriveAPI(session, main)

        # Note the "library" endpoint.
        assert (
            "https://api.overdrive.com/v1/libraries/1"
            == overdrive_main._library_endpoint
        )

        # The advantage_library_id of a non-Advantage Overdrive account
        # is always -1.
        assert "1" == overdrive_main.library_id()
        assert -1 == overdrive_main.advantage_library_id

        # Here's an Overdrive Advantage collection associated with the
        # main Overdrive collection.
        child = db.collection(
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(external_account_id="2"),
        )
        child.parent = main
        overdrive_child = MockOverdriveAPI(session, child)

        # In URL-space, the "library" endpoint for the Advantage
        # collection is beneath the the parent collection's "library"
        # endpoint.
        assert (
            "https://api.overdrive.com/v1/libraries/1/advantageAccounts/2"
            == overdrive_child._library_endpoint
        )

        # The advantage_library_id of an Advantage collection is the
        # numeric value of its external_account_id.
        assert "2" == overdrive_child.library_id()
        assert 2 == overdrive_child.advantage_library_id

    def test__get_book_list_page(self, overdrive_api_fixture: OverdriveAPIFixture):
        fixture = overdrive_api_fixture

        # Test the internal method that retrieves a list of books and
        # preprocesses it.

        class MockExtractor:
            def link(self, content, rel_to_follow):
                self.link_called_with = (content, rel_to_follow)
                return "http://next-page/"

            def availability_link_list(self, content):
                self.availability_link_list_called_with = content
                return ["an availability queue"]

        original_data = {"key": "value"}
        for content in (
            original_data,
            json.dumps(original_data),
            json.dumps(original_data).encode("utf8"),
        ):
            extractor = MockExtractor()
            fixture.api.queue_response(200, content=content)
            result = fixture.api._get_book_list_page(
                "http://first-page/", "some-rel", extractor  # type: ignore[arg-type]
            )

            # A single request was made to the requested page.
            (url, headers, body) = fixture.api.requests.pop()
            assert len(fixture.api.requests) == 0
            assert url == "http://first-page/"

            # The extractor was used to extract a link to the page
            # with rel="some-rel".
            #
            # Note that the Python data structure (`original_data`) is passed in,
            # regardless of whether the mock response body is a Python
            # data structure, a bytestring, or a Unicode string.
            assert extractor.link_called_with == (original_data, "some-rel")

            # The data structure was also passed into the extractor's
            # availability_link_list() method.
            assert extractor.availability_link_list_called_with == original_data

            # The final result is a queue of availability data (from
            # this page) and a link to the next page.
            assert result == (["an availability queue"], "http://next-page/")

    def test_lock_in_format(self, overdrive_api_fixture: OverdriveAPIFixture):
        # Verify which formats do or don't need to be locked in before
        # fulfillment.
        needs_lock_in = overdrive_api_fixture.api.LOCK_IN_FORMATS

        # Streaming and manifest-based formats are exempt; all
        # other formats need lock-in.
        exempt = list(overdrive_api_fixture.api.STREAMING_FORMATS) + list(
            overdrive_api_fixture.api.MANIFEST_INTERNAL_FORMATS
        )
        for i in overdrive_api_fixture.api.FORMATS:
            if i not in exempt:
                assert i in needs_lock_in
        for i in exempt:
            assert i not in needs_lock_in

    def test__run_self_tests(
        self,
        overdrive_api_fixture: OverdriveAPIFixture,
    ):
        # Verify that OverdriveAPI._run_self_tests() calls the right
        # methods.
        db = overdrive_api_fixture.db

        # Mock every method used by OverdriveAPI._run_self_tests.
        api = MockOverdriveAPI(db.session, overdrive_api_fixture.collection)

        # First we will call _refresh_collection_oauth_token
        mock_refresh_token = create_autospec(api._refresh_client_oauth_token)
        api._refresh_client_oauth_token = mock_refresh_token

        # Then we will call get_advantage_accounts().
        mock_get_advantage_accounts = create_autospec(
            api.get_advantage_accounts, return_value=[object(), object()]
        )
        api.get_advantage_accounts = mock_get_advantage_accounts

        # Then we will call get() on the _all_products_link.
        mock_get = create_autospec(
            api.get, return_value=(200, {}, json.dumps(dict(totalItems=2010)))
        )
        api.get = mock_get

        # Finally, for every library associated with this
        # collection, we'll call get_patron_credential() using
        # the credentials of that library's test patron.
        mock_get_patron_credential = create_autospec(api._get_patron_oauth_credential)
        api._get_patron_oauth_credential = mock_get_patron_credential

        # Now let's make sure two Libraries have access to this
        # Collection -- one library with a default patron and one
        # without.
        no_default_patron = db.library()
        overdrive_api_fixture.collection.associated_libraries.append(no_default_patron)

        with_default_patron = db.default_library()
        db.simple_auth_integration(with_default_patron)

        # Now that everything is set up, run the self-test.
        results = sorted(api._run_self_tests(db.session), key=lambda x: x.name)
        [
            no_patron_credential,
            default_patron_credential,
            global_privileges,
            collection_size,
            advantage,
        ] = results

        # Verify that each test method was called and returned the
        # expected SelfTestResult object.
        assert (
            global_privileges.name == "Checking global Client Authentication privileges"
        )
        assert global_privileges.success is True
        assert global_privileges.result == mock_refresh_token.return_value

        assert advantage.name == "Looking up Overdrive Advantage accounts"
        assert advantage.success is True
        assert advantage.result == "Found 2 Overdrive Advantage account(s)."
        mock_get_advantage_accounts.assert_called_once()

        assert collection_size.name == "Counting size of collection"
        assert collection_size.success is True
        assert collection_size.result == "2010 item(s) in collection"
        mock_get.assert_called_once_with(api._all_products_link, {})

        assert (
            no_patron_credential.name
            == f"Acquiring test patron credentials for library {no_default_patron.name}"
        )
        assert no_patron_credential.success is False
        assert (
            str(no_patron_credential.exception)
            == "Library has no test patron configured."
        )

        assert (
            default_patron_credential.name
            == f"Checking Patron Authentication privileges, using test patron for library {with_default_patron.name}"
        )
        assert default_patron_credential.success is True
        assert (
            default_patron_credential.result == mock_get_patron_credential.return_value
        )

        # Although there are two libraries associated with this
        # collection, get_patron_credential was only called once, because
        # one of the libraries doesn't have a default patron.
        mock_get_patron_credential.assert_called_once()
        (patron1, password1) = mock_get_patron_credential.call_args.args
        assert "username1" == patron1.authorization_identifier
        assert "password1" == password1

    def test_run_self_tests_short_circuit(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        """If OverdriveAPI.check_creds can't get credentials, the rest of
        the self-tests aren't even run.

        This probably doesn't matter much, because if check_creds doesn't
        work we won't be able to instantiate the OverdriveAPI class.
        """

        api = overdrive_api_fixture.api
        api._refresh_client_oauth_token = create_autospec(
            api._refresh_client_oauth_token, side_effect=Exception("Failure!")
        )

        # Only one test will be run.
        [check_creds] = overdrive_api_fixture.api._run_self_tests(
            overdrive_api_fixture.db.session
        )
        assert str(check_creds.exception) == "Failure!"

    def test_default_notification_email_address(
        self,
        overdrive_api_fixture: OverdriveAPIFixture,
        library_fixture: LibraryFixture,
    ):
        """Test the ability of the Overdrive API to detect an email address
        previously given by the patron to Overdrive for the purpose of
        notifications.
        """
        db = overdrive_api_fixture.db

        ignore, patron_with_email = overdrive_api_fixture.sample_json(
            "patron_info.json"
        )
        overdrive_api_fixture.api.queue_response(200, content=patron_with_email)
        settings = library_fixture.mock_settings()
        library = library_fixture.library(settings=settings)
        patron = db.patron(library=library)

        # The site default for notification emails will never be used.
        settings.default_notification_email_address = "notifications@example.com"

        # If the patron has used a particular email address to put
        # books on hold, use that email address, not the site default.
        assert (
            "foo@bar.com"
            == overdrive_api_fixture.api.default_notification_email_address(
                patron, "pin"
            )
        )

        # If the patron's email address according to Overdrive _is_
        # the site default, it is ignored. This can only happen if
        # this patron placed a hold using an older version of the
        # circulation manager.
        patron_with_email["lastHoldEmail"] = settings.default_notification_email_address
        overdrive_api_fixture.api.queue_response(200, content=patron_with_email)
        assert None == overdrive_api_fixture.api.default_notification_email_address(
            patron, "pin"
        )

        # If the patron has never before put an Overdrive book on
        # hold, their JSON object has no `lastHoldEmail` key. In this
        # case we return None -- again, ignoring the site default.
        patron_with_no_email = dict(patron_with_email)
        del patron_with_no_email["lastHoldEmail"]
        overdrive_api_fixture.api.queue_response(200, content=patron_with_no_email)
        assert None == overdrive_api_fixture.api.default_notification_email_address(
            patron, "pin"
        )

        # If there's an error getting the information from Overdrive,
        # we return None.
        overdrive_api_fixture.api.queue_response(404)
        assert None == overdrive_api_fixture.api.default_notification_email_address(
            patron, "pin"
        )

    def test_scope_string(self, overdrive_api_fixture: OverdriveAPIFixture):
        # scope_string() puts the website ID of the Overdrive
        # integration and the ILS name associated with the library
        # into the form expected by Overdrive.
        db = overdrive_api_fixture.db
        expect = "websiteid:{} authorizationname:{}".format(
            overdrive_api_fixture.api.website_id(),
            overdrive_api_fixture.api.ils_name(db.default_library()),
        )
        assert expect == overdrive_api_fixture.api.scope_string(db.default_library())

    def test_checkout(self, overdrive_api_fixture: OverdriveAPIFixture):
        # Verify the process of checking out a book.
        db = overdrive_api_fixture.db
        patron = MagicMock()
        pin = MagicMock()
        delivery_mechanism = MagicMock()
        pool = db.licensepool(edition=None, collection=overdrive_api_fixture.collection)
        identifier = pool.identifier

        class Mock(MockOverdriveAPI):
            MOCK_EXPIRATION_DATE = object()
            PROCESS_CHECKOUT_ERROR_RESULT = Exception(
                "exception in _process_checkout_error"
            )

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.extract_expiration_date_called_with = []
                self._process_checkout_error_called_with = []

            def extract_expiration_date(self, loan):
                self.extract_expiration_date_called_with.append(loan)
                return self.MOCK_EXPIRATION_DATE

            def _process_checkout_error(self, patron, pin, licensepool, data):
                self._process_checkout_error_called_with.append(
                    (patron, pin, licensepool, data)
                )
                result = self.PROCESS_CHECKOUT_ERROR_RESULT
                if isinstance(result, Exception):
                    raise result
                return result

        # First, test the successful path.
        api = Mock(db.session, overdrive_api_fixture.collection)
        api_response = json.dumps("some data")
        api.queue_response(201, content=api_response)
        loan = api.checkout(patron, pin, pool, delivery_mechanism)

        # Verify that a good-looking patron request went out.
        endpoint, ignore, kwargs = api.requests.pop()
        assert endpoint.endswith("/me/checkouts")
        assert patron == kwargs.pop("_patron")
        extra_headers = kwargs.pop("extra_headers")
        assert {"Content-Type": "application/json"} == extra_headers
        data = json.loads(kwargs.pop("data"))
        assert {
            "fields": [{"name": "reserveId", "value": pool.identifier.identifier}]
        } == data

        # The API response was passed into extract_expiration_date.
        #
        # The most important thing here is not the content of the response but the
        # fact that the response code was not 400.
        assert "some data" == api.extract_expiration_date_called_with.pop()

        # The return value is a LoanInfo object with all relevant info.
        assert isinstance(loan, LoanInfo)
        assert pool.collection.id == loan.collection_id
        assert identifier.type == loan.identifier_type
        assert identifier.identifier == loan.identifier
        assert None == loan.start_date
        assert api.MOCK_EXPIRATION_DATE == loan.end_date

        # _process_checkout_error was not called
        assert [] == api._process_checkout_error_called_with

        # Now let's test error conditions.

        # Most of the time, an error simply results in an exception.
        api.queue_response(400, content=api_response)
        with pytest.raises(Exception) as excinfo:
            api.checkout(patron, pin, pool, delivery_mechanism)
        assert "exception in _process_checkout_error" in str(excinfo.value)
        assert (
            patron,
            pin,
            pool,
            "some data",
        ) == api._process_checkout_error_called_with.pop()

        # However, if _process_checkout_error is able to recover from
        # the error and ends up returning something, the return value
        # is propagated from checkout().
        api.PROCESS_CHECKOUT_ERROR_RESULT = "Actually, I was able to recover"  # type: ignore[assignment]
        api.queue_response(400, content=api_response)
        assert "Actually, I was able to recover" == api.checkout(
            patron, pin, pool, delivery_mechanism
        )
        assert (
            patron,
            pin,
            pool,
            "some data",
        ) == api._process_checkout_error_called_with.pop()

    def test__process_checkout_error(self, overdrive_api_fixture: OverdriveAPIFixture):
        # Verify that _process_checkout_error handles common API-side errors,
        # making follow-up API calls if necessary.
        db = overdrive_api_fixture.db

        class Mock(MockOverdriveAPI):
            MOCK_LOAN = object()
            MOCK_EXPIRATION_DATE = object()

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.get_loan_called_with = []
                self.extract_expiration_date_called_with = []

            def get_loan(self, patron, pin, identifier):
                self.get_loan_called_with.append((patron, pin, identifier))
                return self.MOCK_LOAN

            def extract_expiration_date(self, loan):
                self.extract_expiration_date_called_with.append(loan)
                return self.MOCK_EXPIRATION_DATE

        patron = MagicMock()
        pin = MagicMock()
        pool = db.licensepool(edition=None, collection=overdrive_api_fixture.collection)
        identifier = pool.identifier
        api = Mock(db.session, overdrive_api_fixture.collection)
        m = api._process_checkout_error

        # Most of the error handling is pretty straightforward.
        def with_error_code(code):
            # Simulate the response of the Overdrive API with a given error code.
            error = dict(errorCode=code)

            # Handle the error.
            return m(patron, pin, pool, error)

        # Errors not specifically known become generic CannotLoan exceptions.
        with pytest.raises(CannotLoan) as excinfo:
            with_error_code("WeirdError")
        assert "WeirdError" in str(excinfo.value)

        # If the data passed in to _process_checkout_error is not what
        # the real Overdrive API would send, the error is even more
        # generic.
        with pytest.raises(CannotLoan) as excinfo:
            m(patron, pin, pool, "Not a dict")
        assert "Unknown Error" in str(excinfo.value)
        with pytest.raises(CannotLoan) as excinfo:
            m(patron, pin, pool, dict(errorCodePresent=False))
        assert "Unknown Error" in str(excinfo.value)

        # Some known errors become specific subclasses of CannotLoan.
        pytest.raises(
            PatronLoanLimitReached, with_error_code, "PatronHasExceededCheckoutLimit"
        )
        pytest.raises(
            PatronLoanLimitReached,
            with_error_code,
            "PatronHasExceededCheckoutLimit_ForCPC",
        )

        # There are two cases where we need to make follow-up API
        # requests as the result of a failure during the loan process.

        # First, if the error is "NoCopiesAvailable", we know we have
        # out-of-date availability information. We raise NoAvailableCopies
        # and let the circulation API take care of handling that.
        pytest.raises(NoAvailableCopies, with_error_code, "NoCopiesAvailable")

        # If the error is "TitleAlreadyCheckedOut", then the problem
        # is that the patron tried to take out a new loan instead of
        # fulfilling an existing loan. In this case we don't raise an
        # exception at all; we fulfill the loan and return a LoanInfo
        # object.
        loan = with_error_code("TitleAlreadyCheckedOut")

        # get_loan was called with the patron's details.
        assert (patron, pin, identifier.identifier) == api.get_loan_called_with.pop()

        # extract_expiration_date was called on the return value of get_loan.
        assert api.MOCK_LOAN == api.extract_expiration_date_called_with.pop()

        # And a LoanInfo was created with all relevant information.
        assert isinstance(loan, LoanInfo)
        assert pool.collection.id == loan.collection_id
        assert identifier.type == loan.identifier_type
        assert identifier.identifier == loan.identifier
        assert None == loan.start_date
        assert api.MOCK_EXPIRATION_DATE == loan.end_date

    def test_extract_expiration_date(self):
        # Test the code that finds and parses a loan expiration date.
        m = OverdriveAPI.extract_expiration_date

        # Success
        assert datetime_utc(2020, 1, 2, 3, 4, 5) == m(
            dict(expires="2020-01-02T03:04:05Z")
        )

        # Various failure cases.
        assert None == m(dict(expiresPresent=False))
        assert None == m(dict(expires="Wrong date format"))
        assert None == m("Not a dict")
        assert None == m(None)

    def test_place_hold(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db

        # Verify that an appropriate request is made to HOLDS_ENDPOINT
        # to create a hold.
        #
        # The request will include different form fields depending on
        # whether default_notification_email_address returns something.
        class Mock(MockOverdriveAPI):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.DEFAULT_NOTIFICATION_EMAIL_ADDRESS = None

            def default_notification_email_address(self, patron, pin):
                self.default_notification_email_address_called_with = (patron, pin)
                return self.DEFAULT_NOTIFICATION_EMAIL_ADDRESS

            def fill_out_form(self, **form_fields):
                # Record the form fields and return some dummy values.
                self.fill_out_form_called_with = form_fields
                return "headers", "filled-out form"

            def patron_request(self, *args, **kwargs):
                # Pretend to make a request to an API endpoint.
                self.patron_request_called_with = (args, kwargs)
                return "A mock response"

            def process_place_hold_response(self, response, patron, pin, licensepool):
                self.process_place_hold_response_called_with = (
                    response,
                    patron,
                    pin,
                    licensepool,
                )
                return "OK, I processed it."

        # First, test the case where no notification email address is
        # provided and there is no default.
        patron = MagicMock()
        pin = MagicMock()
        pool = db.licensepool(edition=None, collection=overdrive_api_fixture.collection)
        api = Mock(db.session, overdrive_api_fixture.collection)
        response = api.place_hold(patron, pin, pool, None)

        # Now we can trace the path of the input through the method calls.

        # The patron and PIN were passed into
        # default_notification_email_address.
        assert (patron, pin) == api.default_notification_email_address_called_with

        # The return value was None, and so 'ignoreHoldEmail' was
        # added to the form to be filled out, rather than
        # 'emailAddress' being added.
        fields = api.fill_out_form_called_with
        identifier = str(pool.identifier.identifier)
        assert dict(ignoreHoldEmail=True, reserveId=identifier) == fields

        # patron_request was called with the filled-out form and other
        # information necessary to authenticate the request.
        args, kwargs = api.patron_request_called_with
        assert (patron, pin, api.HOLDS_ENDPOINT, "headers", "filled-out form") == args
        assert {} == kwargs

        # Finally, process_place_hold_response was called on
        # the return value of patron_request
        assert (
            "A mock response",
            patron,
            pin,
            pool,
        ) == api.process_place_hold_response_called_with
        assert "OK, I processed it." == response

        # Now we need to test two more cases.
        #
        # First, the patron has a holds notification address
        # registered with Overdrive.
        email = "holds@patr.on"
        api.DEFAULT_NOTIFICATION_EMAIL_ADDRESS = email
        response = api.place_hold(patron, pin, pool, None)

        # Same result.
        assert "OK, I processed it." == response

        # Different variables were passed in to fill_out_form.
        fields = api.fill_out_form_called_with
        assert dict(emailAddress=email, reserveId=identifier) == fields

        # Finally, test that when a specific address is passed in, it
        # takes precedence over the patron's holds notification address.

        response = api.place_hold(patron, pin, pool, "another@addre.ss")
        assert "OK, I processed it." == response
        fields = api.fill_out_form_called_with
        assert dict(emailAddress="another@addre.ss", reserveId=identifier) == fields

    def test_process_place_hold_response(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        # Verify that we can handle various error and non-error responses
        # to a HOLDS_ENDPOINT request.

        ignore, successful_hold = overdrive_api_fixture.sample_json(
            "successful_hold.json"
        )

        api = MockOverdriveAPI(db.session, overdrive_api_fixture.collection)

        def process_error_response(message):
            # Attempt to process a response that resulted in an error.
            if isinstance(message, (bytes, str)):
                data = dict(errorCode=message)
            else:
                data = message
            response = MockRequestsResponse(400, content=data)
            return api.process_place_hold_response(response, None, None, None)

        # Some error messages result in specific CirculationExceptions.
        pytest.raises(CannotRenew, process_error_response, "NotWithinRenewalWindow")
        pytest.raises(
            PatronHoldLimitReached, process_error_response, "PatronExceededHoldLimit"
        )
        pytest.raises(AlreadyOnHold, process_error_response, "AlreadyOnWaitList")

        # An unrecognized error message results in a generic
        # CannotHold.
        pytest.raises(CannotHold, process_error_response, "SomeOtherError")

        # Same if the error message is missing or the response can't be
        # processed.
        pytest.raises(CannotHold, process_error_response, dict())
        pytest.raises(CannotHold, process_error_response, json.dumps(None))

        # Same if the error code isn't in the 4xx or 2xx range
        # (which shouldn't happen in real life).
        response = MockRequestsResponse(999)
        pytest.raises(
            CannotHold, api.process_place_hold_response, response, None, None, None
        )

        # At this point patron and book details become important --
        # we're going to return a HoldInfo object and potentially make
        # another API request.
        patron = db.patron()
        pin = MagicMock()
        licensepool = db.licensepool(edition=None)

        # Finally, let's test the case where there was no hold and now
        # there is.
        response = MockRequestsResponse(200, content=successful_hold)
        result = api.process_place_hold_response(response, patron, pin, licensepool)
        assert isinstance(result, HoldInfo)
        assert licensepool.collection == result.collection(db.session)
        assert licensepool.identifier.identifier == result.identifier
        assert licensepool.identifier.type == result.identifier_type
        assert datetime_utc(2015, 3, 26, 11, 30, 29) == result.start_date
        assert None == result.end_date
        assert 1 == result.hold_position

    def test_checkin(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db

        class Mock(MockOverdriveAPI):
            EARLY_RETURN_SUCCESS = False

            def perform_early_return(self, *args):
                self.perform_early_return_call = args
                return self.EARLY_RETURN_SUCCESS

            def patron_request(self, *args, **kwargs):
                self.patron_request_call = (args, kwargs)

        overdrive = Mock(db.session, overdrive_api_fixture.collection)
        overdrive.perform_early_return_call = None

        # In most circumstances we do not bother calling
        # perform_early_return; we just call patron_request.
        pool = db.licensepool(None)
        patron = db.patron()
        pin = MagicMock()
        expect_url = overdrive.endpoint(
            overdrive.CHECKOUT_ENDPOINT, overdrive_id=pool.identifier.identifier
        )

        def assert_no_early_return():
            """Call this to verify that patron_request is
            called within checkin() instead of perform_early_return.
            """
            overdrive.checkin(patron, pin, pool)

            # perform_early_return was not called.
            assert None == overdrive.perform_early_return_call

            # patron_request was called in an attempt to
            # DELETE an active loan.
            args, kwargs = overdrive.patron_request_call
            assert (patron, pin, expect_url) == args
            assert dict(method="DELETE") == kwargs
            overdrive.patron_request_call = None

        # If there is no loan, there is no perform_early_return.
        assert_no_early_return()

        # Same if the loan is not fulfilled...
        loan, ignore = pool.loan_to(patron)
        assert_no_early_return()

        # If the loan is fulfilled but its LicensePoolDeliveryMechanism has
        # no DeliveryMechanism for some reason...
        loan.fulfillment = pool.delivery_mechanisms[0]
        dm = loan.fulfillment.delivery_mechanism
        loan.fulfillment.delivery_mechanism = None
        assert_no_early_return()

        # If the loan is fulfilled but the delivery mechanism uses DRM...
        loan.fulfillment.delivery_mechanism = dm
        assert_no_early_return()

        # If the loan is fulfilled with a DRM-free delivery mechanism,
        # perform_early_return _is_ called.
        dm.drm_scheme = DeliveryMechanism.NO_DRM
        overdrive.checkin(patron, pin, pool)

        assert (patron, pin, loan) == overdrive.perform_early_return_call

        # But if it fails, patron_request is _also_ called.
        args, kwargs = overdrive.patron_request_call
        assert (patron, pin, expect_url) == args
        assert dict(method="DELETE") == kwargs

        # Finally, if the loan is fulfilled with a DRM-free delivery mechanism
        # and perform_early_return succeeds, patron_request_call is not
        # called -- the title was already returned.
        overdrive.patron_request_call = None
        overdrive.EARLY_RETURN_SUCCESS = True
        overdrive.checkin(patron, pin, pool)
        assert (patron, pin, loan) == overdrive.perform_early_return_call
        assert None == overdrive.patron_request_call

    def test_perform_early_return(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db

        class Mock(MockOverdriveAPI):
            EARLY_RETURN_URL = "http://early-return/"

            def get_fulfillment_link(self, *args):
                self.get_fulfillment_link_call = args
                return ("http://fulfillment/", "content/type")

            def _extract_early_return_url(self, *args):
                self._extract_early_return_url_call = args
                return self.EARLY_RETURN_URL

        overdrive = Mock(db.session, overdrive_api_fixture.collection)

        # This patron has a loan.
        pool = db.licensepool(None)
        patron = db.patron()
        pin = MagicMock()
        loan, ignore = pool.loan_to(patron)

        # The loan has been fulfilled and now the patron wants to
        # do early return.
        loan.fulfillment = pool.delivery_mechanisms[0]

        # Our mocked perform_early_return will make two HTTP requests.
        # The first will be to the fulfill link returned by our mock
        # get_fulfillment_link. The response to this request is a
        # redirect that includes an early return link.
        http = MockHTTPClient()
        http.queue_response(
            302,
            other_headers=dict(location="http://fulfill-this-book/?or=return-early"),
        )

        # The second HTTP request made will be to the early return
        # link 'extracted' from that link by our mock
        # _extract_early_return_url. The response here is a copy of
        # the actual response Overdrive sends in this situation.
        http.queue_response(200, content="Success")

        # Do the thing.
        success = overdrive.perform_early_return(patron, pin, loan, http.do_get)

        # The title was 'returned'.
        assert True == success

        # It worked like this:
        #
        # get_fulfillment_link was called with appropriate arguments.
        assert (
            patron,
            pin,
            pool.identifier.identifier,
            "ebook-epub-adobe",
        ) == overdrive.get_fulfillment_link_call

        # The URL returned by that method was 'requested'.
        assert "http://fulfillment/" == http.requests.pop(0)

        # The resulting URL was passed into _extract_early_return_url.
        assert (
            "http://fulfill-this-book/?or=return-early",
        ) == overdrive._extract_early_return_url_call

        # Then the URL returned by _that_ method was 'requested'.
        assert "http://early-return/" == http.requests.pop(0)

        # If no early return URL can be extracted from the fulfillment URL,
        # perform_early_return has no effect.
        #
        overdrive._extract_early_return_url_call = None
        overdrive.EARLY_RETURN_URL = None  # type: ignore
        http.queue_response(
            302, other_headers=dict(location="http://fulfill-this-book/")
        )

        success = overdrive.perform_early_return(patron, pin, loan, http.do_get)
        assert False == success

        # extract_early_return_url_call was called, but since it returned
        # None, no second HTTP request was made.
        assert "http://fulfillment/" == http.requests.pop(0)
        assert (
            "http://fulfill-this-book/",
        ) == overdrive._extract_early_return_url_call
        assert [] == http.requests

        # If we can't map the delivery mechanism to one of Overdrive's
        # internal formats, perform_early_return has no effect.
        #
        loan.fulfillment.delivery_mechanism.content_type = "not-in/overdrive"
        success = overdrive.perform_early_return(patron, pin, loan, http.do_get)
        assert False == success

        # In this case, no HTTP requests were made at all, since we
        # couldn't figure out which arguments to pass into
        # get_fulfillment_link.
        assert [] == http.requests

        # If the final attempt to hit the return URL doesn't result
        # in a 200 status code, perform_early_return has no effect.
        http.queue_response(
            302,
            other_headers=dict(location="http://fulfill-this-book/?or=return-early"),
        )
        http.queue_response(401, content="Unauthorized!")
        success = overdrive.perform_early_return(patron, pin, loan, http.do_get)
        assert False == success

    def test_extract_early_return_url(self):
        m = OverdriveAPI._extract_early_return_url
        assert None == m("http://no-early-return/")
        assert None == m("")
        assert None == m(None)

        # This is based on a real Overdrive early return URL.
        has_early_return = "https://openepub-gk.cdn.overdrive.com/OpenEPUBStore1/1577-1/%7B5880F6D0-48AC-44DE-8BF1-FD1CE62E97A8%7DFzr418.epub?e=1518753718&loanExpirationDate=2018-03-01T17%3a12%3a33Z&loanEarlyReturnUrl=https%3a%2f%2fnotifications-ofs.contentreserve.com%2fEarlyReturn%2fnypl%2f037-1374147-00279%2f5480F6E1-48F3-00DE-96C1-FD3CE32D94FD-312%3fh%3dVgvxBQHdQxtsbgb43AH6%252bEmpni9LoffkPczNiUz7%252b10%253d&sourceId=nypl&h=j7nGk7qxE71X2ZcdLw%2bqa04jqEw%3d"
        assert (
            "https://notifications-ofs.contentreserve.com/EarlyReturn/nypl/037-1374147-00279/5480F6E1-48F3-00DE-96C1-FD3CE32D94FD-312?h=VgvxBQHdQxtsbgb43AH6%2bEmpni9LoffkPczNiUz7%2b10%3d"
            == m(has_early_return)
        )

    def test_place_hold_raises_exception_if_patron_over_hold_limit(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        over_hold_limit = overdrive_api_fixture.error_message(
            "PatronExceededHoldLimit",
            "Patron cannot place any more holds, already has maximum holds placed.",
        )

        edition, pool = db.edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
        )
        overdrive_api_fixture.api.queue_response(400, content=over_hold_limit)
        pytest.raises(
            PatronHoldLimitReached,
            overdrive_api_fixture.api.place_hold,
            db.patron(),
            "pin",
            pool,
            notification_email_address="foo@bar.com",
        )

    def test_place_hold_looks_up_notification_address(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        edition, pool = db.edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
        )

        # The first request we make will be to get patron info,
        # so that we know that the most recent email address used
        # to put a book on hold is foo@bar.com.
        ignore, patron_with_email = overdrive_api_fixture.sample_json(
            "patron_info.json"
        )

        # The second request we make will be to put a book on hold,
        # and when we do so we will ask for the notification to be
        # sent to foo@bar.com.
        ignore, successful_hold = overdrive_api_fixture.sample_json(
            "successful_hold.json"
        )

        overdrive_api_fixture.api.queue_response(200, content=patron_with_email)
        overdrive_api_fixture.api.queue_response(200, content=successful_hold)
        hold = overdrive_api_fixture.api.place_hold(
            db.patron(), "pin", pool, notification_email_address=None
        )

        # The book was placed on hold.
        assert 1 == hold.hold_position
        assert pool.identifier.identifier == hold.identifier

        # And when we placed it on hold, we passed in foo@bar.com
        # as the email address -- not notifications@example.com.
        url, positional_args, kwargs = overdrive_api_fixture.api.requests[-1]
        headers, body = positional_args
        assert '{"name": "emailAddress", "value": "foo@bar.com"}' in body

    def test_fulfill_returns_fulfillmentinfo_if_returned_by_get_fulfillment_link(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        # If get_fulfillment_link returns a Fulfillment, it is returned
        # immediately and the rest of fulfill() does not run.
        fulfillment = create_autospec(Fulfillment)

        edition, pool = db.edition(with_license_pool=True)
        api = OverdriveAPI(db.session, overdrive_api_fixture.collection)
        api.get_fulfillment_link = create_autospec(
            api.get_fulfillment_link, return_value=fulfillment
        )
        api.internal_format = create_autospec(
            api.internal_format, return_value="format"
        )
        result = api.fulfill(MagicMock(), MagicMock(), pool, MagicMock())
        assert result is fulfillment

    def test_fulfill_raises_exception_and_updates_formats_for_outdated_format(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        edition, pool = db.edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
        )

        # This pool has a format that's no longer available from overdrive.
        pool.set_delivery_mechanism(
            Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
            RightsStatus.IN_COPYRIGHT,
            None,
        )

        ignore, loan = overdrive_api_fixture.sample_json("single_loan.json")

        ignore, lock_in_format_not_available = overdrive_api_fixture.sample_json(
            "lock_in_format_not_available.json"
        )

        # We will get the loan, try to lock in the format, and fail.
        overdrive_api_fixture.api.queue_response(200, content=loan)
        overdrive_api_fixture.api.queue_response(
            400, content=lock_in_format_not_available
        )

        # Trying to get a fulfillment link raises an exception.
        pytest.raises(
            FormatNotAvailable,
            overdrive_api_fixture.api.get_fulfillment_link,
            db.patron(),
            "pin",
            pool.identifier.identifier,
            "ebook-epub-adobe",
        )

        # Fulfill will also update the formats.
        ignore, bibliographic = overdrive_api_fixture.sample_json(
            "bibliographic_information.json"
        )

        # To avoid a mismatch, make it look like the information is
        # for the correct Identifier.
        bibliographic["id"] = pool.identifier.identifier

        # If we have the LicensePool available (as opposed to just the
        # identifier), we will get the loan, try to lock in the
        # format, fail, and then update the bibliographic information.
        overdrive_api_fixture.api.queue_response(200, content=loan)
        overdrive_api_fixture.api.queue_response(
            400, content=lock_in_format_not_available
        )
        overdrive_api_fixture.api.queue_response(200, content=bibliographic)

        pytest.raises(
            FormatNotAvailable,
            overdrive_api_fixture.api.fulfill,
            db.patron(),
            "pin",
            pool,
            pool.delivery_mechanisms[0],
        )

        # The delivery mechanisms have been updated.
        assert 4 == len(pool.delivery_mechanisms)
        assert {
            MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.KINDLE_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            MediaTypes.OVERDRIVE_EBOOK_MANIFEST_MEDIA_TYPE,
        } == {lpdm.delivery_mechanism.content_type for lpdm in pool.delivery_mechanisms}
        assert {
            DeliveryMechanism.ADOBE_DRM,
            DeliveryMechanism.KINDLE_DRM,
            DeliveryMechanism.LIBBY_DRM,
            DeliveryMechanism.STREAMING_DRM,
        } == {lpdm.delivery_mechanism.drm_scheme for lpdm in pool.delivery_mechanisms}

    def test_get_fulfillment_link_from_download_link(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        patron = db.patron()

        ignore, streaming_fulfill_link = overdrive_api_fixture.sample_json(
            "streaming_fulfill_link_response.json"
        )

        overdrive_api_fixture.api.queue_response(200, content=streaming_fulfill_link)

        href, type = overdrive_api_fixture.api.get_fulfillment_link_from_download_link(
            patron, "1234", "http://download-link", fulfill_url="http://fulfill"
        )
        assert (
            "https://fulfill.contentreserve.com/PerfectLife9780345530967.epub-sample.overdrive.com?RetailerID=nypl&Expires=1469825647&Token=dd0e19b4-eb70-439d-8c50-a65201060f4c&Signature=asl67/G154KeeUsL1mHPwEbZfgc="
            == href
        )
        assert "text/html" == type

    def test_get_fulfillment_link_returns_fulfillmentinfo_for_manifest_format(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        # When the format requested would result in a link to a
        # manifest file, the manifest link is returned as-is (wrapped
        # in an OverdriveManifestFulfillment) rather than being retrieved
        # and processed.

        # To keep things simple, our mock API will always return the same
        # fulfillment link.
        loan_info = {"isFormatLockedIn": False}

        class MockAPI(MockOverdriveAPI):
            def get_loan(self, patron, pin, overdrive_id):
                self.get_loan_called_with = (patron, pin, overdrive_id)
                return loan_info

            def get_download_link(self, loan, format_type, error_url):
                self.get_download_link_called_with = (loan, format_type, error_url)
                return "http://fulfillment-link/"

            def get_fulfillment_link_from_download_link(self, *args, **kwargs):
                # We want to verify that this method is never called.
                raise Exception("explode!")

        api = MockAPI(db.session, overdrive_api_fixture.collection)
        api.queue_response(200, content=json.dumps({"some": "data"}))

        # Randomly choose one of the formats that must be fulfilled as
        # a link to a manifest.
        overdrive_format = random.choice(list(OverdriveAPI.MANIFEST_INTERNAL_FORMATS))

        # Get the fulfillment link.
        patron = db.patron()
        fulfillmentinfo = api.get_fulfillment_link(
            patron,
            "1234",
            "http://download-link",
            overdrive_format,
        )
        assert isinstance(fulfillmentinfo, OverdriveManifestFulfillment)

        # Before looking at the OverdriveManifestFulfillment,
        # let's see how we got there.

        # First, our mocked get_loan() was called.
        assert (
            patron,
            "1234",
            "http://download-link",
        ) == api.get_loan_called_with

        # It returned a dictionary that contained no information
        # except isFormatLockedIn: false.

        # Since the manifest formats do not lock the loan, this
        # skipped most of the code in get_fulfillment_link, and the
        # loan info was passed into our mocked get_download_link.

        assert (
            loan_info,
            overdrive_format,
            api.DEFAULT_ERROR_URL,
        ) == api.get_download_link_called_with

        # Since the manifest formats cannot be retrieved by the
        # circulation manager, the result of get_download_link was
        # wrapped in an OverdriveManifestFulfillment and returned.
        # get_fulfillment_link_from_download_link was never called.
        assert "http://fulfillment-link/" == fulfillmentinfo.content_link

    def test_update_formats(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db

        # Create a LicensePool with an inaccurate delivery mechanism
        # and the wrong medium.
        edition, pool = db.edition(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True,
        )
        edition.medium = Edition.PERIODICAL_MEDIUM

        # Add the bad delivery mechanism.
        pool.set_delivery_mechanism(
            Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
            RightsStatus.IN_COPYRIGHT,
            None,
        )

        # Prepare the bibliographic information.
        ignore, bibliographic = overdrive_api_fixture.sample_json(
            "bibliographic_information.json"
        )

        # To avoid a mismatch, make it look like the information is
        # for the new pool's Identifier.
        bibliographic["id"] = pool.identifier.identifier

        overdrive_api_fixture.api.queue_response(200, content=bibliographic)

        overdrive_api_fixture.api.update_formats(pool)

        # The delivery mechanisms have been updated.
        assert 4 == len(pool.delivery_mechanisms)
        assert {
            MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.KINDLE_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            MediaTypes.OVERDRIVE_EBOOK_MANIFEST_MEDIA_TYPE,
        } == {lpdm.delivery_mechanism.content_type for lpdm in pool.delivery_mechanisms}
        assert {
            DeliveryMechanism.ADOBE_DRM,
            DeliveryMechanism.KINDLE_DRM,
            DeliveryMechanism.LIBBY_DRM,
            DeliveryMechanism.STREAMING_DRM,
        } == {lpdm.delivery_mechanism.drm_scheme for lpdm in pool.delivery_mechanisms}

        # The Edition's medium has been corrected.
        assert Edition.BOOK_MEDIUM == edition.medium

    def test_update_availability(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db

        # Test the Overdrive implementation of the update_availability
        # method defined by the CirculationAPI interface.

        # Create a LicensePool that needs updating.
        edition, pool = db.edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
            collection=overdrive_api_fixture.collection,
        )

        # We have never checked the circulation information for this
        # LicensePool. Put some random junk in the pool to make sure
        # it gets replaced.
        pool.licenses_owned = 10
        pool.licenses_available = 4
        pool.patrons_in_hold_queue = 3
        assert None == pool.last_checked

        # Prepare availability information.
        ignore, availability = overdrive_api_fixture.sample_json(
            "overdrive_availability_information.json"
        )
        # Since this is the first time we've seen this book,
        # we'll also be updating the bibliographic information.
        ignore, bibliographic = overdrive_api_fixture.sample_json(
            "bibliographic_information.json"
        )

        # To avoid a mismatch, make it look like the information is
        # for the new pool's Identifier.
        availability["id"] = pool.identifier.identifier
        bibliographic["id"] = pool.identifier.identifier

        overdrive_api_fixture.api.queue_response(200, content=availability)
        overdrive_api_fixture.api.queue_response(200, content=bibliographic)

        overdrive_api_fixture.api.update_availability(pool)

        # The availability information has been updated, as has the
        # date the availability information was last checked.
        assert 5 == pool.licenses_owned
        assert 1 == pool.licenses_available
        assert 0 == pool.patrons_in_hold_queue
        assert pool.last_checked is not None

    def test_collection_token(self, db: DatabaseTransactionFixture) -> None:
        api = OverdriveAPI(db.session, db.collection(protocol=OverdriveAPI))
        mock_get_library = MagicMock(return_value={"collectionToken": "abc"})
        api.get_library = mock_get_library

        # If the collection token is set, we just return that
        api._collection_token = "123"
        assert api.collection_token == "123"
        mock_get_library.assert_not_called()

        # If its not we get it from the get_library method
        api._collection_token = None
        assert api.collection_token == "abc"
        mock_get_library.assert_called_once()

        # Calling again returns the cached value
        assert api.collection_token == "abc"
        mock_get_library.assert_called_once()

    def test_circulation_lookup(self, overdrive_api_fixture: OverdriveAPIFixture):
        """Test the method that actually looks up Overdrive circulation
        information.
        """
        db = overdrive_api_fixture.db
        overdrive_api_fixture.api.queue_response(200, content="foo")

        # If passed an identifier, we'll use the endpoint() method to
        # construct a v2 availability URL and make a request to
        # it.
        book, (
            status_code,
            headers,
            content,
        ) = overdrive_api_fixture.api.circulation_lookup("an-identifier")
        assert dict(id="an-identifier") == book
        assert 200 == status_code
        assert b"foo" == content

        request_url, ignore1, ignore2 = overdrive_api_fixture.api.requests.pop()
        expect_url = overdrive_api_fixture.api.endpoint(
            overdrive_api_fixture.api.AVAILABILITY_ENDPOINT,
            collection_token=overdrive_api_fixture.api.collection_token,
            product_id="an-identifier",
        )
        assert request_url == expect_url
        assert "/v2/collections" in request_url

        # If passed the result of an API call that includes an
        # availability link, we'll clean up the URL in the link and
        # use it to get our availability data.
        overdrive_api_fixture.api.queue_response(200, content="foo")
        v1 = "https://qa.api.overdrive.com/v1/collections/abcde/products/12345/availability"
        v2 = "https://qa.api.overdrive.com/v2/collections/abcde/products/12345/availability"
        previous_result = dict(availability_link=v1)
        book, (
            status_code,
            headers,
            content,
        ) = overdrive_api_fixture.api.circulation_lookup(previous_result)
        assert previous_result == book
        assert 200 == status_code
        assert b"foo" == content
        request_url, ignore1, ignore2 = overdrive_api_fixture.api.requests.pop()

        # The v1 URL was converted to a v2 url.
        assert v2 == request_url

    def test_update_licensepool_error(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db

        # Create an identifier.
        identifier = db.identifier(identifier_type=Identifier.OVERDRIVE_ID)
        ignore, availability = overdrive_api_fixture.sample_json(
            "overdrive_availability_information.json"
        )
        overdrive_api_fixture.api.queue_response(500, content="An error occured.")
        book = dict(id=identifier.identifier, availability_link=db.fresh_url())
        pool, was_new, changed = overdrive_api_fixture.api.update_licensepool(book)
        assert None == pool

    def test_update_licensepool_not_found(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        # If the Overdrive API says a book is not found in the
        # collection, that's treated as useful information, not an error.
        # Create an identifier.
        identifier = db.identifier(identifier_type=Identifier.OVERDRIVE_ID)
        ignore, not_found = overdrive_api_fixture.sample_json(
            "overdrive_availability_not_found.json"
        )

        # Queue the 'not found' response twice -- once for the circulation
        # lookup and once for the metadata lookup.
        overdrive_api_fixture.api.queue_response(404, content=not_found)
        overdrive_api_fixture.api.queue_response(404, content=not_found)

        book = dict(id=identifier.identifier, availability_link=db.fresh_url())
        pool, was_new, changed = overdrive_api_fixture.api.update_licensepool(book)
        assert 0 == pool.licenses_owned
        assert 0 == pool.licenses_available
        assert 0 == pool.patrons_in_hold_queue

    def test_update_licensepool_provides_bibliographic_coverage(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        # Create an identifier.
        identifier = db.identifier(identifier_type=Identifier.OVERDRIVE_ID)

        # Prepare bibliographic and availability information
        # for this identifier.
        ignore, availability = overdrive_api_fixture.sample_json(
            "overdrive_availability_information.json"
        )
        ignore, bibliographic = overdrive_api_fixture.sample_json(
            "bibliographic_information.json"
        )

        # To avoid a mismatch, make it look like the information is
        # for the newly created Identifier.
        availability["id"] = identifier.identifier
        bibliographic["id"] = identifier.identifier

        overdrive_api_fixture.api.queue_response(200, content=availability)
        overdrive_api_fixture.api.queue_response(200, content=bibliographic)

        # Now we're ready. When we call update_licensepool, the
        # OverdriveAPI will retrieve the availability information,
        # then the bibliographic information. It will then trigger the
        # OverdriveBibliographicCoverageProvider, which will
        # create an Edition and a presentation-ready Work.
        pool, was_new, changed = overdrive_api_fixture.api.update_licensepool(
            identifier.identifier
        )
        assert True == was_new
        assert availability["copiesOwned"] == pool.licenses_owned

        edition = pool.presentation_edition
        assert "Ancillary Justice" == edition.title

        assert True == pool.work.presentation_ready
        assert pool.work.cover_thumbnail_url.startswith(
            "http://images.contentreserve.com/"
        )

        # The book has been run through the bibliographic coverage
        # provider.
        coverage = [
            x
            for x in identifier.coverage_records
            if x.operation is None and x.data_source.name == DataSource.OVERDRIVE
        ]
        assert 1 == len(coverage)

        # Call update_licensepool on an identifier that is missing a work and make
        # sure that it provides bibliographic coverage in that case.
        db.session.delete(pool.work)
        db.session.commit()
        pool, is_new = LicensePool.for_foreign_id(
            db.session,
            DataSource.OVERDRIVE,
            Identifier.OVERDRIVE_ID,
            identifier.identifier,
            collection=overdrive_api_fixture.collection,
        )
        assert not pool.work
        overdrive_api_fixture.api.queue_response(200, content=availability)
        overdrive_api_fixture.api.queue_response(200, content=bibliographic)
        pool, was_new, changed = overdrive_api_fixture.api.update_licensepool(
            identifier.identifier
        )
        assert False == was_new
        assert True == pool.work.presentation_ready

    def test_update_new_licensepool(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db
        data, raw = overdrive_api_fixture.sample_json(
            "overdrive_availability_information.json"
        )

        # Create an identifier
        identifier = db.identifier(identifier_type=Identifier.OVERDRIVE_ID)

        # Make it look like the availability information is for the
        # newly created Identifier.
        raw["reserveId"] = identifier.identifier

        pool, was_new = LicensePool.for_foreign_id(
            db.session,
            DataSource.OVERDRIVE,
            identifier.type,
            identifier.identifier,
            collection=overdrive_api_fixture.collection,
        )

        (
            pool,
            was_new,
            changed,
        ) = overdrive_api_fixture.api.update_licensepool_with_book_info(
            raw, pool, was_new
        )
        assert True == was_new
        assert True == changed

        db.session.commit()
        assert pool is not None
        assert raw["copiesOwned"] == pool.licenses_owned
        assert raw["copiesAvailable"] == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert raw["numberOfHolds"] == pool.patrons_in_hold_queue

    def test_update_existing_licensepool(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db
        data, raw = overdrive_api_fixture.sample_json(
            "overdrive_availability_information.json"
        )

        # Create a LicensePool.
        wr, pool = db.edition(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True,
        )

        # Make it look like the availability information is for the
        # newly created LicensePool.
        raw["id"] = pool.identifier.identifier

        wr.title = "The real title."
        assert 1 == pool.licenses_owned
        assert 1 == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert 0 == pool.patrons_in_hold_queue

        (
            p2,
            was_new,
            changed,
        ) = overdrive_api_fixture.api.update_licensepool_with_book_info(
            raw, pool, False
        )
        assert False == was_new
        assert True == changed
        assert p2 == pool
        # The title didn't change to that title given in the availability
        # information, because we already set a title for that work.
        assert "The real title." == wr.title
        assert raw["copiesOwned"] == pool.licenses_owned
        assert raw["copiesAvailable"] == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert raw["numberOfHolds"] == pool.patrons_in_hold_queue

    def test_update_new_licensepool_when_same_book_has_pool_in_different_collection(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db
        old_edition, old_pool = db.edition(
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            with_license_pool=True,
        )
        old_pool.calculate_work()
        collection = db.collection()

        data, raw = overdrive_api_fixture.sample_json(
            "overdrive_availability_information.json"
        )

        # Make it look like the availability information is for the
        # old pool's Identifier.
        identifier = old_pool.identifier
        raw["id"] = identifier.identifier

        new_pool, was_new = LicensePool.for_foreign_id(
            db.session,
            DataSource.OVERDRIVE,
            identifier.type,
            identifier.identifier,
            collection=collection,
        )
        # The new pool doesn't have a presentation edition yet,
        # but it will be updated to share the old pool's edition.
        assert new_pool is not None
        assert None == new_pool.presentation_edition

        (
            new_pool,
            was_new,
            changed,
        ) = overdrive_api_fixture.api.update_licensepool_with_book_info(
            raw, new_pool, was_new
        )
        assert new_pool is not None
        assert True == was_new
        assert True == changed
        assert old_edition == new_pool.presentation_edition
        assert old_pool.work == new_pool.work

    def test_update_licensepool_with_holds(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db
        data, raw = overdrive_api_fixture.sample_json(
            "overdrive_availability_information_holds.json"
        )
        identifier = db.identifier(identifier_type=Identifier.OVERDRIVE_ID)
        raw["id"] = identifier.identifier

        license_pool, is_new = LicensePool.for_foreign_id(
            db.session,
            DataSource.OVERDRIVE,
            identifier.type,
            identifier.identifier,
            collection=db.default_collection(),
        )
        (
            pool,
            was_new,
            changed,
        ) = overdrive_api_fixture.api.update_licensepool_with_book_info(
            raw, license_pool, is_new
        )
        assert 10 == pool.patrons_in_hold_queue
        assert True == changed

    def test__refresh_patron_oauth_token(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        """Verify that patron information is included in the request
        when refreshing a patron access token.
        """
        db = overdrive_api_fixture.db
        patron = db.patron()
        patron.authorization_identifier = "barcode"
        credential = db.credential(patron=patron)

        # Try to refresh the patron access token with a PIN, and
        # then without a PIN.
        overdrive_api_fixture.api._refresh_patron_oauth_token(
            credential, patron, "a pin"
        )

        overdrive_api_fixture.api._refresh_patron_oauth_token(credential, patron, None)

        # Verify that the requests that were made correspond to what
        # Overdrive is expecting.
        with_pin, without_pin = overdrive_api_fixture.api.access_token_requests
        url, (payload, headers), kwargs = with_pin
        assert "https://oauth-patron.overdrive.com/patrontoken" == url
        assert "barcode" == payload["username"]
        expect_scope = "websiteid:{} authorizationname:{}".format(
            overdrive_api_fixture.api.website_id(),
            overdrive_api_fixture.api.ils_name(patron.library),
        )
        assert expect_scope == payload["scope"]
        assert "a pin" == payload["password"]
        assert not "password_required" in payload

        url, (payload, headers), kwargs = without_pin
        assert "https://oauth-patron.overdrive.com/patrontoken" == url
        assert "barcode" == payload["username"]
        assert expect_scope == payload["scope"]
        assert "false" == payload["password_required"]
        assert "[ignore]" == payload["password"]

    def test__refresh_patron_oauth_token_failure(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ) -> None:
        db = overdrive_api_fixture.db
        patron = db.patron()
        patron.authorization_identifier = "barcode"
        credential = db.credential(patron=patron)

        # Test with a real 400 response we've seen from overdrive
        data, raw = overdrive_api_fixture.sample_json("patron_token_failed.json")
        overdrive_api_fixture.api.access_token_response = MockRequestsResponse(
            400, content=raw
        )
        with pytest.raises(
            PatronAuthorizationFailedException, match="Invalid Library Card"
        ):
            overdrive_api_fixture.api._refresh_patron_oauth_token(
                credential, patron, "a pin"
            )

        # Test with a fictional 403 response that doesn't contain valid json - we've never
        # seen this come back from overdrive, this test is just to make sure we can handle
        # unexpected responses back from OD API.
        overdrive_api_fixture.api.access_token_response = MockRequestsResponse(
            403, content="garbage { json"
        )
        with pytest.raises(
            PatronAuthorizationFailedException,
            match="Failed to authenticate with Overdrive",
        ):
            overdrive_api_fixture.api._refresh_patron_oauth_token(
                credential, patron, "a pin"
            )

    def test__refresh_patron_oauth_token_palace_context(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        """Verify that patron information is included in the request
        when refreshing a patron access token.
        """
        db = overdrive_api_fixture.db
        patron = db.patron()
        patron.authorization_identifier = "barcode"
        credential = db.credential(patron=patron)

        # Mocked testing credentials
        encoded_auth = base64.b64encode("TestingKey:TestingSecret")

        # use a real Overdrive API
        od_api = OverdriveAPI(db.session, overdrive_api_fixture.collection)
        od_api._server_nickname = OverdriveConstants.TESTING_SERVERS
        # but mock the request methods
        post_response, _ = overdrive_api_fixture.sample_json("patron_token.json")
        od_api._do_post = MagicMock(
            return_value=MockRequestsResponse(200, content=post_response)
        )
        od_api._do_get = MagicMock()
        response_credential = od_api._refresh_patron_oauth_token(
            credential, patron, "a pin", palace_context=True
        )

        # Posted once, no gets
        od_api._do_post.assert_called_once()
        od_api._do_get.assert_not_called()

        # What did we Post?
        call_args = od_api._do_post.call_args[0]
        assert "/patrontoken" in call_args[0]  # url
        assert (
            call_args[2]["Authorization"] == f"Basic {encoded_auth}"
        )  # Basic header should be that of the fulfillment keys
        assert response_credential == credential

    def test_cannot_fulfill_error_audiobook(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db
        patron = db.patron()
        patron.authorization_identifier = "barcode"
        # use a real Overdrive API
        od_api = OverdriveAPI(db.session, overdrive_api_fixture.collection)
        od_api._server_nickname = OverdriveConstants.TESTING_SERVERS
        od_api.get_loan = MagicMock(return_value={"isFormatLockedIn": True})
        od_api.get_download_link = MagicMock(return_value=None)

        exc = pytest.raises(
            CannotFulfill,
            od_api.get_fulfillment_link,
            *(patron, "pin", "odid", "audiobook-overdrive-manifest"),
        )
        assert exc.match("No download link for")

        # Cannot fulfill error within the get auth function
        os.environ.pop(
            f"{Configuration.OD_PREFIX_TESTING_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_KEY_SUFFIX}"
        )
        with pytest.raises(CannotFulfill):
            od_api._palace_context_basic_auth_header

    def test_no_drm_fulfillment(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db
        patron = db.patron()
        work = db.work(with_license_pool=True)
        patron.authorization_identifier = "barcode"

        od_api = OverdriveAPI(db.session, overdrive_api_fixture.collection)
        od_api._server_nickname = OverdriveConstants.TESTING_SERVERS

        # Load the mock API data
        api_data = json.loads(
            overdrive_api_fixture.data.sample_data("no_drm_fulfill.json")
        )

        # Mock out the flow
        od_api.get_loan = MagicMock(return_value=api_data["loan"])

        mock_lock_in_response = create_autospec(Response)
        mock_lock_in_response.status_code = 200
        mock_lock_in_response.json.return_value = api_data["lock_in"]
        od_api.lock_in_format = MagicMock(return_value=mock_lock_in_response)

        od_api.get_fulfillment_link_from_download_link = MagicMock(
            return_value=(
                "https://example.org/epub-redirect",
                "application/epub+zip",
            )
        )

        # Mock delivery mechanism
        delivery_mechanism = create_autospec(LicensePoolDeliveryMechanism)
        delivery_mechanism.delivery_mechanism = create_autospec(DeliveryMechanism)
        delivery_mechanism.delivery_mechanism.drm_scheme = DeliveryMechanism.NO_DRM
        delivery_mechanism.delivery_mechanism.content_type = (
            Representation.EPUB_MEDIA_TYPE
        )

        fulfill = od_api.fulfill(
            patron, "pin", work.active_license_pool(), delivery_mechanism
        )
        assert isinstance(fulfill, RedirectFulfillment)
        assert fulfill.content_type == Representation.EPUB_MEDIA_TYPE
        assert fulfill.content_link == "https://example.org/epub-redirect"

    def test_drm_fulfillment(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db
        patron = db.patron()
        work = db.work(with_license_pool=True)
        patron.authorization_identifier = "barcode"

        od_api = OverdriveAPI(db.session, overdrive_api_fixture.collection)
        od_api._server_nickname = OverdriveConstants.TESTING_SERVERS

        # Mock get fulfillment link
        od_api.get_fulfillment_link = MagicMock(
            return_value=("http://example.com/acsm", "application/vnd.adobe.adept+xml")
        )

        # Mock delivery mechanism
        delivery_mechanism = create_autospec(LicensePoolDeliveryMechanism)
        delivery_mechanism.delivery_mechanism = create_autospec(DeliveryMechanism)
        delivery_mechanism.delivery_mechanism.drm_scheme = DeliveryMechanism.ADOBE_DRM
        delivery_mechanism.delivery_mechanism.content_type = (
            Representation.EPUB_MEDIA_TYPE
        )

        fulfill = od_api.fulfill(
            patron, "pin", work.active_license_pool(), delivery_mechanism
        )
        assert isinstance(fulfill, FetchFulfillment)
        assert fulfill.content_type == "application/vnd.adobe.adept+xml"
        assert fulfill.content_link == "http://example.com/acsm"

    def test_no_recently_changed_books(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        with patch.object(
            overdrive_api_fixture.api, "_get_book_list_page"
        ) as get_book_list:
            get_book_list.return_value = ([], None)
            result = overdrive_api_fixture.api.recently_changed_ids(utc_now(), None)
            assert [i for i in result] == []


class TestOverdriveAPICredentials:
    def test_patron_correct_credentials_for_multiple_overdrive_collections(
        self, db: DatabaseTransactionFixture
    ):
        # Verify that the correct credential will be used
        # when a library has more than one OverDrive collection.

        def _optional_value(self, obj, key):
            return obj.get(key, "none")

        def _make_token(scope, username, password, grant_type="password"):
            return f"{grant_type}|{scope}|{username}|{password}"

        class MockAPI(MockOverdriveAPI):
            def _do_post(self, url, payload, headers, **kwargs):
                url = self.endpoint(url)
                self.access_token_requests.append((url, payload, headers, kwargs))
                token = _make_token(
                    _optional_value(self, payload, "scope"),
                    _optional_value(self, payload, "username"),
                    _optional_value(self, payload, "password"),
                    grant_type=_optional_value(self, payload, "grant_type"),
                )
                response = self.mock_access_token_response(token)

                from palace.manager.util.http import HTTP

                return HTTP._process_response(url, response, **kwargs)

        library = db.default_library()
        patron = db.patron(library=library)
        patron.authorization_identifier = "patron_barcode"
        pin = "patron_pin"

        # clear out any collections added before we add ours
        for collection in library.associated_collections:
            collection.associated_libraries = []

        # Distinct credentials for the two OverDrive collections in which our
        # library has membership.
        library_collection_properties = [
            dict(
                library=library,
                name="Test OD Collection 1",
                client_key="client_key_1",
                client_secret="client_secret_1",
                library_id="lib_id_1",
                website_id="ws_id_1",
                ils_name="lib1_coll1_ils",
            ),
            dict(
                library=library,
                name="Test OD Collection 2",
                client_key="client_key_2",
                client_secret="client_secret_2",
                library_id="lib_id_2",
                website_id="ws_id_2",
                ils_name="lib1_coll2_ils",
            ),
        ]

        # These are the credentials we'll expect for each of our collections.
        expected_credentials = {
            str(props["name"]): _make_token(
                "websiteid:%s authorizationname:%s"
                % (props["website_id"], props["ils_name"]),
                patron.authorization_identifier,
                pin,
            )
            for props in library_collection_properties
        }

        # Add the collections.
        collections = [
            MockAPI.mock_collection(db.session, **props)  # type: ignore[arg-type]
            for props in library_collection_properties
        ]

        od_apis = {
            collection.name: MockAPI(db.session, collection)
            for collection in collections
        }

        # Ensure that we have the correct number of OverDrive collections.
        assert len(library_collection_properties) == len(od_apis)

        # Verify that the expected credentials match what we got.
        for name in list(expected_credentials.keys()) + list(
            reversed(list(expected_credentials.keys()))
        ):
            credential = od_apis[name]._get_patron_oauth_credential(patron, pin)
            assert expected_credentials[name] == credential.credential

    def test_fulfillment_credentials_testing_keys(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        test_key = "tk"
        test_secret = "ts"

        monkeypatch.setenv(
            f"{Configuration.OD_PREFIX_TESTING_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_KEY_SUFFIX}",
            test_key,
        )
        monkeypatch.setenv(
            f"{Configuration.OD_PREFIX_TESTING_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_SECRET_SUFFIX}",
            test_secret,
        )

        testing_credentials = Configuration.overdrive_fulfillment_keys(testing=True)
        assert testing_credentials["key"] == test_key
        assert testing_credentials["secret"] == test_secret

        prod_key = "pk"
        prod_secret = "ps"

        monkeypatch.setenv(
            f"{Configuration.OD_PREFIX_PRODUCTION_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_KEY_SUFFIX}",
            prod_key,
        )
        monkeypatch.setenv(
            f"{Configuration.OD_PREFIX_PRODUCTION_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_SECRET_SUFFIX}",
            prod_secret,
        )

        prod_credentials = Configuration.overdrive_fulfillment_keys()
        assert prod_credentials["key"] == prod_key
        assert prod_credentials["secret"] == prod_secret

    def test_fulfillment_credentials_cannot_load(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv(
            f"{Configuration.OD_PREFIX_PRODUCTION_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_KEY_SUFFIX}",
            raising=False,
        )
        pytest.raises(CannotLoadConfiguration, Configuration.overdrive_fulfillment_keys)

        monkeypatch.delenv(
            f"{Configuration.OD_PREFIX_TESTING_PREFIX}_{Configuration.OD_FULFILLMENT_CLIENT_KEY_SUFFIX}",
            raising=False,
        )
        pytest.raises(
            CannotLoadConfiguration,
            Configuration.overdrive_fulfillment_keys,
            testing=True,
        )


class TestExtractData:
    def test_get_download_link(self, overdrive_api_fixture: OverdriveAPIFixture):
        data, json = overdrive_api_fixture.sample_json(
            "checkout_response_locked_in_format.json"
        )
        url = MockOverdriveAPI.get_download_link(
            json, "ebook-epub-adobe", "http://foo.com/"
        )
        assert (
            "http://patron.api.overdrive.com/v1/patrons/me/checkouts/76C1B7D0-17F4-4C05-8397-C66C17411584/formats/ebook-epub-adobe/downloadlink?errorpageurl=http://foo.com/"
            == url
        )

        pytest.raises(
            NoAcceptableFormat,
            MockOverdriveAPI.get_download_link,
            json,
            "no-such-format",
            "http://foo.com/",
        )

    def test_get_download_link_raises_exception_if_loan_fulfilled_on_incompatible_platform(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        data, json = overdrive_api_fixture.sample_json(
            "checkout_response_book_fulfilled_on_kindle.json"
        )
        pytest.raises(
            FulfilledOnIncompatiblePlatform,
            MockOverdriveAPI.get_download_link,
            json,
            "ebook-epub-adobe",
            "http://foo.com/",
        )

    def test_get_download_link_for_manifest_format(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        # If you ask for the download link for an 'x-manifest' format,
        # it's treated as a variant of the 'x' format.
        data, json = overdrive_api_fixture.sample_json(
            "checkout_response_book_fulfilled_on_kindle.json"
        )

        # This is part of the URL from `json` that we expect
        # get_download_link to use as a base.
        base_url = "http://patron.api.overdrive.com/v1/patrons/me/checkouts/98EA8135-52C0-4480-9C0E-1D0779670D4A/formats/ebook-overdrive/downloadlink"

        # First, let's ask for the streaming format.
        link = MockOverdriveAPI.get_download_link(
            json, "ebook-overdrive", "http://foo.com/"
        )

        # The base URL is returned, with {errorpageurl} filled in and
        # {odreadauthurl} left for other code to fill in.
        assert (
            base_url + "?errorpageurl=http://foo.com/&odreadauthurl={odreadauthurl}"
            == link
        )

        # Now let's ask for the manifest format.
        link = MockOverdriveAPI.get_download_link(
            json, "ebook-overdrive-manifest", "http://bar.com/"
        )

        # The {errorpageurl} and {odreadauthurl} parameters
        # have been removed, and contentfile=true has been appended.
        assert base_url + "?contentfile=true" == link

    def test_extract_download_link(self, overdrive_api_fixture: OverdriveAPIFixture):
        # Verify that extract_download_link can or cannot find a
        # download link for a given format subdocument.

        class Mock(OverdriveAPI):
            called_with = None

            @classmethod
            def make_direct_download_link(cls, download_link):
                cls.called_with = download_link
                return "http://manifest/"

        m = Mock.extract_download_link
        error_url = "http://error/"

        # Here we don't even know the name of the format.
        empty: dict[str, Any] = dict()
        with pytest.raises(IOError) as excinfo:
            m(empty, error_url)
        assert "No linkTemplates for format (unknown)" in str(excinfo.value)

        # Here we know the name, but there are no link templates.
        no_templates = dict(formatType="someformat")
        with pytest.raises(IOError) as excinfo:
            m(no_templates, error_url)
        assert "No linkTemplates for format someformat" in str(excinfo.value)

        # Here there's a link template structure, but no downloadLink
        # inside.
        no_download_link = dict(formatType="someformat", linkTemplates=dict())
        with pytest.raises(IOError) as excinfo:
            m(no_download_link, error_url)
        assert "No downloadLink for format someformat" in str(excinfo.value)

        # Here there's a downloadLink structure, but no href inside.
        href_is_missing = dict(
            formatType="someformat", linkTemplates=dict(downloadLink=dict())
        )
        with pytest.raises(IOError) as excinfo:
            m(href_is_missing, error_url)
        assert "No downloadLink href for format someformat" in str(excinfo.value)

        # Now we finally get to the cases where there is an actual
        # download link.  The behavior is different based on whether
        # or not we want to return a link to the manifest file.

        working = dict(
            formatType="someformat",
            linkTemplates=dict(
                downloadLink=dict(href="http://download/?errorpageurl={errorpageurl}")
            ),
        )

        # If we don't want a manifest, make_direct_download_link is
        # not called.
        do_not_fetch_manifest = m(working, error_url, fetch_manifest=False)
        assert None == Mock.called_with

        # The errorpageurl template is filled in.
        assert "http://download/?errorpageurl=http://error/" == do_not_fetch_manifest

        # If we do want a manifest, make_direct_download_link is called
        # without errorpageurl being affected.
        do_fetch_manifest = m(working, error_url, fetch_manifest=True)
        assert "http://download/?errorpageurl={errorpageurl}" == Mock.called_with
        assert "http://manifest/" == do_fetch_manifest

    def test_make_direct_download_link(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        # Verify that make_direct_download_link handles various more
        # or less weird URLs that the Overdrive might or might not
        # serve.
        base = "http://overdrive/downloadlink"
        m = OverdriveAPI.make_direct_download_link
        assert base + "?contentfile=true" == m(base)
        assert base + "?contentfile=true" == m(base + "?odreadauthurl={odreadauthurl}")
        assert base + "?other=other&contentfile=true" == m(
            base + "?odreadauthurl={odreadauthurl}&other=other"
        )

    def test_extract_data_from_checkout_resource(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        data, json = overdrive_api_fixture.sample_json(
            "checkout_response_locked_in_format.json"
        )
        expires, url = MockOverdriveAPI.extract_data_from_checkout_response(
            json, "ebook-epub-adobe", "http://foo.com/"
        )
        assert 2013 == expires.year
        assert 10 == expires.month
        assert 4 == expires.day
        assert (
            "http://patron.api.overdrive.com/v1/patrons/me/checkouts/76C1B7D0-17F4-4C05-8397-C66C17411584/formats/ebook-epub-adobe/downloadlink?errorpageurl=http://foo.com/"
            == url
        )

    def test_process_checkout_data(self, overdrive_api_fixture: OverdriveAPIFixture):
        data, json = overdrive_api_fixture.sample_json(
            "shelf_with_book_already_fulfilled_on_kindle.json"
        )
        [on_kindle, not_on_kindle] = json["checkouts"]

        # The book already fulfilled on Kindle doesn't get turned into
        # LoanInfo at all.
        assert None == MockOverdriveAPI.process_checkout_data(
            on_kindle, overdrive_api_fixture.collection
        )

        # The book not yet fulfilled does show up as a LoanInfo.
        loan_info = MockOverdriveAPI.process_checkout_data(
            not_on_kindle, overdrive_api_fixture.collection
        )
        assert loan_info is not None
        assert "2fadd2ac-a8ec-4938-a369-4c3260e8922b" == loan_info.identifier

        # Since there are two usable formats (Adobe EPUB and Adobe
        # PDF), the LoanInfo is not locked to any particular format.
        assert None == loan_info.locked_to

        # A book that's on loan and locked to a specific format has a
        # DeliveryMechanismInfo associated with that format.
        data, format_locked_in = overdrive_api_fixture.sample_json(
            "checkout_response_locked_in_format.json"
        )
        loan_info = MockOverdriveAPI.process_checkout_data(
            format_locked_in, overdrive_api_fixture.collection
        )
        assert loan_info is not None
        delivery = loan_info.locked_to
        assert delivery is not None
        assert Representation.EPUB_MEDIA_TYPE == delivery.content_type
        assert DeliveryMechanism.ADOBE_DRM == delivery.drm_scheme

        # This book is on loan and the choice between Kindle and Adobe
        # EPUB has not yet been made, but as far as we're concerned,
        # Adobe EPUB is the only *usable* format, so it's effectively
        # locked.
        data, no_format_locked_in = overdrive_api_fixture.sample_json(
            "checkout_response_no_format_locked_in.json"
        )
        loan_info = MockOverdriveAPI.process_checkout_data(
            no_format_locked_in, overdrive_api_fixture.collection
        )
        assert loan_info is not None
        delivery = loan_info.locked_to
        assert delivery is not None
        assert Representation.EPUB_MEDIA_TYPE == delivery.content_type
        assert DeliveryMechanism.ADOBE_DRM == delivery.drm_scheme

        # TODO: In the future both of these tests should return a
        # LoanInfo with appropriate Fulfillment. The calling code
        # would then decide whether or not to show the loan.


class TestSyncBookshelf:
    def test_sync_patron_activity_creates_local_loans(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        loans_data, json_loans = overdrive_api_fixture.sample_json(
            "shelf_with_some_checked_out_books.json"
        )
        holds_data, json_holds = overdrive_api_fixture.sample_json("no_holds.json")

        overdrive_api_fixture.api.queue_response(200, content=loans_data)
        overdrive_api_fixture.api.queue_response(200, content=holds_data)

        patron = db.patron()
        loans, holds = overdrive_api_fixture.sync_patron_activity(patron)

        # All four loans in the sample data were created.
        assert 4 == len(loans)
        assert set(loans.values()) == set(patron.loans)

        # We have created previously unknown LicensePools and
        # Identifiers.
        identifiers = [
            str(loan.license_pool.identifier.identifier) for loan in loans.values()
        ]
        assert sorted(
            [
                "a5a3d737-34d4-4d69-aad8-eba4e46019a3",
                "99409f99-45a5-4238-9e10-98d1435cde04",
                "993e4b33-823c-40af-8f61-cac54e1cba5d",
                "a2ec6f3a-ebfe-4c95-9638-2cb13be8de5a",
            ]
        ) == sorted(identifiers)

        # We have recorded a new DeliveryMechanism associated with
        # each loan.
        mechanisms = {
            (
                loan.fulfillment.delivery_mechanism.content_type,
                loan.fulfillment.delivery_mechanism.drm_scheme,
            )
            for loan in loans.values()
            if loan.fulfillment
        }
        assert {
            (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM),
            (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
        } == mechanisms

        # There are no holds.
        assert {} == holds

        # Running the sync again leaves all four loans in place.
        overdrive_api_fixture.api.queue_response(200, content=loans_data)
        overdrive_api_fixture.api.queue_response(200, content=holds_data)
        loans, holds = overdrive_api_fixture.sync_patron_activity(patron)
        assert 4 == len(loans)
        assert set(loans.values()) == set(patron.loans)

    def test_sync_patron_activity_removes_loans_not_present_on_remote(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db
        loans_data, json_loans = overdrive_api_fixture.sample_json(
            "shelf_with_some_checked_out_books.json"
        )
        holds_data, json_holds = overdrive_api_fixture.sample_json("no_holds.json")

        overdrive_api_fixture.api.queue_response(200, content=loans_data)
        overdrive_api_fixture.api.queue_response(200, content=holds_data)

        # Create a loan not present in the sample data.
        patron = db.patron()
        overdrive_edition, new = db.edition(
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
            collection=overdrive_api_fixture.collection,
        )
        [pool] = overdrive_edition.license_pools
        overdrive_loan, new = pool.loan_to(patron)
        yesterday = utc_now() - timedelta(days=1)
        overdrive_loan.start = yesterday

        # Sync with Overdrive, and the loan not present in the sample
        # data is removed.
        loans, holds = overdrive_api_fixture.sync_patron_activity(patron)

        assert 4 == len(loans)
        assert set(loans.values()) == set(patron.loans)
        assert overdrive_loan not in patron.loans

    def test_sync_patron_activity_ignores_loans_from_other_sources(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        patron = db.patron()
        gutenberg, new = db.edition(
            data_source_name=DataSource.GUTENBERG, with_license_pool=True
        )
        [pool] = gutenberg.license_pools
        gutenberg_loan, new = pool.loan_to(patron)
        loans_data, json_loans = overdrive_api_fixture.sample_json(
            "shelf_with_some_checked_out_books.json"
        )
        holds_data, json_holds = overdrive_api_fixture.sample_json("no_holds.json")

        # Overdrive doesn't know about the Gutenberg loan, but it was
        # not destroyed, because it came from another source.
        overdrive_api_fixture.api.queue_response(200, content=loans_data)
        overdrive_api_fixture.api.queue_response(200, content=holds_data)

        overdrive_api_fixture.sync_patron_activity(patron)
        assert 5 == len(patron.loans)
        assert gutenberg_loan in patron.loans

    def test_sync_patron_activity_creates_local_holds(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        loans_data, json_loans = overdrive_api_fixture.sample_json("no_loans.json")
        holds_data, json_holds = overdrive_api_fixture.sample_json("holds.json")

        overdrive_api_fixture.api.queue_response(200, content=loans_data)
        overdrive_api_fixture.api.queue_response(200, content=holds_data)
        patron = db.patron()

        loans, holds = overdrive_api_fixture.sync_patron_activity(patron)
        # All four loans in the sample data were created.
        assert 4 == len(holds)
        assert sorted(holds.values()) == sorted(patron.holds)

        # Running the sync again leaves all four holds in place.
        overdrive_api_fixture.api.queue_response(200, content=loans_data)
        overdrive_api_fixture.api.queue_response(200, content=holds_data)
        loans, holds = overdrive_api_fixture.sync_patron_activity(patron)
        assert 4 == len(holds)
        assert sorted(holds.values()) == sorted(patron.holds)

    def test_sync_patron_activity_removes_holds_not_present_on_remote(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        loans_data, json_loans = overdrive_api_fixture.sample_json("no_loans.json")
        holds_data, json_holds = overdrive_api_fixture.sample_json("holds.json")

        patron = db.patron()
        overdrive_edition, new = db.edition(
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
            collection=overdrive_api_fixture.collection,
        )
        [pool] = overdrive_edition.license_pools
        overdrive_hold, new = pool.on_hold_to(patron)
        yesterday = utc_now() - timedelta(days=1)
        overdrive_hold.start = yesterday

        overdrive_api_fixture.api.queue_response(200, content=loans_data)
        overdrive_api_fixture.api.queue_response(200, content=holds_data)

        # The hold not present in the sample data has been removed
        loans, holds = overdrive_api_fixture.sync_patron_activity(patron)
        assert 4 == len(holds)
        assert set(holds.values()) == set(patron.holds)
        assert overdrive_hold not in patron.holds

    def test_sync_patron_activity_ignores_holds_from_other_collections(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db

        loans_data, json_loans = overdrive_api_fixture.sample_json("no_loans.json")
        holds_data, json_holds = overdrive_api_fixture.sample_json("holds.json")
        patron = db.patron()

        # This patron has an Overdrive book on hold, but it derives
        # from an Overdrive Collection that's not managed by
        # self.circulation.
        overdrive, new = db.edition(
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
            collection=db.collection(),
        )
        [pool] = overdrive.license_pools
        overdrive_hold, new = pool.on_hold_to(patron)

        overdrive_api_fixture.api.queue_response(200, content=loans_data)
        overdrive_api_fixture.api.queue_response(200, content=holds_data)

        # overdrive_api_fixture.api doesn't know about the hold, but it was not
        # destroyed, because it came from a different collection.
        overdrive_api_fixture.sync_patron_activity(patron)
        assert 5 == len(patron.holds)
        assert overdrive_hold in patron.holds

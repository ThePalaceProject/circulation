from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, create_autospec, patch

import pytest

from palace.manager.api.circulation.data import HoldInfo, LoanInfo
from palace.manager.api.circulation.exceptions import (
    CannotFulfill,
    CannotLoan,
    CannotReturn,
    FormatNotAvailable,
    FulfilledOnIncompatiblePlatform,
    NoAcceptableFormat,
    NoAvailableCopies,
    NotCheckedOut,
    PatronAuthorizationFailedException,
    PatronHoldLimitReached,
)
from palace.manager.api.circulation.fulfillment import (
    FetchFulfillment,
    RedirectFulfillment,
)
from palace.manager.api.config import Configuration
from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.core.exceptions import BasePalaceException, IntegrationException
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.integration.license.overdrive.constants import OverdriveConstants
from palace.manager.integration.license.overdrive.exception import (
    OverdriveValidationError,
)
from palace.manager.integration.license.overdrive.fulfillment import (
    OverdriveManifestFulfillment,
)
from palace.manager.integration.license.overdrive.model import Checkout, Format, Link
from palace.manager.integration.license.overdrive.representation import (
    OverdriveRepresentationExtractor,
)
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.patron import Hold
from palace.manager.sqlalchemy.model.resource import Representation
from palace.manager.util import base64
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http.exception import BadResponseException
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture
from tests.fixtures.overdrive import OverdriveAPIFixture
from tests.fixtures.services import ServicesFixture
from tests.fixtures.webserver import MockAPIServer, MockAPIServerResponse
from tests.mocks.mock import MockRequestsResponse


class TestOverdriveAPI:
    def test_patron_activity_exception_collection_none(
        self,
        overdrive_api_fixture: OverdriveAPIFixture,
        db: DatabaseTransactionFixture,
    ):
        api = OverdriveAPI(db.session, overdrive_api_fixture.collection)
        db.session.delete(overdrive_api_fixture.collection)
        patron = db.patron()
        with pytest.raises(
            BasePalaceException,
            match=r"Collection with id \d* not found for OverdriveAPI",
        ):
            api.sync_patron_activity(patron, "pin")

    def test_errors_not_retried(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        mock_web_server: MockAPIServer,
    ):
        overdrive_api_fixture.mock_http.stop_patch()
        collection = overdrive_api_fixture.collection

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

        api = OverdriveAPI(db.session, collection)
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
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
    ):
        collection = overdrive_api_fixture.collection

        exception_message = "This is a unit test, you can't make HTTP requests!"
        with (
            patch.object(
                OverdriveAPI, "_do_get", side_effect=Exception(exception_message)
            ),
            patch.object(
                OverdriveAPI, "_do_post", side_effect=Exception(exception_message)
            ),
        ):
            # Invoking the OverdriveAPI constructor does not, by itself,
            # make any HTTP requests.
            api = OverdriveAPI(db.session, collection)

            # Attempting to access ._client_oauth_token or .collection_token _will_
            # try to make an HTTP request.
            with pytest.raises(Exception, match=exception_message):
                api.collection_token

            with pytest.raises(Exception, match=exception_message):
                api._client_oauth_token

    def test_ils_name(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        """The 'ils_name' setting (set in OverdriveAPIFixture.create_collection) is available
        through OverdriveAPI.ils_name().
        """
        api = overdrive_api_fixture.api
        assert api.ils_name(overdrive_api_fixture.library) == "e"

        # The value must be explicitly set for a given library, or
        # else the default will be used.
        l2 = db.library()
        assert api.ils_name(l2) == "default"

    def test_hosts(self, db: DatabaseTransactionFixture):
        # By default, OverdriveAPI is initialized with the production
        # set of hostnames.
        collection = db.collection(
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(),
        )
        testing = OverdriveAPI(db.session, collection)
        assert (
            testing.hosts() == OverdriveAPI.HOSTS[OverdriveConstants.PRODUCTION_SERVERS]
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
        # The .endpoint() method performs string interpolation, including
        # the names of servers.
        api = overdrive_api_fixture.api
        template = (
            "%(host)s %(patron_host)s %(oauth_host)s %(oauth_patron_host)s %(extra)s"
        )
        result = api.endpoint(template, extra="val")

        # The host names and the 'extra' argument have been used to
        # fill in the string interpolations.
        expect_args = dict(api.hosts())
        expect_args["extra"] = "val"
        assert template % expect_args == result

        # The string has been completely interpolated.
        assert "%" not in result

        # Once interpolation has happened, doing it again has no effect.
        assert api.endpoint(result, extra="something else") == result

        # This is important because an interpolated URL may superficially
        # appear to contain extra formatting characters.
        assert api.endpoint(result + "%3A", extra="something else") == result + "%3A"

    def test__collection_context_basic_auth_header(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        # Verify that the Authorization header needed to get an access
        # token for a given collection is encoded properly.
        api = overdrive_api_fixture.api
        assert api._collection_context_basic_auth_header == "Basic YTpi"
        assert (
            api._collection_context_basic_auth_header
            == "Basic "
            + base64.standard_b64encode(f"{api.client_key()}:{api.client_secret()}")
        )

    def test_get_success(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        api = overdrive_api_fixture.api
        http = overdrive_api_fixture.mock_http
        http.queue_response(200, content="some content")
        status_code, headers, content = api.get(db.fresh_url(), {})
        assert 200 == status_code
        assert b"some content" == content

    def test_failure_to_get_library_is_fatal(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        http = overdrive_api_fixture.mock_http
        http.queue_response(500)
        with pytest.raises(BadResponseException) as excinfo:
            overdrive_api_fixture.api.get_library()
        assert "Got status code 500" in str(excinfo.value)

    def test_error_getting_library(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        collection = overdrive_api_fixture.collection

        # This Overdrive client has valid credentials but the library
        # can't be found -- probably because the library ID is wrong.
        with patch.object(
            OverdriveAPI,
            "get_library",
            return_value={
                "errorCode": "Some error",
                "message": "Some message.",
                "token": "abc-def-ghi",
            },
        ):
            # Just instantiating the API doesn't cause this error.
            api = OverdriveAPI(db.session, collection)

            # But trying to access the collection token will cause it.
            with pytest.raises(
                CannotLoadConfiguration,
                match="Overdrive credentials are valid but could not fetch library: Some message.",
            ):
                api.collection_token

    def test_401_on_get_refreshes_bearer_token(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        http = overdrive_api_fixture.mock_http

        # We have a token.
        assert (
            overdrive_api_fixture.api._client_oauth_token == "fake client oauth token"
        )

        # But then we try to GET, and receive a 401.
        http.queue_response(401)

        # We refresh the bearer token.
        overdrive_api_fixture.queue_access_token_response("new bearer token")

        # Then we retry the GET and it succeeds this time.
        http.queue_response(200, content="at last, the content")

        assert overdrive_api_fixture.api.get(db.fresh_url(), {}) == (
            200,
            {},
            b"at last, the content",
        )

        # The bearer token has been updated.
        assert overdrive_api_fixture.api._client_oauth_token == "new bearer token"

    def test__client_oauth_token(self, overdrive_api_fixture: OverdriveAPIFixture):
        """Verify the process of refreshing the Overdrive bearer token."""
        api = overdrive_api_fixture.api
        http = overdrive_api_fixture.mock_http

        # Initially the cached token is None
        api._cached_client_oauth_token = None

        # Accessing the token triggers a refresh
        overdrive_api_fixture.queue_access_token_response("bearer token")
        assert api._client_oauth_token == "bearer token"
        assert len(http.requests) == 1

        # Queue up another bearer token response
        overdrive_api_fixture.queue_access_token_response("new bearer token")

        # Accessing the token again won't refresh, because the old token is still valid
        assert api._client_oauth_token == "bearer token"
        assert len(http.requests) == 1

        # However if the token expires we will get a new one
        assert api._cached_client_oauth_token is not None
        api._cached_client_oauth_token = api._cached_client_oauth_token._replace(
            expires=utc_now() - timedelta(seconds=1)
        )

        assert api._client_oauth_token == "new bearer token"
        assert len(http.requests) == 2

    def test_401_after__refresh_client_oauth_token_raises_error(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        api = overdrive_api_fixture.api
        http = overdrive_api_fixture.mock_http

        # We try to GET and receive a 401.
        http.queue_response(401)

        # We refresh the bearer token.
        overdrive_api_fixture.queue_access_token_response("new bearer token")

        # Then we retry the GET but we get another 401.
        http.queue_response(401)

        # That raises a BadResponseException
        with pytest.raises(
            BadResponseException,
            match="Bad response from .*: Something's wrong with the Overdrive OAuth Bearer Token",
        ):
            api.get_library()

        # We refreshed the token in the process.
        assert overdrive_api_fixture.api._client_oauth_token == "new bearer token"

        # We made three requests, one for the original GET, one for the token refresh,
        # and one for the retry.
        assert len(http.requests) == 3

    def test_401_during__refresh_client_oauth_token_raises_error(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        """If we fail to refresh the OAuth bearer token, an exception is
        raised.
        """
        api = overdrive_api_fixture.api
        http = overdrive_api_fixture.mock_http

        http.queue_response(401)
        with pytest.raises(
            BadResponseException,
            match="Got status code 401 .* can only continue on: 200.",
        ):
            api._refresh_client_oauth_token()

    def test_patron_request_401_refreshes_bearer_token(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        http = overdrive_api_fixture.mock_http
        api = overdrive_api_fixture.api
        patron = db.patron()

        # If we get a 401, we refresh the bearer token and try again.
        overdrive_api_fixture.queue_access_token_response("bearer token")
        http.queue_response(401)
        overdrive_api_fixture.queue_access_token_response("new bearer token")
        http.queue_response(200, content="at last, the content")
        assert (
            api.patron_request(patron, "pin", db.fresh_url()).text
            == "at last, the content"
        )

        # The bearer token has been updated.
        assert (
            api._get_patron_oauth_credential(patron, "pin").credential
            == "new bearer token"
        )

        # If we get two 401 in a row, we raise an error
        http.queue_response(401)
        overdrive_api_fixture.queue_access_token_response("new bearer token")
        http.queue_response(401)
        with pytest.raises(IntegrationException, match="patron OAuth Bearer Token"):
            api.patron_request(patron, "pin", db.fresh_url())

    def test_patron_request_raises_validation_error(
        self,
        overdrive_api_fixture: OverdriveAPIFixture,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        """
        If patron request can't validate the response, it raises a OverdriveValidationError.
        """
        http = overdrive_api_fixture.mock_http

        overdrive_api_fixture.queue_access_token_response()
        http.queue_response(200, content="not json")

        with pytest.raises(OverdriveValidationError) as excinfo:
            overdrive_api_fixture.api.patron_request(
                db.patron(), "pin", db.fresh_url(), response_type=Checkout
            )

        assert (
            excinfo.value.problem_detail.detail
            == "The server made a request to url, and got an unexpected or invalid response."
        )
        assert excinfo.value.problem_detail.debug_message is not None
        assert "Invalid JSON" in excinfo.value.problem_detail.debug_message
        assert "1 validation error for Checkout" in caplog.text

    def test_advantage_differences(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
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
        overdrive_main = overdrive_api_fixture.create_mock_api(main)

        # Note the "library" endpoint.
        assert (
            overdrive_main._library_endpoint
            == "https://api.overdrive.com/v1/libraries/1"
        )

        # The advantage_library_id of a non-Advantage Overdrive account
        # is always -1.
        assert overdrive_main.library_id() == "1"
        assert overdrive_main.advantage_library_id == -1

        # Here's an Overdrive Advantage collection associated with the
        # main Overdrive collection.
        child = db.collection(
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(external_account_id="2"),
        )
        child.parent = main
        overdrive_child = overdrive_api_fixture.create_mock_api(child)

        # In URL-space, the "library" endpoint for the Advantage
        # collection is beneath the the parent collection's "library"
        # endpoint.
        assert (
            overdrive_child._library_endpoint
            == "https://api.overdrive.com/v1/libraries/1/advantageAccounts/2"
        )

        # The advantage_library_id of an Advantage collection is the
        # numeric value of its external_account_id.
        assert overdrive_child.library_id() == "2"
        assert overdrive_child.advantage_library_id == 2

    def test__get_book_list_page(self, overdrive_api_fixture: OverdriveAPIFixture):
        # Test the internal method that retrieves a list of books and
        # preprocesses it.

        http = overdrive_api_fixture.mock_http

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
            http.queue_response(200, content=content)
            result = overdrive_api_fixture.api._get_book_list_page(
                "http://first-page/", "some-rel", extractor  # type: ignore[arg-type]
            )

            # A single request was made to the requested page.
            assert len(http.requests) == 1
            assert http.requests.pop() == "http://first-page/"

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

    def test__run_self_tests(
        self,
        overdrive_api_fixture: OverdriveAPIFixture,
        db: DatabaseTransactionFixture,
    ):
        # Verify that OverdriveAPI._run_self_tests() calls the right
        # methods.

        # Mock every method used by OverdriveAPI._run_self_tests.
        api = overdrive_api_fixture.api

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
        db: DatabaseTransactionFixture,
        library_fixture: LibraryFixture,
    ):
        """Test the ability of the Overdrive API to detect an email address
        previously given by the patron to Overdrive for the purpose of
        notifications.
        """
        http = overdrive_api_fixture.mock_http
        overdrive_api_fixture.queue_access_token_response("patron_token")
        ignore, patron_with_email = overdrive_api_fixture.sample_json(
            "patron_info.json"
        )
        http.queue_response(200, content=patron_with_email)
        settings = library_fixture.mock_settings()
        library = library_fixture.library(settings=settings)
        patron = db.patron(library=library)

        # The site default for notification emails will never be used.
        settings.default_notification_email_address = "notifications@example.com"

        # If the patron has used a particular email address to put
        # books on hold, use that email address, not the site default.
        assert (
            overdrive_api_fixture.api.default_notification_email_address(patron, "pin")
            == "foo@bar.com"
        )

        # If the patron's email address according to Overdrive _is_
        # the site default, it is ignored. This can only happen if
        # this patron placed a hold using an older version of the
        # circulation manager.
        patron_with_email["lastHoldEmail"] = settings.default_notification_email_address
        http.queue_response(200, content=patron_with_email)
        assert (
            overdrive_api_fixture.api.default_notification_email_address(patron, "pin")
            is None
        )

        # If the patron has never before put an Overdrive book on
        # hold, their JSON object has no `lastHoldEmail` key. In this
        # case we return None -- again, ignoring the site default.
        patron_with_no_email = dict(patron_with_email)
        del patron_with_no_email["lastHoldEmail"]
        http.queue_response(200, content=patron_with_no_email)
        assert (
            overdrive_api_fixture.api.default_notification_email_address(patron, "pin")
            is None
        )

        # If there's an error getting the information from Overdrive,
        # we return None.
        http.queue_response(404)
        assert (
            overdrive_api_fixture.api.default_notification_email_address(patron, "pin")
            is None
        )

    def test_scope_string(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        # scope_string() puts the website ID of the Overdrive
        # integration and the ILS name associated with the library
        # into the form expected by Overdrive.
        api = overdrive_api_fixture.api
        expect = "websiteid:{} authorizationname:{}".format(
            api.website_id(),
            api.ils_name(db.default_library()),
        )
        assert api.scope_string(db.default_library()) == expect

    def test_checkout(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        # Verify the process of checking out a book.
        patron = db.patron()
        pin = "1234"
        api = overdrive_api_fixture.api
        http = overdrive_api_fixture.mock_http

        # The licensepool for this checkout has several delivery mechanisms and we incorrectly are identifying that
        # it is available as an epub with no DRM.
        pool = db.licensepool(edition=None, collection=overdrive_api_fixture.collection)
        pool.set_delivery_mechanism(
            Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
            available=False,
            update_available=True,
            rights_uri=None,
        )
        pool.set_delivery_mechanism(
            Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM,
            available=True,
            update_available=True,
            rights_uri=None,
        )
        identifier = pool.identifier

        # First, test the successful path.
        overdrive_api_fixture.queue_access_token_response()
        http.queue_response(
            201,
            content=overdrive_api_fixture.data.sample_data(
                "checkout_response_no_format_locked_in.json"
            ),
        )
        loan = api.checkout(patron, pin, pool, None)

        # Verify that a good-looking patron request went out.
        endpoint = http.requests.pop()
        assert endpoint.endswith("/me/checkouts")
        args = http.requests_args.pop()
        headers = args["headers"]
        assert headers.get("Content-Type") == "application/json"
        data = args["data"]
        assert isinstance(data, str)
        assert json.loads(data) == {
            "fields": [{"name": "reserveId", "value": pool.identifier.identifier}]
        }

        # During the checkout process, we get information about the books formats and update them correctly
        assert {
            (
                dm.delivery_mechanism.content_type,
                dm.delivery_mechanism.drm_scheme,
                dm.available,
            )
            for dm in pool.delivery_mechanisms
        } == {
            (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM, True),
            (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM, False),
        }

        # The return value is a LoanInfo object with all relevant info.
        assert isinstance(loan, LoanInfo)
        assert loan.collection_id == pool.collection.id
        assert loan.identifier_type == identifier.type
        assert loan.identifier == identifier.identifier
        assert loan.start_date is None
        assert loan.end_date == datetime(2014, 11, 26, 14, 22, 00, tzinfo=timezone.utc)

        # Now let's test error conditions.

        # Most of the time, an error simply results in an exception.
        http.queue_response(400, content="error")
        with pytest.raises(
            CannotLoan, match="Got status code 400 from external server"
        ):
            api.checkout(patron, pin, pool, None)

        # If Overdrive gives us a message, we pass it on in the problem detail
        http.queue_response(
            400,
            content=overdrive_api_fixture.error_message("Error", "Some error details"),
        )
        with pytest.raises(CannotLoan, match="Some error details"):
            api.checkout(patron, pin, pool, None)

        # If the error is "TitleAlreadyCheckedOut" or "NoCopiesAvailable", we know that somehow we already have this title
        # checked out. In that case we make a second request to get the loan information, and return a LoanInfo object.
        for err in ["TitleAlreadyCheckedOut", "NoCopiesAvailable"]:
            http.queue_response(400, content=overdrive_api_fixture.error_message(err))
            http.queue_response(
                201,
                content=overdrive_api_fixture.data.sample_data(
                    "checkout_response_no_format_locked_in.json"
                ),
            )
            loan = api.checkout(patron, pin, pool, None)

            # During the checkout process, we get information about the books formats and update them correctly
            assert {
                (
                    dm.delivery_mechanism.content_type,
                    dm.delivery_mechanism.drm_scheme,
                    dm.available,
                )
                for dm in pool.delivery_mechanisms
            } == {
                (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM, True),
                (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM, False),
            }

            # The return value is a LoanInfo object with all relevant info.
            assert isinstance(loan, LoanInfo)
            assert loan.collection_id == pool.collection.id
            assert loan.identifier_type == identifier.type
            assert loan.identifier == identifier.identifier
            assert loan.start_date is None
            assert loan.end_date == datetime(
                2014, 11, 26, 14, 22, 00, tzinfo=timezone.utc
            )

        # If we don't actually have an active loan, we raise the original exception.
        http.queue_response(
            400, content=overdrive_api_fixture.error_message("NoCopiesAvailable")
        )
        http.queue_response(
            400, content=overdrive_api_fixture.error_message("TitleNotCheckedOut")
        )
        with pytest.raises(NoAvailableCopies):
            api.checkout(patron, pin, pool, None)

    @pytest.mark.parametrize(
        "response_file",
        [
            "checkout_response_unsupported.json",
            "checkout_response_unsupported_kindle.json",
        ],
    )
    def test_checkout_unsupported_format(
        self,
        overdrive_api_fixture: OverdriveAPIFixture,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
        response_file: str,
    ):
        # Verify the process of checking out a book.
        patron = db.patron()
        pin = "1234"
        api = overdrive_api_fixture.api
        http = overdrive_api_fixture.mock_http
        pool = db.licensepool(edition=None, collection=overdrive_api_fixture.collection)
        pool.set_delivery_mechanism(
            Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM,
            available=True,
            rights_uri=None,
        )

        # If the book we checked out is only available in a format that the app cannot read,
        # we return the book, and raise a CannotLoan error.
        overdrive_api_fixture.queue_access_token_response()
        http.queue_response(
            201,
            content=overdrive_api_fixture.data.sample_data(response_file),
        )
        http.queue_response(200, content="")
        with pytest.raises(
            CannotLoan,
            match="format of this book is not supported",
        ):
            api.checkout(patron, pin, pool, None)

        # We made requests to checkout the book and return it. The first request is to get the patron's
        # oauth token, so we ignore it here.
        [checkout_request_method, loan_info_request_method] = http.requests_methods[1:]
        [checkout_request_url, loan_info_request_url] = http.requests[1:]

        assert checkout_request_method == "post"
        assert (
            checkout_request_url
            == "https://integration-patron.api.overdrive.com/v1/patrons/me/checkouts"
        )

        assert loan_info_request_method == "delete"
        assert (
            "patron.api.overdrive.com/v1/patrons/me/checkouts/" in loan_info_request_url
        )

        # We updated the pool's delivery mechanisms, so we know that no supported formats are available
        # in the future.
        assert {
            (
                dm.delivery_mechanism.content_type,
                dm.delivery_mechanism.drm_scheme,
                dm.available,
            )
            for dm in pool.delivery_mechanisms
        } == {
            (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM, False),
            (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM, False),
        }

        # If the book was already checked out, we still raise an error, but we don't return the book.
        http.reset_mock()
        http.queue_response(
            400, content=overdrive_api_fixture.error_message("TitleAlreadyCheckedOut")
        )
        http.queue_response(
            200,
            content=overdrive_api_fixture.data.sample_data(response_file),
        )
        with pytest.raises(
            CannotLoan,
            match="format of this book is not supported",
        ):
            api.checkout(patron, pin, pool, None)

        # We made requests to checkout the book and get the loan information, but not to return it.
        [checkout_request_method, loan_info_request_method] = http.requests_methods
        [checkout_request_url, loan_info_request_url] = http.requests

        assert checkout_request_method == "post"
        assert (
            checkout_request_url
            == "https://integration-patron.api.overdrive.com/v1/patrons/me/checkouts"
        )

        assert loan_info_request_method == "get"
        assert (
            "patron.api.overdrive.com/v1/patrons/me/checkouts/" in loan_info_request_url
        )

        # If the patron has a hold on the book, but after checkout it is in an unsupported format,
        # we don't return the book, but we delete the hold since its been converted to a loan and
        # we return an error.
        pool.on_hold_to(patron, position=0)
        assert db.session.query(Hold).count() == 1
        http.reset_mock()
        http.queue_response(
            201,
            content=overdrive_api_fixture.data.sample_data(response_file),
        )
        with pytest.raises(
            CannotLoan,
            match="format of this book is not supported",
        ):
            api.checkout(patron, pin, pool, None)
        mock_collect = services_fixture.analytics.collect_event
        mock_collect.assert_called_once_with(
            db.default_library(),
            pool,
            CirculationEvent.CM_HOLD_CONVERTED_TO_LOAN,
            patron=patron,
        )

        assert db.session.query(Hold).count() == 0

    def test_place_hold(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        # Verify that an appropriate request is made to HOLDS_ENDPOINT
        # to create a hold.
        #
        # The request will include different form fields depending on
        # whether default_notification_email_address returns something.
        patron = db.patron()
        pin = "1234"
        api = overdrive_api_fixture.api
        http = overdrive_api_fixture.mock_http
        pool = db.licensepool(edition=None, collection=overdrive_api_fixture.collection)

        # First, test the case where no notification email address is
        # provided and there is no default.
        mock_default_notification_email_address = create_autospec(
            api.default_notification_email_address, return_value=None
        )
        api.default_notification_email_address = mock_default_notification_email_address

        overdrive_api_fixture.queue_access_token_response()
        http.queue_response(
            201,
            content=overdrive_api_fixture.data.sample_data("successful_hold.json"),
        )

        response = api.place_hold(patron, pin, pool, None)
        assert isinstance(response, HoldInfo)
        assert response.identifier_type == pool.identifier.type
        assert response.identifier == pool.identifier.identifier

        # The patron and PIN were passed into
        # default_notification_email_address.
        mock_default_notification_email_address.assert_called_once_with(patron, pin)

        # The return value was None, and so 'ignoreHoldEmail' was
        # sent in the request data.

        [hold_request_method] = http.requests_methods[1:]
        [hold_request_url] = http.requests[1:]
        [hold_request_args] = http.requests_args[1:]

        assert hold_request_method == "post"
        assert hold_request_url.endswith("/me/holds")
        assert hold_request_args["data"] == json.dumps(
            {
                "fields": [
                    {"name": "reserveId", "value": pool.identifier.identifier},
                    {"name": "ignoreHoldEmail", "value": True},
                ]
            }
        )

        # Now we need to test two more cases.
        #
        # First, the patron has a holds notification address
        # registered with Overdrive.
        http.reset_mock()
        email = "holds@patr.on"
        mock_default_notification_email_address.return_value = email

        http.queue_response(
            201,
            content=overdrive_api_fixture.data.sample_data("successful_hold.json"),
        )

        api.place_hold(patron, pin, pool, None)

        [hold_request_method] = http.requests_methods
        [hold_request_url] = http.requests
        [hold_request_args] = http.requests_args

        assert hold_request_method == "post"
        assert hold_request_url.endswith("/me/holds")
        assert hold_request_args["data"] == json.dumps(
            {
                "fields": [
                    {"name": "reserveId", "value": pool.identifier.identifier},
                    {"name": "emailAddress", "value": email},
                ]
            }
        )

        # Finally, test that when a specific address is passed in, it
        # takes precedence over the patron's holds notification address.
        http.reset_mock()
        mock_default_notification_email_address.return_value = email

        http.queue_response(
            201,
            content=overdrive_api_fixture.data.sample_data("successful_hold.json"),
        )

        api.place_hold(patron, pin, pool, "another@addre.ss")

        [hold_request_method] = http.requests_methods
        [hold_request_url] = http.requests
        [hold_request_args] = http.requests_args

        assert hold_request_method == "post"
        assert hold_request_url.endswith("/me/holds")
        assert hold_request_args["data"] == json.dumps(
            {
                "fields": [
                    {"name": "reserveId", "value": pool.identifier.identifier},
                    {"name": "emailAddress", "value": "another@addre.ss"},
                ]
            }
        )

    def test_checkin(
        self,
        overdrive_api_fixture: OverdriveAPIFixture,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        http = overdrive_api_fixture.mock_http
        api = overdrive_api_fixture.api

        overdrive_api_fixture.queue_access_token_response()
        checkout_response = overdrive_api_fixture.data.sample_data(
            "checkout_response_no_format_locked_in.json"
        )
        checkout_response_dict = json.loads(checkout_response)
        http.queue_response(200, content=checkout_response)
        http.queue_response(200, content="")

        # In most circumstances we do not bother calling
        # perform_early_return; we just call patron_request.
        pool = db.licensepool(None)
        patron = db.patron()
        pin = None

        # Test the happy path where a return succeeds
        api.checkin(patron, pin, pool)

        # We made three requests, one to get the patron token, one for the loan and
        # one to return the loan
        assert len(http.requests) == 3
        token_request_url, loan_request_url, return_request_url = http.requests
        assert token_request_url.endswith("/patrontoken")
        assert "patrons/me/checkouts" in loan_request_url
        assert (
            return_request_url
            == checkout_response_dict["actions"]["earlyReturn"]["href"]
        )

        # Correct request methods
        assert http.requests_methods == ["POST", "get", "delete"]

        # Test error cases

        # Error coming back from Overdrive API fails the return
        http.queue_response(200, content=checkout_response)
        http.queue_response(
            400,
            content=overdrive_api_fixture.error_message(
                "BadRequest", "Something went wrong"
            ),
        )
        with pytest.raises(CannotReturn, match="Something went wrong"):
            api.checkin(patron, pin, pool)

        # Unless the error is that the loan does not exist
        http.queue_response(
            400, content=overdrive_api_fixture.error_message("TitleNotCheckedOut")
        )
        with pytest.raises(NotCheckedOut):
            api.checkin(patron, pin, pool)

        # No earlyReturn link
        # Fetch the loan, but can't find a link, or can't parse it. We just log an error
        # and pretend that the return worked.
        http.reset_mock()
        del checkout_response_dict["actions"]["earlyReturn"]
        http.queue_response(200, content=json.dumps(checkout_response_dict))
        api.checkin(patron, pin, pool)

        # We got the loan, but we were unable to try to return it
        assert len(http.requests) == 1
        assert "Something went wrong calling the earlyReturn action." in caplog.text

    def test_place_hold_raises_exception_if_patron_over_hold_limit(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        api = overdrive_api_fixture.api
        http = overdrive_api_fixture.mock_http
        over_hold_limit = overdrive_api_fixture.error_message(
            "PatronExceededHoldLimit",
            "Patron cannot place any more holds, already has maximum holds placed.",
        )

        edition, pool = db.edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
        )
        overdrive_api_fixture.queue_access_token_response("patron_token")
        http.queue_response(400, content=over_hold_limit)
        with pytest.raises(PatronHoldLimitReached):
            api.place_hold(
                db.patron(), "pin", pool, notification_email_address="foo@bar.com"
            )

    def test_place_hold_looks_up_notification_address(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        http = overdrive_api_fixture.mock_http
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

        overdrive_api_fixture.queue_access_token_response("patron_token")
        http.queue_response(200, content=patron_with_email)
        http.queue_response(200, content=successful_hold)
        hold = overdrive_api_fixture.api.place_hold(
            db.patron(), "pin", pool, notification_email_address=None
        )

        # The book was placed on hold.
        assert hold.hold_position == 1
        assert hold.identifier == pool.identifier.identifier

        # And when we placed it on hold, we passed in foo@bar.com
        # as the email address -- not notifications@example.com.
        args = http.requests_args.pop()
        data_json = args["data"]
        assert isinstance(data_json, str)
        data = json.loads(data_json)
        assert {"name": "emailAddress", "value": "foo@bar.com"} in data.get("fields")

    def test_fulfill_raises_exception_for_outdated_format(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        http = overdrive_api_fixture.mock_http
        api = overdrive_api_fixture.api
        edition, pool = db.edition(
            identifier_type=Identifier.OVERDRIVE_ID,
            data_source_name=DataSource.OVERDRIVE,
            with_license_pool=True,
        )

        # This pool has a format that's no longer available from overdrive.
        delivery_mechanism = pool.set_delivery_mechanism(
            Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM,
            RightsStatus.IN_COPYRIGHT,
            None,
        )

        loan = overdrive_api_fixture.data.sample_data("single_loan.json")

        no_drm_loan = overdrive_api_fixture.data.sample_data(
            "checkout_response_no_format_locked_in_no_drm.json"
        )

        lock_in_format_not_available = overdrive_api_fixture.data.sample_data(
            "lock_in_format_not_available.json"
        )

        # We will get the loan, try to lock in the format, and fail because
        # the format is not in the list of available formats.
        overdrive_api_fixture.queue_access_token_response()
        http.queue_response(200, content=loan)

        # Trying to get a fulfillment link raises an exception.
        with pytest.raises(FormatNotAvailable):
            api.fulfill(
                db.patron(),
                "pin",
                pool,
                delivery_mechanism,
            )

        # Try the case where Overdrive says the format is available, but when we request it
        # it no longer is.
        overdrive_api_fixture.queue_access_token_response()
        http.queue_response(200, content=no_drm_loan)
        http.queue_response(400, content=lock_in_format_not_available)

        # Trying to get a fulfillment link raises an exception.
        with pytest.raises(FormatNotAvailable):
            api.fulfill(
                db.patron(),
                "pin",
                pool,
                delivery_mechanism,
            )

    def test_streaming_fulfillment(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        http = overdrive_api_fixture.mock_http
        patron = db.patron()
        work = db.work(with_license_pool=True)
        patron.authorization_identifier = "barcode"
        api = overdrive_api_fixture.api
        pool = work.active_license_pool()
        delivery_mechanism = pool.set_delivery_mechanism(
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_DRM,
            None,
        )

        # Queue up the responses
        overdrive_api_fixture.queue_access_token_response()
        http.queue_response(
            201,
            content=overdrive_api_fixture.data.sample_data(
                "checkout_response_no_format_locked_in_no_drm.json"
            ),
        )
        http.queue_response(
            200,
            content=overdrive_api_fixture.data.sample_data(
                "streaming_fulfill_link_response.json"
            ),
        )

        fulfill = api.fulfill(
            patron, "pin", work.active_license_pool(), delivery_mechanism
        )
        assert isinstance(fulfill, FetchFulfillment)
        assert fulfill.content_type == "text/html" + DeliveryMechanism.STREAMING_PROFILE
        assert (
            fulfill.content_link
            == "https://fulfill.contentreserve.com/PerfectLife9780345530967.epub-sample.overdrive.com?RetailerID=nypl&Expires=1469825647&Token=dd0e19b4-eb70-439d-8c50-a65201060f4c&Signature=asl67/G154KeeUsL1mHPwEbZfgc="
        )

    def test_manifest_fulfillment(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        http = overdrive_api_fixture.mock_http
        patron = db.patron()
        work = db.work(with_license_pool=True)
        patron.authorization_identifier = "barcode"
        api = overdrive_api_fixture.api
        pool = work.active_license_pool()
        delivery_mechanism = pool.set_delivery_mechanism(
            MediaTypes.OVERDRIVE_AUDIOBOOK_MANIFEST_MEDIA_TYPE,
            DeliveryMechanism.LIBBY_DRM,
            None,
        )

        # Queue up the responses
        overdrive_api_fixture.queue_access_token_response()
        http.queue_response(
            200,
            content=overdrive_api_fixture.data.sample_data(
                "checkout_response_audiobook.json"
            ),
        )

        fulfill = api.fulfill(
            patron, "pin", work.active_license_pool(), delivery_mechanism
        )
        assert isinstance(fulfill, OverdriveManifestFulfillment)
        assert (
            fulfill.content_link
            == "https://patron.api.overdrive.com/v1/patrons/me/checkouts/e14166ba-5022-4e63-969a-6f0b6f86380b/formats/audiobook-overdrive/downloadlink?contentfile=true"
        )
        assert fulfill.access_token == "token"
        assert fulfill.scope_string == "websiteid:d authorizationname:e"

    def test_update_formats(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        # Create a LicensePool with an inaccurate delivery mechanism
        # and the wrong medium.
        http = overdrive_api_fixture.mock_http
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

        http.queue_response(200, content=bibliographic)

        overdrive_api_fixture.api.update_formats(pool)

        # The delivery mechanisms have been updated.
        assert len(pool.delivery_mechanisms) == 4
        assert {
            (
                lpdm.delivery_mechanism.content_type,
                lpdm.delivery_mechanism.drm_scheme,
                lpdm.available,
            )
            for lpdm in pool.delivery_mechanisms
        } == {
            (
                MediaTypes.EPUB_MEDIA_TYPE,
                DeliveryMechanism.ADOBE_DRM,
                True,
            ),
            (
                DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                DeliveryMechanism.STREAMING_DRM,
                True,
            ),
            (
                MediaTypes.EPUB_MEDIA_TYPE,
                DeliveryMechanism.NO_DRM,
                False,
            ),
            (
                MediaTypes.PDF_MEDIA_TYPE,
                DeliveryMechanism.NO_DRM,
                False,
            ),
        }

        # The Edition's medium has been corrected.
        assert edition.medium == Edition.BOOK_MEDIUM

        # We don't know these formats for sure though. It turns out later on that different formats are
        # what is available for this book.
        pool.set_delivery_mechanism(
            MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
            None,
            available=False,
            update_available=True,
        )

        pool.set_delivery_mechanism(
            MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM,
            None,
            available=True,
            update_available=True,
        )

        # Updating the formats again, does not overwrite the changes we made to the pool
        http.queue_response(200, content=bibliographic)

        overdrive_api_fixture.api.update_formats(pool)

        assert len(pool.delivery_mechanisms) == 4
        assert {
            (
                lpdm.delivery_mechanism.content_type,
                lpdm.delivery_mechanism.drm_scheme,
                lpdm.available,
            )
            for lpdm in pool.delivery_mechanisms
        } == {
            (
                MediaTypes.EPUB_MEDIA_TYPE,
                DeliveryMechanism.ADOBE_DRM,
                False,
            ),
            (
                DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                DeliveryMechanism.STREAMING_DRM,
                True,
            ),
            (
                MediaTypes.EPUB_MEDIA_TYPE,
                DeliveryMechanism.NO_DRM,
                True,
            ),
            (
                MediaTypes.PDF_MEDIA_TYPE,
                DeliveryMechanism.NO_DRM,
                False,
            ),
        }

    def test_update_availability(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        # Test the Overdrive implementation of the update_availability
        # method defined by the CirculationAPI interface.
        http = overdrive_api_fixture.mock_http

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

        http.queue_response(200, content=availability)
        http.queue_response(200, content=bibliographic)

        overdrive_api_fixture.api.update_availability(pool)

        # The availability information has been updated, as has the
        # date the availability information was last checked.
        assert pool.licenses_owned == 5
        assert pool.licenses_available == 1
        assert pool.patrons_in_hold_queue == 0
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

    def test_circulation_lookup(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        """Test the method that actually looks up Overdrive circulation
        information.
        """

        http = overdrive_api_fixture.mock_http
        api = overdrive_api_fixture.api

        http.queue_response(200, content="foo")

        # If passed an identifier, we'll use the endpoint() method to
        # construct a v2 availability URL and make a request to
        # it.

        book, (
            status_code,
            headers,
            content,
        ) = api.circulation_lookup("an-identifier")
        assert book == dict(id="an-identifier")
        assert status_code == 200
        assert content == b"foo"

        request_url = http.requests.pop()
        expect_url = api.endpoint(
            api.AVAILABILITY_ENDPOINT,
            collection_token=api.collection_token,
            product_id="an-identifier",
        )
        assert request_url == expect_url
        assert "/v2/collections" in request_url

        # If passed the result of an API call that includes an
        # availability link, we'll clean up the URL in the link and
        # use it to get our availability data.
        http.queue_response(200, content="foo")
        v1 = "https://qa.api.overdrive.com/v1/collections/abcde/products/12345/availability"
        v2 = "https://qa.api.overdrive.com/v2/collections/abcde/products/12345/availability"
        previous_result = dict(availability_link=v1)
        book, (
            status_code,
            headers,
            content,
        ) = api.circulation_lookup(previous_result)
        assert book == previous_result
        assert status_code == 200
        assert content == b"foo"
        request_url = http.requests.pop()

        # The v1 URL was converted to a v2 url.
        assert request_url == v2

    def test_update_licensepool_error(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):

        # Create an identifier.
        identifier = db.identifier(identifier_type=Identifier.OVERDRIVE_ID)
        ignore, availability = overdrive_api_fixture.sample_json(
            "overdrive_availability_information.json"
        )
        http = overdrive_api_fixture.mock_http
        http.queue_response(500, content="An error occured.")
        book = dict(id=identifier.identifier, availability_link=db.fresh_url())
        pool, was_new, changed = overdrive_api_fixture.api.update_licensepool(book)
        assert pool is None

    def test_update_licensepool_not_found(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        # If the Overdrive API says a book is not found in the
        # collection, that's treated as useful information, not an error.
        # Create an identifier.
        http = overdrive_api_fixture.mock_http
        identifier = db.identifier(identifier_type=Identifier.OVERDRIVE_ID)
        ignore, not_found = overdrive_api_fixture.sample_json(
            "overdrive_availability_not_found.json"
        )

        # Queue the 'not found' response twice -- once for the circulation
        # lookup and once for the metadata lookup.
        http.queue_response(404, content=not_found)
        http.queue_response(404, content=not_found)

        book = dict(id=identifier.identifier, availability_link=db.fresh_url())
        pool, was_new, changed = overdrive_api_fixture.api.update_licensepool(book)
        assert pool.licenses_owned == 0
        assert pool.licenses_available == 0
        assert pool.patrons_in_hold_queue == 0

    def test_update_licensepool_provides_bibliographic_coverage(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        http = overdrive_api_fixture.mock_http

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

        http.queue_response(200, content=availability)
        http.queue_response(200, content=bibliographic)

        # Now we're ready. When we call update_licensepool, the
        # OverdriveAPI will retrieve the availability information,
        # then the bibliographic information. It will then trigger the
        # OverdriveBibliographicCoverageProvider, which will
        # create an Edition and a presentation-ready Work.
        pool, was_new, changed = overdrive_api_fixture.api.update_licensepool(
            identifier.identifier
        )
        assert was_new is True
        assert pool.licenses_owned == availability["copiesOwned"]

        edition = pool.presentation_edition
        assert edition.title == "Ancillary Justice"

        assert pool.work.presentation_ready is True
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
        assert len(coverage) == 1

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
        http.queue_response(200, content=availability)
        http.queue_response(200, content=bibliographic)
        pool, was_new, changed = overdrive_api_fixture.api.update_licensepool(
            identifier.identifier
        )
        assert was_new is False
        assert pool.work.presentation_ready is True

    def test_update_new_licensepool(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):

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
        assert was_new is True
        assert changed is True

        db.session.commit()
        assert pool is not None
        assert pool.licenses_owned == raw["copiesOwned"]
        assert pool.licenses_available == raw["copiesAvailable"]
        assert pool.licenses_reserved == 0
        assert pool.patrons_in_hold_queue == raw["numberOfHolds"]

    def test_update_existing_licensepool(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):

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
        assert pool.licenses_owned == 1
        assert pool.licenses_available == 1
        assert pool.licenses_reserved == 0
        assert pool.patrons_in_hold_queue == 0

        (
            p2,
            was_new,
            changed,
        ) = overdrive_api_fixture.api.update_licensepool_with_book_info(
            raw, pool, False
        )
        assert was_new is False
        assert changed is True
        assert pool == p2
        # The title didn't change to that title given in the availability
        # information, because we already set a title for that work.
        assert wr.title == "The real title."
        assert pool.licenses_owned == raw["copiesOwned"]
        assert pool.licenses_available == raw["copiesAvailable"]
        assert pool.licenses_reserved == 0
        assert pool.patrons_in_hold_queue == raw["numberOfHolds"]

    def test_update_new_licensepool_when_same_book_has_pool_in_different_collection(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):

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
        assert new_pool.presentation_edition is None

        (
            new_pool,
            was_new,
            changed,
        ) = overdrive_api_fixture.api.update_licensepool_with_book_info(
            raw, new_pool, was_new
        )
        assert new_pool is not None
        assert was_new is True
        assert changed is True
        assert new_pool.presentation_edition == old_edition
        assert new_pool.work == old_pool.work

    def test_update_licensepool_with_holds(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
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
        assert pool.patrons_in_hold_queue == 10
        assert changed is True

    def test__refresh_patron_oauth_token(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        """Verify that patron information is included in the request
        when refreshing a patron access token.
        """
        http = overdrive_api_fixture.mock_http
        patron = db.patron()
        patron.authorization_identifier = "barcode"
        credential = db.credential(patron=patron)

        overdrive_api_fixture.queue_access_token_response()
        overdrive_api_fixture.queue_access_token_response()

        # Try to refresh the patron access token with a PIN, and
        # then without a PIN.
        overdrive_api_fixture.api._refresh_patron_oauth_token(
            credential, patron, "a pin"
        )

        overdrive_api_fixture.api._refresh_patron_oauth_token(credential, patron, None)

        # Verify that the requests that were made correspond to what
        # Overdrive is expecting.

        expect_scope = "websiteid:{} authorizationname:{}".format(
            overdrive_api_fixture.api.website_id(),
            overdrive_api_fixture.api.ils_name(patron.library),
        )

        # Both requests went to the same patrontoken url
        assert http.requests == [
            "https://oauth-patron.overdrive.com/patrontoken",
            "https://oauth-patron.overdrive.com/patrontoken",
        ]

        with_pin, without_pin = http.requests_args

        payload = with_pin["data"]
        assert isinstance(payload, dict)
        assert payload["username"] == "barcode"
        assert payload["scope"] == expect_scope
        assert payload["password"] == "a pin"
        assert "password_required" not in payload

        payload = without_pin["data"]
        assert isinstance(payload, dict)
        assert payload["username"] == "barcode"
        assert payload["scope"] == expect_scope
        assert payload["password_required"] == "false"
        assert payload["password"] == "[ignore]"

    def test__refresh_patron_oauth_token_failure(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ) -> None:
        http = overdrive_api_fixture.mock_http
        patron = db.patron()
        patron.authorization_identifier = "barcode"
        credential = db.credential(patron=patron)

        # Test with a real 400 response we've seen from overdrive
        data, raw = overdrive_api_fixture.sample_json("patron_token_failed.json")
        http.queue_response(400, content=raw)
        with pytest.raises(
            PatronAuthorizationFailedException, match="Invalid Library Card"
        ):
            overdrive_api_fixture.api._refresh_patron_oauth_token(
                credential, patron, "a pin"
            )

        # Test with a fictional 403 response that doesn't contain valid json - we've never
        # seen this come back from overdrive, this test is just to make sure we can handle
        # unexpected responses back from OD API.
        http.queue_response(403, content="garbage { json")
        with pytest.raises(
            PatronAuthorizationFailedException,
            match="Failed to authenticate with Overdrive",
        ):
            overdrive_api_fixture.api._refresh_patron_oauth_token(
                credential, patron, "a pin"
            )

    def test_no_drm_fulfillment(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        http = overdrive_api_fixture.mock_http
        patron = db.patron()
        work = db.work(with_license_pool=True)
        patron.authorization_identifier = "barcode"
        api = overdrive_api_fixture.api
        pool = work.active_license_pool()
        delivery_mechanism = pool.set_delivery_mechanism(
            Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM,
            None,
        )

        # Queue up the responses
        overdrive_api_fixture.queue_access_token_response()
        http.queue_response(
            201,
            content=overdrive_api_fixture.data.sample_data(
                "checkout_response_no_format_locked_in_no_drm.json"
            ),
        )
        http.queue_response(
            200,
            content=overdrive_api_fixture.data.sample_data(
                "format_response_no_drm.json"
            ),
        )
        http.queue_response(
            200,
            content=overdrive_api_fixture.data.sample_data("download_link_no_drm.json"),
        )

        fulfill = api.fulfill(
            patron, "pin", work.active_license_pool(), delivery_mechanism
        )
        assert isinstance(fulfill, RedirectFulfillment)
        assert fulfill.content_type == "application/epub+zip"
        assert (
            fulfill.content_link
            == "https://fulfill.contentreserve.com/LittleWomen_432890.epub?RetailerID=odapilib&Expires=1742921390&Token=6ae382f6-01b0-4ee6-b5ee-b2ab00c2f713&Signature=C2PB%2bLPnNPjrQ%2bQQ2kquMdjYZzY%3d"
        )

    def test_drm_fulfillment(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        http = overdrive_api_fixture.mock_http
        patron = db.patron()
        work = db.work(with_license_pool=True)
        patron.authorization_identifier = "barcode"
        api = overdrive_api_fixture.api
        pool = work.active_license_pool()
        delivery_mechanism = pool.set_delivery_mechanism(
            Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
            None,
        )

        # Queue up the responses
        overdrive_api_fixture.queue_access_token_response()
        http.queue_response(
            200,
            content=overdrive_api_fixture.data.sample_data(
                "checkout_response_no_format_locked_in.json"
            ),
        )
        format_locked_in = Checkout.model_validate_json(
            overdrive_api_fixture.data.sample_data(
                "checkout_response_locked_in_format.json"
            )
        )
        http.queue_response(
            200,
            content=format_locked_in.get_format(
                "ebook-epub-adobe", raising=True
            ).model_dump_json(),
        )
        lock_in_response = Format(
            format_type="ebook-epub-adobe",
            links={
                "contentlink": Link(
                    href="http://example.com/acsm",
                    type="application/vnd.adobe.adept+xml",
                )
            },
        )
        http.queue_response(200, content=lock_in_response.model_dump_json())

        fulfill = api.fulfill(
            patron, "pin", work.active_license_pool(), delivery_mechanism
        )
        assert isinstance(fulfill, FetchFulfillment)
        assert fulfill.content_type == "application/vnd.adobe.adept+xml"
        assert fulfill.content_link == "http://example.com/acsm"
        assert len(http.requests) == 4

        # Test in the case the format was already locked in
        http.reset_mock()
        http.queue_response(
            200,
            content=overdrive_api_fixture.data.sample_data(
                "checkout_response_locked_in_format.json"
            ),
        )
        http.queue_response(200, content=lock_in_response.model_dump_json())

        fulfill = api.fulfill(
            patron, "pin", work.active_license_pool(), delivery_mechanism
        )
        assert isinstance(fulfill, FetchFulfillment)
        assert fulfill.content_type == "application/vnd.adobe.adept+xml"
        assert fulfill.content_link == "http://example.com/acsm"
        assert len(http.requests) == 2

        # Test some error cases

        # Test when the format is not available
        no_drm_delivery_mechanism = pool.set_delivery_mechanism(
            Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM,
            None,
        )
        http.queue_response(
            200,
            content=overdrive_api_fixture.data.sample_data(
                "checkout_response_no_format_locked_in.json"
            ),
        )

        with pytest.raises(
            FormatNotAvailable,
            match="book is not available in the format you requested",
        ):
            api.fulfill(
                patron, "pin", work.active_license_pool(), no_drm_delivery_mechanism
            )

        # Correct link is missing in the format document
        response = json.loads(
            overdrive_api_fixture.data.sample_data(
                "checkout_response_no_format_locked_in.json"
            )
        )
        del response["actions"]["format"]
        http.queue_response(200, content=json.dumps(response))
        with pytest.raises(CannotFulfill, match="Could not lock in format"):
            api.fulfill(patron, "pin", work.active_license_pool(), delivery_mechanism)

        # Incompatible format
        http.queue_response(
            200,
            content=overdrive_api_fixture.data.sample_data(
                "checkout_response_book_fulfilled_on_kindle.json"
            ),
        )
        with pytest.raises(FulfilledOnIncompatiblePlatform):
            api.fulfill(patron, "pin", work.active_license_pool(), delivery_mechanism)

        # No acceptable format
        http.queue_response(
            200,
            content=overdrive_api_fixture.data.sample_data(
                "checkout_response_locked_in_format.json"
            ),
        )
        with pytest.raises(NoAcceptableFormat):
            api.fulfill(
                patron, "pin", work.active_license_pool(), no_drm_delivery_mechanism
            )

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
        self, db: DatabaseTransactionFixture, overdrive_api_fixture: OverdriveAPIFixture
    ):
        # Verify that the correct credential will be used
        # when a library has more than one OverDrive collection.

        def _optional_value(obj, key):
            return obj.get(key, "none")

        def _make_token(scope, username, password, grant_type="password"):
            return f"{grant_type}|{scope}|{username}|{password}"

        def _do_post(self, url, payload, headers, **kwargs):
            token = _make_token(
                _optional_value(payload, "scope"),
                _optional_value(payload, "username"),
                _optional_value(payload, "password"),
                grant_type=_optional_value(payload, "grant_type"),
            )
            return MockRequestsResponse(
                200, content=json.dumps({"access_token": token, "expires_in": 3600})
            )

        library = overdrive_api_fixture.library
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
            overdrive_api_fixture.create_collection(**props)  # type: ignore[arg-type]
            for props in library_collection_properties
        ]

        with patch.object(OverdriveAPI, "_do_post", _do_post):
            od_apis = {
                collection.name: OverdriveAPI(db.session, collection)
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


class TestSyncBookshelf:
    def test_sync_patron_activity_creates_local_loans(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        loans_data, json_loans = overdrive_api_fixture.sample_json(
            "shelf_with_some_checked_out_books.json"
        )
        holds_data, json_holds = overdrive_api_fixture.sample_json("no_holds.json")

        overdrive_api_fixture.queue_access_token_response()
        overdrive_api_fixture.mock_http.queue_response(200, content=loans_data)
        overdrive_api_fixture.mock_http.queue_response(200, content=holds_data)

        # Before the sync, we don't have any LicensePools or Identifiers.
        assert db.session.query(LicensePool).count() == 0
        assert db.session.query(Identifier).count() == 0

        patron = db.patron()
        loans, holds = overdrive_api_fixture.sync_patron_activity(patron)

        # Loans were created for the four books on loan that
        # had formats we are able to fulfill. One book is excluded
        # because it is locked to ebook-pdf-adobe, which we cannot
        # fulfill.
        assert len(loans) == 4
        assert set(loans.values()) == set(patron.loans)

        # We have created previously unknown LicensePools and
        # Identifiers.
        assert db.session.query(LicensePool).count() == 4
        assert db.session.query(Identifier).count() == 4
        assert {
            str(loan.license_pool.identifier.identifier) for loan in loans.values()
        } == {
            "a4466636-34f5-495a-92ee-3a9c701f46cf",
            "a5a3d737-34d4-4d69-aad8-eba4e46019a3",
            "99409f99-45a5-4238-9e10-98d1435cde04",
            "a2ec6f3a-ebfe-4c95-9638-2cb13be8de5a",
        }

        # We created LicensePools for the books on loan. And these LicensePools
        # accurately reflect the delivery mechanisms that are available for the
        # books.
        assert {
            lp.identifier.identifier: {
                (
                    dm.delivery_mechanism.content_type,
                    dm.delivery_mechanism.drm_scheme,
                    dm.available,
                )
                for dm in lp.delivery_mechanisms
            }
            for lp in {loan.license_pool for loan in loans.values()}
        } == {
            "a4466636-34f5-495a-92ee-3a9c701f46cf": {
                (
                    DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                    DeliveryMechanism.STREAMING_DRM,
                    True,
                ),
                (
                    Representation.EPUB_MEDIA_TYPE,
                    DeliveryMechanism.ADOBE_DRM,
                    False,
                ),
                (
                    Representation.EPUB_MEDIA_TYPE,
                    DeliveryMechanism.NO_DRM,
                    True,
                ),
                (
                    Representation.PDF_MEDIA_TYPE,
                    DeliveryMechanism.NO_DRM,
                    False,
                ),
            },
            "a2ec6f3a-ebfe-4c95-9638-2cb13be8de5a": {
                (
                    DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                    DeliveryMechanism.STREAMING_DRM,
                    True,
                ),
                (
                    Representation.EPUB_MEDIA_TYPE,
                    DeliveryMechanism.ADOBE_DRM,
                    False,
                ),
                (
                    Representation.EPUB_MEDIA_TYPE,
                    DeliveryMechanism.NO_DRM,
                    True,
                ),
                (
                    Representation.PDF_MEDIA_TYPE,
                    DeliveryMechanism.NO_DRM,
                    False,
                ),
            },
            "a5a3d737-34d4-4d69-aad8-eba4e46019a3": {
                (
                    DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                    DeliveryMechanism.STREAMING_DRM,
                    True,
                ),
                (
                    Representation.EPUB_MEDIA_TYPE,
                    DeliveryMechanism.ADOBE_DRM,
                    True,
                ),
                (
                    Representation.EPUB_MEDIA_TYPE,
                    DeliveryMechanism.NO_DRM,
                    False,
                ),
                (
                    Representation.PDF_MEDIA_TYPE,
                    DeliveryMechanism.NO_DRM,
                    False,
                ),
            },
            "99409f99-45a5-4238-9e10-98d1435cde04": {
                (
                    DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                    DeliveryMechanism.STREAMING_DRM,
                    True,
                ),
                (
                    Representation.EPUB_MEDIA_TYPE,
                    DeliveryMechanism.ADOBE_DRM,
                    True,
                ),
                (
                    Representation.EPUB_MEDIA_TYPE,
                    DeliveryMechanism.NO_DRM,
                    False,
                ),
                (
                    Representation.PDF_MEDIA_TYPE,
                    DeliveryMechanism.NO_DRM,
                    False,
                ),
            },
        }

        # Three of the loans are "locked in" to a specific format, for those
        # loans, we set a delivery mechanism on the loan.
        assert {
            (
                loan.license_pool.identifier.identifier,
                loan.fulfillment.delivery_mechanism.content_type,
                loan.fulfillment.delivery_mechanism.drm_scheme,
                loan.fulfillment.available,
            )
            for loan in loans.values()
            if loan.fulfillment
        } == {
            (
                "a2ec6f3a-ebfe-4c95-9638-2cb13be8de5a",
                Representation.EPUB_MEDIA_TYPE,
                DeliveryMechanism.NO_DRM,
                True,
            ),
            (
                "a5a3d737-34d4-4d69-aad8-eba4e46019a3",
                Representation.EPUB_MEDIA_TYPE,
                DeliveryMechanism.ADOBE_DRM,
                True,
            ),
            (
                "99409f99-45a5-4238-9e10-98d1435cde04",
                Representation.EPUB_MEDIA_TYPE,
                DeliveryMechanism.ADOBE_DRM,
                True,
            ),
        }

        # There are no holds.
        assert holds == {}

        # Running the sync again leaves all four loans in place.
        overdrive_api_fixture.mock_http.queue_response(200, content=loans_data)
        overdrive_api_fixture.mock_http.queue_response(200, content=holds_data)
        loans, holds = overdrive_api_fixture.sync_patron_activity(patron)
        assert len(loans) == 4
        assert set(loans.values()) == set(patron.loans)

    def test_sync_patron_activity_updated_inaccurate_delivery_mechanisms(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        session = db.session
        data_source = DataSource.lookup(session, DataSource.OVERDRIVE, autocreate=True)
        identifiers = {
            db.identifier(Identifier.OVERDRIVE_ID, ident)
            for ident in {
                "a4466636-34f5-495a-92ee-3a9c701f46cf",
                "a5a3d737-34d4-4d69-aad8-eba4e46019a3",
                "99409f99-45a5-4238-9e10-98d1435cde04",
                "a2ec6f3a-ebfe-4c95-9638-2cb13be8de5a",
            }
        }
        patron = db.patron()

        # Generic format information we use for ebook-overdrive, when we don't know any more
        # detailed information.
        od_formats = OverdriveRepresentationExtractor.internal_formats(
            "ebook-overdrive"
        )

        # Create the format information for each identifier
        for identifier in identifiers:
            for format in od_formats:
                format.apply(session, data_source, identifier)

        # Queue up the API responses
        loans_data = overdrive_api_fixture.data.sample_data(
            "shelf_with_some_checked_out_books.json"
        )
        holds_data = overdrive_api_fixture.data.sample_data("no_holds.json")
        overdrive_api_fixture.queue_access_token_response()
        overdrive_api_fixture.mock_http.queue_response(200, content=loans_data)
        overdrive_api_fixture.mock_http.queue_response(200, content=holds_data)

        # Run the activity sync
        loans, holds = overdrive_api_fixture.sync_patron_activity(patron)

        assert {
            lp.identifier.identifier: {
                (
                    dm.delivery_mechanism.content_type,
                    dm.delivery_mechanism.drm_scheme,
                    dm.available,
                )
                for dm in lp.delivery_mechanisms
            }
            for lp in {loan.license_pool for loan in loans.values()}
        } == {
            "a4466636-34f5-495a-92ee-3a9c701f46cf": {
                (
                    DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                    DeliveryMechanism.STREAMING_DRM,
                    True,
                ),
                (
                    Representation.EPUB_MEDIA_TYPE,
                    DeliveryMechanism.ADOBE_DRM,
                    False,
                ),
                (
                    Representation.EPUB_MEDIA_TYPE,
                    DeliveryMechanism.NO_DRM,
                    True,
                ),
                (
                    Representation.PDF_MEDIA_TYPE,
                    DeliveryMechanism.NO_DRM,
                    False,
                ),
            },
            "a2ec6f3a-ebfe-4c95-9638-2cb13be8de5a": {
                (
                    DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                    DeliveryMechanism.STREAMING_DRM,
                    True,
                ),
                (
                    Representation.EPUB_MEDIA_TYPE,
                    DeliveryMechanism.ADOBE_DRM,
                    False,
                ),
                (
                    Representation.EPUB_MEDIA_TYPE,
                    DeliveryMechanism.NO_DRM,
                    True,
                ),
                (
                    Representation.PDF_MEDIA_TYPE,
                    DeliveryMechanism.NO_DRM,
                    False,
                ),
            },
            "a5a3d737-34d4-4d69-aad8-eba4e46019a3": {
                (
                    DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                    DeliveryMechanism.STREAMING_DRM,
                    True,
                ),
                (
                    Representation.EPUB_MEDIA_TYPE,
                    DeliveryMechanism.ADOBE_DRM,
                    True,
                ),
                (
                    Representation.EPUB_MEDIA_TYPE,
                    DeliveryMechanism.NO_DRM,
                    False,
                ),
                (
                    Representation.PDF_MEDIA_TYPE,
                    DeliveryMechanism.NO_DRM,
                    False,
                ),
            },
            "99409f99-45a5-4238-9e10-98d1435cde04": {
                (
                    DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                    DeliveryMechanism.STREAMING_DRM,
                    True,
                ),
                (
                    Representation.EPUB_MEDIA_TYPE,
                    DeliveryMechanism.ADOBE_DRM,
                    True,
                ),
                (
                    Representation.EPUB_MEDIA_TYPE,
                    DeliveryMechanism.NO_DRM,
                    False,
                ),
                (
                    Representation.PDF_MEDIA_TYPE,
                    DeliveryMechanism.NO_DRM,
                    False,
                ),
            },
        }

    def test_sync_patron_activity_removes_loans_not_present_on_remote(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        loans_data, json_loans = overdrive_api_fixture.sample_json(
            "shelf_with_some_checked_out_books.json"
        )
        holds_data, json_holds = overdrive_api_fixture.sample_json("no_holds.json")

        overdrive_api_fixture.queue_access_token_response()
        overdrive_api_fixture.mock_http.queue_response(200, content=loans_data)
        overdrive_api_fixture.mock_http.queue_response(200, content=holds_data)

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

        assert len(loans) == 4
        assert set(loans.values()) == set(patron.loans)
        assert overdrive_loan not in patron.loans

    def test_sync_patron_activity_ignores_loans_from_other_sources(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
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
        overdrive_api_fixture.queue_access_token_response()
        overdrive_api_fixture.mock_http.queue_response(200, content=loans_data)
        overdrive_api_fixture.mock_http.queue_response(200, content=holds_data)

        overdrive_api_fixture.sync_patron_activity(patron)
        assert len(patron.loans) == 5
        assert gutenberg_loan in patron.loans

    def test_sync_patron_activity_creates_local_holds(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        loans_data, json_loans = overdrive_api_fixture.sample_json("no_loans.json")
        holds_data, json_holds = overdrive_api_fixture.sample_json("holds.json")

        overdrive_api_fixture.queue_access_token_response()
        overdrive_api_fixture.mock_http.queue_response(200, content=loans_data)
        overdrive_api_fixture.mock_http.queue_response(200, content=holds_data)
        patron = db.patron()

        loans, holds = overdrive_api_fixture.sync_patron_activity(patron)
        # All four loans in the sample data were created.
        assert len(holds) == 4
        assert sorted(holds.values()) == sorted(patron.holds)

        # Running the sync again leaves all four holds in place.
        overdrive_api_fixture.mock_http.queue_response(200, content=loans_data)
        overdrive_api_fixture.mock_http.queue_response(200, content=holds_data)
        loans, holds = overdrive_api_fixture.sync_patron_activity(patron)
        assert len(holds) == 4
        assert sorted(holds.values()) == sorted(patron.holds)

    def test_sync_patron_activity_removes_holds_not_present_on_remote(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
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

        overdrive_api_fixture.queue_access_token_response()
        overdrive_api_fixture.mock_http.queue_response(200, content=loans_data)
        overdrive_api_fixture.mock_http.queue_response(200, content=holds_data)

        # The hold not present in the sample data has been removed
        loans, holds = overdrive_api_fixture.sync_patron_activity(patron)
        assert len(holds) == 4
        assert set(holds.values()) == set(patron.holds)
        assert overdrive_hold not in patron.holds

    def test_sync_patron_activity_ignores_holds_from_other_collections(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
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

        overdrive_api_fixture.queue_access_token_response()
        overdrive_api_fixture.mock_http.queue_response(200, content=loans_data)
        overdrive_api_fixture.mock_http.queue_response(200, content=holds_data)

        # overdrive_api_fixture.api doesn't know about the hold, but it was not
        # destroyed, because it came from a different collection.
        overdrive_api_fixture.sync_patron_activity(patron)
        assert len(patron.holds) == 5
        assert overdrive_hold in patron.holds

    async def test_fetch_book_info_list(
        self,
        overdrive_api_fixture: OverdriveAPIFixture,
    ):
        collection = overdrive_api_fixture.collection
        availability_data, availability_json = overdrive_api_fixture.sample_json(
            "overdrive_availability_information.json"
        )
        metadata_data, metadata_json = overdrive_api_fixture.sample_json(
            "bibliographic_information_book_list_test.json"
        )
        (
            overdrive_book_list_with_next_link_data,
            overdrive_book_list_with_next_link_json,
        ) = overdrive_api_fixture.sample_json("overdrive_book_list_with_next_link.json")
        api = overdrive_api_fixture.api

        mock_async_client = overdrive_api_fixture.mock_async_client

        mock_async_client.queue_response(
            200, content=overdrive_book_list_with_next_link_data
        )
        mock_async_client.queue_response(200, content=availability_data)
        mock_async_client.queue_response(200, content=metadata_data)

        initial_endpoint = api.book_info_initial_endpoint(start=None, page_size=1)

        book_info_list, next_endpoint = await api.fetch_book_info_list(
            initial_endpoint, fetch_metadata=True, fetch_availability=True
        )
        assert next_endpoint
        assert book_info_list
        assert len(book_info_list) == 1
        assert book_info_list[0]["metadata"]
        assert book_info_list[0]["availabilityV2"]

    async def test_fetch_book_info_list_retry_and_unrecoverable_error(
        self,
        overdrive_api_fixture: OverdriveAPIFixture,
    ):
        (
            overdrive_book_list_with_next_link_data,
            overdrive_book_list_with_next_link_json,
        ) = overdrive_api_fixture.sample_json("overdrive_book_list_with_next_link.json")
        api = overdrive_api_fixture.api
        mock_async_client = overdrive_api_fixture.mock_async_client

        # test recovery after failure with book list page
        mock_async_client.queue_response(
            502,
            content="error",
        )

        mock_async_client.queue_response(
            200, content=overdrive_book_list_with_next_link_data
        )

        # test retry and failure with metadata and availabililty
        for x in range(8):
            # error for 4 attempts for availability and metadata
            mock_async_client.queue_response(500, content="500 Internal Server Error")

        # use no backoff since we want the tests to execute quickly
        with patch(
            "palace.manager.integration.license.overdrive.api.WORKER_DEFAULT_BACKOFF",
            None,
        ):
            with pytest.raises(BadResponseException) as e:
                initial_endpoint = api.book_info_initial_endpoint(
                    start=None, page_size=1
                )
                await api.fetch_book_info_list(
                    initial_endpoint, fetch_metadata=True, fetch_availability=True
                )

            assert e.value.response.status_code == 500

    async def test_fetch_book_info_list_with_404_error(
        self,
        overdrive_api_fixture: OverdriveAPIFixture,
    ):
        (
            overdrive_book_list_with_next_link_data,
            overdrive_book_list_with_next_link_json,
        ) = overdrive_api_fixture.sample_json("overdrive_book_list_with_next_link.json")
        api = overdrive_api_fixture.api
        mock_async_client = overdrive_api_fixture.mock_async_client

        mock_async_client.queue_response(
            200, content=overdrive_book_list_with_next_link_data
        )

        # test retry and failure with metadata and availabililty
        for x in range(2):
            # error for 4 attempts for availability and metadata
            mock_async_client.queue_response(404, content="Not Found")

        initial_endpoint = api.book_info_initial_endpoint(start=None, page_size=1)
        data, next_endpoint = await api.fetch_book_info_list(
            initial_endpoint, fetch_metadata=True, fetch_availability=True
        )

        assert len(data) == 1
        assert next_endpoint
        assert data[0]["id"]
        assert data[0].get("metadata", None) is None
        assert data[0].get("availabilityV2", None) is None

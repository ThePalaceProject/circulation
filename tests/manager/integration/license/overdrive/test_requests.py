from __future__ import annotations

import asyncio
import json
from datetime import timedelta
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from palace.manager.api.circulation.exceptions import (
    CannotFulfill,
    NoActiveLoan,
    PatronAuthorizationFailedException,
)
from palace.manager.api.config import Configuration
from palace.manager.api.model.token import OAuthTokenResponse
from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.core.exceptions import IntegrationException
from palace.manager.integration.license.overdrive.constants import OverdriveConstants
from palace.manager.integration.license.overdrive.exception import (
    OverdriveResponseException,
    OverdriveValidationError,
)
from palace.manager.integration.license.overdrive.model import (
    Checkout,
    Checkouts,
    Format,
    Holds,
    PatronInformation,
    RequestSpec,
)
from palace.manager.integration.license.overdrive.requests import (
    BookInfoEndpoint,
    OverdriveAsyncRequests,
    OverdriveClientRequests,
    OverdrivePatronRequests,
)
from palace.manager.util import base64
from palace.manager.util.http.async_http import AsyncClient
from palace.manager.util.http.exception import BadResponseException
from tests.fixtures.files import OverdriveFilesFixture
from tests.fixtures.http import MockAsyncClientFixture, MockHttpClientFixture
from tests.fixtures.webserver import MockAPIServer, MockAPIServerResponse
from tests.manager.integration.license.overdrive.conftest import (
    OverdriveAsyncRequestsFixture,
    OverdriveClientRequestsFixture,
    OverdrivePatronRequestsFixture,
)


class TestOverdriveClientRequests:
    def test_hosts(
        self, overdrive_client_requests: OverdriveClientRequestsFixture
    ) -> None:
        create_settings = overdrive_client_requests.create_settings

        # By default, the production set of hostnames is used.
        production = OverdriveClientRequests(
            create_settings(
                overdrive_server_nickname=OverdriveConstants.PRODUCTION_SERVERS
            )
        )
        assert (
            production._hosts
            == OverdriveClientRequests.HOSTS[OverdriveConstants.PRODUCTION_SERVERS]
        )

        testing = OverdriveClientRequests(
            create_settings(
                overdrive_server_nickname=OverdriveConstants.TESTING_SERVERS
            )
        )
        assert (
            testing._hosts
            == OverdriveClientRequests.HOSTS[OverdriveConstants.TESTING_SERVERS]
        )

        # If the setting doesn't make sense, we default to production hostnames.
        bad = OverdriveClientRequests(
            create_settings(overdrive_server_nickname="nonsensical")
        )
        assert (
            bad._hosts
            == OverdriveClientRequests.HOSTS[OverdriveConstants.PRODUCTION_SERVERS]
        )

    def test_endpoint(
        self, overdrive_client_requests: OverdriveClientRequestsFixture
    ) -> None:
        # The .endpoint() method performs string interpolation, including
        # the names of servers.
        requests = overdrive_client_requests.requests
        template = (
            "%(host)s %(patron_host)s %(oauth_host)s %(oauth_patron_host)s %(extra)s"
        )
        result = requests.endpoint(template, extra="val")

        # The host names and the 'extra' argument have been used to
        # fill in the string interpolations.
        expect_args = dict(requests._hosts)
        expect_args["extra"] = "val"
        assert template % expect_args == result

        # The string has been completely interpolated.
        assert "%" not in result

        # Once interpolation has happened, doing it again has no effect.
        assert requests.endpoint(result, extra="something else") == result

        # This is important because an interpolated URL may superficially
        # appear to contain extra formatting characters.
        assert (
            requests.endpoint(result + "%3A", extra="something else") == result + "%3A"
        )

    @pytest.mark.parametrize(
        "missing_setting",
        [
            "external_account_id",
            "overdrive_client_key",
            "overdrive_client_secret",
        ],
    )
    def test_missing_configuration(
        self,
        overdrive_client_requests: OverdriveClientRequestsFixture,
        missing_setting: str,
    ) -> None:
        # model_copy skips validation, so we can build the partially
        # configured settings that these guards defend against.
        settings = overdrive_client_requests.settings.model_copy(
            update={missing_setting: None}
        )
        with pytest.raises(CannotLoadConfiguration):
            OverdriveClientRequests(settings)

    def test_library_endpoint_url(
        self, overdrive_client_requests: OverdriveClientRequestsFixture
    ) -> None:
        settings = overdrive_client_requests.create_settings(
            external_account_id="1",
            overdrive_server_nickname=OverdriveConstants.PRODUCTION_SERVERS,
        )

        # An ordinary collection uses the library endpoint.
        requests = OverdriveClientRequests(settings)
        assert (
            requests.library_endpoint_url == "https://api.overdrive.com/v1/libraries/1"
        )

        # An Advantage collection uses the advantage library endpoint,
        # which includes the parent library ID.
        advantage = OverdriveClientRequests(settings, parent_library_id="2")
        assert (
            advantage.library_endpoint_url
            == "https://api.overdrive.com/v1/libraries/2/advantageAccounts/1"
        )

    def test__collection_context_basic_auth_header(
        self, overdrive_client_requests: OverdriveClientRequestsFixture
    ) -> None:
        # Verify that the Authorization header needed to get an access
        # token for a given collection is encoded properly.
        requests = OverdriveClientRequests(
            overdrive_client_requests.create_settings(
                overdrive_client_key="a", overdrive_client_secret="b"
            )
        )
        assert requests._collection_context_basic_auth_header == "Basic YTpi"
        assert (
            requests._collection_context_basic_auth_header
            == "Basic " + base64.standard_b64encode("a:b")
        )

    def test_client_oauth_token(
        self, overdrive_client_requests: OverdriveClientRequestsFixture
    ) -> None:
        """Verify the process of refreshing the Overdrive bearer token."""
        requests = overdrive_client_requests.requests
        client = overdrive_client_requests.client

        # Initially the cached token is None.
        assert requests._token_cache.token is None

        with freeze_time() as frozen_time:
            # Accessing the token triggers a refresh.
            overdrive_client_requests.queue_access_token_response("bearer token")
            assert requests._client_oauth_token == "bearer token"
            assert len(client.requests) == 1

            # Queue up another bearer token response.
            overdrive_client_requests.queue_access_token_response("new bearer token")

            # Accessing the token again won't refresh, because the old token
            # is still valid.
            assert requests._client_oauth_token == "bearer token"
            assert len(client.requests) == 1

            # However if the token expires we will get a new one.
            frozen_time.tick(delta=timedelta(seconds=3600))
            assert requests._client_oauth_token == "new bearer token"
            assert len(client.requests) == 2

    def test_refresh_client_oauth_token_401_raises_error(
        self, overdrive_client_requests: OverdriveClientRequestsFixture
    ) -> None:
        """If we fail to refresh the OAuth bearer token, an exception is raised."""
        overdrive_client_requests.client.queue_response(401)
        with pytest.raises(
            BadResponseException,
            match="Got status code 401 .* can only continue on: 200.",
        ):
            overdrive_client_requests.requests.refresh_client_oauth_token()

    def test_refresh_client_oauth_token_unusable_response(
        self,
        overdrive_client_requests: OverdriveClientRequestsFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A 200 body we can't turn into a token raises an Overdrive error.

        OAuthTokenResponse requires more of the body than we used to read, so
        this has to be translated rather than escaping as a bare pydantic
        ValidationError.
        """
        # A token response with no token_type at all. It is a 2xx body, so
        # the access token in it is live.
        overdrive_client_requests.client.queue_response(
            200, content=json.dumps(dict(access_token="tok1", expires_in=3600))
        )
        with pytest.raises(OverdriveValidationError) as excinfo:
            overdrive_client_requests.requests.refresh_client_oauth_token()

        assert excinfo.value.problem_detail.debug_message is not None
        assert "token_type" in caplog.text
        assert "tok1" not in caplog.text
        assert "tok1" not in str(excinfo.value)

    def test_raw_get_success(
        self, overdrive_client_requests: OverdriveClientRequestsFixture
    ) -> None:
        requests = overdrive_client_requests.requests
        overdrive_client_requests.queue_access_token_response()
        overdrive_client_requests.client.queue_response(200, content="some content")

        status_code, headers, content = requests.raw_get("http://example.com/", {})
        assert status_code == 200
        assert content == b"some content"

    def test_raw_get_401_refreshes_bearer_token(
        self, overdrive_client_requests: OverdriveClientRequestsFixture
    ) -> None:
        requests = overdrive_client_requests.requests
        client = overdrive_client_requests.client

        # We have a token.
        overdrive_client_requests.queue_access_token_response("first token")
        assert requests._client_oauth_token == "first token"

        # But then we try to GET, and receive a 401.
        client.queue_response(401)

        # We refresh the bearer token.
        overdrive_client_requests.queue_access_token_response("new bearer token")

        # Then we retry the GET and it succeeds this time.
        client.queue_response(200, content="at last, the content")

        assert requests.raw_get("http://example.com/", {}) == (
            200,
            {},
            b"at last, the content",
        )

        # The bearer token has been updated.
        assert requests._client_oauth_token == "new bearer token"

    def test_raw_get_401_after_refresh_raises_error(
        self, overdrive_client_requests: OverdriveClientRequestsFixture
    ) -> None:
        requests = overdrive_client_requests.requests
        client = overdrive_client_requests.client

        overdrive_client_requests.queue_access_token_response("first token")
        assert requests._client_oauth_token == "first token"

        # We try to GET and receive a 401.
        client.queue_response(401)

        # We refresh the bearer token.
        overdrive_client_requests.queue_access_token_response("new bearer token")

        # Then we retry the GET but we get another 401.
        client.queue_response(401)

        # That raises a BadResponseException.
        with pytest.raises(
            BadResponseException,
            match="Something's wrong with the Overdrive OAuth Bearer Token",
        ):
            requests.raw_get("http://example.com/", {})

        # We refreshed the token in the process.
        assert requests._client_oauth_token == "new bearer token"

        # We made four requests: the initial token, the original GET,
        # the token refresh, and the retry.
        assert len(client.requests) == 4

    def test_errors_not_retried(
        self,
        overdrive_client_requests: OverdriveClientRequestsFixture,
        mock_web_server: MockAPIServer,
    ) -> None:
        overdrive_client_requests.client.stop_patch()
        requests = overdrive_client_requests.requests

        # Enqueue a response for the request that the server will make for a token.
        _r = MockAPIServerResponse()
        _r.status_code = 200
        _r.set_content(
            b"""{
            "access_token": "x",
            "token_type": "bearer",
            "expires_in": 23
        }
        """
        )
        mock_web_server.enqueue_response("POST", "/oauth/token", _r)

        requests._hosts["oauth_host"] = mock_web_server.url("/oauth")

        # Try a raw_get() call for each error code
        for code in [404]:
            _r = MockAPIServerResponse()
            _r.status_code = code
            mock_web_server.enqueue_response("GET", "/a/b/c", _r)
            _status, _, _ = requests.raw_get(mock_web_server.url("/a/b/c"))
            assert _status == code

        for code in [400, 403, 500, 501, 502, 503]:
            _r = MockAPIServerResponse()
            _r.status_code = code

            # The default is to retry 5 times, so enqueue 5 responses.
            for i in range(0, 6):
                mock_web_server.enqueue_response("GET", "/a/b/c", _r)
            try:
                requests.raw_get(mock_web_server.url("/a/b/c"))
            except BadResponseException:
                pass

        # Exactly one request was made for each error code, plus one for a token
        assert len(mock_web_server.requests()) == 8

    def test_feed_urls_are_escaped(
        self, overdrive_client_requests: OverdriveClientRequestsFixture
    ) -> None:
        """The feed URLs escape the characters Overdrive puts in them.

        Both carry a colon in a query value, and the events feed carries a
        space as well, so the escaping is the part of these that would break
        without saying so.
        """
        requests = overdrive_client_requests.requests

        products = requests.all_products_url("a-collection-token")
        assert products.startswith("https://integration.api.overdrive.com")
        assert "a-collection-token" in products
        assert "sort=dateAdded%3Adesc" in products

        # The caller formats the time as OverdriveAPI.TIME_FORMAT does.
        events = requests.events_url("a-collection-token", "2020-01-01T12:00:00Z", 300)
        assert "lastUpdateTime=2020-01-01T12%3A00%3A00Z" in events
        assert "limit=300" in events

    def test_book_list_page_error_status_is_raised(
        self, overdrive_client_requests: OverdriveClientRequestsFixture
    ) -> None:
        """An error document is not an empty page.

        raw_get hands a 404 back rather than raising it, and every field of a
        page is optional, so without this the error would validate into a page
        with no titles and read as an empty collection.
        """
        overdrive_client_requests.queue_access_token_response()
        overdrive_client_requests.client.queue_response(
            404, content=json.dumps({"errorCode": "NotFound", "message": "no"})
        )

        with pytest.raises(BadResponseException, match="Got status code 404"):
            overdrive_client_requests.requests.book_list_page("http://example.com/feed")

    def test_book_list_page_unparseable_is_raised(
        self, overdrive_client_requests: OverdriveClientRequestsFixture
    ) -> None:
        """A page in a shape we do not know raises an Overdrive error."""
        overdrive_client_requests.queue_access_token_response()
        # totalItems is a count, so a word for it is not a page we can read.
        overdrive_client_requests.client.queue_response(
            200, content=json.dumps({"totalItems": "lots"})
        )

        with pytest.raises(OverdriveValidationError) as excinfo:
            overdrive_client_requests.requests.book_list_page("http://example.com/feed")

        assert excinfo.value.problem_detail.debug_message is not None


class TestOverdrivePatronRequests:
    def test_refresh_patron_oauth_token(
        self, overdrive_patron_requests: OverdrivePatronRequestsFixture
    ) -> None:
        """Verify that patron information is included in the request
        when refreshing a patron access token.
        """
        requests = overdrive_patron_requests.requests
        client = overdrive_patron_requests.client

        token = dict(access_token="token", token_type="bearer", expires_in=3600)
        client.queue_response(200, content=json.dumps(token))
        client.queue_response(200, content=json.dumps(token))

        # Try to refresh the patron access token with a PIN, and
        # then without a PIN.
        requests.refresh_patron_oauth_token(
            username="barcode", password="a pin", scope="scope"
        )
        requests.refresh_patron_oauth_token(
            username="barcode", password=None, scope="scope"
        )

        # Both requests went to the same patrontoken url
        assert client.requests == [
            "https://oauth-patron.overdrive.com/patrontoken",
            "https://oauth-patron.overdrive.com/patrontoken",
        ]

        with_pin, without_pin = client.requests_args

        payload = with_pin["data"]
        assert isinstance(payload, dict)
        assert payload["username"] == "barcode"
        assert payload["scope"] == "scope"
        assert payload["password"] == "a pin"
        assert "password_required" not in payload

        payload = without_pin["data"]
        assert isinstance(payload, dict)
        assert payload["username"] == "barcode"
        assert payload["scope"] == "scope"
        assert payload["password_required"] == "false"
        assert payload["password"] == "[ignore]"

    def test_refresh_patron_oauth_token_failure(
        self,
        overdrive_patron_requests: OverdrivePatronRequestsFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
    ) -> None:
        requests = overdrive_patron_requests.requests
        client = overdrive_patron_requests.client

        # Test with a real 400 response we've seen from overdrive
        client.queue_response(
            400, content=overdrive_files_fixture.sample_data("patron_token_failed.json")
        )
        with pytest.raises(
            PatronAuthorizationFailedException, match="Invalid Library Card"
        ):
            requests.refresh_patron_oauth_token(
                username="barcode", password="a pin", scope="scope"
            )

        # Test with a fictional 403 response that doesn't contain valid json - we've never
        # seen this come back from overdrive, this test is just to make sure we can handle
        # unexpected responses back from OD API.
        client.queue_response(403, content="garbage { json")
        with pytest.raises(
            PatronAuthorizationFailedException,
            match="Failed to authenticate with Overdrive",
        ):
            requests.refresh_patron_oauth_token(
                username="barcode", password="a pin", scope="scope"
            )

    def test_refresh_patron_oauth_token_unusable_response(
        self,
        overdrive_patron_requests: OverdrivePatronRequestsFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A 2xx body we can't turn into a token is still an auth failure.

        Overdrive says yes, but sends something that isn't a usable token, so
        the failure has to stay inside the circulation error path rather than
        escaping as a bare pydantic ValidationError.
        """
        requests = overdrive_patron_requests.requests

        # A token response with no token_type at all. The body is a 2xx, so
        # the access token in it is live. It is deliberately short: pydantic
        # elides the middle of a long value, which would hide a leak here
        # rather than catch it.
        overdrive_patron_requests.client.queue_response(
            200,
            content=json.dumps(dict(access_token="tok1", expires_in=3600)),
        )
        with pytest.raises(
            PatronAuthorizationFailedException,
            match="Failed to authenticate with Overdrive",
        ) as excinfo:
            requests.refresh_patron_oauth_token(
                username="barcode", password="a pin", scope="scope"
            )

        # The validation failure is reported...
        assert "token_type" in caplog.text
        assert "Errors:" in caplog.text

        # ...without the token reaching the log or the exception. The
        # traceback counts: caplog.text includes it, and pydantic renders the
        # input it was given there unless we keep it out.
        assert "tok1" not in caplog.text
        assert "input_value" not in caplog.text
        assert "Traceback" not in caplog.text
        assert "tok1" not in str(excinfo.value)
        assert "tok1" not in str(excinfo.value.problem_detail)

    def test_refresh_patron_oauth_token_missing_palace_credentials(
        self,
        overdrive_patron_requests: OverdrivePatronRequestsFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Without the Palace credentials we cannot act for a patron at all.

        These come from the environment rather than the collection, so a
        missing one is a deployment problem, not a patron problem.
        """
        for suffix in (
            Configuration.OD_FULFILLMENT_CLIENT_KEY_SUFFIX,
            Configuration.OD_FULFILLMENT_CLIENT_SECRET_SUFFIX,
        ):
            monkeypatch.delenv(
                f"{Configuration.OD_PREFIX_TESTING_PREFIX}_{suffix}", raising=False
            )

        with pytest.raises(CannotFulfill):
            overdrive_patron_requests.requests.refresh_patron_oauth_token(
                username="barcode", password="a pin", scope="scope"
            )

    def test_patron_token_request_policy(
        self, overdrive_patron_requests: OverdrivePatronRequestsFixture
    ) -> None:
        """The patron token request uses the integration's timeout and retries.

        Overdrive can be slow to answer this endpoint, and the retry count is
        configurable per collection, so neither may fall back to the global
        HTTP defaults.
        """
        settings = overdrive_patron_requests.create_settings(max_retry_count=7)
        requests = OverdrivePatronRequests(settings)

        token = dict(access_token="token", token_type="bearer", expires_in=3600)
        overdrive_patron_requests.client.queue_response(200, content=json.dumps(token))
        requests.refresh_patron_oauth_token(
            username="barcode", password="a pin", scope="scope"
        )

        kwargs = overdrive_patron_requests.client.requests_args[0]
        assert kwargs["timeout"] == 120
        assert kwargs["max_retry_count"] == 7

    def test_patron_request_401_refreshes_bearer_token(
        self, overdrive_patron_requests: OverdrivePatronRequestsFixture
    ) -> None:
        requests = overdrive_patron_requests.requests
        client = overdrive_patron_requests.client
        provider = overdrive_patron_requests.token_provider

        # If we get a 401, we refresh the bearer token and try again.
        client.queue_response(401)
        client.queue_response(200, content="at last, the content")
        assert (
            requests.patron_request(
                provider, RequestSpec.get("http://example.com/")
            ).text
            == "at last, the content"
        )

        # The token provider was asked for a fresh token.
        assert overdrive_patron_requests.token == "new patron token"
        retry_headers = client.requests_args[-1]["headers"]
        assert retry_headers is not None
        assert retry_headers["Authorization"] == "Bearer new patron token"

        # If we get two 401 in a row, we raise an error.
        client.queue_response(401)
        client.queue_response(401)
        with pytest.raises(IntegrationException, match="patron OAuth Bearer Token"):
            requests.patron_request(provider, RequestSpec.get("http://example.com/"))

    def test_patron_request_401_retry_returns_parsed_model(
        self,
        overdrive_patron_requests: OverdrivePatronRequestsFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
    ) -> None:
        """The retry after a 401 still parses into the requested model.

        The retry has to forward response_type, or a caller that asked for a
        model gets a raw Response back the moment a token happens to expire.
        """
        requests = overdrive_patron_requests.requests
        client = overdrive_patron_requests.client

        client.queue_response(401)
        client.queue_response(
            200,
            content=overdrive_files_fixture.sample_data(
                "checkout_response_no_format_locked_in.json"
            ),
        )

        checkout = requests.patron_request(
            overdrive_patron_requests.token_provider,
            RequestSpec.get("http://example.com/"),
            response_type=Checkout,
        )

        assert isinstance(checkout, Checkout)

    def test_patron_request_translates_error_code(
        self, overdrive_patron_requests: OverdrivePatronRequestsFixture
    ) -> None:
        """An Overdrive error body becomes the matching circulation exception.

        This is what turns a 400 into something the circulation layer can act
        on, so it belongs with the request that produces it rather than only
        with the business methods that happen to call it.
        """
        requests = overdrive_patron_requests.requests
        client = overdrive_patron_requests.client

        client.queue_response(
            400,
            content=json.dumps(
                {
                    "errorCode": "TitleNotCheckedOut",
                    "message": "The title is not checked out.",
                }
            ),
        )
        with pytest.raises(NoActiveLoan, match="The title is not checked out."):
            requests.patron_request(
                overdrive_patron_requests.token_provider,
                RequestSpec.get("http://example.com/"),
            )

        # An error code we don't map falls through to a generic Overdrive error.
        client.queue_response(
            400,
            content=json.dumps(
                {
                    "errorCode": "SomethingNewFromOverdrive",
                    "message": "Something we have not seen before.",
                }
            ),
        )
        with pytest.raises(OverdriveResponseException) as excinfo:
            requests.patron_request(
                overdrive_patron_requests.token_provider,
                RequestSpec.get("http://example.com/"),
            )
        assert excinfo.value.error_code == "SomethingNewFromOverdrive"

    def test_patron_request_raises_validation_error(
        self,
        overdrive_patron_requests: OverdrivePatronRequestsFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """
        If patron request can't validate the response, it raises a OverdriveValidationError.
        """
        overdrive_patron_requests.client.queue_response(200, content="not json")

        with pytest.raises(OverdriveValidationError) as excinfo:
            overdrive_patron_requests.requests.patron_request(
                overdrive_patron_requests.token_provider,
                RequestSpec.get("http://example.com/"),
                response_type=Checkout,
            )

        assert (
            excinfo.value.problem_detail.detail
            == "The server made a request to url, and got an unexpected or invalid response."
        )
        assert excinfo.value.problem_detail.debug_message is not None
        assert "Invalid JSON" in excinfo.value.problem_detail.debug_message
        assert "1 validation error for Checkout" in caplog.text

    @pytest.mark.parametrize("method", ["GET", "POST", "PUT", "DELETE"], ids=str.lower)
    def test_patron_request_uses_spec(
        self,
        overdrive_patron_requests: OverdrivePatronRequestsFixture,
        method: str,
    ) -> None:
        """The spec supplies the verb, body and headers verbatim."""
        client = overdrive_patron_requests.client
        client.queue_response(200, content="content")

        overdrive_patron_requests.requests.patron_request(
            overdrive_patron_requests.token_provider,
            RequestSpec(
                method=method,
                url="http://example.com/",
                data="body",
                headers={"Content-Type": "application/json"},
            ),
        )

        assert client.requests_methods[0].upper() == method
        args = client.requests_args[0]
        assert args["data"] == "body"
        headers = args["headers"]
        assert headers is not None
        assert headers["Content-Type"] == "application/json"
        # The Authorization header is added on top of the spec's headers.
        assert headers["Authorization"] == "Bearer patron token"

    def test_get_checkouts(
        self,
        overdrive_patron_requests: OverdrivePatronRequestsFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
    ) -> None:
        client = overdrive_patron_requests.client
        client.queue_response(
            200, content=overdrive_files_fixture.sample_data("no_loans.json")
        )

        checkouts = overdrive_patron_requests.requests.get_checkouts(
            overdrive_patron_requests.token_provider
        )

        assert isinstance(checkouts, Checkouts)
        assert client.requests_methods == ["GET"]
        assert (
            client.requests[0]
            == "https://integration-patron.api.overdrive.com/v1/patrons/me/checkouts"
        )

    def test_get_checkout(
        self,
        overdrive_patron_requests: OverdrivePatronRequestsFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
    ) -> None:
        client = overdrive_patron_requests.client
        client.queue_response(
            200, content=overdrive_files_fixture.sample_data("single_loan.json")
        )

        checkout = overdrive_patron_requests.requests.get_checkout(
            overdrive_patron_requests.token_provider, "an-identifier"
        )

        assert isinstance(checkout, Checkout)
        # The identifier is upper-cased in the URL.
        assert client.requests_methods == ["GET"]
        assert client.requests[0].endswith("/v1/patrons/me/checkouts/AN-IDENTIFIER")

    def test_create_checkout(
        self,
        overdrive_patron_requests: OverdrivePatronRequestsFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
    ) -> None:
        client = overdrive_patron_requests.client
        client.queue_response(
            200, content=overdrive_files_fixture.sample_data("single_loan.json")
        )

        overdrive_patron_requests.requests.create_checkout(
            overdrive_patron_requests.token_provider, "an-identifier"
        )

        assert client.requests_methods == ["POST"]
        assert client.requests[0].endswith("/v1/patrons/me/checkouts")
        assert client.requests_args[0]["data"] == json.dumps(
            {"fields": [{"name": "reserveId", "value": "an-identifier"}]}
        )

    def test_get_holds(
        self,
        overdrive_patron_requests: OverdrivePatronRequestsFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
    ) -> None:
        client = overdrive_patron_requests.client
        client.queue_response(
            200, content=overdrive_files_fixture.sample_data("no_holds.json")
        )

        holds = overdrive_patron_requests.requests.get_holds(
            overdrive_patron_requests.token_provider
        )

        assert isinstance(holds, Holds)
        assert client.requests_methods == ["GET"]
        assert client.requests[0].endswith("/v1/patrons/me/holds")

    @pytest.mark.parametrize(
        "email,expected_field",
        [
            pytest.param(
                "patron@example.com",
                {"name": "emailAddress", "value": "patron@example.com"},
                id="with_email",
            ),
            pytest.param(
                None, {"name": "ignoreHoldEmail", "value": True}, id="without_email"
            ),
            # Overdrive can report the patron's address as an empty string,
            # which suppresses the notice the same way a missing one does.
            pytest.param(
                "", {"name": "ignoreHoldEmail", "value": True}, id="empty_email"
            ),
        ],
    )
    def test_create_hold(
        self,
        overdrive_patron_requests: OverdrivePatronRequestsFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
        email: str | None,
        expected_field: dict[str, object],
    ) -> None:
        client = overdrive_patron_requests.client
        client.queue_response(
            200, content=overdrive_files_fixture.sample_data("successful_hold.json")
        )

        overdrive_patron_requests.requests.create_hold(
            overdrive_patron_requests.token_provider, "an-identifier", email
        )

        assert client.requests_methods == ["POST"]
        assert client.requests[0].endswith("/v1/patrons/me/holds")
        assert client.requests_args[0]["data"] == json.dumps(
            {
                "fields": [
                    {"name": "reserveId", "value": "an-identifier"},
                    expected_field,
                ]
            }
        )

    def test_delete_hold(
        self, overdrive_patron_requests: OverdrivePatronRequestsFixture
    ) -> None:
        client = overdrive_patron_requests.client
        client.queue_response(204)

        overdrive_patron_requests.requests.delete_hold(
            overdrive_patron_requests.token_provider, "an-identifier"
        )

        assert client.requests_methods == ["DELETE"]
        assert client.requests[0].endswith("/v1/patrons/me/holds/an-identifier")

    def test_get_patron_information(
        self,
        overdrive_patron_requests: OverdrivePatronRequestsFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
    ) -> None:
        client = overdrive_patron_requests.client
        client.queue_response(
            200, content=overdrive_files_fixture.sample_data("patron_info.json")
        )

        information = overdrive_patron_requests.requests.get_patron_information(
            overdrive_patron_requests.token_provider
        )

        assert isinstance(information, PatronInformation)
        assert client.requests_methods == ["GET"]
        assert client.requests[0].endswith("/v1/patrons/me")

    def test_follow_download_link(
        self,
        overdrive_patron_requests: OverdrivePatronRequestsFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
    ) -> None:
        client = overdrive_patron_requests.client
        client.queue_response(
            200,
            content=overdrive_files_fixture.sample_data("format_response_no_drm.json"),
        )

        format_data = overdrive_patron_requests.requests.follow_download_link(
            overdrive_patron_requests.token_provider, "http://example.com/format"
        )

        assert isinstance(format_data, Format)
        assert client.requests_methods == ["GET"]
        assert client.requests[0] == "http://example.com/format"


class TestOverdriveAsyncRequests:
    """The async book-info fetching path used by the import workers."""

    async def test_fetch_book_info_list(
        self,
        overdrive_async_requests: OverdriveAsyncRequestsFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
    ) -> None:
        overdrive_async_requests.seed_token()
        overdrive_async_requests.client.queue_response(
            200,
            content=overdrive_files_fixture.sample_data(
                "overdrive_book_list_with_next_link.json"
            ),
        )
        # fetch_book_info_list queues the metadata request before the
        # availability one, and the mock client answers in order.
        overdrive_async_requests.client.queue_response(
            200,
            content=overdrive_files_fixture.sample_data(
                "bibliographic_information_book_list_test.json"
            ),
        )
        overdrive_async_requests.client.queue_response(
            200,
            content=overdrive_files_fixture.sample_data(
                "overdrive_availability_information.json"
            ),
        )

        book_info_list, next_endpoint = (
            await overdrive_async_requests.requests.fetch_book_info_list(
                BookInfoEndpoint(url="/books"),
                fetch_metadata=True,
                fetch_availability=True,
            )
        )
        assert next_endpoint
        assert len(book_info_list) == 1
        assert "id" in book_info_list[0]["metadata"]
        assert "copiesOwned" in book_info_list[0]["availabilityV2"]

        # The host comes from this class's own settings, and the bearer token
        # from the client context it was handed. The mock answers from a queue
        # regardless of either, so they have to be asserted directly.
        assert (
            overdrive_async_requests.client.request_urls[0]
            == "https://integration.api.overdrive.com/books"
        )
        assert (
            overdrive_async_requests.client.requests[0].headers["Authorization"]
            == "Bearer token"
        )

    async def test_fetch_book_info_list_retry_and_unrecoverable_error(
        self,
        overdrive_async_requests: OverdriveAsyncRequestsFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
    ) -> None:
        overdrive_async_requests.seed_token()
        # test recovery after failure with book list page
        overdrive_async_requests.client.queue_response(502, content="error")
        overdrive_async_requests.client.queue_response(
            200,
            content=overdrive_files_fixture.sample_data(
                "overdrive_book_list_with_next_link.json"
            ),
        )

        # test retry and failure with metadata and availability
        for _ in range(8):
            # error for 4 attempts for availability and metadata
            overdrive_async_requests.client.queue_response(
                500, content="500 Internal Server Error"
            )

        # use no backoff since we want the tests to execute quickly
        with patch(
            "palace.manager.integration.license.overdrive.requests.WORKER_DEFAULT_BACKOFF",
            None,
        ):
            with pytest.raises(BadResponseException) as e:
                await overdrive_async_requests.requests.fetch_book_info_list(
                    BookInfoEndpoint(url="/books"),
                    fetch_metadata=True,
                    fetch_availability=True,
                )

            assert e.value.response.status_code == 500

    async def test_fetch_book_info_list_with_404_error(
        self,
        overdrive_async_requests: OverdriveAsyncRequestsFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
    ) -> None:
        overdrive_async_requests.seed_token()
        overdrive_async_requests.client.queue_response(
            200,
            content=overdrive_files_fixture.sample_data(
                "overdrive_book_list_with_next_link.json"
            ),
        )

        # A 404 on the metadata or availability link is tolerated: those
        # relations may not exist for a given identifier.
        for _ in range(2):
            overdrive_async_requests.client.queue_response(404, content="Not Found")

        data, next_endpoint = (
            await overdrive_async_requests.requests.fetch_book_info_list(
                BookInfoEndpoint(url="/books"),
                fetch_metadata=True,
                fetch_availability=True,
            )
        )
        assert next_endpoint
        assert len(data) == 1
        assert "metadata" not in data[0]
        assert "availabilityV2" not in data[0]

    async def test_fetch_book_info_list_missing_products_key(
        self,
        overdrive_async_requests: OverdriveAsyncRequestsFixture,
    ) -> None:
        """When the Overdrive API returns a response without a 'products' key
        and a non-empty collection, a BadResponseException is raised so the
        Celery task can retry."""
        overdrive_async_requests.seed_token()
        # totalItems is non-zero, so the missing 'products' key is a genuinely
        # malformed response rather than an empty collection.
        overdrive_async_requests.client.queue_response(200, content={"totalItems": 5})

        with pytest.raises(BadResponseException, match="missing 'products' key"):
            await overdrive_async_requests.requests.fetch_book_info_list(
                BookInfoEndpoint(url="/books")
            )

    async def test_fetch_book_info_list_last_page(
        self, overdrive_async_requests: OverdriveAsyncRequestsFixture
    ) -> None:
        """The final page carries links, but no next one.

        This is what ends the import loop in production, and it is a
        different branch from a page with no links at all.
        """
        overdrive_async_requests.seed_token()
        overdrive_async_requests.client.queue_response(
            200,
            content={
                "totalItems": 1,
                "links": {
                    "self": {
                        "href": "http://example.com/books",
                        "type": "application/json",
                    }
                },
                "products": [{"id": "ABC", "links": {}}],
            },
        )

        book_info_list, next_endpoint = (
            await overdrive_async_requests.requests.fetch_book_info_list(
                BookInfoEndpoint(url="/books")
            )
        )

        assert next_endpoint is None
        assert len(book_info_list) == 1

    async def test_fetch_book_info_list_error_status(
        self, overdrive_async_requests: OverdriveAsyncRequestsFixture
    ) -> None:
        """A 404 during an import says so, rather than blaming the products.

        The async client allows a 404 through rather than raising it, and an
        error document parses into a page with no products, so without the
        status check the import would report a missing 'products' key.
        """
        overdrive_async_requests.seed_token()
        overdrive_async_requests.client.queue_response(
            404, content={"errorCode": "NotFound", "message": "no"}
        )

        with pytest.raises(BadResponseException, match="Got status code 404"):
            await overdrive_async_requests.requests.fetch_book_info_list(
                BookInfoEndpoint(url="/books")
            )

    async def test_fetch_book_info_list_unparseable_page(
        self, overdrive_async_requests: OverdriveAsyncRequestsFixture
    ) -> None:
        """A page in an unknown shape fails the same way on both paths.

        The synchronous fetch translates this, and an import worker hitting
        the same body should not get a bare pydantic error instead.
        """
        overdrive_async_requests.seed_token()
        # totalItems is a count, so a word for it is not a page we can read.
        overdrive_async_requests.client.queue_response(
            200, content={"totalItems": "lots"}
        )

        with pytest.raises(OverdriveValidationError) as excinfo:
            await overdrive_async_requests.requests.fetch_book_info_list(
                BookInfoEndpoint(url="/books")
            )

        assert excinfo.value.problem_detail.debug_message is not None

    async def test_fetch_book_info_list_empty_collection(
        self,
        overdrive_async_requests: OverdriveAsyncRequestsFixture,
    ) -> None:
        """Overdrive omits the 'products' key for an empty collection
        (totalItems == 0). This is not an error: we return an empty page."""
        overdrive_async_requests.seed_token()
        overdrive_async_requests.client.queue_response(
            200, content={"totalItems": 0, "limit": 100, "offset": 0}
        )

        book_info_list, next_endpoint = (
            await overdrive_async_requests.requests.fetch_book_info_list(
                BookInfoEndpoint(url="/books")
            )
        )

        assert book_info_list == []
        assert next_endpoint is None

    async def test_token_fetched_without_blocking(
        self,
        overdrive_async_requests: OverdriveAsyncRequestsFixture,
        async_http_client: MockAsyncClientFixture,
    ) -> None:
        # With no cached token, the token is fetched over the async client
        # rather than by a blocking call.
        async_http_client.queue_response(
            200, content=overdrive_async_requests.token_response("a token")
        )

        assert await overdrive_async_requests.requests.bearer_token() == "a token"

        [request] = async_http_client.requests
        assert request.url.path == "/token"
        assert request.method == "POST"

    async def test_token_is_cached(
        self,
        overdrive_async_requests: OverdriveAsyncRequestsFixture,
        async_http_client: MockAsyncClientFixture,
    ) -> None:
        async_http_client.queue_response(
            200, content=overdrive_async_requests.token_response("a token")
        )

        assert await overdrive_async_requests.requests.bearer_token() == "a token"
        assert await overdrive_async_requests.requests.bearer_token() == "a token"

        # Only one request was made for it.
        assert len(async_http_client.requests) == 1

    async def test_token_request_keeps_the_long_timeout(
        self,
        overdrive_async_requests: OverdriveAsyncRequestsFixture,
        async_http_client: MockAsyncClientFixture,
    ) -> None:
        """The token endpoint gets the long timeout, not the worker default.

        Overdrive is slow to answer here, and this refresh runs inside the
        auth flow of a page request, so timing out early would be retried by
        both this client and the page's.
        """
        async_http_client.queue_response(
            200, content=overdrive_async_requests.token_response()
        )

        await overdrive_async_requests.requests.bearer_token()

        timeout = async_http_client.requests[0].extensions.get("timeout")
        assert timeout is not None
        assert timeout["connect"] == float(OverdriveAsyncRequests.REQUEST_TIMEOUT)

    async def test_token_request_does_not_retry(
        self,
        overdrive_async_requests: OverdriveAsyncRequestsFixture,
        async_http_client: MockAsyncClientFixture,
    ) -> None:
        """The token client must not retry on top of the caller that does.

        This refresh runs inside the auth flow of a page request whose client
        already retries, and it holds the token lock while it does, so
        retrying here multiplies the attempts and the wait for every other
        request on the page.
        """
        with patch.object(
            AsyncClient, "for_worker", side_effect=AsyncClient.for_worker
        ) as for_worker:
            async_http_client.queue_response(
                200, content=overdrive_async_requests.token_response()
            )
            await overdrive_async_requests.requests.bearer_token()

        assert for_worker.call_args.kwargs["max_retries"] == 0

    async def test_concurrent_refresh_fetches_one_token(
        self,
        overdrive_async_requests: OverdriveAsyncRequestsFixture,
        async_http_client: MockAsyncClientFixture,
    ) -> None:
        # When a page's requests all discover a stale token at once, only one
        # of them should ask Overdrive for a replacement.
        overdrive_async_requests.seed_token("stale token")
        async_http_client.queue_response(
            200, content=overdrive_async_requests.token_response("fresh token")
        )

        tokens = await asyncio.gather(
            *(
                overdrive_async_requests.requests.bearer_token(rejected="stale token")
                for _ in range(10)
            )
        )

        assert tokens == ["fresh token"] * 10
        assert len(async_http_client.requests) == 1

    async def test_token_refreshed_while_waiting_for_the_lock(
        self,
        overdrive_async_requests: OverdriveAsyncRequestsFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A caller that waited for the lock uses what the winner fetched.

        The other dedup test never reaches the lock, because the mocked
        refresh finishes before the later callers start. This one holds the
        refresh open so they queue behind it.
        """
        requests = overdrive_async_requests.requests
        overdrive_async_requests.seed_token("stale token")

        started, finish = asyncio.Event(), asyncio.Event()
        calls = 0

        async def refresh() -> OAuthTokenResponse:
            nonlocal calls
            calls += 1
            started.set()
            await finish.wait()
            overdrive_async_requests.seed_token("fresh token")
            token = overdrive_async_requests.token_cache.token
            assert token is not None
            return token

        monkeypatch.setattr(requests, "_refresh_token", refresh)

        first = asyncio.create_task(requests.bearer_token(rejected="stale token"))
        await started.wait()
        second = asyncio.create_task(requests.bearer_token(rejected="stale token"))
        await asyncio.sleep(0)
        finish.set()

        assert await first == "fresh token"
        assert await second == "fresh token"
        # The waiter took the winner's token rather than fetching its own.
        assert calls == 1

    async def test_unusable_token_response(
        self,
        overdrive_async_requests: OverdriveAsyncRequestsFixture,
        async_http_client: MockAsyncClientFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A 200 that isn't a usable token is an Overdrive error, not a crash.

        The body is a live token document, so neither it nor pydantic's echo
        of it may reach the log or the exception.
        """
        async_http_client.queue_response(
            200, content=json.dumps(dict(access_token="tok1", expires_in=3600))
        )

        with pytest.raises(OverdriveValidationError) as excinfo:
            await overdrive_async_requests.requests.bearer_token()

        assert "token_type" in caplog.text
        assert "tok1" not in caplog.text
        assert "tok1" not in str(excinfo.value)
        # The exception snapshots the response body for the celery result
        # backend, so the token must not be in there either.
        assert "tok1" not in excinfo.value.response.text
        assert b"tok1" not in excinfo.value.response.content

    async def test_dead_token_endpoint_fails_the_page_once(
        self,
        overdrive_async_requests: OverdriveAsyncRequestsFixture,
        async_http_client: MockAsyncClientFixture,
    ) -> None:
        """A page that cannot get a token fails before it starts.

        Every request on the page needs one, so asking up front means one
        failed attempt rather than each request finding out for itself.
        """
        async_http_client.queue_response(500, content="nope")

        with pytest.raises(BadResponseException):
            await overdrive_async_requests.requests.fetch_book_info_list(
                BookInfoEndpoint(url="/books")
            )

        # The token was asked for, and the page never started.
        assert len(async_http_client.requests) == 1
        assert async_http_client.request_urls[0].endswith("/token")

    @pytest.mark.parametrize(
        "missing_setting",
        ["overdrive_client_key", "overdrive_client_secret"],
    )
    def test_missing_configuration(
        self,
        overdrive_async_requests: OverdriveAsyncRequestsFixture,
        missing_setting: str,
    ) -> None:
        # model_copy skips validation, so we can build the partially
        # configured settings that these guards defend against.
        settings = overdrive_async_requests.settings.model_copy(
            update={missing_setting: None}
        )
        with pytest.raises(CannotLoadConfiguration):
            OverdriveAsyncRequests(settings)

    async def test_token_shared_with_sync_layer(
        self,
        overdrive_async_requests: OverdriveAsyncRequestsFixture,
        async_http_client: MockAsyncClientFixture,
        http_client: MockHttpClientFixture,
    ) -> None:
        # http_client is requested for its patching side effect: this is the
        # one test that builds a real OverdriveClientRequests, so if the cache
        # sharing regressed it would otherwise make a live token request.
        # Both layers read the same cache, so a collection only ever holds
        # one token.
        client_requests = OverdriveClientRequests(
            overdrive_async_requests.settings,
            token_cache=overdrive_async_requests.token_cache,
        )
        async_http_client.queue_response(
            200, content=overdrive_async_requests.token_response("shared token")
        )

        assert await overdrive_async_requests.requests.bearer_token() == "shared token"

        # The synchronous layer picks it up without a request of its own.
        assert client_requests._client_oauth_token == "shared token"


class TestOverdriveClientAuth:
    """The httpx auth flow that keeps a page's requests authenticated."""

    async def test_token_attached_to_every_request(
        self,
        overdrive_async_requests: OverdriveAsyncRequestsFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
        async_http_client: MockAsyncClientFixture,
    ) -> None:
        overdrive_async_requests.seed_token("a token")
        async_http_client.queue_response(
            200,
            content=overdrive_files_fixture.sample_data(
                "overdrive_book_list_with_next_link.json"
            ),
        )
        async_http_client.queue_response(
            200,
            content=overdrive_files_fixture.sample_data(
                "overdrive_availability_information.json"
            ),
        )

        await overdrive_async_requests.requests.fetch_book_info_list(
            BookInfoEndpoint(url="/books"), fetch_availability=True
        )

        assert len(async_http_client.requests) == 2
        for request in async_http_client.requests:
            assert request.headers["Authorization"] == "Bearer a token"

    async def test_expired_token_mid_page_is_refreshed(
        self,
        overdrive_async_requests: OverdriveAsyncRequestsFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
        async_http_client: MockAsyncClientFixture,
    ) -> None:
        # A token that expires partway through a page costs one retry of the
        # rejected request, not the whole page.
        overdrive_async_requests.seed_token("stale token")

        # The page itself is fetched successfully...
        async_http_client.queue_response(
            200,
            content=overdrive_files_fixture.sample_data(
                "overdrive_book_list_with_next_link.json"
            ),
        )
        # ...then the token goes stale and the availability request is rejected.
        async_http_client.queue_response(401, content="Unauthorized")
        # The auth flow gets a new token...
        async_http_client.queue_response(
            200, content=overdrive_async_requests.token_response("fresh token")
        )
        # ...and the rejected request succeeds on the retry.
        async_http_client.queue_response(
            200,
            content=overdrive_files_fixture.sample_data(
                "overdrive_availability_information.json"
            ),
        )

        book_info_list, _ = (
            await overdrive_async_requests.requests.fetch_book_info_list(
                BookInfoEndpoint(url="/books"), fetch_availability=True
            )
        )

        # The page survived.
        assert len(book_info_list) == 1
        assert book_info_list[0]["availabilityV2"]

        # The retry carried the new token.
        assert async_http_client.requests[-1].headers["Authorization"] == (
            "Bearer fresh token"
        )

    async def test_401_that_survives_refresh_is_raised(
        self,
        overdrive_async_requests: OverdriveAsyncRequestsFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
        async_http_client: MockAsyncClientFixture,
    ) -> None:
        # If a fresh token is also rejected, the request fails rather than
        # looping.
        overdrive_async_requests.seed_token("stale token")
        async_http_client.queue_response(
            200,
            content=overdrive_files_fixture.sample_data(
                "overdrive_book_list_with_next_link.json"
            ),
        )
        # One round only: the auth flow refreshes and retries once, and the
        # page client is told not to retry a 401 on top of that.
        async_http_client.queue_response(401, content="Unauthorized")
        async_http_client.queue_response(
            200, content=overdrive_async_requests.token_response("fresh token")
        )
        async_http_client.queue_response(401, content="Unauthorized")

        with patch(
            "palace.manager.integration.license.overdrive.requests.WORKER_DEFAULT_BACKOFF",
            None,
        ):
            with pytest.raises(BadResponseException) as excinfo:
                await overdrive_async_requests.requests.fetch_book_info_list(
                    BookInfoEndpoint(url="/books"), fetch_availability=True
                )

        assert excinfo.value.response.status_code == 401

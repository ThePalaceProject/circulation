from __future__ import annotations

import json
from datetime import timedelta

import pytest
from freezegun import freeze_time

from palace.manager.api.circulation.exceptions import (
    PatronAuthorizationFailedException,
)
from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.core.exceptions import IntegrationException
from palace.manager.integration.license.overdrive.constants import OverdriveConstants
from palace.manager.integration.license.overdrive.exception import (
    OverdriveValidationError,
)
from palace.manager.integration.license.overdrive.model import Checkout
from palace.manager.integration.license.overdrive.requests import (
    OverdriveClientRequests,
)
from palace.manager.util import base64
from palace.manager.util.http.exception import BadResponseException
from tests.fixtures.files import OverdriveFilesFixture
from tests.fixtures.webserver import MockAPIServer, MockAPIServerResponse
from tests.manager.integration.license.overdrive.conftest import (
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
        assert requests._cached_token is None

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
        # A token response with no token_type at all.
        overdrive_client_requests.client.queue_response(
            200, content=json.dumps(dict(access_token="token", expires_in=3600))
        )
        with pytest.raises(OverdriveValidationError) as excinfo:
            overdrive_client_requests.requests.refresh_client_oauth_token()

        assert excinfo.value.problem_detail.debug_message is not None
        assert "1 validation error for OAuthTokenResponse" in caplog.text

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


class TestOverdrivePatronRequests:
    def test_refresh_patron_oauth_token(
        self,
        overdrive_patron_requests: OverdrivePatronRequestsFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
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
            requests.patron_request(provider, "http://example.com/").text
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
            requests.patron_request(provider, "http://example.com/")

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
                "http://example.com/",
                response_type=Checkout,
            )

        assert (
            excinfo.value.problem_detail.detail
            == "The server made a request to url, and got an unexpected or invalid response."
        )
        assert excinfo.value.problem_detail.debug_message is not None
        assert "Invalid JSON" in excinfo.value.problem_detail.debug_message
        assert "1 validation error for Checkout" in caplog.text

    @pytest.mark.parametrize(
        "data,method,expected",
        [
            pytest.param(None, None, "GET", id="no_data_defaults_to_get"),
            pytest.param("body", None, "POST", id="data_defaults_to_post"),
            pytest.param(None, "DELETE", "DELETE", id="explicit_method_wins"),
            pytest.param("body", "PUT", "PUT", id="explicit_method_with_data"),
        ],
    )
    def test_patron_request_method_inference(
        self,
        overdrive_patron_requests: OverdrivePatronRequestsFixture,
        data: str | None,
        method: str | None,
        expected: str,
    ) -> None:
        client = overdrive_patron_requests.client
        client.queue_response(200, content="content")

        overdrive_patron_requests.requests.patron_request(
            overdrive_patron_requests.token_provider,
            "http://example.com/",
            data=data,
            method=method,
        )

        assert client.requests_methods[0].upper() == expected

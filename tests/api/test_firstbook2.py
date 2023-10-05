import os
import time
import urllib.parse
from functools import partial
from typing import Callable

import jwt
import pytest
import requests

from api.authentication.base import PatronData
from api.authentication.basic import BasicAuthProviderLibrarySettings
from api.circulation_exceptions import RemoteInitiatedServerError
from api.firstbook2 import FirstBookAuthenticationAPI, FirstBookAuthSettings
from tests.fixtures.database import DatabaseTransactionFixture


class MockFirstBookResponse:
    def __init__(self, status_code, content):
        self.status_code = status_code
        # Guarantee that the response content is always a bytestring,
        # as it would be in real life.
        if isinstance(content, str):
            content = content.encode("utf8")
        self.content = content


class MockFirstBookAuthenticationAPI(FirstBookAuthenticationAPI):
    SUCCESS = '"Valid Code Pin Pair"'
    FAILURE = '{"code":404,"message":"Access Code Pin Pair not found"}'

    def __init__(
        self,
        library_id,
        integration_id,
        settings,
        library_settings,
        valid=None,
        bad_connection=False,
        failure_status_code=None,
    ):
        super().__init__(library_id, integration_id, settings, library_settings, None)

        if valid is None:
            valid = {}
        self.valid = valid
        self.bad_connection = bad_connection
        self.failure_status_code = failure_status_code

        self.request_urls = []

    def request(self, url):
        self.request_urls.append(url)
        if self.bad_connection:
            # Simulate a bad connection.
            raise requests.exceptions.ConnectionError("Could not connect!")
        elif self.failure_status_code:
            # Simulate a server returning an unexpected error code.
            return MockFirstBookResponse(
                self.failure_status_code, "Error %s" % self.failure_status_code
            )
        parsed = urllib.parse.urlparse(url)
        token = parsed.path.split("/")[-1]
        barcode, pin = self._decode(token)

        # The barcode and pin must be present in self.valid.
        if barcode in self.valid and self.valid[barcode] == pin:
            return MockFirstBookResponse(200, self.SUCCESS)
        else:
            return MockFirstBookResponse(200, self.FAILURE)

    def _decode(self, token):
        # Decode a JWT. Only used in tests -- in production, this is
        # First Book's job.

        # The JWT must be signed with the shared secret.
        payload = jwt.decode(token, self.secret, algorithms=self.ALGORITHM)

        # The 'iat' field in the payload must be a recent timestamp.
        assert (time.time() - int(payload["iat"])) < 2

        return payload["barcode"], payload["pin"]


@pytest.fixture
def mock_library_id() -> int:
    return 20


@pytest.fixture
def mock_integration_id() -> int:
    return 20


@pytest.fixture
def create_settings() -> Callable[..., FirstBookAuthSettings]:
    return partial(
        FirstBookAuthSettings,
        url="http://example.com/",
        password="secret",
    )


@pytest.fixture
def create_provider(
    mock_library_id: int,
    mock_integration_id: int,
    create_settings: Callable[..., FirstBookAuthSettings],
) -> Callable[..., MockFirstBookAuthenticationAPI]:
    return partial(
        MockFirstBookAuthenticationAPI,
        library_id=mock_library_id,
        integration_id=mock_integration_id,
        settings=create_settings(),
        library_settings=BasicAuthProviderLibrarySettings(),
        valid={"ABCD": "1234"},
    )


class TestFirstBook:
    def test_from_config(
        self,
        create_settings: Callable[..., FirstBookAuthSettings],
        create_provider: Callable[..., MockFirstBookAuthenticationAPI],
    ):
        settings = create_settings(
            password="the_key",
        )
        provider = create_provider(settings=settings)

        # Verify that the configuration details were stored properly.
        assert "http://example.com/" == provider.root
        assert "the_key" == provider.secret

        # Test the default server-side authentication regular expressions.
        assert provider.server_side_validation("foo' or 1=1 --;", "1234") is False
        assert provider.server_side_validation("foo", "12 34") is False
        assert provider.server_side_validation("foo", "1234") is True
        assert provider.server_side_validation("foo@bar", "1234") is True

    def test_authentication_success(
        self,
        create_provider: Callable[..., MockFirstBookAuthenticationAPI],
    ):
        provider = create_provider()

        # The mock API successfully decodes the JWT and verifies that
        # the given barcode and pin authenticate a specific patron.
        assert provider.remote_pin_test("ABCD", "1234") is True

        # Let's see what the mock API had to work with.
        requested = provider.request_urls.pop()
        assert requested.startswith(provider.root)
        token = requested[len(provider.root) :]

        # It's a JWT, with the provided barcode and PIN in the
        # payload.
        barcode, pin = provider._decode(token)
        assert "ABCD" == barcode
        assert "1234" == pin

    def test_authentication_failure(
        self,
        create_provider: Callable[..., MockFirstBookAuthenticationAPI],
    ):
        provider = create_provider()

        assert provider.remote_pin_test("ABCD", "9999") is False
        assert provider.remote_pin_test("nosuchkey", "9999") is False

        # credentials are uppercased in remote_authenticate;
        # remote_pin_test just passes on whatever it's sent.
        assert provider.remote_pin_test("abcd", "9999") is False

    def test_remote_authenticate(
        self,
        create_provider: Callable[..., MockFirstBookAuthenticationAPI],
    ):
        provider = create_provider()

        patrondata = provider.remote_authenticate("abcd", "1234")
        assert isinstance(patrondata, PatronData)
        assert "ABCD" == patrondata.permanent_id
        assert "ABCD" == patrondata.authorization_identifier
        assert patrondata.username is None

        patrondata = provider.remote_authenticate("ABCD", "1234")
        assert isinstance(patrondata, PatronData)
        assert "ABCD" == patrondata.permanent_id
        assert "ABCD" == patrondata.authorization_identifier
        assert patrondata.username is None

        # When username is none, the patrondata object should be None
        patrondata = provider.remote_authenticate(None, "1234")
        assert patrondata is None

    def test_broken_service_remote_pin_test(
        self,
        create_provider: Callable[..., MockFirstBookAuthenticationAPI],
    ):
        provider = create_provider(failure_status_code=502)
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            provider.remote_pin_test("key", "pin")
        assert "Got unexpected response code 502. Content: Error 502" in str(
            excinfo.value
        )

    def test_bad_connection_remote_pin_test(
        self,
        create_provider: Callable[..., MockFirstBookAuthenticationAPI],
    ):
        provider = create_provider(bad_connection=True)
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            provider.remote_pin_test("key", "pin")
        assert "Could not connect!" in str(excinfo.value)

    def test_authentication_flow_document(
        self,
        create_provider: Callable[..., MockFirstBookAuthenticationAPI],
        db: DatabaseTransactionFixture,
    ):
        # We're about to call url_for, so we must create an
        # application context.
        provider = create_provider()
        os.environ["AUTOINITIALIZE"] = "False"
        from api.app import app

        del os.environ["AUTOINITIALIZE"]
        with app.test_request_context("/"):
            doc = provider.authentication_flow_document(db.session)
            assert provider.label() == doc["description"]
            assert provider.flow_type == doc["type"]

    def test_jwt(
        self,
        create_provider: Callable[..., MockFirstBookAuthenticationAPI],
    ):
        provider = create_provider()
        # Test the code that generates and signs JWTs.
        token = provider.jwt("a barcode", "a pin")

        # The JWT was signed with the shared secret. Decode it (this
        # validates it as a side effect) and we can see the payload.
        barcode, pin = provider._decode(token)

        assert "a barcode" == barcode
        assert "a pin" == pin

        # If the secrets don't match, decoding won't work.
        provider.secret = "bad secret"
        pytest.raises(jwt.DecodeError, provider._decode, token)

    def test_remote_patron_lookup(
        self,
        create_provider: Callable[..., MockFirstBookAuthenticationAPI],
        db: DatabaseTransactionFixture,
    ):
        provider = create_provider()
        # Remote patron lookup is not supported. It always returns
        # the same PatronData object passed into it.
        input_patrondata = PatronData()
        output_patrondata = provider.remote_patron_lookup(input_patrondata)
        assert input_patrondata == output_patrondata

        # if anything else is passed in, it returns None
        output_patrondata = provider.remote_patron_lookup(db.patron())
        assert output_patrondata is None

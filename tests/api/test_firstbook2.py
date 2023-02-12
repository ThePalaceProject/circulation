import os

import jwt
import pytest

from palace.api.circulation_exceptions import RemoteInitiatedServerError
from palace.api.firstbook2 import (
    FirstBookAuthenticationAPI,
    MockFirstBookAuthenticationAPI,
)
from palace.core.model import ExternalIntegration
from tests.fixtures.database import DatabaseTransactionFixture


class FirstBookFixture2:
    db: DatabaseTransactionFixture
    integration: ExternalIntegration
    api: MockFirstBookAuthenticationAPI

    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.integration = db.external_integration(ExternalIntegration.PATRON_AUTH_GOAL)
        self.api = self.mock_api(dict(ABCD="1234"))

    def mock_api(self, *args, **kwargs):
        """Create a MockFirstBookAuthenticationAPI."""
        return MockFirstBookAuthenticationAPI(
            self.db.default_library(), self.integration, *args, **kwargs
        )


@pytest.fixture(scope="function")
def firstbook_fixture2(db: DatabaseTransactionFixture) -> FirstBookFixture2:
    return FirstBookFixture2(db)


class TestFirstBook:
    def test_from_config(self, firstbook_fixture2: FirstBookFixture2):
        api = None
        integration = firstbook_fixture2.db.external_integration(
            firstbook_fixture2.db.fresh_str()
        )
        integration.url = "http://example.com/"
        integration.password = "the_key"
        api = FirstBookAuthenticationAPI(
            firstbook_fixture2.db.default_library(), integration
        )

        # Verify that the configuration details were stored properly.
        assert "http://example.com/" == api.root
        assert "the_key" == api.secret

        # Test the default server-side authentication regular expressions.
        assert False == api.server_side_validation("foo' or 1=1 --;", "1234")
        assert False == api.server_side_validation("foo", "12 34")
        assert True == api.server_side_validation("foo", "1234")
        assert True == api.server_side_validation("foo@bar", "1234")

    def test_authentication_success(self, firstbook_fixture2: FirstBookFixture2):

        # The mock API successfully decodes the JWT and verifies that
        # the given barcode and pin authenticate a specific patron.
        assert True == firstbook_fixture2.api.remote_pin_test("ABCD", "1234")

        # Let's see what the mock API had to work with.
        requested = firstbook_fixture2.api.request_urls.pop()
        assert requested.startswith(firstbook_fixture2.api.root)
        token = requested[len(firstbook_fixture2.api.root) :]

        # It's a JWT, with the provided barcode and PIN in the
        # payload.
        barcode, pin = firstbook_fixture2.api._decode(token)
        assert "ABCD" == barcode
        assert "1234" == pin

    def test_authentication_failure(self, firstbook_fixture2: FirstBookFixture2):
        assert False == firstbook_fixture2.api.remote_pin_test("ABCD", "9999")
        assert False == firstbook_fixture2.api.remote_pin_test("nosuchkey", "9999")

        # credentials are uppercased in remote_authenticate;
        # remote_pin_test just passes on whatever it's sent.
        assert False == firstbook_fixture2.api.remote_pin_test("abcd", "9999")

    def test_remote_authenticate(self, firstbook_fixture2: FirstBookFixture2):
        patrondata = firstbook_fixture2.api.remote_authenticate("abcd", "1234")
        assert "ABCD" == patrondata.permanent_id
        assert "ABCD" == patrondata.authorization_identifier
        assert None == patrondata.username

        patrondata = firstbook_fixture2.api.remote_authenticate("ABCD", "1234")
        assert "ABCD" == patrondata.permanent_id
        assert "ABCD" == patrondata.authorization_identifier
        assert None == patrondata.username

    def test_broken_service_remote_pin_test(
        self, firstbook_fixture2: FirstBookFixture2
    ):
        api = firstbook_fixture2.mock_api(failure_status_code=502)
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            api.remote_pin_test("key", "pin")
        assert "Got unexpected response code 502. Content: Error 502" in str(
            excinfo.value
        )

    def test_bad_connection_remote_pin_test(
        self, firstbook_fixture2: FirstBookFixture2
    ):
        api = firstbook_fixture2.mock_api(bad_connection=True)
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            api.remote_pin_test("key", "pin")
        assert "Could not connect!" in str(excinfo.value)

    def test_authentication_flow_document(self, firstbook_fixture2: FirstBookFixture2):
        # We're about to call url_for, so we must create an
        # application context.
        os.environ["AUTOINITIALIZE"] = "False"
        from palace.api.app import app

        self.app = app
        del os.environ["AUTOINITIALIZE"]
        with self.app.test_request_context("/"):
            doc = firstbook_fixture2.api.authentication_flow_document(
                firstbook_fixture2.db.session
            )
            assert firstbook_fixture2.api.DISPLAY_NAME == doc["description"]
            assert firstbook_fixture2.api.FLOW_TYPE == doc["type"]

    def test_jwt(self, firstbook_fixture2: FirstBookFixture2):
        # Test the code that generates and signs JWTs.
        token = firstbook_fixture2.api.jwt("a barcode", "a pin")

        # The JWT was signed with the shared secret. Decode it (this
        # validates it as a side effect) and we can see the payload.
        barcode, pin = firstbook_fixture2.api._decode(token)

        assert "a barcode" == barcode
        assert "a pin" == pin

        # If the secrets don't match, decoding won't work.
        firstbook_fixture2.api.secret = "bad secret"
        pytest.raises(jwt.DecodeError, firstbook_fixture2.api._decode, token)

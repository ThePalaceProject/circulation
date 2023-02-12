import os

import pytest

from palace.api.circulation_exceptions import RemoteInitiatedServerError
from palace.api.firstbook import (
    FirstBookAuthenticationAPI,
    MockFirstBookAuthenticationAPI,
)
from palace.core.model import ExternalIntegration
from tests.fixtures.database import DatabaseTransactionFixture


class FirstBookFixture:
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
def firstbook_fixture(db: DatabaseTransactionFixture) -> FirstBookFixture:
    return FirstBookFixture(db)


class TestFirstBook:
    def test_from_config(self, firstbook_fixture: FirstBookFixture):
        api = None
        integration = firstbook_fixture.db.external_integration(
            firstbook_fixture.db.fresh_str()
        )
        integration.url = "http://example.com/"
        integration.password = "the_key"
        api = FirstBookAuthenticationAPI(
            firstbook_fixture.db.default_library(), integration
        )

        # Verify that the configuration details were stored properly.
        assert "http://example.com/?key=the_key" == api.root

        # Test the default server-side authentication regular expressions.
        assert False == api.server_side_validation("foo' or 1=1 --;", "1234")
        assert False == api.server_side_validation("foo", "12 34")
        assert True == api.server_side_validation("foo", "1234")
        assert True == api.server_side_validation("foo@bar", "1234")

        # Try another case where the root URL has multiple arguments.
        integration.url = "http://example.com/?foo=bar"
        api = FirstBookAuthenticationAPI(
            firstbook_fixture.db.default_library(), integration
        )
        assert "http://example.com/?foo=bar&key=the_key" == api.root

    def test_authentication_success(self, firstbook_fixture: FirstBookFixture):
        assert True == firstbook_fixture.api.remote_pin_test("ABCD", "1234")

    def test_authentication_failure(self, firstbook_fixture: FirstBookFixture):
        assert False == firstbook_fixture.api.remote_pin_test("ABCD", "9999")
        assert False == firstbook_fixture.api.remote_pin_test("nosuchkey", "9999")

        # credentials are uppercased in remote_authenticate;
        # remote_pin_test just passes on whatever it's sent.
        assert False == firstbook_fixture.api.remote_pin_test("abcd", "9999")

    def test_remote_authenticate(self, firstbook_fixture: FirstBookFixture):
        patrondata = firstbook_fixture.api.remote_authenticate("abcd", "1234")
        assert "ABCD" == patrondata.permanent_id
        assert "ABCD" == patrondata.authorization_identifier
        assert None == patrondata.username

        patrondata = firstbook_fixture.api.remote_authenticate("ABCD", "1234")
        assert "ABCD" == patrondata.permanent_id
        assert "ABCD" == patrondata.authorization_identifier
        assert None == patrondata.username

    def test_broken_service_remote_pin_test(self, firstbook_fixture: FirstBookFixture):
        api = firstbook_fixture.mock_api(failure_status_code=502)
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            api.remote_pin_test("key", "pin")
        assert "Got unexpected response code 502. Content: Error 502" in str(
            excinfo.value
        )

    def test_bad_connection_remote_pin_test(self, firstbook_fixture: FirstBookFixture):
        api = firstbook_fixture.mock_api(bad_connection=True)
        with pytest.raises(RemoteInitiatedServerError) as excinfo:
            api.remote_pin_test("key", "pin")
        assert "Could not connect!" in str(excinfo.value)

    def test_authentication_flow_document(self, firstbook_fixture: FirstBookFixture):
        # We're about to call url_for, so we must create an
        # application context.
        os.environ["AUTOINITIALIZE"] = "False"
        from palace.api.app import app

        self.app = app
        del os.environ["AUTOINITIALIZE"]
        with self.app.test_request_context("/"):
            doc = firstbook_fixture.api.authentication_flow_document(
                firstbook_fixture.db.session
            )
            assert firstbook_fixture.api.DISPLAY_NAME == doc["description"]
            assert firstbook_fixture.api.FLOW_TYPE == doc["type"]

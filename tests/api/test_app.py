from api.app import initialize_application
from core.util.http import HTTP
from tests.fixtures.database import DatabaseTransactionFixture


def test_initialize_application_http(db: DatabaseTransactionFixture):
    # Use the db transaction fixture so that we don't use the production settings by mistake
    assert HTTP.DEFAULT_REQUEST_RETRIES == 5
    assert HTTP.DEFAULT_REQUEST_TIMEOUT == 20
    # Initialize the app, which will set the HTTP configuration
    initialize_application()
    # Now we have quick request failure setup
    assert HTTP.DEFAULT_REQUEST_RETRIES == 0
    assert HTTP.DEFAULT_REQUEST_TIMEOUT == 5

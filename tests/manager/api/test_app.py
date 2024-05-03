from unittest.mock import patch

from palace.manager.api.app import initialize_application
from palace.manager.util.http import HTTP
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


def test_initialize_application_http(
    db: DatabaseTransactionFixture, services_fixture_wired: ServicesFixture
):
    # Use the db transaction fixture so that we don't use the production settings by mistake
    with (
        patch.object(
            HTTP, "set_quick_failure_settings"
        ) as mock_set_quick_failure_settings,
        patch("palace.manager.api.app.initialize_database"),
        patch("palace.manager.api.app.initialize_circulation_manager"),
        patch("palace.manager.api.app.app"),
    ):
        # Initialize the app, which will set the HTTP configuration
        initialize_application()

    # Make sure that the HTTP configuration was set
    mock_set_quick_failure_settings.assert_called_once()

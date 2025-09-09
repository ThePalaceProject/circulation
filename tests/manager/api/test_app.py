from unittest.mock import MagicMock, patch

import pytest

from palace.manager.api.app import (
    initialize_application,
    initialize_circulation_manager,
)
from palace.manager.util.http.http import HTTP
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


def test_initialize_application_http(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
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


def test_initialize_circulation_manager(caplog: pytest.LogCaptureFixture):
    with (
        patch("palace.manager.api.app.app") as mock_app,
        patch("palace.manager.api.app.CirculationManager") as mock_circulation_manager,
        patch("palace.manager.api.app.CachedData") as mock_cached_data,
    ):
        # If app is already initialized, it should not be re-initialized
        mock_app.manager = MagicMock()
        initialize_circulation_manager()
        mock_circulation_manager.assert_not_called()
        mock_cached_data.initialize.assert_not_called()

        # If app is not initialized, it should be initialized
        mock_app.manager = None
        initialize_circulation_manager()
        mock_circulation_manager.assert_called_once()
        mock_cached_data.initialize.assert_called_once()
        assert mock_app.manager == mock_circulation_manager.return_value

        # If an exception is raised, it should be logged
        mock_app.manager = None
        mock_circulation_manager.side_effect = Exception("Test exception")
        with pytest.raises(Exception):
            initialize_circulation_manager()
        assert "Error instantiating circulation manager!" in caplog.text

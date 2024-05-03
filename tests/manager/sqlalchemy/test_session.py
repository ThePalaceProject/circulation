from unittest.mock import patch

from palace.manager.core.config import Configuration
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.session import (
    SessionManager,
    json_serializer,
    production_session,
)
from palace.manager.sqlalchemy.util import get_one
from tests.fixtures.database import DatabaseTransactionFixture


class TestSessionManager:
    def test_initialize_data_does_not_reset_timestamp(
        self, db: DatabaseTransactionFixture
    ):
        # initialize_data() has already been called, so the database is
        # initialized and the 'site configuration changed' Timestamp has
        # been set. Calling initialize_data() again won't change the
        # date on the timestamp.
        timestamp = get_one(
            db.session,
            Timestamp,
            collection=None,
            service=Configuration.SITE_CONFIGURATION_CHANGED,
        )
        assert timestamp is not None
        old_timestamp = timestamp.finish
        SessionManager.initialize_data(db.session)
        assert old_timestamp == timestamp.finish

    @patch("palace.manager.sqlalchemy.session.create_engine")
    @patch.object(Configuration, "database_url")
    def test_engine(self, mock_database_url, mock_create_engine):
        expected_args = {
            "echo": False,
            "json_serializer": json_serializer,
            "pool_pre_ping": True,
            "poolclass": None,
        }

        # If a URL is passed in, it's used.
        SessionManager.engine("url")
        mock_database_url.assert_not_called()
        mock_create_engine.assert_called_once_with("url", **expected_args)
        mock_create_engine.reset_mock()

        # If no URL is passed in, the URL from the configuration is used.
        SessionManager.engine()
        mock_database_url.assert_called_once()
        mock_create_engine.assert_called_once_with(
            mock_database_url.return_value, **expected_args
        )

    @patch.object(SessionManager, "engine")
    @patch.object(SessionManager, "session_from_connection")
    def test_session(self, mock_session_from_connection, mock_engine):
        session = SessionManager.session("test-url")
        mock_engine.assert_called_once_with("test-url")
        mock_engine.return_value.connect.assert_called_once()
        mock_session_from_connection.assert_called_once_with(
            mock_engine.return_value.connect.return_value
        )
        assert session == mock_session_from_connection.return_value


@patch.object(SessionManager, "session")
@patch.object(Configuration, "database_url")
def test_production_session(mock_database_url, mock_session):
    # Make sure production_session() calls session() with the URL from the
    # configuration.
    mock_database_url.return_value = "test-url"
    session = production_session()
    mock_database_url.assert_called_once()
    mock_session.assert_called_once_with("test-url", initialize_data=True)
    assert session == mock_session.return_value

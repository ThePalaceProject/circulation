import pytest
from sqlalchemy.exc import ArgumentError

from palace.manager.core.config import CannotLoadConfiguration, Configuration
from palace.manager.service.logging.configuration import LogLevel


class TestConfiguration:
    def test_database_url(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ):
        # If the environment variable is not set, an exception is raised.
        monkeypatch.delenv(
            Configuration.DATABASE_PRODUCTION_ENVIRONMENT_VARIABLE, raising=False
        )
        with pytest.raises(CannotLoadConfiguration) as exc1:
            Configuration.database_url()
        assert "Database URL was not defined" in str(exc1.value)

        # If the URL is not in the expected format, an exception is raised.
        monkeypatch.setenv(
            Configuration.DATABASE_PRODUCTION_ENVIRONMENT_VARIABLE, "bad-url"
        )
        with pytest.raises(ArgumentError) as exc2:
            Configuration.database_url()
        assert "Bad format for database URL" in str(exc2.value)

        # Make sure database_url() returns the expected URL.
        caplog.set_level(LogLevel.info)
        expected_url = "postgresql://user:pass@palaceproject.io:1234/test_db"
        monkeypatch.setenv(
            Configuration.DATABASE_PRODUCTION_ENVIRONMENT_VARIABLE, expected_url
        )
        assert Configuration.database_url() == expected_url
        assert "Connecting to database" in caplog.text

        # Make sure the password is hidden in the log.
        assert "pass" not in caplog.text

        # But the rest of the URL is visible.
        assert expected_url.replace("pass", "***") in caplog.text

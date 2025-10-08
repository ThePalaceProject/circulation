import pytest

from palace.manager.util.problem_detail import ProblemDetailException
from tests.fixtures.database import DatabaseTransactionFixture


class TestOPDS2ImporterSettings:
    def test__validate_reap_schedule_valid_expressions(self) -> None:
        """Test that valid cron expressions are accepted."""
        opds2_settings = DatabaseTransactionFixture.opds2_settings

        # Standard cron expression (daily at midnight)
        settings = opds2_settings(
            reap_schedule="0 0 * * *",
        )
        assert settings.reap_schedule == "0 0 * * *"

        # Weekly on Monday
        settings = opds2_settings(
            reap_schedule="0 0 * * 1",
        )
        assert settings.reap_schedule == "0 0 * * 1"

        # Every 6 hours
        settings = opds2_settings(
            reap_schedule="0 */6 * * *",
        )
        assert settings.reap_schedule == "0 */6 * * *"

    def test__validate_reap_schedule_none_and_empty_string_accepted(self) -> None:
        """Test that None and empty strings are accepted (no reaping)."""
        opds2_settings = DatabaseTransactionFixture.opds2_settings

        # None is accepted
        settings = opds2_settings(
            reap_schedule=None,
        )
        assert settings.reap_schedule is None

        # Empty string is converted to None
        settings = opds2_settings(
            reap_schedule="",
        )
        assert settings.reap_schedule is None

        # Whitespace-only string is converted to None
        settings = opds2_settings(
            reap_schedule="   ",
        )
        assert settings.reap_schedule is None

    def test__validate_reap_schedule_invalid_cron_expressions(self) -> None:
        """Test that invalid cron expressions raise ProblemDetailException with helpful messages."""
        opds2_settings = DatabaseTransactionFixture.opds2_settings

        # Too few fields
        with pytest.raises(ProblemDetailException) as exc_info:
            opds2_settings(
                reap_schedule="0 0 *",
            )
        error_detail = exc_info.value.problem_detail.detail
        assert error_detail is not None
        assert "Invalid cron expression" in error_detail
        assert "0 0 *" in error_detail

        # Invalid field value
        with pytest.raises(ProblemDetailException) as exc_info:
            opds2_settings(
                reap_schedule="0 0 * * 8",  # 8 is not a valid day of week
            )
        error_detail = exc_info.value.problem_detail.detail
        assert error_detail is not None
        assert "Invalid cron expression" in error_detail

        # Completely invalid format
        with pytest.raises(ProblemDetailException) as exc_info:
            opds2_settings(
                reap_schedule="not a cron expression",
            )
        error_detail = exc_info.value.problem_detail.detail
        assert error_detail is not None
        assert "Invalid cron expression" in error_detail
        assert "not a cron expression" in error_detail

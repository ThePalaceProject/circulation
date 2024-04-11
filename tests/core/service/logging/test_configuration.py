import pytest

from core.config import CannotLoadConfiguration
from core.service.logging.configuration import LoggingConfiguration, LogLevel


def test_cloudwatch_region_none() -> None:
    # If cloudwatch is not enabled, no validation is needed.
    config = LoggingConfiguration(cloudwatch_enabled=False, cloudwatch_region=None)
    assert config.cloudwatch_region is None
    assert config.cloudwatch_enabled is False

    # If cloudwatch is enabled, region must be provided.
    with pytest.raises(CannotLoadConfiguration) as execinfo:
        LoggingConfiguration(cloudwatch_enabled=True, cloudwatch_region=None)

    assert "Region must be provided if cloudwatch is enabled." in str(execinfo.value)


def test_cloudwatch_region_invalid() -> None:
    with pytest.raises(CannotLoadConfiguration) as execinfo:
        LoggingConfiguration(cloudwatch_enabled=True, cloudwatch_region="invalid")

    assert "Invalid region: invalid. Region must be one of:" in str(execinfo.value)


def test_cloudwatch_region_valid() -> None:
    config = LoggingConfiguration(
        cloudwatch_enabled=True, cloudwatch_region="us-east-2"
    )
    assert config.cloudwatch_region == "us-east-2"
    assert config.cloudwatch_enabled is True


class TestLogLevel:
    def test_level_string(self) -> None:
        assert LogLevel.debug == "DEBUG"
        assert LogLevel.info == "INFO"  # type: ignore[unreachable]
        assert LogLevel.warning == "WARNING"
        assert LogLevel.error == "ERROR"

    def test_levelno(self) -> None:
        assert LogLevel.debug.levelno == 10
        assert LogLevel.info.levelno == 20
        assert LogLevel.warning.levelno == 30
        assert LogLevel.error.levelno == 40

    @pytest.mark.parametrize(
        "level, expected",
        [
            (10, LogLevel.debug),
            ("debug", LogLevel.debug),
            ("DEBUG", LogLevel.debug),
            ("info", LogLevel.info),
            ("INFO", LogLevel.info),
            (20, LogLevel.info),
        ],
    )
    def test_from_level(self, level: int | str, expected: LogLevel) -> None:
        assert LogLevel.from_level(level) == expected

    @pytest.mark.parametrize(
        "level",
        [
            "invalid",
            "INVALID",
            999,
            41,
            -1,
            None,
        ],
    )
    def test_from_level_invalid(self, level: str | int | None) -> None:
        with pytest.raises(ValueError) as execinfo:
            LogLevel.from_level(level)  # type: ignore[arg-type]

        assert f"'{level}' is not a valid LogLevel" in str(execinfo.value)

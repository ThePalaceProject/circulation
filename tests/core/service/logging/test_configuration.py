import pytest

from core.config import CannotLoadConfiguration
from core.service.logging.configuration import LoggingConfiguration


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

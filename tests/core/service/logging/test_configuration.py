import pytest

from core.config import CannotLoadConfiguration
from core.service.logging.configuration import LoggingConfiguration


def test_cloudwatch_region_none():
    # If cloudwatch is not enabled, no validation is needed.
    config = LoggingConfiguration(cloudwatch=False, cloudwatch_region=None)
    assert config.cloudwatch_region is None
    assert config.cloudwatch is False

    # If cloudwatch is enabled, region must be provided.
    with pytest.raises(CannotLoadConfiguration):
        LoggingConfiguration(cloudwatch=True, cloudwatch_region=None)


def test_cloudwatch_region_invalid():
    with pytest.raises(CannotLoadConfiguration):
        LoggingConfiguration(cloudwatch=True, cloudwatch_region="invalid")


def test_cloudwatch_region_valid():
    config = LoggingConfiguration(cloudwatch=True, cloudwatch_region="us-east-2")
    assert config.cloudwatch_region == "us-east-2"
    assert config.cloudwatch is True

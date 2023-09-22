import pytest

from core.config import CannotLoadConfiguration
from core.service.storage.configuration import StorageConfiguration


def test_region_validation_fail():
    with pytest.raises(CannotLoadConfiguration) as exc_info:
        StorageConfiguration(region="foo bar baz")

    assert "PALACE_STORAGE_REGION:  Invalid region: foo bar baz." in str(exc_info.value)


def test_region_validation_success():
    configuration = StorageConfiguration(region="us-west-2")
    assert configuration.region == "us-west-2"

    configuration = StorageConfiguration(region=None)
    assert configuration.region is None


@pytest.mark.parametrize(
    "url",
    [
        "http://localhost:9000",
        "https://real.endpoint.com",
        "http://192.168.0.1",
    ],
)
def test_endpoint_url_validation_success(url: str):
    configuration = StorageConfiguration(endpoint_url=url)
    assert configuration.endpoint_url == url


@pytest.mark.parametrize(
    "url, error",
    [
        ("ftp://localhost:9000", "URL scheme not permitted"),
        ("foo bar baz", "invalid or missing URL scheme"),
    ],
)
def test_endpoint_url_validation_fail(url: str, error: str):
    with pytest.raises(CannotLoadConfiguration) as exc_info:
        StorageConfiguration(endpoint_url=url)

    assert "PALACE_STORAGE_ENDPOINT_URL" in str(exc_info.value)
    assert error in str(exc_info.value)

import json
from pathlib import Path

import pytest

from core.config import CannotLoadConfiguration
from core.service.fcm.fcm import credentials
from tests.fixtures.files import FilesFixture


@pytest.fixture()
def fcm_files_fixture() -> FilesFixture:
    """Provides access to fcm test files."""
    return FilesFixture("service/fcm")


def test_fcm_credentials(fcm_files_fixture: FilesFixture):
    invalid_json = "{ this is invalid JSON }"
    valid_credentials_json = fcm_files_fixture.sample_text(
        "fcm-credentials-valid-json.json"
    )
    valid_credentials_object = json.loads(valid_credentials_json)

    # No FCM credentials set
    with pytest.raises(
        CannotLoadConfiguration,
        match=r"FCM Credentials configuration environment variable not defined.",
    ):
        credentials(None, None)

    # Non-existent file.
    with pytest.raises(
        CannotLoadConfiguration,
        match=r"The FCM credentials file .* does not exist.",
    ):
        credentials(Path("filedoesnotexist.deleteifitdoes"), None)

    # Valid JSON file.
    fcm_file = Path(fcm_files_fixture.sample_path("fcm-credentials-valid-json.json"))
    assert valid_credentials_object == credentials(fcm_file, None)

    # Setting more than one FCM credentials environment variable is not valid.
    with pytest.raises(
        CannotLoadConfiguration,
        match=r"Both JSON .* and file-based .* FCM Credential environment variables are defined, but only one is allowed.",
    ):
        credentials(fcm_file, valid_credentials_json)

    # Down to just the JSON FCM credentials environment variable.
    assert valid_credentials_object == credentials(None, valid_credentials_json)

    # But we should get an exception if the JSON is invalid.
    with pytest.raises(
        CannotLoadConfiguration,
        match=r"Cannot parse value of FCM credential environment variable .* as JSON.",
    ):
        credentials(None, invalid_json)

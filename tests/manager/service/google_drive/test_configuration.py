import tempfile

import pytest

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.service.google_drive.configuration import GoogleDriveConfiguration


def test_invalid_service_account_path_validation_fail():
    with pytest.raises(CannotLoadConfiguration) as exc_info:
        GoogleDriveConfiguration(service_account_key_file_path="blah")

    assert (
        "PALACE_GOOGLE_DRIVE_SERVICE_ACCOUNT_KEY_FILE_PATH:  Value error, "
        "file does not exist: service_account_key_file_path: blah"
        in str(exc_info.value)
    )


def test_service_account_path_validation_success():
    temp_file = tempfile.NamedTemporaryFile()
    configuration = GoogleDriveConfiguration(
        service_account_key_file_path=temp_file.name
    )
    assert configuration.service_account_key_file_path == temp_file.name

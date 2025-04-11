import json

import pytest

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.service.google_drive.configuration import GoogleDriveConfiguration


def test_invalid_service_account_path_validation_fail():
    with pytest.raises(
        CannotLoadConfiguration,
        match=(
            "PALACE_GOOGLE_DRIVE_SERVICE_ACCOUNT_INFO_JSON:  Value error, "
            "Unable to parse service_account_info_json: 'blah'"
        ),
    ) as exc_info:
        GoogleDriveConfiguration(service_account_info_json="blah")


def test_service_account_path_validation_success():
    service_account_info_json = {"abc": "123"}

    # Can load from a json string
    configuration = GoogleDriveConfiguration(
        service_account_info_json=json.dumps(service_account_info_json)
    )
    assert configuration.service_account_info_json == service_account_info_json

    # Or from a dict
    configuration = GoogleDriveConfiguration(
        service_account_info_json=service_account_info_json
    )
    assert configuration.service_account_info_json == service_account_info_json

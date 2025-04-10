import json

from pydantic import field_validator
from pydantic_settings import SettingsConfigDict

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)

# Environment variable specifying the google drive parent folder ID for this environment.
# This value may be a folder ID or a shared drive ID. For example, the google drive folder URI will look like
# https://drive.google.com/drive/u/1/folders/0AGtlKYStJaC3Uk9PVZ .   "0AGtlKYStJaC3Uk9PVZ" (not a real
# folder ID) is the value that should be assigned environment variable.
PALACE_GOOGLE_DRIVE_PARENT_FOLDER_ID_ENVIRONMENT_VARIABLE = (
    "PALACE_GOOGLE_DRIVE_PARENT_FOLDER_ID"
)


class GoogleDriveConfiguration(ServiceConfiguration):

    service_account_info_json: str | None = None

    @field_validator("service_account_info_json")
    @classmethod
    def validate_service_account_info_json(cls, v: str | None) -> str | None:
        # No validation if service_account_info_json is not provided.
        if v is None:
            return None

        try:
            json.loads(v)
        except:
            raise ValueError(f"Unable to parse service_account_info_json: {v}")
        return v

    model_config = SettingsConfigDict(env_prefix="PALACE_GOOGLE_DRIVE_")

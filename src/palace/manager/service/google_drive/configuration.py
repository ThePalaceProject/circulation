import json

from pydantic import field_validator
from pydantic_settings import SettingsConfigDict

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)

# Environment variable specifying the google drive root folder for this environment
PALACE_GOOGLE_DRIVE_ROOT_ENVIRONMENT_VARIABLE = "PALACE_GOOGLE_DRIVE_ROOT"


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

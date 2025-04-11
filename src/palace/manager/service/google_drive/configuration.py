import json
from typing import Any

from pydantic import field_validator
from pydantic_settings import SettingsConfigDict

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)


class GoogleDriveConfiguration(ServiceConfiguration):

    # Environment variable specifying the google drive parent folder ID for this environment.
    # This value may be a folder ID or a shared drive ID. For example, the google drive folder URI will look like
    # https://drive.google.com/drive/u/1/folders/0AGtlKYStJaC3Uk9PVZ .   "0AGtlKYStJaC3Uk9PVZ" (not a real
    # folder ID) is the value that should be assigned environment variable.
    parent_folder_id: str | None = None
    service_account_info_json: dict[str, Any] | None = None

    @field_validator("service_account_info_json", mode="before")
    @classmethod
    def validate_service_account_info_json(cls, v: Any) -> Any:
        # No validation if service_account_info_json is not provided.
        if isinstance(v, (str, bytes)):
            # If it's a string, try to parse it as JSON
            try:
                return json.loads(v)
            except:
                raise ValueError(f"Unable to parse service_account_info_json: {v!r}")

        return v

    model_config = SettingsConfigDict(env_prefix="PALACE_GOOGLE_DRIVE_")

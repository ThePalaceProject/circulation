import pathlib

from pydantic import field_validator
from pydantic_settings import SettingsConfigDict

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)


class GoogleDriveConfiguration(ServiceConfiguration):
    service_account_key_file_path: str | None = None

    @field_validator("service_account_key_file_path")
    @classmethod
    def validate_region(cls, v: str | None) -> str | None:
        # No validation if service_account_key_file_path is not provided.
        if v is None:
            return None

        if not pathlib.Path(v).is_file():
            raise ValueError(f"file does not exist: service_account_key_file_path: {v}")
        return v

    model_config = SettingsConfigDict(env_prefix="PALACE_GOOGLE_DRIVE_")

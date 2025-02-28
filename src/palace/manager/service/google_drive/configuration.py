from pydantic_settings import SettingsConfigDict

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)


class GoogleDriveConfiguration(ServiceConfiguration):
    service_account_key_file_path: str | None = None

    model_config = SettingsConfigDict(env_prefix="PALACE_GOOGLE_DRIVE_")

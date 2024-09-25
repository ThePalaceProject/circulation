from pathlib import Path

from pydantic_settings import SettingsConfigDict

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)


class FcmConfiguration(ServiceConfiguration):
    model_config = SettingsConfigDict(env_prefix="PALACE_FCM_")

    credentials_json: str | None = None
    credentials_file: Path | None = None

    @classmethod
    def credentials_json_env_var(cls) -> str:
        return f"{cls.model_config.get('env_prefix') or ''}CREDENTIALS_JSON"

    @classmethod
    def credentials_file_env_var(cls) -> str:
        return f"{cls.model_config.get('env_prefix') or ''}CREDENTIALS_FILE"

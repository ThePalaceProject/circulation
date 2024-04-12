from pathlib import Path

from core.service.configuration import ServiceConfiguration


class FcmConfiguration(ServiceConfiguration):
    class Config:
        env_prefix = "PALACE_FCM_"

    credentials_json: str | None = None
    credentials_file: Path | None = None

    @classmethod
    def credentials_json_env_var(cls) -> str:
        return f"{cls.__config__.env_prefix}CREDENTIALS_JSON"

    @classmethod
    def credentials_file_env_var(cls) -> str:
        return f"{cls.__config__.env_prefix}CREDENTIALS_FILE"

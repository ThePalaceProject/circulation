import json
from pathlib import Path

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.service.fcm.configuration import FcmConfiguration


def credentials(config_file: Path | None, config_json: str | None) -> dict[str, str]:
    """Returns a dictionary containing Firebase Cloud Messaging credentials.

    Credentials are provided as a JSON string, either (1) directly in an environment
    variable or (2) in a file that is specified in another environment variable.
    """
    if config_json and config_file:
        raise CannotLoadConfiguration(
            f"Both JSON ('{FcmConfiguration.credentials_json_env_var()}') "
            f"and file-based ('{FcmConfiguration.credentials_file_env_var()}') "
            "FCM Credential environment variables are defined, but only one is allowed."
        )

    if config_json:
        try:
            return json.loads(config_json, strict=False)  # type: ignore[no-any-return]
        except:
            raise CannotLoadConfiguration(
                "Cannot parse value of FCM credential environment variable "
                f"'{FcmConfiguration.credentials_json_env_var()}' as JSON."
            )

    if config_file:
        if not config_file.exists():
            raise CannotLoadConfiguration(
                f"The FCM credentials file ('{config_file}') does not exist."
            )
        with config_file.open("r") as f:
            try:
                return json.load(f)  # type: ignore[no-any-return]
            except:
                raise CannotLoadConfiguration(
                    f"Cannot parse contents of FCM credentials file ('{config_file}') as JSON."
                )

    # If we get here, neither the JSON nor the file-based configuration was provided.
    raise CannotLoadConfiguration(
        "FCM Credentials configuration environment variable not defined. "
        f"Use either '{FcmConfiguration.credentials_json_env_var()}' "
        f"or '{FcmConfiguration.credentials_file_env_var()}'."
    )

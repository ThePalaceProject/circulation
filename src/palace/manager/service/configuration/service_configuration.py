from __future__ import annotations

from typing import Any

from pydantic import ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from palace.manager.core.config import CannotLoadConfiguration


class ServiceConfiguration(BaseSettings):
    """
    Base class for our service configuration. Each subclass should define its own
    configuration settings as pydantic fields. The settings will be loaded from
    environment variables with the prefix defined in the Config class.

    The env_prefix should also be overridden in subclasses to provide a unique prefix
    for each service.
    """

    model_config = SettingsConfigDict(
        # Each sub-config will have its own prefix
        env_prefix="PALACE_",
        # Strip whitespace from all strings
        str_strip_whitespace=True,
        # Forbid mutation, settings should be loaded once from environment.
        frozen=True,
        # Allow env vars to be loaded from a .env file
        # This loads the .env file from the root of the project
        env_file=".env",
        # Nested settings will be loaded from environment variables with this delimiter.
        env_nested_delimiter="__",
        # Ignore extra fields in the environment
        extra="ignore",
    )

    def __init__(self, *args: Any, **kwargs: Any):
        try:
            super().__init__(*args, **kwargs)
        except ValidationError as error_exception:
            # The services settings failed to validate, we capture the ValidationError and
            # raise a more specific CannotLoadConfiguration error.
            errors = error_exception.errors()
            error_log_message = f"Error loading settings from environment:"
            for error in errors:
                delimiter = self.model_config.get("env_nested_delimiter") or "__"
                pydantic_location = error["loc"]
                if pydantic_location:
                    first_error_location = str(pydantic_location[0])
                    env_var = (
                        f"{self.model_config.get('env_prefix')}{first_error_location.upper()}"
                        if self.model_fields.get(first_error_location)
                        else first_error_location.upper()
                    )
                    location = delimiter.join(
                        str(e).upper() for e in (env_var, *pydantic_location[1:])
                    )
                    error_log_message += f"\n  {location}:  {error['msg']}"
                else:
                    error_log_message += f"\n  {error['msg']}"
            raise CannotLoadConfiguration(error_log_message) from error_exception

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseSettings, ValidationError

from core.config import CannotLoadConfiguration


class ServiceConfiguration(BaseSettings):
    """
    Base class for our service configuration. Each subclass should define its own
    configuration settings as pydantic fields. The settings will be loaded from
    environment variables with the prefix defined in the Config class.

    The env_prefix should also be overridden in subclasses to provide a unique prefix
    for each service.
    """

    class Config:
        # See the pydantic docs for information on these settings
        # https://docs.pydantic.dev/usage/model_config/

        # Each sub-config will have its own prefix
        env_prefix = "PALACE_"

        # Strip whitespace from all strings
        anystr_strip_whitespace = True

        # Forbid mutation, settings should be loaded once from environment.
        allow_mutation = False

        # Allow env vars to be loaded from a .env file
        # This loads the .env file from the root of the project
        env_file = str(Path(__file__).parent.parent.parent.absolute() / ".env")

        # Nested settings will be loaded from environment variables with this delimiter.
        env_nested_delimiter = "__"

    def __init__(self, *args: Any, **kwargs: Any):
        try:
            super().__init__(*args, **kwargs)
        except ValidationError as error_exception:
            # The services settings failed to validate, we capture the ValidationError and
            # raise a more specific CannotLoadConfiguration error.
            errors = error_exception.errors()
            error_log_message = f"Error loading settings from environment:"
            for error in errors:
                delimiter = self.__config__.env_nested_delimiter or "__"
                error_location = delimiter.join(str(e).upper() for e in error["loc"])
                env_var_name = f"{self.__config__.env_prefix}{error_location}"
                error_log_message += f"\n  {env_var_name}:  {error['msg']}"
            raise CannotLoadConfiguration(error_log_message) from error_exception

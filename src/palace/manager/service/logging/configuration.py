from __future__ import annotations

import logging
from enum import StrEnum, auto

import boto3
from pydantic import NonNegativeInt, PositiveInt, field_validator
from pydantic_core.core_schema import ValidationInfo
from pydantic_settings import SettingsConfigDict
from watchtower import DEFAULT_LOG_STREAM_NAME

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)


class LogLevel(StrEnum):
    """
    A helper class to represent log levels as an Enum.

    Since the logging module uses strings to represent log levels, the members of
    this enum can be passed directly to the logging module to set the log level.
    """

    @staticmethod
    def _generate_next_value_(
        name: str, start: int, count: int, last_values: list[str]
    ) -> str:
        """
        Return the upper-cased version of the member name.

        By default, StrEnum uses the lower-cased version of the member name as the value,
        but to match the logging module, we want to use the upper-cased version, so
        we override this method to make auto() generate the correct value.
        """
        return name.upper()

    debug = auto()
    info = auto()
    warning = auto()
    error = auto()

    @property
    def levelno(self) -> int:
        """
        Return the integer value used by the logging module for this log level.
        """
        return logging._nameToLevel[self.value]

    @classmethod
    def from_level(cls, level: int | str) -> LogLevel:
        """
        Get a member of this enum from a string or integer log level.
        """
        if isinstance(level, int):
            parsed_level = logging.getLevelName(level)
        else:
            parsed_level = str(level).upper()

        try:
            return cls(parsed_level)
        except ValueError:
            raise ValueError(f"'{level}' is not a valid LogLevel") from None


class LoggingConfiguration(ServiceConfiguration):
    level: LogLevel = LogLevel.info
    verbose_level: LogLevel = LogLevel.warning

    # This config is useful when debugging, it will print a traceback to stderr
    # at the specified interval (in seconds). It should not be enabled in normal
    # production operation. The default value of 0 disables this feature.
    debug_traceback_interval: NonNegativeInt = 0

    cloudwatch_enabled: bool = False
    cloudwatch_region: str | None = None
    cloudwatch_group: str = "palace"
    cloudwatch_stream: str = DEFAULT_LOG_STREAM_NAME
    cloudwatch_interval: PositiveInt = 60
    cloudwatch_create_group: bool = True
    cloudwatch_access_key: str | None = None
    cloudwatch_secret_key: str | None = None

    @field_validator("cloudwatch_region")
    @classmethod
    def validate_cloudwatch_region(
        cls, v: str | None, info: ValidationInfo
    ) -> str | None:
        if not info.data.get("cloudwatch_enabled"):
            # If cloudwatch is not enabled, no validation is needed.
            return None

        if v is None:
            raise ValueError(f"Region must be provided if cloudwatch is enabled.")

        session = boto3.session.Session()
        regions = session.get_available_regions(service_name="logs")
        if v not in regions:
            raise ValueError(
                f"Invalid region: {v}. Region must be one of: {' ,'.join(regions)}."
            )
        return v

    model_config = SettingsConfigDict(env_prefix="PALACE_LOG_")

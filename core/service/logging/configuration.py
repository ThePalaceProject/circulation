from enum import Enum
from typing import Any

import boto3
from pydantic import PositiveInt, validator
from watchtower import DEFAULT_LOG_STREAM_NAME

from core.service.configuration import ServiceConfiguration


class LogLevel(Enum):
    debug = "DEBUG"
    info = "INFO"
    warning = "WARNING"
    error = "ERROR"


class LoggingConfiguration(ServiceConfiguration):
    level: LogLevel = LogLevel.info
    verbose_level: LogLevel = LogLevel.warning

    cloudwatch_enabled: bool = False
    cloudwatch_region: str | None = None
    cloudwatch_group: str = "palace"
    cloudwatch_stream: str = DEFAULT_LOG_STREAM_NAME
    cloudwatch_interval: PositiveInt = 60
    cloudwatch_create_group: bool = True
    cloudwatch_access_key: str | None = None
    cloudwatch_secret_key: str | None = None

    @validator("cloudwatch_region")
    def validate_cloudwatch_region(
        cls, v: str | None, values: dict[str, Any]
    ) -> str | None:
        if not values.get("cloudwatch_enabled"):
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

    class Config:
        env_prefix = "PALACE_LOG_"

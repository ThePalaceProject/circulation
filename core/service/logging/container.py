from logging import Handler
from typing import TYPE_CHECKING, Optional

import boto3
from dependency_injector import providers
from dependency_injector.containers import DeclarativeContainer
from dependency_injector.providers import Provider, Singleton

from core.service.logging.log import (
    JSONFormatter,
    create_cloudwatch_handler,
    create_stream_handler,
    setup_logging,
)

if TYPE_CHECKING:
    from mypy_boto3_logs import CloudWatchLogsClient


class Logging(DeclarativeContainer):
    config = providers.Configuration()

    cloudwatch_client: Provider[CloudWatchLogsClient] = Singleton(
        boto3.client,
        service_name="logs",
        aws_access_key_id=config.cloudwatch_access_key,
        aws_secret_access_key=config.cloudwatch_secret_key,
        region_name=config.cloudwatch_region,
    )

    json_formatter: Provider[JSONFormatter] = Singleton(JSONFormatter)

    cloudwatch_handler: Provider[Optional[Handler]] = providers.Singleton(
        create_cloudwatch_handler,
        create=config.cloudwatch,
        formatter=json_formatter,
        level=config.level,
        client=cloudwatch_client.provider,
        group=config.cloudwatch_group,
        interval=config.cloudwatch_interval,
        create_group=config.cloudwatch_create_group,
    )

    stream_handler: Provider[Handler] = providers.Singleton(
        create_stream_handler, formatter=json_formatter, level=config.level
    )

    logging = providers.Resource(
        setup_logging,
        level=config.level,
        verbose_level=config.verbose_level,
        stream=stream_handler,
        cloudwatch=cloudwatch_handler,
    )
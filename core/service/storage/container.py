from typing import Optional

import boto3
from dependency_injector import providers
from dependency_injector.containers import DeclarativeContainer
from dependency_injector.providers import Provider, Singleton
from mypy_boto3_s3 import S3Client

from core.service.storage.s3 import S3Service


class Storage(DeclarativeContainer):
    config = providers.Configuration()

    s3_client: Provider[S3Client] = Singleton(
        boto3.client,
        service_name="s3",
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        region_name=config.region,
        endpoint_url=config.endpoint_url,
    )

    analytics: Provider[Optional[S3Service]] = providers.Singleton(
        S3Service.factory,
        client=s3_client,
        region=config.region,
        bucket=config.analytics_bucket,
        url_template=config.url_template,
    )

    public: Provider[Optional[S3Service]] = providers.Singleton(
        S3Service.factory,
        client=s3_client,
        region=config.region,
        bucket=config.public_access_bucket,
        url_template=config.url_template,
    )

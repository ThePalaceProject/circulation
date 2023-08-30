import boto3
from dependency_injector import providers
from dependency_injector.containers import DeclarativeContainer

from core.service.storage.s3 import S3Service


class Storage(DeclarativeContainer):
    config = providers.Configuration()

    s3_client = providers.Singleton(
        boto3.client,
        service_name="s3",
        aws_access_key_id=config.access_key,
        aws_secret_access_key=config.secret_key,
        region_name=config.region,
        endpoint_url=config.endpoint_url,
    )

    analytics = providers.Singleton(
        S3Service.factory,
        client=s3_client,
        region=config.region,
        bucket=config.analytics_bucket,
        url_template=config.url_template,
    )

    public = providers.Singleton(
        S3Service.factory,
        client=s3_client,
        region=config.region,
        bucket=config.public_access_bucket,
        url_template=config.url_template,
    )

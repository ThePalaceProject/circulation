import boto3
from pydantic import field_validator
from pydantic_settings import SettingsConfigDict

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)
from palace.manager.util.pydantic import HttpUrl


class StorageConfiguration(ServiceConfiguration):
    region: str | None = None
    access_key: str | None = None
    secret_key: str | None = None

    public_access_bucket: str | None = None
    analytics_bucket: str | None = None

    endpoint_url: HttpUrl | None = None

    url_template: str = "https://{bucket}.s3.{region}.amazonaws.com/{key}"

    @field_validator("region")
    @classmethod
    def validate_region(cls, v: str | None) -> str | None:
        # No validation if region is not provided.
        if v is None:
            return None

        session = boto3.session.Session()
        regions = session.get_available_regions(service_name="s3")
        if v not in regions:
            raise ValueError(
                f"Invalid region: {v}. Region must be one of: {' ,'.join(regions)}."
            )
        return v

    model_config = SettingsConfigDict(env_prefix="PALACE_STORAGE_")

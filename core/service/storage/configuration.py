import boto3
from pydantic import AnyHttpUrl, parse_obj_as, validator

from core.service.configuration import ServiceConfiguration


class StorageConfiguration(ServiceConfiguration):
    region: str | None = None
    access_key: str | None = None
    secret_key: str | None = None

    public_access_bucket: str | None = None
    analytics_bucket: str | None = None

    endpoint_url: AnyHttpUrl | None = None

    url_template: AnyHttpUrl = parse_obj_as(
        AnyHttpUrl, "https://{bucket}.s3.{region}.amazonaws.com/{key}"
    )

    @validator("region")
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

    class Config:
        env_prefix = "PALACE_STORAGE_"

from typing import Optional

import boto3
from pydantic import AnyHttpUrl, parse_obj_as, validator

from core.service.configuration import ServiceConfiguration


class StorageConfiguration(ServiceConfiguration):
    region: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None

    public_access_bucket: Optional[str] = None
    analytics_bucket: Optional[str] = None

    endpoint_url: Optional[AnyHttpUrl] = None

    url_template: AnyHttpUrl = parse_obj_as(
        AnyHttpUrl, "https://{bucket}.s3.{region}.amazonaws.com/{key}"
    )

    @validator("region")
    def validate_region(cls, v: Optional[str]) -> Optional[str]:
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

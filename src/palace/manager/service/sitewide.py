from __future__ import annotations

from typing import Literal
from urllib.parse import urlparse

from pydantic import field_validator

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)
from palace.manager.util.pydantic import HttpUrl


class SitewideConfiguration(ServiceConfiguration):
    base_url: HttpUrl | None = None
    patron_web_hostnames: list[HttpUrl] | Literal["*"] = []
    quicksight_authorized_arns: dict[str, list[str]] | None = None

    @field_validator("patron_web_hostnames", mode="before")
    @classmethod
    def parse_patron_web_hostnames(
        cls, v: str | list[str] | None
    ) -> list[str] | Literal["*"] | None:
        if v is None or isinstance(v, list):
            return v
        if v == "*":
            return "*"
        return [hostname.strip() for hostname in v.split("|")]

    @field_validator("patron_web_hostnames")
    @classmethod
    def validate_patron_web_hostname(cls, v: list[str] | str) -> list[str] | str:
        if isinstance(v, list):
            for url in v:
                if path := urlparse(url).path:
                    raise ValueError(
                        f"Invalid patron web hostname {url}, path is not allowed ({path})."
                    )
        return v

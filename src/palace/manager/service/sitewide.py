from __future__ import annotations

from typing import Literal, cast

from pydantic import AnyHttpUrl, NonNegativeInt, validator

from palace.manager.service.configuration import ServiceConfiguration


class SitewideConfiguration(ServiceConfiguration):
    base_url: AnyHttpUrl | None = None
    patron_web_hostnames: list[AnyHttpUrl] | Literal["*"] = []
    authentication_document_cache_time: NonNegativeInt = 3600
    quicksight_authorized_arns: dict[str, list[str]] | None = None

    @validator("base_url")
    def validate_base_url(cls, v: AnyHttpUrl | None) -> AnyHttpUrl | None:
        # Our base url should not end with a slash, if it does we remove it.
        if v is not None and v.endswith("/"):
            return cast(AnyHttpUrl, v.rstrip("/"))
        return v

    @validator("patron_web_hostnames", pre=True)
    def parse_patron_web_hostnames(
        cls, v: str | list[str] | None
    ) -> list[str] | Literal["*"] | None:
        if v is None or isinstance(v, list):
            return v
        if v == "*":
            return "*"
        return [hostname.strip().rstrip("/") for hostname in v.split("|")]

    @validator("patron_web_hostnames")
    def validate_patron_web_hostname(
        cls, v: list[AnyHttpUrl] | str
    ) -> list[AnyHttpUrl] | str:
        if isinstance(v, list):
            for url in v:
                if url.path:
                    raise ValueError(
                        f"Invalid patron web hostname {url}, path is not allowed ({url.path})."
                    )
        return v

from __future__ import annotations

from typing import Any, Literal
from urllib.parse import urlparse

from pydantic import field_validator, model_validator

from palace.util.log import LoggerMixin

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)
from palace.manager.util.pydantic import HttpUrl


class SitewideConfiguration(ServiceConfiguration, LoggerMixin):
    base_url: HttpUrl | None = None
    patron_web_hostnames: list[HttpUrl] | Literal["*"] = []
    # The web catalog to send a patron to when we have no better destination,
    # such as an unsolicited SAML login. Unlike the hostnames above, this is a
    # full URL and may include a path.
    web_catalog_default_url: HttpUrl | None = None
    # Deprecated name for web_catalog_default_url, used only when that is unset.
    patron_web_default_url: HttpUrl | None = None
    quicksight_authorized_arns: dict[str, list[str]] | None = None

    @model_validator(mode="before")
    @classmethod
    def use_deprecated_web_catalog_default_url(cls, data: Any) -> Any:
        """Fall back to the deprecated PALACE_PATRON_WEB_DEFAULT_URL.

        A deployment that still sets it gets a warning, and the value is used
        only when PALACE_WEB_CATALOG_DEFAULT_URL is unset.
        """
        if not isinstance(data, dict) or not data.get("patron_web_default_url"):
            return data

        if data.get("web_catalog_default_url"):
            cls.logger().warning(
                "PALACE_PATRON_WEB_DEFAULT_URL is deprecated and is ignored, since "
                "PALACE_WEB_CATALOG_DEFAULT_URL is also set. Remove "
                "PALACE_PATRON_WEB_DEFAULT_URL."
            )
            return data

        cls.logger().warning(
            "PALACE_PATRON_WEB_DEFAULT_URL is deprecated. Rename it to "
            "PALACE_WEB_CATALOG_DEFAULT_URL."
        )
        return {**data, "web_catalog_default_url": data["patron_web_default_url"]}

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

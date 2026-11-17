from __future__ import annotations

from typing import Annotated
from urllib.parse import urlsplit

from flask_babel import lazy_gettext as _
from pydantic import field_validator

from palace.util.exceptions import PalaceValueError

from palace.manager.api.circulation.settings import (
    BaseCirculationApiSettings,
    BaseCirculationLoanSettings,
)
from palace.manager.core.config import ConfigurationAttributeValue
from palace.manager.integration.license.bibliotheca.constants import (
    BIBLIOTHECA_DEFAULT_BASE_URL,
)
from palace.manager.integration.settings import (
    FormFieldType,
    FormMetadata,
)
from palace.manager.util.pydantic import HttpUrl


class BibliothecaSettings(BaseCirculationApiSettings):
    username: Annotated[
        str,
        FormMetadata(
            label=_("Account ID"),
            required=True,
        ),
    ]
    password: Annotated[
        str,
        FormMetadata(
            label=_("Account Key"),
            required=True,
        ),
    ]
    external_account_id: Annotated[
        str,
        FormMetadata(
            label=_("Library ID"),
            required=True,
        ),
    ]
    # Do not rename this field to "url". Legacy collections may still have a
    # stale "url" key in their stored settings.
    server_url: Annotated[
        HttpUrl,
        FormMetadata(
            label=_("Server URL"),
            description=_(
                "The base URL of the cloudLibrary partner API, for example "
                "https://partnerqa.yourcloudlibrary.com. Leave blank to use "
                "the production server (%(url)s).",
                url=BIBLIOTHECA_DEFAULT_BASE_URL,
            ),
            required=False,
        ),
    ] = BIBLIOTHECA_DEFAULT_BASE_URL

    @field_validator("server_url", mode="before")
    @classmethod
    def _default_blank_server_url(cls, value: object) -> object:
        """Use the production server when the field is blank."""
        return BIBLIOTHECA_DEFAULT_BASE_URL if value is None else value

    @field_validator("server_url")
    @classmethod
    def _validate_server_url(cls, value: str) -> str:
        """Require an https URL with no credentials, path, query, or fragment."""
        parts = urlsplit(value)
        if (
            parts.scheme != "https"
            or parts.username
            or parts.password
            or parts.path
            or parts.query
            or parts.fragment
        ):
            raise PalaceValueError(
                "Must be an https URL with no credentials, path, query, or "
                "fragment, such as https://partnerqa.yourcloudlibrary.com."
            )
        return value


class BibliothecaLibrarySettings(BaseCirculationLoanSettings):
    dont_display_reserves: Annotated[
        ConfigurationAttributeValue,
        FormMetadata(
            label=_("Show/Hide Titles with No Available Loans"),
            required=False,
            description=_(
                "Titles with no available loans will not be displayed in the Catalog view."
            ),
            type=FormFieldType.SELECT,
            options={
                ConfigurationAttributeValue.YESVALUE: "Show",
                ConfigurationAttributeValue.NOVALUE: "Hide",
            },
        ),
    ] = ConfigurationAttributeValue.YESVALUE

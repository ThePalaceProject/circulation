from __future__ import annotations

from typing import Annotated

from flask_babel import lazy_gettext as _

from palace.manager.api.circulation.settings import (
    BaseCirculationApiSettings,
    BaseCirculationLoanSettings,
)
from palace.manager.core.config import ConfigurationAttributeValue
from palace.manager.integration.settings import (
    FormFieldType,
    FormMetadata,
)


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

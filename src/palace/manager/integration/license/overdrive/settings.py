from __future__ import annotations

from typing import Annotated

from flask_babel import lazy_gettext as _

from palace.manager.api.circulation.settings import (
    BaseCirculationApiSettings,
    BaseCirculationEbookLoanSettings,
)
from palace.manager.integration.license.overdrive.constants import OverdriveConstants
from palace.manager.integration.license.settings.connection import ConnectionSetting
from palace.manager.integration.settings import (
    BaseSettings,
    FormFieldType,
    FormMetadata,
)


class OverdriveSettings(ConnectionSetting, BaseCirculationApiSettings):
    """The basic Overdrive configuration"""

    external_account_id: Annotated[
        str | None,
        FormMetadata(
            label=_("Library ID"),
            type=FormFieldType.TEXT,
            description="The library identifier.",
            required=True,
        ),
    ]
    overdrive_website_id: Annotated[
        str,
        FormMetadata(
            label=_("Website ID"),
            type=FormFieldType.TEXT,
            description="The web site identifier.",
            required=True,
        ),
    ]
    overdrive_client_key: Annotated[
        str,
        FormMetadata(
            label=_("Client Key"),
            type=FormFieldType.TEXT,
            description="The Overdrive client key.",
            required=True,
        ),
    ]
    overdrive_client_secret: Annotated[
        str,
        FormMetadata(
            label=_("Client Secret"),
            type=FormFieldType.TEXT,
            description="The Overdrive client secret.",
            required=True,
        ),
    ]

    overdrive_server_nickname: Annotated[
        str,
        FormMetadata(
            label=_("Server family"),
            type=FormFieldType.SELECT,
            required=False,
            description="Unless you hear otherwise from Overdrive, your integration should use their production servers.",
            options={
                OverdriveConstants.PRODUCTION_SERVERS: ("Production"),
                OverdriveConstants.TESTING_SERVERS: _("Testing"),
            },
        ),
    ] = OverdriveConstants.PRODUCTION_SERVERS


class OverdriveLibrarySettings(BaseCirculationEbookLoanSettings):
    ils_name: Annotated[
        str,
        FormMetadata(
            label=_("ILS Name"),
            description=_(
                "When multiple libraries share an Overdrive account, Overdrive uses a setting called 'ILS Name' to determine which ILS to check when validating a given patron."
            ),
        ),
    ] = OverdriveConstants.ILS_NAME_DEFAULT


class OverdriveChildSettings(BaseSettings):
    external_account_id: Annotated[
        str | None,
        FormMetadata(
            label=_("Library ID"),
            required=True,
        ),
    ]

from __future__ import annotations

from flask_babel import lazy_gettext as _

from palace.manager.api.circulation import (
    BaseCirculationApiSettings,
    BaseCirculationEbookLoanSettings,
)
from palace.manager.api.overdrive.constants import OverdriveConstants
from palace.manager.integration.configuration.connection import ConnectionSetting
from palace.manager.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)


class OverdriveSettings(ConnectionSetting, BaseCirculationApiSettings):
    """The basic Overdrive configuration"""

    external_account_id: str | None = FormField(
        form=ConfigurationFormItem(
            label=_("Library ID"),
            type=ConfigurationFormItemType.TEXT,
            description="The library identifier.",
            required=True,
        ),
    )
    overdrive_website_id: str = FormField(
        form=ConfigurationFormItem(
            label=_("Website ID"),
            type=ConfigurationFormItemType.TEXT,
            description="The web site identifier.",
            required=True,
        )
    )
    overdrive_client_key: str = FormField(
        form=ConfigurationFormItem(
            label=_("Client Key"),
            type=ConfigurationFormItemType.TEXT,
            description="The Overdrive client key.",
            required=True,
        )
    )
    overdrive_client_secret: str = FormField(
        form=ConfigurationFormItem(
            label=_("Client Secret"),
            type=ConfigurationFormItemType.TEXT,
            description="The Overdrive client secret.",
            required=True,
        )
    )

    overdrive_server_nickname: str = FormField(
        default=OverdriveConstants.PRODUCTION_SERVERS,
        form=ConfigurationFormItem(
            label=_("Server family"),
            type=ConfigurationFormItemType.SELECT,
            required=False,
            description="Unless you hear otherwise from Overdrive, your integration should use their production servers.",
            options={
                OverdriveConstants.PRODUCTION_SERVERS: ("Production"),
                OverdriveConstants.TESTING_SERVERS: _("Testing"),
            },
        ),
    )


class OverdriveLibrarySettings(BaseCirculationEbookLoanSettings):
    ils_name: str = FormField(
        default=OverdriveConstants.ILS_NAME_DEFAULT,
        form=ConfigurationFormItem(
            label=_("ILS Name"),
            description=_(
                "When multiple libraries share an Overdrive account, Overdrive uses a setting called 'ILS Name' to determine which ILS to check when validating a given patron."
            ),
        ),
    )


class OverdriveChildSettings(BaseSettings):
    external_account_id: str | None = FormField(
        form=ConfigurationFormItem(
            label=_("Library ID"),
            required=True,
        )
    )

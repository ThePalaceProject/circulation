from __future__ import annotations

from typing import Any

from flask_babel import lazy_gettext as _
from pydantic import model_validator

from palace.manager.api.axis.constants import ServerNickname
from palace.manager.api.circulation import (
    BaseCirculationApiSettings,
    BaseCirculationLoanSettings,
)
from palace.manager.integration.settings import (
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)


class Axis360Settings(BaseCirculationApiSettings):
    username: str = FormField(
        form=ConfigurationFormItem(label=_("Username"), required=True)
    )
    password: str = FormField(
        form=ConfigurationFormItem(label=_("Password"), required=True)
    )
    external_account_id: str = FormField(
        form=ConfigurationFormItem(
            label=_("Library ID"),
            required=True,
        )
    )
    server_nickname: ServerNickname = FormField(
        default=ServerNickname.production,
        form=ConfigurationFormItem(
            label=_("Server family"),
            type=ConfigurationFormItemType.SELECT,
            required=False,
            description=f"This should generally be set to '{ServerNickname.production}'.",
            options={
                ServerNickname.production: (ServerNickname.production),
                ServerNickname.qa: _(ServerNickname.qa),
            },
        ),
    )
    verify_certificate: bool = FormField(
        default=True,
        form=ConfigurationFormItem(
            label=_("Verify SSL Certificate"),
            description=_(
                "This should always be True in production; though, it may need "
                "to be set to False to use the Axis 360 QA Environment."
            ),
            type=ConfigurationFormItemType.SELECT,
            options={
                "True": _("True"),
                "False": _("False"),
            },
        ),
    )

    @model_validator(mode="before")
    @classmethod
    def _migrate_url_to_server_nickname(cls, data: Any) -> Any:
        """
        This is a temporary migration to handle the change from  `url` to `server_nickname` in the settings.

        Once this is rolled out everywhere, we can do a migration in the database to set this field
        and remove this method.

        TODO: Remove in next release.
        """
        if isinstance(data, dict):
            if "url" in data:
                if "server_nickname" not in data:
                    existing_url = data["url"]
                    if "axis360apiqa.baker-taylor.com" in existing_url:
                        data["server_nickname"] = ServerNickname.qa
                    elif "axis360api.baker-taylor.com" in existing_url:
                        data["server_nickname"] = ServerNickname.production
                    else:
                        raise ValueError(f"Unexpected value URL: {existing_url}.")
                del data["url"]
        return data


class Axis360LibrarySettings(BaseCirculationLoanSettings):
    pass

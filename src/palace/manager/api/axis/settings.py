from __future__ import annotations

from flask_babel import lazy_gettext as _
from pydantic import field_validator

from palace.manager.api.admin.validator import Validator
from palace.manager.api.axis.requests import Axis360Requests
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
    url: str = FormField(
        default=Axis360Requests.PRODUCTION_BASE_URL,
        form=ConfigurationFormItem(
            label=_("Server"),
            required=True,
        ),
    )
    verify_certificate: bool | None = FormField(
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

    @field_validator("url")
    @classmethod
    def _validate_url(cls, v: str) -> str:
        # Validate if the url provided is valid http or a valid nickname
        valid_names = list(Axis360Requests.SERVER_NICKNAMES.keys())
        if not Validator._is_url(v, valid_names):
            raise ValueError(
                f"Server nickname must be one of {valid_names}, or an 'http[s]' URL."
            )
        return v


class Axis360LibrarySettings(BaseCirculationLoanSettings):
    pass

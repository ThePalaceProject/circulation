from __future__ import annotations

from flask_babel import lazy_gettext as _

from palace.manager.api.boundless.constants import ServerNickname
from palace.manager.api.circulation.settings import (
    BaseCirculationApiSettings,
    BaseCirculationLoanSettings,
)
from palace.manager.integration.configuration.formats import FormatPrioritiesSettings
from palace.manager.integration.settings import (
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)


class BoundlessSettings(BaseCirculationApiSettings, FormatPrioritiesSettings):
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
                "to be set to False to use the Boundless QA Environment."
            ),
            type=ConfigurationFormItemType.SELECT,
            options={
                True: _("True"),
                False: _("False"),
            },
        ),
    )


class BoundlessLibrarySettings(BaseCirculationLoanSettings):
    pass

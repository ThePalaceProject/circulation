from __future__ import annotations

from typing import Annotated

from flask_babel import lazy_gettext as _
from pydantic import NonNegativeInt

from palace.manager.api.circulation.settings import (
    BaseCirculationApiSettings,
    BaseCirculationLoanSettings,
)
from palace.manager.integration.license.boundless.constants import ServerNickname
from palace.manager.integration.settings import (
    FormFieldType,
    FormMetadata,
)


class BoundlessSettings(BaseCirculationApiSettings):
    username: Annotated[
        str,
        FormMetadata(label=_("Username"), required=True),
    ]
    password: Annotated[
        str,
        FormMetadata(label=_("Password"), required=True),
    ]
    external_account_id: Annotated[
        str,
        FormMetadata(
            label=_("Library ID"),
            required=True,
        ),
    ]
    server_nickname: Annotated[
        ServerNickname,
        FormMetadata(
            label=_("Server family"),
            type=FormFieldType.SELECT,
            required=False,
            description=f"This should generally be set to '{ServerNickname.production}'.",
            options={
                ServerNickname.production: (ServerNickname.production),
                ServerNickname.qa: _(ServerNickname.qa),
            },
        ),
    ] = ServerNickname.production
    verify_certificate: Annotated[
        bool,
        FormMetadata(
            label=_("Verify SSL Certificate"),
            description=_(
                "This should always be True in production; though, it may need "
                "to be set to False to use the Boundless QA Environment."
            ),
            type=FormFieldType.SELECT,
            options={
                True: _("True"),
                False: _("False"),
            },
        ),
    ] = True
    prioritize_boundless_drm: Annotated[
        bool,
        FormMetadata(
            label=_("Prioritize Boundless DRM"),
            description=_("Always use Boundless DRM if it is available."),
            type=FormFieldType.SELECT,
            options={
                True: _("Yes, prioritize Boundless DRM"),
                False: _("No, do not prioritize Boundless DRM"),
            },
        ),
    ] = False
    timeout: Annotated[
        NonNegativeInt,
        FormMetadata(
            label=_("Timeout (seconds)"),
            description=_(
                "The number of seconds to wait for a response from Boundless. Set to 0 for no timeout. "
                "Care should be taken when increasing this value as it can lead to long waits and "
                "server side performance issues."
            ),
            type=FormFieldType.NUMBER,
        ),
    ] = 15


class BoundlessLibrarySettings(BaseCirculationLoanSettings):
    pass

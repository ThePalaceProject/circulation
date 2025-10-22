from __future__ import annotations

from typing import Annotated

from flask_babel import lazy_gettext as _

from palace.manager.integration.license.opds.opds1.settings import OPDSImporterSettings
from palace.manager.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
)


class OPDSForDistributorsSettings(OPDSImporterSettings):
    username: Annotated[
        str,
        ConfigurationFormItem(
            label=_("Library's username or access key"),
            required=True,
        ),
    ]

    password: Annotated[
        str,
        ConfigurationFormItem(
            label=_("Library's password or secret key"),
            required=True,
        ),
    ]


class OPDSForDistributorsLibrarySettings(BaseSettings):
    pass

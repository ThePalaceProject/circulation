from __future__ import annotations

from typing import Annotated

from flask_babel import lazy_gettext as _

from palace.manager.integration.license.opds.opds1.settings import OPDSImporterSettings
from palace.manager.integration.settings import (
    BaseSettings,
    FormFieldType,
    FormMetadata,
)


class OPDSForDistributorsSettings(OPDSImporterSettings):
    generate_playtime_report: Annotated[
        bool,
        FormMetadata(
            label=_("Generate playtime report for audio books"),
            description=_(
                "When enabled, this collection will be included in the monthly "
                "playtime report uploaded to Google Drive. This is a system "
                "administrator setting."
            ),
            type=FormFieldType.SELECT,
            options={
                True: "Yes",
                False: "(Default) No",
            },
        ),
    ] = False

    username: Annotated[
        str,
        FormMetadata(
            label=_("Library's username or access key"),
            required=True,
        ),
    ]

    password: Annotated[
        str,
        FormMetadata(
            label=_("Library's password or secret key"),
            required=True,
        ),
    ]


class OPDSForDistributorsLibrarySettings(BaseSettings):
    pass

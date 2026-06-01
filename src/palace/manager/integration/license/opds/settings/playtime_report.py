from __future__ import annotations

from typing import Annotated

from flask_babel import lazy_gettext as _

from palace.manager.integration.settings import (
    BaseSettings,
    FormFieldType,
    FormMetadata,
)


class PlaytimeReportSettings(BaseSettings):
    """Mixin that adds the generate_playtime_report opt-in flag.

    Intended for OPDS 2.0 and OPDS for Distributors collections only.
    When True the collection's data source is included in the monthly
    playtime report uploaded to Google Drive.
    """

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

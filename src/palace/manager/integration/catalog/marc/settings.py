from __future__ import annotations

from typing import Annotated

from pydantic import NonNegativeInt

from palace.manager.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
)


class MarcExporterSettings(BaseSettings):
    # This setting (in days) controls how often MARC files should be
    # automatically updated. We run the celery task to update the MARC
    # files on a schedule, but this setting easily allows admins to
    # generate files more or less often.
    update_frequency: Annotated[
        NonNegativeInt,
        ConfigurationFormItem(
            label="Update frequency (in days)",
            type=ConfigurationFormItemType.NUMBER,
            required=True,
        ),
    ] = 30


class MarcExporterLibrarySettings(BaseSettings):
    # MARC organization codes are assigned by the
    # Library of Congress and can be found here:
    # http://www.loc.gov/marc/organizations/org-search.php
    organization_code: Annotated[
        str | None,
        ConfigurationFormItem(
            label="The MARC organization code for this library (003 field).",
            description="MARC organization codes are assigned by the Library of Congress.",
            type=ConfigurationFormItemType.TEXT,
        ),
    ] = None

    web_client_url: Annotated[
        str | None,
        ConfigurationFormItem(
            label="The base URL for the web catalog for this library, for the 856 field.",
            description="If using a library registry that provides a web catalog, this can be left blank.",
            type=ConfigurationFormItemType.TEXT,
        ),
    ] = None

    include_summary: Annotated[
        bool,
        ConfigurationFormItem(
            label="Include summaries in MARC records (520 field)",
            type=ConfigurationFormItemType.SELECT,
            options={False: "Do not include summaries", True: "Include summaries"},
        ),
    ] = False

    include_genres: Annotated[
        bool,
        ConfigurationFormItem(
            label="Include Palace Collection Manager genres in MARC records (650 fields)",
            type=ConfigurationFormItemType.SELECT,
            options={
                False: "Do not include Palace Collection Manager genres",
                True: "Include Palace Collection Manager genres",
            },
        ),
    ] = False

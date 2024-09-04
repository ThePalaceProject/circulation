from __future__ import annotations

from pydantic import NonNegativeInt

from palace.manager.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)


class MarcExporterSettings(BaseSettings):
    # This setting (in days) controls how often MARC files should be
    # automatically updated. We run the celery task to update the MARC
    # files on a schedule, but this setting easily allows admins to
    # generate files more or less often.
    update_frequency: NonNegativeInt = FormField(
        30,
        form=ConfigurationFormItem(
            label="Update frequency (in days)",
            type=ConfigurationFormItemType.NUMBER,
            required=True,
        ),
        alias="marc_update_frequency",
    )


class MarcExporterLibrarySettings(BaseSettings):
    # MARC organization codes are assigned by the
    # Library of Congress and can be found here:
    # http://www.loc.gov/marc/organizations/org-search.php
    organization_code: str | None = FormField(
        None,
        form=ConfigurationFormItem(
            label="The MARC organization code for this library (003 field).",
            description="MARC organization codes are assigned by the Library of Congress.",
            type=ConfigurationFormItemType.TEXT,
        ),
        alias="marc_organization_code",
    )

    web_client_url: str | None = FormField(
        None,
        form=ConfigurationFormItem(
            label="The base URL for the web catalog for this library, for the 856 field.",
            description="If using a library registry that provides a web catalog, this can be left blank.",
            type=ConfigurationFormItemType.TEXT,
        ),
        alias="marc_web_client_url",
    )

    include_summary: bool = FormField(
        False,
        form=ConfigurationFormItem(
            label="Include summaries in MARC records (520 field)",
            type=ConfigurationFormItemType.SELECT,
            options={"false": "Do not include summaries", "true": "Include summaries"},
        ),
    )

    include_genres: bool = FormField(
        False,
        form=ConfigurationFormItem(
            label="Include Palace Collection Manager genres in MARC records (650 fields)",
            type=ConfigurationFormItemType.SELECT,
            options={
                "false": "Do not include Palace Collection Manager genres",
                "true": "Include Palace Collection Manager genres",
            },
        ),
        alias="include_simplified_genres",
    )

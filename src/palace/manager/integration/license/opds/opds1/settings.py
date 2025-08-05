from __future__ import annotations

from enum import Enum

from flask_babel import lazy_gettext as _

from palace.manager.api.circulation.settings import BaseCirculationApiSettings
from palace.manager.core.classifier import Classifier
from palace.manager.integration.license.opds.settings.format_priority import (
    FormatPrioritiesSettings,
)
from palace.manager.integration.license.opds.settings.wayfless import (
    SAMLWAYFlessSetttings,
)
from palace.manager.integration.license.settings.connection import ConnectionSetting
from palace.manager.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)
from palace.manager.util.opds_writer import OPDSFeed
from palace.manager.util.pydantic import HttpUrl


class IdentifierSource(Enum):
    ID = "id"
    DCTERMS_IDENTIFIER = "first_dcterms_identifier"


class OPDSImporterSettings(
    ConnectionSetting,
    SAMLWAYFlessSetttings,
    FormatPrioritiesSettings,
    BaseCirculationApiSettings,
):
    external_account_id: HttpUrl = FormField(
        form=ConfigurationFormItem(
            label=_("URL"),
            required=True,
        )
    )

    data_source: str = FormField(
        form=ConfigurationFormItem(label=_("Data source name"), required=True)
    )

    include_in_inventory_report: bool = FormField(
        True,
        form=ConfigurationFormItem(
            label=_("Include in inventory report?"),
            type=ConfigurationFormItemType.SELECT,
            options={
                True: "(Default) Yes",
                False: "No",
            },
        ),
    )

    default_audience: str | None = FormField(
        None,
        form=ConfigurationFormItem(
            label=_("Default audience"),
            description=_(
                "If the vendor does not specify the target audience for their books, "
                "assume the books have this target audience."
            ),
            type=ConfigurationFormItemType.SELECT,
            options={
                **{None: _("No default audience")},
                **{audience: audience for audience in sorted(Classifier.AUDIENCES)},
            },
            required=False,
        ),
    )

    username: str | None = FormField(
        None,
        form=ConfigurationFormItem(
            label=_("Username"),
            description=_(
                "If HTTP Basic authentication is required to access the OPDS feed (it usually isn't), enter the username here."
            ),
            weight=-1,
        ),
    )

    password: str | None = FormField(
        None,
        form=ConfigurationFormItem(
            label=_("Password"),
            description=_(
                "If HTTP Basic authentication is required to access the OPDS feed (it usually isn't), enter the password here."
            ),
            weight=-1,
        ),
    )

    custom_accept_header: str = FormField(
        default=",".join(
            [
                OPDSFeed.ACQUISITION_FEED_TYPE,
                "application/atom+xml;q=0.9",
                "application/xml;q=0.8",
                "*/*;q=0.1",
            ]
        ),
        form=ConfigurationFormItem(
            label=_("Custom accept header"),
            required=False,
            description=_(
                "Some servers expect an accept header to decide which file to send. You can use */* if the server doesn't expect anything."
            ),
            weight=-1,
        ),
    )

    primary_identifier_source: IdentifierSource = FormField(
        IdentifierSource.ID,
        form=ConfigurationFormItem(
            label=_("Identifer"),
            required=False,
            description=_("Which book identifier to use as ID."),
            type=ConfigurationFormItemType.SELECT,
            options={
                IdentifierSource.ID: "(Default) Use <id>",
                IdentifierSource.DCTERMS_IDENTIFIER: "Use <dcterms:identifier> first, if not exist use <id>",
            },
        ),
    )


class OPDSImporterLibrarySettings(BaseSettings):
    pass

from __future__ import annotations

from enum import Enum
from typing import Annotated

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
    FormFieldType,
    FormMetadata,
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
    external_account_id: Annotated[
        HttpUrl,
        FormMetadata(
            label=_("URL"),
            required=True,
        ),
    ]

    data_source: Annotated[
        str,
        FormMetadata(label=_("Data source name"), required=True),
    ]

    include_in_inventory_report: Annotated[
        bool,
        FormMetadata(
            label=_("Include in inventory report?"),
            type=FormFieldType.SELECT,
            options={
                True: "(Default) Yes",
                False: "No",
            },
        ),
    ] = True

    default_audience: Annotated[
        str | None,
        FormMetadata(
            label=_("Default audience"),
            description=_(
                "If the vendor does not specify the target audience for their books, "
                "assume the books have this target audience."
            ),
            type=FormFieldType.SELECT,
            options={
                **{None: _("No default audience")},
                **{audience: audience for audience in sorted(Classifier.AUDIENCES)},
            },
            required=False,
        ),
    ] = None

    username: Annotated[
        str | None,
        FormMetadata(
            label=_("Username"),
            description=_(
                "If HTTP Basic authentication is required to access the OPDS feed (it usually isn't), enter the username here."
            ),
        ),
    ] = None

    password: Annotated[
        str | None,
        FormMetadata(
            label=_("Password"),
            description=_(
                "If HTTP Basic authentication is required to access the OPDS feed (it usually isn't), enter the password here."
            ),
        ),
    ] = None

    custom_accept_header: Annotated[
        str,
        FormMetadata(
            label=_("Custom accept header"),
            required=False,
            description=_(
                "Some servers expect an accept header to decide which file to send. You can use */* if the server doesn't expect anything."
            ),
            weight=-1,
        ),
    ] = OPDSFeed.ATOM_TYPE

    primary_identifier_source: Annotated[
        IdentifierSource,
        FormMetadata(
            label=_("Identifer"),
            required=False,
            description=_("Which book identifier to use as ID."),
            type=FormFieldType.SELECT,
            options={
                IdentifierSource.ID: "(Default) Use <id>",
                IdentifierSource.DCTERMS_IDENTIFIER: "Use <dcterms:identifier> first, if not exist use <id>",
            },
        ),
    ] = IdentifierSource.ID


class OPDSImporterLibrarySettings(BaseSettings):
    pass

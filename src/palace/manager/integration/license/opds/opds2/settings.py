from __future__ import annotations

from flask_babel import lazy_gettext as _

from palace.manager.integration.license.opds.opds1.settings import (
    OPDSImporterLibrarySettings,
    OPDSImporterSettings,
)
from palace.manager.integration.settings import (
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)
from palace.manager.opds import opds2
from palace.manager.sqlalchemy.constants import IdentifierType


class OPDS2ImporterSettings(OPDSImporterSettings):
    custom_accept_header: str = FormField(
        default="{}, {};q=0.9, */*;q=0.1".format(
            opds2.PublicationFeed.content_type(), "application/json"
        ),
        form=ConfigurationFormItem(
            label=_("Custom accept header"),
            description=_(
                "Some servers expect an accept header to decide which file to send. You can use */* if the server doesn't expect anything."
            ),
            type=ConfigurationFormItemType.TEXT,
            required=False,
        ),
    )

    ignored_identifier_types: list[str] = FormField(
        default=[],
        form=ConfigurationFormItem(
            label=_("List of identifiers that will be skipped"),
            description=_(
                "Circulation Manager will not be importing publications with identifiers having one of the selected types."
            ),
            type=ConfigurationFormItemType.MENU,
            required=False,
            options={
                identifier_type.value: identifier_type.value
                for identifier_type in IdentifierType
            },
            format="narrow",
        ),
    )


class OPDS2ImporterLibrarySettings(OPDSImporterLibrarySettings):
    pass

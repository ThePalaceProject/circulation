from typing import Annotated

from flask_babel import lazy_gettext as _
from pydantic import PositiveInt

from palace.manager.integration.settings import (
    BaseSettings,
    FormFieldType,
    FormMetadata,
)


class ConnectionSetting(BaseSettings):
    max_retry_count: Annotated[
        PositiveInt,
        FormMetadata(
            label=_("Connection retry limit"),
            description=_(
                "The maximum number of times to retry a request for certain connection-related errors."
            ),
            type=FormFieldType.NUMBER,
            required=False,
        ),
    ] = 3

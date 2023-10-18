from flask_babel import lazy_gettext as _
from pydantic import PositiveInt

from core.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)


class ConnectionSetting(BaseSettings):
    max_retry_count: PositiveInt = FormField(
        default=3,
        alias="connection_max_retry_count",
        form=ConfigurationFormItem(
            label=_("Connection retry limit"),
            description=_(
                "The maximum number of times to retry a request for certain connection-related errors."
            ),
            type=ConfigurationFormItemType.NUMBER,
            required=False,
        ),
    )

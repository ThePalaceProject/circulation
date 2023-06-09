from typing import Optional

from flask_babel import lazy_gettext as _

from core.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)


class ConnectionSetting(BaseSettings):
    connection_max_retry_count: Optional[int] = FormField(
        default=3,
        form=ConfigurationFormItem(
            label=_("Connection retry limit"),
            description=_(
                "The maximum number of times to retry a request for certain connection-related errors."
            ),
            type=ConfigurationFormItemType.NUMBER,
            required=False,
        ),
    )

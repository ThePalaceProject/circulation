from core.config import ConfigurationTrait
from core.model.configuration import ConfigurationMetadata, ConfigurationAttributeType

from flask_babel import lazy_gettext as _


class ConnectionConfigurationTrait(ConfigurationTrait):
    """Configuration information for connections to external servers."""

    max_retry_count = ConfigurationMetadata(
        key="connection_max_retry_count",
        label=_("Connection retry limit"),
        description=_(
            "The maximum number of times to retry a request for certain connection-related errors."
        ),
        type=ConfigurationAttributeType.NUMBER,
        required=False,
        default=3,
    )

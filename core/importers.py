from core.configuration.ignored_identifier import (
    IgnoredIdentifierConfiguration,
    IgnoredIdentifierSettings,
)
from core.connection_config import ConnectionConfigurationTrait, ConnectionSetting
from core.model.formats import (
    FormatPrioritiesConfigurationTrait,
    FormatPrioritiesSettings,
)
from core.saml.wayfless import SAMLWAYFlessConfigurationTrait, SAMLWAYFlessSetttings


class BaseImporterConfiguration(
    ConnectionConfigurationTrait,
    SAMLWAYFlessConfigurationTrait,
    FormatPrioritiesConfigurationTrait,
    IgnoredIdentifierConfiguration,
):
    """The abstract base class of importer configurations."""


class BaseImporterSettings(
    ConnectionSetting,
    SAMLWAYFlessSetttings,
    FormatPrioritiesSettings,
    IgnoredIdentifierSettings,
):
    pass

from palace.core.configuration.ignored_identifier import IgnoredIdentifierConfiguration
from palace.core.connection_config import ConnectionConfigurationTrait
from palace.core.model.formats import FormatPrioritiesConfigurationTrait
from palace.core.saml.wayfless import SAMLWAYFlessConfigurationTrait


class BaseImporterConfiguration(
    ConnectionConfigurationTrait,
    SAMLWAYFlessConfigurationTrait,
    FormatPrioritiesConfigurationTrait,
    IgnoredIdentifierConfiguration,
):
    """The abstract base class of importer configurations."""

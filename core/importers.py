from core.configuration.ignored_identifier import IgnoredIdentifierConfiguration
from core.connection_config import ConnectionConfigurationTrait
from core.model.formats import FormatPrioritiesConfigurationTrait
from core.saml.wayfless import SAMLWAYFlessConfigurationTrait


class BaseImporterConfiguration(
    ConnectionConfigurationTrait,
    SAMLWAYFlessConfigurationTrait,
    FormatPrioritiesConfigurationTrait,
    IgnoredIdentifierConfiguration,
):
    """The abstract base class of importer configurations."""

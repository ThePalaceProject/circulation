from core.configuration.ignored_identifier import IgnoredIdentifierConfiguration
from core.model.formats import FormatPrioritiesConfigurationTrait
from core.saml.wayfless import SAMLWAYFlessConfigurationTrait


class BaseImporterConfiguration(
    SAMLWAYFlessConfigurationTrait,
    FormatPrioritiesConfigurationTrait,
    IgnoredIdentifierConfiguration,
):
    """The abstract base class of importer configurations."""

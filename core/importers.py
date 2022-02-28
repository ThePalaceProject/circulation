from core.model.formats import FormatPrioritiesConfigurationTrait
from core.saml.wayfless import SAMLWAYFlessConfigurationTrait


class BaseImporterConfiguration(
    SAMLWAYFlessConfigurationTrait, FormatPrioritiesConfigurationTrait
):
    """The abstract base class of importer configurations."""

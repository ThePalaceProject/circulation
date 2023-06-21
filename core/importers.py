from core.configuration.ignored_identifier import IgnoredIdentifierSettings
from core.connection_config import ConnectionSetting
from core.model.formats import FormatPrioritiesSettings
from core.saml.wayfless import SAMLWAYFlessSetttings


class BaseImporterSettings(
    ConnectionSetting,
    SAMLWAYFlessSetttings,
    FormatPrioritiesSettings,
    IgnoredIdentifierSettings,
):
    pass

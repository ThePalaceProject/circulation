from flask_babel import lazy_gettext as _

from core.config import ConfigurationTrait
from core.model.configuration import ConfigurationAttributeType, ConfigurationMetadata


class SAMLWAYFlessConfigurationTrait(ConfigurationTrait):
    IDP_PLACEHOLDER = "{idp}"
    ACQUISITION_LINK_PLACEHOLDER = "{targetUrl}"
    WAYFLESS_URL_TEMPLATE_KEY: str = "wayfless_url_template"

    wayfless_url_template = ConfigurationMetadata(
        key="saml_wayfless_url_template",
        label=_("SAML WAYFless URL Template"),
        description=_(
            "<b>This configuration setting should be used ONLY when the authentication protocol is SAML.</b>"
            "<br>"
            "The phrase 'Where Are You From?' (WAYF) is often used to characterise identity provider discovery."
            "<br>"
            "Generally speaking, a <i>discovery service</i> is a solution to the "
            "<a href='https://wiki.shibboleth.net/confluence/display/SHIB2/IdPDiscovery'>identity provider discovery</a> problem, "
            "a longstanding problem in the federated identity management space "
            "when there are multiple identity providers available each corresponding to a specific organisation."
            "<br>"
            "To avoid having to use the 'Where Are You From' (WAYF) page it is possible to link directly to "
            "publication on the content provider's site. "
            "If the user is already logged in they will be taken directly to the article, "
            "otherwise they will be taken directly to your login page and then onto the article after logging in. "
            "These links are created using the following format:"
            "<br>"
            "https://fsso.springer.com/saml/login?idp={idp}&targetUrl={targetUrl}"
            "<br>"
            " - <b>idp</b> is an entityID of the SAML Identity Provider. "
            "Circulation Manager will substitute it with the entity ID of the 'active' IdP, "
            "i.e., the IdP that the patron is currently authenticated against."
            "<br>"
            " - <b>targetUrl</b> is substituted with the an encoded direct link to the publication."
        ),
        type=ConfigurationAttributeType.TEXT,
        required=False,
        default=None,
    )

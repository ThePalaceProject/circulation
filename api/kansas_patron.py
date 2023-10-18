from typing import Optional, Type, Union

from flask_babel import lazy_gettext as _
from lxml import etree
from pydantic import HttpUrl

from api.authentication.base import PatronData
from api.authentication.basic import (
    BasicAuthenticationProvider,
    BasicAuthProviderLibrarySettings,
    BasicAuthProviderSettings,
)
from core.integration.settings import ConfigurationFormItem, FormField
from core.model import Patron
from core.util.http import HTTP


class KansasAuthSettings(BasicAuthProviderSettings):
    url: HttpUrl = FormField(
        "https://ks-kansaslibrary3m.civicplus.com/api/UserDetails",
        form=ConfigurationFormItem(
            label=_("URL"),
            required=True,
        ),
    )


class KansasAuthenticationAPI(
    BasicAuthenticationProvider[KansasAuthSettings, BasicAuthProviderLibrarySettings]
):
    @classmethod
    def label(cls) -> str:
        return "Kansas"

    @classmethod
    def description(cls) -> str:
        return "An authentication service for the Kansas State Library."

    @classmethod
    def settings_class(cls) -> Type[KansasAuthSettings]:
        return KansasAuthSettings

    @classmethod
    def library_settings_class(cls) -> Type[BasicAuthProviderLibrarySettings]:
        return BasicAuthProviderLibrarySettings

    def __init__(
        self,
        library_id: int,
        integration_id: int,
        settings: KansasAuthSettings,
        library_settings: BasicAuthProviderLibrarySettings,
        analytics=None,
    ):
        super().__init__(
            library_id, integration_id, settings, library_settings, analytics
        )
        self.base_url = str(settings.url)

    # Begin implementation of BasicAuthenticationProvider abstract
    # methods.

    def remote_authenticate(
        self, username: Optional[str], password: Optional[str]
    ) -> Optional[PatronData]:
        # Create XML doc for request
        authorization_request = self.create_authorize_request(username, password)
        # Post request to the server
        response = self.post_request(authorization_request)
        # Parse response from server
        authorized, patron_name, library_identifier = self.parse_authorize_response(
            response.content
        )
        if not authorized:
            return None
        # Kansas auth gives very little data about the patron. Only name and a library identifier.
        return PatronData(
            permanent_id=username,
            authorization_identifier=username,
            personal_name=patron_name,
            library_identifier=library_identifier,
            complete=True,
        )

    def remote_patron_lookup(
        self, patron_or_patrondata: Union[PatronData, Patron]
    ) -> Optional[PatronData]:
        # Kansas auth gives very little data about the patron. So this function is just a passthrough.
        if isinstance(patron_or_patrondata, PatronData):
            return patron_or_patrondata

        return None

    # End implementation of BasicAuthenticationProvider abstract methods.

    @staticmethod
    def create_authorize_request(barcode, pin):
        # Create the authentication document
        authorize_request = etree.Element("AuthorizeRequest")
        user_id = etree.Element("UserID")
        user_id.text = barcode
        password = etree.Element("Password")
        password.text = pin
        authorize_request.append(user_id)
        authorize_request.append(password)
        return etree.tostring(authorize_request, encoding="utf8")

    def parse_authorize_response(self, response):
        try:
            authorize_response = etree.fromstring(response)
        except etree.XMLSyntaxError:
            self.log.error(
                "Unable to parse response from API. Deny Access. Response: \n%s",
                response,
            )
            return False, None, None
        patron_names = []
        for tag in ["FirstName", "LastName"]:
            element = authorize_response.find(tag)
            if element is not None and element.text is not None:
                patron_names.append(element.text)
        patron_name = " ".join(patron_names) if len(patron_names) != 0 else None
        element = authorize_response.find("LibraryID")
        library_identifier = element.text if element is not None else None
        element = authorize_response.find("Status")
        if element is None:
            self.log.info(
                "Status element not found in response from server. Deny Access."
            )
        authorized = True if element is not None and element.text == "1" else False
        return authorized, patron_name, library_identifier

    def post_request(self, data):
        """Make an HTTP request.

        Defined solely so it can be overridden in the mock.
        """
        return HTTP.post_with_timeout(
            self.base_url,
            data,
            headers={"Content-Type": "application/xml"},
            max_retry_count=0,
            allowed_response_codes=["2xx"],
        )

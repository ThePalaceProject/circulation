from __future__ import annotations

from gettext import gettext as _
from typing import TYPE_CHECKING
from urllib.parse import urljoin

from api.authenticator import BasicAuthenticationProvider, PatronData
from core.model.configuration import ExternalIntegration
from core.util.http import HTTP

if TYPE_CHECKING:
    from requests import Response


class SirsiDynixAuthenticationProvider(BasicAuthenticationProvider):
    NAME = "SirsiDynix Authentication"
    DESCRIPTION = "SirsiDynix Symphony Webservice Authentication"
    FLOW_TYPE = "http://librarysimplified.org/authtype/sirsidynix"

    class Keys:
        APP_ID = "APP_ID"
        CLIENT_ID = "CLIENT_ID"

    SETTINGS = [
        {
            "key": ExternalIntegration.URL,
            "label": _("Server URL"),
            "description": _("The external server url."),
            "required": True,
            "default": "https://vendor1-sym.sirsidynix.net/ilsws_current/",
        },
        {
            "key": Keys.APP_ID,
            "label": _("Application ID"),
            "description": _(
                "This can be anything, it is used to group all requests to the API."
            ),
            "required": True,
        },
        {
            "key": Keys.CLIENT_ID,
            "label": _("Client ID"),
            "description": _("The client ID that should be used to identify this CM."),
            "required": True,
        },
    ]

    def __init__(self, library, integration: ExternalIntegration, analytics=None):
        super().__init__(library, integration, analytics)
        self.server_url = integration.url
        # trailing slash, else urljoin has issues
        self.server_url = self.server_url + (
            "/" if not self.server_url.endswith("/") else ""
        )
        self.sirsi_client_id = integration.setting(self.Keys.CLIENT_ID).value
        self.sirsi_app_id = integration.setting(self.Keys.APP_ID).value

    def remote_authenticate(self, username: str, password: str) -> PatronData | bool:
        """Authenticate this user with the remote server."""
        data = self.api_patron_login(username, password)
        if not data:
            return False

        return PatronData(username=username, authorization_identifier=username)

    ###
    # API requests
    ###

    def _request(self, method: str, path: str, json=None) -> Response:
        """Request wrapper that adds the relevant request headers.

        :param method: The HTTP method for the request
        :param path: The url path that will get joined to the server_url, should not have a leading '/'
        :param json: The json data to be sent to the API endpoint
        """
        if path.startswith("/"):
            raise ValueError(
                f"Sirsidynix URL path {path} should not have a leading '/'"
            )
        headers = {
            "SD-Originating-App-Id": self.sirsi_app_id,
            "x-sirs-clientID": self.sirsi_client_id,
        }
        url = urljoin(self.server_url, path)
        return HTTP.request_with_timeout(method, url, headers=headers, json=json)

    def api_patron_login(self, username: str, password: str) -> bool | dict:
        """API request to verify credentials of a user.

        :param username: The login username
        :param password: The login pin
        """
        response = self._request(
            "POST", "user/patron/login", json=dict(login=username, password=password)
        )
        if response.status_code != 200:
            self.log.info(
                f"Authentication failed for username {username}: {response.text}"
            )
            return False
        return response.json()


AuthenticationProvider = SirsiDynixAuthenticationProvider

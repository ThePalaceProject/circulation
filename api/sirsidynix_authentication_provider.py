from __future__ import annotations

import os
from gettext import gettext as _
from typing import TYPE_CHECKING, Literal
from urllib.parse import urljoin

from sqlalchemy.orm import object_session

from api.authenticator import BasicAuthenticationProvider, PatronData
from api.problem_details import INVALID_CREDENTIALS
from core.config import Configuration
from core.model.configuration import (
    ConfigurationAttributeType,
    ConfigurationSetting,
    ExternalIntegration,
)
from core.util.http import HTTP

if TYPE_CHECKING:
    from requests import Response

    from core.model.patron import Patron


class SirsiBlockReasons:
    NOT_APPROVED = _("Patron has not yet been approved")
    EXPIRED = _("Patron membership has expired")
    INCORRECT_LOCATION = _("Patron is not a member of this library location")
    PATRON_BLOCKED = _("Patron has been blocked.")


class SirsiDynixHorizonAuthenticationProvider(BasicAuthenticationProvider):
    """SirsiDynix Authentication API implementation.

    Currently is only used to authenticate patrons, there is no CRUD implemented for patron profiles.
    It is recommended (but not mandatory) to have the environment variable `SIRSI_DYNIX_APP_ID` set, so that the API requests
    have an identifiying App ID attached to them, which is the recommended approach as per the SirsiDynix docs.
    """

    NAME = "SirsiDynix Horizon Authentication"
    DESCRIPTION = "SirsiDynix Horizon Webservice Authentication"
    FLOW_TYPE = "http://librarysimplified.org/authtype/sirsidynix-horizon"

    DEFAULT_APP_ID = "PALACE"

    class Keys:
        """Keys relevant to the Settings module"""

        CLIENT_ID = "CLIENT_ID"
        LIBRARY_ID = "LIBRARY_ID"
        LIBRARY_PREFIX = "LIBRARY_PREFIX"
        DISALLOWED_SUFFIXES = "DISALLOWED_SUFFIXES"

    SETTINGS = [
        {
            "key": ExternalIntegration.URL,
            "label": _("Server URL"),
            "description": _("The external server url."),
            "required": True,
            "default": "https://vendor1-sym.sirsidynix.net/ilsws_current/",
        },
        {
            "key": Keys.CLIENT_ID,
            "label": _("Client ID"),
            "description": _("The client ID that should be used to identify this CM."),
            "required": True,
        },
        {
            "key": BasicAuthenticationProvider.TEST_IDENTIFIER,
            "label": _("Test Identifier"),
            "description": BasicAuthenticationProvider.TEST_IDENTIFIER_DESCRIPTION_FOR_OPTIONAL_PASSWORD,
            "required": True,
        },
        {
            "key": BasicAuthenticationProvider.TEST_PASSWORD,
            "label": _("Test Password"),
            "description": BasicAuthenticationProvider.TEST_PASSWORD_DESCRIPTION_OPTIONAL,
        },
        {
            "key": Keys.DISALLOWED_SUFFIXES,
            "type": ConfigurationAttributeType.LIST.value,
            "label": _("Disallowed Patron Suffixes"),
            "description": _(
                "Any patron type ending in this suffix will remain unauthenticated. "
                "Eg. A patronType of 'cls' and Library Prefix of 'c' will result in a suffix of 'ls'. "
                "If 'ls' is a disallowed suffix then the patron will not be authenticated."
            ),
            "required": False,
        },
    ]

    LIBRARY_SETTINGS = [
        {
            "key": Keys.LIBRARY_ID,
            "label": _("Library ID"),
            "description": _(
                "This is used to identify a unique library on the API. This must match what the API expects."
            ),
            "required": True,
        },
        {
            "key": Keys.LIBRARY_PREFIX,
            "label": _("Library Prefix"),
            "description": _(
                "This is used to match a member of the Library to their member branch location."
                " Should be a prefix letter, eg. 'p' or 'co' etc..."
            ),
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
        self.sirsi_app_id = os.environ.get(
            Configuration.SIRSI_DYNIX_APP_ID, default=self.DEFAULT_APP_ID
        )
        self.sirsi_disallowed_prefixes = integration.setting(
            self.Keys.DISALLOWED_SUFFIXES
        ).value_or_default([])
        self.sirsi_library_id = (
            ConfigurationSetting.for_library_and_externalintegration(
                object_session(library), self.Keys.LIBRARY_ID, library, integration
            ).value
        )
        self.sirsi_library_prefix = (
            ConfigurationSetting.for_library_and_externalintegration(
                object_session(library), self.Keys.LIBRARY_PREFIX, library, integration
            ).value
        )

    def remote_authenticate(self, username: str, password: str) -> PatronData | bool:
        """Authenticate this user with the remote server."""
        data = self.api_patron_login(username, password)
        if not data:
            return False

        return SirsiDynixPatronData(
            username=username,
            authorization_identifier=username,
            permanent_id=data.get("patronKey"),
            session_token=data.get("sessionToken"),
            complete=False,
        )

    # Because of the odd way in which this method is written in AuthenticationProvider
    # we have to exclude it from type checking, because mypy gets confused with the inheritance
    def _remote_patron_lookup(  # type: ignore
        self, patron_or_patrondata: Patron | SirsiDynixPatronData
    ) -> None | SirsiDynixPatronData:
        """Do a remote patron lookup, this method can only lookup a patron with a patrondata object
        with a session_token already setup within it.
        This method also checks all the reasons that a patron may be blocked for.
        """
        # We cannot do a remote lookup without a session token
        if not isinstance(patron_or_patrondata, SirsiDynixPatronData):
            return None
        elif not patron_or_patrondata.session_token:
            return None

        patrondata = patron_or_patrondata
        # Pull and parse the basic patron information
        data = self.api_read_patron_data(
            patron_key=patrondata.permanent_id,
            session_token=patrondata.session_token,
        )
        if not data or "fields" not in data:
            return None

        fields: dict = data["fields"]
        patrondata.personal_name = fields.get("displayName")
        patron_type: str = fields["patronType"].get("key", "")

        patrondata.external_type = patron_type

        # Basic block reasons

        # Does the patron type start with the library prefix
        # This ensures the patron is a member of the library
        # they are interacting with
        if not patron_type.startswith(self.sirsi_library_prefix):
            # We respond with a problem detail, stopping the upstream authentication immediately
            return INVALID_CREDENTIALS

        if not fields.get("approved", False):
            patrondata.block_reason = SirsiBlockReasons.NOT_APPROVED
            return patrondata

        # Remove the library prefix from the patron type
        # we are left with the patron "suffix"
        # This suffix can be part of the blocked patron types list
        patron_suffix = patron_type[len(self.sirsi_library_prefix) :]
        if patron_suffix in self.sirsi_disallowed_prefixes:
            patrondata.block_reason = SirsiBlockReasons.PATRON_BLOCKED
            return patrondata

        # Get patron "fines" information
        status = self.api_patron_status_info(
            patron_key=patrondata.permanent_id,
            session_token=patrondata.session_token,
        )

        if not status or "fields" not in status:
            return None

        status_fields: dict = status["fields"]
        fines = status_fields.get("estimatedFines")
        if fines is not None:
            # We ignore currency for now, and assume USD
            patrondata.fines = float(fines.get("amount", 0))

        # Blockable statuses
        if status_fields.get("hasMaxDaysWithFines") or status_fields.get("hasMaxFines"):
            patrondata.block_reason = PatronData.EXCESSIVE_FINES
        elif status_fields.get("hasMaxLostItem"):
            patrondata.block_reason = PatronData.TOO_MANY_LOST
        elif status_fields.get("hasMaxOverdueDays") or status_fields.get(
            "hasMaxOverdueItem"
        ):
            patrondata.block_reason = PatronData.TOO_MANY_OVERDUE
        elif status_fields.get("hasMaxItemsCheckedOut"):
            patrondata.block_reason = PatronData.TOO_MANY_LOANS
        elif status_fields.get("expired"):
            patrondata.block_reason = SirsiBlockReasons.EXPIRED

        # If previously, the patron was blocked this should unset the value in the DB
        if patrondata.block_reason is None:
            patrondata.block_reason = PatronData.NO_VALUE
        patrondata.complete = True
        return patrondata

    ###
    # API requests
    ###

    def _request(
        self, method: str, path: str, json=None, session_token=None
    ) -> Response:
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
            "SD-Working-LibraryID": self.sirsi_library_id,
            "x-sirs-clientID": self.sirsi_client_id,
        }
        if session_token:
            headers["x-sirs-sessionToken"] = session_token

        url = urljoin(self.server_url, path)
        # Adding a long timeout because /patronStatusInfo would fail often
        return HTTP.request_with_timeout(method, url, headers=headers, json=json)

    def api_patron_login(self, username: str, password: str) -> Literal[False] | dict:
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

    def api_read_patron_data(
        self, patron_key: str, session_token: str
    ) -> Literal[False] | dict:
        """API request to pull basic patron information

        :param patron_key: The permanent external identifier for a patron
        :param session_token: The session token for a logged in user
        """
        response = self._request(
            "GET", f"user/patron/key/{patron_key}", session_token=session_token
        )
        if response.status_code != 200:
            self.log.info(
                f"Could not fetch patron data for {patron_key}: {response.text}"
            )
            return False
        return response.json()

    def api_patron_status_info(
        self, patron_key: str, session_token: str
    ) -> Literal[False] | dict:
        """API request to pull patron status information, like fines

        :param patron_key: The permanent external identifier for a patron
        :param session_token: The session token for a logged in user
        """
        response = self._request(
            "GET",
            f"user/patronStatusInfo/key/{patron_key}",
            session_token=session_token,
        )
        if response.status_code != 200:
            self.log.info(
                f"Could not fetch patron status info for {patron_key}: {response.text}"
            )
            return False
        return response.json()


class SirsiDynixPatronData(PatronData):
    """Sirsi specific version of patron data.
    Only adds an extra `session_token` to track logged in users
    """

    def __init__(self, session_token=None, **kwargs):
        super().__init__(**kwargs)
        self.session_token = session_token


AuthenticationProvider = SirsiDynixHorizonAuthenticationProvider

from __future__ import annotations

import json
import os
from collections.abc import Callable, Generator
from gettext import gettext as _
from typing import TYPE_CHECKING, Annotated, Any, Literal
from urllib.parse import urljoin

from sqlalchemy.orm import Session

from palace.manager.api.authentication.base import PatronData
from palace.manager.api.authentication.basic import (
    BasicAuthenticationProvider,
    BasicAuthProviderLibrarySettings,
    BasicAuthProviderSettings,
)
from palace.manager.core.config import Configuration
from palace.manager.core.exceptions import BasePalaceException, PalaceValueError
from palace.manager.core.selftest import SelfTestResult
from palace.manager.integration.settings import (
    FormFieldType,
    FormMetadata,
)
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.http.http import HTTP
from palace.manager.util.pydantic import HttpUrl

if TYPE_CHECKING:
    from requests import Response


class SirsiBlockReasons:
    NOT_APPROVED = _("Patron has not yet been approved")
    EXPIRED = _("Patron membership has expired")
    PATRON_BLOCKED = _("Patron has been blocked.")


class SirsiDynixHorizonAuthSettings(BasicAuthProviderSettings):
    url: Annotated[
        HttpUrl,
        FormMetadata(
            label="Server URL",
            description="The external server url.",
        ),
    ]
    client_id: Annotated[
        str,
        FormMetadata(
            label="Client ID",
            description="The client ID that should be used to identify this CM.",
        ),
    ]

    patron_blocks_enforced: Annotated[
        bool,
        FormMetadata(
            label="Enforce Patron ILS Blocks",
            description=(
                "Block patrons from borrowing based on the approved, hasMaxDaysWithFines, hasMaxFines, hasMaxLostItem, "
                "hasMaxOverdueDays, hasMaxItemsCheckedOut fields from the ILS.(Note: expired accounts are always "
                "blocked)."
            ),
            type=FormFieldType.SELECT,
            options={
                True: "Patron blocks enforced",
                False: "Patron blocks NOT enforced.",
            },
        ),
    ] = True


class SirsiDynixHorizonAuthLibrarySettings(BasicAuthProviderLibrarySettings):
    library_id: Annotated[
        str,
        FormMetadata(
            label="Library ID",
            description="This is used to identify a unique library on the API. This must match what the API expects.",
        ),
    ]
    library_disallowed_suffixes: Annotated[
        list[str],
        FormMetadata(
            label="Disallowed Patron Suffixes",
            description=(
                "Any patron type ending in this suffix will remain unauthenticated. "
                "Eg. A patronType of 'cls' and Library Prefix of 'c' will result in a suffix of 'ls'. "
                "If 'ls' is a disallowed suffix then the patron will not be authenticated."
            ),
            type=FormFieldType.LIST,
        ),
    ] = []
    library_identifier_field: Annotated[
        Literal["barcode"] | Literal["patrontype"],
        FormMetadata(
            label="Library Identifier Field",
            description="This is the field on the patron record that the <em>Library Identifier Restriction "
            "Type</em> is applied to, different patron authentication methods provide different "
            "values here. This value is not used if <em>Library Identifier Restriction Type</em> "
            "is set to 'No restriction'.",
            options={
                "barcode": "Barcode",
                "patrontype": "Patron Type",
            },
            type=FormFieldType.SELECT,
        ),
    ] = "patrontype"


class SirsiDynixHorizonAuthenticationProvider(
    BasicAuthenticationProvider[
        SirsiDynixHorizonAuthSettings, SirsiDynixHorizonAuthLibrarySettings
    ]
):
    """SirsiDynix Authentication API implementation.

    Currently, is only used to authenticate patrons, there is no CRUD implemented for patron profiles.
    It is recommended (but not mandatory) to have the environment variable `SIRSI_DYNIX_APP_ID` set, so that the API requests
    have an identifiying App ID attached to them, which is the recommended approach as per the SirsiDynix docs.
    """

    DEFAULT_APP_ID = "PALACE"

    @classmethod
    def label(cls) -> str:
        return "SirsiDynix Horizon Authentication"

    @classmethod
    def description(cls) -> str:
        return "SirsiDynix Horizon Webservice Authentication"

    @classmethod
    def settings_class(cls) -> type[SirsiDynixHorizonAuthSettings]:
        return SirsiDynixHorizonAuthSettings

    @classmethod
    def library_settings_class(
        cls,
    ) -> type[SirsiDynixHorizonAuthLibrarySettings]:
        return SirsiDynixHorizonAuthLibrarySettings

    def __init__(
        self,
        library_id: int,
        integration_id: int,
        settings: SirsiDynixHorizonAuthSettings,
        library_settings: SirsiDynixHorizonAuthLibrarySettings,
        analytics: Analytics | None = None,
    ):
        super().__init__(
            library_id, integration_id, settings, library_settings, analytics
        )
        self.server_url = str(settings.url)
        # trailing slash, else urljoin has issues
        self.server_url = self.server_url + (
            "/" if not self.server_url.endswith("/") else ""
        )

        self.sirsi_client_id = settings.client_id
        self.sirsi_app_id = os.environ.get(
            Configuration.SIRSI_DYNIX_APP_ID, default=self.DEFAULT_APP_ID
        )

        self.sirsi_disallowed_suffixes = library_settings.library_disallowed_suffixes
        self.sirsi_library_id = library_settings.library_id

        # Check if patrons should be blocked based on ILS status
        self.patron_blocks_enforced = settings.patron_blocks_enforced

    def remote_authenticate(
        self, username: str | None, password: str | None
    ) -> PatronData | None:
        """Authenticate this user with the remote server."""
        if username is None or password is None:
            return None

        data = self.api_patron_login(username, password)
        if not data:
            return None

        return SirsiDynixPatronData(
            username=username,
            authorization_identifier=username,
            permanent_id=data.get("patronKey"),
            session_token=data.get("sessionToken"),
            complete=False,
        )

    def remote_patron_lookup(
        self, patron_or_patrondata: Patron | PatronData
    ) -> None | PatronData:
        """Do a remote patron lookup, this method can only look up a patron with a patrondata object
        with a session_token already setup within it.
        This method also checks all the reasons that a patron may be blocked for.
        """

        # Return None if a patron object is passed in.
        if isinstance(patron_or_patrondata, Patron):
            return None

        # if the patron data object is not authenticated just pass it back after ensuring the
        # complete flag is set to False
        if (
            not isinstance(patron_or_patrondata, SirsiDynixPatronData)
            or patron_or_patrondata.session_token is None
        ):
            patron_or_patrondata.complete = False
            return patron_or_patrondata

        patrondata = patron_or_patrondata
        # Pull and parse the basic patron information
        data = self.api_read_patron_data(
            patron_key=patrondata.permanent_id,
            session_token=patrondata.session_token,
        )
        if not data or "fields" not in data:
            return None

        patrondata.complete = True
        fields: dict = data["fields"]
        patrondata.personal_name = fields.get("displayName")

        if self.library_identifier_field == "patrontype":
            patron_type: str = fields["patronType"].get("key", "")
            patrondata.library_identifier = patron_type
        elif self.library_identifier_field == "barcode":
            pass
            # do nothing sense the library identifier field and restriction type are handled in the authentication
            # function (see enforce_library_identifier_restriction())
        else:
            # this should never happen
            raise PalaceValueError(
                f"Unexpected  library_identifier_field value ({self.library_identifier_field})"
            )

        # Basic block reasons

        # Some Symphony installations appear to use "standing" to indicate approval.
        # TODO we may want to make the approval mechanism configurable.
        if not (
            fields.get("approved", False)
            or fields.get("standing", {}).get("key", "").lower() == "ok"
        ):
            patrondata.block_reason = SirsiBlockReasons.NOT_APPROVED
            return patrondata

        # If the patron type ends with a disallowed suffix the
        # patron will be authenticated but marked as blocked.
        if patron_type is not None:
            for suffix in self.sirsi_disallowed_suffixes:
                if patron_type.endswith(suffix):
                    patrondata.block_reason = SirsiBlockReasons.PATRON_BLOCKED
                    return patrondata

        for suffix in self.sirsi_disallowed_suffixes:
            if patron_type.endswith(suffix):
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

        if status_fields.get("expired"):
            patrondata.block_reason = SirsiBlockReasons.EXPIRED
        elif status_fields.get("hasMaxDaysWithFines") or status_fields.get(
            "hasMaxFines"
        ):
            patrondata.block_reason = PatronData.EXCESSIVE_FINES
        elif status_fields.get("hasMaxLostItem"):
            patrondata.block_reason = PatronData.TOO_MANY_LOST
        elif status_fields.get("hasMaxOverdueDays") or status_fields.get(
            "hasMaxOverdueItem"
        ):
            patrondata.block_reason = PatronData.TOO_MANY_OVERDUE
        elif status_fields.get("hasMaxItemsCheckedOut"):
            patrondata.block_reason = PatronData.TOO_MANY_LOANS

        # If previously, the patron was blocked this should unset the value in the DB
        if patrondata.block_reason is None:
            patrondata.block_reason = PatronData.NO_VALUE

        if not self.patron_blocks_enforced:
            # if blocks are ignored and patron is not expired, treat patron as unblocked.
            if patrondata.block_reason != SirsiBlockReasons.EXPIRED:
                patrondata.block_reason = PatronData.NO_VALUE

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
        return HTTP.request_with_timeout(
            method, url, headers=headers, json=json, max_retry_count=0
        )

    def api_patron_login(
        self, username: str, password: str
    ) -> Literal[False] | dict[str, Any]:
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
    ) -> Literal[False] | dict[str, Any]:
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
    ) -> Literal[False] | dict[str, Any]:
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

    def _run_self_tests(self, _db: Session) -> Generator[SelfTestResult]:
        """Verify the credentials of the test patron for this integration,
        and update its metadata.
        """

        test_username = self.test_username
        test_password = self.test_password or ""

        if test_username is None:
            yield self.test_failure(
                "Configuration", "No test patron username configured."
            )
            return

        def login(username: str, password: str) -> dict[str, Any]:
            result = self.api_patron_login(username, password)
            if result is False:
                raise BasePalaceException("Could not authenticate test patron")
            return result

        yield (
            test_result := self.run_test(
                "Login Patron", login, test_username, test_password
            )
        )
        if not test_result.success:
            return

        patron_key = test_result.result.get("patronKey")
        session_token = test_result.result.get("sessionToken")

        def read_data(
            name: str,
            func: Callable[[str, str], Literal[False] | dict[str, Any]],
            patron_key: str,
            session_token: str,
        ) -> str:
            result = func(patron_key, session_token)
            if result is False:
                raise BasePalaceException(f"Could not fetch {name}")
            fields = result.get("fields")
            if fields is None:
                raise BasePalaceException(f"Field data 'fields' not found in {name}.")
            if not isinstance(fields, dict):
                raise BasePalaceException(
                    f"Field data is not a dict (data: {json.dumps(fields)})."
                )
            return json.dumps(fields, indent=4)

        yield (
            test_result := self.run_test(
                "Read Patron Data",
                read_data,
                "Patron Data",
                self.api_read_patron_data,
                patron_key,
                session_token,
            )
        )
        if not test_result.success:
            return

        yield (
            test_result := self.run_test(
                "Patron Status Info",
                read_data,
                "Patron Status",
                self.api_patron_status_info,
                patron_key,
                session_token,
            )
        )
        if not test_result.success:
            return

        yield from super()._run_self_tests(_db)


class SirsiDynixPatronData(PatronData):
    """Sirsi specific version of patron data.
    Only adds an extra `session_token` to track logged in users
    """

    def __init__(self, session_token=None, **kwargs):
        super().__init__(**kwargs)
        self.session_token = session_token

from __future__ import annotations

import base64
import datetime
from collections.abc import Callable, Mapping, Sequence
from functools import cached_property, partial
from typing import TYPE_CHECKING, Any, Literal, TypeVar, Unpack

from lxml.etree import XMLSyntaxError
from pydantic import ValidationError
from pydantic_xml import ParsingError
from requests import Response as RequestsResponse

from palace.manager.api.model.token import OAuthTokenResponse
from palace.manager.integration.license.boundless.constants import (
    API_BASE_URLS,
    LICENSE_SERVER_BASE_URLS,
)
from palace.manager.integration.license.boundless.exception import (
    BoundlessLicenseError,
    BoundlessValidationError,
    StatusResponseParser,
)
from palace.manager.integration.license.boundless.model.base import (
    BaseBoundlessResponse,
)
from palace.manager.integration.license.boundless.model.json import (
    AudiobookMetadataResponse,
    FulfillmentInfoResponse,
    FulfillmentInfoResponseT,
    LicenseServerStatus,
    TitleLicenseResponse,
)
from palace.manager.integration.license.boundless.model.xml import (
    AddHoldResponse,
    AvailabilityResponse,
    CheckoutResponse,
    EarlyCheckinResponse,
    RemoveHoldResponse,
)
from palace.manager.util.http.exception import BadResponseException
from palace.manager.util.http.http import HTTP, RequestKwargs
from palace.manager.util.log import LoggerMixin
from palace.manager.util.sentinel import SentinelType

if TYPE_CHECKING:
    from palace.manager.integration.license.boundless.settings import BoundlessSettings


class BoundlessRequests(LoggerMixin):
    """
    Make requests to the Boundless (Axis 360) API.

    Handles authentication, bearer token management, and
    response parsing.
    """

    DATE_FORMAT = "%m-%d-%Y %H:%M:%S"

    def __init__(self, settings: BoundlessSettings) -> None:
        self._library_id = settings.external_account_id
        self._username = settings.username
        self._password = settings.password
        self._timeout = settings.timeout if settings.timeout > 0 else None

        # Convert the nickname for a server into an actual URL.
        self._base_url = API_BASE_URLS[settings.server_nickname]
        self._license_server_url = LICENSE_SERVER_BASE_URLS[settings.server_nickname]

        self._cached_bearer_token: OAuthTokenResponse | None = None
        self._verify_certificate = settings.verify_certificate

    @classmethod
    def _make_request(
        cls, http_method: str, url: str, **kwargs: Unpack[RequestKwargs]
    ) -> RequestsResponse:
        """Actually make an HTTP request."""
        return HTTP.request_with_timeout(http_method, url, **kwargs)

    @cached_property
    def _refresh_bearer_token_auth_headers(self) -> dict[str, str]:
        authorization = ":".join([self._username, self._password, self._library_id])
        authorization_encoded = authorization.encode("utf_16_le")
        authorization_b64 = base64.standard_b64encode(authorization_encoded).decode()
        return dict(Authorization="Basic " + authorization_b64)

    def refresh_bearer_token(self) -> OAuthTokenResponse:
        url = self._base_url + "accesstoken"
        headers = self._refresh_bearer_token_auth_headers
        response = self._make_request(
            "post", url, headers=headers, allowed_response_codes=["2xx"]
        )
        token = OAuthTokenResponse.model_validate_json(response.content)
        self._cached_bearer_token = token
        return token

    @property
    def _bearer_token(self) -> str:
        if self._cached_bearer_token is None or self._cached_bearer_token.expired:
            token = self.refresh_bearer_token()
        else:
            token = self._cached_bearer_token

        return f"{token.token_type} {token.access_token}"

    _TBoundlessResponse = TypeVar("_TBoundlessResponse", bound=BaseBoundlessResponse)

    def _request(
        self,
        http_method: str,
        url: str,
        response_parser: Callable[[bytes], _TBoundlessResponse],
        params: Mapping[str, Any] | None = None,
        timeout: int | None | Literal[SentinelType.NotGiven] = SentinelType.NotGiven,
    ) -> _TBoundlessResponse:
        """
        Make an HTTP request, acquiring/refreshing a bearer token if necessary.
        """
        headers = {
            "Authorization": self._bearer_token,
            "Library": self._library_id,
        }
        _make_request = partial(
            self._make_request,
            http_method,
            url,
            headers=headers,
            params=params,
            verify=self._verify_certificate,
            # TODO: Hopefully B&T will fix the performance of their endpoints and we can remove
            #  this custom timeout eventually. We should be able to query our logs to see how long
            #  the requests are taking.
            timeout=self._timeout if timeout is SentinelType.NotGiven else timeout,
        )

        response = _make_request()
        if response.status_code == 401:
            parsed_error = StatusResponseParser.parse(response.content)
            if parsed_error is None or parsed_error.code in [1001, 1002]:
                # The token is probably expired. Get a new token and try again.
                # These status codes mean:
                #   1001: Invalid token
                #   1002: Token expired
                self.refresh_bearer_token()
                response = _make_request()

        try:
            parsed_response = response_parser(response.content)
        except (ValidationError, ParsingError, XMLSyntaxError) as e:
            # We were unable to validate or parse the response.
            self.log.exception(
                "Unable to parse response. Exception: %s Status: %s Data: %s",
                str(e),
                response.status_code,
                response.text,
            )

            # If we are able to get a status code and message from the response,
            # we can raise a more specific error.
            StatusResponseParser.parse_and_raise(response.content)

            # The best we can do is raise a generic validation error.
            raise BoundlessValidationError(
                response.url,
                "Unexpected response from Boundless API",
                response,
                debug_message=str(e),
            ) from e
        parsed_response.raise_on_error()
        return parsed_response

    def availability(
        self,
        patron_id: str | None | None = None,
        since: datetime.datetime | None = None,
        title_ids: Sequence[str] | None = None,
        timeout: int | None | Literal[SentinelType.NotGiven] = SentinelType.NotGiven,
    ) -> AvailabilityResponse:
        url = self._base_url + "availability/v2"
        params = {}
        if since:
            since_str = since.strftime(self.DATE_FORMAT)
            params["updatedDate"] = since_str
        if patron_id:
            params["patronId"] = patron_id
        if title_ids:
            params["titleIds"] = ",".join(title_ids)
        response = self._request(
            "GET", url, AvailabilityResponse.from_xml, params=params, timeout=timeout
        )
        return response

    def fulfillment_info(self, transaction_id: str) -> FulfillmentInfoResponseT:
        """Make a call to the getFulfillmentInfoAPI."""
        url = self._base_url + "getfullfillmentInfo/v2"
        params = {"TransactionID": transaction_id}
        response = self._request(
            "POST",
            url,
            FulfillmentInfoResponse.validate_json,
            params=params,
        )
        return response

    def audiobook_metadata(self, findaway_content_id: str) -> AudiobookMetadataResponse:
        """Make a call to the getaudiobookmetadata endpoint."""
        url = self._base_url + "getaudiobookmetadata/v2"
        params = {"fndcontentid": findaway_content_id}
        response = self._request(
            "POST",
            url,
            AudiobookMetadataResponse.model_validate_json,
            params=params,
        )
        return response

    def early_checkin(
        self, title_id: str, patron_id: str | None
    ) -> EarlyCheckinResponse:
        """Make a request to the EarlyCheckInTitle endpoint."""
        url = self._base_url + "EarlyCheckInTitle/v3"
        params = {
            "itemID": title_id,
            "patronID": patron_id,
        }
        response = self._request(
            "GET", url, EarlyCheckinResponse.from_xml, params=params
        )
        return response

    def checkout(
        self, title_id: str, patron_id: str | None, internal_format: str
    ) -> CheckoutResponse:
        url = self._base_url + "checkout/v2"
        params = {"titleId": title_id, "patronId": patron_id, "format": internal_format}
        response = self._request("POST", url, CheckoutResponse.from_xml, params=params)
        return response

    def add_hold(
        self, title_id: str, patron_id: str | None, hold_notification_email: str | None
    ) -> AddHoldResponse:
        url = self._base_url + "addtoHold/v2"
        params = {
            "titleId": title_id,
            "patronId": patron_id,
            "email": hold_notification_email,
        }
        response = self._request("GET", url, AddHoldResponse.from_xml, params=params)
        return response

    def remove_hold(self, title_id: str, patron_id: str | None) -> RemoveHoldResponse:
        url = self._base_url + "removeHold/v2"
        params = {"titleId": title_id, "patronId": patron_id}
        response = self._request("GET", url, RemoveHoldResponse.from_xml, params=params)
        return response

    def license(
        self,
        book_vault_uuid: str,
        device_id: str,
        client_ip: str,
        isbn: str,
        modulus: str,
        exponent: str,
    ) -> dict[str, Any]:
        """
        Make a request to the license server to fetch a license document.

        This function does not use the normal request flow because the license server
        uses a different URL structure, response format, and it does not require the
        same authentication as the main API.

        This function doesn't parse the response, but rather just fetch the license
        document as bytes and passes it back to the caller.
        """
        url = (
            self._license_server_url
            + f"license/{book_vault_uuid}/{device_id}/{client_ip}/{isbn}/{modulus}/{exponent}"
        )
        try:
            response = self._make_request("GET", url, allowed_response_codes=["2xx"])
        except BadResponseException as e:
            self.log.exception(
                "Error fetching license document: %s. "
                "book_vault_uuid=%s device_id=%s client_ip=%s isbn=%s modulus=%s exponent=%s "
                "Status=%d. Content=%s",
                str(e),
                book_vault_uuid,
                device_id,
                client_ip,
                isbn,
                modulus,
                exponent,
                e.response.status_code,
                e.response.text,
            )
            try:
                parsed_error = LicenseServerStatus.model_validate_json(
                    e.response.content
                )
                raise BoundlessLicenseError(parsed_error, e.response.status_code) from e
            except ValidationError:
                # If we can't parse the error, just raise the original exception.
                ...

            raise

        return response.json()  # type: ignore[no-any-return]

    def title_license(
        self,
        modified_since: datetime.datetime,
        page: int = 1,
        timeout: int | None | Literal[SentinelType.NotGiven] = SentinelType.NotGiven,
    ) -> TitleLicenseResponse:
        """
        Make a call to the Title License API to retrieve title license information.

        This returns title license information based on the modified date, allowing
        retrieval of updated content since the last retrieval date.
        """
        url = self._base_url + "titleLicense/v3"
        modified_since_str = modified_since.strftime(self.DATE_FORMAT)
        params = {
            "modifiedSince": modified_since_str,
            "page": str(page),
        }
        response = self._request(
            "GET",
            url,
            TitleLicenseResponse.model_validate_json,
            params=params,
            timeout=timeout,
        )
        return response

    def encrypted_content_url(
        self,
        isbn: str,
    ) -> str:
        """
        Get the URL to download the encrypted content for a given ISBN.
        """
        return self._license_server_url + f"content/download/{isbn}"

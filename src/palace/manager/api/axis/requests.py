from __future__ import annotations

import base64
import datetime
from collections.abc import Callable, Mapping
from functools import cached_property, partial
from typing import TYPE_CHECKING, Any, Literal, TypeVar

from lxml.etree import XMLSyntaxError
from pydantic import ValidationError
from pydantic_xml import ParsingError
from requests import Response as RequestsResponse
from typing_extensions import Unpack

from palace.manager.api.axis.constants import API_BASE_URLS, LICENSE_SERVER_BASE_URLS
from palace.manager.api.axis.exception import (
    Axis360LicenseError,
    Axis360ValidationError,
    StatusResponseParser,
)
from palace.manager.api.axis.models.base import BaseAxisResponse
from palace.manager.api.axis.models.json import (
    AudiobookMetadataResponse,
    FulfillmentInfoResponse,
    FulfillmentInfoResponseT,
    LicenseServerStatus,
    Token,
)
from palace.manager.api.axis.models.xml import (
    AddHoldResponse,
    AvailabilityResponse,
    CheckoutResponse,
    EarlyCheckinResponse,
    RemoveHoldResponse,
)
from palace.manager.util.http import HTTP, BadResponseException, RequestKwargs
from palace.manager.util.log import LoggerMixin
from palace.manager.util.sentinel import SentinelType

if TYPE_CHECKING:
    from palace.manager.api.axis.settings import Axis360Settings


class Axis360Requests(LoggerMixin):
    """
    Make requests to the Axis 360 API.

    Handles authentication, bearer token management, and
    response parsing.
    """

    DATE_FORMAT = "%m-%d-%Y %H:%M:%S"

    def __init__(self, settings: Axis360Settings) -> None:
        self._library_id = settings.external_account_id
        self._username = settings.username
        self._password = settings.password

        # Convert the nickname for a server into an actual URL.
        self._base_url = API_BASE_URLS[settings.server_nickname]
        self._license_server_url = LICENSE_SERVER_BASE_URLS[settings.server_nickname]

        self._cached_bearer_token: Token | None = None
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

    def refresh_bearer_token(self) -> Token:
        url = self._base_url + "accesstoken"
        headers = self._refresh_bearer_token_auth_headers
        response = self._make_request(
            "post", url, headers=headers, allowed_response_codes=["2xx"]
        )
        token = Token.model_validate_json(response.content)
        self._cached_bearer_token = token
        return token

    @property
    def _bearer_token(self) -> str:
        if self._cached_bearer_token is None or self._cached_bearer_token.expired:
            token = self.refresh_bearer_token()
        else:
            token = self._cached_bearer_token

        return f"{token.token_type} {token.access_token}"

    _TAxisResponse = TypeVar("_TAxisResponse", bound=BaseAxisResponse)

    def _request(
        self,
        http_method: str,
        url: str,
        response_parser: Callable[[bytes], _TAxisResponse],
        params: Mapping[str, Any] | None = None,
        timeout: int | None | Literal[SentinelType.NotGiven] = SentinelType.NotGiven,
    ) -> _TAxisResponse:
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
        )
        if timeout is not SentinelType.NotGiven:
            _make_request = partial(_make_request, timeout=timeout)

        response = _make_request()
        if response.status_code == 401:
            parsed_error = StatusResponseParser.parse(response.content)
            if parsed_error is None or parsed_error.code in [1001, 1002]:
                # The token is probably expired. Get a new token and try again.
                # Axis 360's status codes mean:
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
            raise Axis360ValidationError(
                response.url,
                "Unexpected response from Axis 360 API",
                response,
                debug_message=str(e),
            ) from e
        parsed_response.raise_on_error()
        return parsed_response

    def availability(
        self,
        patron_id: str | None | None = None,
        since: datetime.datetime | None = None,
        title_ids: list[str] | None = None,
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
            "GET", url, AvailabilityResponse.from_xml, params=params, timeout=None
        )
        return response

    def fulfillment_info(self, transaction_id: str) -> FulfillmentInfoResponseT:
        """Make a call to the getFulfillmentInfoAPI."""
        url = self._base_url + "getfullfillmentInfo/v2"
        params = {"TransactionID": transaction_id}
        # We set an explicit timeout because this request can take a long time and
        # the default was too short. Ideally B&T would fix this on their end, but
        # in the meantime we need to work around it.
        # TODO: Revisit this timeout. Hopefully B&T will fix the performance
        #   of this endpoint and we can remove this. We should be able to query
        #   our logs to see how long these requests are taking.
        response = self._request(
            "POST",
            url,
            FulfillmentInfoResponse.validate_json,
            params=params,
            timeout=15,
        )
        return response

    def audiobook_metadata(self, findaway_content_id: str) -> AudiobookMetadataResponse:
        """Make a call to the getaudiobookmetadata endpoint."""
        url = self._base_url + "getaudiobookmetadata/v2"
        params = {"fndcontentid": findaway_content_id}
        # We set an explicit timeout because this request can take a long time and
        # the default was too short. Ideally B&T would fix this on their end, but
        # in the meantime we need to work around it.
        # TODO: Revisit this timeout. Hopefully B&T will fix the performance
        #   of this endpoint and we can remove this. We should be able to query
        #   our logs to see how long these requests are taking.
        response = self._request(
            "POST",
            url,
            AudiobookMetadataResponse.model_validate_json,
            params=params,
            timeout=15,
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
    ) -> bytes:
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
                raise Axis360LicenseError(parsed_error, e.response.status_code) from e
            except ValidationError:
                # If we can't parse the error, just raise the original exception.
                ...

            raise

        return response.content

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, NamedTuple

from lxml import etree

from palace.manager.api.circulation.exceptions import (
    AlreadyCheckedOut,
    CannotFulfill,
    CannotLoan,
    CirculationException,
    CurrentlyAvailable,
    InvalidInputException,
    LibraryAuthorizationFailedException,
    LibraryInvalidInputException,
    NoAcceptableFormat,
    NoActiveLoan,
    NoAvailableCopies,
    NotFoundOnRemote,
    PatronAuthorizationFailedException,
    PatronLoanLimitReached,
    RemoteInitiatedServerError,
)
from palace.manager.core.exceptions import BasePalaceException
from palace.manager.util.http.exception import BadResponseException
from palace.manager.util.problem_detail import BaseProblemDetailException, ProblemDetail

if TYPE_CHECKING:
    from palace.manager.integration.license.boundless.model.json import (
        LicenseServerStatus,
    )


class BoundlessException(BasePalaceException): ...


class BoundlessValidationError(BadResponseException, BoundlessException):
    """
    Raise when we are unable to validate a response from Boundless (Axis 360).
    """

    ...


class BoundlessLicenseError(BoundlessException, BaseProblemDetailException):
    """
    Raise when there is a license-related error.
    """

    def __init__(self, status_doc: LicenseServerStatus, http_status_code: int) -> None:
        self._status_doc = status_doc
        self._http_status_code = http_status_code
        super().__init__(self._status_doc.title)

    @property
    def uri(self) -> str:
        return (
            f"http://palaceproject.io/terms/problem/boundless/{self._status_doc.code}"
        )

    @property
    def problem_detail(self) -> ProblemDetail:
        return ProblemDetail(
            uri=self.uri,
            status_code=self._http_status_code,
            title=self._status_doc.title,
            detail=self._status_doc.message,
        )


ErrorType = type[CirculationException | RemoteInitiatedServerError]
ErrorLookupType = Mapping[int | tuple[int, str], ErrorType]


class BoundlessStatusTuple(NamedTuple):
    """
    A named tuple to hold the status code and message from a Boundless response.
    """

    code: int
    message: str | None


class StatusResponseParser:
    SERVICE_NAME = "Boundless"
    NAMESPACES = {"axis": "http://axis360api.baker-taylor.com/vendorAPI"}

    # Map Boundless error codes to our circulation exceptions.
    CODE_TO_EXCEPTION: ErrorLookupType = {
        315: InvalidInputException,  # Bad password
        316: InvalidInputException,  # DRM account already exists
        1000: PatronAuthorizationFailedException,
        1001: PatronAuthorizationFailedException,
        1002: PatronAuthorizationFailedException,
        1003: PatronAuthorizationFailedException,
        2000: LibraryAuthorizationFailedException,
        2001: LibraryAuthorizationFailedException,
        2002: LibraryAuthorizationFailedException,
        2003: LibraryAuthorizationFailedException,  # "Encoded input parameters exceed limit", whatever that means
        2004: LibraryAuthorizationFailedException,  # Authorization string is not properly encoded
        2005: LibraryAuthorizationFailedException,  # Invalid credentials
        2006: LibraryAuthorizationFailedException,  # Library ID not associated with given vendor
        2007: LibraryAuthorizationFailedException,  # Invalid library ID
        2008: LibraryAuthorizationFailedException,  # Invalid library ID
        3100: LibraryInvalidInputException,  # Missing title ID
        3101: LibraryInvalidInputException,  # Missing patron ID
        3102: LibraryInvalidInputException,  # Missing email address (for hold notification)
        3103: NotFoundOnRemote,  # Invalid title ID
        3104: LibraryInvalidInputException,  # Invalid Email Address (for hold notification)
        3105: PatronAuthorizationFailedException,  # Invalid Account Credentials
        3106: InvalidInputException,  # Loan Period is out of bounds
        3108: InvalidInputException,  # DRM Credentials Required
        3109: InvalidInputException,  # Hold already exists or hold does not exist, depending.
        3110: AlreadyCheckedOut,
        3111: CurrentlyAvailable,
        3112: CannotFulfill,
        3113: CannotLoan,
        (3113, "Title ID is not available for checkout"): NoAvailableCopies,
        3114: PatronLoanLimitReached,
        3115: LibraryInvalidInputException,  # Missing DRM format
        3116: LibraryInvalidInputException,  # No patron session ID provided -- we don't use this
        3117: LibraryInvalidInputException,  # Invalid DRM format
        3118: LibraryInvalidInputException,  # Invalid Patron credentials
        3119: LibraryAuthorizationFailedException,  # No Blio account
        3120: LibraryAuthorizationFailedException,  # No Acoustikaccount
        3123: PatronAuthorizationFailedException,  # Patron Session ID expired
        3124: PatronAuthorizationFailedException,  # Patron SessionID is required
        3126: LibraryInvalidInputException,  # Invalid checkout format
        3127: InvalidInputException,  # First name is required
        3128: InvalidInputException,  # Last name is required
        3129: PatronAuthorizationFailedException,  # Invalid Patron Session Id
        3130: LibraryInvalidInputException,  # Invalid hold format (?)
        3131: RemoteInitiatedServerError,  # Custom error message (?)
        3132: LibraryInvalidInputException,  # Invalid delta datetime format
        3134: LibraryInvalidInputException,  # Delta datetime format must not be in the future
        3135: NoAcceptableFormat,
        3136: LibraryInvalidInputException,  # Missing checkout format
        4058: NoActiveLoan,  # No checkout is associated with patron for the title.
        5000: RemoteInitiatedServerError,
        5003: LibraryInvalidInputException,  # Missing TransactionID
        5004: LibraryInvalidInputException,  # Missing TransactionID
    }

    @classmethod
    def _do_raise(cls, error_class: ErrorType, message: str | None) -> None:
        """
        Raise an exception with the given class and message.
        This is a helper method to avoid repeating the raise statement.
        """
        if issubclass(error_class, RemoteInitiatedServerError):
            raise error_class(message or "error", cls.SERVICE_NAME)

        raise error_class(message)

    @classmethod
    def raise_on_error(
        cls,
        code: int,
        message: str | None = None,
        custom_error_classes: ErrorLookupType | None = None,
        ignore_error_codes: list[int] | None = None,
    ) -> None:
        """
        Raise an exception based on the error code and message.
        """

        if ignore_error_codes and code in ignore_error_codes:
            return

        if custom_error_classes is None:
            custom_error_classes = {}
        for d in custom_error_classes, cls.CODE_TO_EXCEPTION:
            if message is not None and (code, message) in d:
                cls._do_raise(d[(code, message)], message)
            elif code in d:
                cls._do_raise(d[code], message)

    @classmethod
    def parse_xml(cls, data: bytes) -> BoundlessStatusTuple | None:
        """
        Best effort attempt to parse an XML response,
        returning a tuple of (status_code, message) if successful,
        or None if parsing fails or the response is not valid.
        """

        parser = etree.XMLParser(recover=True)

        try:
            parsed = etree.fromstring(data, parser=parser)
            if parsed is None:
                return None

            status_results = parsed.xpath(
                "//axis:status/axis:code", namespaces=cls.NAMESPACES
            )
            if not status_results or not status_results[0].text:
                return None
            status_code = int(status_results[0].text)

            message_results = parsed.xpath(
                "//axis:status/axis:statusMessage", namespaces=cls.NAMESPACES
            )
            message = (
                None
                if not message_results or not message_results[0].text
                else str(message_results[0].text)
            )

            return BoundlessStatusTuple(status_code, message)
        except (etree.XMLSyntaxError, ValueError, TypeError):
            return None

    @classmethod
    def parse_json(cls, data: bytes) -> BoundlessStatusTuple | None:
        """
        Best effort attempt to parse an JSON response from,
        returning a tuple of (status_code, message) if successful,
        or None if parsing fails or the response is not valid.
        """
        try:
            response_data = json.loads(data)
            assert isinstance(response_data, dict)
            status = response_data.get("Status", {})
            code = status.get("Code")
            message = status.get("Message")
            if code is None:
                return None
            return BoundlessStatusTuple(
                int(code), str(message) if message is not None else None
            )
        except (AssertionError, ValueError, TypeError):
            return None

    @classmethod
    def parse(cls, data: bytes) -> BoundlessStatusTuple | None:
        """
        Parse the given data as either XML or JSON, returning
        a tuple of (status_code, message) if successful or None
        if parsing fails or the response is not valid.
        """

        return cls.parse_xml(data) or cls.parse_json(data)

    @classmethod
    def parse_and_raise(cls, data: bytes) -> BoundlessStatusTuple | None:
        """
        Parse the given data and raise an exception if we are able to parse
        it and the status code indicates an error that we have an exception for.
        """

        if (parsed := cls.parse(data)) is None:
            return parsed

        cls.raise_on_error(parsed.code, parsed.message)
        return parsed

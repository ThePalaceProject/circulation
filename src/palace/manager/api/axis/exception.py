from __future__ import annotations

import json
from collections.abc import Mapping

from lxml import etree

from palace.manager.api.circulation_exceptions import (
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
from palace.manager.util.http import BadResponseException


class Axis360ValidationError(BadResponseException):
    """
    Raise when we are unable to validate a response from Axis 360.
    """

    ...


ErrorType = type[CirculationException | RemoteInitiatedServerError]
ErrorLookupType = Mapping[int | tuple[int, str], ErrorType]


class StatusResponseParser:
    SERVICE_NAME = "Axis 360"
    NAMESPACES = {"axis": "http://axis360api.baker-taylor.com/vendorAPI"}

    # Map Axis 360 error codes to our circulation exceptions.
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
    def _do_raise(cls, error_class: ErrorType, message: str) -> None:
        """
        Raise an exception with the given class and message.
        This is a helper method to avoid repeating the raise statement.
        """
        if issubclass(error_class, RemoteInitiatedServerError):
            raise error_class(message, cls.SERVICE_NAME)

        raise error_class(message)

    @classmethod
    def raise_on_error(
        cls,
        code: int,
        message: str,
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
            if (code, message) in d:
                cls._do_raise(d[(code, message)], message)
            elif code in d:
                cls._do_raise(d[code], message)

    @classmethod
    def parse_xml(cls, data: bytes) -> tuple[int, str] | None:
        """
        Best effort attempt to parse an XML response from Axis 360,
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
            if not message_results or not message_results[0].text:
                return None
            message = str(message_results[0].text)

            return status_code, message
        except (etree.XMLSyntaxError, AssertionError, ValueError):
            return None

    @classmethod
    def parse_json(cls, data: bytes) -> tuple[int, str] | None:
        """
        Best effort attempt to parse an JSON response from Axis 360,
        returning a tuple of (status_code, message) if successful,
        or None if parsing fails or the response is not valid.
        """
        try:
            response_data = json.loads(data)
            assert isinstance(response_data, dict)
            status = response_data.get("Status", {})
            code = status.get("Code")
            message = status.get("Message")
            if not isinstance(code, str) or not isinstance(message, str):
                return None
            return int(code), str(message)
        except (AssertionError, ValueError, TypeError):
            return None

    @classmethod
    def parse(cls, data: bytes) -> tuple[int, str] | None:
        """
        Parse the given data as either XML or JSON, returning
        a tuple of (status_code, message) if successful or None
        if parsing fails or the response is not valid.
        """

        return cls.parse_xml(data) or cls.parse_json(data)

    @classmethod
    def parse_and_raise(cls, data: bytes) -> tuple[int, str] | None:
        """
        Parse the given data and raise an exception if we are able to parse
        it and the status code indicates an error that we have an exception for.
        """

        if (parsed := cls.parse(data)) is None:
            return parsed

        code, message = parsed
        cls.raise_on_error(code, message)
        return code, message

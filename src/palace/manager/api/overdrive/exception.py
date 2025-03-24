from typing import cast

from requests import Response
from typing_extensions import Self

from palace.manager.api.circulation_exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    CannotRenew,
    CirculationException,
    NoActiveLoan,
    NoAvailableCopies,
    PatronHoldLimitReached,
    PatronLoanLimitReached,
)
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.util.http import BadResponseException
from palace.manager.util.problem_detail import ProblemDetail


class OverdriveResponseException(BadResponseException):
    def __init__(
        self,
        error_code: str | None,
        error_message: str,
        response: Response,
    ) -> None:
        super().__init__(
            url_or_service=response.url, message=error_message, response=response
        )

        self.error_code = error_code
        self.error_message = error_message

    @property
    def uri(self) -> str:
        error_code = self.error_code
        if error_code is None:
            error_code = "unknown"
        return f"http://palaceproject.io/terms/problem/overdrive/{error_code}"

    @property
    def problem_detail(self) -> ProblemDetail:
        return ProblemDetail(
            uri=self.uri,
            status_code=self.response.status_code,
            title="OverDrive Error",
            detail=self.error_message,
        )

    @classmethod
    def from_bad_response(
        cls, bad_response: BadResponseException
    ) -> CirculationException | Self:
        """
        Raise an appropriate problem detail exception, when a bad response exception
        is encountered while making an Overdrive request.
        """

        # Overdrive usually gives a helpful response to errors, see if the response we got
        # contains this info.
        response = bad_response.response
        try:
            json_data = response.json()
        except:
            json_data = {}
        if not isinstance(json_data, dict):
            json_data = {}

        error_code: str | None = json_data.get("errorCode")
        error_message: str | None = json_data.get("message")

        if error_code == "TitleNotCheckedOut":
            return NoActiveLoan(error_message)
        elif error_code == "NoCopiesAvailable":
            return NoAvailableCopies(error_message)
        elif (
            error_code == "PatronHasExceededCheckoutLimit"
            or error_code == "PatronHasExceededCheckoutLimit_ForCPC"
        ):
            return PatronLoanLimitReached(error_message)
        elif error_code == "TitleAlreadyCheckedOut":
            return AlreadyCheckedOut(error_message)
        elif error_code == "AlreadyOnWaitList":
            # The book is already on hold.
            return AlreadyOnHold()
        elif error_code == "NotWithinRenewalWindow":
            # The patron has this book checked out and cannot yet
            # renew their loan.
            return CannotRenew()
        elif error_code == "PatronExceededHoldLimit":
            return PatronHoldLimitReached()

        if error_message is None:
            error_message = cast(str, bad_response.message)

        return cls(error_code, error_message, response)


class MissingSubstitutionsError(PalaceValueError):
    """
    Raised when templating a LinkTemplate, and some of the required
    substitutions are missing.
    """

    def __init__(self, missing: set[str]) -> None:
        super().__init__(f"Missing substitutions: {', '.join(sorted(missing))}")


class FieldNotFoundError(PalaceValueError):
    """
    Raised when a field is not found in an Action.
    """

    def __init__(self, name: str, camel_name: str | None) -> None:
        message = f"No field found with name: {name}"
        if camel_name and name != camel_name:
            message += f" ({camel_name})"
        super().__init__(message)


class MissingRequiredFieldError(PalaceValueError):
    """
    Raised when a required field is missing in an Action.
    """

    def __init__(self, name: str) -> None:
        super().__init__(f"Missing required field: {name}")


class InvalidFieldOptionError(PalaceValueError):
    """
    Raised when a field in an Action has an invalid value.
    """

    def __init__(self, field: str, value: str, options: set[str]) -> None:
        super().__init__(
            f"Invalid value for field {field}: {value}. Valid options: {', '.join(sorted(options))}"
        )

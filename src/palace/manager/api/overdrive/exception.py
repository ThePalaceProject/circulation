from collections.abc import Iterable
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
from palace.manager.core.exceptions import BasePalaceException
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


class OverdriveModelError(BasePalaceException):
    """
    Raised when there is an error in one of our Overdrive models.
    """

    ...


class MissingSubstitutionsError(OverdriveModelError):
    """
    Raised when templating a LinkTemplate, and some of the required
    substitutions are missing.
    """

    def __init__(self, missing: set[str]) -> None:
        super().__init__(f"Missing substitutions: {', '.join(sorted(missing))}")


class MissingRequiredFieldError(OverdriveModelError):
    """
    Raised when a required field is missing in an Action.
    """

    def __init__(self, name: str) -> None:
        super().__init__(f"Action missing required field: {name}")


class InvalidFieldOptionError(OverdriveModelError):
    """
    Raised when a field in an Action has an invalid value.
    """

    def __init__(self, field: str, value: str, options: set[str]) -> None:
        super().__init__(
            f"Invalid value for action field {field}: {value}. Valid options: {', '.join(sorted(options))}"
        )


class ExtraFieldsError(OverdriveModelError):
    """
    Raised when an action is called with a field that is not
    defined in the action.
    """

    def __init__(self, fields: Iterable[str]) -> None:
        field_str = ", ".join(sorted(fields))
        super().__init__(f"Extra fields for action: {field_str}")


class NotFoundError(OverdriveModelError):
    """
    Raised when a field / value is not found in the Overdrive API response models.
    """

    def __init__(
        self, name: str, type: str, available: Iterable[str] | None = None
    ) -> None:
        message = f"{type.capitalize()} not found: {name}."
        if available:
            sorted_available = sorted(available)
            message += (
                f" Available {type}{'s' if len(sorted_available) != 1 else ''}: "
                f"{', '.join(sorted(available))}"
            )
        super().__init__(message)

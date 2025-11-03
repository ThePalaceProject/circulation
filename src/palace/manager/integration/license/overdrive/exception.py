from __future__ import annotations

from collections.abc import Iterable

from requests import Response

from palace.manager.core.exceptions import BasePalaceException
from palace.manager.util.http.exception import BadResponseException, HttpResponse
from palace.manager.util.problem_detail import ProblemDetail


class OverdriveResponseException(BadResponseException):
    def __init__(
        self,
        error_message: str,
        error_code: str | None,
        token: str | None,
        response: Response | HttpResponse,
    ) -> None:
        super().__init__(
            url_or_service=response.url, message=error_message, response=response
        )

        self.error_code = error_code
        self.error_message = error_message
        self.token = token

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


class OverdriveModelError(BasePalaceException):
    """
    Raised when there is an error in one of our Overdrive models.
    """

    ...


class OverdriveValidationError(BadResponseException, OverdriveModelError):
    """
    Raise when we are unable to validate a response from Overdrive.
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

from abc import ABC, abstractmethod

from flask_babel import lazy_gettext as _

from palace.manager.api.problem_details import (
    BAD_DELIVERY_MECHANISM,
    BLOCKED_CREDENTIALS,
    CANNOT_FULFILL,
    CANNOT_RELEASE_HOLD,
    CHECKOUT_FAILED,
    COULD_NOT_MIRROR_TO_REMOTE,
    DELIVERY_CONFLICT,
    EXPIRED_CREDENTIALS,
    HOLD_FAILED,
    HOLD_LIMIT_REACHED,
    HOLDS_NOT_PERMITTED,
    INVALID_CREDENTIALS,
    LOAN_LIMIT_REACHED,
    NO_ACCEPTABLE_FORMAT,
    NO_ACTIVE_LOAN,
    NO_LICENSES,
    NOT_FOUND_ON_REMOTE,
    OUTSTANDING_FINES,
    RENEW_FAILED,
    SPECIFIC_HOLD_LIMIT_MESSAGE,
    SPECIFIC_LOAN_LIMIT_MESSAGE,
)
from palace.manager.core.exceptions import IntegrationException
from palace.manager.core.problem_details import (
    INTEGRATION_ERROR,
    INTERNAL_SERVER_ERROR,
    INVALID_INPUT,
)
from palace.manager.util import MoneyUtility
from palace.manager.util.problem_detail import BaseProblemDetailException, ProblemDetail


class CirculationException(IntegrationException, BaseProblemDetailException, ABC):
    """An exception occurred when carrying out a circulation operation."""

    def __init__(
        self, message: str | None = None, debug_info: str | None = None
    ) -> None:
        super().__init__(message or self.__class__.__name__, debug_info)
        self.message = message

    @property
    def detail(self) -> str | None:
        return self.message

    @property
    @abstractmethod
    def base(self) -> ProblemDetail:
        """A ProblemDetail, used as the basis for conversion of this exception into a
        problem detail document."""

    @property
    def problem_detail(self) -> ProblemDetail:
        """Return a suitable problem detail document."""
        if self.detail is not None:
            return self.base.detailed(
                detail=self.detail, debug_message=self.debug_message
            )
        elif self.debug_message is not None:
            return self.base.with_debug(self.debug_message)
        else:
            return self.base


class InternalServerError(IntegrationException, BaseProblemDetailException):
    @property
    def problem_detail(self) -> ProblemDetail:
        return INTERNAL_SERVER_ERROR


class RemoteInitiatedServerError(InternalServerError):
    """One of the servers we communicate with had an internal error."""

    def __init__(self, debug_info: str, service_name: str):
        super().__init__(debug_info)
        self.service_name = service_name

    @property
    def problem_detail(self) -> ProblemDetail:
        msg = _(
            "Integration error communicating with %(service_name)s",
            service_name=self.service_name,
        )
        return INTEGRATION_ERROR.detailed(msg, debug_message=str(self))


class PatronAuthorizationFailedException(CirculationException):
    @property
    def base(self) -> ProblemDetail:
        return INVALID_CREDENTIALS


class LibraryAuthorizationFailedException(CirculationException):
    @property
    def base(self) -> ProblemDetail:
        return INTEGRATION_ERROR


class InvalidInputException(CirculationException):
    """The patron gave invalid input to the library."""

    @property
    def base(self) -> ProblemDetail:
        return INVALID_INPUT.detailed("The patron gave invalid input to the library.")


class LibraryInvalidInputException(InvalidInputException):
    """The library gave invalid input to the book provider."""

    @property
    def base(self) -> ProblemDetail:
        return INVALID_INPUT.detailed(
            "The library gave invalid input to the book provider."
        )


class DeliveryMechanismError(InvalidInputException):
    """The patron broke the rules about delivery mechanisms."""

    @property
    def base(self) -> ProblemDetail:
        return BAD_DELIVERY_MECHANISM


class DeliveryMechanismMissing(DeliveryMechanismError):
    """The patron needed to specify a delivery mechanism and didn't."""


class DeliveryMechanismConflict(DeliveryMechanismError):
    """The patron specified a delivery mechanism that conflicted with
    one already set in stone.
    """

    @property
    def base(self) -> ProblemDetail:
        return DELIVERY_CONFLICT


class CannotLoan(CirculationException):
    @property
    def base(self) -> ProblemDetail:
        return CHECKOUT_FAILED


class OutstandingFines(CannotLoan):
    """The patron has outstanding fines above the limit in the library's
    policy."""

    def __init__(
        self,
        message: str | None = None,
        debug_info: str | None = None,
        fines: str | None = None,
    ) -> None:
        parsed_fines = None
        if fines:
            try:
                parsed_fines = MoneyUtility.parse(fines)
            except ValueError:
                # If the fines are not in a valid format, we'll just leave them as None.
                ...

        self.fines = parsed_fines
        super().__init__(message, debug_info)

    @property
    def detail(self) -> str | None:
        if self.fines:
            return _(  # type: ignore[no-any-return]
                "You must pay your $%(fine_amount).2f outstanding fines before you can borrow more books.",
                fine_amount=self.fines,
            )
        return super().detail

    @property
    def base(self) -> ProblemDetail:
        return OUTSTANDING_FINES


class AuthorizationExpired(CannotLoan):
    """The patron's authorization has expired."""

    @property
    def base(self) -> ProblemDetail:
        return EXPIRED_CREDENTIALS


class AuthorizationBlocked(CannotLoan):
    """The patron's authorization is blocked for some reason other than
    fines or an expired card.

    For instance, the patron has been banned from the library.
    """

    @property
    def base(self) -> ProblemDetail:
        return BLOCKED_CREDENTIALS


class LimitReached(CirculationException, ABC):
    """The patron cannot carry out an operation because it would push them above
    some limit set by library policy.
    """

    def __init__(
        self,
        message: str | None = None,
        debug_info: str | None = None,
        limit: int | None = None,
    ):
        super().__init__(message, debug_info=debug_info)
        self.limit = limit

    @property
    @abstractmethod
    def message_with_limit(self) -> str:
        """A string containing the interpolation value "%(limit)s", which
        offers a more specific explanation of the limit exceeded."""

    @property
    def detail(self) -> str | None:
        if self.limit:
            return self.message_with_limit % dict(limit=self.limit)
        elif self.message:
            return self.message
        return None


class PatronLoanLimitReached(CannotLoan, LimitReached):
    @property
    def base(self) -> ProblemDetail:
        return LOAN_LIMIT_REACHED

    @property
    def message_with_limit(self) -> str:
        return SPECIFIC_LOAN_LIMIT_MESSAGE  # type: ignore[no-any-return]


class CannotReturn(CirculationException):
    @property
    def base(self) -> ProblemDetail:
        return COULD_NOT_MIRROR_TO_REMOTE


class CannotHold(CirculationException):
    @property
    def base(self) -> ProblemDetail:
        return HOLD_FAILED


class HoldsNotPermitted(CannotHold):
    @property
    def base(self) -> ProblemDetail:
        return HOLDS_NOT_PERMITTED


class PatronHoldLimitReached(CannotHold, LimitReached):
    @property
    def base(self) -> ProblemDetail:
        return HOLD_LIMIT_REACHED

    @property
    def message_with_limit(self) -> str:
        return SPECIFIC_HOLD_LIMIT_MESSAGE  # type: ignore[no-any-return]


class CannotReleaseHold(CirculationException):
    @property
    def base(self) -> ProblemDetail:
        return CANNOT_RELEASE_HOLD


class CannotFulfill(CirculationException):
    @property
    def base(self) -> ProblemDetail:
        return CANNOT_FULFILL


class FormatNotAvailable(CannotFulfill):
    """Our format information for this book was outdated, and it's
    no longer available in the requested format."""

    @property
    def base(self) -> ProblemDetail:
        return NO_ACCEPTABLE_FORMAT


class NotFoundOnRemote(CirculationException):
    """We know about this book but the remote site doesn't seem to."""

    @property
    def base(self) -> ProblemDetail:
        return NOT_FOUND_ON_REMOTE


class NoLicenses(NotFoundOnRemote):
    """The library no longer has licenses for this book."""

    @property
    def base(self) -> ProblemDetail:
        return NO_LICENSES


class CannotRenew(CirculationException):
    """The patron can't renew their loan on this book.

    Probably because it's not available for renewal.
    """

    @property
    def base(self) -> ProblemDetail:
        return RENEW_FAILED


class NoAvailableCopies(CannotLoan):
    """The patron can't check this book out because all available
    copies are already checked out.
    """

    @property
    def base(self) -> ProblemDetail:
        return CHECKOUT_FAILED.detailed(detail="No copies available to check out.")


class AlreadyCheckedOut(CannotLoan):
    """The patron can't put check this book out because they already have
    it checked out.
    """

    @property
    def base(self) -> ProblemDetail:
        return CHECKOUT_FAILED.detailed(
            detail="You already have this book checked out."
        )


class AlreadyOnHold(CannotHold):
    """The patron can't put this book on hold because they already have
    it on hold.
    """

    @property
    def base(self) -> ProblemDetail:
        return HOLD_FAILED.detailed(detail="You already have this book on hold.")


class NotCheckedOut(CannotReturn):
    """The patron can't return this book because they don't
    have it checked out in the first place.
    """

    @property
    def base(self) -> ProblemDetail:
        return COULD_NOT_MIRROR_TO_REMOTE.detailed(
            title="Unable to return", detail="You don't have this book checked out."
        )


class NotOnHold(CannotReleaseHold):
    """The patron can't release a hold for this book because they don't
    have it on hold in the first place.
    """


class CurrentlyAvailable(CannotHold):
    """The patron can't put this book on hold because it's available now."""

    @property
    def base(self) -> ProblemDetail:
        return HOLD_FAILED.detailed(detail="Cannot place a hold on an available title.")


class HoldOnUnlimitedAccess(CannotHold):
    """The patron can't put this book on hold because it's an unlimited
    access title, so it is currently available."""

    @property
    def base(self) -> ProblemDetail:
        return HOLD_FAILED.detailed(
            detail="Cannot place a hold on an unlimited access title."
        )


class NoAcceptableFormat(CannotFulfill):
    """We can't fulfill the patron's loan because the book is not available
    in an acceptable format.
    """

    @property
    def base(self) -> ProblemDetail:
        return super().base.detailed("No acceptable format", status_code=400)


class FulfilledOnIncompatiblePlatform(CannotFulfill):
    """We can't fulfill the patron's loan because the loan was already
    fulfilled on an incompatible platform (i.e. Kindle) in a way that's
    exclusive to that platform.
    """

    @property
    def base(self) -> ProblemDetail:
        return super().base.detailed(
            "Fulfilled on an incompatible platform", status_code=451
        )


class NoActiveLoan(CannotFulfill):
    """We can't fulfill the patron's loan because they don't have an
    active loan.
    """

    @property
    def base(self) -> ProblemDetail:
        return NO_ACTIVE_LOAN.detailed(
            "Can't fulfill loan because you have no active loan for this book.",
        )

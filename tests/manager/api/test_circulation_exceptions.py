import pytest
from flask_babel import lazy_gettext as _

from palace.manager.api.circulation_exceptions import (
    AlreadyCheckedOut,
    AlreadyOnHold,
    AuthorizationBlocked,
    AuthorizationExpired,
    CannotFulfill,
    CannotHold,
    CannotLoan,
    CannotReleaseHold,
    CannotRenew,
    CannotReturn,
    CirculationException,
    CurrentlyAvailable,
    DeliveryMechanismConflict,
    DeliveryMechanismError,
    DeliveryMechanismMissing,
    FormatNotAvailable,
    FulfilledOnIncompatiblePlatform,
    InternalServerError,
    InvalidInputException,
    LibraryAuthorizationFailedException,
    LibraryInvalidInputException,
    LimitReached,
    NoAcceptableFormat,
    NoActiveLoan,
    NoAvailableCopies,
    NoLicenses,
    NotCheckedOut,
    NotFoundOnRemote,
    NotOnHold,
    OutstandingFines,
    PatronAuthorizationFailedException,
    PatronHoldLimitReached,
    PatronLoanLimitReached,
    RemoteInitiatedServerError,
)
from palace.manager.api.problem_details import (
    HOLD_LIMIT_REACHED,
    LOAN_LIMIT_REACHED,
    OUTSTANDING_FINES,
)
from palace.manager.core.problem_details import INTEGRATION_ERROR, INTERNAL_SERVER_ERROR
from palace.manager.util.problem_detail import ProblemDetail


class TestCirculationExceptions:
    @pytest.mark.parametrize(
        "exception",
        [
            PatronAuthorizationFailedException,
            LibraryAuthorizationFailedException,
            InvalidInputException,
            LibraryInvalidInputException,
            DeliveryMechanismError,
            DeliveryMechanismMissing,
            DeliveryMechanismConflict,
            CannotLoan,
            AuthorizationExpired,
            AuthorizationBlocked,
            CannotReturn,
            CannotHold,
            CannotReleaseHold,
            CannotFulfill,
            FormatNotAvailable,
            NotFoundOnRemote,
            NoLicenses,
            CannotRenew,
            NoAvailableCopies,
            AlreadyCheckedOut,
            AlreadyOnHold,
            NotCheckedOut,
            NotOnHold,
            CurrentlyAvailable,
            NoAcceptableFormat,
            FulfilledOnIncompatiblePlatform,
            NoActiveLoan,
            OutstandingFines,
            PatronLoanLimitReached,
            PatronHoldLimitReached,
        ],
    )
    def test_problem_detail(self, exception: type[CirculationException]) -> None:
        """Verify that circulation exceptions can be turned into ProblemDetail
        documents.
        """
        e = exception()
        expected_pd = e.base

        assert e.problem_detail == expected_pd

        e_with_detail = exception("A message")
        assert e_with_detail.problem_detail == expected_pd.detailed("A message")

        e_with_debug = exception(debug_info="A debug message")
        assert e_with_debug.problem_detail == expected_pd.with_debug("A debug message")

        e_with_detail_and_debug = exception("A message", "A debug message")
        assert e_with_detail_and_debug.problem_detail == expected_pd.detailed(
            "A message"
        ).with_debug("A debug message")

    def test_outstanding_fines(self) -> None:
        e = OutstandingFines()
        assert e.problem_detail == OUTSTANDING_FINES

        e = OutstandingFines(fines="$5,000,000.001")
        assert e.problem_detail == OUTSTANDING_FINES.detailed(
            "You must pay your $5000000.00 outstanding fines before you can borrow more books."
        )

        e = OutstandingFines(fines="invalid amount")
        assert e.problem_detail == OUTSTANDING_FINES

    def test_limit_reached(self) -> None:
        generic_message = _(
            "You exceeded the limit, but I don't know what the limit was."
        )
        pd = ProblemDetail("http://uri/", 403, _("Limit exceeded."), generic_message)

        class Mock(LimitReached):
            @property
            def message_with_limit(self) -> str:
                return _("The limit was %(limit)d.")

            @property
            def base(self) -> ProblemDetail:
                return pd

        # No limit -> generic message.
        ex = Mock()
        pd = ex.problem_detail
        assert ex.limit is None
        assert generic_message == pd.detail

        # Limit -> specific message.
        ex = Mock(limit=14)
        assert 14 == ex.limit
        pd = ex.problem_detail
        assert "The limit was 14." == pd.detail

    @pytest.mark.parametrize(
        "exception,pd,limit_type",
        [
            (PatronHoldLimitReached, HOLD_LIMIT_REACHED, "hold"),
            (PatronLoanLimitReached, LOAN_LIMIT_REACHED, "loan"),
        ],
    )
    def test_patron_limit_reached(
        self, exception: type[LimitReached], pd: ProblemDetail, limit_type: str
    ) -> None:
        e = exception()
        assert e.problem_detail == pd

        limit = 10
        e = exception(limit=limit)
        assert e.problem_detail.detail is not None
        assert e.problem_detail.detail.startswith(
            f"You have reached your {limit_type} limit of {limit}."
        )

    def test_internal_server_error(self) -> None:
        e = InternalServerError("message", "debug")
        assert e.problem_detail == INTERNAL_SERVER_ERROR

    def test_remote_initiated_server_error(self) -> None:
        e = RemoteInitiatedServerError("debug message", "some service")
        assert e.problem_detail == INTEGRATION_ERROR.detailed(
            "Integration error communicating with some service"
        ).with_debug("debug message")

from api.admin.problem_details import ADMIN_NOT_AUTHORIZED
from core.util.problem_detail import BaseProblemDetailException, ProblemDetail


class AdminNotAuthorized(BaseProblemDetailException):
    def __init__(self, message: str | None = None) -> None:
        self.message = message
        super().__init__(message)

    @property
    def problem_detail(self) -> ProblemDetail:
        return (
            ADMIN_NOT_AUTHORIZED.detailed(self.message)
            if self.message
            else ADMIN_NOT_AUTHORIZED
        )

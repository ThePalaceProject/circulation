from typing import Any

from api.admin.problem_details import ADMIN_NOT_AUTHORIZED
from core.util.problem_detail import ProblemDetail


class AdminNotAuthorized(Exception):
    status_code = 403

    def __init__(self, *args: Any) -> None:
        self.message = None
        if len(args) > 0:
            self.message = args[0]
        super().__init__(*args)

    def as_problem_detail_document(self, debug=False) -> ProblemDetail:
        return (
            ADMIN_NOT_AUTHORIZED.detailed(self.message)
            if self.message
            else ADMIN_NOT_AUTHORIZED
        )

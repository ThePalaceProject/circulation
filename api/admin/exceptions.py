from .problem_details import *


class AdminNotAuthorized(Exception):
    status_code = 403

    def __init__(self, *args: object) -> None:
        self.message = None
        if len(args) > 0:
            self.message = args[0]
        super().__init__(*args)

    def as_problem_detail_document(self, debug=False):
        return (
            ADMIN_NOT_AUTHORIZED.detailed(self.message)
            if self.message
            else ADMIN_NOT_AUTHORIZED
        )

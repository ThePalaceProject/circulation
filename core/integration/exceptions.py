from typing import Any

from pydantic import PydanticValueError

from core.util.problem_detail import ProblemDetail


class SettingsValidationError(PydanticValueError):
    """
    Raised in a custom pydantic validator when there is a problem
    with the configuration settings. A ProblemDetail should
    be passed to the exception constructor.

    for example:
    raise SettingsValidationError(problem_detail=INVALID_CONFIGURATION_OPTION)
    """

    code = "problem_detail"
    msg_template = "{problem_detail.detail}"

    def __init__(self, problem_detail: ProblemDetail, **kwargs: Any):
        super().__init__(problem_detail=problem_detail, **kwargs)


class ProblemDetailException(Exception):
    """An exception containing a ProblemDetail"""

    def __init__(self, problem_detail: ProblemDetail):
        self.problem_detail = problem_detail

    def __str__(self) -> str:
        return str(self.problem_detail)

from typing import Self

from requests import Response

from palace.manager.util.http.exception import BadResponseException, ResponseData
from palace.manager.util.problem_detail import ProblemDetail


class OpdsResponseException(BadResponseException):
    """
    OPDS in general, and ODL (and Readium LCP) in particular, often return errors as
    Problem Detail documents. This isn't always the case, but we try to use this information
    when we can.
    """

    def __init__(
        self,
        type: str,
        title: str,
        status: int,
        detail: str | None,
        response: Response | ResponseData,
    ) -> None:
        super().__init__(url_or_service=response.url, message=title, response=response)
        self.type = type
        self.title = title
        self.status = status
        self.detail = detail

    @property
    def problem_detail(self) -> ProblemDetail:
        return ProblemDetail(
            uri=self.type,
            status_code=self.status,
            title=self.title,
            detail=self.detail,
        )

    @classmethod
    def from_response_data(cls, response: ResponseData) -> Self | None:
        # Wrap the response if it is a problem detail document.
        #
        # DeMarque sends "application/api-problem+json", but the ODL spec says we should
        # expect "application/problem+json", so we need to check for both.
        if response.headers.get("Content-Type") not in [
            "application/api-problem+json",
            "application/problem+json",
        ]:
            return None

        try:
            json_response = response.json()
        except ValueError:
            json_response = {}

        type = json_response.get("type")
        title = json_response.get("title")
        status = json_response.get("status") or response.status_code
        detail = json_response.get("detail")

        if type is None or title is None:
            return None

        return cls(type, title, status, detail, response)

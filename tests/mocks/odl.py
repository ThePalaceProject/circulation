from typing import Any

from requests import Response
from sqlalchemy.orm import Session

from palace.manager.api.odl.api import OPDS2WithODLApi
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.util.http import HTTP
from tests.mocks.mock import MockRequestsResponse


class MockOPDS2WithODLApi(OPDS2WithODLApi):
    def __init__(
        self,
        _db: Session,
        collection: Collection,
    ) -> None:
        super().__init__(_db, collection)
        self.responses: list[MockRequestsResponse] = []
        self.requests: list[tuple[str, dict[str, str] | None]] = []

    def queue_response(
        self,
        status_code: int,
        headers: dict[str, str] | None = None,
        content: str | None = None,
    ):
        if headers is None:
            headers = {}
        self.responses.insert(0, MockRequestsResponse(status_code, headers, content))

    def _url_for(self, *args: Any, **kwargs: Any) -> str:
        del kwargs["_external"]
        return "http://{}?{}".format(
            "/".join(args),
            "&".join([f"{key}={val}" for key, val in list(kwargs.items())]),
        )

    def _get(self, url: str, headers: dict[str, str] | None = None) -> Response:
        self.requests.append((url, headers))
        response = self.responses.pop()
        return HTTP._process_response(url, response)

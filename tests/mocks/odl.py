from collections.abc import Mapping
from datetime import timedelta
from typing import Any

from requests import Response
from sqlalchemy.orm import Session

from palace.manager.api.odl.api import OPDS2WithODLApi
from palace.manager.api.odl.auth import TokenTuple
from palace.manager.api.odl.settings import OPDS2AuthType
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.util.datetime_helpers import utc_now
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
        self.requests: list[
            tuple[str, Mapping[str, str] | None, Mapping[str, Any]]
        ] = []
        self.mock_auth_type = self.settings.auth_type
        self.refresh_token_calls = 0
        self.refresh_token_timedelta = timedelta(minutes=30)

    @property
    def _auth_type(self) -> OPDS2AuthType:
        return self.mock_auth_type

    def _refresh_token(self) -> None:
        self.refresh_token_calls += 1
        self._session_token = TokenTuple(
            token="new_token", expires=utc_now() + self.refresh_token_timedelta
        )

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

    def _get(
        self, url: str, headers: Mapping[str, str] | None = None, **kwargs: Any
    ) -> Response:
        self.requests.append((url, headers, kwargs))
        response = self.responses.pop()
        return HTTP._process_response(url, response)

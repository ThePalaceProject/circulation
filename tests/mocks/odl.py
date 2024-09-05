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
from tests.mocks.mock import MockHTTPClient


class MockOPDS2WithODLApi(OPDS2WithODLApi):
    def __init__(
        self,
        _db: Session,
        collection: Collection,
        mock_http_client: MockHTTPClient,
    ) -> None:
        super().__init__(_db, collection)

        self.mock_http_client = mock_http_client
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

    def _url_for(self, *args: Any, **kwargs: Any) -> str:
        del kwargs["_external"]
        return "http://{}?{}".format(
            "/".join(args),
            "&".join([f"{key}={val}" for key, val in list(kwargs.items())]),
        )

    def _get(
        self, url: str, headers: Mapping[str, str] | None = None, **kwargs: Any
    ) -> Response:
        return self.mock_http_client.do_get(url, headers=headers, **kwargs)

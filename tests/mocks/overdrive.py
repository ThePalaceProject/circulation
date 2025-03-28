from datetime import timedelta
from typing import Any

from requests import Response
from sqlalchemy.orm import Session
from typing_extensions import Unpack, override

from palace.manager.api.overdrive.api import OverdriveAPI, OverdriveToken
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http import RequestKwargs
from tests.mocks.mock import MockHTTPClient


class MockOverdriveAPI(OverdriveAPI):
    def __init__(
        self, _db: Session, collection: Collection, mock_http: MockHTTPClient
    ) -> None:
        # Almost all tests will try to request the access token, so
        # set the response that will be returned if an attempt is
        # made.
        super().__init__(_db, collection)
        self.mock_http = mock_http

        # Initialize some variables that are normally set when they are first accessed,
        # since most tests will access them.
        self._collection_token = "fake collection token"
        self._cached_client_oauth_token = OverdriveToken(
            "fake client oauth token", utc_now() + timedelta(hours=1)
        )

    @override
    def _do_get(self, url: str, headers: dict[str, str], **kwargs: Any) -> Response:
        url = self.endpoint(url)
        return self.mock_http.do_request("GET", url, headers=headers, **kwargs)

    @override
    def _do_post(
        self, url: str, payload: dict[str, str], headers: dict[str, str], **kwargs: Any
    ) -> Response:
        url = self.endpoint(url)
        return self.mock_http.do_request(
            "POST", url, data=payload, headers=headers, **kwargs
        )

    @override
    def _do_patron_request(
        self, http_method: str, url: str, **kwargs: Unpack[RequestKwargs]
    ) -> Response:
        url = self.endpoint(url)
        return self.mock_http.do_request(http_method, url, **kwargs)

from typing import Any

from sqlalchemy.orm import Session

from palace.manager.integration.license.enki import EnkiAPI
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.util.http.base import raise_for_bad_response
from tests.mocks.mock import MockRequestsResponse


class MockEnkiAPI(EnkiAPI):
    def __init__(self, _db: Session, collection: Collection) -> None:
        self.responses: list[MockRequestsResponse] = []
        self.requests: list[list[Any]] = []

        super().__init__(_db, collection)

    def queue_response(self, status_code, headers={}, content=None):
        self.responses.insert(0, MockRequestsResponse(status_code, headers, content))

    def _request(self, url, method, headers, data, params, **kwargs):
        """Override EnkiAPI._request to pull responses from a
        queue instead of making real HTTP requests
        """
        self.requests.append([method, url, headers, data, params, kwargs])
        response = self.responses.pop()
        return raise_for_bad_response(
            url,
            response,
            kwargs.get("allowed_response_codes", []),
            kwargs.get("disallowed_response_codes", []),
        )

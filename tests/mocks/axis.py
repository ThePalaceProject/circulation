from sqlalchemy.orm import Session

from palace.manager.api.axis.api import Axis360API
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.util.http import HTTP
from tests.mocks.mock import MockRequestsResponse


class MockAxis360API(Axis360API):
    @classmethod
    def mock_collection(
        cls, _db: Session, library: Library, name: str = "Test Axis 360 Collection"
    ) -> Collection:
        """Create a mock Axis 360 collection for use in tests."""
        collection, _ = Collection.by_name_and_protocol(_db, name, Axis360API.label())
        collection.integration_configuration.settings_dict = {
            "username": "a",
            "password": "b",
            "url": "http://axis.test/",
            "external_account_id": "c",
        }
        if library not in collection.associated_libraries:
            collection.associated_libraries.append(library)
        return collection

    def __init__(self, _db, collection, with_token=True, **kwargs):
        """Constructor.

        :param collection: Get Axis 360 credentials from this
            Collection.

        :param with_token: If True, this class will assume that
            it already has a valid token, and will not go through
            the motions of negotiating one with the mock server.
        """
        super().__init__(_db, collection, **kwargs)
        if with_token:
            self._cached_bearer_token = "mock token"
        self.responses = []
        self.requests = []

    def queue_response(self, status_code, headers={}, content=None):
        self.responses.insert(0, MockRequestsResponse(status_code, headers, content))

    def _make_request(self, url, *args, **kwargs):
        self.requests.append([url, args, kwargs])
        response = self.responses.pop()
        return HTTP._process_response(
            url,
            response,
            kwargs.get("allowed_response_codes"),
            kwargs.get("disallowed_response_codes"),
        )

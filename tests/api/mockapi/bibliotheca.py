from datetime import datetime

from sqlalchemy.orm import Session

from api.bibliotheca import BibliothecaAPI
from core.model import Library
from core.model.collection import Collection
from core.model.configuration import ExternalIntegration
from core.util.http import HTTP
from tests.core.mock import MockRequestsResponse


class MockBibliothecaAPI(BibliothecaAPI):
    @classmethod
    def mock_collection(
        self, _db: Session, library: Library, name: str = "Test Bibliotheca Collection"
    ) -> Collection:
        """Create a mock Bibliotheca collection for use in tests."""
        collection, _ = Collection.by_name_and_protocol(
            _db, name=name, protocol=ExternalIntegration.BIBLIOTHECA
        )
        collection.integration_configuration.settings_dict = {
            "username": "a",
            "password": "b",
            "external_account_id": "c",
        }
        if library not in collection.libraries:
            collection.libraries.append(library)
        return collection

    def __init__(self, _db, collection, *args, **kwargs):
        self.responses = []
        self.requests = []
        super().__init__(_db, collection, *args, **kwargs)

    def now(self):
        """Return an unvarying time in the format Bibliotheca expects."""
        return datetime.strftime(datetime(2016, 1, 1), self.AUTH_TIME_FORMAT)

    def queue_response(self, status_code, headers={}, content=None):
        self.responses.insert(0, MockRequestsResponse(status_code, headers, content))

    def _request_with_timeout(self, method, url, *args, **kwargs):
        """Simulate HTTP.request_with_timeout."""
        self.requests.append([method, url, args, kwargs])
        response = self.responses.pop()
        return HTTP._process_response(
            url,
            response,
            kwargs.get("allowed_response_codes"),
            kwargs.get("disallowed_response_codes"),
        )

    def _simple_http_get(self, url, headers, *args, **kwargs):
        """Simulate Representation.simple_http_get."""
        response = self._request_with_timeout("GET", url, *args, **kwargs)
        return response.status_code, response.headers, response.content

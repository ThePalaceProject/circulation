from datetime import datetime

from api.bibliotheca import BibliothecaAPI
from core.model import get_one_or_create
from core.model.collection import Collection
from core.model.configuration import ExternalIntegration
from core.util.http import HTTP
from tests.core.mock import MockRequestsResponse
from tests.fixtures.db import make_default_library


class MockBibliothecaAPI(BibliothecaAPI):
    @classmethod
    def mock_collection(self, _db, name="Test Bibliotheca Collection"):
        """Create a mock Bibliotheca collection for use in tests."""
        library = make_default_library(_db)
        collection, ignore = get_one_or_create(
            _db,
            Collection,
            name=name,
            create_method_kwargs=dict(
                external_account_id="c",
            ),
        )
        integration = collection.create_external_integration(
            protocol=ExternalIntegration.BIBLIOTHECA
        )
        config = collection.create_integration_configuration(
            ExternalIntegration.BIBLIOTHECA
        )
        config["username"] = "a"
        config["password"] = "b"
        config["url"] = "http://bibliotheca.test"
        config.for_library(library.id, create=True)
        library.collections.append(collection)
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

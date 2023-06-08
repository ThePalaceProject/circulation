import json

from api.odilo import OdiloAPI
from core.model import get_one_or_create
from core.model.collection import Collection
from core.model.configuration import ExternalIntegration
from core.util.http import HTTP
from tests.core.mock import MockRequestsResponse
from tests.fixtures.db import make_default_library


class MockOdiloAPI(OdiloAPI):
    def patron_request(self, patron, pin, *args, **kwargs):
        response = self._make_request(*args, **kwargs)

        # Modify the record of the request to include the patron information.
        original_data = self.requests[-1]

        # The last item in the record of the request is keyword arguments.
        # Stick this information in there to minimize confusion.
        original_data[-1]["_patron"] = patron
        original_data[-1]["_pin"] = pin
        return response

    @classmethod
    def mock_collection(cls, _db):
        library = make_default_library(_db)
        collection, ignore = get_one_or_create(
            _db,
            Collection,
            name="Test Odilo Collection",
            create_method_kwargs=dict(
                external_account_id="library_id_123",
            ),
        )
        integration = collection.create_external_integration(
            protocol=ExternalIntegration.ODILO
        )
        config = collection.create_integration_configuration(ExternalIntegration.ODILO)
        config["username"] = "username"
        config["password"] = "password"
        config.set(OdiloAPI.LIBRARY_API_BASE_URL, "http://library_api_base_url/api/v2")
        config.for_library(library.id, create=True)
        library.collections.append(collection)

        return collection

    def __init__(self, _db, collection, *args, **kwargs):
        self.access_token_requests = []
        self.requests = []
        self.responses = []

        self.access_token_response = self.mock_access_token_response("bearer token")
        super().__init__(_db, collection, *args, **kwargs)

    def token_post(self, url, payload, headers={}, **kwargs):
        """Mock the request for an OAuth token."""

        self.access_token_requests.append((url, payload, headers, kwargs))
        response = self.access_token_response
        return HTTP._process_response(url, response, **kwargs)

    def mock_access_token_response(self, credential, expires_in=-1):
        token = dict(token=credential, expiresIn=expires_in)
        return MockRequestsResponse(200, {}, json.dumps(token))

    def queue_response(self, status_code, headers={}, content=None):
        self.responses.insert(0, MockRequestsResponse(status_code, headers, content))

    def _do_get(self, url, *args, **kwargs):
        """Simulate Representation.simple_http_get."""
        response = self._make_request(url, *args, **kwargs)
        return response.status_code, response.headers, response.content

    def _do_post(self, url, *args, **kwargs):
        return self._make_request(url, *args, **kwargs)

    def _make_request(self, url, *args, **kwargs):
        response = self.responses.pop()
        self.requests.append((url, args, kwargs))
        return HTTP._process_response(
            url,
            response,
            kwargs.get("allowed_response_codes"),
            kwargs.get("disallowed_response_codes"),
        )

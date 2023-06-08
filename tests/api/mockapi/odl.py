from api.odl import SharedODLAPI
from core.model import get_one_or_create
from core.model.collection import Collection
from core.util.http import HTTP
from tests.core.mock import MockRequestsResponse
from tests.fixtures.db import make_default_library


class MockSharedODLAPI(SharedODLAPI):
    """Mock API for tests that overrides _get and tracks requests."""

    @classmethod
    def mock_collection(cls, _db):
        """Create a mock ODL collection to use in tests."""
        library = make_default_library(_db)
        collection, ignore = get_one_or_create(
            _db,
            Collection,
            name="Test Shared ODL Collection",
            create_method_kwargs=dict(
                external_account_id="http://shared-odl",
            ),
        )
        integration = collection.create_external_integration(protocol=SharedODLAPI.NAME)
        config = collection.create_integration_configuration(SharedODLAPI.NAME)
        config.for_library(library.id, create=True)
        library.collections.append(collection)
        return collection

    def __init__(self, _db, collection, *args, **kwargs):
        self.responses = []
        self.requests = []
        self.request_args = []
        super().__init__(_db, collection, *args, **kwargs)

    def queue_response(self, status_code, headers={}, content=None):
        self.responses.insert(0, MockRequestsResponse(status_code, headers, content))

    def _get(self, url, patron=None, headers=None, allowed_response_codes=None):
        allowed_response_codes = allowed_response_codes or ["2xx", "3xx"]
        self.requests.append(url)
        self.request_args.append((patron, headers, allowed_response_codes))
        response = self.responses.pop()
        return HTTP._process_response(
            url, response, allowed_response_codes=allowed_response_codes
        )

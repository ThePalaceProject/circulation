from api.axis import Axis360API
from core.model import get_one_or_create
from core.model.collection import Collection
from core.model.configuration import ExternalIntegration
from core.util.http import HTTP
from tests.fixtures.db import DatabaseTest


class MockAxis360API(Axis360API):
    @classmethod
    def mock_collection(cls, _db, name="Test Axis 360 Collection"):
        """Create a mock Axis 360 collection for use in tests."""
        library = DatabaseTest.make_default_library(_db)
        collection, ignore = get_one_or_create(
            _db,
            Collection,
            name=name,
            create_method_kwargs=dict(
                external_account_id="c",
            ),
        )
        integration = collection.create_external_integration(
            protocol=ExternalIntegration.AXIS_360
        )
        integration.username = "a"
        integration.password = "b"
        integration.url = "http://axis.test/"
        library.collections.append(collection)
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
            self.token = "mock token"
        self.responses = []
        self.requests = []

    def queue_response(self, status_code, headers={}, content=None):
        from core.testing import MockRequestsResponse

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

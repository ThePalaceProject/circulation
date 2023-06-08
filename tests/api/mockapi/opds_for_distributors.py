from api.opds_for_distributors import OPDSForDistributorsAPI
from core.model import get_one_or_create
from core.model.collection import Collection
from core.util.http import HTTP
from tests.core.mock import MockRequestsResponse
from tests.fixtures.db import make_default_library


class MockOPDSForDistributorsAPI(OPDSForDistributorsAPI):
    @classmethod
    def mock_collection(
        self, _db, name="Test OPDS For Distributors Collection"
    ) -> Collection:
        """Create a mock OPDS For Distributors collection to use in tests.

        :param _db: Database session.
        :param name: A name for the collection.
        """
        library = make_default_library(_db)
        collection, ignore = get_one_or_create(
            _db,
            Collection,
            name=name,
            create_method_kwargs=dict(
                external_account_id="http://opds",
            ),
        )
        integration = collection.create_external_integration(
            protocol=OPDSForDistributorsAPI.NAME
        )
        config = collection.create_integration_configuration(
            OPDSForDistributorsAPI.NAME
        )
        config["username"] = "a"
        config["password"] = "b"
        config.for_library(library.id, create=True)
        library.collections.append(collection)
        return collection

    def __init__(self, _db, collection, *args, **kwargs):
        self.responses = []
        self.requests = []
        super().__init__(_db, collection, *args, **kwargs)

    def queue_response(self, status_code, headers={}, content=None):
        self.responses.insert(0, MockRequestsResponse(status_code, headers, content))

    def _request_with_timeout(self, method, url, *args, **kwargs):
        self.requests.append([method, url, args, kwargs])
        response = self.responses.pop()
        return HTTP._process_response(
            url,
            response,
            kwargs.get("allowed_response_codes"),
            kwargs.get("disallowed_response_codes"),
        )

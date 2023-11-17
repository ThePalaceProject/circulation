from sqlalchemy.orm import Session

from api.opds_for_distributors import OPDSForDistributorsAPI
from core.model import Library
from core.model.collection import Collection
from core.util.http import HTTP
from tests.core.mock import MockRequestsResponse


class MockOPDSForDistributorsAPI(OPDSForDistributorsAPI):
    @classmethod
    def mock_collection(
        self,
        _db: Session,
        library: Library,
        name: str = "Test OPDS For Distributors Collection",
    ) -> Collection:
        """Create a mock OPDS For Distributors collection to use in tests.

        :param _db: Database session.
        :param name: A name for the collection.
        """
        collection, _ = Collection.by_name_and_protocol(
            _db, name=name, protocol=OPDSForDistributorsAPI.label()
        )
        collection.integration_configuration.settings_dict = dict(
            username="a",
            password="b",
            data_source="data_source",
            external_account_id="http://opds",
        )
        if library not in collection.libraries:
            collection.libraries.append(library)
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

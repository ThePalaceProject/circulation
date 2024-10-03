from palace.manager.api.opds_for_distributors import OPDSForDistributorsAPI
from palace.manager.util.http import HTTP
from tests.mocks.mock import MockRequestsResponse


class MockOPDSForDistributorsAPI(OPDSForDistributorsAPI):
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

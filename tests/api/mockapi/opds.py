from core.opds_import import SimplifiedOPDSLookup
from core.util.http import HTTP
from tests.core.mock import MockRequestsResponse


class MockSimplifiedOPDSLookup(SimplifiedOPDSLookup):
    def __init__(self, *args, **kwargs):
        self.requests = []
        self.responses = []
        super().__init__(*args, **kwargs)

    def queue_response(self, status_code, headers={}, content=None):
        self.responses.insert(0, MockRequestsResponse(status_code, headers, content))

    def _get(self, url, *args, **kwargs):
        self.requests.append((url, args, kwargs))
        response = self.responses.pop()
        return HTTP._process_response(
            url,
            response,
            kwargs.get("allowed_response_codes"),
            kwargs.get("disallowed_response_codes"),
        )

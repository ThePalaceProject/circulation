import json

from sqlalchemy.orm import Session

from api.overdrive import OverdriveAPI, OverdriveLibrarySettings, OverdriveSettings
from core.model import Library
from core.model.collection import Collection
from core.model.configuration import ExternalIntegration
from core.util.http import HTTP
from tests.core.mock import MockRequestsResponse


class MockOverdriveResponse:
    def __init__(self, status_code, headers, content):
        self.status_code = status_code
        self.headers = headers
        self.content = content

    def json(self):
        return json.loads(self.content)


class MockOverdriveAPI(OverdriveAPI):
    library_data = '{"id":1810,"name":"My Public Library (MA)","type":"Library","collectionToken":"1a09d9203","links":{"self":{"href":"http://api.overdrive.com/v1/libraries/1810","type":"application/vnd.overdrive.api+json"},"products":{"href":"http://api.overdrive.com/v1/collections/1a09d9203/products","type":"application/vnd.overdrive.api+json"},"dlrHomepage":{"href":"http://ebooks.nypl.org","type":"text/html"}},"formats":[{"id":"audiobook-wma","name":"OverDrive WMA Audiobook"},{"id":"ebook-pdf-adobe","name":"Adobe PDF eBook"},{"id":"ebook-mediado","name":"MediaDo eBook"},{"id":"ebook-epub-adobe","name":"Adobe EPUB eBook"},{"id":"ebook-kindle","name":"Kindle Book"},{"id":"audiobook-mp3","name":"OverDrive MP3 Audiobook"},{"id":"ebook-pdf-open","name":"Open PDF eBook"},{"id":"ebook-overdrive","name":"OverDrive Read"},{"id":"video-streaming","name":"Streaming Video"},{"id":"ebook-epub-open","name":"Open EPUB eBook"}]}'

    token_data = '{"access_token":"foo","token_type":"bearer","expires_in":3600,"scope":"LIB META AVAIL SRCH"}'

    def __init__(self, _db, collection):
        self.access_token_requests = []
        self.requests = []
        self.responses = []

        # Almost all tests will try to request the access token, so
        # set the response that will be returned if an attempt is
        # made.
        self.access_token_response = self.mock_access_token_response("bearer token")
        super().__init__(_db, collection)
        self._collection_token = "fake token"

    @classmethod
    def mock_collection(
        cls,
        _db: Session,
        library: Library,
        name: str = "Test Overdrive Collection",
        client_key: str = "a",
        client_secret: str = "b",
        library_id: str = "c",
        website_id: str = "d",
        ils_name: str = "e",
    ):
        """Create a mock Overdrive collection for use in tests."""
        collection, _ = Collection.by_name_and_protocol(
            _db, name=name, protocol=ExternalIntegration.OVERDRIVE
        )
        settings = OverdriveSettings(
            external_account_id=library_id,
            overdrive_website_id=website_id,
            overdrive_client_key=client_key,
            overdrive_client_secret=client_secret,
        )
        OverdriveAPI.settings_update(collection.integration_configuration, settings)
        if library not in collection.libraries:
            collection.libraries.append(library)
        library_settings = OverdriveLibrarySettings(
            ils_name=ils_name,
        )
        library_config = collection.integration_configuration.for_library(library.id)
        assert library_config is not None
        OverdriveAPI.library_settings_update(library_config, library_settings)
        return collection

    def queue_collection_token(self):
        # Many tests immediately try to access the
        # collection token. This is a helper method to make it easy to
        # queue up the response.
        self.queue_response(200, content=self.mock_collection_token("collection token"))

    def token_post(self, url, payload, is_fulfillment=False, headers={}, **kwargs):
        """Mock the request for an OAuth token.

        We mock the method by looking at the access_token_response
        property, rather than inserting a mock response in the queue,
        because only the first MockOverdriveAPI instantiation in a
        given test actually makes this call. By mocking the response
        to this method separately we remove the need to figure out
        whether to queue a response in a given test.
        """
        url = self.endpoint(url)
        self.access_token_requests.append((url, payload, headers, kwargs))
        response = self.access_token_response
        return HTTP._process_response(url, response, **kwargs)

    def mock_access_token_response(self, credential):
        token = dict(access_token=credential, expires_in=3600)
        return MockRequestsResponse(200, {}, json.dumps(token))

    def mock_collection_token(self, token):
        return json.dumps(dict(collectionToken=token))

    def queue_response(self, status_code, headers={}, content=None):
        self.responses.insert(0, MockRequestsResponse(status_code, headers, content))

    def _do_get(self, url, *args, **kwargs):
        response = self._make_request(url, *args, **kwargs)
        return MockRequestsResponse(
            response.status_code, response.headers, response.content
        )

    def _do_post(self, url, *args, **kwargs):
        return self._make_request(url, *args, **kwargs)

    def _make_request(self, url, *args, **kwargs):
        url = self.endpoint(url)
        response = self.responses.pop()
        self.requests.append((url, args, kwargs))
        return HTTP._process_response(
            url,
            response,
            kwargs.get("allowed_response_codes"),
            kwargs.get("disallowed_response_codes"),
        )

    def patron_request(self, patron, pin, *args, **kwargs):
        response = self._make_request(*args, **kwargs)

        # Modify the record of the request to include the patron information.
        original_data = self.requests[-1]

        # The last item in the record of the request is keyword arguments.
        # Stick this information in there to minimize confusion.
        original_data[-1]["_patron"] = patron
        original_data[-1]["_pin"] = patron
        return response

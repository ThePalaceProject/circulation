from api.enki import EnkiAPI
from core.model.collection import Collection
from core.util.http import HTTP
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.db import make_default_library


class MockEnkiAPI(EnkiAPI):
    def __init__(self, _db, collection=None, *args, **kwargs):
        self.responses = []
        self.requests = []

        library = make_default_library(_db)
        if not collection:
            collection, ignore = Collection.by_name_and_protocol(
                _db, name="Test Enki Collection", protocol=EnkiAPI.ENKI
            )
            collection.protocol = EnkiAPI.ENKI
        if collection not in library.collections:
            library.collections.append(collection)

        # Set the "Enki library ID" variable between the default library
        # and this Enki collection.
        DatabaseTransactionFixture.set_settings(
            collection.integration_configuration.for_library(library.id, create=True),
            **{self.ENKI_LIBRARY_ID_KEY: "c"}
        )
        _db.commit()

        super().__init__(_db, collection, *args, **kwargs)

    def queue_response(self, status_code, headers={}, content=None):
        from tests.core.mock import MockRequestsResponse

        self.responses.insert(0, MockRequestsResponse(status_code, headers, content))

    def _request(self, method, url, headers, data, params, **kwargs):
        """Override EnkiAPI._request to pull responses from a
        queue instead of making real HTTP requests
        """
        self.requests.append([method, url, headers, data, params, kwargs])
        response = self.responses.pop()
        return HTTP._process_response(
            url,
            response,
            kwargs.get("allowed_response_codes"),
            kwargs.get("disallowed_response_codes"),
        )

from typing import Any, List, Optional

from sqlalchemy.orm import Session

from api.enki import EnkiAPI
from core.model import Library
from core.model.collection import Collection
from core.util.http import HTTP
from tests.core.mock import MockRequestsResponse
from tests.fixtures.database import DatabaseTransactionFixture


class MockEnkiAPI(EnkiAPI):
    def __init__(
        self, _db: Session, library: Library, collection: Optional[Collection] = None
    ) -> None:
        self.responses: List[MockRequestsResponse] = []
        self.requests: List[List[Any]] = []

        if not collection:
            collection, ignore = Collection.by_name_and_protocol(
                _db, name="Test Enki Collection", protocol=EnkiAPI.ENKI
            )
            assert collection is not None
            collection.protocol = EnkiAPI.ENKI
        if collection not in library.collections:
            library.collections.append(collection)

        # Set the "Enki library ID" variable between the default library
        # and this Enki collection.
        assert library.id is not None
        DatabaseTransactionFixture.set_settings(
            collection.integration_configuration.for_library(library.id, create=True),
            **{self.ENKI_LIBRARY_ID_KEY: "c"}
        )
        _db.commit()

        super().__init__(_db, collection)

    def queue_response(self, status_code, headers={}, content=None):
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

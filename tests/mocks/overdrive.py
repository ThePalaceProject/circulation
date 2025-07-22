from datetime import timedelta

from sqlalchemy.orm import Session

from palace.manager.integration.license.overdrive.api import (
    OverdriveAPI,
    OverdriveToken,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.util.datetime_helpers import utc_now


class MockOverdriveAPI(OverdriveAPI):
    def __init__(self, _db: Session, collection: Collection) -> None:
        # Almost all tests will try to request the access token, so
        # set the response that will be returned if an attempt is
        # made.
        super().__init__(_db, collection)

        # Initialize some variables that are normally set when they are first accessed,
        # since most tests will access them.
        self._collection_token = "fake collection token"
        self._cached_client_oauth_token = OverdriveToken(
            "fake client oauth token", utc_now() + timedelta(hours=1)
        )

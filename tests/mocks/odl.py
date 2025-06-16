from datetime import timedelta

from sqlalchemy.orm import Session

from palace.manager.api.odl.api import OPDS2WithODLApi
from palace.manager.api.odl.auth import TokenTuple
from palace.manager.api.odl.settings import OPDS2AuthType
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.util.datetime_helpers import utc_now


class MockOPDS2WithODLApi(OPDS2WithODLApi):
    def __init__(
        self,
        _db: Session,
        collection: Collection,
    ) -> None:
        super().__init__(_db, collection)
        self.mock_auth_type = self.settings.auth_type
        self.refresh_token_calls = 0
        self.refresh_token_timedelta = timedelta(minutes=30)

    @property
    def _auth_type(self) -> OPDS2AuthType:
        return self.mock_auth_type

    def _refresh_token(self) -> None:
        self.refresh_token_calls += 1
        self._session_token = TokenTuple(
            token="new_token", expires=utc_now() + self.refresh_token_timedelta
        )

    @staticmethod
    def _notification_url(
        short_name: str | None, patron_id: str, license_id: str
    ) -> str:
        return f"https://cm/{short_name}/odl/notify/{patron_id}/{license_id}"

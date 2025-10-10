from typing import override

from sqlalchemy.orm import Session

from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
from palace.manager.sqlalchemy.model.collection import Collection


class MockOPDS2WithODLApi(OPDS2WithODLApi):
    def __init__(
        self,
        _db: Session,
        collection: Collection,
    ) -> None:
        super().__init__(_db, collection)

    @staticmethod
    @override
    def _notification_url(
        short_name: str | None, patron_id: str, license_id: str
    ) -> str:
        return f"https://cm/{short_name}/odl/notify/{patron_id}/{license_id}"

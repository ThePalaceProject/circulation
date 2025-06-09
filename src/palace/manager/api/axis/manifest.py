from __future__ import annotations

import json

from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism


class AxisNowManifest:
    """A simple media type for conveying an entry point into the AxisNow access control
    system.
    """

    MEDIA_TYPE = DeliveryMechanism.AXISNOW_DRM

    def __init__(self, book_vault_uuid: str, isbn: str):
        """Constructor.

        :param book_vault_uuid: The UUID of a Book Vault.
        :param isbn: The ISBN of a book in that Book Vault.
        """
        self.book_vault_uuid = book_vault_uuid
        self.isbn = isbn

    def __str__(self) -> str:
        data = dict(isbn=self.isbn, book_vault_uuid=self.book_vault_uuid)
        return json.dumps(data, sort_keys=True)

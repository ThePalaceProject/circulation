from __future__ import annotations

from sqlalchemy.orm import Session
from typing_extensions import Self

from palace.manager.metadata_layer.base.frozen import BaseFrozenData
from palace.manager.sqlalchemy.model.identifier import Identifier


class IdentifierData(BaseFrozenData):
    type: str
    identifier: str
    weight: float = 1

    def load(self, _db: Session) -> tuple[Identifier, bool]:
        return Identifier.for_foreign_id(_db, self.type, self.identifier)

    @classmethod
    def from_identifier(cls, identifier: Identifier | IdentifierData) -> Self:
        """Create an IdentifierData object from a data-model Identifier
        object.
        """
        if isinstance(identifier, cls):
            return identifier

        return cls(type=identifier.type, identifier=identifier.identifier)

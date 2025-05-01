from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session

from palace.manager.sqlalchemy.model.identifier import Identifier


@dataclass(frozen=True)
class IdentifierData:
    type: str
    identifier: str
    weight: float = 1

    def __repr__(self) -> str:
        return '<IdentifierData type="{}" identifier="{}" weight="{}">'.format(
            self.type,
            self.identifier,
            self.weight,
        )

    def load(self, _db: Session) -> tuple[Identifier, bool]:
        return Identifier.for_foreign_id(_db, self.type, self.identifier)

from __future__ import annotations

from pydantic import Field
from sqlalchemy.orm import Session
from typing_extensions import Self

from palace.manager.data_layer.base.frozen import BaseFrozenData
from palace.manager.service.redis.key import RedisKeyGenerator
from palace.manager.sqlalchemy.model.identifier import Identifier


class IdentifierData(BaseFrozenData):
    type: str
    identifier: str
    weight: float = Field(1.0, repr=False)

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

    @classmethod
    def parse_urn(
        cls,
        urn: str,
    ) -> Self:
        """
        Parse identifier string.

        Raises PalaceValueError if the URN cannot be parsed.
        """

        type_, identifier = Identifier.type_and_identifier_for_urn(urn)

        return cls(
            type=type_,
            identifier=identifier,
        )

    def redis_key(self) -> str:
        """
        String representation of the IdentifierData object suitable for use
        as a redis key.
        """
        return (
            f"{self.__class__.__name__}{RedisKeyGenerator.SEPERATOR}"
            f"{self.type}{RedisKeyGenerator.SEPERATOR}{self.identifier}"
        )

    def __str__(self) -> str:
        return f"{self.type}/{self.identifier}"

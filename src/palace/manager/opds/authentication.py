"""
Models for the Authentication for OPDS 1.0 specification.
https://drafts.opds.io/authentication-for-opds-1.0
"""

from typing import ClassVar, Generic

from pydantic import Field, field_validator
from typing_extensions import TypeVar

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.opds.base import BaseOpdsModel
from palace.manager.opds.rwpm import Link
from palace.manager.opds.types.link import BaseLink, CompactCollection

_LinkT = TypeVar("_LinkT", bound=BaseLink, default=Link, covariant=True)


class AuthenticationLabels(BaseOpdsModel):
    login: str
    password: str


class Authentication(BaseOpdsModel, Generic[_LinkT]):
    type: str
    labels: AuthenticationLabels | None = None
    links: CompactCollection[_LinkT]


_AuthT = TypeVar("_AuthT", bound=Authentication[BaseLink], default=Authentication)


class AuthenticationDocument(BaseOpdsModel, Generic[_AuthT, _LinkT]):
    """Authentication for OPDS 1.0 document."""

    MEDIA_TYPE: ClassVar[str] = "application/vnd.opds.authentication.v1.0+json"
    LINK_RELATION: ClassVar[str] = "http://opds-spec.org/auth/document"

    @staticmethod
    def content_types() -> list[str]:
        return [
            "application/opds-authentication+json",
            "application/vnd.opds.authentication.v1.0+json",
        ]

    @classmethod
    def content_type(cls) -> str:
        return cls.content_types()[0]

    id: str
    title: str
    authentication: list[_AuthT]
    description: str | None = None
    links: CompactCollection[_LinkT] = Field(default_factory=CompactCollection)

    @field_validator("authentication")
    @classmethod
    def _validate_authentication(
        cls, value: list[Authentication]
    ) -> list[Authentication]:
        if not value:
            raise ValueError(
                "Authentication document must have at least one authentication object."
            )

        auth_types = set()
        for auth in value:
            if auth.type in auth_types:
                raise ValueError(f"Duplicate authentication type '{auth.type}'.")
            auth_types.add(auth.type)

        return value

    def by_type(self, auth_type: str) -> _AuthT:
        for auth in self.authentication:
            if auth.type == auth_type:
                return auth
        raise PalaceValueError(f"Unable to find authentication for '{auth_type}'")

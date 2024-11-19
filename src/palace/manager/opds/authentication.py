"""
Models for the Authentication for OPDS 1.0 specification.
https://drafts.opds.io/authentication-for-opds-1.0
"""

from pydantic import BaseModel, Field, field_validator

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.opds.rwpm import Link
from palace.manager.opds.types.link import CompactCollection


class AuthenticationLabels(BaseModel):
    login: str
    password: str


class Authentication(BaseModel):
    type: str
    labels: AuthenticationLabels | None = None
    links: CompactCollection[Link]


class AuthenticationDocument(BaseModel):
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
    authentication: list[Authentication]
    description: str | None = None
    links: CompactCollection[Link] = Field(default_factory=CompactCollection)

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

    def by_type(self, auth_type: str) -> Authentication:
        for auth in self.authentication:
            if auth.type == auth_type:
                return auth
        raise PalaceValueError(f"Unable to find authentication for '{auth_type}'")

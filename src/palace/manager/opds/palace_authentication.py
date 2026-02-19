"""
Palace-specific extensions to the Authentication for OPDS 1.0 specification.

These models extend the standard spec models in :mod:`palace.manager.opds.authentication`
with Palace-specific fields, following the same pattern as :mod:`palace.manager.opds.palace`
for OPDS 2.0 feed extensions.
"""

from typing import Any, ClassVar, cast

from pydantic import Field, model_serializer
from pydantic_core.core_schema import SerializerFunctionWrapHandler

from palace.manager.opds.authentication import (
    Authentication,
    AuthenticationDocument,
    AuthenticationLabels,
)
from palace.manager.opds.base import BaseOpdsModel
from palace.manager.opds.types.link import BaseLink, CompactCollection
from palace.manager.opds.util import drop_if_falsy


class LocalizedString(BaseOpdsModel):
    """A localized string value, used for SAML/OIDC link metadata."""

    language: str
    value: str


class LocalizedLogoUrl(BaseOpdsModel):
    """A localized logo URL, optionally with dimensions."""

    language: str
    value: str
    height: int | None = None
    width: int | None = None


class PalaceAuthenticationLink(BaseLink):
    """A link in an authentication flow with Palace-specific SAML/OIDC metadata.

    Inherits ``href``, ``rel``, ``templated``, and ``type`` from :class:`BaseLink`.
    """

    display_names: list[LocalizedString] | None = None
    descriptions: list[LocalizedString] | None = None
    information_urls: list[LocalizedString] | None = None
    privacy_statement_urls: list[LocalizedString] | None = None
    logo_urls: list[LocalizedLogoUrl] | None = None


class InputDescriptor(BaseOpdsModel):
    """Describes input requirements for a login or password field."""

    keyboard: str
    maximum_length: int | None = None
    barcode_format: str | None = None


class AuthenticationInputs(BaseOpdsModel):
    """Input descriptors for login and password fields."""

    login: InputDescriptor
    password: InputDescriptor


class PalaceAuthentication(Authentication):
    """Palace-specific authentication flow entry.

    Extends the standard :class:`Authentication` with Palace fields
    for input descriptors, descriptions, and richer links.
    """

    description: str | None = None
    inputs: AuthenticationInputs | None = None
    links: CompactCollection[PalaceAuthenticationLink] = Field(  # type: ignore[assignment]
        default_factory=CompactCollection
    )
    # CompactCollection is invariant; PalaceAuthenticationLink is a
    # subclass of BaseLink but the generic type is not covariant.
    labels: AuthenticationLabels | None = None


class WebColorScheme(BaseOpdsModel):
    """Web color scheme for a library."""

    primary: str | None = None
    secondary: str | None = None
    background: str | None = None
    foreground: str | None = None


class PublicKey(BaseOpdsModel):
    """A public key associated with a library."""

    type: str
    value: str


class Features(BaseOpdsModel):
    """Feature flags for client applications."""

    enabled: list[str] = Field(default_factory=list)
    disabled: list[str] = Field(default_factory=list)


class AuthenticationAnnouncement(BaseOpdsModel):
    """An announcement to be included in the authentication document."""

    id: str
    content: str


class PalaceAuthenticationDocument(AuthenticationDocument):
    """Palace-specific authentication document.

    Extends the standard :class:`AuthenticationDocument` with
    Palace-specific fields for branding, features, and announcements.
    """

    MEDIA_TYPE: ClassVar[str] = "application/vnd.opds.authentication.v1.0+json"
    LINK_RELATION: ClassVar[str] = "http://opds-spec.org/auth/document"

    authentication: list[PalaceAuthentication]  # type: ignore[assignment]
    # PalaceAuthentication is a subclass of Authentication, but the
    # list type is invariant so we must ignore the assignment.
    links: CompactCollection[PalaceAuthenticationLink] = Field(  # type: ignore[assignment]
        default_factory=CompactCollection
    )
    # CompactCollection is invariant; PalaceAuthenticationLink is a
    # subclass of BaseLink but the generic type is not covariant.

    service_description: str | None = None
    color_scheme: str | None = None
    web_color_scheme: WebColorScheme | None = None
    public_key: PublicKey | None = None
    features: Features | None = None
    announcements: list[AuthenticationAnnouncement] = Field(default_factory=list)

    @model_serializer(mode="wrap")
    def _serialize(self, serializer: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data = cast(dict[str, Any], serializer(self))
        for field_name in (
            "service_description",
            "color_scheme",
            "web_color_scheme",
            "public_key",
            "description",
        ):
            drop_if_falsy(self, field_name, data)
        return data

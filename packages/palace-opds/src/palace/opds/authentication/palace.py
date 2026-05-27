"""
Palace extensions to the Authentication For OPDS 1.0 document.

https://drafts.opds.io/authentication-for-opds-1.0

These model the non-spec fields that Palace Manager has historically emitted in
its authentication documents. They are kept in their own module (rather than
mixed into the spec models in this package's ``__init__``) so the spec models
stay faithful to the specification and remain reusable for parsing remote
documents. The composed, served document is assembled in ``__init__`` via
multiple inheritance, mirroring how ``opds2.py`` composes ``rwpm`` and
``palace`` link properties.

NOTE: Unlike most Palace extensions, these fields are intentionally *not*
namespaced with ``http://palaceproject.io/terms/`` aliases. They serialize to
the same flat keys the document has always used, because existing mobile and
web clients read them by those names.
"""

from __future__ import annotations

from typing import Any, cast

from pydantic import (
    Field,
    PositiveInt,
    SerializerFunctionWrapHandler,
    model_serializer,
)

from palace.opds.base import BaseOpdsModel


class LocalizedValue(BaseOpdsModel):
    """A localized string value, optionally with image dimensions.

    Used by the Palace SAML / OIDC authenticate-link extensions
    (``display_names``, ``descriptions``, ``logo_urls``, ...). ``height`` and
    ``width`` are only meaningful for logos and are dropped when unset.
    """

    value: str
    language: str | None = None
    height: PositiveInt | None = None
    width: PositiveInt | None = None


class AuthenticationInput(BaseOpdsModel):
    """A single input field (login or password) in a Basic auth flow.

    ``maximum_length`` and ``barcode_format`` are only present when configured.
    """

    keyboard: str | None = None
    maximum_length: PositiveInt | None = None
    barcode_format: str | None = None


class AuthenticationInputs(BaseOpdsModel):
    """The ``inputs`` object of a Basic authentication flow."""

    login: AuthenticationInput
    password: AuthenticationInput


class WebColorScheme(BaseOpdsModel):
    """The ``web_color_scheme`` object describing brand colors for web apps."""

    primary: str | None = None
    secondary: str | None = None
    background: str | None = None
    foreground: str | None = None


class PublicKey(BaseOpdsModel):
    """The library's ``public_key`` used to encrypt data sent to the server."""

    type: str = "RSA"
    value: str

    @model_serializer(mode="wrap")
    def _serialize(self, serializer: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data = cast(dict[str, Any], serializer(self))
        # ``type`` is always present, even when it is the default.
        data["type"] = self.type
        return data


class Features(BaseOpdsModel):
    """The ``features`` object signaling which features clients should offer."""

    enabled: tuple[str, ...] = Field(default_factory=tuple)
    disabled: tuple[str, ...] = Field(default_factory=tuple)

    @model_serializer(mode="wrap")
    def _serialize(self, serializer: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data = cast(dict[str, Any], serializer(self))
        # Both arrays are always present, even when empty.
        data["enabled"] = list(self.enabled)
        data["disabled"] = list(self.disabled)
        return data


class AnnouncementDocument(BaseOpdsModel):
    """A single announcement in the ``announcements`` array.

    Distinct from the SQLAlchemy ``Announcement`` model; this is just the
    wire representation served in the authentication document.
    """

    id: str
    content: str


class AuthenticationExtension(BaseOpdsModel):
    """Palace extension fields carried on each authentication flow object.

    The Authentication for OPDS spec only defines ``type``, ``links`` and
    ``labels`` on an authentication object; ``description`` and ``inputs`` are
    Palace additions.
    """

    description: str | None = None
    inputs: AuthenticationInputs | None = None

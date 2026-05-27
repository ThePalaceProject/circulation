"""
Models for the Authentication for OPDS 1.0 specification.

https://drafts.opds.io/authentication-for-opds-1.0
https://drafts.opds.io/schema/authentication.schema.json

This module has two parts:

* The spec models -- ``AuthenticationLabels``, ``Authentication`` and
  ``AuthenticationDocument`` -- model the specification exactly. They are used
  to *parse* authentication documents from remote servers, and serve as the base
  classes for the document we serve.

* The Palace-served document -- ``PalaceAuthenticationDocument`` -- composes
  those spec models with the Palace extension fields defined in ``palace.py``
  (``description``/``inputs`` on a flow, ``color_scheme``/``features``/
  ``announcements``/... on the document, localized fields on SAML/OIDC
  authenticate links). ``AuthenticateLink`` and ``PalaceAuthentication`` are the
  building blocks it is assembled from. This mirrors how ``opds2.py`` composes
  ``rwpm`` and ``palace`` link properties.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

from pydantic import (
    Field,
    SerializerFunctionWrapHandler,
    field_validator,
    model_serializer,
)

from palace.util.exceptions import PalaceValueError

from palace.opds.authentication import palace
from palace.opds.base import BaseOpdsModel
from palace.opds.rwpm import Link
from palace.opds.types.link import CompactCollection
from palace.opds.util import drop_if_falsy

#: The ``rel`` of the link to an authentication document.
AUTH_DOCUMENT_REL = "http://opds-spec.org/auth/document"

#: The media type used when *serving* an authentication document. Note this is
#: the versioned vendor type historically used by Palace clients, which differs
#: from ``AuthenticationDocument.content_type()``; both are valid per
#: ``AuthenticationDocument.content_types()``.
AUTH_DOCUMENT_MEDIA_TYPE = "application/vnd.opds.authentication.v1.0+json"


# ---------------------------------------------------------------------------
# Authentication for OPDS 1.0 spec models
# ---------------------------------------------------------------------------


class AuthenticationLabels(BaseOpdsModel):
    login: str
    password: str


class Authentication(BaseOpdsModel):
    type: str
    labels: AuthenticationLabels | None = None
    links: CompactCollection[Link] = Field(default_factory=CompactCollection)


class AuthenticationDocument(BaseOpdsModel):
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
    # ``Sequence`` (covariant) rather than ``list`` (invariant) so subclasses
    # can narrow this to a list of an ``Authentication`` subclass.
    authentication: Sequence[Authentication]
    description: str | None = None
    links: CompactCollection[Link] = Field(default_factory=CompactCollection)

    @field_validator("authentication")
    @classmethod
    def _validate_authentication(
        cls, value: Sequence[Authentication]
    ) -> Sequence[Authentication]:
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


# ---------------------------------------------------------------------------
# Palace-served authentication document
#
# ``PalaceAuthenticationDocument`` (at the end of this section) is the document
# Palace Manager actually serves: the spec models above composed with the Palace
# extension fields in ``palace.py``. ``AuthenticateLink`` and
# ``PalaceAuthentication`` are the building blocks it is assembled from, and are
# defined first because the document references them.
# ---------------------------------------------------------------------------


# -- Building blocks --

#: The Palace extension fields that SAML / OIDC authenticate links always emit,
#: even when empty.
_AUTHENTICATE_LINK_LOCALIZED_FIELDS = (
    "display_names",
    "descriptions",
    "information_urls",
    "privacy_statement_urls",
    "logo_urls",
)


class AuthenticateLink(Link):
    """A ``rel="authenticate"`` link carrying the Palace localized extensions.

    Used by the SAML and OIDC providers. The five localized arrays are always
    serialized, even when empty, to preserve the document shape clients expect.
    Plain links (e.g. ``logout``) use ``rwpm.Link`` directly so they do not emit
    these keys.
    """

    display_names: tuple[palace.LocalizedValue, ...] = Field(default_factory=tuple)
    descriptions: tuple[palace.LocalizedValue, ...] = Field(default_factory=tuple)
    information_urls: tuple[palace.LocalizedValue, ...] = Field(default_factory=tuple)
    privacy_statement_urls: tuple[palace.LocalizedValue, ...] = Field(
        default_factory=tuple
    )
    logo_urls: tuple[palace.LocalizedValue, ...] = Field(default_factory=tuple)

    @model_serializer(mode="wrap")
    def _serialize(self, serializer: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data = cast(dict[str, Any], serializer(self))

        # Preserve the behavior inherited from rwpm.Link / BaseLink.
        drop_if_falsy(self, "properties", data)
        drop_if_falsy(self, "templated", data)

        # The localized arrays are always present, even when empty.
        for field_name in _AUTHENTICATE_LINK_LOCALIZED_FIELDS:
            data.setdefault(field_name, [])

        return data


class PalaceAuthentication(Authentication, palace.AuthenticationExtension):
    """An authentication flow object including the Palace ``description`` and
    ``inputs`` extensions.
    """

    @model_serializer(mode="wrap")
    def _serialize(self, serializer: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data = cast(dict[str, Any], serializer(self))

        # Drop extension fields / empty collections that this flow doesn't use.
        drop_if_falsy(self, "labels", data)
        drop_if_falsy(self, "inputs", data)
        drop_if_falsy(self, "links", data)

        return data


# -- The served document --


class PalaceAuthenticationDocument(AuthenticationDocument):
    """The authentication document Palace Manager serves, combining the spec
    fields with Palace extensions.

    This is the headline model of this module; the spec models above and the
    ``AuthenticateLink`` / ``PalaceAuthentication`` building blocks exist to
    assemble it.
    """

    authentication: list[PalaceAuthentication]
    color_scheme: str | None = None
    web_color_scheme: palace.WebColorScheme | None = None
    service_description: str | None = None
    public_key: palace.PublicKey | None = None
    features: palace.Features = Field(default_factory=palace.Features)
    announcements: tuple[palace.AnnouncementDocument, ...] = Field(
        default_factory=tuple
    )

    @model_serializer(mode="wrap")
    def _serialize(self, serializer: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data = cast(dict[str, Any], serializer(self))

        # We surface the library description as ``service_description``; the spec
        # ``description`` field is left unused.
        drop_if_falsy(self, "description", data)
        drop_if_falsy(self, "color_scheme", data)
        drop_if_falsy(self, "web_color_scheme", data)
        drop_if_falsy(self, "service_description", data)
        drop_if_falsy(self, "public_key", data)

        # ``features`` and ``announcements`` are always present.
        if "features" not in data:
            data["features"] = self.features.serialize()
        data.setdefault("announcements", [])

        return data

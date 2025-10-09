from pydantic import (
    Base64Bytes,
    Field,
    NonNegativeInt,
    PositiveInt,
    field_validator,
)

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.opds.base import BaseOpdsModel
from palace.manager.opds.types.date import Iso8601AwareDatetime
from palace.manager.opds.types.link import BaseLink, CompactCollection


class Link(BaseLink):
    """
    https://readium.org/lcp-specs/releases/lcp/latest#35-pointing-to-external-resources-the-links-object
    """

    title: str | None = None
    profile: str | None = None
    length: PositiveInt | None = None
    hash: Base64Bytes | None = None


class ContentKey(BaseOpdsModel):
    """
    https://readium.org/lcp-specs/releases/lcp/latest#34-transmitting-keys-the-encryption-object
    """

    algorithm: str
    encrypted_value: Base64Bytes


class UserKey(BaseOpdsModel):
    """
    https://readium.org/lcp-specs/releases/lcp/latest#34-transmitting-keys-the-encryption-object
    """

    algorithm: str
    text_hint: str
    key_check: Base64Bytes


class Encryption(BaseOpdsModel):
    """
    https://readium.org/lcp-specs/releases/lcp/latest#34-transmitting-keys-the-encryption-object
    """

    profile: str
    content_key: ContentKey
    user_key: UserKey


class Rights(BaseOpdsModel):
    """
    https://readium.org/lcp-specs/releases/lcp/latest#36-identifying-rights-and-restrictions-the-rights-object
    """

    # This is aliased because 'copy' is a method on BaseModel. Print is
    # aliased for consistency.
    # NOTE: Although these look like they should be the same as the
    # fields in the Protection model, they are not. The Protection model
    # defines these as booleans, while rights defines them as the integer
    # number of characters/pages allowed to be copied/printed.
    allow_copy: NonNegativeInt | None = Field(None, alias="copy")
    allow_print: NonNegativeInt | None = Field(None, alias="print")
    start: Iso8601AwareDatetime | None = None
    end: Iso8601AwareDatetime | None = None


class User(BaseOpdsModel):
    """
    https://readium.org/lcp-specs/releases/lcp/latest#37-identifying-the-user-the-user-object
    """

    id: str | None = None
    email: str | None = None
    name: str | None = None
    encrypted: list[str] = Field(default_factory=list)


class Signature(BaseOpdsModel):
    """
    https://readium.org/lcp-specs/releases/lcp/latest#38-signing-the-license-the-signature-object
    """

    algorithm: str
    certificate: Base64Bytes
    value: Base64Bytes


class LicenseDocument(BaseOpdsModel):
    """
    LCP License Document

    This document is defined here:
    https://readium.org/lcp-specs/releases/lcp/latest#3-license-document
    """

    @staticmethod
    def content_type() -> str:
        return "application/vnd.readium.lcp.license.v1.0+json"

    id: str
    issued: Iso8601AwareDatetime
    provider: str
    updated: Iso8601AwareDatetime | None = None
    encryption: Encryption
    links: CompactCollection[Link]
    rights: Rights | None = None
    signature: Signature

    @field_validator("links")
    @classmethod
    def _validate_links(cls, value: CompactCollection[Link]) -> CompactCollection[Link]:
        if value.get(rel="hint") is None:
            raise PalaceValueError("links must contain a link with rel 'hint'")
        if value.get(rel="publication") is None:
            raise PalaceValueError("links must contain a link with rel 'publication'")
        return value

from __future__ import annotations

from collections.abc import Sequence
from enum import StrEnum, auto
from functools import cached_property
from typing import Literal

from pydantic import Field, NonNegativeInt, PositiveFloat, PositiveInt

from palace.manager.opds.base import BaseOpdsModel
from palace.manager.opds.types.date import (
    Iso8601AwareDatetime,
    Iso8601DateOrAwareDatetime,
)
from palace.manager.opds.types.language import LanguageMap, LanguageTag
from palace.manager.opds.types.link import BaseLink, CompactCollection
from palace.manager.opds.util import (
    StrModelOrTuple,
    StrOrModel,
    StrOrTuple,
    obj_or_tuple_to_tuple,
)


class Encryption(BaseOpdsModel):
    """
    The Encryption Module defines how a given resource has been encrypted or obfuscated, and provides
    relevant information for decryption by a User Agent.

    https://github.com/readium/webpub-manifest/blob/master/modules/encryption.md#encryption-object
    https://readium.org/webpub-manifest/schema/extensions/encryption/properties.schema.json
    """

    algorithm: str
    scheme: str | None = None
    profile: str | None = None
    compression: Literal["deflate"] | None = None
    original_length: PositiveInt | None = Field(None, alias="originalLength")


class PresentationProperties(BaseOpdsModel):
    """
    Presentation Hints Properties and Metadata Object.

    https://readium.org/webpub-manifest/schema/experimental/presentation/properties.schema.json
    https://readium.org/webpub-manifest/schema/experimental/presentation/metadata.schema.json
    """

    clipped: bool | None = None
    fit: Literal["contain", "cover", "width", "height"] | None = None
    orientation: Literal["auto", "landscape", "portrait"] | None = None


class EPubProperties(BaseOpdsModel):
    """
    EPub extensions to the Properties Object.

    https://readium.org/webpub-manifest/schema/extensions/epub/properties.schema.json
    """

    contains: (
        set[Literal["mathml", "onix", "remote-resources", "js", "svg", "xmp"]] | None
    ) = None
    layout: Literal["fixed", "reflowable"] | None = None


class EncryptionProperties(BaseOpdsModel):
    """
    Encryption extensions to the Properties Object.

    https://readium.org/webpub-manifest/schema/extensions/encryption/properties.schema.json
    """

    encrypted: Encryption | None = None


class DivinaProperties(BaseOpdsModel):
    """
    Divina extensions to the Properties Object.

    https://readium.org/webpub-manifest/schema/extensions/divina/properties.schema.json
    """

    break_scroll_before: bool = Field(False, alias="break-scroll-before")


class LinkProperties(
    EPubProperties,
    EncryptionProperties,
    DivinaProperties,
    PresentationProperties,
):
    """
    Each Link Object may contain a Properties Object, containing a number of relevant information.

    https://github.com/readium/webpub-manifest/blob/master/properties.md
    https://github.com/readium/webpub-manifest/blob/b43ec57fd28028316272987ccb10c326f0130280/schema/link.schema.json#L33-L54
    """

    page: Literal["left", "right", "center"] | None = None


class Link(BaseLink):
    """
    Link to another resource.

    https://github.com/readium/webpub-manifest/blob/master/README.md#23-links
    https://readium.org/webpub-manifest/schema/link.schema.json
    """

    title: str | None = None
    height: PositiveInt | None = None
    width: PositiveInt | None = None
    bitrate: PositiveFloat | None = None
    duration: PositiveFloat | None = None
    language: StrOrTuple[LanguageTag] | None = None

    @cached_property
    def languages(self) -> Sequence[LanguageTag]:
        return obj_or_tuple_to_tuple(self.language)

    alternate: CompactCollection[Link] = Field(default_factory=CompactCollection)
    children: CompactCollection[Link] = Field(default_factory=CompactCollection)

    properties: LinkProperties = Field(default_factory=LinkProperties)


class AltIdentifier(BaseOpdsModel):
    """
    An identifier for the publication.

    https://github.com/readium/webpub-manifest/tree/master/contexts/default#identifier
    https://github.com/readium/webpub-manifest/blob/master/schema/altIdentifier.schema.json
    """

    value: str
    scheme: str | None = None


class Named(BaseOpdsModel):
    """
    An object with a required translatable name.
    """

    name: LanguageMap


class Contributor(Named):
    """
    A contributor to the publication.

    https://github.com/readium/webpub-manifest/tree/master/contexts/default#contributors
    https://github.com/readium/webpub-manifest/blob/master/schema/contributor-object.schema.json
    """

    sort_as: str | None = Field(None, alias="sortAs")
    identifier: str | None = None
    alt_identifier: list[StrOrModel[AltIdentifier]] = Field(
        default_factory=list, alias="altIdentifier"
    )

    @cached_property
    def alt_identifiers(self) -> Sequence[AltIdentifier]:
        return [
            AltIdentifier(value=alt_id) if isinstance(alt_id, str) else alt_id
            for alt_id in self.alt_identifier
        ]

    position: NonNegativeInt | None = None
    links: CompactCollection[Link] = Field(default_factory=CompactCollection)


class ContributorWithRole(Contributor):
    """
    A generic contributor, where an optional role can be included.
    """

    # TODO: Add some validation for the roles that we accept here.
    #   We might want to make role required here, or default it to
    #   something generic like "contributor".
    role: StrOrTuple[str] | None = None

    @cached_property
    def roles(self) -> Sequence[str]:
        return obj_or_tuple_to_tuple(self.role)


class SubjectScheme(StrEnum):
    """
    https://github.com/readium/webpub-manifest/tree/master/contexts/default#subjects
    """

    BIC = "https://bic.org.uk/"
    BISAC = "https://www.bisg.org/#bisac"
    CLIL = "http://clil.org/"
    Thema = "https://ns.editeur.org/thema/"


class Subject(Named):
    """
    A subject of the publication.

    https://github.com/readium/webpub-manifest/tree/master/contexts/default#subjects
    https://github.com/readium/webpub-manifest/blob/master/schema/subject-object.schema.json
    """

    sort_as: str | None = Field(None, alias="sortAs")
    code: str | None = None
    scheme: str | None = None
    links: CompactCollection[Link] = Field(default_factory=CompactCollection)


class BelongsTo(BaseOpdsModel):
    """
    https://github.com/readium/webpub-manifest/tree/master/contexts/default#collections--series
    https://github.com/readium/webpub-manifest/blob/b43ec57fd28028316272987ccb10c326f0130280/schema/metadata.schema.json#L138-L147
    """

    series_data: StrModelOrTuple[Contributor] | None = Field(None, alias="series")

    @cached_property
    def series(self) -> Sequence[Contributor]:
        return _named_or_sequence_to_sequence(self.series_data, Contributor)

    collection: StrModelOrTuple[Contributor] | None = None

    @cached_property
    def collections(self) -> Sequence[Contributor]:
        return _named_or_sequence_to_sequence(self.collection, Contributor)


def _named_or_sequence_to_sequence[NamedT: Named](
    value: str | NamedT | tuple[str | NamedT, ...] | None, cls: type[NamedT]
) -> Sequence[NamedT]:
    return tuple(
        cls(name=item) if isinstance(item, str) else item  # type: ignore[misc]
        for item in obj_or_tuple_to_tuple(value)
    )


class Metadata(BaseOpdsModel):
    """
    Metadata associated with a publication.

    https://github.com/readium/webpub-manifest/tree/master/contexts/default
    https://github.com/readium/webpub-manifest/blob/master/schema/metadata.schema.json
    """

    title: LanguageMap
    type: str | None = Field(None, alias="@type")
    sort_as: str | None = Field(None, alias="sortAs")
    subtitle: LanguageMap | None = None
    identifier: str | None = None
    alt_identifier: list[StrOrModel[AltIdentifier]] = Field(
        default_factory=list, alias="altIdentifier"
    )

    @cached_property
    def alt_identifiers(self) -> Sequence[AltIdentifier]:
        return [
            AltIdentifier(value=alt_id) if isinstance(alt_id, str) else alt_id
            for alt_id in self.alt_identifier
        ]

    modified: Iso8601AwareDatetime | None = None
    published: Iso8601DateOrAwareDatetime | None = None
    language: StrOrTuple[LanguageTag] | None = None

    @cached_property
    def languages(self) -> Sequence[LanguageTag]:
        return obj_or_tuple_to_tuple(self.language)

    author: StrModelOrTuple[Contributor] | None = None

    @cached_property
    def authors(self) -> Sequence[Contributor]:
        return _named_or_sequence_to_sequence(self.author, Contributor)

    translator: StrModelOrTuple[Contributor] | None = None

    @cached_property
    def translators(self) -> Sequence[Contributor]:
        return _named_or_sequence_to_sequence(self.translator, Contributor)

    editor: StrModelOrTuple[Contributor] | None = None

    @cached_property
    def editors(self) -> Sequence[Contributor]:
        return _named_or_sequence_to_sequence(self.editor, Contributor)

    artist: StrModelOrTuple[Contributor] | None = None

    @cached_property
    def artists(self) -> Sequence[Contributor]:
        return _named_or_sequence_to_sequence(self.artist, Contributor)

    illustrator: StrModelOrTuple[Contributor] | None = None

    @cached_property
    def illustrators(self) -> Sequence[Contributor]:
        return _named_or_sequence_to_sequence(self.illustrator, Contributor)

    letterer: StrModelOrTuple[Contributor] | None = None

    @cached_property
    def letterers(self) -> Sequence[Contributor]:
        return _named_or_sequence_to_sequence(self.letterer, Contributor)

    penciler: StrModelOrTuple[Contributor] | None = None

    @cached_property
    def pencilers(self) -> Sequence[Contributor]:
        return _named_or_sequence_to_sequence(self.penciler, Contributor)

    colorist: StrModelOrTuple[Contributor] | None = None

    @cached_property
    def colorists(self) -> Sequence[Contributor]:
        return _named_or_sequence_to_sequence(self.colorist, Contributor)

    inker: StrModelOrTuple[Contributor] | None = None

    @cached_property
    def inkers(self) -> Sequence[Contributor]:
        return _named_or_sequence_to_sequence(self.inker, Contributor)

    narrator: StrModelOrTuple[Contributor] | None = None

    @cached_property
    def narrators(self) -> Sequence[Contributor]:
        return _named_or_sequence_to_sequence(self.narrator, Contributor)

    contributor: StrModelOrTuple[ContributorWithRole] | None = None

    @cached_property
    def contributors(self) -> Sequence[ContributorWithRole]:
        return _named_or_sequence_to_sequence(self.contributor, ContributorWithRole)

    publisher: StrModelOrTuple[Contributor] | None = None

    @cached_property
    def publishers(self) -> Sequence[Contributor]:
        return _named_or_sequence_to_sequence(self.publisher, Contributor)

    imprint: StrModelOrTuple[Contributor] | None = None

    @cached_property
    def imprints(self) -> Sequence[Contributor]:
        return _named_or_sequence_to_sequence(self.imprint, Contributor)

    subject: StrModelOrTuple[Subject] | None = None

    @cached_property
    def subjects(self) -> Sequence[Subject]:
        return _named_or_sequence_to_sequence(self.subject, Subject)

    layout: Literal["fixed", "reflowable", "scrolled"] | None = None
    reading_progression: Literal["ltr", "rtl"] = Field(
        "ltr", alias="readingProgression"
    )
    description: str | None = None
    duration: PositiveFloat | None = None
    number_of_pages: PositiveInt | None = Field(None, alias="numberOfPages")
    abridged: bool | None = None

    belongs_to: BelongsTo = Field(default_factory=BelongsTo, alias="belongsTo")

    presentation: PresentationProperties = Field(default_factory=PresentationProperties)


class LinkRelations(StrEnum):
    """
    https://readium.org/webpub-manifest/relationships.html
    """

    alternate = auto()
    contents = auto()
    cover = auto()
    manifest = auto()
    search = auto()
    self = auto()

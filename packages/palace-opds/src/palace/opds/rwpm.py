from __future__ import annotations

from collections.abc import Sequence
from enum import StrEnum, auto
from functools import cached_property
from typing import Any, Final, Literal, cast

from pydantic import (
    AliasChoices,
    Field,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    SerializerFunctionWrapHandler,
    model_serializer,
)

from palace.opds.a11y import Accessibility
from palace.opds.base import BaseOpdsModel
from palace.opds.types.date import (
    Iso8601AwareDatetime,
    Iso8601DateOrAwareDatetime,
)
from palace.opds.types.language import LanguageMap, LanguageTag
from palace.opds.types.link import BaseLink, CompactCollection
from palace.opds.util import (
    StrModelOrTuple,
    StrOrModel,
    StrOrTuple,
    drop_if_falsy,
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
        frozenset[Literal["mathml", "onix", "remote-resources", "js", "svg", "xmp"]]
        | None
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

    @model_serializer(mode="wrap")
    def _serialize(self, serializer: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data = cast(dict[str, Any], serializer(self))

        # Only include properties in output if there are properties set
        drop_if_falsy(self, "properties", data)

        # Only include templated in the output when it is True
        drop_if_falsy(self, "templated", data)

        return data


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

    @model_serializer(mode="wrap")
    def _serialize(self, serializer: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data = cast(dict[str, Any], serializer(self))
        drop_if_falsy(self, "links", data)
        return data


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


class SerialType(StrEnum):
    """
    What kind of serial a publication belongs to.

    These match the serial types we store on a periodical, so the importer can
    pass the value straight through.
    """

    MAGAZINE = auto()
    JOURNAL = auto()
    NEWSPAPER = auto()

    PERIODICAL = auto()
    """The feed says it comes out in issues, but not what kind of thing it is."""


class SerialReference(Contributor):
    """
    The serial a publication belongs to: a magazine, journal or newspaper.

    This is a normal RWPM contributor object, so it already carries ``name``,
    ``sortAs``, ``identifier`` and ``position``.
    """

    position: int | float | None = None  # type: ignore[assignment] # widens Contributor.position
    """
    Where this issue sits in the serial.

    This widens the whole number a contributor's position allows. An issue's
    position may be a decimal, so that an issue sitting between two numbered
    issues (#0, a half issue, #14.1) still sorts in the right place. Feeds also
    use a date here, such as ``20261124``, which is a whole number and fits
    either way. We keep whichever we were given, because the value is only ever
    a sort key.
    """

    volume: str | int | float | None = None
    """
    The volume, where the feed puts it here instead of in its own ``volume``
    key. It is written as a plain value rather than an object, so it may be a
    number. Read it through :attr:`BelongsTo.volume`, which looks in both
    places and always gives back a string.
    """


class BelongsTo(BaseOpdsModel):
    """
    The series, collections and serials a publication belongs to.

    Beyond the ``series`` and ``collection`` keys in the specification, feeds in
    the wild use a handful of keys to say that a publication is one issue of a
    serial. Each publisher picks a different one, and some use the schema.org
    name rather than a plain word. We accept all of them and sort it out here,
    so that everything reading this model sees one idea instead of a list of
    special cases. Use :attr:`serial`, :attr:`serial_type` and :attr:`volume`
    rather than the raw fields.

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

    magazine: StrModelOrTuple[SerialReference] | None = None
    journal: StrModelOrTuple[SerialReference] | None = None
    newspaper: StrModelOrTuple[SerialReference] | None = None
    periodical: StrModelOrTuple[SerialReference] | None = None

    schema_periodical: StrModelOrTuple[SerialReference] | None = Field(
        None,
        serialization_alias="schema:Periodical",
        validation_alias=AliasChoices(
            "schema:Periodical",
            "http://schema.org/Periodical",
            "https://schema.org/Periodical",
        ),
    )
    """
    The schema.org name for a serial. This is not an RWPM term and feeds are
    asked not to send it, but some do, so we read it rather than drop the
    publication's only link to its magazine. We read the short form and both
    the http and https spellings of the full name, and write back whichever we
    read.

    A feed using this key has told us nothing about what kind of serial it is,
    and in practice it is always a magazine, so we read it as one.

    https://schema.org/Periodical
    """

    volume_data: StrModelOrTuple[Contributor] | None = Field(None, alias="volume")

    @cached_property
    def volumes(self) -> Sequence[Contributor]:
        return _named_or_sequence_to_sequence(self.volume_data, Contributor)

    @cached_property
    def serials(self) -> Sequence[SerialReference]:
        """
        Every serial this publication belongs to, whichever key it came in on.

        More specific keys come first, so a feed that sends both ``magazine``
        and ``periodical`` is read as a magazine.
        """
        return tuple(
            serial
            for field_name, _ in _SERIAL_FIELDS
            for serial in _named_or_sequence_to_sequence(
                getattr(self, field_name), SerialReference
            )
        )

    @cached_property
    def serial(self) -> SerialReference | None:
        """The serial this publication is an issue of, or None if it is not one."""
        return self.serials[0] if self.serials else None

    @cached_property
    def serial_type(self) -> SerialType | None:
        """
        What kind of serial the feed said this is, or None if it did not say.

        A feed that only says "periodical" gets :attr:`SerialType.PERIODICAL`,
        which means we know it comes out in issues but not what kind it is.
        """
        for field_name, serial_type in _SERIAL_FIELDS:
            if _named_or_sequence_to_sequence(
                getattr(self, field_name), SerialReference
            ):
                return serial_type
        return None

    @cached_property
    def volume(self) -> str | None:
        """
        The volume this issue belongs to, or None if the feed did not give one.

        Feeds either give the volume its own ``belongsTo`` key or flatten it
        onto the serial. We look in its own key first, then on the serial.
        """
        for volume in self.volumes:
            return str(volume.name)
        for serial in self.serials:
            if serial.volume is not None:
                return _scalar_to_str(serial.volume)
        return None


def _scalar_to_str(value: str | int | float) -> str:
    """
    Render a flattened volume as the string a reader would see.

    A whole number written as a decimal, ``2026.0``, comes back as ``"2026"``.
    """
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


_SERIAL_FIELDS: Final[tuple[tuple[str, SerialType], ...]] = (
    ("magazine", SerialType.MAGAZINE),
    ("journal", SerialType.JOURNAL),
    ("newspaper", SerialType.NEWSPAPER),
    ("periodical", SerialType.PERIODICAL),
    ("schema_periodical", SerialType.MAGAZINE),
)
"""
The ``belongsTo`` keys that name a serial, most specific first.

``periodical`` is the generic key, used for a serial whose kind the feed does
not say, and for comic series, which have no key of their own.
"""


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

    accessibility: Accessibility = Field(default_factory=Accessibility)

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

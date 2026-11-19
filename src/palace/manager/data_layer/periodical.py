from __future__ import annotations

from pydantic import field_validator

from palace.manager.data_layer.base.frozen import BaseFrozenData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.sqlalchemy.model.periodical import SerialType
from palace.manager.util.languages import LanguageCodes


class PeriodicalData(BaseFrozenData):
    """The serial a publication is an issue of, as a source described it.

    This is what a feed said about an issue's magazine, journal or newspaper,
    before anything has been looked up or created in the database. An importer
    turns it into a :class:`~palace.manager.sqlalchemy.model.periodical.Periodical`,
    a :class:`~palace.manager.sqlalchemy.model.periodical.PeriodicalVolume` and
    a place for the issue within that volume.

    Every field is optional. Sources leave most of them out: a feed that says
    only that an issue belongs to a magazine called "Harper's" gives us a
    title and nothing else.

    The OPDS 2.0 extractor fills this in from ``belongsTo``. Nothing reads it
    yet; the importer that creates the rows comes in a later change.
    """

    serial_type: SerialType | None = None
    """What kind of serial this is, where the source said.

    ``SerialType.PERIODICAL`` means the source said it comes out in issues but
    not what kind of thing it is. ``None`` means it did not say at all.
    """

    serial_type_guessed: bool = False
    """True when the kind above is a guess rather than something the source said.

    A feed may name the serial with the schema.org ``schema:Periodical`` key,
    which is the one name schema.org has for magazines, journals and
    newspapers alike. We read it as a magazine, because that is the likeliest
    of the three and it is the reading the feed profile gives vendors, but a
    journal sent that way is recorded as a magazine and nothing in the feed
    gets the difference back.

    The extractor is the only place that can see this, so it records it here.
    Whatever creates a ``Periodical`` from this reports it, so that a feed we
    have read the wrong way can be found and its vendor asked to say which of
    the three it means.
    """

    identifier: IdentifierData | None = None
    """What the serial is identified by, normally its ISSN.

    This is the identifier of the magazine, not of the issue. It is what
    merges the same magazine arriving from two different distributors, so a
    source that gives one is giving us the most useful thing it has.
    """

    title: str | None = None
    """The name of the serial, for example "Harper's Magazine"."""

    sort_title: str | None = None
    """The name of the serial as it should sort, for example "Harper's Magazine"."""

    publisher: str | None = None
    """Who publishes the serial."""

    language: str | None = None
    """The language the serial is published in, as a three-letter code."""

    volume_label: str | None = None
    """What to call this issue's volume on screen, for example "Volume 12" or "2024".

    Most consumer magazines have no volume at all, and we never make one up.
    """

    volume_position: int | None = None
    """Where this issue's volume sorts within the serial. Lower comes first."""

    issue_position: int | float | None = None
    """Where this issue sorts within its volume. Lower comes first.

    This allows a decimal, because an issue sitting between two numbered
    issues (#0, a half issue, #14.1) needs one to sort in the right place.
    The value is only ever a sort key, so we keep whichever we were given.
    """

    issue_position_from_date: bool = False
    """True when the position above was worked out from the issue's date.

    A source that numbers its issues gives small numbers: 1, 2, 3. A source
    that only dates them gives us nothing to sort by, so we build a number out
    of the date instead, which is large: 20261101. The two are not comparable,
    so a volume holding some of each would sort wrongly.

    Whatever creates a volume from this sees every issue in it at once, which
    is the only place the two can be brought onto one scale. This says which
    issues would need rewriting there.
    """

    @field_validator("language")
    @classmethod
    def _convert_language_alpha3(cls, value: str | None) -> str | None:
        """Store the language the same way an issue's language is stored.

        ``BibliographicData`` converts a language to its three-letter code as
        it comes in, and sources hand us the two-letter one. Without the same
        conversion here, a magazine would be stored as "en" while its own
        issues were stored as "eng".
        """
        if value is not None:
            value = LanguageCodes.string_to_alpha_3(value)
        return value

from __future__ import annotations

from palace.manager.data_layer.base.frozen import BaseFrozenData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.sqlalchemy.model.periodical import SerialType


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

    Nothing reads this yet. The OPDS 2.0 extractor fills it in from
    ``belongsTo`` in a later change.
    """

    serial_type: SerialType | None = None
    """What kind of serial this is, where the source said.

    ``SerialType.PERIODICAL`` means the source said it comes out in issues but
    not what kind of thing it is. ``None`` means it did not say at all.
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
    """The language the serial is published in."""

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

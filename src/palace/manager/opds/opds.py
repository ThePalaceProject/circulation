from pydantic import PositiveInt

from palace.manager.opds.base import BaseLink, BaseOpdsModel


class Price(BaseOpdsModel):
    """
    https://drafts.opds.io/opds-2.0#53-acquisition-links
    """

    currency: str
    value: float


class Link(BaseLink):
    """Link to another resource."""

    title: str | None = None
    height: PositiveInt | None = None
    width: PositiveInt | None = None
    bitrate: PositiveInt | None = None
    duration: PositiveInt | None = None

from functools import cached_property

from pydantic import PositiveInt

from palace.manager.opds.base import (
    BaseLink,
    BaseOpdsModel,
    ListOfLinks,
    obj_or_set_to_set,
)


class Link(BaseLink):
    """Link to another resource."""

    title: str | None = None
    height: PositiveInt | None = None
    width: PositiveInt | None = None
    bitrate: PositiveInt | None = None
    duration: PositiveInt | None = None
    language: frozenset[str] | str | None = None
    alternate: ListOfLinks["Link"] = ListOfLinks()
    children: ListOfLinks["Link"] = ListOfLinks()

    @cached_property
    def languages(self) -> frozenset[str]:
        return obj_or_set_to_set(self.language)


class Price(BaseOpdsModel):
    currency: str
    value: float

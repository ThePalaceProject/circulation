from __future__ import annotations

from functools import cached_property
from typing import Any, TypeVar

from pydantic import BaseModel, ConfigDict, GetCoreSchemaHandler, PositiveInt
from pydantic_core import core_schema
from uritemplate import URITemplate, variable

from palace.manager.core.exceptions import PalaceValueError

T = TypeVar("T")


def obj_or_set_to_set(value: T | set[T] | None) -> set[T]:
    """Convert object or set of objects to a set of objects."""
    if value is None:
        return set()
    if isinstance(value, set):
        return value
    return {value}


class BaseOpdsModel(BaseModel):
    """Base class for OPDS models."""

    model_config = ConfigDict(
        populate_by_name=True,
        frozen=True,
    )


class SetOfLinks(set["Link"]):
    """Property allowing to contain only unique links."""

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: type[Any], handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        assert source_type is SetOfLinks
        return core_schema.no_info_after_validator_function(
            cls._validate,
            core_schema.set_schema(),
        )

    @classmethod
    def _validate(cls, value: set[Link]) -> SetOfLinks:
        links = set()
        for link in value:
            if (link.rels, link.href, link.type) in links:
                raise PalaceValueError(
                    f"Duplicate link with relation '{link.rel}', type '{link.type}' and href '{link.href}'"
                )
            links.add((link.rels, link.href, link.type))
        return cls(value)

    def get(self, rel: str | None = None, type: str | None = None) -> Link | None:
        """
        Return the link with the specific relation and type. Raises an
        exception if there are multiple links with the same relation and type.
        """
        links = self.get_set(rel, type)
        if len(links) > 1:
            raise PalaceValueError(
                f"Multiple links with relation '{rel}' and type '{type}'"
            )
        return next(iter(links), None)

    def get_set(self, rel: str | None = None, type: str | None = None) -> set[Link]:
        """
        Return links with the specific relation and type.
        """
        return {
            link
            for link in self
            if (rel is None or rel in link.rels) and (type is None or type == link.type)
        }


class Link(BaseOpdsModel):
    """Link to another resource."""

    href: str
    templated: bool = False
    type: str | None = None
    title: str | None = None
    rel: set[str] | str | None = None
    height: PositiveInt | None = None
    width: PositiveInt | None = None
    bitrate: PositiveInt | None = None
    duration: PositiveInt | None = None
    language: set[str] | str | None = None
    alternate: SetOfLinks = SetOfLinks()
    children: SetOfLinks = SetOfLinks()

    @cached_property
    def rels(self) -> set[str]:
        return obj_or_set_to_set(self.rel)

    @cached_property
    def languages(self) -> set[str]:
        return obj_or_set_to_set(self.language)

    def href_templated(self, var_dict: variable.VariableValueDict | None = None) -> str:
        """
        Return the URL with template variables expanded, if necessary.
        """
        if not self.templated:
            return self.href
        template = URITemplate(self.href)
        return template.expand(var_dict)


class Price(BaseOpdsModel):
    currency: str
    value: float

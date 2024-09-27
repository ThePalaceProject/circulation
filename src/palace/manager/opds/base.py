from __future__ import annotations

import typing
from functools import cached_property
from typing import Any, TypeVar

from pydantic import BaseModel, ConfigDict, GetCoreSchemaHandler
from pydantic_core import core_schema
from uritemplate import URITemplate, variable

from palace.manager.core.exceptions import PalaceValueError

T = TypeVar("T")


def obj_or_set_to_set(value: T | set[T] | frozenset[T] | None) -> frozenset[T]:
    """Convert object or set of objects to a set of objects."""
    if value is None:
        return frozenset()
    if isinstance(value, set):
        return frozenset(value)
    elif isinstance(value, frozenset):
        return value
    return frozenset({value})


class BaseOpdsModel(BaseModel):
    """Base class for OPDS models."""

    model_config = ConfigDict(
        populate_by_name=True,
        frozen=True,
    )


class BaseLink(BaseOpdsModel):
    """The various models all have links with this same basic structure, but
    with additional fields, so we define this base class to avoid repeating
    the same fields in each model, and so we can use the same basic validation
    for them all.
    """

    href: str
    rel: set[str] | str
    templated: bool = False
    type: str | None = None

    @cached_property
    def rels(self) -> frozenset[str]:
        return obj_or_set_to_set(self.rel)

    def href_templated(self, var_dict: variable.VariableValueDict | None = None) -> str:
        """
        Return the URL with template variables expanded, if necessary.
        """
        if not self.templated:
            return self.href
        template = URITemplate(self.href)
        return template.expand(var_dict)


LinkT = TypeVar("LinkT", bound="BaseLink")


class ListOfLinks(list[LinkT]):
    """Set of links where each link can only appear once."""

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: type[Any], handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        origin_type = typing.get_origin(source_type)
        assert origin_type is ListOfLinks
        [container_type] = typing.get_args(source_type)
        return core_schema.no_info_after_validator_function(
            cls._validate,
            core_schema.list_schema(handler(container_type)),
        )

    @classmethod
    def _validate(cls, value: list[LinkT]) -> ListOfLinks[LinkT]:
        link_set = set()
        links: ListOfLinks[LinkT] = ListOfLinks()
        for link in value:
            if (link.rels, link.href, link.type) in link_set:
                raise PalaceValueError(
                    f"Duplicate link with relation '{link.rel}', type '{link.type}' and href '{link.href}'"
                )
            link_set.add((link.rels, link.href, link.type))
            links.append(link)
        return links

    def get(
        self, *, rel: str | None = None, type: str | None = None, raising: bool = False
    ) -> LinkT | None:
        """
        Return the link with the specific relation and type. Raises an
        exception if there are multiple links with the same relation and type.
        """
        links = self.get_list(rel=rel, type=type)
        if len(links) > 1 and raising:
            match (rel, type):
                case (None, None):
                    err = "Multiple links found"
                case (_, None):
                    err = f"Multiple links with rel='{rel}'"
                case (None, _):
                    err = f"Multiple links with type='{type}'"
                case _:
                    err = f"Multiple links with rel='{rel}' and type='{type}'"
            raise PalaceValueError(err)
        return next(iter(links), None)

    def get_list(
        self, *, rel: str | None = None, type: str | None = None
    ) -> list[LinkT]:
        """
        Return links with the specific relation and type.
        """
        return [
            link
            for link in self
            if (rel is None or rel in link.rels) and (type is None or type == link.type)
        ]

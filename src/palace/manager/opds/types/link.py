from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from functools import cached_property
from typing import Any, Literal, TypeVar, cast, get_args, overload

from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema
from typing_extensions import Self
from uritemplate import URITemplate, variable

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.opds.base import BaseOpdsModel
from palace.manager.opds.util import StrOrTuple, obj_or_tuple_to_tuple


class BaseLink(BaseOpdsModel):
    """The various models all have links with this same basic structure, but
    with additional fields, so we define this base class to avoid repeating
    the same fields in each model, and so we can use the same basic validation
    for them all.
    """

    href: str
    rel: StrOrTuple[str] | None = None
    templated: bool = False
    type: str | None = None

    @cached_property
    def rels(self) -> Sequence[str]:
        return obj_or_tuple_to_tuple(self.rel)

    def href_templated(self, var_dict: variable.VariableValueDict | None = None) -> str:
        """
        Return the URL with template variables expanded, if necessary.
        """
        if not self.templated:
            return self.href
        template = URITemplate(self.href)
        return template.expand(var_dict)


LinkT = TypeVar("LinkT", bound="BaseLink", covariant=True)


class CompactCollection(Sequence[LinkT]):
    """
    Implements a Readium webpub manifest 'Compact Collection'.

    A compact collection is just a json array of link objects. This class
    provides some helpful methods for finding links within the collection
    and has some pydantic configuration to make sure that it can be
    loaded and validated, and serialized correctly.

    This is implemented as an immutable sequence of links, so that LinkT
    can be any subclass of BaseLink. This is helpful because the various
    specifications all have slightly different requirements for the links.
    """

    __slots__ = ("_links", "_by_rel", "_by_type", "_by_rel_type")

    def __init__(self, iterable: Iterable[LinkT] = ()) -> None:
        self._links: tuple[LinkT, ...] = tuple(iterable)
        self._by_rel: dict[str, tuple[LinkT, ...]] | None = None
        self._by_type: dict[str, tuple[LinkT, ...]] | None = None
        self._by_rel_type: dict[tuple[str, str], tuple[LinkT, ...]] | None = None

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: type[Any], handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        """
        Custom Pydantic validation function.

        Implementation is based on:
        https://docs.pydantic.dev/latest/concepts/types/#generic-containers
        """
        args = get_args(source_type)
        if args:
            # replace the type and rely on Pydantic to generate the right schema for `list`
            list_schema = handler.generate_schema(list[args[0]])  # type: ignore[valid-type]
        else:
            list_schema = handler.generate_schema(list)

        from_list_schema = core_schema.chain_schema(
            [
                list_schema,
                core_schema.no_info_plain_validator_function(cls._validate),
            ]
        )

        return core_schema.json_or_python_schema(
            json_schema=from_list_schema,
            python_schema=core_schema.union_schema(
                [
                    # check if it's an instance first before doing any further work
                    core_schema.is_instance_schema(cls),
                    from_list_schema,
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda x: x._links, when_used="json"
            ),
        )

    @classmethod
    def _validate(cls, links: Iterable[LinkT]) -> CompactCollection[LinkT]:
        link_set = set()
        for link in links:
            if (link.rels, link.href, link.type) in link_set:
                raise PalaceValueError(
                    f"Duplicate link with relation '{link.rel}', type '{link.type}' and href '{link.href}'"
                )
            link_set.add((link.rels, link.href, link.type))
        return cls(links)

    @overload
    def __getitem__(self, index: int) -> LinkT: ...

    @overload
    def __getitem__(self, index: slice) -> Self: ...

    def __getitem__(self, index: int | slice) -> LinkT | Self:
        if isinstance(index, slice):
            return self.__class__(self._links[index])
        else:
            return self._links[index]

    def __len__(self) -> int:
        return len(self._links)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, CompactCollection):
            return False
        return self._links == other._links

    def __str__(self) -> str:
        return str(self._links)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self._links})>"

    @overload
    def get(
        self,
        *,
        rel: str | None = ...,
        type: str | None = ...,
        raising: Literal[True],
    ) -> LinkT: ...

    @overload
    def get(
        self, *, rel: str | None = ..., type: str | None = ..., raising: bool = ...
    ) -> LinkT | None: ...

    def get(
        self, *, rel: str | None = None, type: str | None = None, raising: bool = False
    ) -> LinkT | None:
        """
        Return the link with the specific relation and type. Raises an
        exception if there are multiple links with the same relation and type.
        """
        links = self.get_collection(rel=rel, type=type)
        if (num_links := len(links)) != 1 and raising:
            if num_links == 0:
                err = "No links found"
            else:
                err = "Multiple links found"

            match rel, type:
                case None, None:
                    # Nothing to add to the error message
                    ...
                case _, None:
                    err += f" with rel='{rel}'"
                case None, _:
                    err += f" with type='{type}'"
                case _:
                    err += f" with rel='{rel}' and type='{type}'"
            raise PalaceValueError(err)
        return next(iter(links), None)

    def get_collection(
        self, *, rel: str | None = None, type: str | None = None
    ) -> Self:
        """
        Return tuple of links with the specific relation and type.

        We build an index of the links by relation and type so that we can
        quickly find the links with the specific relation and type. We do
        this because validation often requires finding links with specific
        rel and type values, sometimes repeatedly, so we want this operation
        to be reasonably fast.
        """
        match rel, type:
            case None, None:
                return self
            case _, None:
                if self._by_rel is None:
                    self._by_rel = self._build_by_rel()
                return self.__class__(self._by_rel.get(cast(str, rel), ()))
            case None, _:
                if self._by_type is None:
                    self._by_type = self._build_by_type()
                return self.__class__(self._by_type.get(type, ()))
            case _:
                if self._by_rel_type is None:
                    self._by_rel_type = self._build_by_rel_type()
                return self.__class__(self._by_rel_type.get((rel, type), ()))

    def _build_by_rel(self) -> dict[str, tuple[LinkT, ...]]:
        by_rel = defaultdict(list)
        for link in self._links:
            for rel in link.rels:
                by_rel[rel].append(link)
        return {rel: tuple(links) for rel, links in by_rel.items()}

    def _build_by_type(self) -> dict[str, tuple[LinkT, ...]]:
        by_type = defaultdict(list)
        for link in self._links:
            if link.type is not None:
                by_type[link.type].append(link)
        return {type_: tuple(links) for type_, links in by_type.items()}

    def _build_by_rel_type(self) -> dict[tuple[str, str], tuple[LinkT, ...]]:
        by_rel_type = defaultdict(list)
        for link in self._links:
            if link.type is not None:
                for rel in link.rels:
                    by_rel_type[(rel, link.type)].append(link)
        return {rel_type: tuple(links) for rel_type, links in by_rel_type.items()}

from __future__ import annotations

from collections.abc import Sequence
from typing import Annotated, Any, TypeVar

from pydantic import Discriminator, Tag

from palace.manager.opds.base import BaseOpdsModel


def obj_or_tuple_to_tuple[T](value: T | list[T] | tuple[T, ...] | None) -> Sequence[T]:
    """
    Convert object or sequence (list or tuple) of objects to a tuple of objects.
    """
    if value is None:
        return tuple()
    if isinstance(value, (list, tuple)):
        return tuple(value)
    return (value,)


# See these links for more information about Pydantic type discriminators:
# https://docs.pydantic.dev/latest/concepts/types/#generics
# https://docs.pydantic.dev/latest/concepts/unions/#discriminated-unions-with-callable-discriminator
def _discriminate_opds_type_union(value: Any) -> str | None:
    if isinstance(value, (str, bytes, bytearray)):
        return "string"
    if isinstance(value, (dict, BaseOpdsModel)):
        return "OpdsObject"
    if isinstance(value, (list, tuple)):
        return "list"
    return None


# Some type vars used in the TypeAlias definitions below.
# Once https://github.com/pydantic/pydantic/issues/9418 is resolved
# StrT could use a default as well as a bound, so we would only need
# to specify it in cases where we are using a different string type.
StrT = TypeVar("StrT", bound=str)
BaseOpdsModelT = TypeVar("BaseOpdsModelT", bound=BaseOpdsModel)


StrOrTuple = Annotated[
    Annotated[tuple[StrT, ...], Tag("list")] | Annotated[StrT, Tag("string")],
    Discriminator(
        _discriminate_opds_type_union,
        custom_error_type="invalid_str_or_tuple",
        custom_error_message="Input should be a valid string or list of strings",
    ),
]
"""
A Pydantic model field TypeAlias for:
  StrT | tuple[StrT, ...]
Uses a discriminator to determine the union type, so we get better error messages for validation.
"""


StrOrModel = Annotated[
    Annotated[BaseOpdsModelT, Tag("OpdsObject")] | Annotated[str, Tag("string")],
    Discriminator(
        _discriminate_opds_type_union,
        custom_error_type="invalid_str_or_model",
        custom_error_message="Input should be a valid string or OPDS object",
    ),
]
"""
A Pydantic model field TypeAlias for:
  str | BaseOpdsModelT
Uses a discriminator to determine the union type, so we get better error messages for validation.
"""


StrModelOrTuple = Annotated[
    (
        Annotated[BaseOpdsModelT, Tag("OpdsObject")]
        | Annotated[str, Tag("string")]
        | Annotated[tuple[StrOrModel[BaseOpdsModelT], ...], Tag("list")]
    ),
    Discriminator(
        _discriminate_opds_type_union,
        custom_error_type="invalid_str_model_or_tuple",
        custom_error_message="Input should be a valid string, OPDS object, or list",
    ),
]
"""
A Pydantic model field TypeAlias for:
  str | BaseOpdsModelT | tuple[str | BaseOpdsModelT, ...]
Uses a discriminator to determine the union type, so we get better error messages for validation.
"""

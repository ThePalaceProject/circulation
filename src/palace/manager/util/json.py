from __future__ import annotations

import json
from typing import Any, TypedDict, Unpack

from pydantic_core import to_jsonable_python


def json_encoder(obj: Any) -> Any:
    # Handle Flask Babel LazyString objects.
    if hasattr(obj, "__html__"):
        return str(obj.__html__())

    # Pass everything else off to Pydantic JSON encoder.
    return to_jsonable_python(obj)


class _JsonDumpsKwargs(TypedDict, total=False):
    skipkeys: bool
    ensure_ascii: bool
    check_circular: bool
    allow_nan: bool
    indent: None | int | str
    separators: tuple[str, str] | None
    sort_keys: bool


def json_serializer(obj: Any, **kwargs: Unpack[_JsonDumpsKwargs]) -> str:
    return json.dumps(obj, default=json_encoder, **kwargs)

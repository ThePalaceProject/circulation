from __future__ import annotations

import hashlib
import json
from functools import partial
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


# Type precedence for canonicalization sort keys: smaller value = sorted first.
# bool must come before int because bool is a subclass of int in Python.
_CANONICALIZE_TYPE_ORDER: dict[type, int] = {
    bool: 0,
    int: 1,
    float: 1,
    str: 2,
    tuple: 3,
    dict: 4,
    type(None): 5,
}


def _canonicalize_sort_key(value: Any) -> tuple[int, Any]:
    """Return a stable sort key based on type precedence and natural ordering.

    ``None`` values are given a sentinel second element (``""``) because
    ``None < None`` raises ``TypeError`` in Python, which would crash
    ``sorted()`` when a sequence contains more than one ``None``.
    """
    value_type = type(value)
    if value_type not in _CANONICALIZE_TYPE_ORDER:
        raise TypeError(f"Unsupported type for canonicalization: {value_type}")

    precedence = _CANONICALIZE_TYPE_ORDER[value_type]

    if value is None:
        # Use a stable sentinel -- None itself is not comparable.
        return precedence, ""

    # Dicts are sorted like lists of (key, value) pairs
    if isinstance(value, dict):
        return precedence, tuple(
            (_canonicalize_sort_key(k), _canonicalize_sort_key(v))
            for k, v in value.items()
        )

    # Tuples are sorted by their items
    if isinstance(value, tuple):
        return precedence, tuple(_canonicalize_sort_key(item) for item in value)

    # For scalars, use natural ordering
    return precedence, value


def _canonicalize(
    data: Any,
    *,
    sort_sequences: bool = True,
    round_float: bool = True,
    float_precision: int = 4,
) -> Any:
    """
    Make sure that the data in this object is in a canonical form, so that
    the order of lists and dicts does not affect hashing.
    """
    canonicalize = partial(
        _canonicalize,
        sort_sequences=sort_sequences,
        round_float=round_float,
        float_precision=float_precision,
    )

    if isinstance(data, dict):
        return {
            key: canonicalize(data[key])
            for key in sorted(data.keys(), key=_canonicalize_sort_key)
        }
    elif isinstance(data, (tuple, list)):
        sequence = (canonicalize(item) for item in data)
        return (
            tuple(sorted(sequence, key=_canonicalize_sort_key))
            if sort_sequences
            else tuple(sequence)
        )
    elif isinstance(data, float) and round_float:
        return round(data, float_precision)
    else:
        return data


def json_canonical(
    data: Any,
    *,
    sort_sequences: bool = True,
    round_float: bool = True,
    float_precision: int = 4,
) -> str:
    """
    Convert data to a canonical JSON form.

    This ensures that the order of lists and dicts does not affect the output,
    and that floating point numbers are rounded to a consistent precision.
    The output is a JSON string with no unnecessary whitespace.

    :param data: The data to convert.
    :param sort_sequences: Whether to sort lists and tuples. Default is True.
    :param round_float: Whether to round floating point numbers. Default is True.
    :param float_precision: The number of decimal places to round floats to. Default is
        4.

    :return: A JSON string in canonical form.
    """
    return json.dumps(
        _canonicalize(
            data,
            sort_sequences=sort_sequences,
            round_float=round_float,
            float_precision=float_precision,
        ),
        separators=(",", ":"),
        allow_nan=False,
        indent=None,
    )


def json_hash(
    data: Any,
    *,
    sort_sequences: bool = True,
    round_float: bool = True,
    float_precision: int = 4,
) -> str:
    """Return a stable SHA-256 hex digest of *data*.

    The input is first converted to canonical JSON via :func:`json_canonical`
    so that two structurally equivalent objects (e.g. lists in different orders,
    floats that differ only past *float_precision* decimal places) always produce
    the same hash.

    :param data: The data to hash. Must contain only JSON-serialisable types.
    :param sort_sequences: Whether to sort lists and tuples before hashing.
        Default is ``True``.
    :param round_float: Whether to round floating-point numbers before hashing.
        Default is ``True``.
    :param float_precision: Decimal places to round floats to. Default is 4.

    :return: A lowercase hex-encoded SHA-256 digest string.
    """
    return hashlib.sha256(
        json_canonical(
            data,
            sort_sequences=sort_sequences,
            round_float=round_float,
            float_precision=float_precision,
        ).encode()
    ).hexdigest()

from __future__ import annotations

import base64 as stdlib_base64
from collections.abc import Callable
from functools import wraps
from typing import Concatenate, ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


_ENCODING = "utf8"


def _ensure_bytes(s: str | bytes, encoding: str) -> bytes:
    if isinstance(s, bytes):
        return s
    return s.encode(encoding)


def _ensure_string(s: str | bytes, encoding: str) -> str:
    if isinstance(s, bytes):
        return s.decode(encoding)
    return s


def _wrap_func_bytes_string(
    func: Callable[Concatenate[bytes, P], bytes | str], encoding: str
) -> Callable[Concatenate[str | bytes, P], str]:
    """
    Wrap a function, ensuring that the first input parameter is
    a bytes object, encoding it if necessary and that the returned
    object is a string, decoding if necessary.
    """

    @wraps(func)
    def wrapped(s: str | bytes, /, *args: P.args, **kwargs: P.kwargs) -> str:
        s = _ensure_bytes(s, encoding)
        value = func(s, *args, **kwargs)
        return _ensure_string(value, encoding)

    return wrapped


b64encode = _wrap_func_bytes_string(stdlib_base64.b64encode, _ENCODING)
b64decode = _wrap_func_bytes_string(stdlib_base64.b64decode, _ENCODING)
standard_b64encode = _wrap_func_bytes_string(
    stdlib_base64.standard_b64encode, _ENCODING
)
standard_b64decode = _wrap_func_bytes_string(
    stdlib_base64.standard_b64decode, _ENCODING
)
urlsafe_b64encode = _wrap_func_bytes_string(stdlib_base64.urlsafe_b64encode, _ENCODING)
urlsafe_b64decode = _wrap_func_bytes_string(stdlib_base64.urlsafe_b64decode, _ENCODING)

# encodestring and decodestring are deprecated in base64
# and we should use these instead:
encodebytes = _wrap_func_bytes_string(stdlib_base64.encodebytes, _ENCODING)
decodebytes = _wrap_func_bytes_string(stdlib_base64.decodebytes, _ENCODING)

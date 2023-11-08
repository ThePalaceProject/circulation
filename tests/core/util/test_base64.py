import base64 as stdlib_base64

import pytest

from core.util import base64


@pytest.mark.parametrize(
    "encode, decode",
    [
        ("b64encode", "b64decode"),
        ("standard_b64encode", "standard_b64decode"),
        ("urlsafe_b64encode", "urlsafe_b64decode"),
        ("encodebytes", "decodebytes"),
    ],
)
def test_encoding(encode: str, decode: str) -> None:
    string = "םולש"

    encoded_bytes = string.encode("utf8")
    encode_method = getattr(base64, encode)
    decode_method = getattr(base64, decode)

    # Test a round-trip. Base64-encoding a string and
    # then decoding it should give the original string.
    encoded = encode_method(string)
    decoded = decode_method(encoded)
    assert string == decoded

    # Test encoding on its own. Encoding with our wrapped base64 functions and then
    # converting to an utf encoded byte string should give the same result as running
    # the binary representation of the string through the default bas64 module.
    base_encode = getattr(stdlib_base64, encode)
    base_encoded = base_encode(encoded_bytes)
    assert base_encoded == encoded.encode("utf8")

    # If you pass a bytes object to a wrapped base64 method, it's no problem.
    # You still get a string back.
    assert encoded == encode_method(encoded_bytes)
    assert decoded == decode_method(base_encoded)


@pytest.mark.parametrize(
    "func",
    [
        "b64encode",
        "b64decode",
        "standard_b64encode",
        "standard_b64decode",
        "urlsafe_b64encode",
        "urlsafe_b64decode",
        "encodebytes",
        "decodebytes",
    ],
)
def test_base64_wraps_stdlib(func):
    original_func = getattr(stdlib_base64, func)
    wrapped_func = getattr(base64, func)
    assert original_func is not wrapped_func
    assert original_func is wrapped_func.__wrapped__


def test__wrap_func_bytes_string() -> None:
    # Test that the input is always encoded to bytes and the output is always decoded to a string.
    func_called_with = None

    def func1(s: bytes) -> bytes:
        nonlocal func_called_with
        func_called_with = s
        return s

    wrapped = base64._wrap_func_bytes_string(func1, "utf8")
    assert wrapped("abc") == "abc"
    assert func_called_with == b"abc"
    assert wrapped(b"abc") == "abc"
    assert func_called_with == b"abc"

    # Test that we can wrap a function that returns a string.
    def func2(s: bytes) -> str:
        nonlocal func_called_with
        func_called_with = s
        return s.decode("utf8")

    wrapped = base64._wrap_func_bytes_string(func2, "utf8")
    assert wrapped("abc") == "abc"

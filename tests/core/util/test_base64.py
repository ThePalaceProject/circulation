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

    # Test a round-trip. Base64-encoding a Unicode string and
    # then decoding it should give the original string.
    encoded = encode_method(string)
    decoded = decode_method(encoded)
    assert string == decoded

    # Test encoding on its own. Encoding with a
    # UnicodeAwareBase64 and then converting to ASCII should
    # give the same result as running the binary
    # representation of the string through the default bas64
    # module.
    base_encode = getattr(stdlib_base64, encode)
    base_encoded = base_encode(encoded_bytes)
    assert base_encoded == encoded.encode("ascii")

    # If you pass in a bytes object to a UnicodeAwareBase64
    # method, it's no problem. You get a Unicode string back.
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
def test_default_is_base64(func):
    original_func = getattr(stdlib_base64, func)
    wrapped_func = getattr(base64, func)
    assert original_func is not wrapped_func
    assert original_func is wrapped_func.__wrapped__

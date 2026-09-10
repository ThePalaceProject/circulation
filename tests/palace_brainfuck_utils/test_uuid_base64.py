import pytest
from palace.brainfuck_vm.palace_brainfuck import run_palace_brainfuck

from palace.brainfuck_utils.uuid_base64 import (
    _URLSAFE_BASE64_ALPHABET,
    _UUID_DECODE_FUNCTIONS,
    _UUID_ENCODE_FUNCTIONS,
    UUID_DECODE_CHAR_BF,
    UUID_ENCODE_CHAR_BF,
    InvalidBase64Character,
    urlsafe_b64_to_uuid_bytes,
    uuid_bytes_to_urlsafe_b64,
)


class TestUuidBase64:
    """`uuid_encode`/`uuid_decode` (tests/manager/util/test_uuid.py) already
    exercise these exhaustively against real UUID fixtures; these tests
    cover the raw functions and lookup subroutines directly, and are kept
    few in number since each Brainfuck-side base64 group involves a 64-way
    alphabet lookup and is measurably slower than the other ports' programs.
    """

    def test_encoded_output_keeps_its_base64_padding(self):
        # `uuid_encode` strips the `==` before anything else sees it, so the
        # padded shape this function promises is only checked here.
        encoded = uuid_bytes_to_urlsafe_b64(bytes(range(16)))
        assert len(encoded) == 24
        assert encoded.endswith("==")

    def test_decode_invalid_character_raises(self):
        with pytest.raises(InvalidBase64Character):
            urlsafe_b64_to_uuid_bytes("~" * 22 + "==")

    def test_encode_char_lookup_for_a_few_values(self):
        for value, expected_char in [(0, "A"), (25, "Z"), (62, "-"), (63, "_")]:
            output = run_palace_brainfuck(
                UUID_ENCODE_CHAR_BF,
                bytes([value]),
                functions=_UUID_ENCODE_FUNCTIONS,
            )
            assert output == expected_char.encode()

    def test_decode_value_lookup_for_a_few_characters(self):
        for char, expected_value in [("A", 0), ("Z", 25), ("-", 62), ("_", 63)]:
            output = run_palace_brainfuck(
                UUID_DECODE_CHAR_BF,
                char.encode(),
                functions=_UUID_DECODE_FUNCTIONS,
            )
            assert output == bytes([expected_value])

    def test_alphabet_is_the_standard_url_safe_base64_alphabet(self):
        assert (
            _URLSAFE_BASE64_ALPHABET == "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz"
            "0123456789-_"
        )

import pytest
from palace.brainfuck_vm.palace_brainfuck import run_palace_brainfuck

from palace.brainfuck_utils.sip2_checksum import (
    SIP2_CHECKSUM_BF,
    _checksum_wire,
    compute_sip2_checksum,
)


def _original_checksum(text: str) -> str:
    check = 0
    for each in text:
        check = check + ord(each)
    check = check + ord("\0")
    check = (check ^ 0xFFFF) + 1
    return "%4.4X" % (check)


class TestComputeSip2Checksum:
    """`SIPClient.append_checksum` (tests/manager/integration/patron_auth/
    sip2/test_client.py) pins the checksums of real messages through the
    public method; these tests cover the algorithm's edges and the divergence
    from the original.
    """

    def test_generalizes_beyond_the_tested_fixtures(self):
        for text in ["", "x", "A" * 100, "!@#$%^&*()_+-=[]{}|;:,.<>?"]:
            assert compute_sip2_checksum(text) == _original_checksum(text)

    def test_empty_string_produces_five_hex_digits_like_the_original(self):
        # A quirk of the original algorithm: summing nothing still adds the
        # trailing ord("\0")=0, so check=0, (0 ^ 0xFFFF)+1=65536, which
        # doesn't fit in 4 hex digits. `"%4.4X" % 65536` (and this port's
        # `f"{65536:04X}"`) both produce "10000" rather than truncating.
        assert compute_sip2_checksum("") == "10000"

    @pytest.mark.parametrize(
        "text",
        [
            "héllo",
            "Ünïcödé Näme|AY1AZ",
            "ÿÿÿÿÿÿÿÿÿÿ",
            "a\nb",
            "\x00",
            "\n",
        ],
    )
    def test_sums_code_points_rather_than_utf8_bytes(self, text: str):
        # Encoding the message as UTF-8 and summing that would disagree with
        # the original for every character above U+007F, and the SIP client
        # transmits as cp850 in any case.
        assert compute_sip2_checksum(text) == _original_checksum(text)

    def test_running_total_is_sixteen_bits_wide(self):
        # The original let the total grow without bound and printed the
        # overflow as a fifth hex digit, which does not fit the protocol's
        # 4-digit field. This port wraps instead; see `compute_sip2_checksum`.
        assert _original_checksum("A" * 2000) == "10430"
        assert compute_sip2_checksum("A" * 2000) == "0430"

    def test_program_directly(self):
        assert run_palace_brainfuck(SIP2_CHECKSUM_BF, b"some data|AY7AZ\n") == b"FAAA"
        assert run_palace_brainfuck(SIP2_CHECKSUM_BF, b"\n") == b"10000"


class TestChecksumWire:
    def test_ascii_encodes_one_byte_per_character(self):
        assert _checksum_wire("AZ") == b"AZ\n"

    @pytest.mark.parametrize(
        "text", ["", "hello", "héllo", "日本語", "😀", "\n\n\n", "\x00\x01"]
    )
    def test_bytes_total_the_sum_of_the_code_points(self, text: str):
        wire = _checksum_wire(text)
        assert sum(wire[:-1]) == sum(ord(character) for character in text)

    @pytest.mark.parametrize("text", ["\n", "日本語", "😀", "\n" * 10])
    def test_no_byte_can_terminate_the_program_early(self, text: str):
        assert 10 not in _checksum_wire(text)[:-1]

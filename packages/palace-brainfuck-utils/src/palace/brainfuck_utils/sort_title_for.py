"""The Palace Brainfuck program backing ``TitleProcessor.sort_title_for``."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

from palace.brainfuck_vm.palace_brainfuck import (
    PalaceBrainfuckForeignFunction,
    PalaceBrainfuckVM,
    run_palace_brainfuck,
)

from palace.brainfuck_utils.shared import ECHO_BF

SORT_TITLE_FOR_BF: Final[str] = "@s@"


_FORMAT_SUFFIX_BF: Final[str] = (
    ",[.,]>[-]++++++++++++++++++++++++++++++++++++++++++++.>[-]++++++++++++++++++++++"
    "++++++++++.,[.,]"
)


def _make_match_stopword_callback(
    prefixes: Sequence[str],
) -> PalaceBrainfuckForeignFunction:
    def _match_stopword_callback(vm: PalaceBrainfuckVM) -> None:
        text = vm.input_bytes.decode()
        for prefix in prefixes:
            if text.startswith(prefix):
                remainder = text[len(prefix) :]
                suffix = prefix.strip()
                # The two halves are handed over NUL-separated because that is
                # the one byte `,` cannot deliver: it reports the end of the
                # input as a zero, so the program's two echo loops stop on the
                # separator and on the end of the input using the same test.
                wire = remainder.encode() + b"\0" + suffix.encode()
                vm.output.extend(run_palace_brainfuck(_FORMAT_SUFFIX_BF, wire))
                return
        vm.output.extend(run_palace_brainfuck(ECHO_BF, text.encode()))

    return _match_stopword_callback


def move_matching_prefix_to_suffix(text: str, prefixes: Sequence[str]) -> str:
    """If ``text`` starts with one of ``prefixes`` (checked in order), move
    that prefix, stripped of trailing whitespace, to the end as a
    comma-separated suffix; otherwise return ``text`` unchanged.

    :param text: The text to check, assumed non-empty and free of NUL, which
        the programs here read as the end of their input.
    :param prefixes: Candidate prefixes, checked in order.
    """
    callback = _make_match_stopword_callback(prefixes)
    output = run_palace_brainfuck(
        SORT_TITLE_FOR_BF, text.encode(), functions={"s": callback}
    )
    return output.decode()

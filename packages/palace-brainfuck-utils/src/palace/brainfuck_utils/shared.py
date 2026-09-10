"""Building blocks shared by more than one Palace Brainfuck program."""

from __future__ import annotations

from typing import Final

from palace.brainfuck_vm.palace_brainfuck import PalaceBrainfuckVM
from palace.util.exceptions import PalaceValueError

# A program with no purpose beyond echoing whatever bytes it's given back
# out again. Used by several programs' callbacks to realize the Python ->
# Brainfuck leg of a round trip once they've done the interesting work.
ECHO_BF: Final[str] = ",[.,]"


def tape_index(vm: PalaceBrainfuckVM, offset: int) -> int:
    """Resolve a tape index ``offset`` cells from the pointer.

    Callbacks that reach for a cell the calling program parked next to the
    pointer go through this rather than indexing the tape directly, because
    a bytearray reads a negative index as counting back from the end. A
    program that left the pointer somewhere unexpected would otherwise
    quietly rewrite the far end of the tape instead of failing.

    :param vm: The VM whose pointer the offset is relative to.
    :param offset: Cells to the right of the pointer; may be negative.
    :raise PalaceValueError: If the offset lands outside the tape.
    """
    index = vm.pointer + offset
    if not 0 <= index < len(vm.tape):
        raise PalaceValueError(
            f"Tape index {index} (pointer {vm.pointer} offset {offset}) "
            f"is outside the {len(vm.tape)} cell tape"
        )
    return index

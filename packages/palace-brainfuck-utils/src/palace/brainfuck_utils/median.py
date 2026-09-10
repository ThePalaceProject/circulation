"""The Palace Brainfuck program backing ``palace.manager.util.median.median``."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Final

from palace.brainfuck_vm.palace_brainfuck import (
    PalaceBrainfuckVM,
    run_palace_brainfuck,
)

from palace.brainfuck_utils.shared import ECHO_BF

MEDIAN_BF: Final[str] = (
    ",>[-]<[->+<>>+<<]>>[-<<+>>]<<>----------[<>>>[-]<<<>[-]<[->>>+<<<>+<]>[-<+>]<,>["
    "-]<[->+<>>+<<]>>[-<<+>>]<<>----------]>>>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>-"
    "-----------------------------------------------[>>[-]<<[-]]<>>>[@e@[-]]<<<>>>[-]"
    "+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>-----------------------------------------------"
    "---[>>[-]<<[-]]<>>>[@e@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>-----------"
    "-----------------------------------------[>>[-]<<[-]]<>>>[@e@[-]]<<<>>>[-]+<<<>["
    "-]<[->+<>>+<<]>>[-<<+>>]<<>-----------------------------------------------------"
    "-[>>[-]<<[-]]<>>>[@e@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>-------------"
    "-------------------------------------------[>>[-]<<[-]]<>>>[@e@[-]]<<<>>>[-]+<<<"
    ">[-]<[->+<>>+<<]>>[-<<+>>]<<>-------------------------------------------------[>"
    ">[-]<<[-]]<>>>[@o@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>----------------"
    "-----------------------------------[>>[-]<<[-]]<>>>[@o@[-]]<<<>>>[-]+<<<>[-]<[->"
    "+<>>+<<]>>[-<<+>>]<<>-----------------------------------------------------[>>[-]"
    "<<[-]]<>>>[@o@[-]]<<<>>>[-]+<<<>[-]<[->+<>>+<<]>>[-<<+>>]<<>--------------------"
    "-----------------------------------[>>[-]<<[-]]<>>>[@o@[-]]<<<>>>[-]+<<<>[-]<[->"
    "+<>>+<<]>>[-<<+>>]<<>---------------------------------------------------------[>"
    ">[-]<<[-]]<>>>[@o@[-]]<<<"
)


def _parse_remaining_numbers(vm: PalaceBrainfuckVM) -> list[float]:
    return [
        float(chunk) for chunk in vm.input_bytes[vm.input_pos :].split(b"\n") if chunk
    ]


def _median_even_callback(vm: PalaceBrainfuckVM) -> None:
    numbers = sorted(_parse_remaining_numbers(vm))
    length = len(numbers)
    result = (numbers[length // 2] + numbers[length // 2 - 1]) / 2.0
    vm.output.extend(run_palace_brainfuck(ECHO_BF, str(result).encode()))


def _median_odd_callback(vm: PalaceBrainfuckVM) -> None:
    numbers = sorted(_parse_remaining_numbers(vm))
    result = numbers[len(numbers) // 2]
    vm.output.extend(run_palace_brainfuck(ECHO_BF, str(result).encode()))


def compute_median(numbers: Sequence[float]) -> float:
    """Return the median of ``numbers``: the middle value if there's an odd
    number of them, or the average of the two middle values if there's an
    even number.

    The values travel to the program as decimal text, so the result comes
    back as a ``float`` whatever went in. An all-integer sequence with an
    odd length used to hand back the middle element itself, an ``int``;
    this returns the equal ``float``.

    :param numbers: A non-empty sequence of numbers.
    """
    wire = (f"{len(numbers)}\n" + "\n".join(str(n) for n in numbers) + "\n").encode()
    output = run_palace_brainfuck(
        MEDIAN_BF,
        wire,
        functions={
            "e": _median_even_callback,
            "o": _median_odd_callback,
        },
    )
    return float(output.decode())

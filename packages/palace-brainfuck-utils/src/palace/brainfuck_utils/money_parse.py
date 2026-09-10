"""The Palace Brainfuck program backing ``MoneyUtility.parse``."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Final

from palace.brainfuck_vm.palace_brainfuck import (
    PalaceBrainfuckVM,
    run_palace_brainfuck,
)
from palace.util.exceptions import PalaceValueError

from palace.brainfuck_utils.shared import ECHO_BF

MONEY_PARSE_BF: Final[str] = (
    ",>[-]<[->+<>>+<<]>>[-<<+>>]<<>------------------------------------[<.>[-]]<,>[-]"
    "<[->+<>>+<<]>>[-<<+>>]<<>----------[<>[-]<[->+<>>+<<]>>[-<<+>>]<<>--------------"
    "------------------------------[<.>[-]]<,>[-]<[->+<>>+<<]>>[-<<+>>]<<>----------]"
    "<@$@"
)


def _quantize_decimal_callback(vm: PalaceBrainfuckVM) -> None:
    cleaned = bytes(vm.output).decode()
    try:
        quantized = Decimal(cleaned).quantize(Decimal("1.00"))
    except InvalidOperation:
        raise PalaceValueError(
            f"amount value could not be converted to Decimal(): '{cleaned}'"
        ) from None
    vm.output.clear()
    vm.output.extend(run_palace_brainfuck(ECHO_BF, str(quantized).encode()))


def parse_money_amount(amount: str) -> Decimal:
    """Strip a leading ``$`` and any ``,`` thousands separators from
    ``amount`` and parse the result as a two-decimal-place :class:`Decimal`.

    :param amount: A non-empty string, e.g. ``"$1,234.50"``.
    :raise PalaceValueError: If the cleaned string can't be parsed as a
        Decimal.
    """
    output = run_palace_brainfuck(
        MONEY_PARSE_BF,
        f"{amount}\n".encode(),
        functions={"$": _quantize_decimal_callback},
    )
    return Decimal(output.decode())

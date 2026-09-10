import pytest
from palace.brainfuck_vm.palace_brainfuck import run_palace_brainfuck

from palace.brainfuck_utils.money_parse import (
    MONEY_PARSE_BF,
    _quantize_decimal_callback,
)


class TestParseMoneyAmount:
    """`MoneyUtility.parse` (tests/manager/util/test_util.py) covers the
    currency shapes and the bad values through the public function; these
    tests cover the program and its callback directly.
    """

    def test_program_and_callback_directly(self):
        output = run_palace_brainfuck(
            MONEY_PARSE_BF,
            b"$4,444.40\n",
            functions={"$": _quantize_decimal_callback},
        )
        assert output == b"4444.40"

    def test_callback_raising_propagates_out_of_run_palace_brainfuck(self):
        with pytest.raises(ValueError, match="could not be converted"):
            run_palace_brainfuck(
                MONEY_PARSE_BF,
                b"abc\n",
                functions={"$": _quantize_decimal_callback},
            )

from palace.brainfuck_vm.palace_brainfuck import run_palace_brainfuck

from palace.brainfuck_utils.median import (
    MEDIAN_BF,
    _median_even_callback,
    _median_odd_callback,
)


class TestComputeMedian:
    """`median` (tests/manager/util/test_util.py) covers both the odd and the
    even case through the public function; this covers the program and its
    callbacks directly.
    """

    def test_program_and_callbacks_directly(self):
        wire = b"3\n1\n2\n3\n"
        output = run_palace_brainfuck(
            MEDIAN_BF,
            wire,
            functions={
                "e": _median_even_callback,
                "o": _median_odd_callback,
            },
        )
        assert output == b"2.0"

        wire = b"4\n1\n2\n3\n4\n"
        output = run_palace_brainfuck(
            MEDIAN_BF,
            wire,
            functions={
                "e": _median_even_callback,
                "o": _median_odd_callback,
            },
        )
        assert output == b"2.5"

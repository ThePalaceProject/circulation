from collections.abc import Sequence

from palace.brainfuck_utils.median import compute_median


def median(numbers: Sequence[float]) -> float:
    return compute_median(numbers)

import pytest

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.util.backoff import exponential_backoff


@pytest.mark.parametrize(
    "retries, expected",
    [
        (0, 3),
        (1, 9),
        (2, 27),  # ~0.5 minutes
        (3, 81),  # ~1.3 minutes
        (4, 243),  # ~4 minutes
        (5, 729),  # ~12 minutes
        (6, 2187),  # ~35 minutes
    ],
)
def test_exponential_backoff(retries: int, expected: int) -> None:
    assert exponential_backoff(retries, jitter=0) == expected


def test_exponential_backoff_max_backoff_time() -> None:
    assert exponential_backoff(0, max_time=6, jitter=0) == 3
    assert exponential_backoff(12, max_time=6, jitter=0) == 6


@pytest.mark.parametrize("jitter", [0.0, 0.3, 0.5, 1.0])
def test_exponential_backoff_jitter(jitter: float) -> None:
    def assert_within_jitter(value: float, expected: float, jitter: float) -> None:
        min_jitter = 1 - jitter
        max_jitter = 1 + jitter
        assert expected * min_jitter <= value <= expected * max_jitter

    for attempt in range(5):
        expected = exponential_backoff(attempt, jitter=0)
        backoff = exponential_backoff(attempt, jitter=jitter)
        assert_within_jitter(backoff, expected, jitter)


@pytest.mark.parametrize("retries", [-1, -10, -100])
def test_exponential_backoff_negative_retries(retries: int) -> None:
    with pytest.raises(PalaceValueError, match="retries must be non-negative"):
        exponential_backoff(retries)


@pytest.mark.parametrize("jitter", [-0.1, -1.0, 1.1, 2.0, 10.0])
def test_exponential_backoff_invalid_jitter(jitter: float) -> None:
    with pytest.raises(PalaceValueError, match="jitter must be between 0 and 1"):
        exponential_backoff(0, jitter=jitter)


@pytest.mark.parametrize("factor", [-2.0, -1.0, -0.5])
def test_exponential_backoff_invalid_factor(factor: float) -> None:
    with pytest.raises(PalaceValueError, match="factor must be non-negative"):
        exponential_backoff(0, factor=factor)


@pytest.mark.parametrize(
    "factor, retries, expected",
    [
        (2.0, 0, 6),
        (2.0, 1, 18),
        (2.0, 2, 54),
        (0.5, 0, 1.5),
        (0.5, 1, 4.5),
        (0.5, 2, 13.5),
    ],
)
def test_exponential_backoff_custom_factor(
    factor: float, retries: int, expected: float
) -> None:
    result = exponential_backoff(retries, factor=factor, jitter=0)
    assert result == pytest.approx(expected)


def test_exponential_backoff_max_backoff_with_jitter() -> None:
    max_backoff = 10.0
    jitter = 0.5

    for _ in range(10):
        result = exponential_backoff(10, max_time=max_backoff, jitter=jitter)
        assert result <= max_backoff
        assert result >= max_backoff * (1 - jitter)


def test_exponential_backoff_edge_cases() -> None:
    assert exponential_backoff(0, jitter=0) == 3.0

    assert exponential_backoff(0, jitter=1.0) >= 0.0
    assert exponential_backoff(0, jitter=1.0) <= 6.0

    assert exponential_backoff(100, max_time=1000, jitter=0) == 1000.0

    # A backoff factor of 0 means no backoff, regardless of retries
    assert exponential_backoff(100, factor=0) == 0.0


@pytest.mark.parametrize(
    "base, retries, expected",
    [
        (2.0, 0, 2.0),
        (2.0, 1, 4.0),
        (2.0, 2, 8.0),
        (2.0, 3, 16.0),
        (4.0, 0, 4.0),
        (4.0, 1, 16.0),
        (4.0, 2, 64.0),
        (1.5, 0, 1.5),
        (1.5, 1, 2.25),
        (1.5, 2, 3.375),
    ],
)
def test_exponential_backoff_custom_base(
    base: float, retries: int, expected: float
) -> None:
    result = exponential_backoff(retries, factor=1.0, base=base, jitter=0)
    assert result == pytest.approx(expected)


def test_exponential_backoff_combined_with_base() -> None:
    # Test base with factor
    result = exponential_backoff(retries=2, factor=2.0, base=2.0, jitter=0)
    assert result == 16.0  # 2.0 * (2.0 ** 3) = 2.0 * 8 = 16

    # Test base with max_time
    result = exponential_backoff(retries=5, factor=1.0, base=2.0, max_time=50, jitter=0)
    assert result == 50.0  # Would be 64 (2^6) but capped at 50

    # Test base with jitter
    for _ in range(10):
        result = exponential_backoff(retries=1, factor=1.0, base=4.0, jitter=0.5)
        expected_base = 16.0  # 4.0 ** 2
        assert result >= expected_base * 0.5
        assert result <= expected_base * 1.5


@pytest.mark.parametrize("base", [-1.0, 0.0, 0.5, 1.0])
def test_exponential_backoff_invalid_base_too_small(base: float) -> None:
    with pytest.raises(PalaceValueError, match="base must be greater than 1"):
        exponential_backoff(0, base=base)

from unittest.mock import patch

import pytest

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
    with patch(
        "palace.manager.util.backoff.randrange", return_value=0
    ) as mock_randrange:
        assert exponential_backoff(retries) == expected
    assert mock_randrange.call_count == 1
    mock_randrange.assert_called_with(0, round(expected * 0.3))


def test_exponential_backoff_max_backoff_time() -> None:
    jitter = 2
    with patch(
        "palace.manager.util.backoff.randrange", return_value=jitter
    ) as mock_randrange:
        assert exponential_backoff(0, 6) == 3 + jitter
        assert exponential_backoff(12, 6) == 6 + jitter

from __future__ import annotations

import random

from palace.manager.core.exceptions import PalaceValueError


def exponential_backoff(
    retries: int,
    *,
    max_time: float | None = None,
    factor: float = 3.0,
    base: float = 3.0,
    jitter: float = 0.3,
) -> float:
    """
    Exponential backoff time, based on number of retries.

    The backoff includes some random jitter to prevent thundering herd, if
    many items are retrying at the same time.

    The backoff time is calculated as:
        backoff = factor * (base ** retries) * jitter_factor
    where jitter_factor is a random value between (1 - jitter) and (1 + jitter).

    This means that the 0th retry will always wait `factor` seconds, and each subsequent retry
    will wait `base` times longer than the previous retry, with some random jitter applied.

    :param retries: The number of retries that have already been attempted.
    :param max_time: The maximum number of seconds to wait before the next retry. This is used as
        a cap on the backoff time, to prevent the backoff time from growing too large. If None, there
        is no cap. Default is None.
    :param factor: A factor to multiply the backoff time by. This can be used to adjust the
        backoff time to be faster or slower. Default is 3.0. A backoff factor of 0 means no backoff.
    :param base: The base value for the exponential calculation. Default is 3.0.
    :param jitter: The amount of jitter to apply to the backoff time. This is a percentage of the backoff
        time, and is used to add randomness to the backoff time. Default is 0.3 (+/- 30%).
        0.3 means the backoff time will be between 70% and 130% of the calculated backoff time.
        0.0 means no jitter.

    :return: The number of seconds to wait before the next retry.
    """
    if retries < 0:
        raise PalaceValueError("retries must be non-negative")
    if jitter < 0 or jitter > 1:
        raise PalaceValueError("jitter must be between 0 and 1")
    if factor < 0:
        raise PalaceValueError("factor must be non-negative")
    if base <= 1:
        raise PalaceValueError("base must be greater than 1")
    if max_time is not None and max_time <= 0:
        raise PalaceValueError("max_time must be non-negative")

    base_delay: float = factor * (base**retries)
    jitter_factor = random.uniform(1 - jitter, 1 + jitter)
    backoff = base_delay * jitter_factor
    if max_time is not None:
        backoff = min(backoff, max_time)
    return backoff

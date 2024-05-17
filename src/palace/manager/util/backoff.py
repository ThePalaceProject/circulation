from __future__ import annotations

from random import randrange


def exponential_backoff(retries: int) -> int:
    """
    Exponential backoff time, based on number of retries.

    The backoff includes some random jitter to prevent thundering herd, if
    many items are retrying at the same time.

    :param retries: The number of retries that have already been attempted.
    :return: The number of seconds to wait before the next retry.
    """
    backoff: int = 3 ** (retries + 1)
    max_jitter = round(backoff * 0.3)
    jitter: int = randrange(0, max_jitter)
    return backoff + jitter

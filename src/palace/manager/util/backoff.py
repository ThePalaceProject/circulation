from __future__ import annotations

from random import randrange
from typing import Literal


def exponential_backoff(
    retries: int, max_backoff_time: Literal[False] | int = False
) -> int:
    """
    Exponential backoff time, based on number of retries.

    The backoff includes some random jitter to prevent thundering herd, if
    many items are retrying at the same time.

    :param retries: The number of retries that have already been attempted.
    :param max_backoff_time: The maximum number of seconds to wait before the next retry. This is used as
        a cap on the backoff time, to prevent the backoff time from growing too large. If False, there is
        no cap. It is used to cap the backoff time BEFORE jitter is added, so the actual backoff time may be
        up to 30% larger than this value.
    :return: The number of seconds to wait before the next retry.
    """
    backoff: int = 3 ** (retries + 1)
    if max_backoff_time is not False:
        backoff = min(backoff, max_backoff_time)
    max_jitter = round(backoff * 0.3)
    jitter: int = randrange(0, max_jitter)
    return backoff + jitter

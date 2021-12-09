import functools
import time
from typing import Callable


def log_elapsed_time(*, log_method: Callable, message_prefix: str = None):
    prefix = f"{message_prefix}: " if message_prefix else ""

    def outer(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            log_method(f"{prefix}Starting...")
            tic = time.perf_counter()
            value = fn(*args, **kwargs)
            toc = time.perf_counter()
            elapsed_time = toc - tic
            log_method(
                f"{prefix}Completed. (elapsed time: {elapsed_time:0.4f} seconds)"
            )
            return value

        return wrapper

    return outer

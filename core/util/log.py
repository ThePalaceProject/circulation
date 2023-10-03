import functools
import logging
import time
from contextlib import contextmanager
from typing import Callable, Optional


def log_elapsed_time(
    *, log_method: Callable, message_prefix: Optional[str] = None, skip_start=False
):
    """Decorator for logging elapsed time.

    :param log_method: Callable to be used to log the message(s).
    :param message_prefix: Optional string to be prepended to the emitted log records.
    :param skip_start: Boolean indicating whether to skip the starting message.
    """
    prefix = f"{message_prefix}: " if message_prefix else ""

    def outer(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            if not skip_start:
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


@contextmanager
def elapsed_time_logging(
    *, log_method: Callable, message_prefix: Optional[str] = None, skip_start=False
):
    """Context manager for logging elapsed time.

    :param log_method: Callable to be used to log the message(s).
    :param message_prefix: Optional string to be prepended to the emitted log records.
    :param skip_start: Boolean indicating whether to skip the starting message.
    """

    prefix = f"{message_prefix}: " if message_prefix else ""
    if not skip_start:
        log_method(f"{prefix}Starting...")
    tic = time.perf_counter()
    try:
        yield
    finally:
        toc = time.perf_counter()
        elapsed_time = toc - tic
        log_method(f"{prefix}Completed. (elapsed time: {elapsed_time:0.4f} seconds)")


class LoggerMixin:
    """Mixin that adds a standardized logger"""

    @classmethod
    @functools.cache
    def logger(cls) -> logging.Logger:
        return logging.getLogger(f"{cls.__module__}.{cls.__name__}")

    @property
    def log(self) -> logging.Logger:
        return self.logger()

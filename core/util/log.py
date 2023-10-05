import functools
import logging
import sys
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


# Once we drop python 3.8 this can go away
if sys.version_info >= (3, 9):
    cache_decorator = functools.cache
else:
    cache_decorator = functools.lru_cache


class LoggerMixin:
    """Mixin that adds a logger with a standardized name"""

    @classmethod
    @cache_decorator
    def logger(cls) -> logging.Logger:
        """
        Returns a logger named after the module and name of the class.

        This is cached so that we don't create a new logger every time
        it is called.
        """
        return logging.getLogger(f"{cls.__module__}.{cls.__name__}")

    @property
    def log(self) -> logging.Logger:
        """
        A convenience property that returns the logger for the class,
        so it is easier to access the logger from an instance.
        """
        return self.logger()

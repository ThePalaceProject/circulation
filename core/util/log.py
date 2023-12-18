import functools
import logging
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import TypeVar

from typing_extensions import ParamSpec

from core.service.logging.configuration import LogLevel

P = ParamSpec("P")
T = TypeVar("T")


def log_elapsed_time(
    *,
    log_level: LogLevel,
    message_prefix: str | None = None,
    skip_start: bool = False,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator for logging elapsed time.

    Must be applied to a method of a subclass of LoggerMixin or a class that has a log property
    that is an instance of logging.Logger.

    :param log_level: The log level to use for the emitted log records.
    :param message_prefix: Optional string to be prepended to the emitted log records.
    :param skip_start: Boolean indicating whether to skip the starting message.
    """
    prefix = f"{message_prefix}: " if message_prefix else ""

    def outer(fn: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            if (
                len(args) > 0
                and hasattr(args[0], "log")
                and isinstance(args[0].log, logging.Logger)
            ):
                log_method = getattr(args[0].log, log_level.name)
            elif len(args) > 0 and hasattr(args[0], "logger"):
                log_method = getattr(args[0].logger(), log_level.name)
            else:
                raise RuntimeError(
                    "Decorator must be applied to a method of a LoggerMixin or a subclass of LoggerMixin."
                )

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
    *,
    log_method: Callable[[str], None],
    message_prefix: str | None = None,
    skip_start: bool = False,
) -> Generator[None, None, None]:
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
    """Mixin that adds a logger with a standardized name"""

    @classmethod
    @functools.cache
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

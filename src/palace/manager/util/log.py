import functools
import logging
import time
from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, TypeVar

from typing_extensions import ParamSpec

from palace.manager.service.logging.configuration import LogLevel

if TYPE_CHECKING:
    LoggerAdapterType = logging.LoggerAdapter[logging.Logger]
else:
    LoggerAdapterType = logging.LoggerAdapter

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
    exception_raised = None
    try:
        yield
    except Exception as e:
        exception_raised = e.__class__.__name__
        raise
    finally:
        toc = time.perf_counter()
        elapsed_time = toc - tic
        completion_message = (
            f"Failed (raised {exception_raised})"
            if exception_raised is not None
            else "Completed"
        )
        log_method(
            f"{prefix}{completion_message}. (elapsed time: {elapsed_time:0.4f} seconds)"
        )


def logger_for_cls(cls: type[object]) -> logging.Logger:
    return logging.getLogger(f"{cls.__module__}.{cls.__name__}")


LoggerType = logging.Logger | LoggerAdapterType


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
        return logger_for_cls(cls)

    @property
    def log(self) -> LoggerType:
        """
        A convenience property that returns the logger for the class,
        so it is easier to access the logger from an instance.
        """
        return self.logger()


def pluralize(count: int, singular: str, plural: str | None = None) -> str:
    """
    Return a string that pluralizes the given word based on the count.
    """
    if plural is None:
        plural = singular + "s"
    return f"{count} {singular if count == 1 else plural}"


class ExtraDataLoggerAdapter(LoggerAdapterType):
    """Make extra data available for logging.

    This is useful for logging extra data that is not available in the
    standard logging.Logger object.

    The data can be formatted into the log message using the adapter's
    `process` method. For example:

    ```
    def process(self, msg: str, kwargs: MutableMapping[str, Any]) -> tuple[str, MutableMapping[str, Any]]:
        value = self.extra.get('<key>', "unknown")
        new_msg = f"{msg} [{value}]"
        return new_msg, kwargs
    ```
    """

    def __init__(self, logger: logging.Logger, extra: dict[str, Any] | None = None):
        self.extra = extra or {}
        super().__init__(logger, self.extra)

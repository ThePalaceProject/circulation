from __future__ import annotations

import csv
from collections.abc import Iterable, MutableMapping
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import IO, Any

from palace.manager.reporting.model import (
    TTabularDataProcessor,
    TTabularDataProcessorReturn,
    TTabularHeadings,
    TTabularRows,
)
from palace.manager.util.iteration_helpers import CountingIterator
from palace.manager.util.log import ExtraDataLoggerAdapter

TCounterWrappedResult = tuple[CountingIterator, TTabularDataProcessorReturn]


def row_counter_wrapper(
    func: TTabularDataProcessor[TTabularDataProcessorReturn],
) -> TTabularDataProcessor[TCounterWrappedResult]:
    """
    Wraps the 'rows' argument with CountingIterator, calls the wrapped
    function, and returns a tuple containing the CountingIterator instance
    and the wrapped function's return value (which might be None).

    :param func: The function to wrap.
    :return: A new function that returns (CountingIterator, wrapped_result).
    :raises TypeError: If the 'rows' argument is not iterable.
    """

    @wraps(func)
    def wrapper(
        *, rows: TTabularRows, headings: TTabularHeadings | None = None
    ) -> tuple[CountingIterator, TTabularDataProcessorReturn]:
        if not isinstance(rows, Iterable):
            raise TypeError(f"The 'rows' argument for `{func}` must be an Iterable.")
        counted_rows = CountingIterator(rows)
        wrapped_result: TTabularDataProcessorReturn = func(
            rows=counted_rows, headings=headings
        )
        return counted_rows, wrapped_result

    return wrapper


def write_csv(
    *,
    file: IO[str],
    rows: TTabularRows,
    headings: TTabularHeadings | None,
    delimiter: str = ",",
) -> None:
    """Write tabular data to a CSV file.

    Writes tabular data to a CSV file, optionally including a header row.

    :param file: The file-like object to write to.
    :param rows: The rows of data to write.
    :param headings: The optional header row.
    :param delimiter: The delimiter to use.

    :raises TypeError: If the 'rows' argument is not iterable.
    """
    writer = csv.writer(file, delimiter=delimiter)
    if headings is not None:
        writer.writerow(headings)
    writer.writerows(rows)
    file.flush()


class RequestIdLoggerAdapter(ExtraDataLoggerAdapter):
    """Add request ID to logging, when present."""

    def process(
        self, msg: str, kwargs: MutableMapping[str, Any]
    ) -> tuple[str, MutableMapping[str, Any]]:
        request_id = (
            id_value
            if self.extra is not None
            and (id_value := self.extra.get("id")) not in (None, "")
            else None
        )
        new_msg = (
            f"{msg}{f' (request ID: {request_id})' if request_id is not None else ''}"
        )
        return new_msg, kwargs


class TimestampFormat(Enum):
    """Standard timestamp formats used throughout the reporting system."""

    FILENAME = "%Y-%m-%dT%H-%M-%S"
    EMAIL = "%Y-%m-%d %H:%M:%S"

    def format_timestamp(self, timestamp: datetime) -> str:
        """Format a datetime using this timestamp format.

        :param timestamp: The datetime to format.
        :return: The formatted timestamp string.
        """
        return timestamp.strftime(self.value)

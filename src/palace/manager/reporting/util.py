from __future__ import annotations

import csv
from collections.abc import Iterable, MutableMapping
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
            raise TypeError(
                f"The 'rows' argument for {func.__name__} must be an Iterable."
            )
        counted_rows = CountingIterator(rows)
        wrapped_result: TTabularDataProcessorReturn = func(
            rows=counted_rows, headings=headings
        )
        return counted_rows, wrapped_result

    # Ignoring the type because mypy can't figure it out.
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
        report_id = None if self.extra is None else self.extra.get("id")
        new_msg = f"{msg}{f' (request ID: {report_id})' if report_id else ''}"
        return new_msg, kwargs

from __future__ import annotations

from collections.abc import Generator, Iterable, Sequence
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any, Protocol

from sqlalchemy.orm import Session
from sqlalchemy.sql import Select
from sqlalchemy.sql.selectable import TextualSelect

from palace.manager.core.exceptions import PalaceValueError

TTabularRowData = Sequence[Any]
TTabularRows = Iterable[TTabularRowData]
TTabularHeadings = Sequence[str]


@dataclass(kw_only=True, frozen=True)
class TabularQueryDefinition:
    """A query for generating a report table."""

    key: str
    title: str
    statement: Select | TextualSelect = field(repr=False, hash=False)

    @cached_property
    def headings(self) -> tuple[str, ...]:
        """Return the headings for the table's rows."""
        try:
            headings = tuple(c.name for c in self.statement.selected_columns)
        except AttributeError as e:
            raise PalaceValueError(
                f"Unsupported statement in '{self.title}' query (id='{self.key}')."
            ) from e
        if not headings:
            raise PalaceValueError(
                f"No columns in '{self.title}' query (id='{self.key}')."
            )
        return headings

    def rows(self, *, session: Session, **query_params) -> Generator[tuple[Any, ...]]:
        """Run the query and yield its rows."""
        for row in session.execute(self.statement.params(**query_params)):
            yield tuple(row)


class TTabularDataProcessor[TTabularDataProcessorReturn](Protocol):
    """A tabular data processor."""

    def __call__(
        self,
        *,
        rows: TTabularRows,
        headings: TTabularHeadings | None,
    ) -> TTabularDataProcessorReturn: ...


class ReportTable(Protocol):
    """A table of data."""

    def __init__(self, *args, **kwargs) -> None: ...

    @property
    def definition(self) -> TabularQueryDefinition:
        """Get the tabular data definition."""

    def __call__[T](self, processor: TTabularDataProcessor[T]) -> T:
        """Process the tabular data."""

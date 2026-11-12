from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, NamedTuple

import pytest
from sqlalchemy import event
from sqlalchemy.orm import Session

from tests.fixtures.database import DatabaseTransactionFixture


class ExecutedQuery(NamedTuple):
    statement: str
    #: The parameters as the driver received them, so that a test can check
    #: which rows a query asked for, not just that it ran.
    parameters: Any


class QueryCounterFixture:
    """Record the SQL statements a block of code sends to the database.

    A lazy-loading N+1 is invisible to a functional assertion: the endpoint
    returns the same data whether it ran one query or one per row. Counting the
    statements is what keeps an N+1 from coming back once it has been fixed.
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self._statements: list[ExecutedQuery] = []

    def _record(
        self,
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        self._statements.append(ExecutedQuery(statement, parameters))

    @contextmanager
    def record(self) -> Iterator[list[ExecutedQuery]]:
        """Record the statements run inside the block, into the yielded list.

        Pending changes are flushed and the identity map expired first, so that
        objects the test's own setup loaded don't hide the queries under test.
        """
        self.session.flush()
        self.session.expire_all()
        statements: list[ExecutedQuery] = []
        self._statements = statements
        bind = self.session.get_bind()
        event.listen(bind, "before_cursor_execute", self._record)
        try:
            yield statements
        finally:
            event.remove(bind, "before_cursor_execute", self._record)

    @staticmethod
    def touching(queries: list[ExecutedQuery], table: str) -> list[ExecutedQuery]:
        """The recorded queries whose statement names ``table``."""
        return [query for query in queries if table in query.statement]

    @staticmethod
    def parameter_values(query: ExecutedQuery) -> set[Any]:
        """The values a query was executed with, whatever the driver named them."""
        parameters = query.parameters
        if isinstance(parameters, dict):
            return set(parameters.values())
        return set(parameters or ())


@pytest.fixture
def query_counter(db: DatabaseTransactionFixture) -> QueryCounterFixture:
    """A recorder for the statements sent on the test's database session.

    :param db: The database fixture whose session to record.
    :return: A `QueryCounterFixture` bound to that session.
    """
    return QueryCounterFixture(db.session)

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from sqlalchemy import event
from sqlalchemy.orm import Session

from tests.fixtures.database import DatabaseTransactionFixture


class QueryCounterFixture:
    """Record the SQL statements a block of code sends to the database.

    A lazy-loading N+1 is invisible to a functional assertion: the endpoint
    returns the same data whether it ran one query or one per row. Counting the
    statements is what keeps an N+1 from coming back once it has been fixed.
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self._statements: list[str] = []

    def _record(
        self,
        conn: Any,
        cursor: Any,
        statement: str,
        parameters: Any,
        context: Any,
        executemany: bool,
    ) -> None:
        self._statements.append(statement)

    @contextmanager
    def record(self) -> Iterator[list[str]]:
        """Record the statements run inside the block, into the yielded list.

        Pending changes are flushed and the identity map expired first, so that
        objects the test's own setup loaded don't hide the queries under test.
        """
        self.session.flush()
        self.session.expire_all()
        statements: list[str] = []
        self._statements = statements
        bind = self.session.get_bind()
        event.listen(bind, "before_cursor_execute", self._record)
        try:
            yield statements
        finally:
            event.remove(bind, "before_cursor_execute", self._record)

    @staticmethod
    def touching(statements: list[str], table: str) -> list[str]:
        """The recorded statements that name ``table``."""
        return [statement for statement in statements if table in statement]


@pytest.fixture
def query_counter(db: DatabaseTransactionFixture) -> QueryCounterFixture:
    return QueryCounterFixture(db.session)

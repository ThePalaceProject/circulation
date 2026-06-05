from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from palace.manager.sqlalchemy.model.identifier import RecursiveEquivalencyCache
from tests.fixtures.database import DatabaseTransactionFixture


class RecursiveEquivalencyCacheFixture:
    """Helpers for inspecting and resetting the ``RecursiveEquivalencyCache`` in tests."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def cache_for(self, parent_id: int) -> set[int]:
        """Return the set of ``identifier_id`` rows cached for the given parent."""
        rows = (
            self.session.execute(
                select(RecursiveEquivalencyCache.identifier_id).where(
                    RecursiveEquivalencyCache.parent_identifier_id == parent_id
                )
            )
            .scalars()
            .all()
        )
        return set(rows)

    def drop(self) -> None:
        """Delete every cache row (to isolate what a refresh recomputes)."""
        self.session.query(RecursiveEquivalencyCache).delete()
        self.session.commit()


@pytest.fixture
def recursive_equivalency_cache(
    db: DatabaseTransactionFixture,
) -> RecursiveEquivalencyCacheFixture:
    return RecursiveEquivalencyCacheFixture(db.session)

"""
Utilities for recomputing the RecursiveEquivalencyCache.

The cache stores pre-computed chains of equivalent identifiers so that
expensive recursive SQL lookups can be avoided at query time. These
functions handle refreshing it after equivalency changes.

For production use, call the equivalent_identifiers_refresh Celery task.
The refresh_equivalent_identifiers() function is provided for synchronous
use in tests.
"""

from __future__ import annotations

from sqlalchemy import and_, delete, select, union
from sqlalchemy.orm import Session

from palace.manager.sqlalchemy.model.identifier import (
    Equivalency,
    Identifier,
    RecursiveEquivalencyCache,
)


def process_identifier_ids(session: Session, identifier_ids: frozenset[int]) -> None:
    """
    Recompute the RecursiveEquivalencyCache for the given identifier IDs.

    Expands *identifier_ids* to include any chain members already in the
    cache that reference these identifiers, so that related chains are kept
    consistent. Existing cache rows for all affected parent IDs are deleted
    before new rows are inserted.

    :param session: DB session — caller is responsible for committing.
    :param identifier_ids: Identifiers whose chains should be recomputed.
    """
    if not identifier_ids:
        return

    # Expand to include parent IDs whose chains already reference any of
    # the given identifiers, so those chains are kept consistent.
    parent_ids = (
        session.execute(
            select(RecursiveEquivalencyCache.parent_identifier_id).where(
                RecursiveEquivalencyCache.identifier_id.in_(identifier_ids)
            )
        )
        .scalars()
        .all()
    )
    all_ids = identifier_ids | frozenset(parent_ids)

    # Run the stored procedure to compute the new chains.
    qu = (
        Identifier.recursively_equivalent_identifier_ids_query(Identifier.id)
        .select_from(Identifier)
        .where(Identifier.id.in_(all_ids))
        .add_columns(Identifier.id)
    )
    chained_identifiers = session.execute(qu).fetchall()

    # Delete old cache entries for every affected parent, then insert fresh ones.
    completed: set[int] = set()
    new_rows: list[RecursiveEquivalencyCache] = []
    for link_id, parent_id in chained_identifiers:
        if parent_id not in completed:
            session.execute(
                delete(RecursiveEquivalencyCache).where(
                    RecursiveEquivalencyCache.parent_identifier_id == parent_id
                )
            )
        new_rows.append(
            RecursiveEquivalencyCache(
                parent_identifier_id=parent_id, identifier_id=link_id
            )
        )
        completed.add(parent_id)

    session.add_all(new_rows)


def add_identity_equivalents(session: Session, batch_size: int = 200) -> None:
    """
    Insert (id, id) self-reference rows into RecursiveEquivalencyCache for any
    Identifier that lacks one.

    This ensures that queries against the cache always return at least the
    identifier itself, even when it has no equivalencies.
    """
    missing_q = (
        select(Identifier.id)
        .outerjoin(
            RecursiveEquivalencyCache,
            and_(
                RecursiveEquivalencyCache.parent_identifier_id == Identifier.id,
                RecursiveEquivalencyCache.is_parent == True,  # noqa: E712
            ),
        )
        .where(RecursiveEquivalencyCache.id == None)  # noqa: E711
        .execution_options(yield_per=batch_size)
    )

    for (identifier_id,) in session.execute(missing_q):
        session.add(
            RecursiveEquivalencyCache(
                parent_identifier_id=identifier_id, identifier_id=identifier_id
            )
        )


def refresh_equivalent_identifiers(session: Session, batch_size: int = 200) -> None:
    """
    Fully recompute the RecursiveEquivalencyCache for all current equivalencies.

    For use in tests only. Production code should use the
    equivalent_identifiers_refresh Celery task instead.
    """
    session.flush()
    union_q = union(
        select(Equivalency.input_id.label("id")),
        select(Equivalency.output_id.label("id")),
    ).subquery()
    all_ids = frozenset(session.execute(select(union_q.c.id)).scalars().all())

    if all_ids:
        process_identifier_ids(session, all_ids)

    add_identity_equivalents(session, batch_size)
    session.commit()

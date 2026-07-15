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

from collections.abc import Collection, Iterable
from itertools import batched

from sqlalchemy import and_, delete, select, union
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from palace.manager.sqlalchemy.model.identifier import (
    Equivalency,
    Identifier,
    RecursiveEquivalencyCache,
)

# Number of rows sent per INSERT statement.
INSERT_CHUNK_SIZE = 10_000


def _insert_cache_rows(session: Session, rows: Iterable[dict[str, int]]) -> None:
    """
    Insert (parent_identifier_id, identifier_id) rows into the cache.

    Rows that already exist are skipped rather than raising a unique violation.
    We are not the only writer of this table: the ``Identifier`` creation
    listener inserts self-reference rows from webapp transactions, and a
    redelivered Celery task can run a second refresh chain concurrently with the
    first. Because every parent's rows are deleted and recomputed from the same
    source data, a row a concurrent transaction commits under us holds the same
    value we were about to write, so ignoring the conflict converges on the
    correct chain instead of aborting the whole batch.

    Rows are inserted in a stable ``(parent_identifier_id, identifier_id)``
    order so that two transactions inserting overlapping keys acquire the
    unique-index entry locks in the same order, which keeps them from
    deadlocking against each other.
    """
    ordered = sorted(
        rows, key=lambda row: (row["parent_identifier_id"], row["identifier_id"])
    )
    for chunk in batched(ordered, INSERT_CHUNK_SIZE):
        session.execute(
            pg_insert(RecursiveEquivalencyCache)
            .values(list(chunk))
            .on_conflict_do_nothing(
                index_elements=["parent_identifier_id", "identifier_id"]
            )
        )


def _delete_cache_rows(session: Session, parent_ids: Collection[int]) -> None:
    """Delete every cache row belonging to any of the given parent identifiers."""
    if not parent_ids:
        return
    session.execute(
        delete(RecursiveEquivalencyCache).where(
            RecursiveEquivalencyCache.parent_identifier_id.in_(parent_ids)
        )
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
    if not chained_identifiers:
        return

    # Delete the old cache entries for every affected parent, then insert the
    # fresh ones. The delete is a single statement, so its row locks are taken
    # in the index's scan order — the same for any concurrent refresh — and the
    # inserts are ordered by :func:`_insert_cache_rows`, so overlapping refreshes
    # acquire locks consistently rather than deadlocking.
    _delete_cache_rows(session, {parent_id for _, parent_id in chained_identifiers})
    _insert_cache_rows(
        session,
        (
            {"parent_identifier_id": parent_id, "identifier_id": link_id}
            for link_id, parent_id in chained_identifiers
        ),
    )


def add_identity_equivalents(session: Session, batch_size: int = 200) -> None:
    """
    Insert (id, id) self-reference rows into RecursiveEquivalencyCache for any
    Identifier that lacks one.

    This ensures that queries against the cache always return at least the
    identifier itself, even when it has no equivalencies.

    An Identifier created by another transaction after this scan begins gets its
    self-reference from the ``Identifier`` creation listener, which may commit
    before we do; the insert tolerates that (see :func:`_insert_cache_rows`).
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

    for partition in session.execute(missing_q).partitions():
        _insert_cache_rows(
            session,
            (
                {"parent_identifier_id": identifier_id, "identifier_id": identifier_id}
                for (identifier_id,) in partition
            ),
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

from __future__ import annotations

from sqlalchemy import select, union
from sqlalchemy.orm import Session

from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.identifier import Equivalency


class DirtyIdentifierIds:
    """
    A persistent Redis set of identifier IDs whose recursive equivalents
    need to be recomputed.

    Unlike task-scoped Redis sets, this queue has a stable well-known key and
    no expiry — it accumulates dirty IDs between Celery task runs.

    IDs are pushed by SQLAlchemy listeners when Equivalency rows are created
    or deleted, and popped in batches by the equivalent_identifiers_refresh
    Celery task.
    """

    def __init__(self, redis_client: Redis) -> None:
        self._client = redis_client
        self._key = redis_client.get_key("DirtyIdentifierIds")

    def add(self, *identifier_ids: int) -> int:
        """
        Add identifier IDs to the dirty set.

        :return: Number of newly added IDs (already-present IDs are not counted).
        """
        if not identifier_ids:
            return 0
        return self._client.sadd(self._key, *[str(i) for i in identifier_ids])

    def pop(self, count: int) -> frozenset[int]:
        """
        Atomically pop up to *count* identifier IDs from the set.

        :return: A frozenset of the popped IDs (may be smaller than *count*
                 if fewer IDs are in the set).
        """
        return frozenset(int(v) for v in self._client.spop(self._key, count))

    def count(self) -> int:
        """Return the number of identifier IDs currently in the set."""
        return self._client.scard(self._key)

    def add_all_from_db(self, session: Session, chunk_size: int = 10_000) -> int:
        """
        Push all identifier IDs that appear in the equivalents table to the dirty set.

        Only identifiers referenced by at least one Equivalency row are pushed;
        identifiers with no equivalencies already have (id, id) self-references
        maintained by the Identifier creation listener and do not need recomputation.

        IDs are streamed from the DB with ``yield_per`` and sent to Redis in chunks
        of *chunk_size* (one ``SADD`` per chunk) to avoid a single large network call.

        :return: Total number of identifier IDs pushed.
        """
        union_q = union(
            select(Equivalency.input_id.label("identifier_id")).where(
                Equivalency.input_id.isnot(None)
            ),
            select(Equivalency.output_id.label("identifier_id")).where(
                Equivalency.output_id.isnot(None)
            ),
        ).subquery()
        query = select(union_q.c.identifier_id).execution_options(yield_per=chunk_size)

        total = 0
        for partition in session.execute(query).partitions():
            ids = [str(row.identifier_id) for row in partition]
            total += self._client.sadd(self._key, *ids)

        return total

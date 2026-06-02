from sqlalchemy import select

from palace.manager.sqlalchemy.model.identifier import (
    Equivalency,
    RecursiveEquivalencyCache,
)
from palace.manager.sqlalchemy.refresh_equivalents import (
    add_identity_equivalents,
    process_identifier_ids,
    refresh_equivalent_identifiers,
)
from tests.fixtures.database import DatabaseTransactionFixture


def _cache_for(session, parent_id: int) -> set[int]:
    """Return the set of identifier_ids cached for the given parent_id."""
    rows = (
        session.execute(
            select(RecursiveEquivalencyCache.identifier_id).where(
                RecursiveEquivalencyCache.parent_identifier_id == parent_id
            )
        )
        .scalars()
        .all()
    )
    return set(rows)


def _drop_cache(session) -> None:
    session.query(RecursiveEquivalencyCache).delete()
    session.commit()


class TestProcessIdentifierIds:
    def test_empty_input(self, db: DatabaseTransactionFixture) -> None:
        # Should be a no-op.
        process_identifier_ids(db.session, frozenset())
        assert db.session.query(RecursiveEquivalencyCache).count() == 0

    def test_computes_chain(self, db: DatabaseTransactionFixture) -> None:
        a = db.identifier()
        b = db.identifier()
        db.session.add(Equivalency(input_id=a.id, output_id=b.id, strength=1.0))
        db.session.flush()
        _drop_cache(db.session)

        process_identifier_ids(db.session, frozenset([a.id, b.id]))
        db.session.flush()

        # Both directions should produce a chain containing both identifiers.
        assert _cache_for(db.session, a.id) == {a.id, b.id}
        assert _cache_for(db.session, b.id) == {a.id, b.id}

    def test_expands_to_chain_members(self, db: DatabaseTransactionFixture) -> None:
        a = db.identifier()
        b = db.identifier()
        c = db.identifier()
        db.session.add(Equivalency(input_id=a.id, output_id=b.id, strength=1.0))
        db.session.add(Equivalency(input_id=b.id, output_id=c.id, strength=1.0))
        db.session.flush()
        _drop_cache(db.session)

        # Seed cache: compute chain for a and b first.
        process_identifier_ids(db.session, frozenset([a.id, b.id, c.id]))
        db.session.flush()

        # Now add a new equivalency and only provide the new identifier IDs.
        d = db.identifier()
        db.session.add(Equivalency(input_id=a.id, output_id=d.id, strength=1.0))
        db.session.flush()

        # Providing only d.id should expand to include a (the chain member),
        # and recompute a's chain to include d.
        process_identifier_ids(db.session, frozenset([a.id, d.id]))
        db.session.flush()

        assert d.id in _cache_for(db.session, a.id)
        assert a.id in _cache_for(db.session, d.id)

    def test_replaces_stale_cache(self, db: DatabaseTransactionFixture) -> None:
        a = db.identifier()
        b = db.identifier()
        db.session.add(Equivalency(input_id=a.id, output_id=b.id, strength=1.0))
        db.session.flush()
        _drop_cache(db.session)

        process_identifier_ids(db.session, frozenset([a.id, b.id]))
        db.session.flush()
        assert b.id in _cache_for(db.session, a.id)

        # Delete the equivalency and recompute — b should no longer appear.
        db.session.query(Equivalency).filter(Equivalency.input_id == a.id).delete()
        db.session.flush()

        process_identifier_ids(db.session, frozenset([a.id, b.id]))
        db.session.flush()

        assert b.id not in _cache_for(db.session, a.id)


class TestAddIdentityEquivalents:
    def test_adds_self_references(self, db: DatabaseTransactionFixture) -> None:
        a = db.identifier()
        b = db.identifier()
        _drop_cache(db.session)

        add_identity_equivalents(db.session)
        db.session.flush()

        # Both identifiers should now have (id, id) self-references.
        assert _cache_for(db.session, a.id) == {a.id}
        assert _cache_for(db.session, b.id) == {b.id}

    def test_skips_existing(self, db: DatabaseTransactionFixture) -> None:
        a = db.identifier()
        db.session.flush()
        # a already has a self-reference from the creation listener.

        before = db.session.query(RecursiveEquivalencyCache).count()
        add_identity_equivalents(db.session)
        db.session.flush()
        after = db.session.query(RecursiveEquivalencyCache).count()

        # No duplicate rows should be added.
        assert after == before


class TestRefreshEquivalentIdentifiers:
    def test_full_refresh(self, db: DatabaseTransactionFixture) -> None:
        a = db.identifier()
        b = db.identifier()
        c = db.identifier()  # no equivalency
        db.session.add(Equivalency(input_id=a.id, output_id=b.id, strength=1.0))
        db.session.flush()
        _drop_cache(db.session)

        refresh_equivalent_identifiers(db.session)

        # a and b should have chains to each other.
        assert _cache_for(db.session, a.id) == {a.id, b.id}
        assert _cache_for(db.session, b.id) == {a.id, b.id}
        # c should have a self-reference.
        assert _cache_for(db.session, c.id) == {c.id}

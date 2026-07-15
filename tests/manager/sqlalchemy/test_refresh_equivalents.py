import pytest

from palace.manager.sqlalchemy.model.identifier import (
    Equivalency,
    RecursiveEquivalencyCache,
)
from palace.manager.sqlalchemy.refresh_equivalents import (
    _delete_cache_rows,
    _insert_cache_rows,
    add_identity_equivalents,
    process_identifier_ids,
    refresh_equivalent_identifiers,
)
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.equivalents import RecursiveEquivalencyCacheFixture


class TestProcessIdentifierIds:
    def test_empty_input(self, db: DatabaseTransactionFixture) -> None:
        # Should be a no-op.
        process_identifier_ids(db.session, frozenset())
        assert db.session.query(RecursiveEquivalencyCache).count() == 0

    def test_computes_chain(
        self,
        db: DatabaseTransactionFixture,
        recursive_equivalency_cache: RecursiveEquivalencyCacheFixture,
    ) -> None:
        a = db.identifier()
        b = db.identifier()
        db.session.add(Equivalency(input_id=a.id, output_id=b.id, strength=1.0))
        db.session.flush()
        recursive_equivalency_cache.drop()

        process_identifier_ids(db.session, frozenset([a.id, b.id]))
        db.session.flush()

        # Both directions should produce a chain containing both identifiers.
        assert recursive_equivalency_cache.cache_for(a.id) == {a.id, b.id}
        assert recursive_equivalency_cache.cache_for(b.id) == {a.id, b.id}

    def test_expands_to_chain_members(
        self,
        db: DatabaseTransactionFixture,
        recursive_equivalency_cache: RecursiveEquivalencyCacheFixture,
    ) -> None:
        a = db.identifier()
        b = db.identifier()
        c = db.identifier()
        db.session.add(Equivalency(input_id=a.id, output_id=b.id, strength=1.0))
        db.session.add(Equivalency(input_id=b.id, output_id=c.id, strength=1.0))
        db.session.flush()
        recursive_equivalency_cache.drop()

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

        assert d.id in recursive_equivalency_cache.cache_for(a.id)
        assert a.id in recursive_equivalency_cache.cache_for(d.id)

    def test_replaces_stale_cache(
        self,
        db: DatabaseTransactionFixture,
        recursive_equivalency_cache: RecursiveEquivalencyCacheFixture,
    ) -> None:
        a = db.identifier()
        b = db.identifier()
        db.session.add(Equivalency(input_id=a.id, output_id=b.id, strength=1.0))
        db.session.flush()
        recursive_equivalency_cache.drop()

        process_identifier_ids(db.session, frozenset([a.id, b.id]))
        db.session.flush()
        assert b.id in recursive_equivalency_cache.cache_for(a.id)

        # Delete the equivalency and recompute — b should no longer appear.
        db.session.query(Equivalency).filter(Equivalency.input_id == a.id).delete()
        db.session.flush()

        process_identifier_ids(db.session, frozenset([a.id, b.id]))
        db.session.flush()

        assert b.id not in recursive_equivalency_cache.cache_for(a.id)

    def test_tolerates_rows_the_delete_did_not_remove(
        self,
        db: DatabaseTransactionFixture,
        recursive_equivalency_cache: RecursiveEquivalencyCacheFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # A concurrent writer can commit a cache row for a parent after we have
        # already deleted that parent's rows, so the rows we are about to insert
        # may collide. Simulate that by disabling the delete entirely: the insert
        # must skip the existing rows rather than raise a unique violation.
        a = db.identifier()
        b = db.identifier()
        db.session.add(Equivalency(input_id=a.id, output_id=b.id, strength=1.0))
        db.session.flush()

        process_identifier_ids(db.session, frozenset([a.id, b.id]))
        db.session.flush()
        assert recursive_equivalency_cache.cache_for(a.id) == {a.id, b.id}

        monkeypatch.setattr(
            "palace.manager.sqlalchemy.refresh_equivalents._delete_cache_rows",
            lambda session, parent_ids: None,
        )
        process_identifier_ids(db.session, frozenset([a.id, b.id]))
        db.session.flush()

        assert recursive_equivalency_cache.cache_for(a.id) == {a.id, b.id}
        assert recursive_equivalency_cache.cache_for(b.id) == {a.id, b.id}

    def test_no_chained_identifiers_is_noop(
        self,
        db: DatabaseTransactionFixture,
    ) -> None:
        # A non-empty set of identifier IDs that don't exist passes the empty
        # check but yields no chains, so the function returns without touching
        # the cache.
        db.identifier()
        db.session.flush()
        before = db.session.query(RecursiveEquivalencyCache).count()

        process_identifier_ids(db.session, frozenset([-1, -2]))
        db.session.flush()

        # Nothing was deleted or inserted.
        assert db.session.query(RecursiveEquivalencyCache).count() == before


class TestAddIdentityEquivalents:
    def test_adds_self_references(
        self,
        db: DatabaseTransactionFixture,
        recursive_equivalency_cache: RecursiveEquivalencyCacheFixture,
    ) -> None:
        a = db.identifier()
        b = db.identifier()
        recursive_equivalency_cache.drop()

        add_identity_equivalents(db.session)
        db.session.flush()

        # Both identifiers should now have (id, id) self-references.
        assert recursive_equivalency_cache.cache_for(a.id) == {a.id}
        assert recursive_equivalency_cache.cache_for(b.id) == {b.id}

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

    def test_sweeps_across_multiple_partitions(
        self,
        db: DatabaseTransactionFixture,
        recursive_equivalency_cache: RecursiveEquivalencyCacheFixture,
    ) -> None:
        # With more missing identifiers than one yield_per partition, the sweep
        # inserts self-references while the streaming cursor is still open and
        # spans several partitions. A batch_size of 2 over 5 identifiers forces
        # at least three partitions, exercising the interleaved-insert path.
        identifiers = [db.identifier() for _ in range(5)]
        db.session.flush()
        recursive_equivalency_cache.drop()

        add_identity_equivalents(db.session, batch_size=2)
        db.session.flush()

        for identifier in identifiers:
            assert recursive_equivalency_cache.cache_for(identifier.id) == {
                identifier.id
            }


class TestInsertCacheRows:
    def test_skips_existing_rows(
        self,
        db: DatabaseTransactionFixture,
        recursive_equivalency_cache: RecursiveEquivalencyCacheFixture,
    ) -> None:
        # A row committed by a concurrent writer — the Identifier creation
        # listener, or a second refresh chain — must not abort the insert.
        a = db.identifier()
        b = db.identifier()
        db.session.flush()
        # a and b already have self-references from the creation listener.

        rows = [
            {"parent_identifier_id": a.id, "identifier_id": a.id},
            {"parent_identifier_id": a.id, "identifier_id": b.id},
        ]
        _insert_cache_rows(db.session, rows)
        db.session.flush()

        assert recursive_equivalency_cache.cache_for(a.id) == {a.id, b.id}

        # Re-inserting the same rows is a no-op.
        _insert_cache_rows(db.session, rows)
        db.session.flush()

        assert recursive_equivalency_cache.cache_for(a.id) == {a.id, b.id}

    def test_empty_input(self, db: DatabaseTransactionFixture) -> None:
        _insert_cache_rows(db.session, [])
        assert db.session.query(RecursiveEquivalencyCache).count() == 0

    def test_batches_across_multiple_chunks(
        self,
        db: DatabaseTransactionFixture,
        recursive_equivalency_cache: RecursiveEquivalencyCacheFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # More rows than a single INSERT chunk must all be inserted, across
        # multiple statements. Shrink the chunk size so a handful of rows spans
        # several chunks.
        monkeypatch.setattr(
            "palace.manager.sqlalchemy.refresh_equivalents.INSERT_CHUNK_SIZE", 2
        )
        parent = db.identifier()
        others = [db.identifier() for _ in range(5)]
        db.session.flush()
        recursive_equivalency_cache.drop()

        rows = [
            {"parent_identifier_id": parent.id, "identifier_id": identifier.id}
            for identifier in [parent, *others]
        ]
        _insert_cache_rows(db.session, rows)
        db.session.flush()

        assert recursive_equivalency_cache.cache_for(parent.id) == {
            parent.id,
            *(identifier.id for identifier in others),
        }


class TestDeleteCacheRows:
    def test_empty_input(self, db: DatabaseTransactionFixture) -> None:
        # An empty parent set is a no-op: no DELETE is issued, so existing rows
        # (e.g. the creation listener's self-references) are left in place.
        db.identifier()
        db.session.flush()
        before = db.session.query(RecursiveEquivalencyCache).count()

        _delete_cache_rows(db.session, set())

        assert db.session.query(RecursiveEquivalencyCache).count() == before


class TestRefreshEquivalentIdentifiers:
    def test_full_refresh(
        self,
        db: DatabaseTransactionFixture,
        recursive_equivalency_cache: RecursiveEquivalencyCacheFixture,
    ) -> None:
        a = db.identifier()
        b = db.identifier()
        c = db.identifier()  # no equivalency
        db.session.add(Equivalency(input_id=a.id, output_id=b.id, strength=1.0))
        db.session.flush()
        recursive_equivalency_cache.drop()

        refresh_equivalent_identifiers(db.session)

        # a and b should have chains to each other.
        assert recursive_equivalency_cache.cache_for(a.id) == {a.id, b.id}
        assert recursive_equivalency_cache.cache_for(b.id) == {a.id, b.id}
        # c should have a self-reference.
        assert recursive_equivalency_cache.cache_for(c.id) == {c.id}

    def test_no_equivalencies(
        self,
        db: DatabaseTransactionFixture,
        recursive_equivalency_cache: RecursiveEquivalencyCacheFixture,
    ) -> None:
        # With no equivalencies at all, the chain computation is skipped and
        # only the self-reference sweep runs.
        a = db.identifier()
        db.session.flush()
        recursive_equivalency_cache.drop()

        refresh_equivalent_identifiers(db.session)

        assert recursive_equivalency_cache.cache_for(a.id) == {a.id}

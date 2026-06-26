"""Unit tests for BibliothecaCirculationUpdater."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from palace.manager.api.circulation.exceptions import RemoteInitiatedServerError
from palace.manager.celery.tasks import apply
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
from palace.manager.integration.license.bibliotheca_circulation_updater import (
    CIRCULATION_UPDATE_BATCH_SIZE,
    CIRCULATION_UPDATE_SERVICE_NAME,
    BibliothecaCirculationUpdater,
)
from palace.manager.sqlalchemy.constants import DataSourceConstants
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks.bibliotheca import MockBibliothecaAPI


def _make_updater(
    db: DatabaseTransactionFixture,
    api: MockBibliothecaAPI | None = None,
) -> tuple[BibliothecaCirculationUpdater, MockBibliothecaAPI]:
    """Return an updater and its bound API for the default test collection."""
    collection = MockBibliothecaAPI.mock_collection(db.session, db.default_library())
    mock_api = api or MockBibliothecaAPI(db.session, collection)
    updater = BibliothecaCirculationUpdater(db.session, collection, api=mock_api)
    return updater, mock_api


class TestBibliothecaCirculationUpdaterGetOffset:
    def test_no_prior_timestamp_returns_zero(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """With no stored Timestamp, get_offset returns 0."""
        updater, _ = _make_updater(db)
        assert updater.get_offset() == 0

    def test_existing_timestamp_with_counter_returns_it(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """With a stored Timestamp whose counter is set, get_offset returns counter."""
        updater, mock_api = _make_updater(db)
        collection = mock_api.collection

        Timestamp.stamp(
            db.session,
            service=CIRCULATION_UPDATE_SERVICE_NAME,
            service_type=Timestamp.TASK_TYPE,
            collection=collection,
            counter=42,
        )

        assert updater.get_offset() == 42

    def test_timestamp_with_counter_none_returns_zero(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """A Timestamp row whose counter is None falls back to 0."""
        updater, mock_api = _make_updater(db)
        collection = mock_api.collection

        ts = Timestamp.stamp(
            db.session,
            service=CIRCULATION_UPDATE_SERVICE_NAME,
            service_type=Timestamp.TASK_TYPE,
            collection=collection,
        )
        ts.counter = None
        db.session.flush()

        assert updater.get_offset() == 0


class TestBibliothecaCirculationUpdaterUpdateBatch:
    def test_correct_identifier_query_respects_offset_and_collection(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """update_batch only queries identifiers with id > offset licensed through this collection."""
        updater, mock_api = _make_updater(db)
        collection = mock_api.collection

        # Create three identifiers with license pools in this collection.
        id1 = db.identifier(identifier_type=Identifier.BIBLIOTHECA_ID, foreign_id="aaa")
        id2 = db.identifier(identifier_type=Identifier.BIBLIOTHECA_ID, foreign_id="bbb")
        id3 = db.identifier(identifier_type=Identifier.BIBLIOTHECA_ID, foreign_id="ccc")
        for identifier in [id1, id2, id3]:
            LicensePool.for_foreign_id(
                db.session,
                mock_api.data_source,
                Identifier.BIBLIOTHECA_ID,
                identifier.identifier,
                collection=collection,
            )
        db.session.flush()

        # Sort them by ID so we know which ones appear after offset.
        ids_sorted = sorted([id1, id2, id3], key=lambda i: i.id)
        offset = ids_sorted[0].id  # skip the first one

        with patch.object(BibliothecaAPI, "bibliographic_lookup", return_value=[]):
            result = updater.update_batch(offset)

        # Two identifiers have id > offset.
        assert result.records_handled == 2

    def test_full_batch_sets_next_offset_and_updates_counter(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """A full batch returns next_offset = last identifier id and stamps counter."""
        updater, mock_api = _make_updater(db)
        collection = mock_api.collection

        # Create exactly CIRCULATION_UPDATE_BATCH_SIZE identifiers.
        identifiers = []
        for i in range(CIRCULATION_UPDATE_BATCH_SIZE):
            ident = db.identifier(
                identifier_type=Identifier.BIBLIOTHECA_ID, foreign_id=f"id{i:04d}"
            )
            LicensePool.for_foreign_id(
                db.session,
                mock_api.data_source,
                Identifier.BIBLIOTHECA_ID,
                ident.identifier,
                collection=collection,
            )
            identifiers.append(ident)
        db.session.flush()

        with patch.object(BibliothecaAPI, "bibliographic_lookup", return_value=[]):
            result = updater.update_batch(0)

        expected_last_id = max(i.id for i in identifiers)
        assert result.records_handled == CIRCULATION_UPDATE_BATCH_SIZE
        assert result.next_offset == expected_last_id

        ts = Timestamp.lookup(
            db.session,
            CIRCULATION_UPDATE_SERVICE_NAME,
            Timestamp.TASK_TYPE,
            collection,
        )
        assert ts is not None
        assert ts.counter == expected_last_id

    def test_partial_batch_resets_next_offset_to_none_and_stamps_finish(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """A partial batch returns next_offset=None and stamps counter=0 with finish."""
        updater, mock_api = _make_updater(db)
        collection = mock_api.collection

        # Create fewer than CIRCULATION_UPDATE_BATCH_SIZE identifiers.
        for i in range(3):
            ident = db.identifier(
                identifier_type=Identifier.BIBLIOTHECA_ID, foreign_id=f"partial{i}"
            )
            LicensePool.for_foreign_id(
                db.session,
                mock_api.data_source,
                Identifier.BIBLIOTHECA_ID,
                ident.identifier,
                collection=collection,
            )
        db.session.flush()

        with patch.object(BibliothecaAPI, "bibliographic_lookup", return_value=[]):
            result = updater.update_batch(0)

        assert result.records_handled == 3
        assert result.next_offset is None

        ts = Timestamp.lookup(
            db.session,
            CIRCULATION_UPDATE_SERVICE_NAME,
            Timestamp.TASK_TYPE,
            collection,
        )
        assert ts is not None
        assert ts.counter == 0
        assert ts.finish is not None

    def test_empty_batch_returns_next_offset_none(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """With no identifiers in the collection, update_batch returns next_offset=None."""
        updater, _ = _make_updater(db)

        with patch.object(BibliothecaAPI, "bibliographic_lookup", return_value=[]):
            result = updater.update_batch(0)

        assert result.records_handled == 0
        assert result.next_offset is None

    def test_identifiers_not_returned_by_api_get_zeroed_out(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """Identifiers the API does not mention have their license pool zeroed out."""
        updater, mock_api = _make_updater(db)
        collection = mock_api.collection

        ident = db.identifier(
            identifier_type=Identifier.BIBLIOTHECA_ID, foreign_id="gone123"
        )
        pool, _ = LicensePool.for_foreign_id(
            db.session,
            mock_api.data_source,
            Identifier.BIBLIOTHECA_ID,
            ident.identifier,
            collection=collection,
        )
        pool.licenses_owned = 5
        pool.licenses_available = 3
        db.session.flush()

        # API returns nothing — identifier "gone123" was not mentioned.
        with patch.object(BibliothecaAPI, "bibliographic_lookup", return_value=[]):
            updater.update_batch(0)

        assert pool.licenses_owned == 0
        assert pool.licenses_available == 0

    def test_empty_api_response_does_not_zero_out_availability(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """A transient empty response must not be mistaken for "all titles removed".

        When Bibliotheca returns an empty body, ``bibliographic_lookup`` raises
        ``RemoteInitiatedServerError`` rather than an empty list (see
        ``BibliothecaAPI.bibliographic_lookup``).  The error must propagate so the
        Celery task retries, and the batch's availability must be left untouched —
        the opposite of the not-mentioned case, where a zero-count is applied.
        """
        updater, mock_api = _make_updater(db)
        collection = mock_api.collection

        ident = db.identifier(
            identifier_type=Identifier.BIBLIOTHECA_ID, foreign_id="keepme"
        )
        pool, _ = LicensePool.for_foreign_id(
            db.session,
            mock_api.data_source,
            Identifier.BIBLIOTHECA_ID,
            ident.identifier,
            collection=collection,
        )
        pool.licenses_owned = 5
        pool.licenses_available = 3
        db.session.flush()

        with patch.object(
            BibliothecaAPI,
            "bibliographic_lookup",
            side_effect=RemoteInitiatedServerError(
                "empty body", BibliothecaAPI.SERVICE_NAME
            ),
        ):
            with pytest.raises(RemoteInitiatedServerError):
                updater.update_batch(0)

        # Availability is untouched — the book was NOT removed from circulation.
        assert pool.licenses_owned == 5
        assert pool.licenses_available == 3

    def test_needs_apply_true_queues_bibliographic_apply(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """bibliographic_apply is queued when needs_apply() returns True."""
        updater, mock_api = _make_updater(db)
        collection = mock_api.collection

        ident = db.identifier(
            identifier_type=Identifier.BIBLIOTHECA_ID, foreign_id="applytest"
        )
        LicensePool.for_foreign_id(
            db.session,
            mock_api.data_source,
            Identifier.BIBLIOTHECA_ID,
            ident.identifier,
            collection=collection,
        )
        db.session.flush()

        mock_bib = MagicMock()
        mock_bib.needs_apply.return_value = True
        mock_bib.primary_identifier_data.identifier = ident.identifier

        with (
            patch.object(
                BibliothecaAPI, "bibliographic_lookup", return_value=[mock_bib]
            ),
            patch.object(apply, "bibliographic_apply") as mock_apply,
        ):
            updater.update_batch(0)

        mock_apply.delay.assert_called_once()

    def test_no_changes_does_not_queue_bibliographic_apply(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """Nothing is queued when neither bibliographic nor circulation data changed."""
        updater, mock_api = _make_updater(db)
        collection = mock_api.collection

        ident = db.identifier(
            identifier_type=Identifier.BIBLIOTHECA_ID, foreign_id="noapply"
        )
        LicensePool.for_foreign_id(
            db.session,
            mock_api.data_source,
            Identifier.BIBLIOTHECA_ID,
            ident.identifier,
            collection=collection,
        )
        db.session.flush()

        mock_bib = MagicMock()
        mock_bib.needs_apply.return_value = False
        mock_bib.circulation.needs_apply.return_value = False
        mock_bib.primary_identifier_data.identifier = ident.identifier

        with (
            patch.object(
                BibliothecaAPI, "bibliographic_lookup", return_value=[mock_bib]
            ),
            patch.object(apply, "bibliographic_apply") as mock_apply,
        ):
            updater.update_batch(0)

        mock_apply.delay.assert_not_called()

    def test_circulation_change_queues_bibliographic_apply(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """A circulation-only change is queued even when metadata is unchanged.

        Regression test: ``BibliographicData.needs_apply()`` excludes circulation
        from its hash, so an availability-only change leaves it ``False``.  The
        sweep must still dispatch, keyed on ``CirculationData.needs_apply()``.
        """
        updater, mock_api = _make_updater(db)
        collection = mock_api.collection

        ident = db.identifier(
            identifier_type=Identifier.BIBLIOTHECA_ID, foreign_id="circchanged"
        )
        LicensePool.for_foreign_id(
            db.session,
            mock_api.data_source,
            Identifier.BIBLIOTHECA_ID,
            ident.identifier,
            collection=collection,
        )
        db.session.flush()

        mock_bib = MagicMock()
        mock_bib.needs_apply.return_value = False
        mock_bib.circulation.needs_apply.return_value = True
        mock_bib.primary_identifier_data.identifier = ident.identifier

        with (
            patch.object(
                BibliothecaAPI, "bibliographic_lookup", return_value=[mock_bib]
            ),
            patch.object(apply, "bibliographic_apply") as mock_apply,
        ):
            updater.update_batch(0)

        mock_apply.delay.assert_called_once()
        mock_bib.circulation.needs_apply.assert_called_once_with(db.session, collection)


class TestBibliothecaCirculationUpdaterProcessIdentifiers:
    def test_calls_process_batch_without_touching_timestamp(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """process_identifiers delegates to _process_batch and does not modify Timestamp."""
        updater, mock_api = _make_updater(db)
        collection = mock_api.collection

        ident = db.identifier(
            identifier_type=Identifier.BIBLIOTHECA_ID, foreign_id="direct"
        )

        with patch.object(
            updater, "_process_batch", wraps=updater._process_batch
        ) as mock_process:
            with patch.object(BibliothecaAPI, "bibliographic_lookup", return_value=[]):
                updater.process_identifiers([ident])

        mock_process.assert_called_once_with([ident], synchronous=True)

        # No Timestamp should have been created.
        ts = Timestamp.lookup(
            db.session,
            CIRCULATION_UPDATE_SERVICE_NAME,
            Timestamp.TASK_TYPE,
            collection,
        )
        assert ts is None

    def test_changed_title_applied_synchronously(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """On the on-demand path, a changed title is applied in-band, not queued."""
        updater, mock_api = _make_updater(db)
        collection = mock_api.collection

        ident = db.identifier(
            identifier_type=Identifier.BIBLIOTHECA_ID, foreign_id="syncapply"
        )
        LicensePool.for_foreign_id(
            db.session,
            mock_api.data_source,
            Identifier.BIBLIOTHECA_ID,
            ident.identifier,
            collection=collection,
        )
        db.session.flush()

        mock_bib = MagicMock()
        mock_bib.needs_apply.return_value = True
        mock_bib.primary_identifier_data.identifier = ident.identifier
        mock_bib.edition.return_value = (MagicMock(), False)

        with (
            patch.object(
                BibliothecaAPI, "bibliographic_lookup", return_value=[mock_bib]
            ),
            patch.object(apply, "bibliographic_apply") as mock_apply,
        ):
            updater.process_identifiers([ident])

        # The change was applied synchronously, not dispatched to a worker.
        mock_bib.apply.assert_called_once()
        assert mock_bib.apply.call_args.kwargs["create_coverage_record"] is False
        mock_apply.delay.assert_not_called()

    def test_unchanged_title_and_circulation_is_not_applied(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """On the synchronous path, nothing is applied when neither bibliographic
        nor circulation data changed."""
        updater, mock_api = _make_updater(db)
        collection = mock_api.collection

        ident = db.identifier(
            identifier_type=Identifier.BIBLIOTHECA_ID, foreign_id="syncnoapply"
        )
        LicensePool.for_foreign_id(
            db.session,
            mock_api.data_source,
            Identifier.BIBLIOTHECA_ID,
            ident.identifier,
            collection=collection,
        )
        db.session.flush()

        mock_bib = MagicMock()
        mock_bib.needs_apply.return_value = False
        mock_bib.circulation.needs_apply.return_value = False
        mock_bib.primary_identifier_data.identifier = ident.identifier

        with (
            patch.object(
                BibliothecaAPI, "bibliographic_lookup", return_value=[mock_bib]
            ),
            patch.object(apply, "bibliographic_apply") as mock_apply,
        ):
            updater.process_identifiers([ident])

        mock_bib.apply.assert_not_called()
        mock_apply.delay.assert_not_called()

    def test_circulation_applied_when_metadata_unchanged(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """Regression: availability updates land even when metadata is unchanged.

        End-to-end proof that the sweep applies a circulation-only change. The
        first lookup establishes the edition's bibliographic hash and the pool's
        initial availability; the second carries identical metadata but fewer
        available copies. Because ``BibliographicData``'s hash excludes
        circulation, ``needs_apply()`` is ``False`` for the second lookup -- yet
        the pool must still reflect the new availability.
        """
        updater, mock_api = _make_updater(db)
        collection = mock_api.collection

        edition, pool = db.edition(
            identifier_type=Identifier.BIBLIOTHECA_ID,
            data_source_name=DataSourceConstants.BIBLIOTHECA,
            with_license_pool=True,
            collection=collection,
        )
        bibliotheca_id = pool.identifier.identifier

        def make_bib(owned: int, available: int) -> BibliographicData:
            identifier_data = IdentifierData(
                type=Identifier.BIBLIOTHECA_ID, identifier=bibliotheca_id
            )
            return BibliographicData(
                data_source_name=DataSourceConstants.BIBLIOTHECA,
                primary_identifier_data=identifier_data,
                title="Unchanging Title",
                medium=Edition.BOOK_MEDIUM,
                circulation=CirculationData(
                    data_source_name=DataSourceConstants.BIBLIOTHECA,
                    primary_identifier_data=identifier_data,
                    licenses_owned=owned,
                    licenses_available=available,
                    licenses_reserved=0,
                    patrons_in_hold_queue=0,
                ),
            )

        # First sweep: establishes the bibliographic hash and seeds availability.
        with patch.object(
            BibliothecaAPI, "bibliographic_lookup", return_value=[make_bib(5, 5)]
        ):
            updater.process_identifiers([pool.identifier])
        assert pool.licenses_owned == 5
        assert pool.licenses_available == 5

        # Second sweep: identical metadata, so the bibliographic hash is unchanged...
        second = make_bib(5, 0)
        assert second.needs_apply(db.session) is False

        # ...but the availability change must still be applied.
        with patch.object(
            BibliothecaAPI, "bibliographic_lookup", return_value=[second]
        ):
            updater.process_identifiers([pool.identifier])

        assert pool.licenses_owned == 5
        assert pool.licenses_available == 0

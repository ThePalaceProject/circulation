"""Tests for the OverdriveImporter class."""

from __future__ import annotations

import datetime
import json
import logging
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest
from sqlalchemy.orm import Session

from palace.util.datetime_helpers import datetime_utc
from palace.util.exceptions import PalaceValueError

from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.integration.license.overdrive.importer import (
    FeedImportResult,
    OverdriveImporter,
)
from palace.manager.integration.license.overdrive.model import (
    Checkout,
    MetadataResponse,
)
from palace.manager.integration.license.overdrive.representation import (
    OverdriveRepresentationExtractor,
)
from palace.manager.integration.license.overdrive.requests import BookInfoEndpoint
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism, LicensePool
from palace.manager.sqlalchemy.model.resource import Representation
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OverdriveFilesFixture
from tests.fixtures.overdrive import OverdriveAPIFixture
from tests.fixtures.redis import RedisFixture
from tests.fixtures.services import ServicesFixture

# The policy a real import runs under. Referenced, not copied, so that a change to
# it cannot leave these tests passing against a policy production no longer uses.
IMPORT_POLICY = OverdriveImporter.REPLACEMENT_POLICY


class TestOverdriveImporter:
    """Tests for the OverdriveImporter class."""

    def test_init_success(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """Test successful initialization of OverdriveImporter."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session, collection=collection, registry=registry
        )

        assert importer._db == db.session
        assert importer._collection == collection
        assert importer._identifier_set is None
        assert importer._parent_identifiers is None
        assert isinstance(importer._api, OverdriveAPI)
        assert isinstance(importer._extractor, OverdriveRepresentationExtractor)

    def test_init_with_api_provided(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """Test initialization when API instance is provided."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        mock_api = Mock(spec=OverdriveAPI)

        importer = OverdriveImporter(
            db=db.session, collection=collection, registry=registry, api=mock_api
        )

        assert importer._api == mock_api

    def test_init_with_identifier_set(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """Test initialization with identifier_set provided."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        mock_identifier_set = Mock(spec=IdentifierSet)

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            identifier_set=mock_identifier_set,
        )

        assert importer._identifier_set == mock_identifier_set

    def test_init_with_parent_identifier_set(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """Test initialization with parent_identifier_set."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        # Create mock identifiers
        id1 = IdentifierData(type=Identifier.OVERDRIVE_ID, identifier="id1")
        id2 = IdentifierData(type=Identifier.OVERDRIVE_ID, identifier="id2")
        mock_parent_set = Mock(spec=IdentifierSet)
        mock_parent_set.get.return_value = {id1, id2}

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            parent_identifier_set=mock_parent_set,
        )

        assert importer._parent_identifiers == {id1, id2}

    def test_init_invalid_collection_protocol(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """Test that initialization fails with invalid collection protocol."""
        # Create a collection with wrong protocol
        collection = db.collection(protocol="Not Overdrive")
        registry = services_fixture.services.integration_registry.license_providers()

        with pytest.raises(PalaceValueError) as exc:
            OverdriveImporter(db=db.session, collection=collection, registry=registry)

        assert "is not an OverDrive collection" in str(exc.value)

    def test_get_timestamp(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """Test get_timestamp creates or retrieves a timestamp."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session, collection=collection, registry=registry
        )

        # First call should create a new timestamp
        timestamp1 = importer.get_timestamp()
        assert isinstance(timestamp1, Timestamp)
        assert timestamp1.service == "OverDrive Import"
        assert timestamp1.service_type == Timestamp.TASK_TYPE
        assert timestamp1.collection == collection

        # Second call should retrieve the same timestamp
        timestamp2 = importer.get_timestamp()
        assert timestamp1.id == timestamp2.id

    def test_process_book_skips_metadata_update_if_identifier_in_parent_identifiers(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
        services_fixture: ServicesFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """Ensure _process_book skips metadata update if identifier is in the parent identifier set."""

        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        book_list_data = json.loads(
            overdrive_files_fixture.sample_data("overdrive_book_list.json")
        )
        sample_book = book_list_data["products"][0]
        parent_identifier = IdentifierData(
            type=Identifier.OVERDRIVE_ID, identifier=sample_book["id"]
        )
        redis_client = redis_fixture.client
        parent_identifier_set = IdentifierSet(
            redis_client=redis_client, key="test_parent_set_key"
        )
        parent_identifier_set.add(parent_identifier)

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=overdrive_api_fixture.api,
            parent_identifier_set=parent_identifier_set,
        )

        metadata_lookup_mock = Mock(
            side_effect=AssertionError("Metadata lookup should not be called")
        )
        importer._api.metadata_lookup = metadata_lookup_mock

        book = sample_book.copy()
        book.pop("metadata", None)
        book["availabilityV2"] = json.loads(
            overdrive_files_fixture.sample_data(
                "overdrive_availability_information.json"
            )
        )

        apply_bibliographic = Mock()
        apply_circulation = Mock()

        identifier, changed = importer._process_book(
            book=book,
            fetch_metadata=False,
            policy=ReplacementPolicy(),
            apply_bibliographic=apply_bibliographic,
            apply_circulation=apply_circulation,
        )

        assert isinstance(identifier, Identifier)
        assert isinstance(changed, bool)
        metadata_lookup_mock.assert_not_called()
        apply_bibliographic.assert_not_called()
        assert apply_circulation.called

    def test_process_book_uses_metadata_record_not_product_summary(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """_process_book must convert the fetched metadata record, not the product summary.

        The product-list summary does not carry ``creators``, ``publisher``,
        ``publishDate`` or ``languages``, so converting it loses the author,
        publisher and publication date and silently defaults the language to
        English.
        """
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=overdrive_api_fixture.api,
        )

        book = self._book_with_both_documents(overdrive_files_fixture)
        summary = json.loads(
            overdrive_files_fixture.sample_data("overdrive_book_list.json")
        )["products"][0]
        metadata = book["metadata"]

        # Sanity check on the fixture: the fields we care about really are absent
        # from the summary and present in the metadata record.
        for field in ("creators", "publisher", "publishDate", "languages"):
            assert field not in summary
            assert field in metadata

        apply_bibliographic = Mock()
        apply_circulation = Mock()

        importer._process_book(
            book=book,
            fetch_metadata=True,
            policy=ReplacementPolicy(),
            apply_bibliographic=apply_bibliographic,
            apply_circulation=apply_circulation,
        )

        apply_bibliographic.assert_called_once()
        bibliographic = apply_bibliographic.call_args.args[0]

        assert bibliographic.publisher == "Wiley"
        assert [c.display_name for c in bibliographic.contributors] == [
            "Andreas Rüping"
        ]
        assert bibliographic.published == datetime.date(2005, 1, 31)
        assert bibliographic.title == metadata["title"]
        # The language is not checked here. This record's language is English and
        # so is the value an absent ``languages`` key falls back to, so the check
        # would pass either way. ``test_process_book_non_english_language_preserved``
        # covers it with a record that is not in English.

        # Formats are deliberately not part of what converting the metadata record
        # recovers. The product-list summary carries a ``formats`` list of its own,
        # and for this fixture pair both documents yield the same four FormatData,
        # so nothing here tells the fixed code from the unfixed code. Assert them
        # only as a non-regression check that routing through the metadata record
        # did not drop them. Noticing a *change* to the formats is a separate
        # concern, covered by
        # ``test_process_book_dispatches_a_single_apply_carrying_both_halves``
        # below and by ``TestCirculationData.test_needs_apply_detects_a_format_only_change``.
        assert bibliographic.circulation is not None
        assert len(bibliographic.circulation.formats) == 4

    @staticmethod
    def _mock_extractor(
        importer: OverdriveImporter,
        identifier: str,
        *,
        bibliographic_changed: bool = True,
        circulation_changed: bool = True,
    ) -> tuple[Mock, Mock]:
        """Replace the importer's extractor with mocks shaped like the real one.

        Two details matter and are easy to leave out. The real extractor always
        attaches a CirculationData carrying the metadata record's formats, which
        ``_process_book`` folds into the availability snapshot; and it keys the
        primary identifier off the record it was handed, which ``_process_book``
        checks against the identifier the product list gave it. A mock missing
        either one is not shaped like anything the importer actually receives.

        :param importer: The importer whose ``_extractor`` is replaced.
        :param identifier: The Overdrive id the metadata record reports.
        :param bibliographic_changed: What ``BibliographicData.needs_apply`` returns.
        :param circulation_changed: What ``CirculationData.needs_apply`` returns.
        :return: The (bibliographic, circulation) mocks the extractor will return.
        """
        bibliographic = Mock(spec=BibliographicData)
        bibliographic.circulation = Mock(spec=CirculationData)
        bibliographic.circulation.formats = []
        bibliographic.needs_apply.return_value = bibliographic_changed
        bibliographic.primary_identifier_data = IdentifierData(
            type=Identifier.OVERDRIVE_ID, identifier=identifier
        )

        circulation = Mock(spec=CirculationData)
        circulation.needs_apply.return_value = circulation_changed

        importer._extractor.book_info_to_bibliographic = Mock(
            return_value=bibliographic
        )
        importer._extractor.book_info_to_circulation = Mock(return_value=circulation)
        return bibliographic, circulation

    def _book_with_both_documents(
        self, overdrive_files_fixture: OverdriveFilesFixture
    ) -> dict[str, Any]:
        """A product-list entry with its metadata record and availability attached."""
        book = json.loads(
            overdrive_files_fixture.sample_data("overdrive_book_list.json")
        )["products"][0].copy()
        metadata = json.loads(
            overdrive_files_fixture.sample_data("overdrive_metadata.json")
        )
        metadata["id"] = book["id"]
        book["metadata"] = metadata
        availability = json.loads(
            overdrive_files_fixture.sample_data(
                "overdrive_availability_information.json"
            )
        )
        availability["reserveId"] = book["id"]
        book["availabilityV2"] = availability
        return book

    def test_process_book_dispatches_a_single_apply_carrying_both_halves(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """Formats and availability must travel together, in one task.

        Overdrive is the only distributor that reads them from two documents, and
        converting them yields two CirculationData objects. Dispatching both as
        independent tasks makes two writers for one pool: each stamps
        ``pool.updated_at`` with its own ``as_of_timestamp`` and ``should_apply_to``
        rejects anything strictly older, so whichever lands second is discarded. When
        that is the formats half, the edition's hash is stamped anyway and no later
        import retries it -- the title keeps zero delivery mechanisms permanently.
        """
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=overdrive_api_fixture.api,
        )

        book = self._book_with_both_documents(overdrive_files_fixture)

        apply_bibliographic = Mock()
        apply_circulation = Mock()
        importer._process_book(
            book=book,
            fetch_metadata=True,
            policy=ReplacementPolicy(),
            apply_bibliographic=apply_bibliographic,
            apply_circulation=apply_circulation,
        )

        # Exactly one task, never two.
        apply_bibliographic.assert_called_once()
        apply_circulation.assert_not_called()

        # And the one object it carries holds both halves: the formats from the
        # metadata record and the counts from the availability document.
        circulation = apply_bibliographic.call_args.args[0].circulation
        assert circulation is not None
        assert len(circulation.formats) == 4
        assert circulation.licenses_owned is not None

    @pytest.mark.parametrize("bibliographic_changed", [True, False])
    def test_process_book_without_availability_still_carries_formats(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
        services_fixture: ServicesFixture,
        bibliographic_changed: bool,
    ) -> None:
        """Metadata present but no availability document -- a real Overdrive 404.

        The converter's formats-only snapshot is then the only circulation data we
        have, and it still has to reach the database by whichever branch runs: hung
        off the bibliographic payload when the book's details changed, dispatched on
        its own when they did not.
        """
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=overdrive_api_fixture.api,
        )
        book = self._book_with_both_documents(overdrive_files_fixture)
        del book["availabilityV2"]

        apply_bibliographic = Mock()
        apply_circulation = Mock()
        policy = IMPORT_POLICY
        with patch.object(
            BibliographicData, "needs_apply", return_value=bibliographic_changed
        ):
            importer._process_book(
                book=book,
                fetch_metadata=True,
                policy=policy,
                apply_bibliographic=apply_bibliographic,
                apply_circulation=apply_circulation,
            )

        if bibliographic_changed:
            apply_circulation.assert_not_called()
            apply_bibliographic.assert_called_once()
            circulation = apply_bibliographic.call_args.args[0].circulation
        else:
            apply_bibliographic.assert_not_called()
            apply_circulation.assert_called_once()
            circulation = apply_circulation.call_args.args[0]
            # The formats came from the metadata record, so this snapshot is
            # authoritative about them and may remove one that has gone away.
            assert apply_circulation.call_args.kwargs["replace"].formats is True

        # Either way it is the converter's formats-only snapshot: formats present,
        # no availability counts, because there was no availability document.
        assert circulation is not None
        assert len(circulation.formats) == 4
        assert circulation.licenses_owned is None

    @pytest.mark.parametrize(
        "has_metadata, expected_format_count, may_remove_formats",
        [
            pytest.param(True, 4, True, id="metadata-record formats are authoritative"),
            pytest.param(False, 0, False, id="availability-only formats are not"),
        ],
    )
    def test_process_book_circulation_branch_removes_formats_only_when_authoritative(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
        services_fixture: ServicesFixture,
        has_metadata: bool,
        expected_format_count: int,
        may_remove_formats: bool,
    ) -> None:
        """Only a snapshot built from the metadata record may remove a format.

        Both cases take the circulation branch, and getting either one wrong does
        real damage:

        * With the metadata record, the format list is complete, so a format
          Overdrive has dropped must go away. Deny the policy and the stale delivery
          mechanism survives while the apply still records the new hash -- no later
          import retries the removal, and patrons keep being offered a format
          Overdrive no longer has.
        * Without it the snapshot has no formats at all, because an Advantage import
          skips the metadata lookup for any title the parent collection already
          handled. Grant the policy and that empty list is treated as complete,
          deleting every delivery mechanism -- and since delivery mechanisms are
          keyed on data source and identifier rather than on collection, the
          parent's titles go with them.
        """
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=overdrive_api_fixture.api,
        )
        book = self._book_with_both_documents(overdrive_files_fixture)
        if not has_metadata:
            del book["metadata"]

        apply_bibliographic = Mock()
        apply_circulation = Mock()
        # The book's other details are unchanged, so the circulation branch runs.
        with patch.object(BibliographicData, "needs_apply", return_value=False):
            importer._process_book(
                book=book,
                fetch_metadata=True,
                policy=IMPORT_POLICY,
                apply_bibliographic=apply_bibliographic,
                apply_circulation=apply_circulation,
            )

        apply_bibliographic.assert_not_called()
        apply_circulation.assert_called_once()

        circulation = apply_circulation.call_args.args[0]
        assert len(circulation.formats) == expected_format_count

        replace = apply_circulation.call_args.kwargs.get("replace")
        assert bool(replace and replace.formats) is may_remove_formats

    def test_process_book_apply_leaves_loan_created_formats_alone(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """An import apply with removal authority leaves the loan path's formats alone.

        Overdrive is the only distributor that writes delivery mechanisms outside the
        import. Its metadata record never says which ebook format a title is really
        in, so the import guesses -- Adobe EPUB available, open EPUB not -- and the
        checkout document corrects the guess once there is a loan. The correction
        used here is the exact inverse of the guess.

        Now that the circulation branch may delete a format, those corrections are
        worth pinning. They survive for two reasons:

        * the loan path only creates mechanisms drawn from
          ``internal_format_to_delivery_mechanism``, every one of which is already in
          the default ``ebook-overdrive`` list, so none is ever absent from the
          incoming formats and none is selected for deletion;
        * ``FormatData.update_available`` defaults to False on the import path, so
          re-applying the defaults does not flip an availability back.
        """
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=overdrive_api_fixture.api,
        )
        book = self._book_with_both_documents(overdrive_files_fixture)
        pool, _ = LicensePool.for_foreign_id(
            db.session,
            DataSource.OVERDRIVE,
            Identifier.OVERDRIVE_ID,
            book["id"],
            collection=collection,
        )

        # A patron borrows the book, and the checkout document tells us which
        # formats the loan is really available in.
        checkout = Checkout.model_validate_json(
            overdrive_files_fixture.sample_data(
                "checkout_response_no_format_locked_in_no_drm.json"
            )
        )
        assert collection.id is not None
        loan_info = OverdriveAPI.process_checkout_data(checkout, collection.id)
        assert loan_info is not None
        loan_info.create_or_update(db.patron(), license_pool=pool)

        epub_adobe = (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM)
        epub_no_drm = (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM)
        after_loan = self._format_availability(db.session, pool)
        # The loan inverted the import's guess about both EPUB mechanisms.
        assert after_loan[epub_adobe] is False
        assert after_loan[epub_no_drm] is True

        # Import the title again. Its other details are unchanged, so the
        # circulation branch runs, and it carries the policy because its formats
        # came from the metadata record.
        apply_circulation = Mock()
        with patch.object(BibliographicData, "needs_apply", return_value=False):
            importer._process_book(
                book=book,
                fetch_metadata=True,
                policy=IMPORT_POLICY,
                apply_bibliographic=Mock(),
                apply_circulation=apply_circulation,
            )
        circulation = apply_circulation.call_args.args[0]
        replace = apply_circulation.call_args.kwargs.get("replace")
        assert replace is not None and replace.formats
        circulation.apply(db.session, collection, replace)

        # Nothing was deleted, and no availability was flipped back.
        assert self._format_availability(db.session, pool) == after_loan

    @staticmethod
    def _format_availability(
        session: Session, pool: LicensePool
    ) -> dict[tuple[str | None, str | None], bool]:
        """A pool's delivery mechanisms, keyed by content type and DRM scheme."""
        session.flush()
        session.expire(pool)
        return {
            (
                lpdm.delivery_mechanism.content_type,
                lpdm.delivery_mechanism.drm_scheme,
            ): lpdm.available
            for lpdm in pool.delivery_mechanisms
        }

    @pytest.mark.parametrize("fetch_metadata", [True, False])
    def test_process_book_metadata_error_document_is_skipped(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
        services_fixture: ServicesFixture,
        caplog: pytest.LogCaptureFixture,
        fetch_metadata: bool,
    ) -> None:
        """An Overdrive error document must not crash the import.

        Overdrive reports an unrecognized identifier as an HTTP 200 whose body is an
        error document rather than as an HTTP error, so ``book["metadata"]`` can be a
        non-empty dict with no ``id`` in it. That passes the ``if book.get("metadata")``
        guard but gives the extractor nothing to key on, and it returns None.

        Both ingest paths are covered: ``fetch_metadata=True`` is the bulk path used for
        main collections, where the error document is attached by the book-list fetch;
        ``fetch_metadata=False`` is the lazy path used for advantage collections, where
        it comes back from a per-title metadata lookup.
        """
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=overdrive_api_fixture.api,
        )

        error_document = {
            "errorCode": "NotFound",
            "message": "The requested record was not found.",
            "token": "a-token",
        }

        book_list_data = json.loads(
            overdrive_files_fixture.sample_data("overdrive_book_list.json")
        )
        book = book_list_data["products"][0].copy()
        book["availabilityV2"] = json.loads(
            overdrive_files_fixture.sample_data(
                "overdrive_availability_information.json"
            )
        )

        if fetch_metadata:
            # Bulk path: the error document is already attached to the book.
            book["metadata"] = error_document
            importer._api.metadata_lookup = Mock(
                side_effect=AssertionError("Metadata lookup should not be called")
            )
        else:
            # Lazy path: the error document comes back from the metadata lookup.
            book.pop("metadata", None)
            importer._api.metadata_lookup = Mock(
                return_value=MetadataResponse.model_validate(error_document)
            )

        apply_bibliographic = Mock()
        apply_circulation = Mock()

        with caplog.at_level(logging.WARNING):
            identifier, changed = importer._process_book(
                book=book,
                fetch_metadata=fetch_metadata,
                policy=ReplacementPolicy(),
                apply_bibliographic=apply_bibliographic,
                apply_circulation=apply_circulation,
            )

        # The book is skipped rather than blowing up the whole import run.
        assert isinstance(identifier, Identifier)
        apply_bibliographic.assert_not_called()
        assert (
            "Could not extract bibliographic data from Overdrive metadata"
            in caplog.text
        )

        # The circulation data is independent of the metadata record, so it is still
        # applied -- availability must keep flowing even when the record is unusable.
        apply_circulation.assert_called_once()
        assert changed is True

    def test_process_book_metadata_for_a_different_identifier_is_skipped(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
        services_fixture: ServicesFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Metadata describing a different title must not be applied.

        The identifier comes from the product list while the bibliographic data comes
        from the separately fetched metadata record, so the two can disagree. Nothing
        downstream would catch it: the apply task resolves the edition from the
        record's own identifier, so its primary-identifier check compares the record
        against itself and passes.

        The importer therefore stops before the merge, which would otherwise graft
        the wrong title's format list onto this title's availability snapshot and
        apply it with authority to remove the formats it really has.
        """
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=overdrive_api_fixture.api,
        )

        book_list_data = json.loads(
            overdrive_files_fixture.sample_data("overdrive_book_list.json")
        )
        book = book_list_data["products"][0].copy()
        metadata = json.loads(
            overdrive_files_fixture.sample_data("overdrive_metadata.json")
        )
        # The fixtures describe two different titles, so leaving the ids as they are
        # is exactly the mismatch we want to detect.
        assert metadata["id"] != book["id"]
        book["metadata"] = metadata
        book["availabilityV2"] = json.loads(
            overdrive_files_fixture.sample_data(
                "overdrive_availability_information.json"
            )
        )

        apply_bibliographic = Mock()
        apply_circulation = Mock()

        with caplog.at_level(logging.WARNING):
            importer._process_book(
                book=book,
                fetch_metadata=True,
                policy=IMPORT_POLICY,
                apply_bibliographic=apply_bibliographic,
                apply_circulation=apply_circulation,
            )

        apply_bibliographic.assert_not_called()
        assert "reports a different identifier" in caplog.text

        # The availability document still applies on its own -- that part is about
        # this title and is worth doing. But it must carry none of the other title's
        # formats, and no authority to remove this one's.
        apply_circulation.assert_called_once()
        assert apply_circulation.call_args.args[0].formats == []
        assert apply_circulation.call_args.kwargs.get("replace") is None

    def test_process_book_identifier_case_mismatch_is_tolerated(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """Overdrive is inconsistent about id case; that alone is not a mismatch."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=overdrive_api_fixture.api,
        )

        book = self._book_with_both_documents(overdrive_files_fixture)
        book["metadata"]["id"] = book["id"].upper()
        assert book["metadata"]["id"] != book["id"]

        apply_bibliographic = Mock()

        importer._process_book(
            book=book,
            fetch_metadata=True,
            policy=ReplacementPolicy(),
            apply_bibliographic=apply_bibliographic,
            apply_circulation=Mock(),
        )

        apply_bibliographic.assert_called_once()

    def test_process_book_non_english_language_preserved(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """A non-English book must not be miscatalogued as English."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=overdrive_api_fixture.api,
        )

        book = self._book_with_both_documents(overdrive_files_fixture)
        book["metadata"]["languages"] = [{"code": "fr", "name": "French"}]

        apply_bibliographic = Mock()

        importer._process_book(
            book=book,
            fetch_metadata=True,
            policy=ReplacementPolicy(),
            apply_bibliographic=apply_bibliographic,
            apply_circulation=Mock(),
        )

        apply_bibliographic.assert_called_once()
        assert apply_bibliographic.call_args.args[0].language == "fre"

    def test_import_collection_basic(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """Test import_collection with basic book data."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        api = overdrive_api_fixture.api

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=api,
        )

        # Mock the API and extractor
        mock_apply_bib = Mock()
        mock_apply_circ = Mock()
        modified_since = datetime_utc(2023, 1, 1)

        # Load real book data from test files
        book_list_data = json.loads(
            overdrive_files_fixture.sample_data("overdrive_book_list.json")
        )
        # Get the first product from the real data
        mock_book_data = [book_list_data["products"][0]]
        # Add metadata and availability that would be fetched
        mock_book_data[0]["metadata"] = json.loads(
            overdrive_files_fixture.sample_data("overdrive_metadata.json")
        )
        mock_book_data[0]["availabilityV2"] = json.loads(
            overdrive_files_fixture.sample_data(
                "overdrive_availability_information.json"
            )
        )

        mock_next_endpoint = BookInfoEndpoint(url="http://next.page")

        api.async_requests.fetch_book_info_list = AsyncMock(
            return_value=(mock_book_data, mock_next_endpoint)
        )

        mock_bibliographic, mock_circulation = self._mock_extractor(
            importer,
            mock_book_data[0]["id"],
        )

        # Run import
        result = importer.import_collection(
            apply_bibliographic=mock_apply_bib,
            apply_circulation=mock_apply_circ,
            modified_since=modified_since,
        )

        # Verify result
        assert isinstance(result, FeedImportResult)
        assert result.processed_count == 1
        assert result.next_page == mock_next_endpoint

        # Exactly one apply is dispatched per book. The bibliographic data changed,
        # so it carries the circulation update with it rather than racing a second task.
        assert mock_apply_bib.call_count == 1
        assert mock_apply_circ.call_count == 0

        # Verify identifier was created
        identifier, _ = Identifier.for_foreign_id(
            db.session,
            foreign_id="overdrive-id-1",
            foreign_identifier_type=Identifier.OVERDRIVE_ID,
        )
        assert identifier is not None

    def test_import_collection_with_endpoint_provided(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """Test import_collection when endpoint is provided."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        api = overdrive_api_fixture.api

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=api,
        )

        mock_apply_bib = Mock()
        mock_apply_circ = Mock()
        modified_since = datetime_utc(2023, 1, 1)
        custom_endpoint = BookInfoEndpoint(url="http://custom.endpoint")

        # Mock empty book data
        api.async_requests.fetch_book_info_list = AsyncMock(return_value=([], None))

        # Run import with custom endpoint
        result = importer.import_collection(
            apply_bibliographic=mock_apply_bib,
            apply_circulation=mock_apply_circ,
            modified_since=modified_since,
            endpoint=custom_endpoint,
        )

        # Verify the custom endpoint was used
        assert result.current_page == custom_endpoint
        assert result.processed_count == 0

    def test_import_collection_with_identifier_set(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """Test import_collection adds identifiers to identifier_set."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        mock_identifier_set = Mock(spec=IdentifierSet)
        api = overdrive_api_fixture.api
        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            identifier_set=mock_identifier_set,
            api=api,
        )

        mock_apply_bib = Mock()
        mock_apply_circ = Mock()
        modified_since = datetime_utc(2023, 1, 1)

        # Mock book data
        mock_book_data = [
            {
                "id": "overdrive-id-1",
                "metadata": {"title": "Test Book"},
                "availabilityV2": {"copiesOwned": 1},
            }
        ]
        api.async_requests.fetch_book_info_list = AsyncMock(
            return_value=(mock_book_data, None)
        )

        mock_bibliographic, mock_circulation = self._mock_extractor(
            importer,
            "overdrive-id-1",
        )

        # Run import
        importer.import_collection(
            apply_bibliographic=mock_apply_bib,
            apply_circulation=mock_apply_circ,
            modified_since=modified_since,
        )

        # Verify identifier was added to set
        mock_identifier_set.add.assert_called_once()

    def test_import_collection_skips_unchanged_metadata(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """Test import_collection skips unchanged bibliographic data."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        api = overdrive_api_fixture.api
        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=api,
        )

        mock_apply_bib = Mock()
        mock_apply_circ = Mock()
        modified_since = datetime_utc(2023, 1, 1)

        # Mock book data
        mock_book_data = [
            {
                "id": "overdrive-id-1",
                "metadata": {"title": "Unchanged Book"},
                "availabilityV2": {"copiesOwned": 1},
            }
        ]
        api.async_requests.fetch_book_info_list = AsyncMock(
            return_value=(mock_book_data, None)
        )

        mock_bibliographic, mock_circulation = self._mock_extractor(
            importer,
            "overdrive-id-1",
            bibliographic_changed=False,
        )

        # Run import
        importer.import_collection(
            apply_bibliographic=mock_apply_bib,
            apply_circulation=mock_apply_circ,
            modified_since=modified_since,
        )

        # Bibliographic should not be applied
        assert mock_apply_bib.call_count == 0
        # But circulation should still be applied
        assert mock_apply_circ.call_count == 1

    def test_import_collection_handles_missing_metadata(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """Test import_collection handles books with missing metadata."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        api = overdrive_api_fixture.api

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=api,
        )

        mock_apply_bib = Mock()
        mock_apply_circ = Mock()
        modified_since = datetime_utc(2023, 1, 1)

        # Mock book data without metadata
        mock_book_data = [
            {
                "id": "overdrive-id-1",
                "metadata": None,  # Missing metadata
                "availabilityV2": {"copiesOwned": 1},
            }
        ]

        api.async_requests.fetch_book_info_list = AsyncMock(
            return_value=(mock_book_data, None)
        )

        mock_circulation = Mock(spec=CirculationData)
        mock_circulation.needs_apply.return_value = True

        importer._extractor.book_info_to_circulation = Mock(
            return_value=mock_circulation
        )

        # Run import
        result = importer.import_collection(
            apply_bibliographic=mock_apply_bib,
            apply_circulation=mock_apply_circ,
            modified_since=modified_since,
        )

        # Bibliographic should not be called due to missing metadata
        assert mock_apply_bib.call_count == 0
        # But circulation should still be processed
        assert mock_apply_circ.call_count == 1
        assert result.processed_count == 1

    def test_import_collection_with_parent_identifiers_fetches_metadata_upfront(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """Test import_collection without parent identifiers fetches metadata upfront."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        api = overdrive_api_fixture.api

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=api,
            # No parent_identifier_set provided
        )

        mock_apply_bib = Mock()
        mock_apply_circ = Mock()
        modified_since = datetime_utc(2023, 1, 1)

        # Mock book data - metadata should be included since fetch_metadata=True
        mock_book_data = [
            {
                "id": "overdrive-id-1",
                "metadata": {"title": "Test Book"},
                "availabilityV2": {"copiesOwned": 1},
            }
        ]

        api.async_requests.fetch_book_info_list = AsyncMock(
            return_value=(mock_book_data, None)
        )
        api.metadata_lookup = Mock(
            return_value=MetadataResponse.model_validate({"title": "New Book"})
        )
        mock_bibliographic, mock_circulation = self._mock_extractor(
            importer,
            "overdrive-id-1",
        )

        # Run import
        result = importer.import_collection(
            apply_bibliographic=mock_apply_bib,
            apply_circulation=mock_apply_circ,
            modified_since=modified_since,
        )

        # Verify fetch_book_info_list was called with fetch_metadata=True
        api.async_requests.fetch_book_info_list.assert_called_once()
        call_kwargs = api.async_requests.fetch_book_info_list.call_args.kwargs
        assert call_kwargs["fetch_metadata"] is True
        assert call_kwargs["fetch_availability"] is True

        # Verify metadata_lookup was NOT called (metadata was fetched upfront)
        api.metadata_lookup.assert_not_called()

        assert result.processed_count == 1

    def test_import_collection_with_parent_identifiers_skips_metadata_for_known_identifiers(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """Test import_collection with parent identifiers skips fetching metadata for books in parent set."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        api = overdrive_api_fixture.api

        # Create a mock parent identifier set
        mock_parent_id1 = IdentifierData(
            type=Identifier.OVERDRIVE_ID, identifier="overdrive-id-1"
        )
        mock_parent_id2 = IdentifierData(
            type=Identifier.OVERDRIVE_ID, identifier="overdrive-id-2"
        )

        mock_parent_set = Mock(spec=IdentifierSet)
        mock_parent_set.get.return_value = {mock_parent_id1, mock_parent_id2}

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=api,
            parent_identifier_set=mock_parent_set,
        )

        mock_apply_bib = Mock()
        mock_apply_circ = Mock()
        modified_since = datetime_utc(2023, 1, 1)

        # Mock book data - one book in parent set, one not
        # Metadata should NOT be included since fetch_metadata=False with parent set
        mock_book_data = [
            {
                "id": "overdrive-id-1",  # In parent set
                "metadata": None,  # No metadata fetched upfront
                "availabilityV2": {"copiesOwned": 1},
            },
            {
                "id": "overdrive-id-3",  # NOT in parent set
                "metadata": None,  # No metadata fetched upfront
                "availabilityV2": {"copiesOwned": 1},
            },
        ]

        api.async_requests.fetch_book_info_list = AsyncMock(
            return_value=(mock_book_data, None)
        )

        # Mock metadata_lookup to return metadata for the book not in parent set
        api.metadata_lookup = Mock(
            return_value=MetadataResponse.model_validate({"title": "New Book"})
        )

        mock_bibliographic, mock_circulation = self._mock_extractor(
            importer,
            "overdrive-id-3",
        )

        # Run import
        result = importer.import_collection(
            apply_bibliographic=mock_apply_bib,
            apply_circulation=mock_apply_circ,
            modified_since=modified_since,
        )

        # Verify fetch_book_info_list was called with fetch_metadata=False
        api.async_requests.fetch_book_info_list.assert_called_once()
        call_kwargs = api.async_requests.fetch_book_info_list.call_args.kwargs
        assert call_kwargs["fetch_metadata"] is False
        assert call_kwargs["fetch_availability"] is True

        # Verify metadata_lookup was called once for the book NOT in parent set
        assert api.metadata_lookup.call_count == 1

        # One apply per book, never two. The book with metadata takes the
        # bibliographic branch, which carries its circulation data along; the book
        # already covered by the parent has no metadata, so it takes the circulation
        # branch on its own.
        assert mock_apply_bib.call_count == 1
        assert mock_apply_circ.call_count == 1

        assert result.processed_count == 2


class TestFeedImportResult:
    """Tests for the FeedImportResult dataclass."""

    def test_feed_import_result_basic(self) -> None:
        """Test FeedImportResult creation with basic data."""
        current_page = BookInfoEndpoint(url="http://current.page")
        result = FeedImportResult(current_page=current_page)

        assert result.current_page == current_page
        assert result.next_page is None
        assert result.processed_count == 0

    def test_feed_import_result_with_all_fields(self) -> None:
        """Test FeedImportResult with all fields populated."""
        current_page = BookInfoEndpoint(url="http://current.page")
        next_page = BookInfoEndpoint(url="http://next.page")

        result = FeedImportResult(
            current_page=current_page, next_page=next_page, processed_count=42
        )

        assert result.current_page == current_page
        assert result.next_page == next_page
        assert result.processed_count == 42

    def test_feed_import_result_frozen(self) -> None:
        """Test that FeedImportResult is immutable (frozen)."""
        current_page = BookInfoEndpoint(url="http://current.page")
        result = FeedImportResult(current_page=current_page)

        # Should raise an error when trying to modify
        with pytest.raises(AttributeError):
            # Assigning to a read-only property is exactly what this test is
            # checking raises, so mypy's complaint about it is the point.
            result.processed_count = 100  # type: ignore[misc]

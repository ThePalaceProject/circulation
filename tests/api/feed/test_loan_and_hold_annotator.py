from unittest.mock import MagicMock, patch

from api.app import app
from api.problem_details import NOT_FOUND_ON_REMOTE
from core.classifier import (  # type: ignore[attr-defined]
    Classifier,
    Fantasy,
    Urban_Fantasy,
)
from core.feed.acquisition import OPDSAcquisitionFeed
from core.feed.annotator.loan_and_hold import LibraryLoanAndHoldAnnotator
from core.feed.types import WorkEntry, WorkEntryData
from core.lane import WorkList
from core.model import ExternalIntegration, get_one
from core.model.constants import EditionConstants, LinkRelations
from core.model.licensing import LicensePool
from core.model.patron import Loan
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.search import ExternalSearchFixtureFake


class TestLibraryLoanAndHoldAnnotator:
    def test_single_item_feed(self, db: DatabaseTransactionFixture):
        # Test the generation of single-item OPDS feeds for loans (with and
        # without fulfillment) and holds.
        class MockAnnotator(LibraryLoanAndHoldAnnotator):
            def url_for(self, controller, **kwargs):
                self.url_for_called_with = (controller, kwargs)
                return "a URL"

        def mock_single_entry(work, annotator, *args, **kwargs):
            annotator._single_entry_response_called_with = (
                (work, annotator) + args,
                kwargs,
            )
            w = WorkEntry(
                work=work,
                license_pool=work.active_license_pool(),
                edition=work.presentation_edition,
                identifier=work.presentation_edition.primary_identifier,
            )
            w.computed = WorkEntryData()
            return w

        def test_annotator(item, fulfillment=None):
            # Call MockAnnotator.single_item_feed with certain arguments
            # and make some general assertions about the return value.
            circulation = object()
            test_mode = object()
            feed_class = object()
            annotator = MockAnnotator(MagicMock(), None, db.default_library())

            with patch.object(
                OPDSAcquisitionFeed, "single_entry", new=mock_single_entry
            ):
                result = OPDSAcquisitionFeed.single_entry_loans_feed(
                    MagicMock(),
                    item,
                    annotator,
                    fulfillment=fulfillment,
                )

            assert db.default_library() == annotator.library

            # Now let's see what we did with it after calling its
            # constructor.

            # The return value of that was the string "a URL". We then
            # passed that into _single_entry_response, along with
            # `item` and a number of arguments that we made up.
            response_call = annotator._single_entry_response_called_with
            (_work, _annotator), kwargs = response_call
            assert work == _work
            assert annotator == _annotator

            # Return the MockAnnotator for further examination.
            return annotator

        # Now we're going to call test_annotator a couple times in
        # different situations.
        work = db.work(with_license_pool=True)
        [pool] = work.license_pools
        patron = db.patron()
        loan, ignore = pool.loan_to(patron)

        # First, let's ask for a single-item feed for a loan.
        annotator = test_annotator(loan)

        # Everything tested by test_annotator happened, but _also_,
        # when the annotator was created, the Loan was stored in
        # active_loans_by_work.
        assert {work: loan} == annotator.active_loans_by_work

        # Since we passed in a loan rather than a hold,
        # active_holds_by_work is empty.
        assert {} == annotator.active_holds_by_work

        # Since we didn't pass in a fulfillment for the loan,
        # active_fulfillments_by_work is empty.
        assert {} == annotator.active_fulfillments_by_work

        # Now try it again, but give the loan a fulfillment.
        fulfillment = object()
        annotator = test_annotator(loan, fulfillment)
        assert {work: loan} == annotator.active_loans_by_work
        assert {work: fulfillment} == annotator.active_fulfillments_by_work

        # Finally, try it with a hold.
        hold, ignore = pool.on_hold_to(patron)
        annotator = test_annotator(hold)
        assert {work: hold} == annotator.active_holds_by_work
        assert {} == annotator.active_loans_by_work
        assert {} == annotator.active_fulfillments_by_work

    def test_single_item_feed_without_work(self, db: DatabaseTransactionFixture):
        """If a licensepool has no work or edition the single_item_feed mustn't raise an exception"""
        mock = MagicMock()
        # A loan without a pool
        annotator = LibraryLoanAndHoldAnnotator(mock, None, db.default_library())
        loan = Loan()
        loan.patron = db.patron()
        not_found_result = OPDSAcquisitionFeed.single_entry_loans_feed(
            mock,
            loan,
            annotator,
        )
        assert not_found_result == NOT_FOUND_ON_REMOTE

        work = db.work(with_license_pool=True)
        pool = get_one(db.session, LicensePool, work_id=work.id)
        assert isinstance(pool, LicensePool)
        # Pool with no work, and the presentation edition has no work either
        pool.work_id = None
        work.presentation_edition_id = None
        db.session.commit()
        assert (
            OPDSAcquisitionFeed.single_entry_loans_feed(
                mock,
                pool,
                annotator,
            )
            == NOT_FOUND_ON_REMOTE
        )

        # pool with no work and no presentation edition
        pool.presentation_edition_id = None
        db.session.commit()
        assert (
            OPDSAcquisitionFeed.single_entry_loans_feed(
                mock,
                pool,
                annotator,
            )
            == NOT_FOUND_ON_REMOTE
        )

    def test_choose_best_hold_for_work(self, db: DatabaseTransactionFixture):
        # First create two license pools for the same work so we could create two holds for the same work.
        patron = db.patron()

        coll_1 = db.collection(name="Collection 1")
        coll_2 = db.collection(name="Collection 2")

        work = db.work()

        pool_1 = db.licensepool(
            edition=work.presentation_edition, open_access=False, collection=coll_1
        )
        pool_2 = db.licensepool(
            edition=work.presentation_edition, open_access=False, collection=coll_2
        )

        hold_1, _ = pool_1.on_hold_to(patron)
        hold_2, _ = pool_2.on_hold_to(patron)

        # When there is no licenses_owned/available on one license pool the LibraryLoanAndHoldAnnotator should choose
        # hold associated with the other license pool.
        pool_1.licenses_owned = 0
        pool_1.licenses_available = 0

        assert hold_2 == LibraryLoanAndHoldAnnotator.choose_best_hold_for_work(
            [hold_1, hold_2]
        )

        # Now we have different number of licenses owned across two LPs and the same hold position.
        # Hold associated with LP with more owned licenses will be chosen as best.
        pool_1.licenses_owned = 2

        pool_2.licenses_owned = 3
        pool_2.licenses_available = 0

        hold_1.position = 7
        hold_2.position = 7

        assert hold_2 == LibraryLoanAndHoldAnnotator.choose_best_hold_for_work(
            [hold_1, hold_2]
        )

    def test_annotate_work_entry(
        self,
        db: DatabaseTransactionFixture,
        external_search_fake_fixture: ExternalSearchFixtureFake,
    ):
        library = db.default_library()
        patron = db.patron()
        identifier = db.identifier()
        lane = WorkList()
        lane.initialize(
            library,
        )
        annotator = LibraryLoanAndHoldAnnotator(None, lane, library, patron)
        feed = OPDSAcquisitionFeed("title", "url", [], annotator)

        # Annotate time tracking
        opds_for_distributors = db.collection(
            protocol=ExternalIntegration.OPDS_FOR_DISTRIBUTORS
        )
        work = db.work(with_license_pool=True, collection=opds_for_distributors)
        work.active_license_pool().should_track_playtime = True
        edition = work.presentation_edition
        edition.medium = EditionConstants.AUDIO_MEDIUM
        edition.primary_identifier = identifier
        loan, _ = work.active_license_pool().loan_to(patron)
        annotator.active_loans_by_work = {work: loan}

        with app.test_request_context("/") as request:
            request.library = library  # type: ignore [attr-defined]
            entry = feed.single_entry(work, annotator)
            assert isinstance(entry, WorkEntry)
            assert entry and entry.computed is not None
            time_tracking_links = list(
                filter(
                    lambda l: l.rel == LinkRelations.TIME_TRACKING,
                    entry.computed.other_links,
                )
            )
            assert len(time_tracking_links) == 1
            assert time_tracking_links[0].href == annotator.url_for(
                "track_playtime_events",
                identifier_type=identifier.type,
                identifier=identifier.identifier,
                library_short_name=annotator.library.short_name,
                collection_id=opds_for_distributors.id,
                _external=True,
            )

            # No active loan means no tracking link
            annotator.active_loans_by_work = {}
            entry = feed.single_entry(work, annotator)
            assert isinstance(entry, WorkEntry)
            assert entry and entry.computed is not None

            time_tracking_links = list(
                filter(
                    lambda l: l.rel == LinkRelations.TIME_TRACKING,
                    entry.computed.other_links,
                )
            )
            assert len(time_tracking_links) == 0

            # Add the loan back in
            annotator.active_loans_by_work = {work: loan}

            # Book mediums don't get time tracking
            edition.medium = EditionConstants.BOOK_MEDIUM
            entry = feed.single_entry(work, annotator)
            assert isinstance(entry, WorkEntry)
            assert entry and entry.computed is not None

            time_tracking_links = list(
                filter(
                    lambda l: l.rel == LinkRelations.TIME_TRACKING,
                    entry.computed.other_links,
                )
            )
            assert len(time_tracking_links) == 0

            # Non OPDS for distributor works do not get links either
            work = db.work(with_license_pool=True)
            edition = work.presentation_edition
            edition.medium = EditionConstants.AUDIO_MEDIUM

            entry = feed.single_entry(work, annotator)
            assert isinstance(entry, WorkEntry)
            assert entry and entry.computed is not None

            time_tracking_links = list(
                filter(
                    lambda l: l.rel == LinkRelations.TIME_TRACKING,
                    entry.computed.other_links,
                )
            )
            assert len(time_tracking_links) == 0

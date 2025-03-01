import datetime
import random

from palace.manager.api.monitor import (
    HoldReaper,
    IdlingAnnotationReaper,
    LoanlikeReaperMonitor,
    LoanReaper,
)
from palace.manager.api.opds_for_distributors import OPDSForDistributorsAPI
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.patron import Annotation
from palace.manager.sqlalchemy.util import get_one_or_create
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


class TestLoanlikeReaperMonitor:
    """Tests the loan and hold reapers."""

    def test_source_of_truth_protocols(self):
        """Verify that well-known source of truth protocols
        will be exempt from the reaper.
        """
        assert LoanlikeReaperMonitor.SOURCE_OF_TRUTH_PROTOCOLS == [
            OPDSForDistributorsAPI.label()
        ]

    def test_reaping(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ):
        # This patron stopped using the circulation manager a long time
        # ago.
        inactive_patron = db.patron()

        # This patron is still using the circulation manager.
        current_patron = db.patron()

        # We're going to give these patrons some loans and holds.
        edition, open_access = db.edition(
            with_license_pool=True, with_open_access_download=True
        )

        not_open_access_1 = db.licensepool(
            edition, open_access=False, data_source_name=DataSource.OVERDRIVE
        )
        not_open_access_2 = db.licensepool(
            edition, open_access=False, data_source_name=DataSource.BIBLIOTHECA
        )
        not_open_access_3 = db.licensepool(
            edition, open_access=False, data_source_name=DataSource.AXIS_360
        )
        not_open_access_4 = db.licensepool(
            edition, open_access=False, data_source_name=DataSource.BIBBLIO
        )
        unlimited_access = db.licensepool(
            edition, unlimited_access=True, data_source_name=DataSource.AMAZON
        )

        # Here's a collection that is the source of truth for its
        # loans and holds, rather than mirroring loan and hold information
        # from some remote source.
        sot_collection = db.collection(
            "Source of Truth",
            protocol=random.choice(LoanReaper.SOURCE_OF_TRUTH_PROTOCOLS),
        )

        edition2 = db.edition(with_license_pool=False)

        sot_lp1 = db.licensepool(
            edition2,
            open_access=False,
            data_source_name=DataSource.OVERDRIVE,
            collection=sot_collection,
        )

        sot_lp2 = db.licensepool(
            edition2,
            open_access=False,
            data_source_name=DataSource.BIBLIOTHECA,
            collection=sot_collection,
        )

        now = utc_now()
        a_long_time_ago = now - datetime.timedelta(days=1000)
        not_very_long_ago = now - datetime.timedelta(days=60)
        even_longer = now - datetime.timedelta(days=2000)
        the_future = now + datetime.timedelta(days=1)

        # This loan has expired.
        not_open_access_1.loan_to(
            inactive_patron, start=even_longer, end=a_long_time_ago
        )

        # This hold expired without ever becoming a loan (that we saw).
        not_open_access_2.on_hold_to(
            inactive_patron, start=even_longer, end=a_long_time_ago
        )

        # This hold has no end date and is older than a year.
        not_open_access_3.on_hold_to(
            inactive_patron,
            start=a_long_time_ago,
            end=None,
        )

        # This loan has no end date and is older than 90 days.
        not_open_access_4.loan_to(
            inactive_patron,
            start=a_long_time_ago,
            end=None,
        )

        # This loan has no end date, but it's for an open-access work.
        open_access_loan, ignore = open_access.loan_to(
            inactive_patron,
            start=a_long_time_ago,
            end=None,
        )

        # An unlimited loan should not get reaped regardless of age
        unlimited_access_loan, ignore = unlimited_access.loan_to(
            inactive_patron, start=a_long_time_ago, end=None
        )

        # This loan has not expired yet.
        not_open_access_1.loan_to(current_patron, start=now, end=the_future)

        # This hold has not expired yet.
        not_open_access_2.on_hold_to(current_patron, start=now, end=the_future)

        # This loan has no end date but is pretty recent.
        not_open_access_3.loan_to(current_patron, start=not_very_long_ago, end=None)

        # This hold has no end date but is pretty recent.
        not_open_access_4.on_hold_to(current_patron, start=not_very_long_ago, end=None)

        # Reapers will not touch loans or holds from the
        # source-of-truth collection, even ones that have 'obviously'
        # expired.
        sot_loan, ignore = sot_lp1.loan_to(
            inactive_patron, start=a_long_time_ago, end=a_long_time_ago
        )

        sot_hold, ignore = sot_lp2.on_hold_to(
            inactive_patron, start=a_long_time_ago, end=a_long_time_ago
        )

        assert 5 == len(inactive_patron.loans)
        assert 3 == len(inactive_patron.holds)

        assert 2 == len(current_patron.loans)
        assert 2 == len(current_patron.holds)

        # Now we fire up the loan reaper.
        monitor = LoanReaper(db.session)
        monitor.services.analytics = services_fixture.analytics_fixture.analytics_mock
        monitor.run()

        # All of the inactive patron's loans have been reaped,
        # except for the loans for which the circulation manager is the
        # source of truth (the SOT loan and the open-access loan),
        # which will never be reaped.
        #
        # Holds are unaffected.
        assert {open_access_loan, sot_loan, unlimited_access_loan} == set(
            inactive_patron.loans
        )
        assert len(inactive_patron.holds) == 3

        # The active patron's loans and holds are unaffected, either
        # because they have not expired or because they have no known
        # expiration date and were created relatively recently.
        assert len(current_patron.loans) == 2
        assert len(current_patron.holds) == 2

        # Now fire up the hold reaper.
        hold_monitor = HoldReaper(db.session)
        hold_monitor.services.analytics = (
            services_fixture.analytics_fixture.analytics_mock
        )
        hold_monitor.run()

        # All of the inactive patron's holds have been reaped,
        # except for the one from the source-of-truth collection.
        # The active patron is unaffected.
        assert [sot_hold] == inactive_patron.holds
        assert 2 == len(current_patron.holds)

        # verify expected circ event count for hold reaper run
        call_args_list = (
            services_fixture.analytics_fixture.analytics_mock.collect_event.call_args_list
        )
        assert len(call_args_list) == 2
        event_types = [call_args.kwargs["event_type"] for call_args in call_args_list]
        assert event_types == [
            CirculationEvent.CM_HOLD_EXPIRED,
            CirculationEvent.CM_HOLD_EXPIRED,
        ]


class TestIdlingAnnotationReaper:
    def test_where_clause(self, db: DatabaseTransactionFixture):
        # Two books.
        ignore, lp1 = db.edition(with_license_pool=True)
        ignore, lp2 = db.edition(with_license_pool=True)

        # Two patrons who sync their annotations.
        p1 = db.patron()
        p2 = db.patron()
        for p in [p1, p2]:
            p.synchronize_annotations = True
        now = utc_now()
        not_that_old = now - datetime.timedelta(days=59)
        very_old = now - datetime.timedelta(days=61)

        def _annotation(
            patron, pool, content, motivation=Annotation.IDLING, timestamp=very_old
        ):
            annotation, _ = get_one_or_create(
                db.session,
                Annotation,
                patron=patron,
                identifier=pool.identifier,
                motivation=motivation,
            )
            annotation.timestamp = timestamp
            annotation.content = content
            return annotation

        # The first patron will not be affected by the
        # reaper. Although their annotations are very old, they have
        # an active loan for one book and a hold on the other.
        loan = lp1.loan_to(p1)
        old_loan = _annotation(p1, lp1, "old loan")

        hold = lp2.on_hold_to(p1)
        old_hold = _annotation(p1, lp2, "old hold")

        # The second patron has a very old annotation for the first
        # book. This is the only annotation that will be affected by
        # the reaper.
        reapable = _annotation(p2, lp1, "abandoned")

        # The second patron also has a very old non-idling annotation
        # for the first book, which will not be reaped because only
        # idling annotations are reaped.
        not_idling = _annotation(
            p2, lp1, "not idling", motivation="some other motivation"
        )

        # The second patron has a non-old idling annotation for the
        # second book, which will not be reaped (even though there is
        # no active loan or hold) because it's not old enough.
        new_idling = _annotation(p2, lp2, "recent", timestamp=not_that_old)
        reaper = IdlingAnnotationReaper(db.session)
        qu = db.session.query(Annotation).filter(reaper.where_clause)
        assert [reapable] == qu.all()

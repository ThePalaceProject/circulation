import datetime
from unittest.mock import MagicMock, call

import pytest

from core.classifier import Classifier
from core.model import create, tuple_to_numericrange
from core.model.constants import LinkRelations
from core.model.credential import Credential
from core.model.datasource import DataSource
from core.model.licensing import PolicyException
from core.model.patron import Annotation, Hold, Loan, Patron, PatronProfileStorage
from core.util.datetime_helpers import datetime_utc, utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture


class TestAnnotation:
    def test_set_inactive(self, db: DatabaseTransactionFixture):
        pool = db.licensepool(None)
        annotation, ignore = create(
            db.session,
            Annotation,
            patron=db.patron(),
            identifier=pool.identifier,
            motivation=Annotation.IDLING,
            content="The content",
            active=True,
        )
        yesterday = utc_now() - datetime.timedelta(days=1)
        annotation.timestamp = yesterday

        annotation.set_inactive()
        assert False == annotation.active
        assert None == annotation.content
        assert annotation.timestamp > yesterday

    def test_patron_annotations_are_descending(self, db: DatabaseTransactionFixture):
        pool1 = db.licensepool(None)
        pool2 = db.licensepool(None)
        patron = db.patron()
        annotation1, ignore = create(
            db.session,
            Annotation,
            patron=patron,
            identifier=pool2.identifier,
            motivation=Annotation.IDLING,
            content="The content",
            active=True,
        )
        annotation2, ignore = create(
            db.session,
            Annotation,
            patron=patron,
            identifier=pool2.identifier,
            motivation=Annotation.IDLING,
            content="The content",
            active=True,
        )

        yesterday = utc_now() - datetime.timedelta(days=1)
        today = utc_now()
        annotation1.timestamp = yesterday
        annotation2.timestamp = today

        assert 2 == len(patron.annotations)
        assert annotation2 == patron.annotations[0]
        assert annotation1 == patron.annotations[1]


class TestHold:
    def test_on_hold_to(self, db: DatabaseTransactionFixture):
        now = utc_now()
        later = now + datetime.timedelta(days=1)
        patron = db.patron()
        edition = db.edition()
        pool = db.licensepool(edition)
        hold, is_new = pool.on_hold_to(patron, now, later, 4)
        assert True == is_new
        assert now == hold.start
        assert later == hold.end
        assert 4 == hold.position

        # Now update the position to 0. It's the patron's turn
        # to check out the book.
        hold, is_new = pool.on_hold_to(patron, now, later, 0)
        assert False == is_new
        assert now == hold.start
        # The patron has until `hold.end` to actually check out the book.
        assert later == hold.end
        assert 0 == hold.position

    def test_holds_not_allowed(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        settings = library_fixture.mock_settings()
        settings.allow_holds = False
        library = library_fixture.library(settings=settings)
        patron = db.patron(library=library)
        edition = db.edition()
        pool = db.licensepool(edition)
        with pytest.raises(PolicyException) as excinfo:
            pool.on_hold_to(patron, utc_now(), 4)
        assert "Holds are disabled for this library." in str(excinfo.value)

    def test_work(self, db: DatabaseTransactionFixture):
        # We don't need to test the functionality--that's tested in
        # Loan--just that Hold also has access to .work.
        patron = db.patron()
        work = db.work(with_license_pool=True)
        pool = work.license_pools[0]
        hold, is_new = pool.on_hold_to(patron)
        assert work == hold.work

    def test_until(self, db: DatabaseTransactionFixture):
        one_day = datetime.timedelta(days=1)
        two_days = datetime.timedelta(days=2)

        now = utc_now()
        the_past = now - datetime.timedelta(seconds=1)
        the_future = now + two_days

        patron = db.patron()
        pool = db.licensepool(None)
        pool.patrons_in_hold_queue = 100
        hold, ignore = pool.on_hold_to(patron)
        hold.position = 10

        m = hold.until

        # If the value in Hold.end is in the future, it's used, no
        # questions asked.
        hold.end = the_future
        assert the_future == m(object(), object())

        # If Hold.end is not specified, or is in the past, it's more
        # complicated.

        # If no default_loan_period or default_reservation_period is
        # specified, a Hold has no particular end date.
        hold.end = the_past
        assert None == m(None, one_day)
        assert None == m(one_day, None)

        hold.end = None
        assert None == m(None, one_day)
        assert None == m(one_day, None)

        # Otherwise, the answer is determined by _calculate_until.
        def _mock__calculate_until(self, *args):
            """Track the arguments passed into _calculate_until."""
            self.called_with = args
            return "mock until"

        old__calculate_until = hold._calculate_until
        Hold._calculate_until = _mock__calculate_until

        assert "mock until" == m(one_day, two_days)

        (
            calculate_from,
            position,
            licenses_available,
            default_loan_period,
            default_reservation_period,
        ) = hold.called_with

        assert (calculate_from - now).total_seconds() < 5
        assert hold.position == position
        assert pool.licenses_available == licenses_available
        assert one_day == default_loan_period
        assert two_days == default_reservation_period

        # If we don't know the patron's position in the hold queue, we
        # assume they're at the end.
        hold.position = None
        assert "mock until" == m(one_day, two_days)
        (
            calculate_from,
            position,
            licenses_available,
            default_loan_period,
            default_reservation_period,
        ) = hold.called_with
        assert pool.patrons_in_hold_queue == position

        Hold._calculate_until = old__calculate_until

    def test_calculate_until(self):
        start = datetime_utc(2010, 1, 1)

        # The cycle time is one week.
        default_loan = datetime.timedelta(days=6)
        default_reservation = datetime.timedelta(days=1)

        # I'm 20th in line for 4 books.
        #
        # After 7 days, four copies are released and I am 16th in line.
        # After 14 days, those copies are released and I am 12th in line.
        # After 21 days, those copies are released and I am 8th in line.
        # After 28 days, those copies are released and I am 4th in line.
        # After 35 days, those copies are released and get my notification.
        a = Hold._calculate_until(start, 20, 4, default_loan, default_reservation)
        assert a == start + datetime.timedelta(days=(7 * 5))

        # If I am 21st in line, I need to wait six weeks.
        b = Hold._calculate_until(start, 21, 4, default_loan, default_reservation)
        assert b == start + datetime.timedelta(days=(7 * 6))

        # If I am 3rd in line, I only need to wait seven days--that's when
        # I'll get the notification message.
        b = Hold._calculate_until(start, 3, 4, default_loan, default_reservation)
        assert b == start + datetime.timedelta(days=7)

        # A new person gets the book every week. Someone has the book now
        # and there are 3 people ahead of me in the queue. I will get
        # the book in 7 days + 3 weeks
        c = Hold._calculate_until(start, 3, 1, default_loan, default_reservation)
        assert c == start + datetime.timedelta(days=(7 * 4))

        # I'm first in line for 1 book. After 7 days, one copy is
        # released and I'll get my notification.
        a = Hold._calculate_until(start, 1, 1, default_loan, default_reservation)
        assert a == start + datetime.timedelta(days=7)

        # The book is reserved to me. I need to hurry up and check it out.
        d = Hold._calculate_until(start, 0, 1, default_loan, default_reservation)
        assert d == start + datetime.timedelta(days=1)

        # If there are no licenses, I will never get the book.
        e = Hold._calculate_until(start, 10, 0, default_loan, default_reservation)
        assert e == None

    def test_vendor_hold_end_value_takes_precedence_over_calculated_value(
        self, db: DatabaseTransactionFixture
    ):
        """If the vendor has provided an estimated availability time,
        that is used in preference to the availability time we
        calculate.
        """
        now = utc_now()
        tomorrow = now + datetime.timedelta(days=1)

        patron = db.patron()
        pool = db.licensepool(edition=None)
        hold, is_new = pool.on_hold_to(patron)
        hold.position = 1
        hold.end = tomorrow

        default_loan = datetime.timedelta(days=1)
        default_reservation = datetime.timedelta(days=2)
        assert tomorrow == hold.until(default_loan, default_reservation)

        calculated_value = hold._calculate_until(
            now,
            hold.position,
            pool.licenses_available,
            default_loan,
            default_reservation,
        )

        # If the vendor value is not in the future, it's ignored
        # and the calculated value is used instead.
        def assert_calculated_value_used():
            result = hold.until(default_loan, default_reservation)
            assert (result - calculated_value).seconds < 5

        hold.end = now
        assert_calculated_value_used()

        # The calculated value is also used there is no
        # vendor-provided value.
        hold.end = None
        assert_calculated_value_used()


class TestLoans:
    def test_open_access_loan(self, db: DatabaseTransactionFixture):
        patron = db.patron()
        work = db.work(with_license_pool=True)
        pool = work.license_pools[0]
        pool.is_open_access = True

        # The patron has no active loans.
        assert [] == patron.loans

        # Loan them the book
        fulfillment = pool.delivery_mechanisms[0]
        loan, was_new = pool.loan_to(patron, fulfillment=fulfillment)

        # Now they have a loan!
        assert [loan] == patron.loans
        assert loan.patron == patron
        assert loan.license_pool == pool
        assert fulfillment == loan.fulfillment
        assert (utc_now() - loan.start) < datetime.timedelta(seconds=1)

        # TODO: At some future point it may be relevant that loan.end
        # is None here, but before that happens the loan process will
        # become more complicated, so there's no point in writing
        # a bunch of test code now.

        # Try getting another loan for this book.
        loan2, was_new = pool.loan_to(patron)

        # They're the same!
        assert loan == loan2
        assert False == was_new

    def test_work(self, db: DatabaseTransactionFixture):
        """Test the attribute that finds the Work for a Loan or Hold."""
        patron = db.patron()
        work = db.work(with_license_pool=True)
        pool = work.license_pools[0]

        # The easy cases.
        loan, is_new = pool.loan_to(patron)
        assert work == loan.work

        loan.license_pool = None
        assert None == loan.work

        # If pool.work is None but pool.edition.work is valid, we use that.
        loan.license_pool = pool
        pool.work = None
        # Presentation_edition is not representing a lendable object,
        # but it is on a license pool, and a pool has lending capacity.
        assert pool.presentation_edition.work == loan.work

        # If that's also None, we're helpless.
        pool.presentation_edition.work = None
        assert None == loan.work

    def test_library(self, db: DatabaseTransactionFixture):
        patron = db.patron()
        work = db.work(with_license_pool=True)
        pool = work.license_pools[0]

        loan, is_new = pool.loan_to(patron)
        assert db.default_library() == loan.library

        loan.patron = None
        assert None == loan.library

        patron.library = db.library()
        loan.patron = patron
        assert patron.library == loan.library


class TestPatron:
    def test_repr(self, db: DatabaseTransactionFixture):
        patron = db.patron(external_identifier="a patron")

        patron.authorization_expires = datetime_utc(2018, 1, 2, 3, 4, 5)
        patron.last_external_sync = None
        assert (
            "<Patron authentication_identifier=None expires=2018-01-02 sync=None>"
            == repr(patron)
        )

    def test_identifier_to_remote_service(self, db: DatabaseTransactionFixture):
        # Here's a patron.
        patron = db.patron()

        # Get identifiers to use when identifying that patron on two
        # different remote services.
        axis = DataSource.AXIS_360
        axis_identifier = patron.identifier_to_remote_service(axis)

        feedbooks = DataSource.lookup(db.session, DataSource.FEEDBOOKS)
        feedbooks_identifier = patron.identifier_to_remote_service(feedbooks)

        # The identifiers are different.
        assert axis_identifier != feedbooks_identifier

        # But they're both 36-character UUIDs.
        assert 36 == len(axis_identifier)
        assert 36 == len(feedbooks_identifier)

        # They're persistent.
        assert feedbooks_identifier == patron.identifier_to_remote_service(feedbooks)
        assert axis_identifier == patron.identifier_to_remote_service(axis)

        # You can customize the function used to generate the
        # identifier, in case the data source won't accept a UUID as a
        # patron identifier.
        def fake_generator():
            return "fake string"

        bib = DataSource.BIBLIOTHECA
        assert "fake string" == patron.identifier_to_remote_service(bib, fake_generator)

        # Once the identifier is created, specifying a different generator
        # does nothing.
        assert "fake string" == patron.identifier_to_remote_service(bib)
        assert axis_identifier == patron.identifier_to_remote_service(
            axis, fake_generator
        )

    def test_set_synchronize_annotations(self, db: DatabaseTransactionFixture):
        # Two patrons.
        p1 = db.patron()
        p2 = db.patron()

        identifier = db.identifier()

        for patron in [p1, p2]:
            # Each patron decides they want to synchronize annotations
            # to a library server.
            assert None == patron.synchronize_annotations
            patron.synchronize_annotations = True

            # Each patron gets one annotation.
            annotation, ignore = Annotation.get_one_or_create(
                db.session,
                patron=patron,
                identifier=identifier,
                motivation=Annotation.IDLING,
            )
            annotation.content = ("The content for %s" % patron.id,)

            assert 1 == len(patron.annotations)

        # Patron #1 decides they don't want their annotations stored
        # on a library server after all. This deletes their
        # annotation.
        p1.synchronize_annotations = False
        db.session.commit()
        assert 0 == len(p1.annotations)

        # But patron #2 can use Annotation.get_one_or_create.
        i2, is_new = Annotation.get_one_or_create(
            db.session,
            patron=p2,
            identifier=db.identifier(),
            motivation=Annotation.IDLING,
        )
        assert True == is_new

        # Once you make a decision, you can change your mind, but you
        # can't go back to not having made the decision.
        def try_to_set_none(patron):
            patron.synchronize_annotations = None

        pytest.raises(ValueError, try_to_set_none, p2)

    def test_cascade_delete(self, db: DatabaseTransactionFixture):
        # Create a patron and check that it has  been created
        patron = db.patron()
        assert len(db.session.query(Patron).all()) == 1

        # Give the patron a loan, and check that it has been created
        work_for_loan = db.work(with_license_pool=True)
        pool = work_for_loan.license_pools[0]
        loan, is_new = pool.loan_to(patron)
        assert [loan] == patron.loans
        assert len(db.session.query(Loan).all()) == 1

        # Give the patron a hold and check that it has been created
        work_for_hold = db.work(with_license_pool=True)
        pool = work_for_hold.license_pools[0]
        hold, is_new = pool.on_hold_to(patron)
        assert [hold] == patron.holds
        assert len(db.session.query(Hold).all()) == 1

        # Give the patron an annotation and check that it has been created
        annotation, is_new = create(db.session, Annotation, patron=patron)
        assert [annotation] == patron.annotations
        assert len(db.session.query(Annotation).all()) == 1

        # Give the patron a credential and check that it has been created
        credential, is_new = create(db.session, Credential, patron=patron)
        assert [credential] == patron.credentials
        assert len(db.session.query(Credential).all()) == 1

        # Delete the patron and check that it has been deleted
        db.session.delete(patron)
        assert len(db.session.query(Patron).all()) == 0

        # The patron's loan, hold, annotation, and credential should also be gone
        assert db.session.query(Loan).all() == []
        assert db.session.query(Hold).all() == []
        assert db.session.query(Annotation).all() == []
        assert db.session.query(Credential).all() == []

    def test_loan_activity_max_age(self, db: DatabaseTransactionFixture):
        # Currently, patron.loan_activity_max_age is a constant
        # and cannot be changed.
        assert 15 * 60 == db.patron().loan_activity_max_age

    def test_last_loan_activity_sync(self, db: DatabaseTransactionFixture):
        # Verify that last_loan_activity_sync is cleared out
        # beyond a certain point.
        patron = db.patron()
        now = utc_now()
        max_age = patron.loan_activity_max_age
        recently = now - datetime.timedelta(seconds=max_age / 2)
        long_ago = now - datetime.timedelta(seconds=max_age * 2)

        # So long as last_loan_activity_sync is relatively recent,
        # it's treated as a normal piece of data.
        patron.last_loan_activity_sync = recently
        assert recently == patron._last_loan_activity_sync
        assert recently == patron.last_loan_activity_sync

        # If it's _not_ relatively recent, attempting to access it
        # doesn't clear it out, but accessor returns None
        patron.last_loan_activity_sync = long_ago
        assert long_ago == patron._last_loan_activity_sync
        assert None == patron.last_loan_activity_sync
        assert long_ago == patron._last_loan_activity_sync

    def test_root_lane(self, db: DatabaseTransactionFixture):
        root_1 = db.lane()
        root_2 = db.lane()

        # If a library has no root lanes, its patrons have no root
        # lanes.
        patron = db.patron()
        patron.external_type = "x"
        assert None == patron.root_lane

        # Patrons of external type '1' and '2' have a certain root lane.
        root_1.root_for_patron_type = ["1", "2"]

        # Patrons of external type '3' have a different root.
        root_2.root_for_patron_type = ["3"]

        # Flush the database to clear the Library._has_root_lane_cache.
        db.session.flush()

        # A patron with no external type has no root lane.
        assert None == patron.root_lane

        # If a patron's external type associates them with a specific lane, that
        # lane is their root lane.
        patron.external_type = "1"
        assert root_1 == patron.root_lane

        patron.external_type = "2"
        assert root_1 == patron.root_lane

        patron.external_type = "3"
        assert root_2 == patron.root_lane

        # This shouldn't happen, but if two different lanes are the
        # root lane for a single patron type, the one with the lowest
        # database ID is chosen.  This way we avoid denying service to
        # a patron based on a server misconfiguration.
        root_1.root_for_patron_type = ["1", "2", "3"]
        assert root_1 == patron.root_lane

    def test_work_is_age_appropriate(self, db: DatabaseTransactionFixture):
        # The target audience and age of a patron's root lane controls
        # whether a given book is 'age-appropriate' for them.
        lane = db.lane()
        lane.audiences = [Classifier.AUDIENCE_CHILDREN, Classifier.AUDIENCE_YOUNG_ADULT]
        lane.target_age = (9, 14)
        lane.root_for_patron_type = ["1"]
        db.session.flush()

        def mock_age_appropriate(
            work_audience, work_target_age, reader_audience, reader_target_age
        ):
            """Returns True only if reader_audience is the preconfigured
            expected value.
            """
            if reader_audience == self.return_true_for:
                return True
            return False

        patron = db.patron()
        mock = MagicMock(side_effect=mock_age_appropriate)
        patron.age_appropriate_match = mock
        self.calls: list = []
        self.return_true_for = None

        # If the patron has no root lane, age_appropriate_match is not
        # even called -- all works are age-appropriate.
        m = patron.work_is_age_appropriate
        work_audience = object()
        work_target_age = object()
        assert True == m(work_audience, work_target_age)
        assert 0 == mock.call_count

        # Give the patron a root lane and try again.
        patron.external_type = "1"
        assert False == m(work_audience, work_target_age)

        # age_appropriate_match method was called on
        # each audience associated with the patron's root lane.
        mock.assert_has_calls(
            [
                call(
                    work_audience,
                    work_target_age,
                    Classifier.AUDIENCE_CHILDREN,
                    lane.target_age,
                ),
                call(
                    work_audience,
                    work_target_age,
                    Classifier.AUDIENCE_YOUNG_ADULT,
                    lane.target_age,
                ),
            ]
        )

        # work_is_age_appropriate() will only return True if at least
        # one of the age_appropriate_match() calls returns True.
        #
        # Simulate this by telling our mock age_appropriate_match() to
        # return True only when passed a specific reader audience. Our
        # Mock lane has two audiences, and at most one can match.
        self.return_true_for = Classifier.AUDIENCE_CHILDREN
        assert True == m(work_audience, work_target_age)

        self.return_true_for = Classifier.AUDIENCE_YOUNG_ADULT
        assert True == m(work_audience, work_target_age)

        self.return_true_for = Classifier.AUDIENCE_ADULT
        assert False == m(work_audience, work_target_age)

    def test_age_appropriate_match(self):
        # Check whether there's any overlap between a work's target age
        # and a reader's age.
        m = Patron.age_appropriate_match

        ya = Classifier.AUDIENCE_YOUNG_ADULT
        children = Classifier.AUDIENCE_CHILDREN
        adult = Classifier.AUDIENCE_ADULT
        all_ages = Classifier.AUDIENCE_ALL_AGES

        # A reader with no particular audience can see everything.
        assert True == m(object(), object(), None, object())

        # A reader associated with a non-juvenile audience, such as
        # AUDIENCE_ADULT, can see everything.
        for reader_audience in Classifier.AUDIENCES:
            if reader_audience in Classifier.AUDIENCES_JUVENILE:
                # Tested later.
                continue
            assert True == m(object(), object(), reader_audience, object())

        # Everyone can see 'all-ages' books.
        for reader_audience in Classifier.AUDIENCES:
            assert True == m(all_ages, object(), reader_audience, object())

        # Children cannot see YA or adult books.
        for work_audience in (ya, adult):
            assert False == m(work_audience, object(), children, None)

            # This is true even if the "child's" target age is set to
            # a value that would allow for this (as can happen when
            # the patron's root lane is set up to show both children's
            # and YA titles).
            assert False == m(work_audience, object(), children, (14, 18))

        # YA readers can see any children's title.
        assert True == m(children, object(), ya, object())

        # A YA reader is treated as an adult (with no reading
        # restrictions) if they have no associated age range, or their
        # age range includes ADULT_AGE_CUTOFF.
        for reader_age in [None, 18, (14, 18), tuple_to_numericrange((14, 18))]:
            assert True == m(adult, object(), ya, reader_age)

        # Otherwise, YA readers cannot see books for adults.
        for reader_age in [16, (14, 17)]:
            assert False == m(adult, object(), ya, reader_age)

        # Now let's consider the most complicated cases. First, a
        # child who wants to read a children's book.
        work_audience = children
        for reader_audience in Classifier.AUDIENCES_YOUNG_CHILDREN:
            # If the work has no target age, it's fine (or at least
            # we don't have the information necessary to say it's not
            # fine).
            work_target_age = None
            assert True == m(work_audience, work_target_age, reader_audience, object())

            # Now give the work a specific target age range.
            for work_target_age in [(5, 7), tuple_to_numericrange((5, 7))]:
                # The lower end of the age range is old enough.
                for age in range(5, 9):
                    for reader_age in (
                        age,
                        (age - 1, age),
                        tuple_to_numericrange((age - 1, age)),
                    ):
                        assert True == m(
                            work_audience, work_target_age, reader_audience, reader_age
                        )

                # Anything lower than that is not.
                for age in range(2, 5):
                    for reader_age in (
                        age,
                        (age - 1, age),
                        tuple_to_numericrange((age - 1, age)),
                    ):
                        assert False == m(
                            work_audience, work_target_age, reader_audience, reader_age
                        )

        # Similar rules apply for a YA reader who wants to read a YA
        # book.
        work_audience = ya
        reader_audience = ya

        # If there's no target age, it's fine (or at least we don't
        # have the information necessary to say it's not fine).
        work_target_age = None
        assert True == m(work_audience, work_target_age, reader_audience, object())

        # Now give the work a specific target age range.
        for work_target_age in ((14, 16), tuple_to_numericrange((14, 16))):
            # The lower end of the age range is old enough
            for age in range(14, 20):
                for reader_age in (
                    age,
                    (age - 1, age),
                    tuple_to_numericrange((age - 1, age)),
                ):
                    assert True == m(
                        work_audience, work_target_age, reader_audience, reader_age
                    )

            # Anything lower than that is not.
            for age in range(7, 14):
                for reader_age in (
                    age,
                    (age - 1, age),
                    tuple_to_numericrange((age - 1, age)),
                ):
                    assert False == m(
                        work_audience, work_target_age, reader_audience, reader_age
                    )


def mock_url_for(url, **kwargs):
    item_list = [f"{k}={v}" for k, v in kwargs.items()]
    item_list.sort()  # Ensure repeatable order
    items = ";".join(item_list)
    return f"{url} : {items}"


class ExamplePatronProfileStorageFixture:
    patron: Patron
    store: PatronProfileStorage
    transaction: DatabaseTransactionFixture

    @classmethod
    def create(
        cls, transaction: DatabaseTransactionFixture
    ) -> "ExamplePatronProfileStorageFixture":
        data = ExamplePatronProfileStorageFixture()
        data.patron = transaction.patron()
        data.store = PatronProfileStorage(data.patron, url_for=mock_url_for)
        data.transaction = transaction
        return data


@pytest.fixture()
def example_patron_profile_fixture(
    db,
) -> ExamplePatronProfileStorageFixture:
    return ExamplePatronProfileStorageFixture.create(db)


class TestPatronProfileStorage:
    def test_writable_setting_names(
        self, example_patron_profile_fixture: ExamplePatronProfileStorageFixture
    ):
        data = example_patron_profile_fixture

        """Only one setting is currently writable."""
        assert {data.store.SYNCHRONIZE_ANNOTATIONS} == data.store.writable_setting_names

    def test_profile_document(
        self, example_patron_profile_fixture: ExamplePatronProfileStorageFixture
    ):
        data = example_patron_profile_fixture

        links = [
            dict(
                rel=LinkRelations.DEVICE_REGISTRATION,
                type="application/json",
                href="put_patron_devices : _external=True;library_short_name=default",
            )
        ]

        # synchronize_annotations always shows up as settable, even if
        # the current value is None.
        data.patron.authorization_identifier = "abcd"
        assert None == data.patron.synchronize_annotations
        rep = data.store.profile_document
        assert {
            "simplified:authorization_identifier": "abcd",
            "settings": {"simplified:synchronize_annotations": None},
            "links": links,
        } == rep

        data.patron.synchronize_annotations = True
        data.patron.authorization_expires = datetime_utc(2016, 1, 1, 10, 20, 30)
        rep = data.store.profile_document
        assert {
            "simplified:authorization_expires": "2016-01-01T10:20:30Z",
            "simplified:authorization_identifier": "abcd",
            "settings": {"simplified:synchronize_annotations": True},
            "links": links,
        } == rep

    def test_update(
        self, example_patron_profile_fixture: ExamplePatronProfileStorageFixture
    ):
        data = example_patron_profile_fixture

        # This is a no-op.
        data.store.update({}, {})
        assert None == data.patron.synchronize_annotations

        # This is not.
        data.store.update({data.store.SYNCHRONIZE_ANNOTATIONS: True}, {})
        assert True == data.patron.synchronize_annotations

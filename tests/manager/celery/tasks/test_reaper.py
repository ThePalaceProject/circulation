import datetime
from unittest.mock import MagicMock

import pytest

from palace.manager.celery.tasks.reaper import (
    annotation_reaper,
    collection_reaper,
    credential_reaper,
    hold_reaper,
    loan_reaper,
    measurement_reaper,
    patron_reaper,
    reap_holds_in_inactive_collections,
    reap_loans_in_inactive_collections,
    reap_unassociated_holds,
    reap_unassociated_loans,
    work_reaper,
)
from palace.manager.integration.license.opds.opds2.api import OPDS2API
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.classification import Genre
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.credential import Credential
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.devicetokens import DeviceToken, DeviceTokenTypes
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.model.patron import Annotation, Hold, Loan, Patron
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import get_one_or_create
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


def test_credential_reaper(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(LogLevel.info)

    # Create four Credentials: two expired, two valid.
    expired1 = db.credential()
    expired2 = db.credential()
    now = utc_now()
    expiration_date = now - datetime.timedelta(days=1, seconds=1)
    for e in [expired1, expired2]:
        e.expires = expiration_date

    active = db.credential()
    active.expires = now

    eternal = db.credential()

    # Run the reaper.
    credential_reaper.delay().wait()

    # The expired credentials have been reaped; the others
    # are still in the database.
    remaining = set(db.session.query(Credential).all())
    assert {active, eternal} == remaining

    # The reaper logged its work.
    assert "Deleted 2 expired credentials." in caplog.messages


def test_patron_reaper(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(LogLevel.info)

    # Patron that has expired. The patron has some related objects that should be deleted along with the patron.
    expired = db.patron()
    now = utc_now()
    expired.authorization_expires = now - datetime.timedelta(days=61)
    db.credential(patron=expired)
    db.session.add(Annotation(patron=expired))
    DeviceToken.create(db.session, DeviceTokenTypes.FCM_ANDROID, "token", expired)

    # Patron that is about to expire
    active = db.patron()
    active.authorization_expires = now - datetime.timedelta(days=59)

    # Patron that has no expiration
    no_expiration = db.patron()
    no_expiration.authorization_expires = None

    # Run the reaper.
    patron_reaper.delay().wait()

    # The expired patron has been reaped; the others are still in the database.
    assert set(db.session.query(Patron).all()) == {active, no_expiration}

    # The reaper logged its work.
    assert "Deleted 1 expired patron record." in caplog.messages


class TestWorkReaper:
    def test_reap(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        # Set up our search mock to track calls to remove_work
        removed = set()
        mock_remove_work = services_fixture.search_index.remove_work
        mock_remove_work.side_effect = lambda x: removed.add(x.id)

        # First, create three works.

        # This work has a license pool.
        has_license_pool = db.work(with_license_pool=True)

        # This work had a license pool and then lost it.
        had_license_pool = db.work(with_license_pool=True)
        db.session.delete(had_license_pool.license_pools[0])

        # This work never had a license pool.
        never_had_license_pool = db.work(with_license_pool=False)

        # Each work has a presentation edition -- keep track of these
        # for later.
        works = db.session.query(Work)
        presentation_editions = [x.presentation_edition for x in works]

        # If and when Work gets database-level cascading deletes, this
        # is where they will all be triggered, with no chance that an
        # ORM-level delete is doing the work. So let's verify that all
        # the cascades work.

        # First, set up some related items for each Work.

        # Each work is assigned to a genre.
        genre, ignore = Genre.lookup(db.session, "Science Fiction")
        for work in works:
            work.genres = [genre]

        # Each work is on the same CustomList.
        l, ignore = db.customlist("a list", num_entries=0)
        for work in works:
            l.add_entry(work)

        # Run the reaper.
        work_reaper.delay().wait()

        # Search index was updated
        assert len(removed) == 2
        assert has_license_pool.id not in removed
        assert had_license_pool.id in removed
        assert never_had_license_pool.id in removed

        # Only the work with a license pool remains.
        assert db.session.query(Work).all() == [has_license_pool]

        # The presentation editions are still around, since they might
        # theoretically be used by other parts of the system.
        assert set(db.session.query(Edition).all()) == set(presentation_editions)

        # The surviving work is still assigned to the Genre
        assert genre.works == [has_license_pool]

        # The CustomListEntries still exist, but two of them have lost
        # their work.
        assert len([x for x in l.entries if not x.work]) == 2
        assert [x.work for x in l.entries if x.work] == [has_license_pool]

    def test_batch(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        # Create some works that will be reaped.
        [db.work(with_license_pool=False) for i in range(6)]

        # Run the reaper, with a batch size of 2, so it will have to
        # requeue itself to fully process all the works.
        work_reaper.delay(batch_size=2).wait()

        # Make sure the works were deleted
        assert db.session.query(Work).all() == []


class TestCollectionReaper:
    def test_reap(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        # End-to-end test
        caplog.set_level(LogLevel.info)

        # Three collections: two marked for deletion (one active, and one inactive), one not.
        c1 = db.collection()
        c2 = db.collection(inactive=True)
        c2.marked_for_deletion = True
        c3 = db.collection(inactive=False)
        c3.marked_for_deletion = True

        # Run reaper
        collection_reaper.delay().wait()

        # The Collections marked for deletion have been deleted; the other
        # one is unaffected.
        assert [c1] == db.session.query(Collection).all()
        assert f"Deleting {c2!r}." in caplog.messages
        assert (
            f"1 collection waiting for delete. Re-queueing the reaper."
            in caplog.messages
        )
        assert f"Deleting {c3!r}." in caplog.messages

    def test_reaper_delete_calls_collection_delete(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        monkeypatch: pytest.MonkeyPatch,
    ):
        # The collection reaper should call the delete method on the collection
        # rather than deleting the collection directly in the database.
        collection = db.collection()
        collection.marked_for_deletion = True

        mock_delete = MagicMock(side_effect=collection.delete)
        monkeypatch.setattr(Collection, "delete", mock_delete)

        # Run reaper
        collection_reaper.delay().wait()

        # Make sure we called the delete method on the collection
        mock_delete.assert_called_once()

    def test_reaper_no_collections(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ):
        # Some collections that don't need to be deleted
        collections = {db.collection() for idx in range(3)}

        # Run reaper
        collection_reaper.delay().wait()

        # Make sure no collections were deleted
        assert set(db.session.query(Collection).all()) == collections


def test_measurement_reaper(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    caplog: pytest.LogCaptureFixture,
):
    # End-to-end test
    caplog.set_level(LogLevel.info)

    recent_measurement, _ = get_one_or_create(
        db.session,
        Measurement,
        quantity_measured="answer",
        value=12,
        is_most_recent=True,
    )
    outdated_measurement, _ = get_one_or_create(
        db.session,
        Measurement,
        quantity_measured="answer",
        value=42,
        is_most_recent=False,
    )

    measurement_reaper.delay().wait()

    assert db.session.query(Measurement).all() == [recent_measurement]
    assert "Deleted 1 outdated measurement." in caplog.messages


def test_annotation_reaper(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(LogLevel.info)

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
        patron: Patron,
        pool: LicensePool,
        content: str,
        motivation: str = Annotation.IDLING,
        timestamp: datetime.datetime = very_old,
    ) -> Annotation:
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
    should_be_reaped = _annotation(p2, lp1, "abandoned")

    # The second patron also has a very old non-idling annotation
    # for the first book, which will not be reaped because only
    # idling annotations are reaped.
    not_idling = _annotation(p2, lp1, "not idling", motivation="some other motivation")

    # The second patron has a non-old idling annotation for the
    # second book, which will not be reaped (even though there is
    # no active loan or hold) because it's not old enough.
    new_idling = _annotation(p2, lp2, "recent", timestamp=not_that_old)

    # Run the reaper
    annotation_reaper.delay().wait()

    # The reaper logged its work.
    assert "Deleted 1 outdated idling annotation." in caplog.messages

    # The annotation that should have been reaped is gone
    assert set(db.session.query(Annotation).all()) == {
        old_loan,
        old_hold,
        not_idling,
        new_idling,
    }


def test_hold_reaper(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    services_fixture: ServicesFixture,
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(LogLevel.info)

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
        edition, open_access=False, data_source_name=DataSource.BIBLIOTHECA
    )
    not_open_access_2 = db.licensepool(
        edition, open_access=False, data_source_name=DataSource.BOUNDLESS
    )
    not_open_access_3 = db.licensepool(
        edition, open_access=False, data_source_name=DataSource.BIBBLIO
    )

    now = utc_now()
    a_long_time_ago = now - datetime.timedelta(days=1000)
    not_very_long_ago = now - datetime.timedelta(days=60)
    even_longer = now - datetime.timedelta(days=2000)
    the_future = now + datetime.timedelta(days=1)

    # This hold expired without ever becoming a loan (that we saw).
    not_open_access_1.on_hold_to(
        inactive_patron, start=even_longer, end=a_long_time_ago
    )

    # This hold has no end date and is older than a year.
    not_open_access_2.on_hold_to(
        inactive_patron,
        start=a_long_time_ago,
        end=None,
    )

    # This hold has not expired yet.
    not_open_access_1.on_hold_to(current_patron, start=now, end=the_future)

    # This hold has no end date but is pretty recent.
    not_open_access_3.on_hold_to(current_patron, start=not_very_long_ago, end=None)

    assert len(inactive_patron.holds) == 2
    assert len(current_patron.holds) == 2

    # Now we fire up the hold reaper.
    hold_reaper.delay(batch_size=1).wait()

    # All the inactive patron's holds have been reaped
    assert db.session.query(Hold).where(Hold.patron == inactive_patron).all() == []
    assert len(db.session.query(Hold).where(Hold.patron == current_patron).all()) == 2

    # verify expected circ event count for hold reaper run
    call_args_list = services_fixture.analytics.collect.call_args_list
    assert len(call_args_list) == 2
    event_types = [call_args.kwargs["event"].type for call_args in call_args_list]
    assert event_types == [
        CirculationEvent.CM_HOLD_EXPIRED,
        CirculationEvent.CM_HOLD_EXPIRED,
    ]


def test_loan_reaper(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(LogLevel.info)

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
        edition, open_access=False, data_source_name=DataSource.BOUNDLESS
    )
    not_open_access_3 = db.licensepool(
        edition, open_access=False, data_source_name=DataSource.BIBBLIO
    )
    unlimited_access = db.licensepool(
        edition, unlimited_access=True, data_source_name=DataSource.AMAZON
    )

    now = utc_now()
    a_long_time_ago = now - datetime.timedelta(days=1000)
    not_very_long_ago = now - datetime.timedelta(days=60)
    even_longer = now - datetime.timedelta(days=2000)
    the_future = now + datetime.timedelta(days=1)

    # This loan has expired.
    not_open_access_1.loan_to(inactive_patron, start=even_longer, end=a_long_time_ago)

    # This loan has no end date and is older than 90 days.
    not_open_access_3.loan_to(
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
    not_expired, _ = not_open_access_1.loan_to(
        current_patron, start=now, end=the_future
    )

    # This loan has no end date but is pretty recent.
    recent, _ = not_open_access_2.loan_to(
        current_patron, start=not_very_long_ago, end=None
    )

    assert len(inactive_patron.loans) == 4
    assert len(current_patron.loans) == 2

    # Now we fire up the loan reaper.
    loan_reaper.delay().wait()

    # All the inactive patron's loans have been reaped,
    # except for the loans for open-access works and unlimited-access works,
    # which will never be reaped.
    assert set(db.session.query(Loan).where(Loan.patron == inactive_patron).all()) == {
        open_access_loan,
        unlimited_access_loan,
    }

    # The current patron's loans are unaffected, either
    # because they have not expired or because they have no known
    # expiration date and were created relatively recently.
    assert set(db.session.query(Loan).where(Loan.patron == current_patron).all()) == {
        not_expired,
        recent,
    }

    # The reaper logged its work.
    assert "Deleted 2 expired loans." in caplog.messages


def test_reap_unassociated_loans(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
):

    # create a collection associated with the patrons library
    library = db.library(short_name="my_library")
    biblio_collection = db.collection(protocol=OPDS2API, library=library)
    patron = db.patron(library=library)

    # create an associated loan
    edition, lp_biblio = db.edition(
        with_license_pool=True,
        collection=biblio_collection,
        data_source_name=DataSource.BIBBLIO,
    )

    now = utc_now()
    assert not patron.loans

    # make a loan
    lp_biblio.loan_to(patron, start=now, end=now + datetime.timedelta(days=14))

    assert len(patron.loans) == 1

    # run reaper and verify that it is not deleted.
    reap_unassociated_loans.delay().wait()
    db.session.refresh(patron)
    assert len(patron.loans) == 1

    # remove the association
    biblio_collection.associated_libraries.clear()

    # run the reaper and verify that it is deleted.
    reap_unassociated_loans.delay().wait()

    db.session.refresh(patron)
    assert not patron.loans


def test_reap_unassociated_holds(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
):
    # create a collection associated with the patrons library
    library = db.library(short_name="my_library")
    biblio_collection = db.collection(protocol=OPDS2API, library=library)
    patron = db.patron(library=library)

    # create an associated hold
    edition, lp_biblio = db.edition(
        with_license_pool=True,
        collection=biblio_collection,
        data_source_name=DataSource.BIBBLIO,
    )

    now = utc_now()
    assert not patron.holds

    # place a hold
    lp_biblio.on_hold_to(patron, start=now, end=now + datetime.timedelta(days=14))

    assert len(patron.holds) == 1

    # run reaper and verify that it is not deleted.
    reap_unassociated_holds.delay().wait()
    db.session.refresh(patron)
    assert len(patron.holds) == 1

    # remove the association
    biblio_collection.associated_libraries.clear()

    # run the reaper and verify that it is deleted.
    reap_unassociated_holds.delay().wait()

    db.session.refresh(patron)
    assert not patron.holds


def test_reap_loans_in_inactive_collections(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(LogLevel.info)

    # create a collection associated with the patrons library
    library = db.library(short_name="my_library")
    biblio_collection = db.collection(protocol=OPDS2API, library=library)
    patron = db.patron(library=library)

    # create an associated loan
    edition, lp_biblio = db.edition(
        with_license_pool=True,
        collection=biblio_collection,
        data_source_name=DataSource.BIBBLIO,
    )

    now = utc_now()
    assert not patron.loans

    # make a loan
    lp_biblio.loan_to(patron, start=now, end=now + datetime.timedelta(days=14))

    assert biblio_collection.is_active
    assert len(patron.loans) == 1

    # run reaper and verify that it is not deleted.
    reap_loans_in_inactive_collections.delay().wait()

    # make the collection inactive.
    biblio_collection.is_active

    day_before_yesterday = utc_now() - datetime.timedelta(days=2)
    one_week_ago = utc_now() - datetime.timedelta(days=7)

    biblio_collection._set_settings(
        subscription_expiration_date=day_before_yesterday.date(),
        subscription_activation_date=one_week_ago.date(),
    )

    assert not biblio_collection.is_active

    # run the reaper and verify that it is deleted.
    reap_loans_in_inactive_collections.delay().wait()
    assert "deleted 1 loan" in caplog.text
    db.session.refresh(patron)
    assert not patron.loans


def test_reap_holds_in_inactive_collections(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    caplog: pytest.LogCaptureFixture,
):
    caplog.set_level(LogLevel.info)

    # create a collection associated with the patrons library
    library = db.library(short_name="my_library")
    biblio_collection = db.collection(protocol=OPDS2API, library=library)
    patron = db.patron(library=library)

    # create an associated hold
    edition, lp_biblio = db.edition(
        with_license_pool=True,
        collection=biblio_collection,
        data_source_name=DataSource.BIBBLIO,
    )

    now = utc_now()
    assert not patron.holds

    # place a hold
    lp_biblio.on_hold_to(patron, start=now, end=now + datetime.timedelta(days=14))

    assert biblio_collection.is_active
    assert len(patron.holds) == 1

    # run reaper and verify that it is not deleted.
    reap_holds_in_inactive_collections.delay().wait()

    # make the collection inactive.
    biblio_collection.is_active

    day_before_yesterday = utc_now() - datetime.timedelta(days=2)
    one_week_ago = utc_now() - datetime.timedelta(days=7)

    biblio_collection._set_settings(
        subscription_expiration_date=day_before_yesterday,
        subscription_activation_date=one_week_ago,
    )

    assert not biblio_collection.is_active

    # run the reaper and verify that it is deleted.
    reap_holds_in_inactive_collections.delay().wait()
    assert "deleted 1 hold" in caplog.text
    db.session.refresh(patron)
    assert not patron.holds

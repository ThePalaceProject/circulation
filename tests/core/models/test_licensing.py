import datetime
import json
from typing import Callable, Optional
from unittest.mock import MagicMock, PropertyMock

import pytest
from sqlalchemy.exc import IntegrityError

from core.mock_analytics_provider import MockAnalyticsProvider
from core.model import create
from core.model.circulationevent import CirculationEvent
from core.model.collection import CollectionMissing
from core.model.constants import MediaTypes
from core.model.contributor import Contributor
from core.model.datasource import DataSource
from core.model.edition import Edition
from core.model.formats import FormatPriorities
from core.model.identifier import Identifier
from core.model.licensing import (
    DeliveryMechanism,
    Hold,
    LicensePool,
    LicensePoolDeliveryMechanism,
    LicenseStatus,
    Loan,
    RightsStatus,
)
from core.model.resource import Hyperlink, Representation
from core.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class TestDeliveryMechanismFixture:
    epub_no_drm: DeliveryMechanism
    epub_adobe_drm: DeliveryMechanism
    overdrive_streaming_text: DeliveryMechanism
    audiobook_drm_scheme: DeliveryMechanism
    transaction: DatabaseTransactionFixture


@pytest.fixture()
def test_delivery_mechanism_fixture(
    db: DatabaseTransactionFixture,
) -> TestDeliveryMechanismFixture:
    fix = TestDeliveryMechanismFixture()
    fix.epub_no_drm, ignore = DeliveryMechanism.lookup(
        db.session, Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM
    )
    fix.epub_adobe_drm, ignore = DeliveryMechanism.lookup(
        db.session, Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM
    )
    fix.overdrive_streaming_text, ignore = DeliveryMechanism.lookup(
        db.session,
        DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
        DeliveryMechanism.OVERDRIVE_DRM,
    )
    fix.audiobook_drm_scheme, ignore = DeliveryMechanism.lookup(
        db.session,
        Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
        DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM,
    )
    fix.transaction = db
    return fix


class TestDeliveryMechanism:
    def test_implicit_medium(
        self, test_delivery_mechanism_fixture: TestDeliveryMechanismFixture
    ):
        data = test_delivery_mechanism_fixture
        assert Edition.BOOK_MEDIUM == data.epub_no_drm.implicit_medium
        assert Edition.BOOK_MEDIUM == data.epub_adobe_drm.implicit_medium
        assert Edition.BOOK_MEDIUM == data.overdrive_streaming_text.implicit_medium

    def test_is_media_type(self):
        assert False == DeliveryMechanism.is_media_type(None)
        assert True == DeliveryMechanism.is_media_type(Representation.EPUB_MEDIA_TYPE)
        assert False == DeliveryMechanism.is_media_type(
            DeliveryMechanism.KINDLE_CONTENT_TYPE
        )
        assert False == DeliveryMechanism.is_media_type(
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE
        )

    def test_is_streaming(
        self, test_delivery_mechanism_fixture: TestDeliveryMechanismFixture
    ):
        data = test_delivery_mechanism_fixture
        assert False == data.epub_no_drm.is_streaming
        assert False == data.epub_adobe_drm.is_streaming
        assert True == data.overdrive_streaming_text.is_streaming

    def test_drm_scheme_media_type(
        self, test_delivery_mechanism_fixture: TestDeliveryMechanismFixture
    ):
        data = test_delivery_mechanism_fixture
        assert None == data.epub_no_drm.drm_scheme_media_type
        assert DeliveryMechanism.ADOBE_DRM == data.epub_adobe_drm.drm_scheme_media_type
        assert None == data.overdrive_streaming_text.drm_scheme_media_type

    def test_content_type_media_type(
        self, test_delivery_mechanism_fixture: TestDeliveryMechanismFixture
    ):
        data = test_delivery_mechanism_fixture
        assert (
            Representation.EPUB_MEDIA_TYPE == data.epub_no_drm.content_type_media_type
        )
        assert (
            Representation.EPUB_MEDIA_TYPE
            == data.epub_adobe_drm.content_type_media_type
        )
        assert (
            Representation.TEXT_HTML_MEDIA_TYPE + DeliveryMechanism.STREAMING_PROFILE
            == data.overdrive_streaming_text.content_type_media_type
        )
        assert (
            Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
            + DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_PROFILE
            == data.audiobook_drm_scheme.content_type_media_type
        )

    def test_default_fulfillable(
        self, test_delivery_mechanism_fixture: TestDeliveryMechanismFixture
    ):
        data = test_delivery_mechanism_fixture
        session = data.transaction.session

        # Try some well-known media type/DRM combinations known to be
        # fulfillable by the default client.
        for media, drm in (
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM),
            (MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.BEARER_TOKEN),
            (MediaTypes.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM),
            (MediaTypes.PDF_MEDIA_TYPE, DeliveryMechanism.BEARER_TOKEN),
            (None, DeliveryMechanism.FINDAWAY_DRM),
            (MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE, DeliveryMechanism.NO_DRM),
            (MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE, DeliveryMechanism.BEARER_TOKEN),
        ):
            # All of these DeliveryMechanisms were created when the
            # database was initialized.
            mechanism, is_new = DeliveryMechanism.lookup(session, media, drm)
            assert False == is_new
            assert True == mechanism.default_client_can_fulfill

        # It's possible to create new DeliveryMechanisms at runtime,
        # but their .default_client_can_fulfill will be False.
        mechanism, is_new = DeliveryMechanism.lookup(
            session, MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM
        )
        assert False == is_new
        assert True == mechanism.default_client_can_fulfill

        mechanism, is_new = DeliveryMechanism.lookup(
            session, MediaTypes.PDF_MEDIA_TYPE, DeliveryMechanism.STREAMING_DRM
        )
        assert True == is_new
        assert False == mechanism.default_client_can_fulfill

    def test_association_with_licensepool(
        self, test_delivery_mechanism_fixture: TestDeliveryMechanismFixture
    ):
        data = test_delivery_mechanism_fixture
        ignore, with_download = data.transaction.edition(with_open_access_download=True)
        [lpmech] = with_download.delivery_mechanisms
        assert b"Dummy content" == lpmech.resource.representation.content
        mech = lpmech.delivery_mechanism
        assert MediaTypes.EPUB_MEDIA_TYPE == mech.content_type
        assert mech.NO_DRM == mech.drm_scheme

    def test_compatible_with(
        self, test_delivery_mechanism_fixture: TestDeliveryMechanismFixture
    ):
        session = test_delivery_mechanism_fixture.transaction.session

        """Test the rules about which DeliveryMechanisms are
        mutually compatible and which are mutually exclusive.
        """
        epub_adobe, ignore = DeliveryMechanism.lookup(
            session, MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM
        )

        pdf_adobe, ignore = DeliveryMechanism.lookup(
            session, MediaTypes.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM
        )

        epub_no_drm, ignore = DeliveryMechanism.lookup(
            session, MediaTypes.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        )

        pdf_no_drm, ignore = DeliveryMechanism.lookup(
            session, MediaTypes.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        )

        streaming, ignore = DeliveryMechanism.lookup(
            session,
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_DRM,
        )

        # A non-streaming DeliveryMechanism is compatible only with
        # itself or a streaming mechanism.
        assert False == epub_adobe.compatible_with(None)
        assert False == epub_adobe.compatible_with("Not a DeliveryMechanism")
        assert False == epub_adobe.compatible_with(epub_no_drm)
        assert False == epub_adobe.compatible_with(pdf_adobe)
        assert False == epub_no_drm.compatible_with(pdf_no_drm)
        assert True == epub_adobe.compatible_with(epub_adobe)
        assert True == epub_adobe.compatible_with(streaming)

        # A streaming mechanism is compatible with anything.
        assert True == streaming.compatible_with(epub_adobe)
        assert True == streaming.compatible_with(pdf_adobe)
        assert True == streaming.compatible_with(epub_no_drm)

        # Rules are slightly different for open-access books: books
        # in any format are compatible so long as they have no DRM.
        assert True == epub_no_drm.compatible_with(pdf_no_drm, True)
        assert False == epub_no_drm.compatible_with(pdf_adobe, True)

    def test_uniqueness_constraint(
        self, test_delivery_mechanism_fixture: TestDeliveryMechanismFixture
    ):
        session = test_delivery_mechanism_fixture.transaction.session
        dm = DeliveryMechanism

        # You can't create two DeliveryMechanisms with the same values
        # for content_type and drm_scheme.
        with_drm_args = dict(content_type="type1", drm_scheme="scheme1")
        without_drm_args = dict(content_type="type1", drm_scheme=None)
        with_drm = create(session, dm, **with_drm_args)
        pytest.raises(IntegrityError, create, session, dm, **with_drm_args)
        session.rollback()

        # You can't create two DeliveryMechanisms with the same value
        # for content_type and a null value for drm_scheme.
        without_drm = create(session, dm, **without_drm_args)
        pytest.raises(IntegrityError, create, session, dm, **without_drm_args)
        session.rollback()


class TestRightsStatus:
    def test_lookup(self, db: DatabaseTransactionFixture):
        status = RightsStatus.lookup(db.session, RightsStatus.IN_COPYRIGHT)
        assert RightsStatus.IN_COPYRIGHT == status.uri
        assert RightsStatus.NAMES.get(RightsStatus.IN_COPYRIGHT) == status.name

        status = RightsStatus.lookup(db.session, RightsStatus.CC0)
        assert RightsStatus.CC0 == status.uri
        assert RightsStatus.NAMES.get(RightsStatus.CC0) == status.name

        status = RightsStatus.lookup(db.session, "not a known rights uri")
        assert RightsStatus.UNKNOWN == status.uri
        assert RightsStatus.NAMES.get(RightsStatus.UNKNOWN) == status.name

    def test_unique_uri_constraint(self, db: DatabaseTransactionFixture):
        # We already have this RightsStatus.
        status = RightsStatus.lookup(db.session, RightsStatus.IN_COPYRIGHT)

        # Let's try to create another one with the same URI.
        dupe = RightsStatus(uri=RightsStatus.IN_COPYRIGHT)
        db.session.add(dupe)

        # Nope.
        pytest.raises(IntegrityError, db.session.commit)


class TestLicenseFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.db = db
        self.pool = db.licensepool(None)

        now = utc_now()
        next_year = now + datetime.timedelta(days=365)
        yesterday = now - datetime.timedelta(days=1)

        self.perpetual = db.license(
            self.pool,
            expires=None,
            checkouts_left=None,
            checkouts_available=1,
            terms_concurrency=2,
        )

        self.time_limited = db.license(
            self.pool,
            expires=next_year,
            checkouts_left=None,
            checkouts_available=1,
            terms_concurrency=1,
        )

        self.loan_limited = db.license(
            self.pool,
            expires=None,
            checkouts_left=4,
            checkouts_available=2,
            terms_concurrency=3,
        )

        self.time_and_loan_limited = db.license(
            self.pool,
            expires=next_year + datetime.timedelta(days=1),
            checkouts_left=52,
            checkouts_available=1,
            terms_concurrency=1,
        )

        self.expired_time_limited = db.license(
            self.pool,
            expires=yesterday,
            checkouts_left=None,
            checkouts_available=1,
        )

        self.expired_loan_limited = db.license(
            self.pool, expires=None, checkouts_left=0, checkouts_available=1
        )

        self.unavailable = db.license(
            self.pool,
            expires=None,
            checkouts_left=None,
            checkouts_available=20,
            status=LicenseStatus.unavailable,
        )


@pytest.fixture(scope="function")
def licenses(db: DatabaseTransactionFixture) -> TestLicenseFixture:
    return TestLicenseFixture(db)


class TestLicense:
    def test_loan_to(self, licenses: TestLicenseFixture):
        # Verify that loaning a license also loans its pool.
        pool = licenses.pool
        license = licenses.perpetual
        patron = licenses.db.patron()
        patron.last_loan_activity_sync = utc_now()
        loan, is_new = license.loan_to(patron)
        assert license == loan.license
        assert pool == loan.license_pool
        assert True == is_new
        assert None == patron.last_loan_activity_sync

        loan2, is_new = license.loan_to(patron)
        assert loan == loan2
        assert license == loan2.license
        assert pool == loan2.license_pool
        assert False == is_new

    @pytest.mark.parametrize(
        (
            "license_type",
            "is_perpetual",
            "is_time_limited",
            "is_loan_limited",
            "is_inactive",
            "total_remaining_loans",
            "currently_available_loans",
        ),
        [
            ("perpetual", True, False, False, False, 2, 1),
            ("time_limited", False, True, False, False, 1, 1),
            ("loan_limited", False, False, True, False, 4, 2),
            ("time_and_loan_limited", False, True, True, False, 52, 1),
            ("expired_time_limited", False, True, False, True, 0, 0),
            ("expired_loan_limited", False, False, True, True, 0, 0),
            ("unavailable", True, False, False, True, 0, 0),
        ],
    )
    def test_license_types(
        self,
        license_type,
        is_perpetual,
        is_time_limited,
        is_loan_limited,
        is_inactive,
        total_remaining_loans,
        currently_available_loans,
        licenses: TestLicenseFixture,
    ):
        license = getattr(licenses, license_type)
        assert is_perpetual == license.is_perpetual
        assert is_time_limited == license.is_time_limited
        assert is_loan_limited == license.is_loan_limited
        assert is_inactive == license.is_inactive
        assert total_remaining_loans == license.total_remaining_loans
        assert currently_available_loans == license.currently_available_loans

    @pytest.mark.parametrize(
        "license_type,left,available",
        [
            ("perpetual", None, 0),
            ("time_limited", None, 0),
            ("loan_limited", 3, 1),
            ("time_and_loan_limited", 51, 0),
            ("expired_time_limited", None, 1),
            ("expired_loan_limited", 0, 1),
            ("unavailable", None, 20),
        ],
    )
    def test_license_checkout(
        self, license_type, left, available, licenses: TestLicenseFixture
    ):
        license = getattr(licenses, license_type)
        license.checkout()
        assert left == license.checkouts_left
        assert available == license.checkouts_available

    @pytest.mark.parametrize(
        "license_params,left,available",
        [
            ({"checkouts_available": 1, "terms_concurrency": 2}, None, 2),
            ({"checkouts_available": 1, "terms_concurrency": 1}, None, 1),
            (
                {
                    "expires": utc_now() + datetime.timedelta(days=7),
                    "checkouts_available": 0,
                    "terms_concurrency": 1,
                },
                None,
                1,
            ),
            (
                {"checkouts_available": 0, "terms_concurrency": 1, "checkouts_left": 4},
                4,
                1,
            ),
            (
                {"checkouts_available": 2, "terms_concurrency": 5, "checkouts_left": 2},
                2,
                2,
            ),
            (
                {"checkouts_available": 0, "terms_concurrency": 1, "checkouts_left": 0},
                0,
                0,
            ),
            (
                {
                    "expires": utc_now() + datetime.timedelta(days=7),
                    "checkouts_available": 5,
                    "terms_concurrency": 6,
                    "checkouts_left": 40,
                },
                40,
                6,
            ),
            (
                {
                    "expires": utc_now() - datetime.timedelta(days=7),
                    "checkouts_available": 4,
                    "terms_concurrency": 5,
                },
                None,
                4,
            ),
        ],
    )
    def test_license_checkin(
        self, license_params, left, available, licenses: TestLicenseFixture
    ):
        l = licenses.db.license(licenses.pool, **license_params)
        l.checkin()
        assert left == l.checkouts_left
        assert available == l.checkouts_available

    def test_best_available_license(self, licenses: TestLicenseFixture):
        next_week = utc_now() + datetime.timedelta(days=7)
        time_limited_2 = licenses.db.license(
            licenses.pool,
            expires=next_week,
            checkouts_left=None,
            checkouts_available=1,
        )
        loan_limited_2 = licenses.db.license(
            licenses.pool, expires=None, checkouts_left=2, checkouts_available=1
        )

        # First, we use the time-limited license that's expiring first.
        assert time_limited_2 == licenses.pool.best_available_license()
        time_limited_2.loan_to(licenses.db.patron())

        # When that's not available, we use the next time-limited license.
        assert licenses.time_limited == licenses.pool.best_available_license()
        licenses.time_limited.loan_to(licenses.db.patron())

        # The time-and-loan-limited license also counts as time-limited for this.
        assert licenses.time_and_loan_limited == licenses.pool.best_available_license()
        licenses.time_and_loan_limited.loan_to(licenses.db.patron())

        # Next is the perpetual license.
        assert licenses.perpetual == licenses.pool.best_available_license()
        licenses.perpetual.loan_to(licenses.db.patron())

        # Then the loan-limited license with the most remaining checkouts.
        assert licenses.loan_limited == licenses.pool.best_available_license()
        licenses.loan_limited.loan_to(licenses.db.patron())

        # That license allows 2 concurrent checkouts, so it's still the
        # best license until it's checked out again.
        assert licenses.loan_limited == licenses.pool.best_available_license()
        licenses.loan_limited.loan_to(licenses.db.patron())

        # There's one more loan-limited license.
        assert loan_limited_2 == licenses.pool.best_available_license()
        loan_limited_2.loan_to(licenses.db.patron())

        # Now all licenses are either loaned out or expired.
        assert None == licenses.pool.best_available_license()


class TestLicensePool:
    def test_for_foreign_id(self, db: DatabaseTransactionFixture):
        """Verify we can get a LicensePool for a data source, an
        appropriate work identifier, and a Collection."""
        now = utc_now()
        pool, was_new = LicensePool.for_foreign_id(
            db.session,
            DataSource.GUTENBERG,
            Identifier.GUTENBERG_ID,
            "541",
            collection=db.collection(),
        )
        assert pool is not None
        assert pool.availability_time is not None
        assert (pool.availability_time - now).total_seconds() < 2
        assert True == was_new
        assert DataSource.GUTENBERG == pool.data_source.name
        assert Identifier.GUTENBERG_ID == pool.identifier.type
        assert "541" == pool.identifier.identifier
        assert 0 == pool.licenses_owned
        assert 0 == pool.licenses_available
        assert 0 == pool.licenses_reserved
        assert 0 == pool.patrons_in_hold_queue

    def test_for_foreign_id_fails_when_no_collection_provided(
        self, db: DatabaseTransactionFixture
    ):
        """We cannot create a LicensePool that is not associated
        with some Collection.
        """
        pytest.raises(
            CollectionMissing,
            LicensePool.for_foreign_id,
            db.session,
            DataSource.GUTENBERG,
            Identifier.GUTENBERG_ID,
            "541",
            collection=None,
        )

    def test_with_no_delivery_mechanisms(self, db: DatabaseTransactionFixture):
        # LicensePool.with_no_delivery_mechanisms returns a
        # query that finds all LicensePools which are missing
        # delivery mechanisms.
        qu = LicensePool.with_no_delivery_mechanisms(db.session)
        pool = db.licensepool(None)

        # The LicensePool was created with a delivery mechanism.
        assert [] == qu.all()

        # Let's delete it.
        for x in pool.delivery_mechanisms:
            db.session.delete(x)
        assert [pool] == qu.all()

    def test_no_license_pool_for_non_primary_identifier(
        self, db: DatabaseTransactionFixture
    ):
        """Overdrive offers licenses, but to get an Overdrive license pool for
        a book you must identify the book by Overdrive's primary
        identifier, not some other kind of identifier.
        """
        collection = db.collection()
        with pytest.raises(ValueError) as excinfo:
            LicensePool.for_foreign_id(
                db.session,
                DataSource.OVERDRIVE,
                Identifier.ISBN,
                "{1-2-3}",
                collection=collection,
            )
        assert (
            "License pools for data source 'Overdrive' are keyed to identifier type 'Overdrive ID' (not 'ISBN', which was provided)"
            in str(excinfo.value)
        )

    def test_licensepools_for_same_identifier_have_same_presentation_edition(
        self, db: DatabaseTransactionFixture
    ):
        """Two LicensePools for the same Identifier will get the same
        presentation edition.
        """
        identifier = db.identifier()
        edition1, pool1 = db.edition(
            with_license_pool=True,
            data_source_name=DataSource.GUTENBERG,
            identifier_type=identifier.type,
            identifier_id=identifier.identifier,
        )
        edition2, pool2 = db.edition(
            with_license_pool=True,
            data_source_name=DataSource.UNGLUE_IT,
            identifier_type=identifier.type,
            identifier_id=identifier.identifier,
        )
        pool1.set_presentation_edition()
        pool2.set_presentation_edition()
        assert pool1.presentation_edition == pool2.presentation_edition

    def test_collection_datasource_identifier_must_be_unique(
        self, db: DatabaseTransactionFixture
    ):
        """You can't have two LicensePools with the same Collection,
        DataSource, and Identifier.
        """
        data_source = DataSource.lookup(db.session, DataSource.GUTENBERG)
        identifier = db.identifier()
        collection = db.default_collection()
        pool = create(
            db.session,
            LicensePool,
            data_source=data_source,
            identifier=identifier,
            collection=collection,
        )

        pytest.raises(
            IntegrityError,
            create,
            db.session,
            LicensePool,
            data_source=data_source,
            identifier=identifier,
            collection=collection,
        )

    def test_with_no_work(self, db: DatabaseTransactionFixture):
        p1, ignore = LicensePool.for_foreign_id(
            db.session,
            DataSource.GUTENBERG,
            Identifier.GUTENBERG_ID,
            "1",
            collection=db.default_collection(),
        )
        assert p1 is not None

        p2, ignore = LicensePool.for_foreign_id(
            db.session,
            DataSource.OVERDRIVE,
            Identifier.OVERDRIVE_ID,
            "2",
            collection=db.default_collection(),
        )
        assert p2 is not None

        work = db.work(title="Foo")
        p1.work = work

        assert p1 in work.license_pools
        assert [p2] == LicensePool.with_no_work(db.session)

    def test_update_availability(self, db: DatabaseTransactionFixture):
        work = db.work(with_license_pool=True)
        work.last_update_time = None

        [pool] = work.license_pools
        pool.update_availability(30, 20, 2, 0)
        assert 30 == pool.licenses_owned
        assert 20 == pool.licenses_available
        assert 2 == pool.licenses_reserved
        assert 0 == pool.patrons_in_hold_queue

        assert work.last_update_time is not None

        # Updating availability also modified work.last_update_time.
        assert (utc_now() - work.last_update_time) < datetime.timedelta(seconds=2)  # type: ignore[unreachable]

    def test_update_availability_does_nothing_if_given_no_data(
        self, db: DatabaseTransactionFixture
    ):
        """Passing an empty set of data into update_availability is
        a no-op.
        """

        # Set up a Work.
        work = db.work(with_license_pool=True)
        work.last_update_time = None

        # Set up a LicensePool.
        [pool] = work.license_pools
        pool.last_checked = None
        pool.licenses_owned = 10
        pool.licenses_available = 20
        pool.licenses_reserved = 30
        pool.patrons_in_hold_queue = 40

        # Pass empty values into update_availability.
        pool.update_availability(None, None, None, None)

        # The LicensePool's circulation data is what it was before.
        assert 10 == pool.licenses_owned
        assert 20 == pool.licenses_available
        assert 30 == pool.licenses_reserved
        assert 40 == pool.patrons_in_hold_queue

        # Work.update_time and LicensePool.last_checked are unaffected.
        assert None == work.last_update_time
        assert None == pool.last_checked

        # If we pass a mix of good and null values...
        pool.update_availability(5, None, None, None)

        # Only the good values are changed.
        assert 5 == pool.licenses_owned
        assert 20 == pool.licenses_available
        assert 30 == pool.licenses_reserved
        assert 40 == pool.patrons_in_hold_queue

    def test_open_access_links(self, db: DatabaseTransactionFixture):
        edition, pool = db.edition(with_open_access_download=True)
        source = DataSource.lookup(db.session, DataSource.GUTENBERG)

        [oa1] = list(pool.open_access_links)

        # We have one open-access download, let's
        # add another.
        url = db.fresh_url()
        media_type = MediaTypes.EPUB_MEDIA_TYPE
        link2, new = pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD, url, source, media_type
        )
        oa2 = link2.resource

        # And let's add a link that's not an open-access download.
        url = db.fresh_url()
        image, new = pool.identifier.add_link(
            Hyperlink.IMAGE, url, source, MediaTypes.JPEG_MEDIA_TYPE
        )
        db.session.commit()

        # Only the two open-access download links show up.
        assert {oa1, oa2} == set(pool.open_access_links)

    def test_better_open_access_pool_than(self, db: DatabaseTransactionFixture):
        gutenberg_1 = db.licensepool(
            None,
            open_access=True,
            data_source_name=DataSource.GUTENBERG,
            with_open_access_download=True,
        )

        gutenberg_2 = db.licensepool(
            None,
            open_access=True,
            data_source_name=DataSource.GUTENBERG,
            with_open_access_download=True,
        )

        assert int(gutenberg_1.identifier.identifier) < int(
            gutenberg_2.identifier.identifier
        )

        standard_ebooks = db.licensepool(
            None,
            open_access=True,
            data_source_name=DataSource.STANDARD_EBOOKS,
            with_open_access_download=True,
        )

        # Make sure Feedbooks data source exists -- it's not created
        # by default.
        feedbooks_data_source = DataSource.lookup(
            db.session, DataSource.FEEDBOOKS, autocreate=True
        )
        feedbooks = db.licensepool(
            None,
            open_access=True,
            data_source_name=DataSource.FEEDBOOKS,
            with_open_access_download=True,
        )

        overdrive = db.licensepool(
            None, open_access=False, data_source_name=DataSource.OVERDRIVE
        )

        suppressed = db.licensepool(
            None, open_access=True, data_source_name=DataSource.GUTENBERG
        )
        suppressed.suppressed = True

        def better(x, y):
            return x.better_open_access_pool_than(y)

        # We would rather have nothing at all than a suppressed
        # LicensePool.
        assert False == better(suppressed, None)

        # A non-open-access LicensePool is not considered at all.
        assert False == better(overdrive, None)

        # Something is better than nothing.
        assert True == better(gutenberg_1, None)

        # An open access book from a high-quality source beats one
        # from a low-quality source.
        assert True == better(standard_ebooks, gutenberg_1)
        assert True == better(feedbooks, gutenberg_1)
        assert False == better(gutenberg_1, standard_ebooks)

        # A high Gutenberg number beats a low Gutenberg number.
        assert True == better(gutenberg_2, gutenberg_1)
        assert False == better(gutenberg_1, gutenberg_2)

        # If a supposedly open-access LicensePool doesn't have an
        # open-access download resource, it will only be considered if
        # there is no other alternative.
        no_resource = db.licensepool(
            None,
            open_access=True,
            data_source_name=DataSource.STANDARD_EBOOKS,
            with_open_access_download=False,
        )
        no_resource.open_access = True
        assert True == better(no_resource, None)
        assert False == better(no_resource, gutenberg_1)

    def test_set_presentation_edition(self, db: DatabaseTransactionFixture):
        """
        Make sure composite edition creation makes good choices when combining
        field data from provider, metadata wrangler, admin interface, etc. editions.
        """
        # Here's an Overdrive audiobook which also has data from the metadata
        # wrangler and from library staff.
        od, pool = db.edition(
            data_source_name=DataSource.OVERDRIVE, with_license_pool=True
        )
        od.medium = Edition.AUDIO_MEDIUM

        admin = db.edition(
            data_source_name=DataSource.LIBRARY_STAFF, with_license_pool=False
        )
        admin.primary_identifier = pool.identifier

        mw = db.edition(
            data_source_name=DataSource.METADATA_WRANGLER, with_license_pool=False
        )
        mw.primary_identifier = pool.identifier

        # The library staff has no opinion on the book's medium,
        # and the metadata wrangler has an incorrect opinion.
        admin.medium = None
        mw.medium = Edition.BOOK_MEDIUM

        # Overdrive, the metadata wrangler, and the library staff all have
        # opinions on the book's title. The metadata wrangler has also
        # identified a subtitle.
        od.title = "OverdriveTitle1"

        mw.title = "MetadataWranglerTitle1"
        mw.subtitle = "MetadataWranglerSubTitle1"

        admin.title = "AdminInterfaceTitle1"

        # Create a presentation edition, a composite of the available
        # Editions.
        pool.set_presentation_edition()
        presentation = pool.presentation_edition
        assert [pool] == presentation.is_presentation_for

        # The presentation edition is a completely new Edition.
        assert mw != od
        assert od != admin
        assert admin != presentation
        assert od != presentation

        # Within the presentation edition, information from the
        # library staff takes precedence over anything else.
        assert presentation.title == "AdminInterfaceTitle1"
        assert admin.contributors == presentation.contributors

        # Where the library staff has no opinion, the license source
        # takes precedence over the metadata wrangler.
        assert Edition.AUDIO_MEDIUM == presentation.medium

        # The metadata wrangler fills in any missing information.
        assert presentation.subtitle == "MetadataWranglerSubTitle1"

        # Now, change the admin interface's opinion about who the
        # author is.
        for c in admin.contributions:
            db.session.delete(c)
        db.session.commit()
        [jane], ignore = Contributor.lookup(db.session, "Doe, Jane")
        jane.family_name, jane.display_name = jane.default_names()
        admin.add_contributor(jane, Contributor.AUTHOR_ROLE)
        pool.set_presentation_edition()

        # The old contributor has been removed from the presentation
        # edition, and the new contributor added.
        assert {jane} == presentation.contributors

    def test_circulation_changelog(self, db: DatabaseTransactionFixture):
        edition, pool = db.edition(with_license_pool=True)
        pool.licenses_owned = 10
        pool.licenses_available = 9
        pool.licenses_reserved = 8
        pool.patrons_in_hold_queue = 7

        msg, args = pool.circulation_changelog(1, 2, 3, 4)

        # Since all four circulation values changed, the message is as
        # long as it could possibly get.
        assert (
            'CHANGED %s "%s" %s (%s/%s) %s: %s=>%s %s: %s=>%s %s: %s=>%s %s: %s=>%s'
            == msg
        )
        assert args == (
            edition.medium,
            edition.title,
            edition.author,
            pool.identifier.type,
            pool.identifier.identifier,
            "OWN",
            1,
            10,
            "AVAIL",
            2,
            9,
            "RSRV",
            3,
            8,
            "HOLD",
            4,
            7,
        )

        # If only one circulation value changes, the message is a lot shorter.
        msg, args = pool.circulation_changelog(10, 9, 8, 15)
        assert 'CHANGED %s "%s" %s (%s/%s) %s: %s=>%s' == msg
        assert args == (
            edition.medium,
            edition.title,
            edition.author,
            pool.identifier.type,
            pool.identifier.identifier,
            "HOLD",
            15,
            7,
        )

        # This works even if, for whatever reason, the edition's
        # bibliographic data is missing.
        edition.title = None
        edition.author = None

        msg, args = pool.circulation_changelog(10, 9, 8, 15)
        assert "[NO TITLE]" == args[1]
        assert "[NO AUTHOR]" == args[2]

    def test_update_availability_from_delta(self, db: DatabaseTransactionFixture):
        """A LicensePool may have its availability information updated based
        on a single observed change.
        """

        edition, pool = db.edition(with_license_pool=True)
        assert None == pool.last_checked
        assert 1 == pool.licenses_owned
        assert 1 == pool.licenses_available

        add = CirculationEvent.DISTRIBUTOR_LICENSE_ADD
        checkout = CirculationEvent.DISTRIBUTOR_CHECKOUT
        analytics = MockAnalyticsProvider()
        assert 0 == analytics.count

        # This observation has no timestamp, but the pool has no
        # history, so we process it.
        pool.update_availability_from_delta(add, CirculationEvent.NO_DATE, 1, analytics)
        assert None == pool.last_checked
        assert 2 == pool.licenses_owned
        assert 2 == pool.licenses_available

        # Processing triggered two analytics events -- one for creating
        # the license pool and one for making it available.
        # No more DISTRIBUTOR events
        assert 0 == analytics.count

        # Now the pool has a history, and we can't fit an undated
        # observation into that history, so undated observations
        # have no effect on circulation data.
        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)
        pool.last_checked = yesterday
        pool.update_availability_from_delta(add, CirculationEvent.NO_DATE, 1, analytics)
        assert 2 == pool.licenses_owned
        assert yesterday == pool.last_checked

        # However, outdated events are passed on to analytics so that
        # we record the fact that they happened... at some point.
        # No more DISTRIBUTOR events
        assert 0 == analytics.count

        # This observation is more recent than the last time the pool
        # was checked, so it's processed and the last check time is
        # updated.
        pool.update_availability_from_delta(checkout, now, 1, analytics)
        assert 2 == pool.licenses_owned
        assert 1 == pool.licenses_available
        assert now == pool.last_checked
        # No more DISTRIBUTOR events
        assert 0 == analytics.count

        # This event is less recent than the last time the pool was
        # checked, so it's ignored. Processing it is likely to do more
        # harm than good.
        pool.update_availability_from_delta(add, yesterday, 1, analytics)
        assert 2 == pool.licenses_owned
        assert now == pool.last_checked

        # It's still logged to analytics, though.
        # No more DISTRIBUTOR events
        assert 0 == analytics.count

        # This event is new but does not actually cause the
        # circulation to change at all.
        pool.update_availability_from_delta(add, now, 0, analytics)
        assert 2 == pool.licenses_owned
        assert now == pool.last_checked

        # We still send the analytics event.
        # No more DISTRIBUTOR events
        assert 0 == analytics.count

    def test_calculate_change_from_one_event(self, db: DatabaseTransactionFixture):
        """Test the internal method called by update_availability_from_delta."""
        CE = CirculationEvent

        # Create a LicensePool with a large number of available licenses.
        edition, pool = db.edition(with_license_pool=True)
        pool.licenses_owned = 5
        pool.licenses_available = 4
        pool.licenses_reserved = 0
        pool.patrons_in_hold_queue = 0

        # Calibrate _calculate_change_from_one_event by sending it an
        # event that makes no difference. This lets us see what a
        # 'status quo' response from the method would look like.
        calc = pool._calculate_change_from_one_event
        assert (5, 4, 0, 0) == calc(CE.DISTRIBUTOR_CHECKIN, 0)

        # If there ever appear to be more licenses available than
        # owned, the number of owned licenses is left alone. It's
        # possible that we have more licenses than we thought, but
        # it's more likely that a license has expired or otherwise
        # been removed.
        assert (5, 5, 0, 0) == calc(CE.DISTRIBUTOR_CHECKIN, 3)

        # But we don't bump up the number of available licenses just
        # because one becomes available.
        assert (5, 5, 0, 0) == calc(CE.DISTRIBUTOR_CHECKIN, 1)

        # When you signal a hold on a book that's available, we assume
        # that the book has stopped being available.
        assert (5, 0, 0, 3) == calc(CE.DISTRIBUTOR_HOLD_PLACE, 3)

        # If a license stops being owned, it implicitly stops being
        # available. (But we don't know if the license that became
        # unavailable is one of the ones currently checked out to
        # someone, or one of the other ones.)
        assert (3, 3, 0, 0) == calc(CE.DISTRIBUTOR_LICENSE_REMOVE, 2)

        # If a license stops being available, it doesn't stop
        # being owned.
        assert (5, 3, 0, 0) == calc(CE.DISTRIBUTOR_CHECKOUT, 1)

        # None of these numbers will go below zero.
        assert (0, 0, 0, 0) == calc(CE.DISTRIBUTOR_LICENSE_REMOVE, 100)

        # Newly added licenses start out available if there are no
        # patrons in the hold queue.
        assert (6, 5, 0, 0) == calc(CE.DISTRIBUTOR_LICENSE_ADD, 1)

        # Now let's run some tests with a LicensePool that has a large holds
        # queue.
        pool.licenses_owned = 5
        pool.licenses_available = 0
        pool.licenses_reserved = 1
        pool.patrons_in_hold_queue = 3
        assert (5, 0, 1, 3) == calc(CE.DISTRIBUTOR_HOLD_PLACE, 0)

        # When you signal a hold on a book that already has holds, it
        # does nothing but increase the number of patrons in the hold
        # queue.
        assert (5, 0, 1, 6) == calc(CE.DISTRIBUTOR_HOLD_PLACE, 3)

        # A checkin event has no effect...
        assert (5, 0, 1, 3) == calc(CE.DISTRIBUTOR_CHECKIN, 1)

        # ...because it's presumed that it will be followed by an
        # availability notification event, which takes a patron off
        # the hold queue and adds them to the reserved list.
        assert (5, 0, 2, 2) == calc(CE.DISTRIBUTOR_AVAILABILITY_NOTIFY, 1)

        # The only exception is if the checkin event wipes out the
        # entire holds queue, in which case the number of available
        # licenses increases.  (But nothing else changes -- we're
        # still waiting for the availability notification events.)
        assert (5, 3, 1, 3) == calc(CE.DISTRIBUTOR_CHECKIN, 6)

        # Again, note that even though six copies were checked in,
        # we're not assuming we own more licenses than we
        # thought. It's more likely that the sixth license expired and
        # we weren't notified.

        # When there are no licenses available, a checkout event
        # draws from the pool of licenses reserved instead.
        assert (5, 0, 0, 3) == calc(CE.DISTRIBUTOR_CHECKOUT, 2)

        # Newly added licenses do not start out available if there are
        # patrons in the hold queue.
        assert (6, 0, 1, 3) == calc(CE.DISTRIBUTOR_LICENSE_ADD, 1)

    def test_loan_to_patron(self, db: DatabaseTransactionFixture):
        # Test our ability to loan LicensePools to Patrons.
        #
        # TODO: The path where the LicensePool is loaned to an
        # IntegrationClient rather than a Patron is currently not
        # directly tested.

        pool = db.licensepool(None)
        patron = db.patron()
        now = utc_now()
        patron.last_loan_activity_sync = now

        yesterday = now - datetime.timedelta(days=1)
        tomorrow = now + datetime.timedelta(days=1)

        fulfillment = pool.delivery_mechanisms[0]
        external_identifier = db.fresh_str()
        loan, is_new = pool.loan_to(
            patron,
            start=yesterday,
            end=tomorrow,
            fulfillment=fulfillment,
            external_identifier=external_identifier,
        )

        assert True == is_new
        assert isinstance(loan, Loan)
        assert pool == loan.license_pool
        assert patron == loan.patron
        assert yesterday == loan.start
        assert tomorrow == loan.end
        assert fulfillment == loan.fulfillment
        assert external_identifier == loan.external_identifier

        # Issuing a loan locally created uncertainty about a patron's
        # loans, since we don't know how the external vendor dealt
        # with the request. The last_loan_activity_sync has been
        # cleared out so we know to check back with the source of
        # truth.
        assert None == patron.last_loan_activity_sync

        # 'Creating' a loan that already exists does not create any
        # uncertainty.
        patron.last_loan_activity_sync = now
        loan2, is_new = pool.loan_to(
            patron,
            start=yesterday,
            end=tomorrow,
            fulfillment=fulfillment,
            external_identifier=external_identifier,
        )
        assert False == is_new
        assert loan == loan2
        assert now == patron.last_loan_activity_sync

    def test_on_hold_to_patron(self, db: DatabaseTransactionFixture):
        # Test our ability to put a Patron in the holds queue for a LicensePool.
        #
        # TODO: The path where the 'patron' is an IntegrationClient
        # rather than a Patron is currently not directly tested.

        pool = db.licensepool(None)
        patron = db.patron()
        now = utc_now()
        patron.last_loan_activity_sync = now

        yesterday = now - datetime.timedelta(days=1)
        tomorrow = now + datetime.timedelta(days=1)

        fulfillment = pool.delivery_mechanisms[0]
        position = 99
        external_identifier = db.fresh_str()
        hold, is_new = pool.on_hold_to(
            patron,
            start=yesterday,
            end=tomorrow,
            position=position,
            external_identifier=external_identifier,
        )

        assert True == is_new
        assert isinstance(hold, Hold)
        assert pool == hold.license_pool
        assert patron == hold.patron
        assert yesterday == hold.start
        assert tomorrow == hold.end
        assert position == hold.position
        assert external_identifier == hold.external_identifier

        # Issuing a hold locally created uncertainty about a patron's
        # loans, since we don't know how the external vendor dealt
        # with the request. The last_loan_activity_sync has been
        # cleared out so we know to check back with the source of
        # truth.
        assert None == patron.last_loan_activity_sync

        # 'Creating' a hold that already exists does not create any
        # uncertainty.
        patron.last_loan_activity_sync = now
        hold2, is_new = pool.on_hold_to(
            patron,
            start=yesterday,
            end=tomorrow,
            position=position,
            external_identifier=external_identifier,
        )
        assert False == is_new
        assert hold == hold2
        assert now == patron.last_loan_activity_sync


class TestLicensePoolDeliveryMechanism:
    def test_lpdm_change_may_change_open_access_status(
        self, db: DatabaseTransactionFixture
    ):
        # Here's a book that's not open access.
        edition, pool = db.edition(with_license_pool=True)
        assert False == pool.open_access

        # We're going to use LicensePoolDeliveryMechanism.set to
        # to give it a non-open-access LPDM.
        data_source = pool.data_source
        identifier = pool.identifier
        content_type = MediaTypes.EPUB_MEDIA_TYPE
        drm_scheme = DeliveryMechanism.NO_DRM
        LicensePoolDeliveryMechanism.set(
            data_source, identifier, content_type, drm_scheme, RightsStatus.IN_COPYRIGHT
        )

        # Now there's a way to get the book, but it's not open access.
        assert False == pool.open_access

        # Now give it an open-access LPDM.
        link, new = pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD,
            db.fresh_url(),
            data_source,
            content_type,
        )
        oa_lpdm = LicensePoolDeliveryMechanism.set(
            data_source,
            identifier,
            content_type,
            drm_scheme,
            RightsStatus.GENERIC_OPEN_ACCESS,
            link.resource,
        )

        # Now it's open access.
        assert True == pool.open_access

        # Delete the open-access LPDM, and it stops being open access.
        oa_lpdm.delete()
        assert False == pool.open_access

    def test_set_rights_status(self, db: DatabaseTransactionFixture):
        # Here's a non-open-access book.
        edition, pool = db.edition(with_license_pool=True)
        pool.open_access = False
        [lpdm] = pool.delivery_mechanisms

        # We set its rights status to 'in copyright', and nothing changes.
        uri = RightsStatus.IN_COPYRIGHT
        status = lpdm.set_rights_status(uri)
        assert status == lpdm.rights_status
        assert uri == status.uri
        assert RightsStatus.NAMES.get(uri) == status.name
        assert False == pool.open_access

        # Setting it again won't change anything.
        status2 = lpdm.set_rights_status(uri)
        assert status == status2

        # Set the rights status to a different URL, we change to a different
        # RightsStatus object.
        uri2 = "http://unknown"
        status3 = lpdm.set_rights_status(uri2)
        assert status != status3
        assert RightsStatus.UNKNOWN == status3.uri
        assert RightsStatus.NAMES.get(RightsStatus.UNKNOWN) == status3.name

        # Set the rights status to a URL that implies open access,
        # and the status of the LicensePool is changed.
        open_access_uri = RightsStatus.GENERIC_OPEN_ACCESS
        open_access_status = lpdm.set_rights_status(open_access_uri)
        assert open_access_uri == open_access_status.uri
        assert RightsStatus.NAMES.get(open_access_uri) == open_access_status.name
        assert True == pool.open_access

        # Set it back to a URL that does not imply open access, and
        # the status of the LicensePool is changed back.
        non_open_access_status = lpdm.set_rights_status(uri)
        assert False == pool.open_access

        # Now add a second delivery mechanism, so the pool has one
        # open-access and one commercial delivery mechanism.
        lpdm2 = pool.set_delivery_mechanism(
            MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.NO_DRM,
            RightsStatus.CC_BY,
            None,
        )
        assert 2 == len(pool.delivery_mechanisms)

        # Now the pool is open access again
        assert True == pool.open_access

        # But if we change the new delivery mechanism to non-open
        # access, the pool won't be open access anymore either.
        lpdm2.set_rights_status(uri)
        assert False == pool.open_access

    def test_uniqueness_constraint(self, db: DatabaseTransactionFixture):
        # with_open_access_download will create a LPDM
        # for the open-access download.
        edition, pool = db.edition(
            with_license_pool=True, with_open_access_download=True
        )
        [lpdm] = pool.delivery_mechanisms

        # We can create a second LPDM with the same data type and DRM status,
        # so long as the resource is different.
        link, new = pool.identifier.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD,
            db.fresh_url(),
            pool.data_source,
            "text/html",
        )
        lpdm2 = pool.set_delivery_mechanism(
            lpdm.delivery_mechanism.content_type,
            lpdm.delivery_mechanism.drm_scheme,
            lpdm.rights_status.uri,
            link.resource,
        )
        assert lpdm2.delivery_mechanism == lpdm.delivery_mechanism
        assert lpdm2.resource != lpdm.resource

        # We can even create an LPDM with the same data type and DRM
        # status and _no_ resource.
        lpdm3 = pool.set_delivery_mechanism(
            lpdm.delivery_mechanism.content_type,
            lpdm.delivery_mechanism.drm_scheme,
            lpdm.rights_status.uri,
            None,
        )
        assert lpdm3.delivery_mechanism == lpdm.delivery_mechanism
        assert None == lpdm3.resource

        # But we can't create a second such LPDM -- it violates a
        # constraint of a unique index.
        pytest.raises(
            IntegrityError,
            create,
            db.session,
            LicensePoolDeliveryMechanism,
            delivery_mechanism=lpdm3.delivery_mechanism,
            identifier=pool.identifier,
            data_source=pool.data_source,
            resource=None,
        )
        db.session.rollback()

    def test_compatible_with(self, db: DatabaseTransactionFixture):
        """Test the rules about which LicensePoolDeliveryMechanisms are
        mutually compatible and which are mutually exclusive.
        """

        edition, pool = db.edition(
            with_license_pool=True, with_open_access_download=True
        )
        [mech] = pool.delivery_mechanisms

        # Test the simple cases.
        assert False == mech.compatible_with(None)
        assert False == mech.compatible_with("Not a LicensePoolDeliveryMechanism")
        assert True == mech.compatible_with(mech)

        # Now let's set up a scenario that works and then see how it fails.
        db.add_generic_delivery_mechanism(pool)

        # This book has two different LicensePoolDeliveryMechanisms
        # with the same underlying DeliveryMechanism. They're
        # compatible.
        [mech1, mech2] = pool.delivery_mechanisms
        assert mech1.id != mech2.id
        assert mech1.delivery_mechanism == mech2.delivery_mechanism
        assert True == mech1.compatible_with(mech2)

        # The LicensePoolDeliveryMechanisms must identify the same
        # book from the same data source.
        mech1.data_source_id = db.fresh_id()
        assert False == mech1.compatible_with(mech2)

        mech1.data_source_id = mech2.data_source_id
        mech1.identifier_id = db.fresh_id()
        assert False == mech1.compatible_with(mech2)
        mech1.identifier_id = mech2.identifier_id

        # The underlying delivery mechanisms don't have to be exactly
        # the same, but they must be compatible.
        pdf_adobe, ignore = DeliveryMechanism.lookup(
            db.session, MediaTypes.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM
        )
        mech1.delivery_mechanism = pdf_adobe
        db.session.commit()
        assert False == mech1.compatible_with(mech2)

        streaming, ignore = DeliveryMechanism.lookup(
            db.session,
            DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_DRM,
        )
        mech1.delivery_mechanism = streaming
        db.session.commit()
        assert True == mech1.compatible_with(mech2)

    def test_compatible_with_calls_compatible_with_on_deliverymechanism(
        self, db: DatabaseTransactionFixture
    ):
        # Create two LicensePoolDeliveryMechanisms with different
        # media types.
        edition, pool = db.edition(
            with_license_pool=True, with_open_access_download=True
        )
        [mech1] = pool.delivery_mechanisms
        mech2 = db.add_generic_delivery_mechanism(pool)
        mech2.delivery_mechanism, ignore = DeliveryMechanism.lookup(
            db.session, MediaTypes.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM
        )
        db.session.commit()

        assert True == mech1.is_open_access
        assert False == mech2.is_open_access

        # Determining whether the mechanisms are compatible requires
        # calling compatible_with on the first mechanism's
        # DeliveryMechanism, passing in the second DeliveryMechanism
        # plus the answer to 'are both LicensePoolDeliveryMechanisms
        # open-access?'
        class Mock:
            called_with = None

            @classmethod
            def compatible_with(cls, other, open_access):
                cls.called_with = (other, open_access)
                return True

        mech1.delivery_mechanism.compatible_with = Mock.compatible_with

        # Call compatible_with, and the mock method is called with the
        # second DeliveryMechanism and (since one of the
        # LicensePoolDeliveryMechanisms is not open-access) the value
        # False.
        mech1.compatible_with(mech2)
        assert (mech2.delivery_mechanism, False) == Mock.called_with

        # If both LicensePoolDeliveryMechanisms are open-access,
        # True is passed in instead, so that
        # DeliveryMechanism.compatible_with() applies the less strict
        # compatibility rules for open-access fulfillment.
        mech2.set_rights_status(RightsStatus.GENERIC_OPEN_ACCESS)
        mech1.compatible_with(mech2)
        assert (mech2.delivery_mechanism, True) == Mock.called_with

    @pytest.mark.parametrize(
        "_,data_source,identifier,delivery_mechanism",
        [("ascii_sy", "a", "a", "a"), ("", "ą", "ą", "ą")],
    )
    def test_repr(self, _, data_source, identifier, delivery_mechanism):
        """Test that LicensePoolDeliveryMechanism.__repr__ correctly works for both ASCII and non-ASCII symbols.

        :param _: Name of the test case
        :type _: str

        :param data_source: String representation of the data source
        :type data_source: str

        :param identifier: String representation of the publication's identifier
        :type identifier: str

        :param delivery_mechanism: String representation of the delivery mechanism
        :type delivery_mechanism: str
        """
        # Arrange
        data_source_mock = DataSource()
        data_source_mock.__str__ = MagicMock(return_value=data_source)

        identifier_mock = Identifier()
        identifier_mock.__repr__ = MagicMock(return_value=identifier)

        delivery_mechanism_mock = DeliveryMechanism()
        delivery_mechanism_mock.__repr__ = MagicMock(return_value=delivery_mechanism)

        license_delivery_mechanism_mock = LicensePoolDeliveryMechanism()
        license_delivery_mechanism_mock.data_source = PropertyMock(
            return_value=data_source_mock
        )
        license_delivery_mechanism_mock.identifier = PropertyMock(
            return_value=identifier_mock
        )
        license_delivery_mechanism_mock.delivery_mechanism = PropertyMock(
            return_value=delivery_mechanism_mock
        )

        # Act
        # NOTE: we are not interested in the result returned by repr,
        # we just want to make sure that repr doesn't throw any unexpected exceptions
        repr(license_delivery_mechanism_mock)


class TestFormatPriorities:
    @pytest.fixture
    def mock_delivery(
        self,
    ) -> Callable[[Optional[str], Optional[str]], DeliveryMechanism]:
        def delivery_mechanism(
            drm_scheme: Optional[str] = None,
            content_type: Optional[str] = "application/epub+zip",
        ) -> DeliveryMechanism:
            def _delivery_eq(self, other):
                return (
                    self.drm_scheme == other.drm_scheme
                    and self.content_type == other.content_type
                )

            def _delivery_repr(self):
                return f"DeliveryMechanism(drm_scheme={self.drm_scheme}, content_type={self.content_type})"

            _delivery = MagicMock(spec=DeliveryMechanism)
            _delivery.drm_scheme = drm_scheme
            _delivery.content_type = content_type
            setattr(_delivery, "__eq__", _delivery_eq)
            setattr(_delivery, "__repr__", _delivery_repr)

            return _delivery

        return delivery_mechanism

    @pytest.fixture
    def mock_mechanism(
        self, mock_delivery
    ) -> Callable[[Optional[str], Optional[str]], LicensePoolDeliveryMechanism]:
        def mechanism(
            drm_scheme: Optional[str] = None,
            content_type: Optional[str] = "application/epub+zip",
        ) -> LicensePoolDeliveryMechanism:
            def _mechanism_eq(self, other):
                return self.delivery_mechanism == other.delivery_mechanism

            def _mechanism_repr(self):
                return f"LicensePoolDeliveryMechanism(delivery_mechanism={self.delivery_mechanism})"

            _mechanism = MagicMock(spec=LicensePoolDeliveryMechanism)
            _mechanism.delivery_mechanism = mock_delivery(drm_scheme, content_type)
            setattr(_mechanism, "__eq__", _mechanism_eq)
            setattr(_mechanism, "__repr__", _mechanism_repr)
            return _mechanism

        return mechanism

    @pytest.fixture
    def sample_data_0(self, mock_mechanism):
        """An arrangement of delivery mechanisms taken from a working database."""
        return [
            mock_mechanism("application/vnd.adobe.adept+xml", "application/epub+zip"),
            mock_mechanism(
                "Libby DRM",
                "application/vnd.overdrive.circulation.api+json;profile=audiobook",
            ),
            mock_mechanism(None, "application/audiobook+json"),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json", "application/pdf"
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json",
                "application/epub+zip",
            ),
            mock_mechanism(None, "application/epub+zip"),
            mock_mechanism(None, "application/pdf"),
            mock_mechanism(
                "application/vnd.librarysimplified.findaway.license+json", None
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json",
                "application/audiobook+json",
            ),
            mock_mechanism(None, "application/kepub+zip"),
            mock_mechanism(None, "application/x-mobipocket-ebook"),
            mock_mechanism(None, "application/x-mobi8-ebook"),
            mock_mechanism(None, "text/plain; charset=utf-8"),
            mock_mechanism(None, "application/octet-stream"),
            mock_mechanism(None, "text/html; charset=utf-8"),
            mock_mechanism(
                "http://www.feedbooks.com/audiobooks/access-restriction",
                "application/audiobook+json",
            ),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json",
                "application/audiobook+lcp",
            ),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json", "application/epub+zip"
            ),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json",
                "application/pdf",
            ),
        ]

    def test_identity_empty(self):
        priorities = FormatPriorities(
            prioritized_drm_schemes=[],
            prioritized_content_types=[],
            hidden_content_types=[],
            deprioritize_lcp_non_epubs=False,
        )
        assert [] == priorities.prioritize_mechanisms([])

    def test_identity_one(self, mock_mechanism):
        priorities = FormatPriorities(
            prioritized_drm_schemes=[],
            prioritized_content_types=[],
            hidden_content_types=[],
            deprioritize_lcp_non_epubs=False,
        )
        mechanism_0 = mock_mechanism()
        assert [mechanism_0] == priorities.prioritize_mechanisms([mechanism_0])

    def test_hidden_types_excluded(self, mock_mechanism):
        priorities = FormatPriorities(
            prioritized_drm_schemes=[],
            prioritized_content_types=[],
            hidden_content_types=["application/epub+zip"],
            deprioritize_lcp_non_epubs=False,
        )
        mechanism_0 = mock_mechanism()
        assert [] == priorities.prioritize_mechanisms([mechanism_0])

    def test_non_prioritized_drm_0(self, sample_data_0):
        priorities = FormatPriorities(
            prioritized_drm_schemes=[],
            prioritized_content_types=[],
            hidden_content_types=[],
            deprioritize_lcp_non_epubs=False,
        )
        expected = sample_data_0.copy()
        assert expected == priorities.prioritize_mechanisms(sample_data_0)

    def test_prioritized_content_type_0(self, mock_mechanism, sample_data_0):
        """A simple configuration where an unusual content type is prioritized."""
        priorities = FormatPriorities(
            prioritized_drm_schemes=[],
            prioritized_content_types=["application/x-mobi8-ebook"],
            hidden_content_types=[],
            deprioritize_lcp_non_epubs=False,
        )

        # We expect the mobi8-ebook format to be pushed to the front of the list.
        # All other non-DRM formats are moved to the start of the list in a more or less arbitrary order.
        expected = [
            mock_mechanism(None, "application/x-mobi8-ebook"),
            mock_mechanism(None, "application/audiobook+json"),
            mock_mechanism(None, "application/epub+zip"),
            mock_mechanism(None, "application/pdf"),
            mock_mechanism(None, "application/kepub+zip"),
            mock_mechanism(None, "application/x-mobipocket-ebook"),
            mock_mechanism(None, "text/plain; charset=utf-8"),
            mock_mechanism(None, "application/octet-stream"),
            mock_mechanism(None, "text/html; charset=utf-8"),
            mock_mechanism("application/vnd.adobe.adept+xml", "application/epub+zip"),
            mock_mechanism(
                "Libby DRM",
                "application/vnd.overdrive.circulation.api+json;profile=audiobook",
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json", "application/pdf"
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json",
                "application/epub+zip",
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.findaway.license+json", None
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json",
                "application/audiobook+json",
            ),
            mock_mechanism(
                "http://www.feedbooks.com/audiobooks/access-restriction",
                "application/audiobook+json",
            ),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json",
                "application/audiobook+lcp",
            ),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json", "application/epub+zip"
            ),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json", "application/pdf"
            ),
        ]

        received = priorities.prioritize_mechanisms(sample_data_0)
        assert expected == received
        assert len(sample_data_0) == len(received)

    def test_prioritized_content_type_1(self, mock_mechanism, sample_data_0):
        """A test of a more aggressive configuration where multiple content types
        and DRM schemes are prioritized."""
        priorities = FormatPriorities(
            prioritized_drm_schemes=[
                "application/vnd.readium.lcp.license.v1.0+json",
                "application/vnd.librarysimplified.bearer-token+json",
                "application/vnd.adobe.adept+xml",
            ],
            prioritized_content_types=[
                "application/epub+zip",
                "application/audiobook+json",
                "application/audiobook+lcp",
                "application/pdf",
            ],
            hidden_content_types=[
                "application/x-mobipocket-ebook",
                "application/x-mobi8-ebook",
                "application/kepub+zip",
                "text/plain; charset=utf-8",
                "application/octet-stream",
                "text/html; charset=utf-8",
            ],
            deprioritize_lcp_non_epubs=False,
        )
        expected = [
            mock_mechanism(None, "application/epub+zip"),
            mock_mechanism(None, "application/audiobook+json"),
            mock_mechanism(None, "application/pdf"),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json", "application/epub+zip"
            ),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json",
                "application/audiobook+lcp",
            ),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json",
                "application/pdf",
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json",
                "application/epub+zip",
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json",
                "application/audiobook+json",
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json", "application/pdf"
            ),
            mock_mechanism("application/vnd.adobe.adept+xml", "application/epub+zip"),
            mock_mechanism(
                "http://www.feedbooks.com/audiobooks/access-restriction",
                "application/audiobook+json",
            ),
            mock_mechanism(
                "Libby DRM",
                "application/vnd.overdrive.circulation.api+json;profile=audiobook",
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.findaway.license+json", None
            ),
        ]
        received = priorities.prioritize_mechanisms(sample_data_0)
        assert expected == received

    def test_prioritized_content_lcp_audiobooks(self, mock_mechanism, sample_data_0):
        """A test of configuration where LCP audiobooks are artificially deprioritized, whilst
        keeping the priorities of everything else the same."""
        priorities = FormatPriorities(
            prioritized_drm_schemes=[
                "application/vnd.readium.lcp.license.v1.0+json",
                "application/vnd.librarysimplified.bearer-token+json",
                "application/vnd.adobe.adept+xml",
            ],
            prioritized_content_types=[
                "application/epub+zip",
                "application/audiobook+json",
                "application/pdf",
                "application/audiobook+lcp",
            ],
            hidden_content_types=[
                "application/x-mobipocket-ebook",
                "application/x-mobi8-ebook",
                "application/kepub+zip",
                "text/plain; charset=utf-8",
                "application/octet-stream",
                "text/html; charset=utf-8",
            ],
            deprioritize_lcp_non_epubs=True,
        )
        expected = [
            mock_mechanism(None, "application/epub+zip"),
            mock_mechanism(None, "application/audiobook+json"),
            mock_mechanism(None, "application/pdf"),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json", "application/epub+zip"
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json",
                "application/epub+zip",
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json",
                "application/audiobook+json",
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.bearer-token+json",
                "application/pdf",
            ),
            mock_mechanism("application/vnd.adobe.adept+xml", "application/epub+zip"),
            mock_mechanism(
                "http://www.feedbooks.com/audiobooks/access-restriction",
                "application/audiobook+json",
            ),
            mock_mechanism(
                "Libby DRM",
                "application/vnd.overdrive.circulation.api+json;profile=audiobook",
            ),
            mock_mechanism(
                "application/vnd.librarysimplified.findaway.license+json", None
            ),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json", "application/pdf"
            ),
            mock_mechanism(
                "application/vnd.readium.lcp.license.v1.0+json",
                "application/audiobook+lcp",
            ),
        ]
        received = priorities.prioritize_mechanisms(sample_data_0)
        assert expected == received

    @staticmethod
    def _show(mechanisms):
        output = []
        for mechanism in mechanisms:
            item = {}
            if mechanism.delivery_mechanism.drm_scheme:
                item["drm"] = mechanism.delivery_mechanism.drm_scheme
            if mechanism.delivery_mechanism.content_type:
                item["type"] = mechanism.delivery_mechanism.content_type
            output.append(item)
        print(json.dumps(output, indent=2))

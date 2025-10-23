import datetime
from datetime import timedelta
from functools import partial
from typing import Any
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from bidict import frozenbidict
from sqlalchemy.exc import IntegrityError

from palace.manager.api.circulation.exceptions import CannotHold, CannotLoan
from palace.manager.opds.odl.info import LicenseStatus
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.collection import CollectionMissing
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    DeliveryMechanismTuple,
    LicensePool,
    LicensePoolDeliveryMechanism,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.patron import Hold, Loan
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.sqlalchemy.util import create
from palace.manager.util import first_or_default
from palace.manager.util.datetime_helpers import utc_now
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
        with_drm_args: dict[str, Any] = dict(content_type="type1", drm_scheme="scheme1")
        create(session, dm, **with_drm_args)
        pytest.raises(IntegrityError, create, session, dm, **with_drm_args)
        session.rollback()

        # You can't create two DeliveryMechanisms with the same value
        # for content_type and a null value for drm_scheme.
        without_drm_args: dict[str, Any] = dict(content_type="type1", drm_scheme=None)
        create(session, dm, **without_drm_args)
        pytest.raises(IntegrityError, create, session, dm, **without_drm_args)
        session.rollback()

    def test_sort(self) -> None:
        def create_lpdm(
            content_type: str | None, drm_scheme: str | None
        ) -> LicensePoolDeliveryMechanism:
            return LicensePoolDeliveryMechanism(
                delivery_mechanism=DeliveryMechanism(
                    content_type=content_type, drm_scheme=drm_scheme
                )
            )

        no_drm = create_lpdm(Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM)
        adobe_drm = create_lpdm(
            Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM
        )
        bearer_token = create_lpdm(
            Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.BEARER_TOKEN
        )
        unknown_drm_1 = create_lpdm(Representation.EPUB_MEDIA_TYPE, "unknown_drm_1")
        unknown_drm_2 = create_lpdm(Representation.EPUB_MEDIA_TYPE, "unknown_drm_2")

        # Even if Adobe DRM comes first, we sort it after no DRM.
        assert DeliveryMechanism.sort([adobe_drm, no_drm]) == [no_drm, adobe_drm]

        # Bearer token is also sorted before Adobe DRM.
        assert DeliveryMechanism.sort([adobe_drm, bearer_token]) == [
            bearer_token,
            adobe_drm,
        ]

        # If all three are present, no DRM comes first, then bearer token, then Adobe DRM.
        assert DeliveryMechanism.sort([no_drm, adobe_drm, bearer_token]) == [
            no_drm,
            bearer_token,
            adobe_drm,
        ]

        # If we have unknown DRM schemes, they are sorted last, but maintain their relative order.
        assert DeliveryMechanism.sort(
            [adobe_drm, unknown_drm_1, unknown_drm_2, no_drm]
        ) == [no_drm, adobe_drm, unknown_drm_1, unknown_drm_2]

        assert DeliveryMechanism.sort(
            [unknown_drm_2, adobe_drm, unknown_drm_1, no_drm]
        ) == [no_drm, adobe_drm, unknown_drm_2, unknown_drm_1]

    def test_as_tuple(self) -> None:
        content_type = Representation.EPUB_MEDIA_TYPE
        drm_scheme = DeliveryMechanism.NO_DRM
        dm = DeliveryMechanism(content_type=content_type, drm_scheme=drm_scheme)

        assert dm.as_tuple == (content_type, drm_scheme)
        assert dm.as_tuple == DeliveryMechanismTuple(content_type, drm_scheme)


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


class LicenseTestFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.db = db
        self.pool = db.licensepool(None, collection=db.default_collection())

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
            terms_concurrency=5,
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

        self.inactive_pool = db.licensepool(
            None, collection=db.default_inactive_collection()
        )
        self.inactive = db.license(
            self.inactive_pool,
            status=LicenseStatus.available,
        )


@pytest.fixture(scope="function")
def licenses(db: DatabaseTransactionFixture) -> LicenseTestFixture:
    return LicenseTestFixture(db)


class TestLicense:
    def test_loan_to(self, licenses: LicenseTestFixture):
        # Verify that loaning a license also loans its pool.
        pool = licenses.pool
        collection = pool.collection
        license = licenses.time_limited

        patron1 = licenses.db.patron()
        patron2 = licenses.db.patron()
        assert patron1 != patron2

        loan, is_new = license.loan_to(patron1)
        assert license == loan.license
        assert pool == loan.license_pool
        assert True == is_new

        loan2, is_new = license.loan_to(patron1)
        assert loan == loan2
        assert license == loan2.license
        assert pool == loan2.license_pool
        assert False == is_new

        # Now the collection becomes inactive.
        db = licenses.db
        db.make_collection_inactive(collection)

        # Getting the existing loan should work, even if the collection is inactive.
        loan3, is_new = license.loan_to(patron1)
        assert loan3 == loan
        assert loan3.license == license
        assert loan3.license_pool == pool
        assert is_new is False

        # However, trying to get a new loan should fail.
        with pytest.raises(CannotLoan) as exc:
            license.loan_to(patron2)
        assert "Cannot create a new loan on an inactive collection" in str(exc.value)

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
            ("time_and_loan_limited", False, True, True, False, 1, 1),
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
        licenses: LicenseTestFixture,
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
        self, license_type, left, available, licenses: LicenseTestFixture
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
        self, license_params, left, available, licenses: LicenseTestFixture
    ):
        l = licenses.db.license(licenses.pool, **license_params)
        l.checkin()
        assert left == l.checkouts_left
        assert available == l.checkouts_available

    def test_best_available_licenses(self, licenses: LicenseTestFixture):
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

        # First, make sure the overall order is correct
        assert licenses.pool.best_available_licenses() == [
            time_limited_2,
            licenses.time_limited,
            licenses.perpetual,
            licenses.time_and_loan_limited,
            licenses.loan_limited,
            loan_limited_2,
        ]

        # We use the time-limited license that's expiring first.
        assert (
            first_or_default(licenses.pool.best_available_licenses()) == time_limited_2
        )
        time_limited_2.checkout()

        # When that's not available, we use the next time-limited license.
        assert (
            first_or_default(licenses.pool.best_available_licenses())
            == licenses.time_limited
        )
        licenses.time_limited.checkout()

        # Next is the perpetual license.
        assert (
            first_or_default(licenses.pool.best_available_licenses())
            == licenses.perpetual
        )
        licenses.perpetual.checkout()

        # Next up is the time-and-loan-limited license.
        assert (
            first_or_default(licenses.pool.best_available_licenses())
            == licenses.time_and_loan_limited
        )
        licenses.time_and_loan_limited.checkout()

        # Then the loan-limited license with the most remaining checkouts.
        assert (
            first_or_default(licenses.pool.best_available_licenses())
            == licenses.loan_limited
        )
        licenses.loan_limited.checkout()

        # That license allows 2 concurrent checkouts, so it's still the
        # best license until it's checked out again.
        assert (
            first_or_default(licenses.pool.best_available_licenses())
            == licenses.loan_limited
        )
        licenses.loan_limited.checkout()

        # There's one more loan-limited license.
        assert (
            first_or_default(licenses.pool.best_available_licenses()) == loan_limited_2
        )
        loan_limited_2.checkout()

        # Now all licenses are either loaned out or expired.
        assert licenses.pool.best_available_licenses() == []


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

    @patch.object(
        Identifier,
        "DEPRECATED_NAMES",
        frozenbidict({"deprecated": Identifier.GUTENBERG_ID}),
    )
    def test_for_foreign_id_with_deprecated_type(self, db: DatabaseTransactionFixture):
        # Create using an identifier type that has been deprecated.
        pool, _ = LicensePool.for_foreign_id(
            db.session,
            DataSource.GUTENBERG,
            "deprecated",
            "541",
            collection=db.collection(),
        )
        assert pool is not None
        assert pool.identifier.type == Identifier.GUTENBERG_ID

        datasource = DataSource.lookup(db.session, DataSource.GUTENBERG)
        datasource.primary_identifier_type = "deprecated"
        pool, _ = LicensePool.for_foreign_id(
            db.session,
            DataSource.GUTENBERG,
            Identifier.GUTENBERG_ID,
            "541",
            collection=db.collection(),
        )
        assert pool is not None
        assert pool.identifier.type == Identifier.GUTENBERG_ID

    def test_for_foreign_id_with_autocreate_false(self, db: DatabaseTransactionFixture):
        # If autocreate is False, we get None back when no LicensePool
        # exists.
        source = "test data source"
        id_type = "test identifier type"
        identifier = db.fresh_str()
        collection = db.default_collection()

        pool, was_new = LicensePool.for_foreign_id(
            db.session,
            source,
            id_type,
            identifier,
            collection=collection,
            autocreate=False,
        )
        assert pool is None
        assert was_new is False

        # The call did not create the datasource or identifier.
        assert DataSource.lookup(db.session, source, autocreate=False) is None
        assert Identifier.for_foreign_id(
            db.session, id_type, identifier, autocreate=False
        ) == (None, False)

        # Create the datasource, but not the identifier
        DataSource.lookup(db.session, source, autocreate=True)

        # The call should still return None because the identifier doesn't exist, and it should not
        # create the identifier.
        pool, was_new = LicensePool.for_foreign_id(
            db.session,
            source,
            id_type,
            identifier,
            collection=collection,
            autocreate=False,
        )
        assert pool is None
        assert was_new is False

        # The call did not create the identifier
        assert Identifier.for_foreign_id(
            db.session, id_type, identifier, autocreate=False
        ) == (None, False)

        # Create the licensepool
        LicensePool.for_foreign_id(
            db.session,
            source,
            id_type,
            identifier,
            collection=collection,
        )

        pool, was_new = LicensePool.for_foreign_id(
            db.session,
            source,
            id_type,
            identifier,
            collection=collection,
            autocreate=False,
        )
        assert pool is not None
        assert was_new is False

        assert pool.identifier.type == id_type
        assert pool.identifier.identifier == identifier
        assert pool.data_source.name == source

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

    def test_get_active_holds(self, db: DatabaseTransactionFixture):
        pool = db.licensepool(None)
        decoy_pool = db.licensepool(None)

        last_week = utc_now() - timedelta(days=7)
        yesterday = utc_now() - timedelta(days=1)
        tomorrow = utc_now() + timedelta(days=1)

        # Holds that should be considered active.
        active_hold1, _ = pool.on_hold_to(
            db.patron(), start=last_week, end=None, position=None
        )
        active_hold2, _ = pool.on_hold_to(
            db.patron(), start=last_week, end=None, position=1
        )
        # This one is a tricky case. It's active because the hold is not in position 0, so end
        # is the estimated availability date, not the date that the hold expires. It is possible
        # for a hold not to be ready by its estimated availability date, so it's still active.
        active_hold3, _ = pool.on_hold_to(
            db.patron(), start=last_week, end=yesterday, position=2
        )
        active_hold4, _ = pool.on_hold_to(
            db.patron(), start=yesterday, end=tomorrow, position=0
        )

        # Holds that should not be considered active.
        inactive_hold1, _ = pool.on_hold_to(
            db.patron(), start=last_week, end=yesterday, position=0
        )

        # Holds on a different pool.
        decoy_pool.on_hold_to(db.patron(), start=last_week, end=tomorrow, position=1)

        active_holds = pool.get_active_holds()
        assert len(active_holds) == 4
        assert set(active_holds) == {
            active_hold1,
            active_hold2,
            active_hold3,
            active_hold4,
        }

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
        assert (utc_now() - work.last_update_time) < datetime.timedelta(seconds=2)

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
        admin.add_contributor(jane, Contributor.Role.AUTHOR)
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

        # This observation has no timestamp, but the pool has no
        # history, so we process it.
        pool.update_availability_from_delta(add, CirculationEvent.NO_DATE, 1)
        assert None == pool.last_checked
        assert 2 == pool.licenses_owned
        assert 2 == pool.licenses_available

        # Now the pool has a history, and we can't fit an undated
        # observation into that history, so undated observations
        # have no effect on circulation data.
        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)
        pool.last_checked = yesterday
        pool.update_availability_from_delta(add, CirculationEvent.NO_DATE, 1)
        assert 2 == pool.licenses_owned
        assert yesterday == pool.last_checked

        # This observation is more recent than the last time the pool
        # was checked, so it's processed and the last check time is
        # updated.
        pool.update_availability_from_delta(checkout, now, 1)
        assert 2 == pool.licenses_owned
        assert 1 == pool.licenses_available
        assert now == pool.last_checked

        # This event is less recent than the last time the pool was
        # checked, so it's ignored. Processing it is likely to do more
        # harm than good.
        pool.update_availability_from_delta(add, yesterday, 1)
        assert 2 == pool.licenses_owned
        assert now == pool.last_checked

        # This event is new but does not actually cause the
        # circulation to change at all.
        pool.update_availability_from_delta(add, now, 0)
        assert 2 == pool.licenses_owned
        assert now == pool.last_checked

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

        pool = db.licensepool(None)
        patron = db.patron()
        now = utc_now()

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

        assert is_new is True
        assert isinstance(loan, Loan)
        assert pool == loan.license_pool
        assert patron == loan.patron
        assert yesterday == loan.start
        assert tomorrow == loan.end
        assert fulfillment == loan.fulfillment
        assert external_identifier == loan.external_identifier

        # 'Creating' a loan that already exists returns the existing loan.
        loan2, is_new = pool.loan_to(
            patron,
            start=yesterday,
            end=tomorrow,
            fulfillment=fulfillment,
            external_identifier=external_identifier,
        )
        assert is_new == False
        assert loan == loan2

    def test_on_hold_to_patron(self, db: DatabaseTransactionFixture):
        # Test our ability to put a Patron in the holds queue for a LicensePool.

        pool = db.licensepool(None)
        collection = pool.collection

        patron1 = db.patron()
        patron2 = db.patron()
        assert patron1 != patron2

        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)
        tomorrow = now + datetime.timedelta(days=1)

        fulfillment = pool.delivery_mechanisms[0]
        position = 99
        hold, is_new = pool.on_hold_to(
            patron1,
            start=yesterday,
            end=tomorrow,
            position=position,
        )

        assert is_new is True
        assert isinstance(hold, Hold)
        assert pool == hold.license_pool
        assert patron1 == hold.patron
        assert yesterday == hold.start
        assert tomorrow == hold.end
        assert position == hold.position

        # 'Creating' a hold that already exists returns the existing hold.
        hold2, is_new = pool.on_hold_to(
            patron1,
            start=yesterday,
            end=tomorrow,
            position=position,
        )
        assert is_new is False
        assert hold == hold2

        # Now the collection becomes inactive.
        db.make_collection_inactive(collection)

        # Getting the existing hold should work, even if the collection is inactive.
        hold3, is_new = pool.on_hold_to(
            patron1,
            start=yesterday,
            end=tomorrow,
            position=position,
        )
        assert hold3 == hold
        assert is_new is False

        # However, trying to get a new hold should fail.
        with pytest.raises(CannotHold) as exc:
            pool.on_hold_to(patron2)
        assert "Cannot create a new hold on an inactive collection" in str(exc.value)

    def test_delivery_mechanisms(self, db: DatabaseTransactionFixture) -> None:
        # Test the delivery_mechanisms and available_delivery_mechanisms property.
        pool = db.licensepool(None)

        # The pool is created with one delivery mechanism.
        assert pool.delivery_mechanisms == pool.available_delivery_mechanisms
        assert len(pool.available_delivery_mechanisms) == 1
        [lpdm1] = pool.available_delivery_mechanisms
        assert lpdm1.available

        # Set lpdm1 to unavailable and create three new delivery mechanisms, two of which are available.
        lpdm1.available = False
        lpmd2 = pool.set_delivery_mechanism(
            "lpdm2",
            None,
            None,
        )
        lpmd3 = pool.set_delivery_mechanism(
            "lpdm3",
            None,
            None,
        )
        lpmd4 = pool.set_delivery_mechanism(
            "lpdm4",
            None,
            None,
            available=False,
        )

        # The properties should reflect the new state.
        assert set(pool.delivery_mechanisms) == {lpdm1, lpmd2, lpmd3, lpmd4}
        assert set(pool.available_delivery_mechanisms) == {lpmd2, lpmd3}


class TestLicensePoolDeliveryMechanism:
    def test_set(self, db: DatabaseTransactionFixture) -> None:
        datasource = DataSource.lookup(
            db.session, DataSource.GUTENBERG, autocreate=True
        )
        identifier = db.identifier()

        assert db.session.query(LicensePoolDeliveryMechanism).count() == 0

        lpdm_set = partial(
            LicensePoolDeliveryMechanism.set,
            data_source=datasource,
            identifier=identifier,
            drm_scheme=DeliveryMechanism.NO_DRM,
            rights_uri=RightsStatus.IN_COPYRIGHT,
        )

        # Create a LicensePoolDeliveryMechanism.
        lpdm = lpdm_set(
            content_type=MediaTypes.EPUB_MEDIA_TYPE,
            available=False,
            update_available=False,
        )

        assert lpdm.data_source == datasource
        assert lpdm.identifier == identifier
        assert lpdm.available == False
        assert lpdm.delivery_mechanism.content_type == MediaTypes.EPUB_MEDIA_TYPE
        assert lpdm.delivery_mechanism.drm_scheme is None
        assert lpdm.rights_status.uri == RightsStatus.IN_COPYRIGHT

        assert db.session.query(LicensePoolDeliveryMechanism).count() == 1

        # Calling set again with the same content_type should return the existing LicensePoolDeliveryMechanism.
        lpdm2 = lpdm_set(content_type=MediaTypes.EPUB_MEDIA_TYPE)
        assert lpdm2 is lpdm
        assert db.session.query(LicensePoolDeliveryMechanism).count() == 1
        assert lpdm2.available is False

        # LicensePoolDeliveryMechanism.available is updated when calling set()
        lpdm = lpdm_set(content_type=MediaTypes.EPUB_MEDIA_TYPE, available=True)
        assert lpdm.available is True

        # Unless the update_available flag is set to False, then available is only set on creation
        lpdm = lpdm_set(
            content_type=MediaTypes.EPUB_MEDIA_TYPE,
            available=False,
            update_available=False,
        )
        assert lpdm.available is True

        # Create a new LicensePoolDeliveryMechanism with a different content type.
        lpdm = lpdm_set(content_type=MediaTypes.PDF_MEDIA_TYPE)
        assert lpdm.available is True
        assert lpdm.delivery_mechanism.content_type == MediaTypes.PDF_MEDIA_TYPE
        assert db.session.query(LicensePoolDeliveryMechanism).count() == 2

        # Its available status is changed when calling set() with an available argument
        lpdm = lpdm_set(content_type=MediaTypes.PDF_MEDIA_TYPE, available=False)
        assert lpdm.available == False

        # Unless the update_available flag is set to False, then available is only set on creation
        lpdm = lpdm_set(
            content_type=MediaTypes.PDF_MEDIA_TYPE,
            available=True,
            update_available=False,
        )
        assert lpdm.available == False

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
        "data_source,identifier,delivery_mechanism",
        [
            pytest.param("a", "a", "a", id="ascii_sy"),
            pytest.param("ą", "ą", "ą", id=""),
        ],
    )
    def test_repr(self, data_source, identifier, delivery_mechanism):
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

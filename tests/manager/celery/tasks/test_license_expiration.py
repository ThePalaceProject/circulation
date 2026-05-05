import datetime

from freezegun import freeze_time

from palace.util.datetime_helpers import utc_now

from palace.manager.celery.tasks.license_expiration import update_expired_licenses
from palace.manager.sqlalchemy.model.licensing import LicensePoolType
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture


class TestUpdateExpiredLicenses:
    def test_no_expired_licenses(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ) -> None:
        """Task runs cleanly when no licenses have expired."""
        edition = db.edition()
        pool = db.licensepool(edition)
        pool.type = LicensePoolType.AGGREGATED
        future = utc_now() + datetime.timedelta(days=30)
        db.license(pool, expires=future, checkouts_available=5)
        pool.update_availability_from_licenses()

        original_available = pool.licenses_available

        update_expired_licenses.delay().wait()

        db.session.refresh(pool)
        assert pool.licenses_available == original_available

    @freeze_time()
    def test_pool_with_newly_expired_license(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ) -> None:
        """A pool whose license expired after the last update has its availability recalculated."""
        now = utc_now()
        expired_at = now - datetime.timedelta(hours=1)
        last_updated = now - datetime.timedelta(hours=2)

        edition = db.edition()
        pool = db.licensepool(edition)
        pool.type = LicensePoolType.AGGREGATED

        # One active license, one that has already expired
        db.license(pool, expires=None, checkouts_available=3, terms_concurrency=3)
        db.license(pool, expires=expired_at, checkouts_available=2, terms_concurrency=2)

        # Simulate the pool being last checked before the expiry
        pool.last_checked = last_updated
        pool.licenses_available = 5  # stale — includes the now-expired license

        update_expired_licenses.delay().wait()

        db.session.refresh(pool)
        # Only the active license contributes to availability
        assert pool.licenses_available == 3
        assert pool.last_checked == now

    @freeze_time()
    def test_already_processed_expiry_skipped(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ) -> None:
        """A pool whose last_checked is after the license's expiry is not re-processed."""
        now = utc_now()
        expired_at = now - datetime.timedelta(hours=2)
        last_updated = now - datetime.timedelta(hours=1)  # checked AFTER expiry

        edition = db.edition()
        pool = db.licensepool(edition)
        pool.type = LicensePoolType.AGGREGATED
        db.license(pool, expires=expired_at, checkouts_available=2, terms_concurrency=2)

        # Pool already accounts for the expiry
        pool.last_checked = last_updated
        pool.licenses_available = 0

        update_expired_licenses.delay().wait()

        db.session.refresh(pool)
        # last_checked is unchanged — proves the pool was skipped, not just that the
        # result happened to be 0 (which an expired license would also produce).
        assert pool.last_checked == last_updated

    @freeze_time()
    def test_multiple_pools_only_stale_updated(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ) -> None:
        """Only pools with unprocessed expirations are updated; already-current pools are skipped."""
        now = utc_now()
        expired_at = now - datetime.timedelta(hours=1)
        before_expiry = now - datetime.timedelta(hours=2)
        after_expiry = now - datetime.timedelta(minutes=30)

        edition1 = db.edition()
        stale_pool = db.licensepool(edition1)
        stale_pool.type = LicensePoolType.AGGREGATED
        db.license(
            stale_pool, expires=expired_at, checkouts_available=3, terms_concurrency=3
        )
        stale_pool.last_checked = before_expiry
        stale_pool.licenses_available = 3  # stale

        edition2 = db.edition()
        current_pool = db.licensepool(edition2)
        current_pool.type = LicensePoolType.AGGREGATED
        db.license(
            current_pool, expires=expired_at, checkouts_available=0, terms_concurrency=2
        )
        current_pool.last_checked = after_expiry
        current_pool.licenses_available = 0  # already up to date

        update_expired_licenses.delay().wait()

        db.session.refresh(stale_pool)
        db.session.refresh(current_pool)

        # Stale pool recalculated — expired license contributes 0; last_checked advanced
        assert stale_pool.licenses_available == 0
        assert stale_pool.last_checked == now
        # Current pool untouched — last_checked unchanged proves it was skipped
        assert current_pool.licenses_available == 0
        assert current_pool.last_checked == after_expiry

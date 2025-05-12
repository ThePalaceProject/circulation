import pytest

from palace.manager.celery.tasks import apply
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.service.redis.models.lock import LockNotAcquired
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture


class TestBibliographicApply:
    def test_apply(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        edition = db.edition()
        title = db.fresh_str()
        data = BibliographicData(
            data_source_name="Test Data Source",
            primary_identifier_data=IdentifierData.from_identifier(
                edition.primary_identifier
            ),
            title=title,
        )
        assert edition.title != title

        # Calling apply sets the title as you would expect
        apply.bibliographic_apply.delay(data, edition.id, None).wait()
        assert edition.title == title

    def test_apply_no_primary_identifier(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        edition = db.edition()
        data = BibliographicData(
            data_source_name="Test Data Source",
            primary_identifier_data=None,
        )

        with pytest.raises(PalaceValueError, match="No primary identifier provided"):
            apply.bibliographic_apply.delay(data, edition.id, None).wait()

    def test_already_locked(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        edition = db.edition()
        identifier = IdentifierData.from_identifier(edition.primary_identifier)
        data = BibliographicData(
            data_source_name="Test Data Source",
            primary_identifier_data=identifier,
        )

        # Lock the identifier, so the task will fail to acquire the lock
        apply._lock(redis_fixture.client, identifier).acquire()

        with (
            # Patch the retry backoff, so we don't have to wait for the retries
            celery_fixture.patch_retry_backoff() as retry_mock,
            # After the task retries, it will finally fail with a LockNotAcquired exception
            pytest.raises(LockNotAcquired),
        ):
            apply.bibliographic_apply.delay(data, edition.id, None).wait()

        # Make sure the task was retried
        assert retry_mock.retry_count == 4

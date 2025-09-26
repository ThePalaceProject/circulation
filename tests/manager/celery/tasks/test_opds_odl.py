from __future__ import annotations

import copy
import functools
import json
import uuid
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, call, create_autospec, patch

import pytest
from freezegun import freeze_time
from jinja2 import Template
from sqlalchemy import func, select

from palace.manager.celery.tasks import apply, opds_odl
from palace.manager.celery.tasks.opds_odl import (
    _licensepool_ids_with_holds,
    _recalculate_holds_for_licensepool,
    _redis_lock_recalculate_holds,
    _remove_expired_holds_for_collection,
    recalculate_hold_queue,
    recalculate_hold_queue_collection,
    remove_expired_holds,
    remove_expired_holds_for_collection_task,
)
from palace.manager.integration.license.opds.importer import (
    FeedImportResult,
    OpdsImporter,
)
from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
from palace.manager.integration.license.opds.opds2.api import OPDS2API
from palace.manager.integration.license.opds.requests import (
    OAuthOpdsRequest,
    OpdsAuthType,
)
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.opds.odl.info import Checkouts, LicenseInfo, LicenseStatus
from palace.manager.opds.odl.terms import Terms
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.service.redis.models.lock import LockNotAcquired
from palace.manager.sqlalchemy.constants import (
    EditionConstants,
    IdentifierConstants,
    MediaTypes,
)
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.contributor import Contribution, Contributor
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    License,
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.patron import Hold, Patron
from palace.manager.sqlalchemy.model.resource import Hyperlink
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import create
from palace.manager.util import datetime_helpers
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.celery import ApplyTaskFixture, CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OPDS2WithODLFilesFixture
from tests.fixtures.http import MockAsyncClientFixture, MockHttpClientFixture
from tests.fixtures.redis import RedisFixture
from tests.fixtures.services import ServicesFixture


class OpdsTaskFixture:
    def __init__(self, db: DatabaseTransactionFixture, services: ServicesFixture):
        self.db = db
        self.services = services

        self.two_weeks_ago = utc_now() - timedelta(weeks=2)
        self.yesterday = utc_now() - timedelta(days=1)
        self.tomorrow = utc_now() + timedelta(days=1)

    def hold(
        self,
        collection: Collection,
        *,
        start: datetime,
        end: datetime,
        position: int,
        pool: LicensePool | None = None,
        patron: Patron | None = None,
    ) -> Hold:
        if patron is None:
            patron = self.db.patron()
        if pool is None:
            _, pool = self.db.edition(collection=collection, with_license_pool=True)
        hold, _ = create(
            self.db.session,
            Hold,
            patron=patron,
            license_pool=pool,
            start=start,
            end=end,
            position=position,
        )
        return hold

    def holds(
        self, collection: Collection, pool: LicensePool | None = None
    ) -> tuple[set[int], set[int]]:
        # IMPORTANT: Each hold must have a unique start time to ensure deterministic ordering.
        # The get_active_holds() method orders holds by start time, and if multiple holds
        # have the same start time, the database may return them in any order, causing
        # flaky test failures when positions are recalculated.
        # We offset expired holds by 1 hour to avoid collision with ready holds at day 0.
        expired_holds = {
            self.hold(
                collection,
                start=self.two_weeks_ago - timedelta(hours=1) + timedelta(seconds=idx),
                end=self.yesterday,
                position=0,
                pool=pool,
            ).id
            for idx in range(10)
        }
        ready_non_expired_holds = {
            self.hold(
                collection,
                start=self.two_weeks_ago + timedelta(days=idx),
                end=self.tomorrow,
                position=0,
                pool=pool,
            ).id
            for idx in range(10)
        }
        not_ready_non_expired_holds = {
            self.hold(
                collection,
                start=self.yesterday + timedelta(seconds=idx),
                end=self.tomorrow,
                position=idx,
                pool=pool,
            ).id
            for idx in range(10)
        }

        return expired_holds, ready_non_expired_holds | not_ready_non_expired_holds

    def pool_with_licenses(
        self, collection: Collection, num_licenses: int = 2, available: bool = False
    ) -> tuple[LicensePool, list[License]]:
        edition = self.db.edition(collection=collection)
        pool = self.db.licensepool(
            edition, open_access=False, unlimited_access=False, collection=collection
        )
        licenses = [
            self.db.license(
                pool=pool,
                checkouts_available=idx + 1 if available else 0,
                terms_concurrency=idx + 1,
            )
            for idx in range(num_licenses)
        ]
        self.holds(collection, pool=pool)
        return pool, licenses


@pytest.fixture
def opds_task_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
) -> OpdsTaskFixture:
    return OpdsTaskFixture(db, services_fixture)


def _hold_sort_key(hold: Hold) -> int:
    position = hold.position
    assert position is not None
    return position


def test__remove_expired_holds_for_collection(
    db: DatabaseTransactionFixture,
    opds_task_fixture: OpdsTaskFixture,
    celery_fixture: CeleryFixture,
):
    collection = db.collection(protocol=OPDS2WithODLApi)
    decoy_collection = db.collection(protocol=OverdriveAPI)

    expired_holds, non_expired_holds = opds_task_fixture.holds(collection)
    decoy_expired_holds, decoy_non_expired_holds = opds_task_fixture.holds(
        decoy_collection
    )

    pools_before = db.session.scalars(
        select(func.count()).select_from(LicensePool)
    ).one()

    # Remove the expired holds
    assert collection.id is not None
    events = _remove_expired_holds_for_collection(
        db.session,
        collection.id,
    )

    # Assert that the correct holds were removed
    current_holds = {h.id for h in db.session.scalars(select(Hold))}

    assert expired_holds.isdisjoint(current_holds)
    assert non_expired_holds.issubset(current_holds)
    assert decoy_non_expired_holds.issubset(current_holds)
    assert decoy_expired_holds.issubset(current_holds)

    pools_after = db.session.scalars(
        select(func.count()).select_from(LicensePool)
    ).one()

    # Make sure the license pools for those holds were not deleted
    assert pools_before == pools_after

    # verify that the correct analytics calls were made
    assert len(events) == 10
    for event in events:
        assert event.type == CirculationEvent.CM_HOLD_EXPIRED
        assert event.library_id == db.default_library().id


def test__licensepools_with_holds(
    db: DatabaseTransactionFixture, opds_task_fixture: OpdsTaskFixture
):
    collection1 = db.collection(protocol=OPDS2WithODLApi)
    collection2 = db.collection(protocol=OPDS2WithODLApi)

    # create some holds on Collection2 to ensure that the query is correct
    opds_task_fixture.holds(collection2)

    # Create some license pools
    pools = [
        db.edition(collection=collection1, with_license_pool=True)[1]
        for idx in range(10)
    ]

    # Create holds for some of the license pools
    for pool in pools[5:]:
        opds_task_fixture.holds(collection1, pool=pool)

    queried_pools: list[int] = []
    iterations = 0

    # Query the license pools with holds
    assert collection1.id is not None
    while license_pools := _licensepool_ids_with_holds(
        db.session,
        collection1.id,
        batch_size=2,
        after_id=queried_pools[-1] if queried_pools else None,
    ):
        queried_pools.extend(license_pools)
        iterations += 1

    assert len(queried_pools) == 5
    assert iterations == 3
    assert queried_pools == [p.id for p in pools[5:]]


@freeze_time()
def test__recalculate_holds_for_licensepool(
    db: DatabaseTransactionFixture, opds_task_fixture: OpdsTaskFixture
):
    collection = db.collection(protocol=OPDS2WithODLApi)
    pool, [license1, license2] = opds_task_fixture.pool_with_licenses(collection)

    # Recalculate the hold queue
    _recalculate_holds_for_licensepool(pool, timedelta(days=5))

    current_holds = pool.get_active_holds()
    assert len(current_holds) == 20
    assert current_holds[0].position == 1
    assert current_holds[-1].position == len(current_holds)

    # Make a couple of copies available and recalculate the hold queue
    license1.checkouts_available = 1
    license2.checkouts_available = 2
    reservation_time = timedelta(days=5)
    _, events = _recalculate_holds_for_licensepool(pool, reservation_time)

    assert pool.licenses_reserved == 3
    assert pool.licenses_available == 0
    current_holds = pool.get_active_holds()
    assert len(current_holds) == 20

    reserved_holds = [h for h in current_holds if h.position == 0]
    waiting_holds = [h for h in current_holds if h.position and h.position > 0]

    assert len(reserved_holds) == 3
    assert len(waiting_holds) == 17

    assert all(h.end == utc_now() + reservation_time for h in reserved_holds)
    assert all(
        h.start and waiting_holds[0].start and h.start < waiting_holds[0].start
        for h in reserved_holds
    )

    waiting_holds.sort(key=_hold_sort_key)
    for idx, hold in enumerate(waiting_holds):
        assert hold.position == idx + 1
        assert hold.end is None

        expected_start = (
            waiting_holds[idx - 1].start if idx else reserved_holds[-1].start
        )
        assert hold.start and expected_start and hold.start >= expected_start

    # verify that the correct analytics events were returned
    assert len(events) == 3
    for event in events:
        assert event.type == CirculationEvent.CM_HOLD_READY_FOR_CHECKOUT


def test_remove_expired_holds_for_collection_task(
    celery_fixture: CeleryFixture,
    db: DatabaseTransactionFixture,
    opds_task_fixture: OpdsTaskFixture,
):
    collection1 = db.collection(protocol=OPDS2WithODLApi)

    expired_holds1, non_expired_holds1 = opds_task_fixture.holds(collection1)

    # Remove the expired holds
    remove_expired_holds_for_collection_task.delay(collection1.id).wait()

    assert len(opds_task_fixture.services.analytics.method_calls) == len(expired_holds1)

    current_holds = {h.id for h in db.session.scalars(select(Hold))}
    assert expired_holds1.isdisjoint(current_holds)

    assert non_expired_holds1.issubset(current_holds)


def test_remove_expired_holds(
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    db: DatabaseTransactionFixture,
    opds_task_fixture: OpdsTaskFixture,
):
    collection1 = db.collection(protocol=OPDS2WithODLApi)
    collection2 = db.collection(protocol=OPDS2WithODLApi)
    decoy_collection = db.collection(protocol=OverdriveAPI)

    with patch.object(
        opds_odl, "remove_expired_holds_for_collection_task"
    ) as mock_remove:
        remove_expired_holds.delay().wait()

    assert mock_remove.delay.call_count == 2
    mock_remove.delay.assert_has_calls(
        [call(collection1.id), call(collection2.id)], any_order=True
    )


def test_recalculate_hold_queue(
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    db: DatabaseTransactionFixture,
    opds_task_fixture: OpdsTaskFixture,
):
    collection1 = db.collection(protocol=OPDS2WithODLApi)
    collection2 = db.collection(protocol=OPDS2WithODLApi)
    decoy_collection = db.collection(protocol=OverdriveAPI)

    with patch.object(
        opds_odl, "recalculate_hold_queue_collection"
    ) as mock_recalculate:
        recalculate_hold_queue.delay().wait()

    assert mock_recalculate.delay.call_count == 2
    mock_recalculate.delay.assert_has_calls(
        [call(collection1.id), call(collection2.id)], any_order=True
    )


class TestRecalculateHoldQueueCollection:
    def test_success(
        self,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        db: DatabaseTransactionFixture,
        opds_task_fixture: OpdsTaskFixture,
    ):
        collection = db.collection(protocol=OPDS2WithODLApi)
        pools = [
            opds_task_fixture.pool_with_licenses(
                collection, num_licenses=1, available=True
            )
            for idx in range(15)
        ]

        # Do recalculation
        recalculate_hold_queue_collection.delay(collection.id, batch_size=2).wait()

        for pool, [license] in pools:
            current_holds = pool.get_active_holds()
            assert len(current_holds) == 20
            [reserved_hold] = [h for h in current_holds if h.position == 0]
            waiting_holds = [h for h in current_holds if h.position and h.position > 0]

            assert len(waiting_holds) == 19

            assert reserved_hold.end is not None
            assert reserved_hold.start is not None
            assert waiting_holds[0].start is not None
            assert reserved_hold.start < waiting_holds[0].start

            waiting_holds.sort(key=_hold_sort_key)
            for idx, hold in enumerate(waiting_holds):
                assert hold.position == idx + 1
                assert hold.end is None
                assert hold.start is not None
                expected_start = (
                    waiting_holds[idx - 1].start if idx else reserved_hold.start
                )
                assert expected_start is not None
                assert hold.start >= expected_start

    def test_already_running(
        self,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        db: DatabaseTransactionFixture,
    ):
        collection = db.collection(protocol=OPDS2WithODLApi)
        assert collection.id is not None
        lock = _redis_lock_recalculate_holds(redis_fixture.client, collection.id)

        # Acquire the lock, to simulate another task already running
        lock.acquire()

        with pytest.raises(LockNotAcquired):
            recalculate_hold_queue_collection.delay(collection.id).wait()

    def test_collection_deleted(
        self,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        caplog.set_level(LogLevel.info)
        collection = db.collection(protocol=OPDS2WithODLApi)
        collection_id = collection.id
        db.session.delete(collection)

        recalculate_hold_queue_collection.delay(collection_id).wait()

        assert "because it no longer exists" in caplog.text

    def test_pool_deleted(
        self,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        db: DatabaseTransactionFixture,
        opds_task_fixture: OpdsTaskFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        caplog.set_level(LogLevel.info)
        collection = db.collection(protocol=OPDS2WithODLApi)
        pool, _ = opds_task_fixture.pool_with_licenses(
            collection, num_licenses=1, available=True
        )
        deleted_pool, _ = opds_task_fixture.pool_with_licenses(
            collection, num_licenses=1, available=True
        )
        deleted_pool_id = deleted_pool.id
        db.session.delete(deleted_pool)

        assert pool.licenses_reserved != 1

        with patch.object(
            opds_odl, "_licensepool_ids_with_holds"
        ) as mock_licensepool_ids_with_holds:
            mock_licensepool_ids_with_holds.return_value = [deleted_pool_id, pool.id]
            recalculate_hold_queue_collection.delay(collection.id).wait()

        # The deleted pool was skipped
        assert (
            f"Skipping license pool {deleted_pool_id} because it no longer exists"
            in caplog.text
        )

        # The other pool was recalculated
        assert pool.licenses_reserved == 1


class TestImportAll:
    @pytest.mark.parametrize(
        "force",
        [
            pytest.param(True, id="Force import"),
            pytest.param(False, id="Do not force import"),
        ],
    )
    def test_import_all(
        self, db: DatabaseTransactionFixture, celery_fixture: CeleryFixture, force: bool
    ) -> None:
        collection1 = db.collection(protocol=OPDS2WithODLApi)
        collection2 = db.collection(protocol=OPDS2WithODLApi)
        decoy_collection = db.collection(protocol=OPDS2API)

        with patch.object(opds_odl, "import_collection") as mock_import_collection:
            opds_odl.import_all.delay(force=force).wait()

        # We queued up tasks for all OPDS2+ODL collections, but not for OPDS2
        mock_import_collection.s.assert_called_once_with(
            force=force,
        )
        mock_import_collection.s.return_value.delay.assert_has_calls(
            [
                call(collection_id=collection1.id),
                call(collection_id=collection2.id),
            ],
            any_order=True,
        )


class OPDS2WithODLImportFixture:
    def __init__(
        self,
        db: DatabaseTransactionFixture,
        http_client: MockHttpClientFixture,
        async_http_client: MockAsyncClientFixture,
        files_fixture: OPDS2WithODLFilesFixture,
        apply_fixture: ApplyTaskFixture,
    ):
        self.db = db
        self.collection = db.collection(
            protocol=OPDS2WithODLApi,
            settings=db.opds2_odl_settings(data_source="test collection"),
        )
        self.client = http_client
        self.async_client = async_http_client
        self.files = files_fixture
        self.apply_fixture = apply_fixture

    def queue_license_response(self, item: LicenseInfo) -> None:
        """Queue a license response to the async client."""
        self.async_client.queue_response(200, content=item.model_dump_json())

    def queue_feed_response(self, content: str | bytes) -> None:
        """Queue a feed response to the sync client."""
        self.client.queue_response(200, content=content)

    def queue_fixture_file(self, filename: str) -> None:
        """Queue a feed fixture file to the sync client."""
        self.client.queue_response(200, content=self.files.sample_data(filename))

    def queue_license_fixture_file(self, filename: str) -> None:
        """Queue a license fixture file to the async client."""
        self.async_client.queue_response(200, content=self.files.sample_data(filename))

    def import_fixture_file(
        self,
        filename: str = "feed_template.json.jinja",
        licenses: list[LicenseInfo] | None = None,
        collection: Collection | None = None,
    ) -> tuple[
        list[Edition],
        list[LicensePool],
        list[Work],
    ]:
        feed = self.files.sample_text(filename)

        if licenses is not None:
            for _license in licenses:
                self.queue_license_response(_license)
            feed = Template(feed).render(licenses=licenses)

        return self.import_feed(feed, collection)

    def import_feed(
        self,
        feed: str,
        collection: Collection | None = None,
    ) -> tuple[
        list[Edition],
        list[LicensePool],
        list[Work],
    ]:
        collection = collection if collection is not None else self.collection
        self.client.queue_response(
            200,
            content=feed,
            index=0,
        )
        opds_odl.import_collection.delay(collection_id=collection.id).wait()
        self.apply_fixture.process_apply_queue()

        return (
            self.apply_fixture.get_editions(),
            self.apply_fixture.get_pools(),
            self.apply_fixture.get_works(),
        )

    @staticmethod
    def get_delivery_mechanism_by_drm_scheme_and_content_type(
        delivery_mechanisms: list[LicensePoolDeliveryMechanism],
        content_type: str,
        drm_scheme: str | None,
    ) -> DeliveryMechanism | None:
        """Find a delivery mechanism by its DRM scheme and content type.

        :param delivery_mechanisms: List of delivery mechanisms
        :param content_type: Content type
        :param drm_scheme: DRM scheme

        :return: Delivery mechanism with the specified DRM scheme and content type (if any)
        """
        for delivery_mechanism in delivery_mechanisms:
            mechanism = delivery_mechanism.delivery_mechanism

            if (
                mechanism.drm_scheme == drm_scheme
                and mechanism.content_type == content_type
            ):
                return mechanism

        return None


@pytest.fixture
def opds2_with_odl_import_fixture(
    db: DatabaseTransactionFixture,
    http_client: MockHttpClientFixture,
    async_http_client: MockAsyncClientFixture,
    opds2_with_odl_files_fixture: OPDS2WithODLFilesFixture,
    apply_task_fixture: ApplyTaskFixture,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
) -> OPDS2WithODLImportFixture:
    return OPDS2WithODLImportFixture(
        db,
        http_client,
        async_http_client,
        opds2_with_odl_files_fixture,
        apply_task_fixture,
    )


class TestImportCollection:
    @pytest.mark.parametrize(
        "force",
        [
            pytest.param(True, id="Force import"),
            pytest.param(False, id="Do not force import"),
        ],
    )
    def test_import_collection(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        force: bool,
    ) -> None:
        """
        We mock out the actual importer calls, those get tested in the
        importer tests. This just makes sure we are calling the importer
        correctly.
        """
        mock_importer = create_autospec(OpdsImporter)
        mock_importer.import_feed.side_effect = [
            FeedImportResult(
                next_url="next_url",
                feed=MagicMock(),
                results={},
                failures=[],
                identifier_set=None,
            ),
            FeedImportResult(
                next_url=None,
                feed=MagicMock(),
                results={},
                failures=[],
                identifier_set=None,
            ),
        ]
        collection = db.collection(
            protocol=OPDS2WithODLApi,
            settings=db.opds2_odl_settings(external_account_id="http://feed.com"),
        )

        with patch.object(
            opds_odl,
            "importer_from_collection",
            autospec=True,
            return_value=mock_importer,
        ):
            opds_odl.import_collection.delay(collection.id, force=force).wait()

        mock_importer.import_feed.assert_has_calls(
            [
                call(
                    collection,
                    None,
                    apply_bibliographic=apply.bibliographic_apply.delay,
                    apply_circulation=apply.circulation_apply.delay,
                    import_even_if_unchanged=force,
                ),
                call(
                    collection,
                    "next_url",
                    apply_bibliographic=apply.bibliographic_apply.delay,
                    apply_circulation=apply.circulation_apply.delay,
                    import_even_if_unchanged=force,
                ),
            ]
        )

    def test_wrong_protocol(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        collection = db.collection(
            protocol=OverdriveAPI,
        )
        with pytest.raises(ValueError, match=r"is not a OPDS2\+ODL collection"):
            opds_odl.import_collection.delay(collection.id).wait()

    @freeze_time("2016-01-01T00:00:00+00:00")
    def test_import(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_import_fixture: OPDS2WithODLImportFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Ensure that OPDSWithODLImporter correctly processes and imports the ODL feed encoded using OPDS 2.x.

        NOTE: `freeze_time` decorator is required to treat the licenses in the ODL feed as non-expired.
        """
        caplog.set_level(LogLevel.error)
        collection = db.collection(
            protocol=OPDS2WithODLApi,
            settings=db.opds2_odl_settings(
                data_source="test collection",
                ignored_identifier_types={
                    IdentifierConstants.URI,
                },
            ),
        )

        # Act
        (
            imported_editions,
            pools,
            works,
        ) = opds2_with_odl_import_fixture.import_fixture_file(
            "feed.json",
            [
                LicenseInfo(
                    identifier="urn:uuid:f7847120-fc6f-11e3-8158-56847afe9799",
                    status=LicenseStatus.available,
                    checkouts=Checkouts(left=30, available=10),
                    terms=Terms(
                        concurrency=10,
                        checkouts=30,
                        expires="2016-04-25T12:25:21+02:00",
                    ),
                )
            ],
            collection=collection,
        )

        # Assert

        # 1. Make sure that there is a single edition only
        assert isinstance(imported_editions, list)
        assert 1 == len(imported_editions)

        [moby_dick_edition] = imported_editions
        assert isinstance(moby_dick_edition, Edition)
        assert moby_dick_edition.primary_identifier.identifier == "978-3-16-148410-0"
        assert moby_dick_edition.primary_identifier.type == "ISBN"
        assert Hyperlink.SAMPLE in {
            l.rel for l in moby_dick_edition.primary_identifier.links
        }

        assert "Moby-Dick" == moby_dick_edition.title
        assert "eng" == moby_dick_edition.language
        assert "eng" == moby_dick_edition.language
        assert EditionConstants.BOOK_MEDIUM == moby_dick_edition.medium
        assert "Herman Melville" == moby_dick_edition.author

        assert 1 == len(moby_dick_edition.author_contributors)
        [moby_dick_author] = moby_dick_edition.author_contributors
        assert isinstance(moby_dick_author, Contributor)
        assert "Herman Melville" == moby_dick_author.display_name
        assert "Melville, Herman" == moby_dick_author.sort_name

        assert 1 == len(moby_dick_author.contributions)
        [moby_dick_author_author_contribution] = moby_dick_author.contributions
        assert isinstance(moby_dick_author_author_contribution, Contribution)
        assert moby_dick_author == moby_dick_author_author_contribution.contributor
        assert moby_dick_edition == moby_dick_author_author_contribution.edition
        assert Contributor.Role.AUTHOR == moby_dick_author_author_contribution.role

        assert "test collection" == moby_dick_edition.data_source.name

        assert "Test Publisher" == moby_dick_edition.publisher
        assert date(2015, 9, 29) == moby_dick_edition.published

        assert "http://example.org/cover.jpg" == moby_dick_edition.cover_full_url
        assert (
            "http://example.org/cover-small.jpg"
            == moby_dick_edition.cover_thumbnail_url
        )

        # 2. Make sure that license pools have correct configuration
        assert isinstance(pools, list)
        assert 1 == len(pools)

        [moby_dick_license_pool] = pools
        assert isinstance(moby_dick_license_pool, LicensePool)
        assert moby_dick_license_pool.identifier.identifier == "978-3-16-148410-0"
        assert moby_dick_license_pool.identifier.type == "ISBN"
        assert not moby_dick_license_pool.open_access
        assert 10 == moby_dick_license_pool.licenses_owned
        assert 10 == moby_dick_license_pool.licenses_available

        assert 2 == len(moby_dick_license_pool.delivery_mechanisms)

        moby_dick_epub_adobe_drm_delivery_mechanism = opds2_with_odl_import_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
            moby_dick_license_pool.delivery_mechanisms,
            MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
        )
        assert moby_dick_epub_adobe_drm_delivery_mechanism is not None

        moby_dick_epub_lcp_drm_delivery_mechanism = opds2_with_odl_import_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
            moby_dick_license_pool.delivery_mechanisms,
            MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.LCP_DRM,
        )
        assert moby_dick_epub_lcp_drm_delivery_mechanism is not None

        assert 1 == len(moby_dick_license_pool.licenses)
        [moby_dick_license] = moby_dick_license_pool.licenses
        assert (
            "urn:uuid:f7847120-fc6f-11e3-8158-56847afe9799"
            == moby_dick_license.identifier
        )
        assert (
            "http://www.example.com/get{?id,checkout_id,expires,patron_id,passphrase,hint,hint_url,notification_url}"
            == moby_dick_license.checkout_url
        )
        assert "http://www.example.com/status/294024" == moby_dick_license.status_url
        assert (
            datetime(2016, 4, 25, 10, 25, 21, tzinfo=timezone.utc)
            == moby_dick_license.expires
        )
        assert 30 == moby_dick_license.checkouts_left
        assert 10 == moby_dick_license.checkouts_available

        # 3. Make sure that work objects contain all the required metadata
        assert isinstance(works, list)
        assert 1 == len(works)

        [moby_dick_work] = works
        assert isinstance(moby_dick_work, Work)
        assert moby_dick_edition == moby_dick_work.presentation_edition
        assert 1 == len(moby_dick_work.license_pools)
        assert moby_dick_license_pool == moby_dick_work.license_pools[0]

        # 4. Make sure that the failure is covered
        assert (
            "Failed to import publication: urn:isbn:9781234567897 (None) - Error validating publication: 2 validation errors"
            in caplog.text
        )

    @freeze_time("2016-01-01T00:00:00+00:00")
    def test_import_audiobook_with_streaming(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_import_fixture: OPDS2WithODLImportFixture,
    ) -> None:
        """Ensure that OPDSWithODLImporter correctly processes and imports a feed with an audiobook."""

        opds2_with_odl_import_fixture.queue_license_fixture_file(
            "license-audiobook.json"
        )
        (imported_editions, pools, works) = (
            opds2_with_odl_import_fixture.import_fixture_file(
                "feed-audiobook-streaming.json"
            )
        )

        # Make sure we imported one edition and it is an audiobook
        assert isinstance(imported_editions, list)
        assert 1 == len(imported_editions)

        [edition] = imported_editions
        assert isinstance(edition, Edition)
        assert edition.primary_identifier.identifier == "9780792766919"
        assert edition.primary_identifier.type == "ISBN"
        assert EditionConstants.AUDIO_MEDIUM == edition.medium

        # Make sure that license pools have correct configuration
        assert isinstance(pools, list)
        assert 1 == len(pools)

        [license_pool] = pools
        assert not license_pool.open_access
        assert 1 == license_pool.licenses_owned
        assert 1 == license_pool.licenses_available

        assert 2 == len(license_pool.delivery_mechanisms)

        lcp_delivery_mechanism = opds2_with_odl_import_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
            license_pool.delivery_mechanisms,
            MediaTypes.AUDIOBOOK_PACKAGE_LCP_MEDIA_TYPE,
            DeliveryMechanism.LCP_DRM,
        )
        assert lcp_delivery_mechanism is not None

        feedbooks_delivery_mechanism = opds2_with_odl_import_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
            license_pool.delivery_mechanisms,
            MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
            DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM,
        )
        assert feedbooks_delivery_mechanism is not None

    @freeze_time("2016-01-01T00:00:00+00:00")
    def test_import_audiobook_no_streaming(
        self,
        opds2_with_odl_import_fixture: OPDS2WithODLImportFixture,
    ) -> None:
        """
        Ensure that OPDSWithODLImporter correctly processes and imports a feed with an audiobook
        that is not available for streaming.
        """
        opds2_with_odl_import_fixture.queue_license_fixture_file(
            "license-audiobook.json"
        )

        (
            imported_editions,
            pools,
            works,
        ) = opds2_with_odl_import_fixture.import_fixture_file(
            "feed-audiobook-no-streaming.json"
        )

        # Make sure we imported one edition and it is an audiobook
        assert isinstance(imported_editions, list)
        assert 1 == len(imported_editions)

        [edition] = imported_editions
        assert isinstance(edition, Edition)
        assert edition.primary_identifier.identifier == "9781603937221"
        assert edition.primary_identifier.type == "ISBN"
        assert EditionConstants.AUDIO_MEDIUM == edition.medium

        # Make sure that license pools have correct configuration
        assert isinstance(pools, list)
        assert 1 == len(pools)

        [license_pool] = pools
        assert not license_pool.open_access
        assert 1 == license_pool.licenses_owned
        assert 1 == license_pool.licenses_available

        assert 1 == len(license_pool.delivery_mechanisms)

        lcp_delivery_mechanism = opds2_with_odl_import_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
            license_pool.delivery_mechanisms,
            MediaTypes.AUDIOBOOK_PACKAGE_LCP_MEDIA_TYPE,
            DeliveryMechanism.LCP_DRM,
        )
        assert lcp_delivery_mechanism is not None

    @pytest.mark.parametrize(
        "auth_type",
        [
            OpdsAuthType.BASIC,
            OpdsAuthType.OAUTH,
        ],
    )
    def test_import_open_access(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_import_fixture: OPDS2WithODLImportFixture,
        monkeypatch: pytest.MonkeyPatch,
        auth_type: OpdsAuthType,
    ) -> None:
        """
        Ensure that OPDSWithODLImporter correctly processes and imports a feed with an
        open access book.
        """
        collection = db.collection(
            protocol=OPDS2WithODLApi,
            settings=db.opds2_odl_settings(
                data_source="test collection", auth_type=auth_type
            ),
        )

        # Mock out the refresh_token method to avoid OAuth flow during tests
        monkeypatch.setattr(OAuthOpdsRequest, "refresh_token", MagicMock())

        (
            imported_editions,
            pools,
            works,
        ) = opds2_with_odl_import_fixture.import_fixture_file(
            "open-access-title.json", collection=collection
        )

        assert isinstance(imported_editions, list)
        assert 1 == len(imported_editions)

        [edition] = imported_editions
        assert isinstance(edition, Edition)
        assert (
            edition.primary_identifier.identifier
            == "https://www.feedbooks.com/book/7256"
        )
        assert edition.primary_identifier.type == "URI"
        assert edition.medium == EditionConstants.BOOK_MEDIUM

        # Make sure that license pools have correct configuration
        assert isinstance(pools, list)
        assert 1 == len(pools)

        [license_pool] = pools
        assert license_pool.open_access is True
        assert license_pool.unlimited_access is True

        assert 1 == len(license_pool.delivery_mechanisms)

        oa_ebook_delivery_mechanism = opds2_with_odl_import_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
            license_pool.delivery_mechanisms,
            MediaTypes.EPUB_MEDIA_TYPE,
            None,
        )
        assert oa_ebook_delivery_mechanism is not None

    @pytest.mark.parametrize(
        "auth_type",
        [
            OpdsAuthType.BASIC,
            OpdsAuthType.OAUTH,
        ],
    )
    def test_import_unlimited_access(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_import_fixture: OPDS2WithODLImportFixture,
        monkeypatch: pytest.MonkeyPatch,
        auth_type: OpdsAuthType,
    ) -> None:
        """
        Ensure that OPDSWithODLImporter correctly processes and imports a feed with an
        unlimited access book.
        """
        collection = db.collection(
            protocol=OPDS2WithODLApi,
            settings=db.opds2_odl_settings(
                data_source="test collection", auth_type=auth_type
            ),
        )
        # Mock out the refresh_token method to avoid OAuth flow during tests
        monkeypatch.setattr(OAuthOpdsRequest, "refresh_token", MagicMock())

        (
            imported_editions,
            pools,
            works,
        ) = opds2_with_odl_import_fixture.import_fixture_file(
            "unlimited-access-title.json", collection=collection
        )

        assert isinstance(imported_editions, list)
        assert 1 == len(imported_editions)

        [edition] = imported_editions
        assert isinstance(edition, Edition)
        assert (
            edition.primary_identifier.identifier
            == "urn:uuid:a0f77af3-a2a6-4a29-8e1f-18e06e4e573e"
        )
        assert edition.primary_identifier.type == "URI"
        assert edition.medium == EditionConstants.AUDIO_MEDIUM

        # Make sure that license pools have correct configuration
        assert isinstance(pools, list)
        assert 1 == len(pools)

        [license_pool] = pools
        assert license_pool.open_access is False
        assert license_pool.unlimited_access is True

        assert 1 == len(license_pool.delivery_mechanisms)

        ebook_delivery_mechanism = opds2_with_odl_import_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
            license_pool.delivery_mechanisms,
            MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
            (
                DeliveryMechanism.BEARER_TOKEN
                if auth_type == OpdsAuthType.OAUTH
                else None
            ),
        )
        assert ebook_delivery_mechanism is not None

    @freeze_time("2016-01-01T00:00:00+00:00")
    def test_import_availability(
        self,
        opds2_with_odl_import_fixture: OPDS2WithODLImportFixture,
    ) -> None:
        feed_json = json.loads(
            opds2_with_odl_import_fixture.files.sample_text("feed.json")
        )

        moby_dick_license_dict = feed_json["publications"][0]["licenses"][0]
        test_book_license_dict = feed_json["publications"][2]["licenses"][0]

        huck_finn_publication_dict = feed_json["publications"][1]
        huck_finn_publication_dict["licenses"] = copy.deepcopy(
            feed_json["publications"][0]["licenses"]
        )
        huck_finn_publication_dict["images"] = copy.deepcopy(
            feed_json["publications"][0]["images"]
        )
        huck_finn_license_dict = huck_finn_publication_dict["licenses"][0]

        MOBY_DICK_LICENSE_ID = "urn:uuid:f7847120-fc6f-11e3-8158-56847afe9799"
        TEST_BOOK_LICENSE_ID = "urn:uuid:f7847120-fc6f-11e3-8158-56847afe9798"
        HUCK_FINN_LICENSE_ID = f"urn:uuid:{uuid.uuid4()}"

        test_book_license_dict["metadata"]["availability"] = {
            "state": "unavailable",
            "reason": "https://registry.opds.io/reason#preordered",
            "until": "2016-01-20T00:00:00Z",
        }
        huck_finn_license_dict["metadata"]["identifier"] = HUCK_FINN_LICENSE_ID
        huck_finn_publication_dict["metadata"][
            "title"
        ] = "Adventures of Huckleberry Finn"

        # Mock responses from license status server
        def license_status_reply(
            license_id: str,
            concurrency: int = 10,
            checkouts: int | None = 30,
            expires: str | None = "2016-04-25T12:25:21+02:00",
        ) -> LicenseInfo:
            return LicenseInfo(
                identifier=license_id,
                status=LicenseStatus.available,
                checkouts=Checkouts(
                    available=concurrency,
                    left=checkouts,
                ),
                terms=Terms(
                    expires=expires,
                    concurrency=concurrency,
                ),
            )

        opds2_with_odl_import_fixture.queue_license_response(
            license_status_reply(MOBY_DICK_LICENSE_ID)
        )
        opds2_with_odl_import_fixture.queue_license_response(
            license_status_reply(HUCK_FINN_LICENSE_ID)
        )

        (
            imported_editions,
            pools,
            works,
        ) = opds2_with_odl_import_fixture.import_feed(json.dumps(feed_json))

        assert isinstance(pools, list)
        assert 3 == len(pools)

        [moby_dick_pool, huck_finn_pool, test_book_pool] = pools

        def assert_pool(
            pool: LicensePool,
            identifier: str,
            identifier_type: str,
            licenses_owned: int,
            licenses_available: int,
            license_id: str,
            available_for_borrowing: bool,
            license_status: LicenseStatus,
        ) -> None:
            assert pool.identifier.identifier == identifier
            assert pool.identifier.type == identifier_type
            assert pool.licenses_owned == licenses_owned
            assert pool.licenses_available == licenses_available
            assert len(pool.licenses) == 1
            [license_info] = pool.licenses
            assert license_info.identifier == license_id
            assert license_info.is_available_for_borrowing is available_for_borrowing
            assert license_info.status == license_status

        assert_moby_dick_pool = functools.partial(
            assert_pool,
            identifier="978-3-16-148410-0",
            identifier_type="ISBN",
            license_id=MOBY_DICK_LICENSE_ID,
        )
        assert_test_book_pool = functools.partial(
            assert_pool,
            identifier="http://example.org/test-book",
            identifier_type="URI",
            license_id=TEST_BOOK_LICENSE_ID,
        )
        assert_huck_finn_pool = functools.partial(
            assert_pool,
            identifier="9781234567897",
            identifier_type="ISBN",
            license_id=HUCK_FINN_LICENSE_ID,
        )

        assert_moby_dick_pool(
            moby_dick_pool,
            licenses_owned=10,
            licenses_available=10,
            available_for_borrowing=True,
            license_status=LicenseStatus.available,
        )

        assert_test_book_pool(
            test_book_pool,
            licenses_owned=0,
            licenses_available=0,
            available_for_borrowing=False,
            license_status=LicenseStatus.unavailable,
        )

        assert_huck_finn_pool(
            huck_finn_pool,
            licenses_owned=10,
            licenses_available=10,
            available_for_borrowing=True,
            license_status=LicenseStatus.available,
        )

        # Harvest the feed again, but this time the status has changed
        moby_dick_license_dict["metadata"]["availability"] = {
            "state": "unavailable",
        }
        del test_book_license_dict["metadata"]["availability"]
        huck_finn_publication_dict["metadata"]["availability"] = {
            "state": "unavailable",
        }

        # Mock responses from license status server
        opds2_with_odl_import_fixture.queue_license_response(
            license_status_reply(TEST_BOOK_LICENSE_ID, checkouts=None, expires=None)
        )

        # Harvest the feed again
        (
            imported_editions,
            pools,
            works,
        ) = opds2_with_odl_import_fixture.import_feed(json.dumps(feed_json))

        assert isinstance(pools, list)
        assert 3 == len(pools)

        [moby_dick_pool, huck_finn_pool, test_book_pool] = pools

        assert_moby_dick_pool(
            moby_dick_pool,
            licenses_owned=0,
            licenses_available=0,
            available_for_borrowing=False,
            license_status=LicenseStatus.unavailable,
        )

        assert_test_book_pool(
            test_book_pool,
            licenses_owned=10,
            licenses_available=10,
            available_for_borrowing=True,
            license_status=LicenseStatus.available,
        )

        assert_huck_finn_pool(
            huck_finn_pool,
            licenses_owned=0,
            licenses_available=0,
            available_for_borrowing=False,
            license_status=LicenseStatus.unavailable,
        )

    @pytest.mark.parametrize(
        "license",
        [
            pytest.param(
                LicenseInfo(
                    identifier="urn:uuid:expired-license-1",
                    status=LicenseStatus.available,
                    checkouts=Checkouts(
                        left=52,
                        available=1,
                    ),
                    terms=Terms(
                        concurrency=1,
                        expires="2021-01-01T00:01:00+01:00",
                    ),
                ),
                id="expiration_date_in_the_past",
            ),
            pytest.param(
                LicenseInfo(
                    identifier="urn:uuid:left-is-zero",
                    status=LicenseStatus.available,
                    checkouts=Checkouts(
                        left=0,
                        available=1,
                    ),
                    terms=Terms(
                        concurrency=1,
                    ),
                ),
                id="left_is_zero",
            ),
            pytest.param(
                LicenseInfo(
                    identifier="urn:uuid:status-unavailable",
                    status=LicenseStatus.unavailable,
                    checkouts=Checkouts(
                        available=1,
                    ),
                ),
                id="status_unavailable",
            ),
        ],
    )
    @freeze_time("2021-01-01T00:00:00+00:00")
    def test_odl_importer_expired_licenses(
        self,
        opds2_with_odl_import_fixture: OPDS2WithODLImportFixture,
        license: LicenseInfo,
    ):
        """Ensure OPDSWithODLImporter imports expired licenses, but does not count them."""
        # Import the test feed with an expired ODL license.
        (
            imported_editions,
            imported_pools,
            imported_works,
        ) = opds2_with_odl_import_fixture.import_fixture_file(licenses=[license])

        # The importer created 1 edition and 1 work.
        assert len(imported_editions) == 1
        assert len(imported_works) == 1

        # Ensure that the license pool was successfully created, with no available copies.
        assert len(imported_pools) == 1

        [imported_pool] = imported_pools
        assert imported_pool.licenses_owned == 0
        assert imported_pool.licenses_available == 0
        assert len(imported_pool.licenses) == 1

        # Ensure the license was imported and is expired.
        [imported_license] = imported_pool.licenses
        assert imported_license.is_inactive is True

    def test_odl_importer_reimport_expired_licenses(
        self,
        opds2_with_odl_import_fixture: OPDS2WithODLImportFixture,
    ):
        license_expiry = datetime.fromisoformat("2021-01-01T00:01:00+00:00")
        licenses = [
            LicenseInfo(
                identifier="test",
                status=LicenseStatus.available,
                checkouts=Checkouts(available=1),
                terms=Terms(
                    concurrency=1,
                    expires=license_expiry,
                ),
            )
        ]

        # First import the license when it is not expired
        with freeze_time(license_expiry - timedelta(days=1)):
            # Import the test feed.
            (
                imported_editions,
                imported_pools,
                imported_works,
            ) = opds2_with_odl_import_fixture.import_fixture_file(licenses=licenses)

            # The importer created 1 edition and 1 work with no failures.
            assert len(imported_editions) == 1
            assert len(imported_works) == 1
            assert len(imported_pools) == 1

            # Ensure that the license pool was successfully created, with available copies.
            [imported_pool] = imported_pools
            assert imported_pool.licenses_owned == 1
            assert imported_pool.licenses_available == 1
            assert len(imported_pool.licenses) == 1

            # Ensure the license was imported and is not expired.
            [imported_license] = imported_pool.licenses
            assert imported_license.is_inactive is False

        # Reimport the license when it is expired
        with freeze_time(license_expiry + timedelta(days=1)):
            # Import the test feed.
            (
                imported_editions,
                imported_pools,
                imported_works,
            ) = opds2_with_odl_import_fixture.import_fixture_file(licenses=licenses)

            # The importer created 1 edition and 1 work with no failures.
            assert len(imported_editions) == 1
            assert len(imported_works) == 1
            assert len(imported_pools) == 1

            # Ensure that the license pool was successfully created, with no available copies.
            [imported_pool] = imported_pools
            assert imported_pool.licenses_owned == 0
            assert imported_pool.licenses_available == 0
            assert len(imported_pool.licenses) == 1

            # Ensure the license was imported and is expired.
            [imported_license] = imported_pool.licenses
            assert imported_license.is_inactive is True

    @freeze_time("2021-01-01T00:00:00+00:00")
    def test_odl_importer_multiple_expired_licenses(
        self,
        opds2_with_odl_import_fixture: OPDS2WithODLImportFixture,
    ):
        """Ensure OPDSWithODLImporter imports expired licenses
        and does not count them in the total number of available licenses."""

        # 1.1. Import the test feed with three inactive ODL licenses and two active licenses.
        inactive = [
            # Expired
            # (expiry date in the past)
            LicenseInfo(
                identifier="urn:uuid:expired-license-1",
                status=LicenseStatus.available,
                checkouts=Checkouts(
                    available=1,
                ),
                terms=Terms(
                    concurrency=1,
                    expires=datetime_helpers.utc_now() - timedelta(days=1),
                ),
            ),
            # Expired
            # (left is 0)
            LicenseInfo(
                identifier="urn:uuid:expired-license-2",
                status=LicenseStatus.available,
                checkouts=Checkouts(
                    available=1,
                    left=0,
                ),
            ),
            # Expired
            # (status is unavailable)
            LicenseInfo(
                identifier="urn:uuid:expired-license-3",
                status=LicenseStatus.unavailable,
                checkouts=Checkouts(
                    available=1,
                ),
            ),
        ]
        active = [
            LicenseInfo(
                identifier="urn:uuid:active-license-1",
                status=LicenseStatus.available,
                checkouts=Checkouts(
                    available=1,
                ),
                terms=Terms(
                    concurrency=1,
                ),
            ),
            LicenseInfo(
                identifier="urn:uuid:active-license-2",
                status=LicenseStatus.available,
                checkouts=Checkouts(
                    available=5,
                    left=40,
                ),
                terms=Terms(
                    concurrency=5,
                ),
            ),
        ]

        (
            imported_editions,
            imported_pools,
            imported_works,
        ) = opds2_with_odl_import_fixture.import_fixture_file(
            licenses=active + inactive
        )

        # License pool was successfully created
        assert len(imported_pools) == 1
        [imported_pool] = imported_pools

        # All licenses were imported
        assert len(imported_pool.licenses) == 5

        # Make sure that the license statistics are correct and include only active licenses.
        assert imported_pool.licenses_owned == 6
        assert imported_pool.licenses_available == 6

        # Correct number of active and inactive licenses
        assert sum(not l.is_inactive for l in imported_pool.licenses) == len(active)
        assert sum(l.is_inactive for l in imported_pool.licenses) == len(inactive)

    def test_odl_importer_reimport_multiple_licenses(
        self,
        opds2_with_odl_import_fixture: OPDS2WithODLImportFixture,
    ):
        """Ensure OPDSWithODLImporter correctly imports licenses that have already been imported."""

        # 1.1. Import the test feed with ODL licenses that are not expired.
        license_expiry = datetime.fromisoformat("2021-01-01T00:01:00+00:00")

        date = LicenseInfo(
            identifier="urn:uuid:date-license",
            status=LicenseStatus.available,
            checkouts=Checkouts(
                available=1,
            ),
            terms=Terms(
                concurrency=1,
                expires=license_expiry,
            ),
        )
        left = LicenseInfo(
            identifier="urn:uuid:left-license",
            status=LicenseStatus.available,
            checkouts=Checkouts(
                available=1,
                left=1,
            ),
            terms=Terms(
                concurrency=2,
            ),
        )
        perpetual = LicenseInfo(
            identifier="urn:uuid:perpetual-license",
            status=LicenseStatus.available,
            checkouts=Checkouts(
                available=0,
            ),
            terms=Terms(
                concurrency=1,
            ),
        )

        # Import with all licenses valid
        with freeze_time(license_expiry - timedelta(days=1)):
            (
                imported_editions,
                imported_pools,
                imported_works,
            ) = opds2_with_odl_import_fixture.import_fixture_file(
                licenses=[date, left, perpetual]
            )

            assert len(imported_pools) == 1

            [imported_pool] = imported_pools
            assert len(imported_pool.licenses) == 3
            assert imported_pool.licenses_available == 2
            assert imported_pool.licenses_owned == 3

            # No licenses are expired
            assert sum(not l.is_inactive for l in imported_pool.licenses) == 3

        # Expire the first two licenses

        # The first one is expired by changing the time
        with freeze_time(license_expiry + timedelta(days=1)):
            # The second one is expired by setting left to 0
            left = LicenseInfo(
                identifier="urn:uuid:left-license",
                status=LicenseStatus.available,
                checkouts=Checkouts(
                    available=1,
                    left=0,
                ),
                terms=Terms(
                    concurrency=2,
                ),
            )

            # The perpetual license has a copy available
            perpetual = LicenseInfo(
                identifier="urn:uuid:perpetual-license",
                status=LicenseStatus.available,
                checkouts=Checkouts(
                    available=1,
                ),
                terms=Terms(
                    concurrency=1,
                ),
            )

            # Reimport
            (
                imported_editions,
                imported_pools,
                imported_works,
            ) = opds2_with_odl_import_fixture.import_fixture_file(
                licenses=[date, left, perpetual]
            )

            assert len(imported_pools) == 1

            [imported_pool] = imported_pools
            assert len(imported_pool.licenses) == 3
            assert imported_pool.licenses_available == 1
            assert imported_pool.licenses_owned == 1

            # One license not expired
            assert sum(not l.is_inactive for l in imported_pool.licenses) == 1

            # Two licenses expired
            assert sum(l.is_inactive for l in imported_pool.licenses) == 2

from unittest.mock import call, create_autospec, patch

import pytest

from palace.manager.celery.importer import import_lock
from palace.manager.celery.tasks import boundless
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.integration.license.boundless.api import BoundlessApi
from palace.manager.integration.license.boundless.importer import (
    BoundlessImporter,
    FeedImportResult,
)
from palace.manager.integration.license.boundless.model.json import (
    Pagination,
    Status,
    Title,
    TitleLicenseResponse,
)
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.service.redis.models.lock import LockNotAcquired
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http.exception import BadResponseException, RequestTimedOut
from tests.fixtures.celery import ApplyTaskFixture, CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import FilesFixture
from tests.fixtures.http import MockHttpClientFixture
from tests.fixtures.redis import RedisFixture
from tests.fixtures.services import ServicesFixture
from tests.mocks.mock import MockRequestsResponse


class TestImportCollection:
    def test_import_lock(
        self,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """
        The import_collection task is protected by a lock, so only one import can run at a time.
        """
        mock_collection_id = 1234
        redis = services_fixture.services.redis().client()
        import_lock(redis, mock_collection_id).acquire()
        with pytest.raises(LockNotAcquired):
            boundless.import_collection.delay(collection_id=mock_collection_id).wait()

    @pytest.mark.parametrize(
        "import_all",
        [
            pytest.param(True, id="import all"),
            pytest.param(False, id="no import all"),
        ],
    )
    def test_parameters_passed_to_importer(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        services_fixture: ServicesFixture,
        import_all: bool,
    ) -> None:
        """
        The import_collection task passes the correct parameters to the BoundlessImporter.
        """
        collection = db.collection(name="test_collection", protocol=BoundlessApi)

        # Create a FeedImportResult indicating import is complete
        mock_result = FeedImportResult(
            complete=True,
            active_processed=10,
            inactive_processed=5,
            current_page=1,
            total_pages=1,
            next_page=None,
        )

        with patch.object(
            boundless, "BoundlessImporter", autospec=BoundlessImporter
        ) as mock_importer:
            mock_importer.return_value.import_collection.return_value = mock_result
            mock_importer.return_value.get_timestamp.return_value = (
                db.session.query(Timestamp).first() or Timestamp()
            )

            boundless.import_collection.delay(
                collection_id=collection.id,
                import_all=import_all,
            ).wait()

        registry = services_fixture.services.integration_registry().license_providers()

        mock_importer.assert_called_once_with(
            db.session,
            collection,
            registry,
            import_all,
        )

    def test_full_import(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        apply_task_fixture: ApplyTaskFixture,
        boundless_files_fixture: FilesFixture,
        http_client: MockHttpClientFixture,
    ) -> None:
        """
        Test a full import run from the task.
        """
        collection = db.collection(name="test_collection", protocol=BoundlessApi)
        assert get_one(db.session, Timestamp, collection=collection) is None

        # Queue responses: token, title_license (page 1), availability
        http_client.queue_response(
            200,
            content=boundless_files_fixture.sample_text("token.json"),
        )
        http_client.queue_response(
            200,
            content=boundless_files_fixture.sample_text(
                "title_license_single_item.json"
            ),
        )
        http_client.queue_response(
            200, content=boundless_files_fixture.sample_data("single_item.xml")
        )

        # Run the import
        result = boundless.import_collection.delay(
            collection_id=collection.id,
            import_all=True,
        ).wait()

        # The task returns None
        assert result is None

        # Check that we would have queued up the expected apply tasks.
        assert len(apply_task_fixture.apply_queue) == 1
        apply_task_fixture.process_apply_queue()

        # A LicensePool was created. We know both how many copies of this
        # book are available, and what formats it's available in.
        [pool] = apply_task_fixture.get_pools()

        assert pool.identifier.type == "Axis 360 ID"
        assert pool.identifier.identifier == "0003642860"

        assert pool.licenses_owned == 9
        [lpdm] = pool.delivery_mechanisms
        assert (
            lpdm.delivery_mechanism.name
            == "application/epub+zip (application/vnd.adobe.adept+xml)"
        )

        # A Work was created and made presentation ready.
        assert pool.work.title == "Faith of My Fathers : A Family Memoir"
        assert pool.work.presentation_ready is True

        # We created a Timestamp for this import.
        timestamp = get_one(db.session, Timestamp, collection=collection)
        assert timestamp is not None
        assert timestamp.start is not None
        assert timestamp.finish is not None
        assert timestamp.start <= timestamp.finish <= utc_now()

        # If we do the exact same import again, nothing new happens because
        # the title hasn't changed since the last import.
        http_client.queue_response(
            200,
            content=boundless_files_fixture.sample_text("token.json"),
        )
        http_client.queue_response(
            200,
            content=boundless_files_fixture.sample_text(
                "title_license_single_item.json"
            ),
        )
        http_client.queue_response(
            200, content=boundless_files_fixture.sample_data("single_item.xml")
        )
        assert (
            boundless.import_collection.delay(
                collection_id=collection.id,
                import_all=False,
            ).wait()
            is None
        )
        # No new apply tasks were queued
        assert len(apply_task_fixture.apply_queue) == 0

    def test_retry_due_to_bad_response_exception(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        apply_task_fixture: ApplyTaskFixture,
        boundless_files_fixture: FilesFixture,
        http_client: MockHttpClientFixture,
    ):
        collection = db.collection(name="test_collection", protocol=BoundlessApi)

        # Create a successful result
        successful_result = FeedImportResult(
            complete=True,
            active_processed=5,
            inactive_processed=2,
            current_page=1,
            total_pages=1,
            next_page=None,
        )

        with (
            celery_fixture.patch_retry_backoff() as retries,
            patch.object(boundless, "BoundlessImporter") as mock_create_importer,
        ):
            mock_importer = create_autospec(BoundlessImporter)
            mock_create_importer.return_value = mock_importer
            mock_importer.get_timestamp.return_value = (
                db.session.query(Timestamp).first() or Timestamp()
            )
            mock_importer.import_collection.side_effect = [
                BadResponseException(
                    "http://test.com",
                    "Temporary failure",
                    MockRequestsResponse(500),
                ),
                RequestTimedOut(
                    "http://test.com",
                    "Temporary timeout",
                ),
                successful_result,
            ]

            result = boundless.import_collection.delay(
                collection_id=collection.id,
            ).wait()

            # The task returns None
            assert result is None

        assert retries.retry_count == 2
        assert mock_importer.import_collection.call_count == 3

    def test_parameter_validation_error(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """
        Test that import_collection raises PalaceValueError when page > 1 but
        modified_since or start_time is None.
        """
        collection = db.collection(name="test_collection", protocol=BoundlessApi)

        # Test with page > 1 but missing modified_since
        with pytest.raises(
            PalaceValueError, match="modified_since and start_time are required"
        ):
            boundless.import_collection.delay(
                collection_id=collection.id,
                page=2,
                start_time=utc_now(),
            ).wait()

        # Test with page > 1 but missing start_time
        with pytest.raises(
            PalaceValueError, match="modified_since and start_time are required"
        ):
            boundless.import_collection.delay(
                collection_id=collection.id,
                page=2,
                modified_since=utc_now(),
            ).wait()

        # Test with page > 1 but missing both
        with pytest.raises(
            PalaceValueError, match="modified_since and start_time are required"
        ):
            boundless.import_collection.delay(
                collection_id=collection.id,
                page=2,
            ).wait()

    def test_multipage_import(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        apply_task_fixture: ApplyTaskFixture,
        boundless_files_fixture: FilesFixture,
        http_client: MockHttpClientFixture,
    ) -> None:
        """
        Test that a multi-page import correctly chains tasks and maintains
        state across pages.
        """
        collection = db.collection(name="test_collection", protocol=BoundlessApi)

        # Create a mock response for page 1 that indicates there are 2 pages
        page1_response = TitleLicenseResponse(
            pagination=Pagination(
                current_page=1,
                page_size=2,
                total_count=3,
                total_page=2,
            ),
            status=Status(code=0, message="Titles Retrieved Successfully."),
            titles=[
                Title(title_id="0000000001", active=True),
                Title(title_id="0000000002", active=False),
            ],
        )

        # Create a mock response for page 2 (final page)
        page2_response = TitleLicenseResponse(
            pagination=Pagination(
                current_page=2,
                page_size=2,
                total_count=3,
                total_page=2,
            ),
            status=Status(code=0, message="Titles Retrieved Successfully."),
            titles=[
                Title(title_id="0000000003", active=True),
            ],
        )

        # Queue responses for page 1:
        # - token
        # - title_license page 1
        # - availability for active title
        http_client.queue_response(
            200, content=boundless_files_fixture.sample_data("token.json")
        )
        http_client.queue_response(200, content=page1_response.model_dump_json())
        http_client.queue_response(
            200, content=boundless_files_fixture.sample_data("single_item.xml")
        )

        # Queue responses for page 2:
        # - token (refreshed for new task)
        # - title_license page 2
        # - availability for active title
        http_client.queue_response(
            200, content=boundless_files_fixture.sample_data("token.json")
        )
        http_client.queue_response(200, content=page2_response.model_dump_json())
        http_client.queue_response(
            200, content=boundless_files_fixture.sample_data("single_item.xml")
        )

        # Run the import - this should process page 1 and automatically chain to page 2
        result = boundless.import_collection.delay(
            collection_id=collection.id, import_all=True
        ).wait()

        # The task should return None when complete
        assert result is None

        # We should have queued 3 apply tasks:
        # - 1 bibliographic for page 1 active title
        # - 1 circulation for page 1 inactive title (marked as having 0 licenses)
        # - 1 bibliographic for page 2 active title
        assert len(apply_task_fixture.apply_queue) == 3

        # Verify the queued tasks have the correct types and structure
        from tests.fixtures.celery import ApplyBibliographicCall, ApplyCirculationCall

        task_0 = apply_task_fixture.apply_queue[0]
        task_1 = apply_task_fixture.apply_queue[1]
        task_2 = apply_task_fixture.apply_queue[2]

        # Task 0: bibliographic for active title from page 1
        assert isinstance(task_0, ApplyBibliographicCall)
        assert task_0.bibliographic.primary_identifier_data is not None
        assert task_0.collection_id == collection.id

        # Task 1: circulation for inactive title from page 1 - should have 0 licenses
        assert isinstance(task_1, ApplyCirculationCall)
        assert task_1.circulation.licenses_owned == 0
        assert task_1.circulation.licenses_available == 0
        assert task_1.collection_id == collection.id

        # Task 2: bibliographic for active title from page 2
        assert isinstance(task_2, ApplyBibliographicCall)
        assert task_2.bibliographic.primary_identifier_data is not None
        assert task_2.collection_id == collection.id

        # The timestamp should be created and populated only after both pages are processed
        timestamp = get_one(db.session, Timestamp, collection=collection)
        assert timestamp is not None
        assert timestamp.start is not None
        assert timestamp.finish is not None
        assert timestamp.start <= timestamp.finish


@pytest.mark.parametrize(
    "import_all",
    [
        pytest.param(True, id="import all flag"),
        pytest.param(False, id="no import all flag"),
    ],
)
def test_import_all_collections(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    caplog: pytest.LogCaptureFixture,
    import_all: bool,
):
    caplog.set_level(LogLevel.info)
    decoy_collection = db.default_collection()
    collection1 = db.collection(protocol=BoundlessApi)
    collection2 = db.collection(protocol=BoundlessApi)
    with patch.object(boundless, "import_collection") as import_collection:
        boundless.import_all_collections.delay(import_all=import_all).wait()

    import_collection.s.assert_called_once_with(import_all=import_all)
    import_collection.s.return_value.delay.assert_has_calls(
        [call(collection_id=collection1.id), call(collection_id=collection2.id)],
        any_order=True,
    )
    assert "Queued 2 collections for import." in caplog.text

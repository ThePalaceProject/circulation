from unittest.mock import call, create_autospec, patch

import pytest

from palace.manager.celery.importer import import_lock
from palace.manager.celery.tasks import boundless, identifiers
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.license.boundless.api import BoundlessApi
from palace.manager.integration.license.boundless.importer import BoundlessImporter
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.service.redis.models.lock import LockNotAcquired
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.identifier import Identifier
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
        "return_identifiers, import_all",
        [
            pytest.param(True, True, id="return identifiers, import all"),
            pytest.param(True, False, id="return identifiers, no import all"),
            pytest.param(False, True, id="no return identifiers, import all"),
            pytest.param(False, False, id="no return identifiers, no import all"),
        ],
    )
    def test_parameters_passed_to_importer(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        services_fixture: ServicesFixture,
        return_identifiers: bool,
        import_all: bool,
    ) -> None:
        """
        The import_collection task passes the correct parameters to the BoundlessImporter.
        """
        collection = db.collection(name="test_collection", protocol=BoundlessApi)
        with (
            patch.object(
                boundless, "BoundlessImporter", autospec=BoundlessImporter
            ) as mock_importer,
            patch.object(boundless, "IdentifierSet") as mock_identifier_set,
        ):
            mock_importer.return_value.import_collection.return_value = None
            boundless.import_collection.delay(
                collection_id=collection.id,
                import_all=import_all,
                return_identifiers=return_identifiers,
            ).wait()

        if return_identifiers:
            mock_identifier_set.assert_called_once()
        else:
            mock_identifier_set.assert_not_called()

        registry = services_fixture.services.integration_registry().license_providers()

        mock_importer.assert_called_once_with(
            db.session,
            collection,
            registry,
            import_all,
            mock_identifier_set.return_value if return_identifiers else None,
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
        http_client.queue_response(
            200,
            content=boundless_files_fixture.sample_text("token.json"),
        )
        http_client.queue_response(
            200, content=boundless_files_fixture.sample_data("single_item.xml")
        )

        # The identifier that will be imported
        expected_identifier = IdentifierData(
            type=Identifier.AXIS_360_ID, identifier="0003642860"
        )

        identifiers_kwargs = boundless.import_collection.delay(
            collection_id=collection.id,
            import_all=True,
            return_identifiers=True,
        ).wait()

        # Check that we would have queued up the expected apply tasks.
        assert len(apply_task_fixture.apply_queue) == 1
        apply_task_fixture.process_apply_queue()

        # A LicensePool was created. We know both how many copies of this
        # book are available, and what formats it's available in.
        [pool] = apply_task_fixture.get_pools()

        assert pool.identifier.type == expected_identifier.type
        assert pool.identifier.identifier == expected_identifier.identifier

        assert pool.licenses_owned == 9
        [lpdm] = pool.delivery_mechanisms
        assert (
            lpdm.delivery_mechanism.name
            == "application/epub+zip (application/vnd.adobe.adept+xml)"
        )

        # A Work was created and made presentation ready.
        assert pool.work.title == "Faith of My Fathers : A Family Memoir"
        assert pool.work.presentation_ready is True

        # We returned an IdentifierSet with one identifier in it.
        assert isinstance(identifiers_kwargs, dict)
        identifier_set = IdentifierSet(redis_fixture.client, **identifiers_kwargs)
        assert identifier_set.get() == {expected_identifier}

        # We created a Timestamp for this import.
        timestamp = get_one(db.session, Timestamp, collection=collection)
        assert timestamp is not None
        assert timestamp.start is not None
        assert timestamp.finish is not None
        assert timestamp.start <= timestamp.finish <= utc_now()

        # If we do the exact same import again, nothing new happens.
        http_client.queue_response(
            200,
            content=boundless_files_fixture.sample_text("token.json"),
        )
        http_client.queue_response(
            200, content=boundless_files_fixture.sample_data("single_item.xml")
        )
        assert (
            boundless.import_collection.delay(
                collection_id=collection.id,
                import_all=False,
                return_identifiers=False,
            ).wait()
            is None
        )
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
        expected_result = [1, 2, 3]

        with (
            celery_fixture.patch_retry_backoff() as retries,
            patch.object(boundless, "BoundlessImporter") as mock_create_importer,
        ):
            mock_importer = create_autospec(BoundlessImporter)
            mock_create_importer.return_value = mock_importer
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
                expected_result,
            ]

            assert (
                boundless.import_collection.delay(
                    collection_id=collection.id,
                ).wait()
                == expected_result
            )

        assert retries.retry_count == 2
        assert mock_importer.import_collection.call_count == 3


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


@pytest.mark.parametrize(
    "import_all",
    [
        pytest.param(True, id="import all flag"),
        pytest.param(False, id="no import all flag"),
    ],
)
def test_reap_all_collections(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    caplog: pytest.LogCaptureFixture,
    import_all: bool,
):
    caplog.set_level(LogLevel.info)
    decoy_collection = db.default_collection()
    collection1 = db.collection(protocol=BoundlessApi)
    collection2 = db.collection(protocol=BoundlessApi)
    with patch.object(boundless, "import_and_reap_not_found_chord") as mock_reap:
        boundless.reap_all_collections.delay(import_all=import_all).wait()

    mock_reap.assert_has_calls(
        [
            call(collection_id=collection1.id, import_all=import_all),
            call(collection_id=collection2.id, import_all=import_all),
        ],
        any_order=True,
    )

    assert mock_reap.return_value.delay.call_count == 2

    for collection in [collection1, collection2]:
        assert (
            f'Queued collection("{collection.name}" [id={collection.id}] for reaping...'
            in caplog.text
        )
    assert "Finished queuing all collection reaping tasks" in caplog.text


class TestImportAndReapNotFoundChord:
    @pytest.mark.parametrize(
        "import_all",
        [
            pytest.param(True, id="import all flag"),
            pytest.param(False, id="no import all flag"),
        ],
    )
    def test_import_and_reap_not_found_chord(self, import_all: bool) -> None:
        """Test the import and reap not found chord."""
        # Reap the collection
        collection_id = 12  # Example collection ID
        with (
            patch.object(identifiers, "create_mark_unavailable_chord") as mock_chord,
            patch.object(boundless, "import_collection") as mock_import,
        ):
            boundless.import_and_reap_not_found_chord(
                collection_id=collection_id, import_all=import_all
            )

        mock_import.s.assert_called_once_with(
            collection_id=collection_id, import_all=import_all, return_identifiers=True
        )
        mock_chord.assert_called_once_with(collection_id, mock_import.s.return_value)

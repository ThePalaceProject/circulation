from unittest.mock import MagicMock, call

import pytest
from celery.exceptions import Ignore, Retry

from palace.manager.marc.uploader import MarcUploadManager
from palace.manager.service.redis.models.marc import (
    MarcFileUpload,
    MarcFileUploadSession,
)
from palace.manager.sqlalchemy.model.resource import Representation
from tests.fixtures.redis import RedisFixture
from tests.fixtures.s3 import S3ServiceFixture, S3ServiceIntegrationFixture


class MarcUploadManagerFixture:
    def __init__(
        self, redis_fixture: RedisFixture, s3_service_fixture: S3ServiceFixture
    ):
        self._redis_fixture = redis_fixture
        self._s3_service_fixture = s3_service_fixture

        self.test_key1 = "test.123"
        self.test_record1 = b"test_record_123"
        self.test_key2 = "test*456"
        self.test_record2 = b"test_record_456"
        self.test_key3 = "test--?789"
        self.test_record3 = b"test_record_789"

        self.mock_s3_service = s3_service_fixture.mock_service()
        # Reduce the minimum upload size to make testing easier
        self.mock_s3_service.MINIMUM_MULTIPART_UPLOAD_SIZE = len(self.test_record1) * 4
        self.redis_client = redis_fixture.client

        self.mock_collection_id = 52

        self.uploads = MarcFileUploadSession(self.redis_client, self.mock_collection_id)
        self.uploader = MarcUploadManager(self.mock_s3_service, self.uploads)


@pytest.fixture
def marc_upload_manager_fixture(
    redis_fixture: RedisFixture, s3_service_fixture: S3ServiceFixture
):
    return MarcUploadManagerFixture(redis_fixture, s3_service_fixture)


class TestMarcUploadManager:
    def test_begin(
        self,
        marc_upload_manager_fixture: MarcUploadManagerFixture,
        redis_fixture: RedisFixture,
    ):
        uploader = marc_upload_manager_fixture.uploader

        assert uploader.locked is False
        assert marc_upload_manager_fixture.uploads.locked(by_us=True) is False

        with uploader.begin() as u:
            # The context manager returns the uploader object
            assert u is uploader

            # It directly tells us the lock status
            assert uploader.locked is True

            # The lock is also reflected in the uploads object
            assert marc_upload_manager_fixture.uploads.locked(by_us=True) is True  # type: ignore[unreachable]

        # The lock is released after the context manager exits
        assert uploader.locked is False  # type: ignore[unreachable]
        assert marc_upload_manager_fixture.uploads.locked(by_us=True) is False

        # If an exception occurs, the lock is deleted and the exception is raised by calling
        # the _abort method
        mock_abort = MagicMock(wraps=uploader._abort)
        uploader._abort = mock_abort
        with pytest.raises(Exception):
            with uploader.begin():
                assert uploader.locked is True
                raise Exception()
        assert (
            redis_fixture.client.json().get(marc_upload_manager_fixture.uploads.key)
            is None
        )
        mock_abort.assert_called_once()

        # If a expected celery exception occurs, the lock is released, but not deleted
        # and the abort method isn't called
        mock_abort.reset_mock()
        for exception in Retry, Ignore:
            with pytest.raises(exception):
                with uploader.begin():
                    assert uploader.locked is True
                    raise exception()
            assert marc_upload_manager_fixture.uploads.locked(by_us=True) is False
            assert (
                redis_fixture.client.json().get(marc_upload_manager_fixture.uploads.key)
                is not None
            )
            mock_abort.assert_not_called()

    def test_add_record(self, marc_upload_manager_fixture: MarcUploadManagerFixture):
        uploader = marc_upload_manager_fixture.uploader

        uploader.add_record(
            marc_upload_manager_fixture.test_key1,
            marc_upload_manager_fixture.test_record1,
        )
        assert (
            uploader._buffers[marc_upload_manager_fixture.test_key1]
            == marc_upload_manager_fixture.test_record1.decode()
        )

        uploader.add_record(
            marc_upload_manager_fixture.test_key1,
            marc_upload_manager_fixture.test_record1,
        )
        assert (
            uploader._buffers[marc_upload_manager_fixture.test_key1]
            == marc_upload_manager_fixture.test_record1.decode() * 2
        )

    def test_sync(self, marc_upload_manager_fixture: MarcUploadManagerFixture):
        uploader = marc_upload_manager_fixture.uploader

        uploader.add_record(
            marc_upload_manager_fixture.test_key1,
            marc_upload_manager_fixture.test_record1,
        )
        uploader.add_record(
            marc_upload_manager_fixture.test_key2,
            marc_upload_manager_fixture.test_record2 * 2,
        )
        with uploader.begin():
            uploader.sync()

        # Sync clears the local buffer
        assert uploader._buffers == {}

        # And pushes the local records to redis
        assert marc_upload_manager_fixture.uploads.get() == {
            marc_upload_manager_fixture.test_key1: MarcFileUpload(
                buffer=marc_upload_manager_fixture.test_record1
            ),
            marc_upload_manager_fixture.test_key2: MarcFileUpload(
                buffer=marc_upload_manager_fixture.test_record2 * 2
            ),
        }

        # Because the buffer did not contain enough data, it was not uploaded to S3
        assert marc_upload_manager_fixture.mock_s3_service.upload_in_progress == {}

        # Add enough data for test_key1 to be uploaded to S3
        uploader.add_record(
            marc_upload_manager_fixture.test_key1,
            marc_upload_manager_fixture.test_record1 * 2,
        )
        uploader.add_record(
            marc_upload_manager_fixture.test_key1,
            marc_upload_manager_fixture.test_record1 * 2,
        )
        uploader.add_record(
            marc_upload_manager_fixture.test_key2,
            marc_upload_manager_fixture.test_record2,
        )

        with uploader.begin():
            uploader.sync()

        # The buffer is cleared
        assert uploader._buffers == {}

        # Because the data for test_key1 was large enough, it was uploaded to S3, and its redis data structure was
        # updated to reflect this. test_key2 was not large enough to upload, so it remains in redis and not in s3.
        redis_data = marc_upload_manager_fixture.uploads.get()
        assert redis_data[marc_upload_manager_fixture.test_key2] == MarcFileUpload(
            buffer=marc_upload_manager_fixture.test_record2 * 3
        )
        redis_data_test1 = redis_data[marc_upload_manager_fixture.test_key1]
        assert redis_data_test1.buffer == ""

        assert len(marc_upload_manager_fixture.mock_s3_service.upload_in_progress) == 1
        assert (
            marc_upload_manager_fixture.test_key1
            in marc_upload_manager_fixture.mock_s3_service.upload_in_progress
        )
        upload = marc_upload_manager_fixture.mock_s3_service.upload_in_progress[
            marc_upload_manager_fixture.test_key1
        ]
        assert upload.upload_id is not None
        assert upload.content_type is Representation.MARC_MEDIA_TYPE
        [part] = upload.parts.values()
        assert part.content == marc_upload_manager_fixture.test_record1 * 5

        # And the s3 part data and upload_id is synced to redis
        assert redis_data_test1.parts == [part.part_data]
        assert redis_data_test1.upload_id == upload.upload_id

    def test_complete(self, marc_upload_manager_fixture: MarcUploadManagerFixture):
        uploader = marc_upload_manager_fixture.uploader

        # Wrap the clear method so we can check if it was called
        mock_clear_uploads = MagicMock(
            wraps=marc_upload_manager_fixture.uploads.clear_uploads
        )
        marc_upload_manager_fixture.uploads.clear_uploads = mock_clear_uploads

        # Set up the records for the test
        uploader.add_record(
            marc_upload_manager_fixture.test_key1,
            marc_upload_manager_fixture.test_record1 * 5,
        )
        uploader.add_record(
            marc_upload_manager_fixture.test_key2,
            marc_upload_manager_fixture.test_record2 * 5,
        )
        with uploader.begin():
            uploader.sync()

        uploader.add_record(
            marc_upload_manager_fixture.test_key1,
            marc_upload_manager_fixture.test_record1 * 5,
        )
        with uploader.begin():
            uploader.sync()

        uploader.add_record(
            marc_upload_manager_fixture.test_key2,
            marc_upload_manager_fixture.test_record2,
        )

        uploader.add_record(
            marc_upload_manager_fixture.test_key3,
            marc_upload_manager_fixture.test_record3,
        )

        # Complete the uploads
        with uploader.begin():
            completed = uploader.complete()

        # The complete method should return the keys that were completed
        assert completed == {
            marc_upload_manager_fixture.test_key1,
            marc_upload_manager_fixture.test_key2,
            marc_upload_manager_fixture.test_key3,
        }

        # The local buffers should be empty
        assert uploader._buffers == {}

        # The redis record should have the completed uploads cleared
        mock_clear_uploads.assert_called_once()

        # The s3 service should have the completed uploads
        assert len(marc_upload_manager_fixture.mock_s3_service.uploads) == 3
        assert len(marc_upload_manager_fixture.mock_s3_service.upload_in_progress) == 0

        test_key1_upload = marc_upload_manager_fixture.mock_s3_service.uploads[
            marc_upload_manager_fixture.test_key1
        ]
        assert test_key1_upload.key == marc_upload_manager_fixture.test_key1
        assert test_key1_upload.content == marc_upload_manager_fixture.test_record1 * 10
        assert test_key1_upload.media_type == Representation.MARC_MEDIA_TYPE

        test_key2_upload = marc_upload_manager_fixture.mock_s3_service.uploads[
            marc_upload_manager_fixture.test_key2
        ]
        assert test_key2_upload.key == marc_upload_manager_fixture.test_key2
        assert test_key2_upload.content == marc_upload_manager_fixture.test_record2 * 6
        assert test_key2_upload.media_type == Representation.MARC_MEDIA_TYPE

        test_key3_upload = marc_upload_manager_fixture.mock_s3_service.uploads[
            marc_upload_manager_fixture.test_key3
        ]
        assert test_key3_upload.key == marc_upload_manager_fixture.test_key3
        assert test_key3_upload.content == marc_upload_manager_fixture.test_record3
        assert test_key3_upload.media_type == Representation.MARC_MEDIA_TYPE

    def test__abort(
        self,
        marc_upload_manager_fixture: MarcUploadManagerFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        uploader = marc_upload_manager_fixture.uploader

        # Set up the records for the test
        uploader.add_record(
            marc_upload_manager_fixture.test_key1,
            marc_upload_manager_fixture.test_record1 * 10,
        )
        uploader.add_record(
            marc_upload_manager_fixture.test_key2,
            marc_upload_manager_fixture.test_record2 * 10,
        )
        with uploader.begin():
            uploader.sync()

        # Mock the multipart_abort method so we can check if it was called and have it
        # raise an exception on the first call
        mock_abort = MagicMock(side_effect=[Exception("Boom"), None])
        marc_upload_manager_fixture.mock_s3_service.multipart_abort = mock_abort

        # Wrap the delete method so we can check if it was called
        mock_delete = MagicMock(wraps=marc_upload_manager_fixture.uploads.delete)
        marc_upload_manager_fixture.uploads.delete = mock_delete

        upload_id_1 = marc_upload_manager_fixture.mock_s3_service.upload_in_progress[
            marc_upload_manager_fixture.test_key1
        ].upload_id
        upload_id_2 = marc_upload_manager_fixture.mock_s3_service.upload_in_progress[
            marc_upload_manager_fixture.test_key2
        ].upload_id

        # Abort the uploads, the original exception should propagate, and the exception
        # thrown by the first call to abort should be logged
        with pytest.raises(Exception) as exc_info:
            with uploader.begin():
                raise Exception("Bang")
        assert str(exc_info.value) == "Bang"

        assert (
            f"Failed to abort upload {marc_upload_manager_fixture.test_key1} (UploadID: {upload_id_1}) due to exception (Boom)"
            in caplog.text
        )

        mock_abort.assert_has_calls(
            [
                call(marc_upload_manager_fixture.test_key1, upload_id_1),
                call(marc_upload_manager_fixture.test_key2, upload_id_2),
            ]
        )

        # The redis record should have been deleted
        mock_delete.assert_called_once()

    def test_real_storage_service(
        self,
        redis_fixture: RedisFixture,
        s3_service_integration_fixture: S3ServiceIntegrationFixture,
    ):
        """
        Full end-to-end test of the MarcUploadManager using the real S3Service
        """
        s3_service = s3_service_integration_fixture.public
        uploads = MarcFileUploadSession(redis_fixture.client, 99)
        uploader = MarcUploadManager(s3_service, uploads)
        batch_size = s3_service.MINIMUM_MULTIPART_UPLOAD_SIZE + 1

        with uploader.begin() as locked:
            assert locked

            # Test three buffer size cases for the complete() method.
            #
            # 1. A small record that isn't in S3 at the time `complete` is called (test1).
            # 2. A large record that needs to be uploaded in parts. On the first `sync`
            #    call, its buffer is large enough to trigger an upload. When `complete` is
            #    called, the buffer has data waiting for upload (test2).
            # 3. A large record that needs to be uploaded in parts. On the first `sync`
            #    call, its buffer is large enough to trigger the upload. When `complete`
            #    is called, the buffer is empty (test3).

            uploader.add_record("test1", b"test_record")
            uploader.add_record("test2", b"a" * batch_size)
            uploader.add_record("test3", b"b" * batch_size)

            # Start the sync. This will begin the multipart upload for test2 and test3.
            uploader.sync()

            # Add some more data
            uploader.add_record("test1", b"test_record")
            uploader.add_record("test2", b"a" * batch_size)

            # Complete the uploads
            completed = uploader.complete()

        assert completed == {"test1", "test2", "test3"}
        assert uploads.get() == {}
        assert set(s3_service_integration_fixture.list_objects("public")) == completed

        assert (
            s3_service_integration_fixture.get_object("public", "test1")
            == b"test_record" * 2
        )
        assert (
            s3_service_integration_fixture.get_object("public", "test2")
            == b"a" * batch_size * 2
        )
        assert (
            s3_service_integration_fixture.get_object("public", "test3")
            == b"b" * batch_size
        )

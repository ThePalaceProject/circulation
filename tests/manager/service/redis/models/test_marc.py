import string

import pytest

from palace.manager.service.redis.models.marc import (
    MarcFileUpload,
    MarcFileUploadSession,
    MarcFileUploadSessionError,
    MarcFileUploadState,
)
from palace.manager.service.redis.redis import Pipeline
from palace.manager.service.storage.s3 import MultipartS3UploadPart
from tests.fixtures.redis import RedisFixture


class MarcFileUploadSessionFixture:
    def __init__(self, redis_fixture: RedisFixture):
        self._redis_fixture = redis_fixture

        self.mock_collection_id = 1

        self.uploads = MarcFileUploadSession(
            self._redis_fixture.client, self.mock_collection_id
        )

        # Some keys with special characters to make sure they are handled correctly.
        self.mock_upload_key_1 = "test/test1/?$xyz.abc"
        self.mock_upload_key_2 = "t'est💣/tëst2.\"ext`"
        self.mock_upload_key_3 = string.printable

        self.mock_unset_upload_key = "test4"

        self.test_data = {
            self.mock_upload_key_1: "test",
            self.mock_upload_key_2: "another_test",
            self.mock_upload_key_3: "another_another_test",
        }

        self.part_1 = MultipartS3UploadPart(etag="abc", part_number=1)
        self.part_2 = MultipartS3UploadPart(etag="def", part_number=2)

    def load_test_data(self) -> dict[str, int]:
        lock_acquired = False
        if not self.uploads.locked():
            self.uploads.acquire()
            lock_acquired = True

        return_value = self.uploads.append_buffers(self.test_data)

        if lock_acquired:
            self.uploads.release()

        return return_value

    def test_data_records(self, *keys: str) -> dict[str, MarcFileUpload]:
        return {key: MarcFileUpload(buffer=self.test_data[key]) for key in keys}


@pytest.fixture
def marc_file_upload_session_fixture(redis_fixture: RedisFixture):
    return MarcFileUploadSessionFixture(redis_fixture)


class TestMarcFileUploadSession:
    def test__pipeline(
        self, marc_file_upload_session_fixture: MarcFileUploadSessionFixture
    ):
        uploads = marc_file_upload_session_fixture.uploads

        # Using the _pipeline() context manager makes sure that we hold the lock
        with pytest.raises(MarcFileUploadSessionError) as exc_info:
            with uploads._pipeline():
                pass
        assert "Must hold lock" in str(exc_info.value)

        uploads.acquire()

        # It also checks that the update_number is correct
        uploads._update_number = 1
        with pytest.raises(MarcFileUploadSessionError) as exc_info:
            with uploads._pipeline():
                pass
        assert "Update number mismatch" in str(exc_info.value)

        uploads._update_number = 0
        with uploads._pipeline() as pipe:
            # If the lock and update number are correct, we should get a pipeline object
            assert isinstance(pipe, Pipeline)

            # We are watching the key for this object, so that we know all the data within the
            # transaction is consistent, and we are still holding the lock when the pipeline
            # executes
            assert pipe.watching is True

            # By default it starts the pipeline transaction
            assert pipe.explicit_transaction is True

        # We can also start the pipeline without a transaction
        with uploads._pipeline(begin_transaction=False) as pipe:
            assert pipe.explicit_transaction is False

    def test__execute_pipeline(
        self,
        marc_file_upload_session_fixture: MarcFileUploadSessionFixture,
        redis_fixture: RedisFixture,
    ):
        client = redis_fixture.client
        uploads = marc_file_upload_session_fixture.uploads
        uploads.acquire()

        # If we try to execute a pipeline without a transaction, we should get an error
        with pytest.raises(MarcFileUploadSessionError) as exc_info:
            with uploads._pipeline(begin_transaction=False) as pipe:
                uploads._execute_pipeline(pipe, 0)
        assert "Pipeline should be in explicit transaction mode" in str(exc_info.value)

        # The _execute_pipeline function takes care of extending the timeout and incrementing
        # the update number and setting the state of the session
        [update_number] = client.json().get(
            uploads.key, uploads._update_number_json_key
        )
        client.pexpire(uploads.key, 500)
        old_state = uploads.state()
        with uploads._pipeline() as pipe:
            # If we execute the pipeline, we should get a list of results, excluding the
            # operations that _execute_pipeline does.
            assert uploads._execute_pipeline(pipe, 2) == []
        [new_update_number] = client.json().get(
            uploads.key, uploads._update_number_json_key
        )
        assert new_update_number == update_number + 2
        assert client.pttl(uploads.key) > 500
        assert uploads.state() != old_state
        assert uploads.state() == MarcFileUploadState.UPLOADING

        # If we try to execute a pipeline that has been modified by another process, we should get an error
        with uploads._pipeline() as pipe:
            client.json().set(
                uploads.key, uploads._update_number_json_key, update_number
            )
            with pytest.raises(MarcFileUploadSessionError) as exc_info:
                uploads._execute_pipeline(pipe, 1)
            assert "Another process is modifying the buffers" in str(exc_info.value)

    def test_append_buffers(
        self, marc_file_upload_session_fixture: MarcFileUploadSessionFixture
    ):
        uploads = marc_file_upload_session_fixture.uploads

        # If we try to update buffers without acquiring the lock, we should get an error
        with pytest.raises(MarcFileUploadSessionError) as exc_info:
            uploads.append_buffers(
                {marc_file_upload_session_fixture.mock_upload_key_1: "test"}
            )
        assert "Must hold lock" in str(exc_info.value)

        # Acquire the lock and try to update buffers
        with uploads.lock() as locked:
            assert locked
            assert uploads.append_buffers({}) == {}

            assert uploads.append_buffers(
                {
                    marc_file_upload_session_fixture.mock_upload_key_1: "test",
                    marc_file_upload_session_fixture.mock_upload_key_2: "another_test",
                }
            ) == {
                marc_file_upload_session_fixture.mock_upload_key_1: 4,
                marc_file_upload_session_fixture.mock_upload_key_2: 12,
            }
            assert uploads._update_number == 2

            assert uploads.append_buffers(
                {
                    marc_file_upload_session_fixture.mock_upload_key_1: "x",
                    marc_file_upload_session_fixture.mock_upload_key_2: "y",
                    marc_file_upload_session_fixture.mock_upload_key_3: "new",
                }
            ) == {
                marc_file_upload_session_fixture.mock_upload_key_1: 5,
                marc_file_upload_session_fixture.mock_upload_key_2: 13,
                marc_file_upload_session_fixture.mock_upload_key_3: 3,
            }
            assert uploads._update_number == 5

            # If we try to update buffers with an old update number, we should get an error
            uploads._update_number = 4
            with pytest.raises(MarcFileUploadSessionError) as exc_info:
                uploads.append_buffers(marc_file_upload_session_fixture.test_data)
            assert "Update number mismatch" in str(exc_info.value)

        # Exiting the context manager should release the lock
        assert not uploads.locked()

    def test_get(self, marc_file_upload_session_fixture: MarcFileUploadSessionFixture):
        uploads = marc_file_upload_session_fixture.uploads

        assert uploads.get() == {}
        assert uploads.get(marc_file_upload_session_fixture.mock_upload_key_1) == {}

        marc_file_upload_session_fixture.load_test_data()

        # You don't need to acquire the lock to get the uploads, but you should if you
        # are using the data to do updates.

        # You can get a subset of the uploads
        assert uploads.get(
            marc_file_upload_session_fixture.mock_upload_key_1,
        ) == marc_file_upload_session_fixture.test_data_records(
            marc_file_upload_session_fixture.mock_upload_key_1
        )

        # Or multiple uploads, any that don't exist are not included in the result dict
        assert uploads.get(
            [
                marc_file_upload_session_fixture.mock_upload_key_1,
                marc_file_upload_session_fixture.mock_upload_key_2,
                marc_file_upload_session_fixture.mock_unset_upload_key,
            ]
        ) == marc_file_upload_session_fixture.test_data_records(
            marc_file_upload_session_fixture.mock_upload_key_1,
            marc_file_upload_session_fixture.mock_upload_key_2,
        )

        # Or you can get all the uploads
        assert uploads.get() == marc_file_upload_session_fixture.test_data_records(
            marc_file_upload_session_fixture.mock_upload_key_1,
            marc_file_upload_session_fixture.mock_upload_key_2,
            marc_file_upload_session_fixture.mock_upload_key_3,
        )

    def test_set_upload_id(
        self, marc_file_upload_session_fixture: MarcFileUploadSessionFixture
    ):
        uploads = marc_file_upload_session_fixture.uploads

        # must hold lock to do update
        with pytest.raises(MarcFileUploadSessionError) as exc_info:
            uploads.set_upload_id(
                marc_file_upload_session_fixture.mock_upload_key_1, "xyz"
            )
        assert "Must hold lock" in str(exc_info.value)

        uploads.acquire()

        # We are unable to set an upload id for an item that hasn't been initialized
        with pytest.raises(MarcFileUploadSessionError) as exc_info:
            uploads.set_upload_id(
                marc_file_upload_session_fixture.mock_upload_key_1, "xyz"
            )
        assert "Failed to set upload ID" in str(exc_info.value)

        marc_file_upload_session_fixture.load_test_data()
        uploads.set_upload_id(marc_file_upload_session_fixture.mock_upload_key_1, "def")
        uploads.set_upload_id(marc_file_upload_session_fixture.mock_upload_key_2, "abc")

        all_uploads = uploads.get()
        assert (
            all_uploads[marc_file_upload_session_fixture.mock_upload_key_1].upload_id
            == "def"
        )
        assert (
            all_uploads[marc_file_upload_session_fixture.mock_upload_key_2].upload_id
            == "abc"
        )

        # We can't change the upload id for a library that has already been set
        with pytest.raises(MarcFileUploadSessionError) as exc_info:
            uploads.set_upload_id(
                marc_file_upload_session_fixture.mock_upload_key_1, "ghi"
            )
        assert "Failed to set upload ID" in str(exc_info.value)

        all_uploads = uploads.get()
        assert (
            all_uploads[marc_file_upload_session_fixture.mock_upload_key_1].upload_id
            == "def"
        )
        assert (
            all_uploads[marc_file_upload_session_fixture.mock_upload_key_2].upload_id
            == "abc"
        )

    def test_clear_uploads(
        self, marc_file_upload_session_fixture: MarcFileUploadSessionFixture
    ):
        uploads = marc_file_upload_session_fixture.uploads

        # must hold lock to do update
        with pytest.raises(MarcFileUploadSessionError) as exc_info:
            uploads.clear_uploads()
        assert "Must hold lock" in str(exc_info.value)

        uploads.acquire()

        # We are unable to clear the uploads for an item that hasn't been initialized
        with pytest.raises(MarcFileUploadSessionError) as exc_info:
            uploads.clear_uploads()
        assert "Failed to clear uploads" in str(exc_info.value)

        marc_file_upload_session_fixture.load_test_data()
        assert uploads.get() != {}

        uploads.clear_uploads()
        assert uploads.get() == {}

    def test_get_upload_ids(
        self, marc_file_upload_session_fixture: MarcFileUploadSessionFixture
    ):
        uploads = marc_file_upload_session_fixture.uploads

        # If the id is not set, we should get None
        assert uploads.get_upload_ids(
            [marc_file_upload_session_fixture.mock_upload_key_1]
        ) == {marc_file_upload_session_fixture.mock_upload_key_1: None}

        marc_file_upload_session_fixture.load_test_data()

        # If the buffer has been set, but the upload id has not, we should still get None
        assert uploads.get_upload_ids(
            [marc_file_upload_session_fixture.mock_upload_key_1]
        ) == {marc_file_upload_session_fixture.mock_upload_key_1: None}

        with uploads.lock() as locked:
            assert locked
            uploads.set_upload_id(
                marc_file_upload_session_fixture.mock_upload_key_1, "abc"
            )
            uploads.set_upload_id(
                marc_file_upload_session_fixture.mock_upload_key_2, "def"
            )
        assert uploads.get_upload_ids(
            marc_file_upload_session_fixture.mock_upload_key_1
        ) == {marc_file_upload_session_fixture.mock_upload_key_1: "abc"}
        assert uploads.get_upload_ids(
            [
                marc_file_upload_session_fixture.mock_upload_key_1,
                marc_file_upload_session_fixture.mock_upload_key_2,
            ]
        ) == {
            marc_file_upload_session_fixture.mock_upload_key_1: "abc",
            marc_file_upload_session_fixture.mock_upload_key_2: "def",
        }

    def test_add_part_and_clear_buffer(
        self, marc_file_upload_session_fixture: MarcFileUploadSessionFixture
    ):
        uploads = marc_file_upload_session_fixture.uploads

        # If we try to add parts without acquiring the lock, we should get an error
        with pytest.raises(MarcFileUploadSessionError) as exc_info:
            uploads.add_part_and_clear_buffer(
                marc_file_upload_session_fixture.mock_upload_key_1,
                marc_file_upload_session_fixture.part_1,
            )
        assert "Must hold lock" in str(exc_info.value)

        # Acquire the lock
        uploads.acquire()

        # We are unable to add parts to a library whose buffers haven't been initialized
        with pytest.raises(MarcFileUploadSessionError) as exc_info:
            uploads.add_part_and_clear_buffer(
                marc_file_upload_session_fixture.mock_upload_key_1,
                marc_file_upload_session_fixture.part_1,
            )
        assert "Failed to add part and clear buffer" in str(exc_info.value)

        marc_file_upload_session_fixture.load_test_data()

        # We are able to add parts to a library that exists
        uploads.add_part_and_clear_buffer(
            marc_file_upload_session_fixture.mock_upload_key_1,
            marc_file_upload_session_fixture.part_1,
        )
        uploads.add_part_and_clear_buffer(
            marc_file_upload_session_fixture.mock_upload_key_2,
            marc_file_upload_session_fixture.part_1,
        )
        uploads.add_part_and_clear_buffer(
            marc_file_upload_session_fixture.mock_upload_key_1,
            marc_file_upload_session_fixture.part_2,
        )

        all_uploads = uploads.get()
        # The parts are added in order and the buffers are cleared
        assert all_uploads[
            marc_file_upload_session_fixture.mock_upload_key_1
        ].parts == [
            marc_file_upload_session_fixture.part_1,
            marc_file_upload_session_fixture.part_2,
        ]
        assert all_uploads[
            marc_file_upload_session_fixture.mock_upload_key_2
        ].parts == [marc_file_upload_session_fixture.part_1]
        assert (
            all_uploads[marc_file_upload_session_fixture.mock_upload_key_1].buffer == ""
        )
        assert (
            all_uploads[marc_file_upload_session_fixture.mock_upload_key_2].buffer == ""
        )

    def test_get_part_num_and_buffer(
        self, marc_file_upload_session_fixture: MarcFileUploadSessionFixture
    ):
        uploads = marc_file_upload_session_fixture.uploads

        # If the key has not been initialized, we get an exception
        with pytest.raises(MarcFileUploadSessionError) as exc_info:
            uploads.get_part_num_and_buffer(
                marc_file_upload_session_fixture.mock_upload_key_1
            )
        assert "Failed to get part number and buffer data" in str(exc_info.value)

        marc_file_upload_session_fixture.load_test_data()

        # If the buffer has been set, but no parts have been added
        assert uploads.get_part_num_and_buffer(
            marc_file_upload_session_fixture.mock_upload_key_1
        ) == (
            0,
            marc_file_upload_session_fixture.test_data[
                marc_file_upload_session_fixture.mock_upload_key_1
            ],
        )

        with uploads.lock() as locked:
            assert locked
            uploads.add_part_and_clear_buffer(
                marc_file_upload_session_fixture.mock_upload_key_1,
                marc_file_upload_session_fixture.part_1,
            )
            uploads.add_part_and_clear_buffer(
                marc_file_upload_session_fixture.mock_upload_key_1,
                marc_file_upload_session_fixture.part_2,
            )
            uploads.append_buffers(
                {
                    marc_file_upload_session_fixture.mock_upload_key_1: "1234567",
                }
            )

        assert uploads.get_part_num_and_buffer(
            marc_file_upload_session_fixture.mock_upload_key_1
        ) == (2, "1234567")

    def test_state(
        self, marc_file_upload_session_fixture: MarcFileUploadSessionFixture
    ):
        uploads = marc_file_upload_session_fixture.uploads

        # If the session doesn't exist, the state should be None
        assert uploads.state() is None

        # Once the state is created, by locking for example, the state should be SessionState.INITIAL
        with uploads.lock():
            assert uploads.state() == MarcFileUploadState.INITIAL

    def test_set_state(
        self, marc_file_upload_session_fixture: MarcFileUploadSessionFixture
    ):
        uploads = marc_file_upload_session_fixture.uploads

        # If we don't hold the lock, we can't set the state
        with pytest.raises(MarcFileUploadSessionError) as exc_info:
            uploads.set_state(MarcFileUploadState.UPLOADING)
        assert "Must hold lock" in str(exc_info.value)

        # Once the state is created, by locking for example, we can set the state
        with uploads.lock():
            uploads.set_state(MarcFileUploadState.UPLOADING)
            assert uploads.state() == MarcFileUploadState.UPLOADING

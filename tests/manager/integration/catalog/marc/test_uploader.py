from functools import partial
from io import BytesIO
from tempfile import TemporaryFile
from unittest.mock import create_autospec
from uuid import UUID, uuid4

import pytest

from palace.manager.integration.catalog.marc.uploader import (
    MarcUploadException,
    MarcUploadManager,
    UploadContext,
)
from palace.manager.util.datetime_helpers import datetime_utc
from tests.fixtures.s3 import S3ServiceFixture


class MarcUploadManagerFixture:
    def __init__(self, s3_service_fixture: S3ServiceFixture):
        self._s3_service_fixture = s3_service_fixture
        self.mock_s3_service = s3_service_fixture.mock_service()

        # Reduce the minimum upload size to make testing easier
        self.mock_s3_service.MINIMUM_MULTIPART_UPLOAD_SIZE = 4

        self.collection_name = "collection"
        self.library_short_name = "short_name"
        self.creation_time = datetime_utc(year=2001, month=1, day=1)

        self.create_uploader = partial(
            MarcUploadManager,
            storage_service=self.mock_s3_service,
            collection_name=self.collection_name,
            library_short_name=self.library_short_name,
            creation_time=self.creation_time,
            since_time=None,
        )


@pytest.fixture
def marc_upload_manager_fixture(s3_service_fixture: S3ServiceFixture):
    return MarcUploadManagerFixture(s3_service_fixture)


class TestMarcUploadManager:
    def test__s3_key(self) -> None:
        library_short_name = "short"
        collection_name = "Palace is great"
        uuid = UUID("c2370bf2-28e1-40ff-9f04-4864306bd11c")
        now = datetime_utc(2024, 8, 27)
        since = datetime_utc(2024, 8, 20)

        s3_key = partial(
            MarcUploadManager._s3_key, library_short_name, collection_name, now, uuid
        )
        collection_name_no_spaces = collection_name.replace(" ", "_")

        assert (
            s3_key()
            == f"marc/{library_short_name}/{collection_name_no_spaces}.full.2024-08-27.wjcL8ijhQP-fBEhkMGvRHA.mrc"
        )

        assert (
            s3_key(since_time=since)
            == f"marc/{library_short_name}/{collection_name_no_spaces}.delta.2024-08-20.2024-08-27.wjcL8ijhQP-fBEhkMGvRHA.mrc"
        )

    def test__init_(self, marc_upload_manager_fixture: MarcUploadManagerFixture):
        # If we initialize with a pre-existing context, the context is set directly
        context = UploadContext(
            upload_uuid=uuid4(), s3_key="s3_key", upload_id="upload_id"
        )
        uploader = marc_upload_manager_fixture.create_uploader(context=context)
        assert uploader.context is context

        # If we don't give a context, one is created and set
        uploader = marc_upload_manager_fixture.create_uploader()
        assert uploader.context.upload_id is None
        assert uploader.context.s3_key.startswith(
            f"marc/{marc_upload_manager_fixture.library_short_name}/"
            f"{marc_upload_manager_fixture.collection_name}.full.2001-01-01."
        )
        assert isinstance(uploader.context.upload_uuid, UUID)
        assert uploader.context.parts == []

    def test_begin_upload(self, marc_upload_manager_fixture: MarcUploadManagerFixture):
        uploader = marc_upload_manager_fixture.create_uploader()
        assert len(marc_upload_manager_fixture.mock_s3_service.upload_in_progress) == 0
        uploader.begin_upload()
        assert len(marc_upload_manager_fixture.mock_s3_service.upload_in_progress) == 1
        [upload] = (
            marc_upload_manager_fixture.mock_s3_service.upload_in_progress.values()
        )
        assert uploader.context.upload_id == upload.upload_id

    def test_upload_part(self, marc_upload_manager_fixture: MarcUploadManagerFixture):
        uploader = marc_upload_manager_fixture.create_uploader()

        # If begin_upload hasn't been called, it will be called by upload_part
        assert len(marc_upload_manager_fixture.mock_s3_service.upload_in_progress) == 0

        # Can upload parts as a binary file, or a byte string
        assert uploader.upload_part(b"test")
        with TemporaryFile() as f:
            f.write(b" another test")
            assert uploader.upload_part(f)

        # Empty parts are ignored
        assert not uploader.upload_part(b"")
        assert not uploader.upload_part(BytesIO())

        assert len(marc_upload_manager_fixture.mock_s3_service.upload_in_progress) == 1

        [upload_parts] = (
            marc_upload_manager_fixture.mock_s3_service.upload_in_progress.values()
        )
        assert len(upload_parts.parts) == 2

        # Complete the upload
        assert uploader.complete()
        [complete_upload] = marc_upload_manager_fixture.mock_s3_service.uploads.values()
        assert complete_upload.content == b"test another test"

        # Trying to add a part to a complete upload raises an error
        with pytest.raises(MarcUploadException, match="Upload is already finalized."):
            uploader.upload_part(b"123")

    def test_abort(self, marc_upload_manager_fixture: MarcUploadManagerFixture):
        # If an upload hasn't been started abort just sets finalized
        uploader = marc_upload_manager_fixture.create_uploader()
        uploader.abort()
        assert uploader.finalized
        assert len(marc_upload_manager_fixture.mock_s3_service.upload_in_progress) == 0
        assert len(marc_upload_manager_fixture.mock_s3_service.aborted) == 0

        # Otherwise abort calls to the API to abort the upload
        uploader = marc_upload_manager_fixture.create_uploader()
        uploader.begin_upload()
        uploader.abort()
        assert uploader.finalized
        assert (
            uploader.context.s3_key
            in marc_upload_manager_fixture.mock_s3_service.aborted
        )

        # calling abort again is a no-op
        uploader.abort()

    def test_complete(self, marc_upload_manager_fixture: MarcUploadManagerFixture):
        # If the upload hasn't started, the upload isn't aborted, but it is finalized
        uploader = marc_upload_manager_fixture.create_uploader()
        assert not uploader.complete()
        assert uploader.finalized

        # If the upload has no parts, it is aborted
        uploader = marc_upload_manager_fixture.create_uploader()
        uploader.begin_upload()
        assert not uploader.complete()
        assert uploader.finalized
        assert (
            uploader.context.s3_key
            in marc_upload_manager_fixture.mock_s3_service.aborted
        )

        # Upload with parts is completed
        uploader = marc_upload_manager_fixture.create_uploader()
        uploader.upload_part(b"test data")
        assert uploader.complete()
        assert uploader.finalized
        assert (
            uploader.context.s3_key
            in marc_upload_manager_fixture.mock_s3_service.uploads
        )
        assert (
            marc_upload_manager_fixture.mock_s3_service.uploads[
                uploader.context.s3_key
            ].content
            == b"test data"
        )

        # Calling complete a second time raises an exception
        with pytest.raises(MarcUploadException, match="Upload is already finalized."):
            uploader.complete()

    def test_context_manager(
        self,
        marc_upload_manager_fixture: MarcUploadManagerFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        # Nesting context manager raises an exception
        uploader = marc_upload_manager_fixture.create_uploader()
        with uploader:
            with pytest.raises(
                MarcUploadException, match="Cannot nest MarcUploadManager"
            ):
                with uploader:
                    ...

        # The context manager doesn't complete an in-progress upload
        with marc_upload_manager_fixture.create_uploader() as uploader:
            uploader.upload_part(b"test data")
        assert not uploader.finalized
        assert len(marc_upload_manager_fixture.mock_s3_service.uploads) == 0

        # But if there is an exception it cleans up the upload
        with pytest.raises(Exception, match="Boom!"):
            with uploader:
                raise Exception("Boom!")
        assert uploader.finalized
        assert len(marc_upload_manager_fixture.mock_s3_service.uploads) == 0
        assert len(marc_upload_manager_fixture.mock_s3_service.aborted) == 1

        # If the exception causes an exception, we just swallow the exception
        # and log it, since we are already handing the outer exception.
        uploader = marc_upload_manager_fixture.create_uploader()
        uploader.abort = create_autospec(
            uploader.abort, side_effect=Exception("Another exception")
        )
        caplog.clear()
        with pytest.raises(Exception, match="Boom!"):
            with uploader:
                raise Exception("Boom!")
        assert "Failed to abort upload" in caplog.text
        assert "due to exception (Another exception)." in caplog.text

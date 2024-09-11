from collections import defaultdict
from collections.abc import Generator, Sequence
from contextlib import contextmanager

from celery.exceptions import Ignore, Retry
from typing_extensions import Self

from palace.manager.service.redis.models.marc import MarcFileUploadSession
from palace.manager.service.storage.s3 import S3Service
from palace.manager.sqlalchemy.model.resource import Representation
from palace.manager.util.log import LoggerMixin


class MarcUploadManager(LoggerMixin):
    """
    This class is used to manage the upload of MARC files to S3. The upload is done in multiple
    parts, so that the Celery task can be broken up into multiple steps, saving the progress
    between steps to redis, and flushing them to S3 when the buffer is large enough.

    This class orchestrates the upload process, delegating the redis operation to the
    `MarcFileUploadSession` class, and the S3 upload to the `S3Service` class.
    """

    def __init__(
        self, storage_service: S3Service, upload_session: MarcFileUploadSession
    ):
        self.storage_service = storage_service
        self.upload_session = upload_session
        self._buffers: defaultdict[str, str] = defaultdict(str)
        self._locked = False

    @property
    def locked(self) -> bool:
        return self._locked

    @property
    def update_number(self) -> int:
        return self.upload_session.update_number

    def add_record(self, key: str, record: bytes) -> None:
        self._buffers[key] += record.decode()

    def _s3_sync(self, needs_upload: Sequence[str]) -> None:
        upload_ids = self.upload_session.get_upload_ids(needs_upload)
        for key in needs_upload:
            if upload_ids.get(key) is None:
                upload_id = self.storage_service.multipart_create(
                    key, content_type=Representation.MARC_MEDIA_TYPE
                )
                self.upload_session.set_upload_id(key, upload_id)
                upload_ids[key] = upload_id

            part_number, data = self.upload_session.get_part_num_and_buffer(key)
            upload_part = self.storage_service.multipart_upload(
                key, upload_ids[key], part_number, data.encode()
            )
            self.upload_session.add_part_and_clear_buffer(key, upload_part)

    def sync(self, *, complete: bool = False) -> None:
        # First sync our buffers to redis
        buffer_lengths = self.upload_session.append_buffers(self._buffers)
        self._buffers.clear()

        # Then, if any of our redis buffers are large enough, or the upload is complete
        # sync them to S3.
        needs_upload = [
            key
            for key, length in buffer_lengths.items()
            if length > self.storage_service.MINIMUM_MULTIPART_UPLOAD_SIZE or complete
        ]

        if not needs_upload:
            return

        self._s3_sync(needs_upload)

    def _abort(self) -> None:
        in_progress = self.upload_session.get()
        for key, upload in in_progress.items():
            if upload.upload_id is None:
                # This upload has not started, so there is nothing to abort.
                continue
            try:
                self.storage_service.multipart_abort(key, upload.upload_id)
            except Exception as e:
                # We log and keep going, since we want to abort as many uploads as possible
                # even if some fail, this is likely already being called in an exception handler.
                # So we want to do as much cleanup as possible.
                self.log.exception(
                    f"Failed to abort upload {key} (UploadID: {upload.upload_id}) due to exception ({e})."
                )

        # Delete our in-progress uploads from redis as well
        self.remove_session()

    def complete(self) -> set[str]:
        # Make sure any local data we have is synced
        self.sync(complete=True)

        in_progress = self.upload_session.get()
        for key, upload in in_progress.items():
            # Since the sync method is called with complete=True, all the data should be in S3
            # and have a valid upload_id. Therefore, we can complete the upload. Mypy doesn't
            # know that upload_id shouldn't be None though, so we assert it for safety.
            assert upload.upload_id is not None

            # Complete the multipart upload
            self.storage_service.multipart_complete(key, upload.upload_id, upload.parts)

        # Delete our in-progress uploads data from redis
        if in_progress:
            self.upload_session.clear_uploads()

        # Return the keys that were uploaded
        return set(in_progress.keys())

    def remove_session(self) -> None:
        self.upload_session.delete()

    @contextmanager
    def begin(self) -> Generator[Self, None, None]:
        self._locked = self.upload_session.acquire()
        try:
            yield self
        except Exception as e:
            # We want to ignore any celery exceptions that are expected, but
            # handle cleanup for any other cases.
            if not isinstance(e, (Retry, Ignore)):
                self._abort()
            raise
        finally:
            self.upload_session.release()
            self._locked = False

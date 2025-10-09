import datetime
import uuid
from types import TracebackType
from typing import IO, Literal, Self

from pydantic import BaseModel

from palace.manager.core.exceptions import BasePalaceException
from palace.manager.service.storage.s3 import MultipartS3UploadPart, S3Service
from palace.manager.sqlalchemy.model.resource import Representation
from palace.manager.util.log import LoggerMixin
from palace.manager.util.uuid import uuid_encode


class UploadContext(BaseModel):
    upload_uuid: uuid.UUID
    s3_key: str
    upload_id: str | None = None
    parts: list[MultipartS3UploadPart] = []


class MarcUploadException(BasePalaceException): ...


class MarcUploadManager(LoggerMixin):
    """
    This class is used to manage the upload of MARC files to S3. The upload is done in multiple
    parts, so that the Celery task can be broken up into multiple steps.
    """

    def __init__(
        self,
        storage_service: S3Service,
        collection_name: str,
        library_short_name: str,
        creation_time: datetime.datetime,
        since_time: datetime.datetime | None,
        context: UploadContext | None = None,
    ):
        self.storage_service = storage_service
        self._in_context_manager = False
        self._finalized = False

        if context is None:
            upload_uuid = uuid.uuid4()
            s3_key = self._s3_key(
                library_short_name,
                collection_name,
                creation_time,
                upload_uuid,
                since_time,
            )
            context = UploadContext(
                upload_uuid=upload_uuid,
                s3_key=s3_key,
            )
        self.context = context

    @staticmethod
    def _s3_key(
        library_short_name: str,
        collection_name: str,
        creation_time: datetime.datetime,
        upload_uuid: uuid.UUID,
        since_time: datetime.datetime | None = None,
    ) -> str:
        """The path to the hosted MARC file for the given library, collection,
        and date range."""

        def date_to_string(date: datetime.datetime) -> str:
            return date.astimezone(datetime.UTC).strftime("%Y-%m-%d")

        root = "marc"
        creation = date_to_string(creation_time)

        if since_time:
            file_type = f"delta.{date_to_string(since_time)}.{creation}"
        else:
            file_type = f"full.{creation}"

        uuid_encoded = uuid_encode(upload_uuid)
        collection_name = collection_name.replace(" ", "_")
        filename = f"{collection_name}.{file_type}.{uuid_encoded}.mrc"
        parts = [root, library_short_name, filename]
        return "/".join(parts)

    def __enter__(self) -> Self:
        if self._in_context_manager:
            raise MarcUploadException(f"Cannot nest {self.__class__.__name__}.")
        self._in_context_manager = True
        return self

    def __exit__(
        self,
        exctype: type[BaseException] | None,
        excinst: BaseException | None,
        exctb: TracebackType | None,
    ) -> Literal[False]:
        if excinst is not None and not self._finalized:
            self.log.error(
                "An exception occurred during upload of MARC files. Cancelling in progress upload.",
            )
            try:
                self.abort()
            except Exception as e:
                # We log and keep going, since this was already triggered by an exception.
                self.log.exception(
                    f"Failed to abort upload {self.context.s3_key} (UploadID: {self.context.upload_id}) due to exception ({e})."
                )

        self._in_context_manager = False
        return False

    def begin_upload(self) -> str:
        upload_id = self.storage_service.multipart_create(
            self.context.s3_key, content_type=Representation.MARC_MEDIA_TYPE
        )
        self.context.upload_id = upload_id
        return upload_id

    def upload_part(self, data: IO[bytes] | bytes) -> bool:
        if self._finalized:
            raise MarcUploadException("Upload is already finalized.")

        if isinstance(data, bytes):
            length = len(data)
        else:
            length = data.tell()
            data.seek(0)

        if length == 0:
            return False

        if self.context.upload_id is None:
            upload_id = self.begin_upload()
        else:
            upload_id = self.context.upload_id

        part_number = len(self.context.parts) + 1
        upload_part = self.storage_service.multipart_upload(
            self.context.s3_key, upload_id, part_number, data
        )
        self.context.parts.append(upload_part)
        return True

    def complete(self) -> bool:
        if self._finalized:
            raise MarcUploadException("Upload is already finalized.")

        if self.context.upload_id is None or not self.context.parts:
            self.abort()
            return False

        self.storage_service.multipart_complete(
            self.context.s3_key, self.context.upload_id, self.context.parts
        )
        self._finalized = True
        return True

    def abort(self) -> None:
        if self._finalized:
            return

        if self.context.upload_id is None:
            self._finalized = True
            return

        self.storage_service.multipart_abort(
            self.context.s3_key, self.context.upload_id
        )
        self._finalized = True

    @property
    def finalized(self) -> bool:
        return self._finalized

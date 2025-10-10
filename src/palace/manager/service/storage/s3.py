from __future__ import annotations

from functools import cached_property
from io import BytesIO
from string import Formatter
from types import TracebackType
from typing import IO, TYPE_CHECKING, Self
from urllib.parse import quote

from botocore.exceptions import BotoCoreError, ClientError
from pydantic import BaseModel, ConfigDict, Field

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.util.log import LoggerMixin

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.type_defs import FileobjTypeDef


class MultipartS3UploadPart(BaseModel):
    etag: str = Field(..., alias="ETag")
    part_number: int = Field(..., alias="PartNumber")
    model_config = ConfigDict(populate_by_name=True, frozen=True)


class MultipartS3ContextManager(LoggerMixin):
    def __init__(
        self,
        service: S3Service,
        key: str,
        media_type: str | None = None,
    ) -> None:
        self._service = service
        self.key = key
        self.parts: list[MultipartS3UploadPart] = []
        self.media_type = media_type
        self.upload_id: str | None = None
        self._complete = False
        self._exception: BaseException | None = None

    def __enter__(self) -> Self:
        if self.upload_id is not None:
            raise RuntimeError("Upload already in progress.")
        self.upload_id = self._service.multipart_create(self.key, self.media_type)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        if exc_val is None:
            self._upload_complete()
        else:
            self.log.debug(
                f"Exception {exc_type} occurred during upload of {self.key}. Aborting.",
                exc_info=exc_val,
            )
            self._upload_abort()
            self._exception = exc_val
        return True

    def upload_part(self, content: bytes) -> None:
        if self.complete or self.exception or self.upload_id is None:
            raise RuntimeError("Upload already complete or aborted.")

        result = self._service.multipart_upload(
            self.key, self.upload_id, len(self.parts) + 1, content
        )
        self.parts.append(result)

    def _upload_complete(self) -> None:
        if not self.parts:
            self.log.info(f"Upload of {self.key} was empty.")
            self._upload_abort()
        elif self.upload_id is None:
            raise RuntimeError("Upload ID not set.")
        else:
            self._service.multipart_complete(self.key, self.upload_id, self.parts)
            self._complete = True

    def _upload_abort(self) -> None:
        if self.upload_id is None:
            self.log.error("Upload ID not set, unable to abort.")
            return

        self._service.multipart_abort(self.key, self.upload_id)

    @cached_property
    def url(self) -> str:
        return self._service.generate_url(self.key)

    @property
    def complete(self) -> bool:
        return self._complete

    @property
    def exception(self) -> BaseException | None:
        return self._exception


class S3Service(LoggerMixin):
    MINIMUM_MULTIPART_UPLOAD_SIZE = 5 * 1024 * 1024  # 5MB
    DOWNLOADS_PREFIX = "downloads"

    def __init__(
        self,
        client: S3Client,
        region: str | None,
        bucket: str,
        url_template: str,
    ) -> None:
        self.client = client
        self.region = region
        self.bucket = bucket
        self.url_template = url_template

        # Validate the URL template.
        formatter = Formatter()
        field_tuple = formatter.parse(self.url_template)
        field_names = [field[1] for field in field_tuple]
        if "region" in field_names and self.region is None:
            raise CannotLoadConfiguration(
                f"URL template requires a region, but no region was provided ({self.url_template})."
            )
        if "key" not in field_names:
            raise CannotLoadConfiguration(
                f"URL template requires a key, but no key was provided ({self.url_template})."
            )

    @classmethod
    def factory(
        cls,
        client: S3Client,
        region: str | None,
        bucket: str | None,
        url_template: str,
    ) -> Self | None:
        if bucket is None:
            return None
        return cls(client, region, bucket, url_template)

    def generate_url(self, key: str) -> str:
        return self.url_template.format(
            bucket=self.bucket, key=quote(key), region=self.region
        )

    def delete(self, key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=key)

    def store(
        self,
        key: str,
        content: str | bytes,
        content_type: str | None = None,
    ) -> str | None:
        if isinstance(content, str):
            content = content.encode("utf8")
        return self.store_stream(
            key=key, stream=BytesIO(content), content_type=content_type
        )

    def store_stream(
        self,
        key: str,
        stream: FileobjTypeDef,
        content_type: str | None = None,
    ) -> str | None:
        try:
            extra_args = {} if content_type is None else {"ContentType": content_type}
            self.client.upload_fileobj(
                Fileobj=stream,
                Bucket=self.bucket,
                Key=key,
                ExtraArgs=extra_args,
            )
        except (BotoCoreError, ClientError) as e:
            # BotoCoreError happens when there's a problem with
            # the network transport. ClientError happens when
            # there's a problem with the credentials. Either way,
            # the best thing to do is treat this as a transient
            # error and try again later. There's no scenario where
            # giving up is the right move.
            self.log.exception(f"Error uploading {key}: {str(e)}")
            return None
        finally:
            stream.close()

        url = self.generate_url(key)
        self.log.info(f"Stored '{key}' to {url}.")
        return url

    def multipart(
        self, key: str, content_type: str | None = None
    ) -> MultipartS3ContextManager:
        return MultipartS3ContextManager(self, key, content_type)

    def multipart_create(self, key: str, content_type: str | None = None) -> str:
        params = {
            "Bucket": self.bucket,
            "Key": key,
        }
        if content_type is not None:
            params["ContentType"] = content_type
        upload = self.client.create_multipart_upload(**params)  # type: ignore[arg-type]
        return upload["UploadId"]

    def multipart_upload(
        self, key: str, upload_id: str, part_number: int, content: bytes | IO[bytes]
    ) -> MultipartS3UploadPart:
        self.log.info(f"Uploading part {part_number} of {key} to {self.bucket}")
        result = self.client.upload_part(
            Body=content,
            Bucket=self.bucket,
            Key=key,
            PartNumber=part_number,
            UploadId=upload_id,
        )
        return MultipartS3UploadPart(etag=result["ETag"], part_number=part_number)

    def multipart_complete(
        self, key: str, upload_id: str, parts: list[MultipartS3UploadPart]
    ) -> None:
        self.log.info(
            f"Completing multipart upload of {key} to {self.bucket} ({len(parts)} parts)."
        )
        self.client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload=dict(Parts=[part.model_dump(by_alias=True) for part in parts]),  # type: ignore[misc]
        )

    def multipart_abort(self, key: str, upload_id: str) -> None:
        self.log.info(f"Aborting multipart upload of {key} to {self.bucket}.")
        self.client.abort_multipart_upload(
            Bucket=self.bucket,
            Key=key,
            UploadId=upload_id,
        )

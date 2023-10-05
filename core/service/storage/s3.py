from __future__ import annotations

import dataclasses
import logging
import sys
from io import BytesIO
from string import Formatter
from types import TracebackType
from typing import TYPE_CHECKING, BinaryIO, List, Optional, Type
from urllib.parse import quote

from botocore.exceptions import BotoCoreError, ClientError

from core.config import CannotLoadConfiguration
from core.util.log import LoggerMixin

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from mypy_boto3_s3.type_defs import CreateMultipartUploadOutputTypeDef


@dataclasses.dataclass
class MultipartS3UploadPart:
    ETag: str
    PartNumber: int


class MultipartS3ContextManager(LoggerMixin):
    def __init__(
        self,
        client: S3Client,
        bucket: str,
        key: str,
        url: str,
        media_type: Optional[str] = None,
    ) -> None:
        self.client = client
        self.key = key
        self.bucket = bucket
        self.part_number = 1
        self.parts: List[MultipartS3UploadPart] = []
        self.media_type = media_type
        self.upload: Optional[CreateMultipartUploadOutputTypeDef] = None
        self.upload_id: Optional[str] = None
        self._complete = False
        self._url = url
        self._exception: Optional[BaseException] = None

    def __enter__(self) -> Self:
        params = {
            "Bucket": self.bucket,
            "Key": self.key,
        }
        if self.media_type is not None:
            params["ContentType"] = self.media_type
        self.upload = self.client.create_multipart_upload(**params)  # type: ignore[arg-type]
        self.upload_id = self.upload["UploadId"]
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
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
            if isinstance(exc_val, (ClientError, BotoCoreError)):
                return True
        return False

    def upload_part(self, content: bytes) -> None:
        if self.complete or self.exception or self.upload_id is None:
            raise RuntimeError("Upload already complete or aborted.")

        logging.info(
            f"Uploading part {self.part_number} of {self.key} to {self.bucket}"
        )
        result = self.client.upload_part(
            Body=content,
            Bucket=self.bucket,
            Key=self.key,
            PartNumber=self.part_number,
            UploadId=self.upload_id,
        )
        self.parts.append(MultipartS3UploadPart(result["ETag"], self.part_number))
        self.part_number += 1

    def _upload_complete(self) -> None:
        if not self.parts:
            logging.info(f"Upload of {self.key} was empty.")
            self._upload_abort()
        elif self.upload_id is None:
            raise RuntimeError("Upload ID not set.")
        else:
            self.client.complete_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self.upload_id,
                MultipartUpload=dict(Parts=[dataclasses.asdict(part) for part in self.parts]),  # type: ignore[misc]
            )
            self._complete = True

    def _upload_abort(self) -> None:
        logging.info(f"Aborting upload of {self.key}.")
        if self.upload_id is not None:
            self.client.abort_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self.upload_id,
            )
        else:
            logging.error("Upload ID not set, unable to abort.")

    @property
    def url(self) -> str:
        return self._url

    @property
    def complete(self) -> bool:
        return self._complete

    @property
    def exception(self) -> Optional[BaseException]:
        return self._exception


class S3Service(LoggerMixin):
    def __init__(
        self,
        client: S3Client,
        region: Optional[str],
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
                "URL template requires a region, but no region was provided."
            )
        if "key" not in field_names:
            raise CannotLoadConfiguration(
                "URL template requires a key, but no key was provided."
            )

    @classmethod
    def factory(
        cls,
        client: S3Client,
        region: Optional[str],
        bucket: Optional[str],
        url_template: str,
    ) -> Optional[Self]:
        if bucket is None:
            return None
        return cls(client, region, bucket, url_template)

    def generate_url(self, key: str) -> str:
        return self.url_template.format(
            bucket=self.bucket, key=quote(key), region=self.region
        )

    def store(
        self,
        key: str,
        content: str | bytes,
        content_type: Optional[str] = None,
    ) -> Optional[str]:
        if isinstance(content, str):
            content = content.encode("utf8")
        return self.store_stream(
            key=key, stream=BytesIO(content), content_type=content_type
        )

    def store_stream(
        self,
        key: str,
        stream: BinaryIO,
        content_type: Optional[str] = None,
    ) -> Optional[str]:
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
        self, key: str, content_type: Optional[str] = None
    ) -> MultipartS3ContextManager:
        url = self.generate_url(key)
        return MultipartS3ContextManager(
            self.client, self.bucket, key, url, content_type
        )

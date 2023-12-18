from __future__ import annotations

import functools
import sys
from typing import TYPE_CHECKING, BinaryIO, NamedTuple, Protocol
from unittest.mock import MagicMock

import pytest

from core.service.storage.s3 import MultipartS3ContextManager, S3Service

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client


class MockS3ServiceUpload(NamedTuple):
    key: str
    content: bytes
    media_type: str | None


class MockMultipartS3ContextManager(MultipartS3ContextManager):
    def __init__(
        self,
        parent: MockS3Service,
        bucket: str,
        key: str,
        url: str,
        media_type: str | None = None,
    ) -> None:
        self.parent = parent
        self.key = key
        self.bucket = bucket
        self.media_type = media_type
        self.content = b""
        self.content_parts: list[bytes] = []
        self._complete = False
        self._url = url
        self._exception = None

    def __enter__(self) -> Self:
        return self

    def upload_part(self, content: bytes) -> None:
        self.content_parts.append(content)
        self.content += content

    def _upload_complete(self) -> None:
        if self.content:
            self._complete = True
            self.parent.uploads.append(
                MockS3ServiceUpload(self.key, self.content, self.media_type)
            )

    def _upload_abort(self) -> None:
        ...


class MockS3Service(S3Service):
    def __init__(
        self,
        client: S3Client,
        region: str,
        bucket: str,
        url_template: str,
    ) -> None:
        super().__init__(client, region, bucket, url_template)
        self.uploads: list[MockS3ServiceUpload] = []
        self.mocked_multipart_upload: MockMultipartS3ContextManager | None = None

    def store_stream(
        self,
        key: str,
        stream: BinaryIO,
        content_type: str | None = None,
    ) -> str | None:
        self.uploads.append(MockS3ServiceUpload(key, stream.read(), content_type))
        return self.generate_url(key)

    def multipart(
        self, key: str, content_type: str | None = None
    ) -> MultipartS3ContextManager:
        self.mocked_multipart_upload = MockMultipartS3ContextManager(
            self, self.bucket, key, self.generate_url(key), content_type
        )
        return self.mocked_multipart_upload


class S3ServiceProtocol(Protocol):
    def __call__(
        self,
        client: S3Client | None = None,
        region: str | None = None,
        bucket: str | None = None,
        url_template: str | None = None,
    ) -> S3Service:
        ...


class S3ServiceFixture:
    def __init__(self):
        self.mock_s3_client = MagicMock()
        self.region = "region"
        self.url_template = "https://{region}.test.com/{bucket}/{key}"
        self.bucket = "bucket"

    @property
    def service(self) -> S3ServiceProtocol:
        return functools.partial(
            S3Service,
            client=self.mock_s3_client,
            region=self.region,
            bucket=self.bucket,
            url_template=self.url_template,
        )

    def mock_service(self) -> MockS3Service:
        return MockS3Service(
            client=self.mock_s3_client,
            region=self.region,
            bucket=self.bucket,
            url_template=self.url_template,
        )


@pytest.fixture
def s3_service_fixture() -> S3ServiceFixture:
    return S3ServiceFixture()

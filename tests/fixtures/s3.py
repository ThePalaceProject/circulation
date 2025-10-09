from __future__ import annotations

import functools
import uuid
from collections.abc import Generator
from dataclasses import dataclass, field
from typing import IO, TYPE_CHECKING, NamedTuple, Protocol, Self
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from pydantic_settings import SettingsConfigDict

from palace.manager.service.storage.container import Storage
from palace.manager.service.storage.s3 import (
    MultipartS3ContextManager,
    MultipartS3UploadPart,
    S3Service,
)
from palace.manager.util.pydantic import HttpUrl
from tests.fixtures.config import FixtureTestUrlConfiguration

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
    from mypy_boto3_s3.type_defs import FileobjTypeDef


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
            self.parent.uploads[self.key] = MockS3ServiceUpload(
                self.key, self.content, self.media_type
            )

    def _upload_abort(self) -> None: ...


class MockMultipartUploadPart:
    def __init__(
        self, part_data: MultipartS3UploadPart, content: bytes | IO[bytes]
    ) -> None:
        self.part_data = part_data
        if isinstance(content, bytes):
            self.content = content
        else:
            self.content = content.read()


@dataclass
class MockMultipartUpload:
    key: str
    upload_id: str
    parts: dict[int, MockMultipartUploadPart] = field(default_factory=dict)
    content_type: str | None = None


class MockS3Service(S3Service):
    def __init__(
        self,
        client: S3Client,
        region: str,
        bucket: str,
        url_template: str,
    ) -> None:
        super().__init__(client, region, bucket, url_template)
        self.uploads: dict[str, MockS3ServiceUpload] = {}
        self.mocked_multipart_upload: MockMultipartS3ContextManager | None = None

        self.upload_in_progress: dict[str, MockMultipartUpload] = {}
        self.aborted: list[str] = []
        self.deleted: list[str] = []

    def delete(self, key: str) -> None:
        self.deleted.append(key)

    def store_stream(
        self,
        key: str,
        stream: FileobjTypeDef,
        content_type: str | None = None,
    ) -> str | None:
        self.uploads[key] = MockS3ServiceUpload(key, stream.read(), content_type)
        return self.generate_url(key)

    def multipart(
        self, key: str, content_type: str | None = None
    ) -> MultipartS3ContextManager:
        self.mocked_multipart_upload = MockMultipartS3ContextManager(
            self, self.bucket, key, self.generate_url(key), content_type
        )
        return self.mocked_multipart_upload

    def multipart_create(self, key: str, content_type: str | None = None) -> str:
        upload_id = str(uuid4())
        self.upload_in_progress[key] = MockMultipartUpload(
            key, upload_id, content_type=content_type
        )
        return upload_id

    def multipart_upload(
        self, key: str, upload_id: str, part_number: int, content: bytes | IO[bytes]
    ) -> MultipartS3UploadPart:
        etag = str(uuid4())
        if not 1 <= part_number <= 10000:
            raise ValueError("Part number must be between 1 and 10000")

        part = MultipartS3UploadPart(etag=etag, part_number=part_number)
        assert key in self.upload_in_progress
        assert self.upload_in_progress[key].upload_id == upload_id
        self.upload_in_progress[key].parts[part_number] = MockMultipartUploadPart(
            part, content
        )
        return part

    def multipart_complete(
        self, key: str, upload_id: str, parts: list[MultipartS3UploadPart]
    ) -> None:
        assert key in self.upload_in_progress
        assert self.upload_in_progress[key].upload_id == upload_id
        complete_upload = self.upload_in_progress.pop(key)
        assert len(complete_upload.parts) == len(parts)
        expected_parts = [x.part_data for x in complete_upload.parts.values()]
        expected_parts.sort(key=lambda x: x.part_number)
        assert parts == expected_parts
        self.uploads[key] = MockS3ServiceUpload(
            key,
            b"".join(
                part_stored.content for part_stored in complete_upload.parts.values()
            ),
            complete_upload.content_type,
        )

    def multipart_abort(self, key: str, upload_id: str) -> None:
        assert key in self.upload_in_progress
        assert self.upload_in_progress[key].upload_id == upload_id
        self.upload_in_progress.pop(key)
        self.aborted.append(key)


class S3ServiceProtocol(Protocol):
    def __call__(
        self,
        client: S3Client | None = None,
        region: str | None = None,
        bucket: str | None = None,
        url_template: str | None = None,
    ) -> S3Service: ...


class S3ServiceFixture:
    def __init__(self):
        self.mock_s3_client = MagicMock()
        self.mock_s3_client.upload_part = MagicMock(return_value={"ETag": "xyz"})
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


class S3UploaderIntegrationConfiguration(FixtureTestUrlConfiguration):
    url: HttpUrl
    user: str
    password: str

    model_config = SettingsConfigDict(env_prefix="PALACE_TEST_MINIO_")


class S3ServiceIntegrationFixture:
    def __init__(self):
        self.container = Storage()
        self.configuration = S3UploaderIntegrationConfiguration.from_env()
        self.analytics_bucket = self.random_name("analytics")
        self.public_access_bucket = self.random_name("public")
        self.container.config.from_dict(
            {
                "access_key": self.configuration.user,
                "secret_key": self.configuration.password,
                "endpoint_url": self.configuration.url,
                "region": "us-east-1",
                "analytics_bucket": self.analytics_bucket,
                "public_access_bucket": self.public_access_bucket,
                "url_template": self.configuration.url + "/{bucket}/{key}",
            }
        )
        self.buckets = []
        self.create_buckets()

    @classmethod
    def random_name(cls, prefix: str = "test"):
        return f"{prefix}-{uuid.uuid4()}"

    @property
    def s3_client(self) -> S3Client:
        return self.container.s3_client()

    @property
    def public(self) -> S3Service:
        return self.container.public()

    @property
    def analytics(self) -> S3Service:
        return self.container.analytics()

    def create_bucket(self, bucket_name: str) -> None:
        client = self.s3_client
        client.create_bucket(Bucket=bucket_name)
        self.buckets.append(bucket_name)

    def get_bucket(self, bucket_name: str) -> str:
        if bucket_name == "public":
            return self.public_access_bucket
        elif bucket_name == "analytics":
            return self.analytics_bucket
        else:
            raise ValueError(f"Unknown bucket name: {bucket_name}")

    def create_buckets(self) -> None:
        for bucket in [self.analytics_bucket, self.public_access_bucket]:
            self.create_bucket(bucket)

    def list_objects(self, bucket_name: str) -> list[str]:
        bucket = self.get_bucket(bucket_name)
        response = self.s3_client.list_objects(Bucket=bucket)
        return [object["Key"] for object in response.get("Contents", [])]

    def get_object(self, bucket_name: str, key: str) -> bytes:
        bucket = self.get_bucket(bucket_name)
        response = self.s3_client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()

    def close(self):
        for bucket in self.buckets:
            response = self.s3_client.list_objects(Bucket=bucket)

            for object in response.get("Contents", []):
                object_key = object["Key"]
                self.s3_client.delete_object(Bucket=bucket, Key=object_key)

            self.s3_client.delete_bucket(Bucket=bucket)


@pytest.fixture
def s3_service_integration_fixture() -> Generator[S3ServiceIntegrationFixture]:
    fixture = S3ServiceIntegrationFixture()
    yield fixture
    fixture.close()

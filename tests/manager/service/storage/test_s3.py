from __future__ import annotations

import functools
from io import BytesIO
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import BotoCoreError, ClientError

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.service.storage.s3 import S3Service
from tests.fixtures.s3 import S3ServiceFixture, S3ServiceIntegrationFixture


class TestS3Service:
    def test_factory(self, s3_service_fixture: S3ServiceFixture):
        """The S3Service.factory method returns an S3Service, if given
        a bucket, or None otherwise.
        """
        # No bucket, no service.
        factory = functools.partial(
            S3Service.factory,
            client=s3_service_fixture.mock_s3_client,
            region=s3_service_fixture.region,
            url_template=s3_service_fixture.url_template,
        )
        assert factory(bucket=None) is None

        # Bucket, service.
        service = factory(bucket="bucket")
        assert isinstance(service, S3Service)
        assert service.client == s3_service_fixture.mock_s3_client
        assert service.region == s3_service_fixture.region
        assert service.bucket == "bucket"
        assert service.url_template == s3_service_fixture.url_template

    @pytest.mark.parametrize(
        "url_template",
        [
            # No region passed into the constructor, but the URL template
            # contains a region.
            "https://{bucket}.s3.{region}.amazonaws.com/{key}",
            # No key in the URL template.
            "https://no-key-in-template.com/",
        ],
    )
    def test_constructor_exception(
        self, url_template: str, s3_service_fixture: S3ServiceFixture
    ):
        """The S3Service constructor raises an exception if the URL template is invalid."""
        with pytest.raises(CannotLoadConfiguration):
            s3_service_fixture.service(url_template=url_template, region=None)

    @pytest.mark.parametrize(
        "template,key,expected",
        [
            (
                "https://{bucket}.s3.{region}.amazonaws.com/{key}",
                "key",
                "https://bucket.s3.region.amazonaws.com/key",
            ),
            (
                "https://test.com/{bucket}/{key}",
                "key with spaces",
                "https://test.com/bucket/key%20with%20spaces",
            ),
            (
                "https://test.com/{bucket}/{key}",
                "s p a c e s/🔥/slashes%",
                "https://test.com/bucket/s%20p%20a%20c%20e%20s/%F0%9F%94%A5/slashes%25",
            ),
            (
                "https://cdn.com/{key}",
                "filename.ext",
                "https://cdn.com/filename.ext",
            ),
        ],
    )
    def test_generate_url(
        self,
        template: str,
        key: str,
        expected: str,
        s3_service_fixture: S3ServiceFixture,
    ):
        """
        Generate URL creates a URL based on the URL template, it uses format to template in
        the region, bucket, and key, then makes sure the URL is urlencoded.
        """
        service = s3_service_fixture.service(url_template=template)
        url = service.generate_url(key)
        assert url == expected

    def test_delete(self, s3_service_fixture: S3ServiceFixture):
        """The S3Service.delete method deletes the object from the bucket."""
        service = s3_service_fixture.service()
        service.client.delete_object = MagicMock()
        service.delete("key")
        service.client.delete_object.assert_called_once_with(
            Bucket=s3_service_fixture.bucket, Key="key"
        )

    @pytest.mark.parametrize(
        "content",
        ["foo bar baz", b"byte string"],
    )
    def test_store(self, content: bytes | str, s3_service_fixture: S3ServiceFixture):
        service = s3_service_fixture.service()
        service.store_stream = MagicMock()

        if isinstance(content, str):
            expected_content = content.encode("utf8")
        else:
            expected_content = content

        service.store("key", content, "text/plain")
        service.store_stream.assert_called_once()
        assert service.store_stream.call_args.kwargs["key"] == "key"
        stream = service.store_stream.call_args.kwargs["stream"]
        assert isinstance(stream, BytesIO)
        assert stream.getvalue() == expected_content
        assert service.store_stream.call_args.kwargs["content_type"] == "text/plain"

    @pytest.mark.parametrize(
        "content_type",
        ["text/plain", "application/binary", None],
    )
    def test_store_stream(
        self, content_type: str, s3_service_fixture: S3ServiceFixture
    ):
        service = s3_service_fixture.service()
        stream = MagicMock(spec=BytesIO)

        if content_type:
            url = service.store_stream("key", stream, content_type)
        else:
            url = service.store_stream("key", stream)

        mock_s3_client = s3_service_fixture.mock_s3_client
        mock_s3_client.upload_fileobj.assert_called_once()
        assert mock_s3_client.upload_fileobj.call_args.kwargs["Fileobj"] == stream
        assert (
            mock_s3_client.upload_fileobj.call_args.kwargs["Bucket"]
            == s3_service_fixture.bucket
        )
        assert mock_s3_client.upload_fileobj.call_args.kwargs["Key"] == "key"
        assert url == "https://region.test.com/bucket/key"
        stream.close.assert_called_once()

        if content_type:
            assert mock_s3_client.upload_fileobj.call_args.kwargs["ExtraArgs"] == {
                "ContentType": content_type
            }
        else:
            assert mock_s3_client.upload_fileobj.call_args.kwargs["ExtraArgs"] == {}

    @pytest.mark.parametrize(
        "exception",
        [BotoCoreError(), ClientError({}, "")],
    )
    def test_store_stream_exception(
        self, exception: Exception, s3_service_fixture: S3ServiceFixture
    ):
        service = s3_service_fixture.service()
        stream = MagicMock(spec=BytesIO)

        mock_s3_client = s3_service_fixture.mock_s3_client
        mock_s3_client.upload_fileobj.side_effect = exception
        assert service.store_stream("key", stream) is None
        mock_s3_client.upload_fileobj.assert_called_once()
        stream.close.assert_called_once()

    def test_multipart_upload(self, s3_service_fixture: S3ServiceFixture):
        service = s3_service_fixture.service()

        with (ctx := service.multipart(key="key")) as upload:
            # You are not allowed to nest the multipart upload context manager.
            with pytest.raises(RuntimeError):
                with ctx:
                    pass

            # Successful upload
            assert upload._service == service
            assert upload.key == "key"
            assert upload.parts == []

            s3_service_fixture.mock_s3_client.create_multipart_upload.assert_called_once()
            assert upload.complete is False
            assert upload.url == "https://region.test.com/bucket/key"
            assert upload.exception is None

            upload.upload_part(b"Part 1")
            assert s3_service_fixture.mock_s3_client.upload_part.call_count == 1
            upload.upload_part(b"Part 2")
            assert s3_service_fixture.mock_s3_client.upload_part.call_count == 2

            assert len(upload.parts) == 2
            [part1, part2] = upload.parts
            assert part1.part_number == 1
            assert part2.part_number == 2

            s3_service_fixture.mock_s3_client.complete_multipart_upload.assert_not_called()

        assert upload.complete is True
        assert upload.exception is None
        s3_service_fixture.mock_s3_client.complete_multipart_upload.assert_called_once()

    def test_multipart_upload_exception(self, s3_service_fixture: S3ServiceFixture):
        service = s3_service_fixture.service()
        exception = BotoCoreError()
        s3_service_fixture.mock_s3_client.upload_part.side_effect = exception

        # A boto exception is raised during upload, but it is captured
        # and the upload is aborted.
        with service.multipart(key="key") as upload:
            assert upload.complete is False
            assert upload.url == "https://region.test.com/bucket/key"
            assert upload.exception is None
            upload.upload_part(b"test")

        assert upload.complete is False
        assert upload.exception is exception
        s3_service_fixture.mock_s3_client.abort_multipart_upload.assert_called_once()

        with pytest.raises(RuntimeError):
            upload.upload_part(b"foo")

    def _configuration(self, prefix, expiration):
        return {
            "Rules": [
                {
                    "Expiration": {
                        "Days": expiration,
                    },
                    "ID": f"expiration_on_{prefix}",
                    "Prefix": prefix,
                    "Filter": {"Prefix": prefix},
                    "Status": "Enabled",
                }
            ]
        }


@pytest.mark.minio
class TestS3ServiceIntegration:
    def test_delete(self, s3_service_integration_fixture: S3ServiceIntegrationFixture):
        """The S3Service.delete method deletes the object from the bucket."""
        service = s3_service_integration_fixture.public
        bucket = service.bucket

        raw_client = s3_service_integration_fixture.s3_client
        content = BytesIO()
        content.write(b"foo bar baz")
        raw_client.upload_fileobj(content, bucket, "key")

        bucket_contents = raw_client.list_objects(Bucket=bucket).get("Contents", [])
        assert len(bucket_contents) == 1
        assert bucket_contents[0]["Key"] == "key"

        service.delete("key")
        bucket_contents = raw_client.list_objects(Bucket=bucket).get("Contents", [])
        assert len(bucket_contents) == 0

        service.delete("key")  # Deleting a non-existent key should not raise an error.

    @pytest.mark.parametrize(
        "key, service_name, content, content_type",
        [
            ("key", "public", "foo bar baz", "text/plain"),
            ("key/w i t h/slash/.!%:", "public", b"byte string", None),
            ("key/with/🥏", "public", "🔥", None),
            ("ûberkey", "analytics", "foo bar", "application/pdf"),
            ("õ/🤖/analytics.foo", "analytics", b"another byte string", None),
            ("normal/key", "analytics", "🚀", None),
        ],
    )
    def test_store(
        self,
        key: str,
        service_name: str,
        content: bytes | str,
        content_type: str | None,
        s3_service_integration_fixture: S3ServiceIntegrationFixture,
    ):
        """The S3Service.store method stores content in the bucket."""
        service = getattr(s3_service_integration_fixture, service_name)
        bucket = s3_service_integration_fixture.get_bucket(service_name)
        service.store(key, content, content_type)
        response = s3_service_integration_fixture.s3_client.get_object(
            Bucket=bucket, Key=key
        )

        if isinstance(content, str):
            # The response we get back from S3 is always utf-8 encoded bytes.
            expected_content = content.encode("utf8")
        else:
            expected_content = content

        assert response["Body"].read() == expected_content

        if content_type is None:
            expected_content_type = "binary/octet-stream"
        else:
            expected_content_type = content_type
        assert response["ContentType"] == expected_content_type

    @pytest.mark.parametrize(
        "key, service_name, content, content_type",
        [
            ("key", "public", b"foo bar baz", "text/plain"),
            ("key/with/slash", "public", b"byte string", None),
            ("key/with/🥏", "public", "🔥".encode(), None),
            ("ûberkey", "analytics", b"foo bar", "application/pdf"),
            ("õ/🤖/analytics.foo", "analytics", b"another byte string", None),
            ("normal/key", "analytics", "🚀".encode(), None),
        ],
    )
    def test_multipart(
        self,
        key: str,
        service_name: str,
        content: bytes,
        content_type: str | None,
        s3_service_integration_fixture: S3ServiceIntegrationFixture,
    ):
        service = getattr(s3_service_integration_fixture, service_name)
        bucket = s3_service_integration_fixture.get_bucket(service_name)
        part_1_data = (
            b"a" * 5 * 1024**2
        )  # Minimum part size is 5MB, so we generate some junk data to send.
        part_2_data = b"b" * 5 * 1024**2
        with service.multipart(key=key, content_type=content_type) as upload:
            upload.upload_part(part_1_data)
            upload.upload_part(part_2_data)
            upload.upload_part(content)
            assert not upload.complete
            assert upload.exception is None

        assert upload.complete
        assert upload.exception is None

        response = s3_service_integration_fixture.s3_client.get_object(
            Bucket=bucket, Key=key
        )
        assert response["Body"].read() == part_1_data + part_2_data + content

        if content_type is None:
            expected_content_type = "binary/octet-stream"
        else:
            expected_content_type = content_type
        assert response["ContentType"] == expected_content_type

    def test_multipart_one_small_part(
        self,
        s3_service_integration_fixture: S3ServiceIntegrationFixture,
    ):
        # If we only have one part, we are allowed to upload less than 5MB.
        service = s3_service_integration_fixture.public
        with service.multipart(key="key") as upload:
            upload.upload_part(b"small data")
            assert not upload.complete
            assert upload.exception is None

        assert upload.complete
        assert upload.exception is None

        response = s3_service_integration_fixture.s3_client.get_object(
            Bucket=s3_service_integration_fixture.public_access_bucket, Key="key"
        )
        assert response["Body"].read() == b"small data"

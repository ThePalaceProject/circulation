import functools
from unittest.mock import MagicMock

import botocore
import pytest
from botocore.exceptions import BotoCoreError, ClientError

from palace.core.mirror import MirrorUploader
from palace.core.model import (
    DataSource,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    Representation,
    create,
)
from palace.core.s3 import (
    MinIOUploader,
    MinIOUploaderConfiguration,
    MockS3Client,
    MultipartS3Upload,
    S3AddressingStyle,
    S3Uploader,
    S3UploaderConfiguration,
)
from palace.core.util.datetime_helpers import datetime_utc, utc_now

# TODO: we can drop this when we drop support for Python 3.6 and 3.7
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.s3 import S3UploaderFixture, S3UploaderIntegrationFixture
from tests.fixtures.sample_covers import SampleCoversFixture


class TestS3Uploader:
    def test_names(self):
        # The NAME associated with this class must be the same as its
        # key in the MirrorUploader implementation registry, and it's
        # better if it's the same as the name of the external
        # integration.
        assert S3Uploader.NAME == ExternalIntegration.S3
        assert (
            S3Uploader == MirrorUploader.IMPLEMENTATION_REGISTRY[ExternalIntegration.S3]
        )

    def test_instantiation(self, s3_uploader_fixture: S3UploaderFixture):
        transaction = s3_uploader_fixture.transaction

        integration = transaction.external_integration(
            ExternalIntegration.S3, goal=ExternalIntegration.STORAGE_GOAL
        )
        integration.username = "your-access-key"
        integration.password = "your-secret-key"
        integration.setting(
            S3UploaderConfiguration.URL_TEMPLATE_KEY
        ).value = "a transform"
        uploader = MirrorUploader.implementation(integration)
        assert True == isinstance(uploader, S3Uploader)

        # The URL_TEMPLATE_KEY setting becomes the .url_transform
        # attribute on the S3Uploader object.
        assert "a transform" == uploader.url_transform

    @pytest.mark.parametrize(
        "name,username,password",
        [
            ("empty_credentials", None, None),
            ("empty_string_credentials", "", ""),
            ("non_empty_string_credentials", "username", "password"),
        ],
    )
    def test_initialization(
        self, s3_uploader_fixture: S3UploaderFixture, name, username, password
    ):
        # Arrange
        transaction = s3_uploader_fixture.transaction
        settings = {"username": username, "password": password}
        integration = transaction.external_integration(
            ExternalIntegration.S3,
            goal=ExternalIntegration.STORAGE_GOAL,
            settings=settings,
        )
        client_class = MagicMock()

        # Act
        S3Uploader(integration, client_class=client_class)

        # Assert
        assert client_class.call_count == 2

        service_name = client_class.call_args_list[0].args[0]
        region_name = client_class.call_args_list[0].kwargs["region_name"]
        aws_access_key_id = client_class.call_args_list[0].kwargs["aws_access_key_id"]
        aws_secret_access_key = client_class.call_args_list[0].kwargs[
            "aws_secret_access_key"
        ]
        config = client_class.call_args_list[0].kwargs["config"]
        assert service_name == "s3"
        assert region_name == S3UploaderConfiguration.S3_DEFAULT_REGION
        assert aws_access_key_id == None
        assert aws_secret_access_key == None
        assert config.signature_version == botocore.UNSIGNED
        assert (
            config.s3["addressing_style"]
            == S3UploaderConfiguration.S3_DEFAULT_ADDRESSING_STYLE
        )

        service_name = client_class.call_args_list[1].args[0]
        region_name = client_class.call_args_list[1].kwargs["region_name"]
        aws_access_key_id = client_class.call_args_list[1].kwargs["aws_access_key_id"]
        aws_secret_access_key = client_class.call_args_list[1].kwargs[
            "aws_secret_access_key"
        ]
        assert service_name == "s3"
        assert region_name == S3UploaderConfiguration.S3_DEFAULT_REGION
        assert aws_access_key_id == (username if username != "" else None)
        assert aws_secret_access_key == (password if password != "" else None)
        assert "config" not in client_class.call_args_list[1].kwargs

    def test_custom_client_class(self, s3_uploader_fixture: S3UploaderFixture):
        """You can specify a client class to use instead of boto3.client."""
        integration = s3_uploader_fixture.integration()
        uploader = S3Uploader(integration, MockS3Client)
        assert isinstance(uploader.client, MockS3Client)

    def test_get_bucket(self, s3_uploader_fixture: S3UploaderFixture):
        buckets = {
            S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY: "banana",
            S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY: "bucket",
        }
        buckets_plus_irrelevant_setting = dict(buckets)
        buckets_plus_irrelevant_setting["not-a-bucket-at-all"] = "value"
        uploader = s3_uploader_fixture.create_s3_uploader(
            **buckets_plus_irrelevant_setting
        )

        # This S3Uploader knows about the configured buckets.  It
        # wasn't informed of the irrelevant 'not-a-bucket-at-all'
        # setting.
        assert buckets == uploader.buckets

        # get_bucket just does a lookup in .buckets
        uploader.buckets["foo"] = object()
        result = uploader.get_bucket("foo")
        assert uploader.buckets["foo"] == result

    @pytest.mark.parametrize(
        "name,bucket,path,expected_result,region,addressing_style",
        [
            (
                "s3_url_with_path_without_slash",
                "a-bucket",
                "a-path",
                "https://a-bucket.s3.amazonaws.com/a-path",
                None,
                None,
            ),
            (
                "s3_dummy_url_with_path_without_slash",
                "dummy",
                "dummy",
                "https://dummy.s3.amazonaws.com/dummy",
                None,
                None,
            ),
            (
                "s3_path_style_url_with_path_without_slash",
                "a-bucket",
                "a-path",
                "https://s3.amazonaws.com/a-bucket/a-path",
                None,
                S3AddressingStyle.PATH.value,
            ),
            (
                "s3_path_style_dummy_url_with_path_without_slash",
                "dummy",
                "dummy",
                "https://s3.amazonaws.com/dummy/dummy",
                None,
                S3AddressingStyle.PATH.value,
            ),
            (
                "s3_url_with_path_with_slash",
                "a-bucket",
                "/a-path",
                "https://a-bucket.s3.amazonaws.com/a-path",
                None,
                None,
            ),
            (
                "s3_path_style_url_with_path_with_slash",
                "a-bucket",
                "/a-path",
                "https://s3.amazonaws.com/a-bucket/a-path",
                None,
                S3AddressingStyle.PATH.value,
            ),
            (
                "s3_url_with_custom_region_and_path_without_slash",
                "a-bucket",
                "a-path",
                "https://a-bucket.s3.us-east-2.amazonaws.com/a-path",
                "us-east-2",
                None,
            ),
            (
                "s3_path_style_url_with_custom_region_and_path_without_slash",
                "a-bucket",
                "a-path",
                "https://s3.us-east-2.amazonaws.com/a-bucket/a-path",
                "us-east-2",
                S3AddressingStyle.PATH.value,
            ),
            (
                "s3_url_with_custom_region_and_path_with_slash",
                "a-bucket",
                "/a-path",
                "https://a-bucket.s3.us-east-3.amazonaws.com/a-path",
                "us-east-3",
                None,
            ),
            (
                "s3_path_style_url_with_custom_region_and_path_with_slash",
                "a-bucket",
                "/a-path",
                "https://s3.us-east-3.amazonaws.com/a-bucket/a-path",
                "us-east-3",
                S3AddressingStyle.PATH.value,
            ),
            (
                "custom_http_url_and_path_without_slash",
                "http://a-bucket.com/",
                "a-path",
                "http://a-bucket.com/a-path",
                None,
                None,
            ),
            (
                "custom_http_url_and_path_with_slash",
                "http://a-bucket.com/",
                "/a-path",
                "http://a-bucket.com/a-path",
                None,
                None,
            ),
            (
                "custom_http_url_and_path_without_slash",
                "https://a-bucket.com/",
                "a-path",
                "https://a-bucket.com/a-path",
                None,
                None,
            ),
            (
                "custom_http_url_and_path_with_slash",
                "https://a-bucket.com/",
                "/a-path",
                "https://a-bucket.com/a-path",
                None,
                None,
            ),
        ],
    )
    def test_url(
        self,
        s3_uploader_fixture: S3UploaderFixture,
        name,
        bucket,
        path,
        expected_result,
        region,
        addressing_style,
    ):
        # Arrange
        uploader = s3_uploader_fixture.create_s3_uploader(
            region=region, addressing_style=addressing_style
        )

        # Act
        result = uploader.url(bucket, path)

        # Assert
        assert result == expected_result

    @pytest.mark.parametrize(
        "name,bucket,key,expected_result,url_transform,region",
        [
            (
                "implicit_s3_url_template",
                "bucket",
                "the key",
                "https://bucket.s3.amazonaws.com/the%20key",
                None,
                None,
            ),
            (
                "implicit_s3_url_template_with_custom_region",
                "bucket",
                "the key",
                "https://bucket.s3.us-east-2.amazonaws.com/the%20key",
                None,
                "us-east-2",
            ),
            (
                "explicit_s3_url_template",
                "bucket",
                "the key",
                "https://bucket.s3.amazonaws.com/the%20key",
                S3UploaderConfiguration.URL_TEMPLATE_DEFAULT,
                None,
            ),
            (
                "explicit_s3_url_template_with_custom_region",
                "bucket",
                "the key",
                "https://bucket.s3.us-east-2.amazonaws.com/the%20key",
                S3UploaderConfiguration.URL_TEMPLATE_DEFAULT,
                "us-east-2",
            ),
            (
                "http_url_template",
                "bucket",
                "the këy",
                "http://bucket/the%20k%C3%ABy",
                S3UploaderConfiguration.URL_TEMPLATE_HTTP,
                None,
            ),
            (
                "https_url_template",
                "bucket",
                "the këy",
                "https://bucket/the%20k%C3%ABy",
                S3UploaderConfiguration.URL_TEMPLATE_HTTPS,
                None,
            ),
        ],
    )
    def test_final_mirror_url(
        self,
        s3_uploader_fixture: S3UploaderFixture,
        name,
        bucket,
        key,
        expected_result,
        url_transform,
        region,
    ):
        # Arrange
        uploader = s3_uploader_fixture.create_s3_uploader(region=region)

        if url_transform:
            uploader.url_transform = url_transform

        # Act
        result = uploader.final_mirror_url(bucket, key)

        # Assert
        if not url_transform:
            assert (
                S3UploaderConfiguration.URL_TEMPLATE_DEFAULT == uploader.url_transform
            )

        assert result == expected_result

    def test_key_join(self):
        """Test the code used to build S3 keys from parts."""
        parts = ["Gutenberg", b"Gutenberg ID", 1234, "Die Flügelmaus+.epub"]
        assert (
            "Gutenberg/Gutenberg%20ID/1234/Die%20Fl%C3%BCgelmaus%2B.epub"
            == S3Uploader.key_join(parts)
        )

    @pytest.mark.parametrize(
        "name,bucket,data_source_name,expected_result,scaled_size,region,",
        [
            (
                "with_gutenberg_cover_generator_data_source",
                "test-book-covers-s3-bucket",
                DataSource.GUTENBERG_COVER_GENERATOR,
                "https://test-book-covers-s3-bucket.s3.amazonaws.com/Gutenberg%20Illustrated/",
                None,
                None,
            ),
            (
                "with_overdrive_data_source",
                "test-book-covers-s3-bucket",
                DataSource.OVERDRIVE,
                "https://test-book-covers-s3-bucket.s3.amazonaws.com/Overdrive/",
                None,
                None,
            ),
            (
                "with_overdrive_data_source_and_scaled_size",
                "test-book-covers-s3-bucket",
                DataSource.OVERDRIVE,
                "https://test-book-covers-s3-bucket.s3.amazonaws.com/scaled/300/Overdrive/",
                300,
                None,
            ),
            (
                "with_gutenberg_cover_generator_data_source_and_custom_region",
                "test-book-covers-s3-bucket",
                DataSource.GUTENBERG_COVER_GENERATOR,
                "https://test-book-covers-s3-bucket.s3.us-east-3.amazonaws.com/Gutenberg%20Illustrated/",
                None,
                "us-east-3",
            ),
            (
                "with_overdrive_data_source_and_custom_region",
                "test-book-covers-s3-bucket",
                DataSource.OVERDRIVE,
                "https://test-book-covers-s3-bucket.s3.us-east-3.amazonaws.com/Overdrive/",
                None,
                "us-east-3",
            ),
            (
                "with_overdrive_data_source_and_scaled_size_and_custom_region",
                "test-book-covers-s3-bucket",
                DataSource.OVERDRIVE,
                "https://test-book-covers-s3-bucket.s3.us-east-3.amazonaws.com/scaled/300/Overdrive/",
                300,
                "us-east-3",
            ),
        ],
    )
    def test_cover_image_root(
        self,
        s3_uploader_fixture: S3UploaderFixture,
        name,
        bucket,
        data_source_name,
        expected_result,
        scaled_size,
        region,
    ):
        # Arrange
        session = s3_uploader_fixture.transaction.session
        uploader = s3_uploader_fixture.create_s3_uploader(region=region)
        data_source = DataSource.lookup(session, data_source_name)

        # Act
        result = uploader.cover_image_root(bucket, data_source, scaled_size=scaled_size)

        # Assert
        assert result == expected_result

    @pytest.mark.parametrize(
        "name,bucket,expected_result,region",
        [
            (
                "with_default_region",
                "test-open-access-s3-bucket",
                "https://test-open-access-s3-bucket.s3.amazonaws.com/",
                None,
            ),
            (
                "with_custom_region",
                "test-open-access-s3-bucket",
                "https://test-open-access-s3-bucket.s3.us-east-3.amazonaws.com/",
                "us-east-3",
            ),
        ],
    )
    def test_content_root(
        self,
        s3_uploader_fixture: S3UploaderFixture,
        name,
        bucket,
        expected_result,
        region,
    ):
        # Arrange
        uploader = s3_uploader_fixture.create_s3_uploader(region=region)

        # Act
        result = uploader.content_root(bucket)

        # Assert
        assert result == expected_result

    @pytest.mark.parametrize(
        "name,bucket,library_name,expected_result,region",
        [
            (
                "s3_url",
                "test-marc-s3-bucket",
                "SHORT",
                "https://test-marc-s3-bucket.s3.amazonaws.com/SHORT/",
                None,
            ),
            (
                "s3_url_with_custom_region",
                "test-marc-s3-bucket",
                "SHORT",
                "https://test-marc-s3-bucket.s3.us-east-2.amazonaws.com/SHORT/",
                "us-east-2",
            ),
            (
                "custom_http_url",
                "http://my-feed/",
                "SHORT",
                "http://my-feed/SHORT/",
                None,
            ),
            (
                "custom_https_url",
                "https://my-feed/",
                "SHORT",
                "https://my-feed/SHORT/",
                None,
            ),
        ],
    )
    def test_marc_file_root(
        self,
        s3_uploader_fixture: S3UploaderFixture,
        name,
        bucket,
        library_name,
        expected_result,
        region,
    ):
        # Arrange
        uploader = s3_uploader_fixture.create_s3_uploader(region=region)
        library = s3_uploader_fixture.transaction.library(short_name=library_name)

        # Act
        result = uploader.marc_file_root(bucket, library)

        # Assert
        assert result == expected_result

    @pytest.mark.parametrize(
        "name,buckets,identifier,expected_result,extension,data_source_name,title,region,open_access",
        [
            (
                "with_identifier",
                {S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY: "thebooks"},
                "ABOOK",
                "https://thebooks.s3.amazonaws.com/Gutenberg%20ID/ABOOK.epub",
                None,
                None,
                None,
                None,
                True,
            ),
            (
                "with_custom_extension",
                {S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY: "thebooks"},
                "ABOOK",
                "https://thebooks.s3.amazonaws.com/Gutenberg%20ID/ABOOK.pdf",
                "pdf",
                None,
                None,
                None,
                True,
            ),
            (
                "with_custom_dotted_extension",
                {S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY: "thebooks"},
                "ABOOK",
                "https://thebooks.s3.amazonaws.com/Gutenberg%20ID/ABOOK.pdf",
                ".pdf",
                None,
                None,
                None,
                True,
            ),
            (
                "with_custom_data_source",
                {S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY: "thebooks"},
                "ABOOK",
                "https://thebooks.s3.amazonaws.com/unglue.it/Gutenberg%20ID/ABOOK.epub",
                None,
                DataSource.UNGLUE_IT,
                None,
                None,
                True,
            ),
            (
                "with_custom_title",
                {S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY: "thebooks"},
                "ABOOK",
                "https://thebooks.s3.amazonaws.com/Gutenberg%20ID/ABOOK/On%20Books.epub",
                None,
                None,
                "On Books",
                None,
                True,
            ),
            (
                "with_custom_extension_and_title_and_data_source",
                {S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY: "thebooks"},
                "ABOOK",
                "https://thebooks.s3.amazonaws.com/unglue.it/Gutenberg%20ID/ABOOK/On%20Books.pdf",
                ".pdf",
                DataSource.UNGLUE_IT,
                "On Books",
                None,
                True,
            ),
            (
                "with_custom_extension_and_title_and_data_source_and_region",
                {S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY: "thebooks"},
                "ABOOK",
                "https://thebooks.s3.us-east-3.amazonaws.com/unglue.it/Gutenberg%20ID/ABOOK/On%20Books.pdf",
                ".pdf",
                DataSource.UNGLUE_IT,
                "On Books",
                "us-east-3",
                True,
            ),
            (
                "with_protected_access_and_custom_extension_and_title_and_data_source_and_region",
                {S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY: "thebooks"},
                "ABOOK",
                "https://thebooks.s3.us-east-3.amazonaws.com/unglue.it/Gutenberg%20ID/ABOOK/On%20Books.pdf",
                ".pdf",
                DataSource.UNGLUE_IT,
                "On Books",
                "us-east-3",
                False,
            ),
        ],
    )
    def test_book_url(
        self,
        s3_uploader_fixture: S3UploaderFixture,
        name,
        buckets,
        identifier,
        expected_result,
        extension,
        data_source_name,
        title,
        region,
        open_access,
    ):
        # Arrange
        transaction = s3_uploader_fixture.transaction
        identifier = transaction.identifier(foreign_id=identifier)
        uploader = s3_uploader_fixture.create_s3_uploader(region=region, **buckets)

        parameters = {"identifier": identifier, "open_access": open_access}

        if extension:
            parameters["extension"] = extension
        if title:
            parameters["title"] = title

        if data_source_name:
            data_source = DataSource.lookup(transaction.session, DataSource.UNGLUE_IT)
            parameters["data_source"] = data_source

        # Act
        result = uploader.book_url(**parameters)

        # Assert
        assert result == expected_result

    @pytest.mark.parametrize(
        "name,buckets,data_source_name,identifier,filename,expected_result,scaled_size,region",
        [
            (
                "without_scaled_size",
                {S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY: "thecovers"},
                DataSource.UNGLUE_IT,
                "ABOOK",
                "filename",
                "https://thecovers.s3.amazonaws.com/unglue.it/Gutenberg%20ID/ABOOK/filename",
                None,
                None,
            ),
            (
                "without_scaled_size_and_with_custom_region",
                {S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY: "thecovers"},
                DataSource.UNGLUE_IT,
                "ABOOK",
                "filename",
                "https://thecovers.s3.us-east-3.amazonaws.com/unglue.it/Gutenberg%20ID/ABOOK/filename",
                None,
                "us-east-3",
            ),
            (
                "with_scaled_size",
                {S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY: "thecovers"},
                DataSource.UNGLUE_IT,
                "ABOOK",
                "filename",
                "https://thecovers.s3.amazonaws.com/scaled/601/unglue.it/Gutenberg%20ID/ABOOK/filename",
                601,
                None,
            ),
            (
                "with_scaled_size_and_custom_region",
                {S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY: "thecovers"},
                DataSource.UNGLUE_IT,
                "ABOOK",
                "filename",
                "https://thecovers.s3.us-east-3.amazonaws.com/scaled/601/unglue.it/Gutenberg%20ID/ABOOK/filename",
                601,
                "us-east-3",
            ),
        ],
    )
    def test_cover_image_url(
        self,
        s3_uploader_fixture: S3UploaderFixture,
        name,
        buckets,
        data_source_name,
        identifier,
        filename,
        expected_result,
        scaled_size,
        region,
    ):
        transaction = s3_uploader_fixture.transaction

        # identifier = self._identifier(foreign_id="ABOOK")
        # buckets = {S3Uploader.BOOK_COVERS_BUCKET_KEY : 'thecovers'}
        # uploader = self._uploader(**buckets)
        # m = uploader.cover_image_url
        #
        # unglueit = DataSource.lookup(self._db, DataSource.UNGLUE_IT)
        # identifier = self._identifier(foreign_id="ABOOK")
        # eq_('https://s3.amazonaws.com/thecovers/scaled/601/unglue.it/Gutenberg+ID/ABOOK/filename',
        #     m(unglueit, identifier, "filename", scaled_size=601))

        # Arrange
        data_source = DataSource.lookup(transaction.session, data_source_name)
        identifier = transaction.identifier(foreign_id=identifier)
        uploader = s3_uploader_fixture.create_s3_uploader(region=region, **buckets)

        # Act
        result = uploader.cover_image_url(
            data_source, identifier, filename, scaled_size=scaled_size
        )

        # Assert
        assert result == expected_result

    @pytest.mark.parametrize(
        "name,bucket,library_name,lane_name,end_time,expected_result,start_time,region",
        [
            (
                "with_s3_bucket_and_end_time",
                "marc",
                "SHORT",
                "Lane",
                datetime_utc(2020, 1, 1, 0, 0, 0),
                "https://marc.s3.amazonaws.com/SHORT/2020-01-01%2000%3A00%3A00%2B00%3A00/Lane.mrc",
                None,
                None,
            ),
            (
                "with_s3_bucket_and_end_time_and_start_time",
                "marc",
                "SHORT",
                "Lane",
                datetime_utc(2020, 1, 2, 0, 0, 0),
                "https://marc.s3.amazonaws.com/SHORT/2020-01-01%2000%3A00%3A00%2B00%3A00-2020-01-02%2000%3A00%3A00%2B00%3A00/Lane.mrc",
                datetime_utc(2020, 1, 1, 0, 0, 0),
                None,
            ),
            (
                "with_s3_bucket_and_end_time_and_start_time_and_custom_region",
                "marc",
                "SHORT",
                "Lane",
                datetime_utc(2020, 1, 2, 0, 0, 0),
                "https://marc.s3.us-east-2.amazonaws.com/SHORT/2020-01-01%2000%3A00%3A00%2B00%3A00-2020-01-02%2000%3A00%3A00%2B00%3A00/Lane.mrc",
                datetime_utc(2020, 1, 1, 0, 0, 0),
                "us-east-2",
            ),
            (
                "with_http_bucket_and_end_time_and_start_time",
                "http://marc",
                "SHORT",
                "Lane",
                datetime_utc(2020, 1, 2, 0, 0, 0),
                "http://marc/SHORT/2020-01-01%2000%3A00%3A00%2B00%3A00-2020-01-02%2000%3A00%3A00%2B00%3A00/Lane.mrc",
                datetime_utc(2020, 1, 1, 0, 0, 0),
                None,
            ),
            (
                "with_https_bucket_and_end_time_and_start_time",
                "https://marc",
                "SHORT",
                "Lane",
                datetime_utc(2020, 1, 2, 0, 0, 0),
                "https://marc/SHORT/2020-01-01%2000%3A00%3A00%2B00%3A00-2020-01-02%2000%3A00%3A00%2B00%3A00/Lane.mrc",
                datetime_utc(2020, 1, 1, 0, 0, 0),
                None,
            ),
        ],
    )
    def test_marc_file_url(
        self,
        s3_uploader_fixture: S3UploaderFixture,
        name,
        bucket,
        library_name,
        lane_name,
        end_time,
        expected_result,
        start_time,
        region,
    ):
        # Arrange
        transaction = s3_uploader_fixture.transaction
        library = transaction.library(short_name=library_name)
        lane = transaction.lane(display_name=lane_name)
        buckets = {S3UploaderConfiguration.MARC_BUCKET_KEY: bucket}
        uploader = s3_uploader_fixture.create_s3_uploader(region=region, **buckets)

        # Act
        result = uploader.marc_file_url(library, lane, end_time, start_time)

        # Assert
        assert result == expected_result

    @pytest.mark.parametrize(
        "name,url,expected_result,unquote",
        [
            (
                "s3_path_style_request_without_region",
                "https://s3.amazonaws.com/bucket/directory/filename.jpg",
                ("bucket", "directory/filename.jpg"),
                True,
            ),
            (
                "s3_path_style_request_with_region",
                "https://s3.us-east-2.amazonaws.com/bucket/directory/filename.jpg",
                ("bucket", "directory/filename.jpg"),
                True,
            ),
            (
                "s3_virtual_hosted_style_request_with_global_endpoint",
                "https://bucket.s3.amazonaws.com/directory/filename.jpg",
                ("bucket", "directory/filename.jpg"),
                True,
            ),
            (
                "s3_virtual_hosted_style_request_with_dashed_region",
                "https://bucket.s3-us-east-2.amazonaws.com/directory/filename.jpg",
                ("bucket", "directory/filename.jpg"),
                True,
            ),
            (
                "s3_virtual_hosted_style_request_with_dotted_region",
                "https://bucket.s3.us-east-2.amazonaws.com/directory/filename.jpg",
                ("bucket", "directory/filename.jpg"),
                True,
            ),
            (
                "http_url",
                "http://book-covers.nypl.org/directory/filename.jpg",
                ("book-covers.nypl.org", "directory/filename.jpg"),
                True,
            ),
            (
                "https_url",
                "https://book-covers.nypl.org/directory/filename.jpg",
                ("book-covers.nypl.org", "directory/filename.jpg"),
                True,
            ),
            (
                "http_url_with_escaped_symbols",
                "http://book-covers.nypl.org/directory/filename+with+spaces%21.jpg",
                ("book-covers.nypl.org", "directory/filename with spaces!.jpg"),
                True,
            ),
            (
                "http_url_with_escaped_symbols_but_unquote_set_to_false",
                "http://book-covers.nypl.org/directory/filename+with+spaces%21.jpg",
                ("book-covers.nypl.org", "directory/filename+with+spaces%21.jpg"),
                False,
            ),
        ],
    )
    def test_split_url(
        self,
        s3_uploader_fixture: S3UploaderFixture,
        name,
        url,
        expected_result,
        unquote,
    ):
        # Arrange
        s3_uploader = s3_uploader_fixture.create_s3_uploader()

        # Act
        result = s3_uploader.split_url(url, unquote)

        # Assert
        assert result == expected_result

    def test_mirror_one(
        self,
        s3_uploader_fixture: S3UploaderFixture,
        sample_covers_fixture: SampleCoversFixture,
    ):
        transaction = s3_uploader_fixture.transaction

        edition, pool = transaction.edition(with_license_pool=True)
        original_cover_location = "http://example.com/a-cover.png"
        content = open(
            sample_covers_fixture.sample_cover_path("test-book-cover.png"), "rb"
        ).read()
        cover, ignore = pool.add_link(
            Hyperlink.IMAGE,
            original_cover_location,
            edition.data_source,
            Representation.PNG_MEDIA_TYPE,
            content=content,
        )
        cover_rep = cover.resource.representation
        assert None == cover_rep.mirrored_at

        original_epub_location = "https://books.com/a-book.epub"
        epub, ignore = pool.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD,
            original_epub_location,
            edition.data_source,
            Representation.EPUB_MEDIA_TYPE,
            content="i'm an epub",
        )
        epub_rep = epub.resource.representation
        assert None == epub_rep.mirrored_at

        s3 = s3_uploader_fixture.create_s3_uploader(client_class=MockS3Client)

        # Mock final_mirror_url so we can verify that it's called with
        # the right arguments
        def mock_final_mirror_url(bucket, key):
            return "final_mirror_url was called with bucket {}, key {}".format(
                bucket, key
            )

        s3.final_mirror_url = mock_final_mirror_url

        book_url = "http://books-go/here.epub"
        cover_url = "http://s3.amazonaws.com/covers-go/here.png"
        s3.mirror_one(cover.resource.representation, cover_url)
        s3.mirror_one(epub.resource.representation, book_url)
        [
            [data1, bucket1, key1, args1, ignore1],
            [data2, bucket2, key2, args2, ignore2],
        ] = s3.client.uploads

        # Both representations have had .mirror_url set and been
        # mirrored to those URLs.
        assert data1.startswith(b"\x89")
        assert "covers-go" == bucket1
        assert "here.png" == key1
        assert Representation.PNG_MEDIA_TYPE == args1["ContentType"]
        assert (utc_now() - cover_rep.mirrored_at).seconds < 10

        assert b"i'm an epub" == data2
        assert "books-go" == bucket2
        assert "here.epub" == key2
        assert Representation.EPUB_MEDIA_TYPE == args2["ContentType"]

        # In both cases, mirror_url was set to the result of final_mirror_url.
        assert (
            "final_mirror_url was called with bucket books-go, key here.epub"
            == epub_rep.mirror_url
        )
        assert (
            "final_mirror_url was called with bucket covers-go, key here.png"
            == cover_rep.mirror_url
        )

        # mirrored-at was set when the representation was 'mirrored'
        for rep in epub_rep, cover_rep:
            assert (utc_now() - rep.mirrored_at).seconds < 10

    def test_mirror_failure(self, s3_uploader_fixture: S3UploaderFixture):
        transaction = s3_uploader_fixture.transaction

        edition, pool = transaction.edition(with_license_pool=True)
        original_epub_location = "https://books.com/a-book.epub"
        epub, ignore = pool.add_link(
            Hyperlink.OPEN_ACCESS_DOWNLOAD,
            original_epub_location,
            edition.data_source,
            Representation.EPUB_MEDIA_TYPE,
            content="i'm an epub",
        )
        epub_rep = epub.resource.representation

        uploader = s3_uploader_fixture.create_s3_uploader(MockS3Client)

        # A network failure is treated as a transient error.
        uploader.client.fail_with = BotoCoreError()
        uploader.mirror_one(epub_rep, transaction.fresh_url())
        assert None == epub_rep.mirrored_at
        assert None == epub_rep.mirror_exception

        # An S3 credential failure is treated as a transient error.
        response = dict(
            Error=dict(
                Code=401,
                Message="Bad credentials",
            )
        )
        uploader.client.fail_with = ClientError(response, "SomeOperation")
        uploader.mirror_one(epub_rep, transaction.fresh_url())
        assert None == epub_rep.mirrored_at
        assert None == epub_rep.mirror_exception

        # Because the file was not successfully uploaded,
        # final_mirror_url was never called and mirror_url is
        # was not set.
        assert None == epub_rep.mirror_url

        # A bug in the code is not treated as a transient error --
        # the exception propagates through.
        uploader.client.fail_with = Exception("crash!")
        pytest.raises(Exception, uploader.mirror_one, epub_rep, transaction.fresh_url())

    def test_svg_mirroring(self, s3_uploader_fixture: S3UploaderFixture):
        transaction = s3_uploader_fixture.transaction

        edition, pool = transaction.edition(with_license_pool=True)
        original = transaction.fresh_url()

        # Create an SVG cover for the book.
        svg = """<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN"
  "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">

<svg xmlns="http://www.w3.org/2000/svg" width="100" height="50">
    <ellipse cx="50" cy="25" rx="50" ry="25" style="fill:blue;"/>
</svg>"""
        hyperlink, ignore = pool.add_link(
            Hyperlink.IMAGE,
            original,
            edition.data_source,
            Representation.SVG_MEDIA_TYPE,
            content=svg,
        )

        # 'Upload' it to S3.
        s3 = s3_uploader_fixture.create_s3_uploader(MockS3Client)
        s3.mirror_one(hyperlink.resource.representation, transaction.fresh_url())
        [[data, bucket, key, args, ignore]] = s3.client.uploads

        assert Representation.SVG_MEDIA_TYPE == args["ContentType"]
        assert b"svg" in data
        assert b"PNG" not in data

    def test_multipart_upload(self, s3_uploader_fixture: S3UploaderFixture):
        transaction = s3_uploader_fixture.transaction

        class MockMultipartS3Upload(MultipartS3Upload):
            completed = None
            aborted = None

            def __init__(self, uploader, representation, mirror_to):
                self.parts = []
                MockMultipartS3Upload.completed = False
                MockMultipartS3Upload.aborted = False

            def upload_part(self, content):
                self.parts.append(content)

            def complete(self):
                MockMultipartS3Upload.completed = True

            def abort(self):
                MockMultipartS3Upload.aborted = True

        rep, ignore = create(
            transaction.session,
            Representation,
            url="http://books.mrc",
            media_type=Representation.MARC_MEDIA_TYPE,
        )

        s3 = s3_uploader_fixture.create_s3_uploader(MockS3Client)

        # Successful upload
        with s3.multipart_upload(
            rep, rep.url, upload_class=MockMultipartS3Upload
        ) as upload:
            assert [] == upload.parts
            assert False == upload.completed
            assert False == upload.aborted

            upload.upload_part("Part 1")
            upload.upload_part("Part 2")

            assert ["Part 1", "Part 2"] == upload.parts

        assert True == MockMultipartS3Upload.completed
        assert False == MockMultipartS3Upload.aborted
        assert None == rep.mirror_exception

        class FailingMultipartS3Upload(MockMultipartS3Upload):
            def upload_part(self, content):
                raise Exception("Error!")

        # Failed during upload
        with s3.multipart_upload(
            rep, rep.url, upload_class=FailingMultipartS3Upload
        ) as upload:
            upload.upload_part("Part 1")

        assert False == MockMultipartS3Upload.completed
        assert True == MockMultipartS3Upload.aborted
        assert "Error!" == rep.mirror_exception

        class AnotherFailingMultipartS3Upload(MockMultipartS3Upload):
            def complete(self):
                raise Exception("Error!")

        rep.mirror_exception = None
        # Failed during completion
        with s3.multipart_upload(
            rep, rep.url, upload_class=AnotherFailingMultipartS3Upload
        ) as upload:
            upload.upload_part("Part 1")

        assert False == MockMultipartS3Upload.completed
        assert True == MockMultipartS3Upload.aborted
        assert "Error!" == rep.mirror_exception

    @pytest.mark.parametrize(
        "name,expiration_settings,expected_expiration",
        [
            (
                "default_expiration_parameter",
                None,
                int(S3UploaderConfiguration.S3_DEFAULT_PRESIGNED_URL_EXPIRATION),
            ),
            (
                "empty_expiration_parameter",
                {S3UploaderConfiguration.S3_PRESIGNED_URL_EXPIRATION: 100},
                100,
            ),
        ],
    )
    def test_sign_url(
        self,
        s3_uploader_fixture: S3UploaderFixture,
        name,
        expiration_settings,
        expected_expiration,
    ):
        # Arrange
        region = "us-east-1"
        bucket = "bucket"
        filename = "filename"
        url = f"https://{bucket}.s3.{region}.amazonaws.com/{filename}"
        expected_url = url + "?AWSAccessKeyId=KEY&Expires=1&Signature=S"
        settings = expiration_settings if expiration_settings else {}
        s3_uploader = s3_uploader_fixture.create_s3_uploader(region=region, **settings)
        s3_uploader.split_url = MagicMock(return_value=(bucket, filename))
        s3_uploader.client.generate_presigned_url = MagicMock(return_value=expected_url)

        # Act
        result = s3_uploader.sign_url(url)

        # Assert
        assert result == expected_url
        s3_uploader.split_url.assert_called_once_with(url)
        s3_uploader.client.generate_presigned_url.assert_called_once_with(
            "get_object",
            ExpiresIn=expected_expiration,
            Params={"Bucket": bucket, "Key": filename},
        )


class TestMultiPartS3Upload:
    @staticmethod
    def _representation(transaction: DatabaseTransactionFixture):
        rep, ignore = create(
            transaction.session,
            Representation,
            url="http://bucket/books.mrc",
            media_type=Representation.MARC_MEDIA_TYPE,
        )
        return rep

    def test_init(self, s3_uploader_fixture: S3UploaderFixture):
        uploader = s3_uploader_fixture.create_s3_uploader(MockS3Client)
        rep = self._representation(s3_uploader_fixture.transaction)
        upload = MultipartS3Upload(uploader, rep, rep.url)
        assert uploader == upload.uploader
        assert rep == upload.representation
        assert "bucket" == upload.bucket
        assert "books.mrc" == upload.filename
        assert 1 == upload.part_number
        assert [] == upload.parts
        assert 1 == upload.upload.get("UploadId")

        uploader.client.fail_with = Exception("Error!")
        pytest.raises(Exception, MultipartS3Upload, uploader, rep, rep.url)

    def test_upload_part(self, s3_uploader_fixture: S3UploaderFixture):
        uploader = s3_uploader_fixture.create_s3_uploader(MockS3Client)
        rep = self._representation(s3_uploader_fixture.transaction)
        upload = MultipartS3Upload(uploader, rep, rep.url)
        upload.upload_part("Part 1")
        upload.upload_part("Part 2")
        assert [
            {
                "Body": "Part 1",
                "UploadId": 1,
                "PartNumber": 1,
                "Bucket": "bucket",
                "Key": "books.mrc",
            },
            {
                "Body": "Part 2",
                "UploadId": 1,
                "PartNumber": 2,
                "Bucket": "bucket",
                "Key": "books.mrc",
            },
        ] == uploader.client.parts
        assert 3 == upload.part_number
        assert [
            {"ETag": "etag", "PartNumber": 1},
            {"ETag": "etag", "PartNumber": 2},
        ] == upload.parts

        uploader.client.fail_with = Exception("Error!")
        pytest.raises(Exception, upload.upload_part, "Part 3")

    def test_complete(self, s3_uploader_fixture: S3UploaderFixture):
        uploader = s3_uploader_fixture.create_s3_uploader(MockS3Client)
        rep = self._representation(s3_uploader_fixture.transaction)
        upload = MultipartS3Upload(uploader, rep, rep.url)
        upload.upload_part("Part 1")
        upload.upload_part("Part 2")
        upload.complete()
        assert [
            {
                "Bucket": "bucket",
                "Key": "books.mrc",
                "UploadId": 1,
                "MultipartUpload": {
                    "Parts": [
                        {"ETag": "etag", "PartNumber": 1},
                        {"ETag": "etag", "PartNumber": 2},
                    ],
                },
            }
        ] == uploader.client.uploads

    def test_abort(self, s3_uploader_fixture: S3UploaderFixture):
        uploader = s3_uploader_fixture.create_s3_uploader(MockS3Client)
        rep = self._representation(s3_uploader_fixture.transaction)
        upload = MultipartS3Upload(uploader, rep, rep.url)
        upload.upload_part("Part 1")
        upload.upload_part("Part 2")
        upload.abort()
        assert [] == uploader.client.parts


@pytest.mark.minio
class TestS3UploaderIntegration:
    @pytest.mark.parametrize(
        "name,uploader_class,bucket_type,bucket_name,open_access,settings",
        [
            (
                "using_s3_uploader_and_open_access_bucket",
                functools.partial(
                    S3Uploader,
                    host=S3UploaderIntegrationFixture.SIMPLIFIED_TEST_MINIO_HOST,
                ),
                S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY,
                "test-bucket",
                True,
                None,
            ),
            (
                "using_s3_uploader_and_protected_access_bucket",
                functools.partial(
                    S3Uploader,
                    host=S3UploaderIntegrationFixture.SIMPLIFIED_TEST_MINIO_HOST,
                ),
                S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY,
                "test-bucket",
                False,
                None,
            ),
            (
                "using_minio_uploader_and_open_access_bucket",
                MinIOUploader,
                S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY,
                "test-bucket",
                True,
                {
                    MinIOUploaderConfiguration.ENDPOINT_URL: S3UploaderIntegrationFixture.SIMPLIFIED_TEST_MINIO_ENDPOINT_URL
                },
            ),
            (
                "using_minio_uploader_and_protected_access_bucket",
                MinIOUploader,
                S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY,
                "test-bucket",
                False,
                {
                    MinIOUploaderConfiguration.ENDPOINT_URL: S3UploaderIntegrationFixture.SIMPLIFIED_TEST_MINIO_ENDPOINT_URL
                },
            ),
        ],
    )
    def test_mirror(
        self,
        s3_uploader_integration_fixture: S3UploaderIntegrationFixture,
        name,
        uploader_class,
        bucket_type,
        bucket_name,
        open_access,
        settings,
    ):
        fixture = s3_uploader_integration_fixture

        # Arrange
        book_title = "1234567890"
        book_content = "1234567890"
        identifier = Identifier(type=Identifier.ISBN, identifier=book_title)
        representation = Representation(
            content=book_content, media_type=Representation.EPUB_MEDIA_TYPE
        )
        buckets = {
            bucket_type: bucket_name,
        }

        if settings:
            settings.update(buckets)
        else:
            settings = buckets

        s3_uploader = fixture.create_s3_uploader(
            uploader_class=uploader_class, **settings
        )

        fixture.minio_s3_client.create_bucket(Bucket=bucket_name)

        # Act
        book_url = s3_uploader.book_url(identifier, open_access=open_access)
        s3_uploader.mirror_one(representation, book_url)

        # Assert
        response = fixture.minio_s3_client.list_objects(Bucket=bucket_name)
        assert "Contents" in response
        assert len(response["Contents"]) == 1

        [object] = response["Contents"]

        assert object["Key"] == f"ISBN/{book_title}.epub"

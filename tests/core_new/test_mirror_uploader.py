import pytest

from core.config import CannotLoadConfiguration
from core.mirror import MirrorUploader
from core.model import ExternalIntegration
from core.model.configuration import ExternalIntegrationLink
from core.s3 import (
    MinIOUploader,
    MinIOUploaderConfiguration,
    S3Uploader,
    S3UploaderConfiguration,
)
from core.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class DummySuccessUploader(MirrorUploader):
    def __init__(self, integration=None):
        pass

    def book_url(
        self,
        identifier,
        extension=".epub",
        open_access=True,
        data_source=None,
        title=None,
    ):
        pass

    def cover_image_url(self, data_source, identifier, filename=None, scaled_size=None):
        pass

    def sign_url(self, url, expiration=None):
        pass

    def split_url(self, url, unquote=True):
        pass

    def do_upload(self, representation):
        return None


class DummyFailureUploader(MirrorUploader):
    def __init__(self, integration=None):
        pass

    def book_url(
        self,
        identifier,
        extension=".epub",
        open_access=True,
        data_source=None,
        title=None,
    ):
        pass

    def cover_image_url(self, data_source, identifier, filename=None, scaled_size=None):
        pass

    def sign_url(self, url, expiration=None):
        pass

    def split_url(self, url, unquote=True):
        pass

    def do_upload(self, representation):
        return "I always fail."


class TestInitialization:
    """Test the ability to get a MirrorUploader for various aspects of site
    configuration.
    """

    @staticmethod
    def _integration(data: DatabaseTransactionFixture) -> ExternalIntegration:
        """Helper method to make a storage ExternalIntegration."""
        storage_name = "some storage"
        integration = data.external_integration("my protocol")
        integration.goal = ExternalIntegration.STORAGE_GOAL
        integration.name = storage_name
        return integration

    @pytest.mark.parametrize(
        "name,protocol,uploader_class,settings",
        [
            ("s3_uploader", ExternalIntegration.S3, S3Uploader, None),
            (
                "minio_uploader",
                ExternalIntegration.MINIO,
                MinIOUploader,
                {MinIOUploaderConfiguration.ENDPOINT_URL: "http://localhost"},
            ),
        ],
    )
    def test_mirror(
        self,
        database_transaction: DatabaseTransactionFixture,
        name,
        protocol,
        uploader_class,
        settings,
    ):
        data, session = database_transaction, database_transaction.session()

        storage_name = "some storage"
        # If there's no integration with goal=STORAGE or name=storage_name,
        # MirrorUploader.mirror raises an exception.
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            MirrorUploader.mirror(session, storage_name)
        assert "No storage integration with name 'some storage' is configured" in str(
            excinfo.value
        )

        # If there's only one, mirror() uses it to initialize a
        # MirrorUploader.
        integration = self._integration(data)
        integration.protocol = protocol

        if settings:
            for key, value in settings.items():
                integration.setting(key).value = value

        uploader = MirrorUploader.mirror(session, integration=integration)

        assert isinstance(uploader, uploader_class)

    def test_integration_by_name(
        self, database_transaction: DatabaseTransactionFixture
    ):
        data, session = database_transaction, database_transaction.session()
        integration = self._integration(data)

        # No name was passed so nothing is found
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            MirrorUploader.integration_by_name(session)
        assert "No storage integration with name 'None' is configured" in str(
            excinfo.value
        )

        # Correct name was passed
        integration = MirrorUploader.integration_by_name(session, integration.name)
        assert isinstance(integration, ExternalIntegration)

    def test_for_collection(self, database_transaction: DatabaseTransactionFixture):
        data, session = database_transaction, database_transaction.session()

        # This collection has no mirror_integration, so
        # there is no MirrorUploader for it.
        collection = data.collection()
        assert None == MirrorUploader.for_collection(
            collection, ExternalIntegrationLink.COVERS
        )

        # This collection has a properly configured mirror_integration,
        # so it can have an MirrorUploader.
        integration = data.external_integration(
            ExternalIntegration.S3,
            ExternalIntegration.STORAGE_GOAL,
            username="username",
            password="password",
            settings={S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY: "some-covers"},
        )
        integration_link = data.external_integration_link(
            integration=collection._external_integration,
            other_integration=integration,
            purpose=ExternalIntegrationLink.COVERS,
        )

        uploader = MirrorUploader.for_collection(
            collection, ExternalIntegrationLink.COVERS
        )
        assert isinstance(uploader, MirrorUploader)

    @pytest.mark.parametrize(
        "name,protocol,uploader_class,settings",
        [
            ("s3_uploader", ExternalIntegration.S3, S3Uploader, None),
            (
                "minio_uploader",
                ExternalIntegration.MINIO,
                MinIOUploader,
                {MinIOUploaderConfiguration.ENDPOINT_URL: "http://localhost"},
            ),
        ],
    )
    def test_constructor(
        self,
        database_transaction: DatabaseTransactionFixture,
        name,
        protocol,
        uploader_class,
        settings,
    ):
        # You can't create a MirrorUploader with an integration
        # that's not designed for storage.
        integration = self._integration(database_transaction)
        integration.goal = ExternalIntegration.LICENSE_GOAL
        integration.protocol = protocol

        if settings:
            for key, value in settings.items():
                integration.setting(key).value = value
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            uploader_class(integration)
        assert "from an integration with goal=licenses" in str(excinfo.value)

    def test_implementation_registry(
        self, database_transaction: DatabaseTransactionFixture
    ):
        data, session = database_transaction, database_transaction.session()

        # The implementation class used for a given ExternalIntegration
        # is controlled by the integration's protocol and the contents
        # of the MirrorUploader's implementation registry.
        MirrorUploader.IMPLEMENTATION_REGISTRY["my protocol"] = DummyFailureUploader

        integration = self._integration(data)
        uploader = MirrorUploader.mirror(session, integration=integration)
        assert isinstance(uploader, DummyFailureUploader)
        del MirrorUploader.IMPLEMENTATION_REGISTRY["my protocol"]


class TestMirrorUploader:
    """Test the basic workflow of MirrorUploader."""

    def test_mirror_batch(self, database_transaction: DatabaseTransactionFixture):
        data, session = database_transaction, database_transaction.session()

        r1, ignore = data.representation()
        r2, ignore = data.representation()
        uploader = DummySuccessUploader()
        uploader.mirror_batch([r1, r2])
        assert r1.mirrored_at != None
        assert r2.mirrored_at != None

    def test_success_and_then_failure(
        self, database_transaction: DatabaseTransactionFixture
    ):
        data, session = database_transaction, database_transaction.session()

        r, ignore = data.representation()
        now = utc_now()
        DummySuccessUploader().mirror_one(r, "")
        assert r.mirrored_at > now
        assert None == r.mirror_exception

        # Even if the original upload succeeds, a subsequent upload
        # may fail in a way that leaves the image in an inconsistent
        # state.
        DummyFailureUploader().mirror_one(r, "")
        assert None == r.mirrored_at
        assert "I always fail." == r.mirror_exception

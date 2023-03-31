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
        db,
        name,
        protocol,
        uploader_class,
        settings,
    ):
        storage_name = "some storage"
        # If there's no integration with goal=STORAGE or name=storage_name,
        # MirrorUploader.mirror raises an exception.
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            MirrorUploader.mirror(db.session, storage_name)
        assert "No storage integration with name 'some storage' is configured" in str(
            excinfo.value
        )

        # If there's only one, mirror() uses it to initialize a
        # MirrorUploader.
        integration = self._integration(db)
        integration.protocol = protocol

        if settings:
            for key, value in settings.items():
                integration.setting(key).value = value

        uploader = MirrorUploader.mirror(db.session, integration=integration)

        assert isinstance(uploader, uploader_class)

    def test_integration_by_name(self, db: DatabaseTransactionFixture):
        integration = self._integration(db)

        # No name was passed so nothing is found
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            MirrorUploader.integration_by_name(db.session)
        assert "No storage integration with name 'None' is configured" in str(
            excinfo.value
        )

        # Correct name was passed
        integration = MirrorUploader.integration_by_name(db.session, integration.name)
        assert isinstance(integration, ExternalIntegration)

    def test_for_collection(self, db: DatabaseTransactionFixture):

        # This collection has no mirror_integration, so
        # there is no MirrorUploader for it.
        collection = db.collection()
        assert None == MirrorUploader.for_collection(
            collection, ExternalIntegrationLink.COVERS
        )

        # This collection has a properly configured mirror_integration,
        # so it can have an MirrorUploader.
        integration = db.external_integration(
            ExternalIntegration.S3,
            ExternalIntegration.STORAGE_GOAL,
            username="username",
            password="password",
            settings={S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY: "some-covers"},
        )
        integration_link = db.external_integration_link(
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
        db,
        name,
        protocol,
        uploader_class,
        settings,
    ):
        # You can't create a MirrorUploader with an integration
        # that's not designed for storage.
        integration = self._integration(db)
        integration.goal = ExternalIntegration.LICENSE_GOAL
        integration.protocol = protocol

        if settings:
            for key, value in settings.items():
                integration.setting(key).value = value
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            uploader_class(integration)
        assert "from an integration with goal=licenses" in str(excinfo.value)

    def test_implementation_registry(self, db: DatabaseTransactionFixture):
        session = db.session

        # The implementation class used for a given ExternalIntegration
        # is controlled by the integration's protocol and the contents
        # of the MirrorUploader's implementation registry.
        MirrorUploader.IMPLEMENTATION_REGISTRY["my protocol"] = DummyFailureUploader

        integration = self._integration(db)
        uploader = MirrorUploader.mirror(session, integration=integration)
        assert isinstance(uploader, DummyFailureUploader)
        del MirrorUploader.IMPLEMENTATION_REGISTRY["my protocol"]


class TestMirrorUploader:
    """Test the basic workflow of MirrorUploader."""

    def test_mirror_batch(self, db: DatabaseTransactionFixture):
        r1, ignore = db.representation()
        r2, ignore = db.representation()
        uploader = DummySuccessUploader()
        uploader.mirror_batch([r1, r2])
        assert r1.mirrored_at != None
        assert r2.mirrored_at != None

    def test_success_and_then_failure(self, db: DatabaseTransactionFixture):
        r, ignore = db.representation()
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

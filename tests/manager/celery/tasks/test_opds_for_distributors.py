from unittest.mock import call, patch

import pytest
from freezegun import freeze_time

from palace.manager.api.model.token import OAuthTokenResponse
from palace.manager.celery.tasks import identifiers, opds_for_distributors
from palace.manager.integration.license.opds.for_distributors.api import (
    OPDSForDistributorsAPI,
)
from palace.manager.integration.license.opds.for_distributors.settings import (
    OPDSForDistributorsSettings,
)
from palace.manager.integration.license.opds.for_distributors.utils import (
    STREAMING_MEDIA_LINK_TYPE,
)
from palace.manager.integration.license.opds.opds2.api import OPDS2API
from palace.manager.opds.authentication import Authentication, AuthenticationDocument
from palace.manager.opds.opds2 import Link
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
    LicensePoolType,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.celery import ApplyTaskFixture, CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OPDSForDistributorsFilesFixture
from tests.fixtures.http import MockHttpClientFixture
from tests.fixtures.redis import RedisFixture


class OPDSForDistributorsImportFixture:
    def __init__(
        self,
        db: DatabaseTransactionFixture,
        http_client: MockHttpClientFixture,
        apply_fixture: ApplyTaskFixture,
        files: OPDSForDistributorsFilesFixture,
    ):
        self.db = db
        self.collection = self.db.collection(
            protocol=OPDSForDistributorsAPI,
            settings=OPDSForDistributorsSettings(
                username="a",
                password="b",
                data_source="data_source",
                external_account_id="http://opds",
            ),
        )
        self.client = http_client
        self.apply = apply_fixture
        self.files = files

    def run_import_task(self, collection: Collection | None = None) -> None:
        collection = collection if collection is not None else self.collection
        opds_for_distributors.import_collection.delay(collection.id).wait()
        self.apply.process_apply_queue()

    def authentication_document(self) -> str:
        """Return the authentication document URL for the collection."""
        return AuthenticationDocument(
            id="http://test-authentication-document",
            title="Test Authentication Document",
            authentication=[
                Authentication(
                    type="http://opds-spec.org/auth/oauth/client_credentials",
                    links=[Link(href=self.db.fresh_url(), rel="authenticate")],
                )
            ],
        ).model_dump_json()

    def token(self) -> str:
        return OAuthTokenResponse(
            access_token="token",
            token_type="Bearer",
            expires_in=3600,
        ).model_dump_json()

    def queue_up_auth_responses(self) -> None:
        """Queue up the authentication document and token responses."""
        self.client.queue_response(
            200,
            content=self.files.sample_data("biblioboard_mini_feed.opds"),
        )
        self.client.queue_response(
            200,
            content=self.authentication_document(),
            headers={"Content-Type": AuthenticationDocument.content_type()},
        )
        self.client.queue_response(
            200,
            content=self.token(),
            headers={"Content-Type": "application/json"},
        )


@pytest.fixture
def opds_for_distributors_import_fixture(
    db: DatabaseTransactionFixture,
    http_client: MockHttpClientFixture,
    apply_task_fixture: ApplyTaskFixture,
    opds_dist_files_fixture: OPDSForDistributorsFilesFixture,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
) -> OPDSForDistributorsImportFixture:
    return OPDSForDistributorsImportFixture(
        db, http_client, apply_task_fixture, opds_dist_files_fixture
    )


class TestImportCollection:
    @staticmethod
    def get_link_url(pool: LicensePool, rel: str, media_type: str) -> str | None:
        links = [
            l
            for l in pool.identifier.links
            if l.rel == rel and l.resource.representation.media_type == media_type
        ]

        if len(links) != 1:
            return None

        return links[0].resource.representation.url

    @staticmethod
    def sorted_mechanisms(pool: LicensePool) -> list[LicensePoolDeliveryMechanism]:
        return sorted(
            pool.delivery_mechanisms,
            key=lambda x: x.delivery_mechanism.content_type or "",
        )

    @freeze_time()
    def test_import(
        self, opds_for_distributors_import_fixture: OPDSForDistributorsImportFixture
    ):
        opds_for_distributors_import_fixture.queue_up_auth_responses()
        opds_for_distributors_import_fixture.client.queue_response(
            200,
            content=(
                opds_for_distributors_import_fixture.files.sample_data(
                    "biblioboard_mini_feed.opds"
                )
            ),
        )
        opds_for_distributors_import_fixture.run_import_task()

        imported_works = opds_for_distributors_import_fixture.apply.get_works()

        # This importer works the same as the base OPDSImporter, except that
        # it adds delivery mechanisms for books with epub acquisition links
        # and sets pools' licenses_owned and licenses_available.

        # All four works in the feed were created, since we can use their acquisition links
        # to give copies to patrons.
        [camelot, camelot_audio, shogun, southern] = sorted(
            imported_works, key=lambda x: x.title
        )

        # Each work has a license pool.
        [camelot_pool] = camelot.license_pools
        [southern_pool] = southern.license_pools
        [camelot_audio_pool] = camelot_audio.license_pools
        [shogun_pool] = shogun.license_pools
        now = utc_now()

        # -- Test the Camelot license pool --
        assert camelot_pool.open_access is False
        assert camelot_pool.type == LicensePoolType.UNLIMITED
        assert camelot_pool.work.last_update_time == now
        assert camelot_pool.should_track_playtime is False

        # Test its delivery mechanisms
        [streaming_mechanism, epub_mechanism] = self.sorted_mechanisms(camelot_pool)

        # The epub with bearer token DRM
        assert epub_mechanism.rights_status.uri == RightsStatus.IN_COPYRIGHT
        assert (
            epub_mechanism.delivery_mechanism.drm_scheme
            == DeliveryMechanism.BEARER_TOKEN
        )
        assert (
            epub_mechanism.delivery_mechanism.content_type
            == Representation.EPUB_MEDIA_TYPE
        )

        assert (
            epub_mechanism.resource.url
            == "https://library.biblioboard.com/ext/api/media/04377e87-ab69-41c8-a2a4-812d55dc0952/assets/content.epub"
        )
        assert (
            self.get_link_url(
                camelot_pool,
                Hyperlink.GENERIC_OPDS_ACQUISITION,
                Representation.EPUB_MEDIA_TYPE,
            )
            == "https://library.biblioboard.com/ext/api/media/04377e87-ab69-41c8-a2a4-812d55dc0952/assets/content.epub"
        )

        # The streaming mechanism with streaming DRM
        assert streaming_mechanism.rights_status.uri == RightsStatus.IN_COPYRIGHT
        assert (
            streaming_mechanism.delivery_mechanism.drm_scheme
            == DeliveryMechanism.STREAMING_DRM
        )
        assert (
            streaming_mechanism.delivery_mechanism.content_type
            == DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE
        )

        assert (
            streaming_mechanism.resource.url
            == "https://library.biblioboard.com/viewer/04377e87-ab69-41c8-a2a4-812d55dc0952"
        )
        assert (
            self.get_link_url(
                camelot_pool,
                Hyperlink.GENERIC_OPDS_ACQUISITION,
                STREAMING_MEDIA_LINK_TYPE,
            )
            == "https://library.biblioboard.com/viewer/04377e87-ab69-41c8-a2a4-812d55dc0952"
        )

        # -- Test the southern license pool --
        assert southern_pool.open_access is False
        assert southern_pool.type == LicensePoolType.UNLIMITED
        assert southern_pool.work.last_update_time == now
        assert southern_pool.should_track_playtime is False

        # Test its delivery mechanisms
        [streaming_mechanism, epub_mechanism] = self.sorted_mechanisms(southern_pool)

        # The epub with bearer token DRM
        assert epub_mechanism.rights_status.uri == RightsStatus.IN_COPYRIGHT
        assert (
            epub_mechanism.delivery_mechanism.drm_scheme
            == DeliveryMechanism.BEARER_TOKEN
        )
        assert (
            epub_mechanism.delivery_mechanism.content_type
            == Representation.EPUB_MEDIA_TYPE
        )

        assert (
            epub_mechanism.resource.url
            == "https://library.biblioboard.com/ext/api/media/04da95cd-6cfc-4e82-810f-121d418b6963/assets/content.epub"
        )
        assert (
            self.get_link_url(
                southern_pool,
                Hyperlink.GENERIC_OPDS_ACQUISITION,
                Representation.EPUB_MEDIA_TYPE,
            )
            == "https://library.biblioboard.com/ext/api/media/04da95cd-6cfc-4e82-810f-121d418b6963/assets/content.epub"
        )

        # The streaming mechanism with streaming DRM
        assert streaming_mechanism.rights_status.uri == RightsStatus.IN_COPYRIGHT
        assert (
            streaming_mechanism.delivery_mechanism.drm_scheme
            == DeliveryMechanism.STREAMING_DRM
        )
        assert (
            streaming_mechanism.delivery_mechanism.content_type
            == DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE
        )

        assert (
            streaming_mechanism.resource.url
            == "https://library.biblioboard.com/viewer/04da95cd-6cfc-4e82-810f-121d418b6963"
        )
        assert (
            self.get_link_url(
                southern_pool,
                Hyperlink.GENERIC_OPDS_ACQUISITION,
                STREAMING_MEDIA_LINK_TYPE,
            )
            == "https://library.biblioboard.com/viewer/04da95cd-6cfc-4e82-810f-121d418b6963"
        )

        # -- Test the camelot audio license pool --
        assert camelot_audio_pool.open_access is False
        assert camelot_audio_pool.type == LicensePoolType.UNLIMITED
        assert camelot_audio_pool.work.last_update_time == now
        assert camelot_audio_pool.should_track_playtime is False

        # Test its delivery mechanisms
        [streaming_mechanism, audio_mechanism] = self.sorted_mechanisms(
            camelot_audio_pool
        )

        # The audiobook manifest with bearer token DRM
        assert audio_mechanism.rights_status.uri == RightsStatus.IN_COPYRIGHT
        assert (
            audio_mechanism.delivery_mechanism.drm_scheme
            == DeliveryMechanism.BEARER_TOKEN
        )
        assert (
            audio_mechanism.delivery_mechanism.content_type
            == Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
        )

        assert (
            audio_mechanism.resource.url
            == "https://library.biblioboard.com/ext/api/media/04377e87-ab69-41c8-a2a4-812d55dc0953/assets/content.json"
        )
        assert (
            self.get_link_url(
                camelot_audio_pool,
                Hyperlink.GENERIC_OPDS_ACQUISITION,
                Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
            )
            == "https://library.biblioboard.com/ext/api/media/04377e87-ab69-41c8-a2a4-812d55dc0953/assets/content.json"
        )

        # The streaming mechanism with streaming DRM
        assert streaming_mechanism.rights_status.uri == RightsStatus.IN_COPYRIGHT
        assert (
            streaming_mechanism.delivery_mechanism.drm_scheme
            == DeliveryMechanism.STREAMING_DRM
        )
        assert (
            streaming_mechanism.delivery_mechanism.content_type
            == DeliveryMechanism.STREAMING_AUDIO_CONTENT_TYPE
        )

        assert (
            streaming_mechanism.resource.url
            == "https://library.biblioboard.com/viewer/04377e87-ab69-41c8-a2a4-812d55dc0953"
        )
        assert (
            self.get_link_url(
                camelot_audio_pool,
                Hyperlink.GENERIC_OPDS_ACQUISITION,
                STREAMING_MEDIA_LINK_TYPE,
            )
            == "https://library.biblioboard.com/viewer/04377e87-ab69-41c8-a2a4-812d55dc0953"
        )

        # -- Test the shogun audio license pool --
        assert shogun_pool.open_access is False
        assert shogun_pool.type == LicensePoolType.UNLIMITED
        assert shogun_pool.work.last_update_time == now
        assert shogun_pool.should_track_playtime is True

        # Test its delivery mechanisms
        [audio_mechanism] = self.sorted_mechanisms(shogun_pool)

        # The audiobook manifest with bearer token DRM
        assert audio_mechanism.rights_status.uri == RightsStatus.IN_COPYRIGHT
        assert (
            audio_mechanism.delivery_mechanism.drm_scheme
            == DeliveryMechanism.BEARER_TOKEN
        )
        assert (
            audio_mechanism.delivery_mechanism.content_type
            == Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
        )

        assert (
            audio_mechanism.resource.url
            == "https://catalog.biblioboard.com/opds/items/12905232-0b38-4c3f-a1f3-1a3a34db0011/manifest.json"
        )
        assert (
            self.get_link_url(
                shogun_pool,
                Hyperlink.GENERIC_OPDS_ACQUISITION,
                Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
            )
            == "https://catalog.biblioboard.com/opds/items/12905232-0b38-4c3f-a1f3-1a3a34db0011/manifest.json"
        )

        # This pool has no streaming mechanism


class TestImportAll:
    @pytest.mark.parametrize(
        "force",
        [
            pytest.param(True, id="Force import"),
            pytest.param(False, id="Do not force import"),
        ],
    )
    def test_import_all(
        self, db: DatabaseTransactionFixture, celery_fixture: CeleryFixture, force: bool
    ) -> None:
        collection1 = db.collection(protocol=OPDSForDistributorsAPI)
        collection2 = db.collection(protocol=OPDSForDistributorsAPI)
        decoy_collection = db.collection(protocol=OPDS2API)

        with patch.object(
            opds_for_distributors, "import_collection"
        ) as mock_import_collection:
            opds_for_distributors.import_all.delay(force=force).wait()

        # We queued up tasks for all OPDS for Distributors collections.
        mock_import_collection.s.assert_called_once_with(
            force=force,
        )
        mock_import_collection.s.return_value.delay.assert_has_calls(
            [
                call(collection_id=collection1.id),
                call(collection_id=collection2.id),
            ],
            any_order=True,
        )


class TestReapAll:
    @pytest.mark.parametrize(
        "force",
        [
            pytest.param(True, id="Force import"),
            pytest.param(False, id="Do not force import"),
        ],
    )
    def test_reap_all(
        self, db: DatabaseTransactionFixture, celery_fixture: CeleryFixture, force: bool
    ) -> None:
        collection1 = db.collection(protocol=OPDSForDistributorsAPI)
        collection2 = db.collection(protocol=OPDSForDistributorsAPI)
        decoy_collection = db.collection(protocol=OPDS2API)

        with patch.object(
            opds_for_distributors, "import_and_reap_not_found_chord"
        ) as mock_reap_chord:
            opds_for_distributors.reap_all.delay(force=force).wait()

        mock_reap_chord.assert_has_calls(
            [
                call(collection1.id, force),
                call(collection2.id, force),
            ],
            any_order=True,
        )


class TestImportAndReapNotFoundChord:
    @pytest.mark.parametrize(
        "force",
        [
            pytest.param(True, id="Force reaping"),
            pytest.param(False, id="Do not force reaping"),
        ],
    )
    def test_import_and_reap_not_found_chord(self, force: bool) -> None:
        """Test the import and reap not found chord."""
        # Reap the collection
        collection_id = 12  # Example collection ID
        with (
            patch.object(identifiers, "create_mark_unavailable_chord") as mock_chord,
            patch.object(opds_for_distributors, "import_collection") as mock_import,
        ):
            opds_for_distributors.import_and_reap_not_found_chord(
                collection_id=collection_id, force=force
            )

        mock_import.s.assert_called_once_with(
            collection_id=collection_id, force=force, return_identifiers=True
        )
        mock_chord.assert_called_once_with(collection_id, mock_import.s.return_value)

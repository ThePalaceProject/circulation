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
from palace.manager.integration.license.opds.opds2.api import OPDS2API
from palace.manager.opds.authentication import Authentication, AuthenticationDocument
from palace.manager.opds.opds2 import Link
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
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

        for pool in [camelot_pool, southern_pool, camelot_audio_pool, shogun_pool]:
            assert pool.open_access is False
            assert (
                pool.delivery_mechanisms[0].rights_status.uri
                == RightsStatus.IN_COPYRIGHT
            )
            assert (
                pool.delivery_mechanisms[0].delivery_mechanism.drm_scheme
                == DeliveryMechanism.BEARER_TOKEN
            )
            assert pool.licenses_owned == LicensePool.UNLIMITED_ACCESS
            assert pool.licenses_available == LicensePool.UNLIMITED_ACCESS
            assert pool.type == LicensePoolType.UNLIMITED
            assert pool.work.last_update_time == now

        # The ebooks have the correct delivery mechanism, and they don't track playtime
        for pool in [camelot_pool, southern_pool]:
            assert (
                pool.delivery_mechanisms[0].delivery_mechanism.content_type
                == Representation.EPUB_MEDIA_TYPE
            )
            assert pool.should_track_playtime is False

        # The audiobooks have the correct delivery mechanism
        for pool in [camelot_audio_pool, shogun_pool]:
            assert (
                pool.delivery_mechanisms[0].delivery_mechanism.content_type
                == Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
            )

        # The camelot audiobook does not track playtime
        assert camelot_audio_pool.should_track_playtime is False

        # The shogun audiobook does track playtime
        assert shogun_pool.should_track_playtime is True

        [camelot_audio_acquisition_link] = [
            l
            for l in camelot_audio_pool.identifier.links
            if l.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
            and l.resource.representation.media_type
            == Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
        ]
        assert (
            camelot_audio_acquisition_link.resource.representation.url
            == "https://library.biblioboard.com/ext/api/media/04377e87-ab69-41c8-a2a4-812d55dc0953/assets/content.json"
        )

        [shogun_acquisition_link] = [
            l
            for l in shogun_pool.identifier.links
            if l.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
            and l.resource.representation.media_type
            == Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
        ]
        assert (
            shogun_acquisition_link.resource.representation.url
            == "https://catalog.biblioboard.com/opds/items/12905232-0b38-4c3f-a1f3-1a3a34db0011/manifest.json"
        )

        [camelot_acquisition_link] = [
            l
            for l in camelot_pool.identifier.links
            if l.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
            and l.resource.representation.media_type == Representation.EPUB_MEDIA_TYPE
        ]
        camelot_acquisition_url = camelot_acquisition_link.resource.representation.url
        assert (
            camelot_acquisition_url
            == "https://library.biblioboard.com/ext/api/media/04377e87-ab69-41c8-a2a4-812d55dc0952/assets/content.epub"
        )

        [southern_acquisition_link] = [
            l
            for l in southern_pool.identifier.links
            if l.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
            and l.resource.representation.media_type == Representation.EPUB_MEDIA_TYPE
        ]
        southern_acquisition_url = southern_acquisition_link.resource.representation.url
        assert (
            southern_acquisition_url
            == "https://library.biblioboard.com/ext/api/media/04da95cd-6cfc-4e82-810f-121d418b6963/assets/content.epub"
        )


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

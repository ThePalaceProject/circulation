from unittest.mock import patch

from freezegun import freeze_time

from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.link import LinkData
from palace.manager.integration.license.opds.base import importer
from palace.manager.integration.license.opds.for_distributors.api import (
    OPDSForDistributorsAPI,
)
from palace.manager.integration.license.opds.for_distributors.importer import (
    OPDSForDistributorsImporter,
)
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.datetime_helpers import utc_now
from tests.manager.integration.license.opds.for_distributors.conftest import (
    OPDSForDistributorsAPIFixture,
)


class TestOPDSForDistributorsImporter:
    @freeze_time()
    def test_import(self, opds_dist_api_fixture: OPDSForDistributorsAPIFixture):
        feed = opds_dist_api_fixture.files.sample_data("biblioboard_mini_feed.opds")

        collection = opds_dist_api_fixture.mock_collection()

        importer = OPDSForDistributorsImporter(
            opds_dist_api_fixture.db.session,
            collection=collection,
        )

        (
            imported_editions,
            imported_pools,
            imported_works,
            failures,
        ) = importer.import_from_feed(feed)

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
            assert False == pool.open_access
            assert (
                RightsStatus.IN_COPYRIGHT
                == pool.delivery_mechanisms[0].rights_status.uri
            )
            assert (
                DeliveryMechanism.BEARER_TOKEN
                == pool.delivery_mechanisms[0].delivery_mechanism.drm_scheme
            )
            assert LicensePool.UNLIMITED_ACCESS == pool.licenses_owned
            assert LicensePool.UNLIMITED_ACCESS == pool.licenses_available
            assert pool.work.last_update_time == now

        # The ebooks have the correct delivery mechanism and they don't track playtime
        for pool in [camelot_pool, southern_pool]:
            assert (
                Representation.EPUB_MEDIA_TYPE
                == pool.delivery_mechanisms[0].delivery_mechanism.content_type
            )
            assert pool.should_track_playtime == False

        # The audiobooks have the correct delivery mechanism
        for pool in [camelot_audio_pool, shogun_pool]:
            assert (
                Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
                == pool.delivery_mechanisms[0].delivery_mechanism.content_type
            )

        # The camelot audiobook does not track playtime
        assert camelot_audio_pool.should_track_playtime == False

        # The shogun audiobook does track playtime
        assert shogun_pool.should_track_playtime == True

        [camelot_audio_acquisition_link] = [
            l
            for l in camelot_audio_pool.identifier.links
            if l.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
            and l.resource.representation.media_type
            == Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
        ]
        assert (
            "https://library.biblioboard.com/ext/api/media/04377e87-ab69-41c8-a2a4-812d55dc0953/assets/content.json"
            == camelot_audio_acquisition_link.resource.representation.url
        )

        [shogun_acquisition_link] = [
            l
            for l in shogun_pool.identifier.links
            if l.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
            and l.resource.representation.media_type
            == Representation.AUDIOBOOK_MANIFEST_MEDIA_TYPE
        ]
        assert (
            "https://catalog.biblioboard.com/opds/items/12905232-0b38-4c3f-a1f3-1a3a34db0011/manifest.json"
            == shogun_acquisition_link.resource.representation.url
        )

        [camelot_acquisition_link] = [
            l
            for l in camelot_pool.identifier.links
            if l.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
            and l.resource.representation.media_type == Representation.EPUB_MEDIA_TYPE
        ]
        camelot_acquisition_url = camelot_acquisition_link.resource.representation.url
        assert (
            "https://library.biblioboard.com/ext/api/media/04377e87-ab69-41c8-a2a4-812d55dc0952/assets/content.epub"
            == camelot_acquisition_url
        )

        [southern_acquisition_link] = [
            l
            for l in southern_pool.identifier.links
            if l.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
            and l.resource.representation.media_type == Representation.EPUB_MEDIA_TYPE
        ]
        southern_acquisition_url = southern_acquisition_link.resource.representation.url
        assert (
            "https://library.biblioboard.com/ext/api/media/04da95cd-6cfc-4e82-810f-121d418b6963/assets/content.epub"
            == southern_acquisition_url
        )

    def test__add_format_data(
        self, opds_dist_api_fixture: OPDSForDistributorsAPIFixture
    ):
        # Mock SUPPORTED_MEDIA_TYPES for purposes of test.
        api = OPDSForDistributorsAPI
        old_value = api.SUPPORTED_MEDIA_TYPES
        good_media_type = "media/type"
        api.SUPPORTED_MEDIA_TYPES = [good_media_type]

        # Create a CirculationData object with a number of links.
        # Only the third of these links will become a FormatData
        # object.
        circulation = CirculationData(
            data_source_name="data source",
            primary_identifier_data=IdentifierData(
                type="ISBN", identifier="1234567890"
            ),
        )
        good_rel = Hyperlink.GENERIC_OPDS_ACQUISITION
        circulation.links = [
            LinkData(
                rel="http://wrong/rel/", media_type=good_media_type, href="http://url1/"
            ),
            LinkData(rel=good_rel, media_type="wrong/media type", href="http://url2/"),
            LinkData(rel=good_rel, media_type=good_media_type, href="http://url3/"),
        ]

        assert [] == circulation.formats
        OPDSForDistributorsImporter._add_format_data(circulation)

        # Only one FormatData was created.
        [format] = circulation.formats

        # It's the third link we created -- the one where both rel and
        # media_type were good.
        assert "http://url3/" == format.link.href
        assert good_rel == format.link.rel

        # The FormatData has the content type provided by the LinkData,
        # and the implicit Bearer Token access control scheme defined
        # by OPDS For Distrubutors.
        assert good_media_type == format.content_type
        assert DeliveryMechanism.BEARER_TOKEN == format.drm_scheme

        # Undo the mock of SUPPORTED_MEDIA_TYPES.
        api.SUPPORTED_MEDIA_TYPES = old_value

    def test_update_work_for_edition_returns_correct_license_pool(
        self, opds_dist_api_fixture: OPDSForDistributorsAPIFixture
    ):
        collection1 = opds_dist_api_fixture.mock_collection(name="Test Collection 1")
        collection2 = opds_dist_api_fixture.mock_collection(name="Test Collection 2")

        work = opds_dist_api_fixture.db.work(
            with_license_pool=False,
            collection=collection1,
        )
        edition = work.presentation_edition

        collection1_lp = opds_dist_api_fixture.db.licensepool(
            edition=edition, collection=collection1, set_edition_as_presentation=True
        )
        collection2_lp = opds_dist_api_fixture.db.licensepool(
            edition=edition, collection=collection2, set_edition_as_presentation=True
        )
        importer1 = OPDSForDistributorsImporter(
            opds_dist_api_fixture.db.session,
            collection=collection1,
        )
        importer2 = OPDSForDistributorsImporter(
            opds_dist_api_fixture.db.session,
            collection=collection2,
        )

        with patch.object(importer, "get_one", wraps=get_one) as get_one_mock:
            importer1_lp, _ = importer1.update_work_for_edition(edition)
            importer2_lp, _ = importer2.update_work_for_edition(edition)

        # Ensure distinct collections.
        assert collection1_lp != collection2_lp
        assert collection1_lp.collection.name == "Test Collection 1"
        assert collection2_lp.collection.name == "Test Collection 2"

        # The license pool returned to the importer should be the
        # same one originally created for a given collection.
        assert collection1_lp == importer1_lp
        assert collection2_lp == importer2_lp

        # With OPDS for Distributors imports, `update_work_for_edition`
        # should include `collection` in the license pool lookup criteria.
        assert 2 == len(get_one_mock.call_args_list)
        for call_args in get_one_mock.call_args_list:
            assert "collection" in call_args.kwargs

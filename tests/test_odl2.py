import datetime
import json
import os

import requests_mock
from api.odl2 import ODL2APIConfiguration, ODL2Importer
from webpub_manifest_parser.core.ast import PresentationMetadata
from webpub_manifest_parser.odl import ODLFeedParserFactory
from webpub_manifest_parser.odl.ast import ODLPublication
from webpub_manifest_parser.odl.semantic import (
    ODL_PUBLICATION_MUST_CONTAIN_EITHER_LICENSES_OR_OA_ACQUISITION_LINK_ERROR,
)

from core.coverage import CoverageFailure
from core.model import (
    Contribution,
    Contributor,
    DataSource,
    DeliveryMechanism,
    Edition,
    EditionConstants,
    LicensePool,
    MediaTypes,
    Work,
)
from core.model.configuration import ConfigurationFactory, ConfigurationStorage
from core.opds2_import import RWPMManifestParser
from core.tests.test_opds2_import import TestOPDS2Importer


class TestODL2Importer(TestOPDS2Importer):
    @staticmethod
    def _get_delivery_mechanism_by_drm_scheme_and_content_type(
        delivery_mechanisms, content_type, drm_scheme
    ):
        """Find a license pool in the list by its identifier.

        :param delivery_mechanisms: List of delivery mechanisms
        :type delivery_mechanisms: List[DeliveryMechanism]

        :param content_type: Content type
        :type content_type: str

        :param drm_scheme: DRM scheme
        :type drm_scheme: str

        :return: Delivery mechanism with the the specified DRM scheme and content type (if any)
        :rtype: Optional[DeliveryMechanism]
        """
        for delivery_mechanism in delivery_mechanisms:
            delivery_mechanism = delivery_mechanism.delivery_mechanism

            if (
                delivery_mechanism.drm_scheme == drm_scheme
                and delivery_mechanism.content_type == content_type
            ):
                return delivery_mechanism

        return None

    def sample_opds(self, filename):
        base_path = os.path.split(__file__)[0]
        resource_path = os.path.join(base_path, "files", "odl2")
        return open(os.path.join(resource_path, filename)).read()

    def test(self):
        # Arrange
        odl_status = {"checkouts": {"left": 10, "available": 10}}
        collection = self._default_collection
        data_source = DataSource.lookup(
            self._db, "ODL 2.0 Data Source", autocreate=True
        )

        collection.data_source = data_source

        importer = ODL2Importer(
            self._db, collection, RWPMManifestParser(ODLFeedParserFactory())
        )
        content_server_feed = self.sample_opds("feed.json")

        configuration_storage = ConfigurationStorage(importer)
        configuration_factory = ConfigurationFactory()

        with configuration_factory.create(
            configuration_storage, self._db, ODL2APIConfiguration
        ) as configuration:
            configuration.skipped_license_formats = json.dumps(["text/html"])

        # Act
        with requests_mock.Mocker() as request_mock:
            request_mock.get("http://www.example.com/status/294024", json=odl_status)

            imported_editions, pools, works, failures = importer.import_from_feed(
                content_server_feed
            )

            self._db.commit()

        # Assert

        # 1. Make sure that there is a single edition only
        assert isinstance(imported_editions, list)
        assert 1 == len(imported_editions)

        moby_dick_edition = self._get_edition_by_identifier(
            imported_editions, "urn:isbn:978-3-16-148410-0"
        )
        assert isinstance(moby_dick_edition, Edition)

        assert u"Moby-Dick" == moby_dick_edition.title
        assert u"eng" == moby_dick_edition.language
        assert u"eng" == moby_dick_edition.language
        assert EditionConstants.BOOK_MEDIUM == moby_dick_edition.medium
        assert u"Herman Melville" == moby_dick_edition.author

        assert 1 == len(moby_dick_edition.author_contributors)
        [moby_dick_author] = moby_dick_edition.author_contributors
        assert isinstance(moby_dick_author, Contributor)
        assert u"Herman Melville" == moby_dick_author.display_name
        assert u"Melville, Herman" == moby_dick_author.sort_name

        assert 1 == len(moby_dick_author.contributions)
        [moby_dick_author_author_contribution] = moby_dick_author.contributions
        assert isinstance(moby_dick_author_author_contribution, Contribution)
        assert moby_dick_author == moby_dick_author_author_contribution.contributor
        assert moby_dick_edition == moby_dick_author_author_contribution.edition
        assert Contributor.AUTHOR_ROLE == moby_dick_author_author_contribution.role

        assert data_source == moby_dick_edition.data_source

        assert u"Test Publisher" == moby_dick_edition.publisher
        assert datetime.date(2015, 9, 29) == moby_dick_edition.published

        assert u"http://example.org/cover.jpg" == moby_dick_edition.cover_full_url
        assert (
            u"http://example.org/cover-small.jpg"
            == moby_dick_edition.cover_thumbnail_url
        )

        # 2. Make sure that license pools have correct configuration
        assert isinstance(pools, list)
        assert 1 == len(pools)

        moby_dick_license_pool = self._get_license_pool_by_identifier(
            pools, "urn:isbn:978-3-16-148410-0"
        )
        assert isinstance(moby_dick_license_pool, LicensePool)
        assert not moby_dick_license_pool.open_access
        assert 10 == moby_dick_license_pool.licenses_owned
        assert (
            odl_status["checkouts"]["available"]
            == moby_dick_license_pool.licenses_available
        )

        assert 5 == len(moby_dick_license_pool.delivery_mechanisms)

        moby_dick_epub_adobe_drm_delivery_mechanism = (
            self._get_delivery_mechanism_by_drm_scheme_and_content_type(
                moby_dick_license_pool.delivery_mechanisms,
                MediaTypes.EPUB_MEDIA_TYPE,
                DeliveryMechanism.ADOBE_DRM,
            )
        )
        assert moby_dick_epub_adobe_drm_delivery_mechanism is not None

        moby_dick_epub_lcp_drm_delivery_mechanism = (
            self._get_delivery_mechanism_by_drm_scheme_and_content_type(
                moby_dick_license_pool.delivery_mechanisms,
                MediaTypes.EPUB_MEDIA_TYPE,
                DeliveryMechanism.LCP_DRM,
            )
        )
        assert moby_dick_epub_lcp_drm_delivery_mechanism is not None

        moby_dick_audio_book_adobe_drm_delivery_mechanism = (
            self._get_delivery_mechanism_by_drm_scheme_and_content_type(
                moby_dick_license_pool.delivery_mechanisms,
                MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
                DeliveryMechanism.ADOBE_DRM,
            )
        )
        assert moby_dick_audio_book_adobe_drm_delivery_mechanism is not None

        moby_dick_audio_book_lcp_drm_delivery_mechanism = (
            self._get_delivery_mechanism_by_drm_scheme_and_content_type(
                moby_dick_license_pool.delivery_mechanisms,
                MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
                DeliveryMechanism.LCP_DRM,
            )
        )
        assert moby_dick_audio_book_lcp_drm_delivery_mechanism is not None

        moby_dick_audio_book_feedbooks_drm_delivery_mechanism = (
            self._get_delivery_mechanism_by_drm_scheme_and_content_type(
                moby_dick_license_pool.delivery_mechanisms,
                MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
                DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM,
            )
        )
        assert moby_dick_audio_book_feedbooks_drm_delivery_mechanism is not None

        assert 1 == len(moby_dick_license_pool.licenses)
        [moby_dick_license] = moby_dick_license_pool.licenses
        assert (
            "urn:uuid:f7847120-fc6f-11e3-8158-56847afe9799"
            == moby_dick_license.identifier
        )
        assert (
            "http://www.example.com/get{?id,checkout_id,expires,patron_id,passphrase,hint,hint_url,notification_url}"
            == moby_dick_license.checkout_url
        )
        assert "http://www.example.com/status/294024" == moby_dick_license.status_url
        assert datetime.datetime(2016, 4, 25, 10, 25, 21) == moby_dick_license.expires
        assert 10 == moby_dick_license.remaining_checkouts
        assert 10 == moby_dick_license.concurrent_checkouts

        # 3. Make sure that work objects contain all the required metadata
        assert isinstance(works, list)
        assert 1 == len(works)

        moby_dick_work = self._get_work_by_identifier(
            works, "urn:isbn:978-3-16-148410-0"
        )
        assert isinstance(moby_dick_work, Work)
        assert moby_dick_edition == moby_dick_work.presentation_edition
        assert 1 == len(moby_dick_work.license_pools)
        assert moby_dick_license_pool == moby_dick_work.license_pools[0]

        # 4. Make sure that the failure is covered
        assert 1 == len(failures)
        huck_finn_failures = failures["9781234567897"]

        assert 1 == len(huck_finn_failures)
        [huck_finn_failure] = huck_finn_failures
        assert isinstance(huck_finn_failure, CoverageFailure)
        assert "9781234567897" == huck_finn_failure.obj.identifier

        huck_finn_semantic_error = ODL_PUBLICATION_MUST_CONTAIN_EITHER_LICENSES_OR_OA_ACQUISITION_LINK_ERROR(
            node=ODLPublication(
                metadata=PresentationMetadata(identifier="urn:isbn:9781234567897")
            ),
            node_property=None,
        )
        assert huck_finn_semantic_error.message == huck_finn_failure.exception

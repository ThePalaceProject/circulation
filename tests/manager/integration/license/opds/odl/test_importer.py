from __future__ import annotations

import copy
import datetime
import functools
import json
import uuid
from functools import partial
from typing import Any

import pytest
from freezegun import freeze_time
from jinja2 import Template
from sqlalchemy import select

from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.policy.presentation import PresentationCalculationPolicy
from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
from palace.manager.integration.license.opds.odl.importer import (
    OPDS2WithODLImporter,
    importer_from_collection,
)
from palace.manager.integration.license.opds.requests import OPDS2AuthType
from palace.manager.opds.odl.info import Checkouts, LicenseInfo, LicenseStatus
from palace.manager.opds.odl.terms import Terms
from palace.manager.opds.opds2 import PublicationFeedNoValidation
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.sqlalchemy.constants import (
    EditionConstants,
    IdentifierConstants,
    MediaTypes,
)
from palace.manager.sqlalchemy.model.contributor import Contribution, Contributor
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.resource import Hyperlink
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util import datetime_helpers
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OPDS2FilesFixture, OPDS2WithODLFilesFixture
from tests.fixtures.http import MockHttpClientFixture
from tests.fixtures.services import ServicesFixture
from tests.fixtures.work import WorkIdPolicyQueuePresentationRecalculationFixture


class OPDS2WithODLImporterFixture:
    def __init__(
        self,
        db: DatabaseTransactionFixture,
        services: ServicesFixture,
        http_client: MockHttpClientFixture,
        files_fixture: OPDS2WithODLFilesFixture,
        work_policy_recalc_fixture: WorkIdPolicyQueuePresentationRecalculationFixture,
    ):
        self.db = db
        self.collection = db.collection(
            protocol=OPDS2WithODLApi,
            settings=db.opds2_odl_settings(data_source="test collection"),
        )
        self.importer = importer_from_collection(
            self.collection,
            services.services.integration_registry().license_providers(),
        )
        self.client = http_client
        self.files = files_fixture
        self.work_policy_recalc_fixture = work_policy_recalc_fixture

    def queue_response(self, item: LicenseInfo | str | bytes) -> None:
        if isinstance(item, LicenseInfo):
            self.client.queue_response(200, content=item.model_dump_json())
        else:
            self.client.queue_response(200, content=item)

    def queue_fixture_file(self, filename: str) -> None:
        self.client.queue_response(200, content=self.files.sample_data(filename))

    def mock_apply_bibliographic(
        self, data: BibliographicData, collection_id: int
    ) -> None:
        assert collection_id == self.collection.id
        edition, _ = data.edition(self.db.session)
        data.apply(self.db.session, edition, self.collection)

    def mock_apply_circulation(self, data: CirculationData, collection_id: int) -> None:
        assert collection_id == self.collection.id
        data.apply(self.db.session, self.collection)

    def import_fixture_file(
        self,
        filename: str = "feed_template.json.jinja",
        licenses: list[LicenseInfo] | None = None,
    ) -> tuple[
        list[Edition],
        list[LicensePool],
        list[Work],
    ]:
        feed = self.files.sample_text(filename)

        if licenses is not None:
            for _license in licenses:
                self.queue_response(_license)
            feed = Template(feed).render(licenses=licenses)

        return self.import_feed(feed)

    def import_feed(self, feed: str) -> tuple[
        list[Edition],
        list[LicensePool],
        list[Work],
    ]:
        self.client.queue_response(200, content=feed, index=0)

        feed_parsed = self.importer.get_feed("http://example.org/feed.json")

        self.importer.import_feed(
            self.db.session,
            feed_parsed,
            self.collection,
            apply_bibliographic=self.mock_apply_bibliographic,
            apply_circulation=self.mock_apply_circulation,
        )

        editions = self.db.session.scalars(select(Edition).order_by(Edition.id)).all()
        license_pools = (
            self.db.session.scalars(select(LicensePool).order_by(LicensePool.id))
            .unique()
            .all()
        )
        works = self.db.session.scalars(select(Work).order_by(Work.id)).unique().all()

        return editions, license_pools, works

    @staticmethod
    def get_delivery_mechanism_by_drm_scheme_and_content_type(
        delivery_mechanisms: list[LicensePoolDeliveryMechanism],
        content_type: str,
        drm_scheme: str | None,
    ) -> DeliveryMechanism | None:
        """Find a license pool in the list by its identifier.

        :param delivery_mechanisms: List of delivery mechanisms
        :param content_type: Content type
        :param drm_scheme: DRM scheme

        :return: Delivery mechanism with the specified DRM scheme and content type (if any)
        """
        for delivery_mechanism in delivery_mechanisms:
            mechanism = delivery_mechanism.delivery_mechanism

            if (
                mechanism.drm_scheme == drm_scheme
                and mechanism.content_type == content_type
            ):
                return mechanism

        return None


@pytest.fixture
def opds2_with_odl_importer_fixture(
    db: DatabaseTransactionFixture,
    services_fixture: ServicesFixture,
    http_client: MockHttpClientFixture,
    opds2_with_odl_files_fixture: OPDS2WithODLFilesFixture,
    work_policy_recalc_fixture: WorkIdPolicyQueuePresentationRecalculationFixture,
) -> OPDS2WithODLImporterFixture:
    return OPDS2WithODLImporterFixture(
        db,
        services_fixture,
        http_client,
        opds2_with_odl_files_fixture,
        work_policy_recalc_fixture,
    )


class TestOPDS2WithODLImporter:
    @freeze_time("2016-01-01T00:00:00+00:00")
    def test_import(
        self,
        opds2_with_odl_importer_fixture: OPDS2WithODLImporterFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Ensure that OPDSWithODLImporter correctly processes and imports the ODL feed encoded using OPDS 2.x.

        NOTE: `freeze_time` decorator is required to treat the licenses in the ODL feed as non-expired.
        """
        caplog.set_level(LogLevel.error)

        opds2_with_odl_importer_fixture.importer._ignored_identifier_types = {
            IdentifierConstants.URI
        }

        # Act
        (
            imported_editions,
            pools,
            works,
        ) = opds2_with_odl_importer_fixture.import_fixture_file(
            "feed.json",
            [
                LicenseInfo(
                    identifier="urn:uuid:f7847120-fc6f-11e3-8158-56847afe9799",
                    status=LicenseStatus.available,
                    checkouts=Checkouts(left=30, available=10),
                    terms=Terms(
                        concurrency=10,
                        checkouts=30,
                        expires="2016-04-25T12:25:21+02:00",
                    ),
                )
            ],
        )

        # Assert

        # 1. Make sure that there is a single edition only
        assert isinstance(imported_editions, list)
        assert 1 == len(imported_editions)

        [moby_dick_edition] = imported_editions
        assert isinstance(moby_dick_edition, Edition)
        assert moby_dick_edition.primary_identifier.identifier == "978-3-16-148410-0"
        assert moby_dick_edition.primary_identifier.type == "ISBN"
        assert Hyperlink.SAMPLE in {
            l.rel for l in moby_dick_edition.primary_identifier.links
        }

        assert "Moby-Dick" == moby_dick_edition.title
        assert "eng" == moby_dick_edition.language
        assert "eng" == moby_dick_edition.language
        assert EditionConstants.BOOK_MEDIUM == moby_dick_edition.medium
        assert "Herman Melville" == moby_dick_edition.author

        assert 1 == len(moby_dick_edition.author_contributors)
        [moby_dick_author] = moby_dick_edition.author_contributors
        assert isinstance(moby_dick_author, Contributor)
        assert "Herman Melville" == moby_dick_author.display_name
        assert "Melville, Herman" == moby_dick_author.sort_name

        assert 1 == len(moby_dick_author.contributions)
        [moby_dick_author_author_contribution] = moby_dick_author.contributions
        assert isinstance(moby_dick_author_author_contribution, Contribution)
        assert moby_dick_author == moby_dick_author_author_contribution.contributor
        assert moby_dick_edition == moby_dick_author_author_contribution.edition
        assert Contributor.Role.AUTHOR == moby_dick_author_author_contribution.role

        assert "test collection" == moby_dick_edition.data_source.name

        assert "Test Publisher" == moby_dick_edition.publisher
        assert datetime.date(2015, 9, 29) == moby_dick_edition.published

        assert "http://example.org/cover.jpg" == moby_dick_edition.cover_full_url
        assert (
            "http://example.org/cover-small.jpg"
            == moby_dick_edition.cover_thumbnail_url
        )

        # 2. Make sure that license pools have correct configuration
        assert isinstance(pools, list)
        assert 1 == len(pools)

        [moby_dick_license_pool] = pools
        assert isinstance(moby_dick_license_pool, LicensePool)
        assert moby_dick_license_pool.identifier.identifier == "978-3-16-148410-0"
        assert moby_dick_license_pool.identifier.type == "ISBN"
        assert not moby_dick_license_pool.open_access
        assert 10 == moby_dick_license_pool.licenses_owned
        assert 10 == moby_dick_license_pool.licenses_available

        assert 2 == len(moby_dick_license_pool.delivery_mechanisms)

        moby_dick_epub_adobe_drm_delivery_mechanism = opds2_with_odl_importer_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
            moby_dick_license_pool.delivery_mechanisms,
            MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
        )
        assert moby_dick_epub_adobe_drm_delivery_mechanism is not None

        moby_dick_epub_lcp_drm_delivery_mechanism = opds2_with_odl_importer_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
            moby_dick_license_pool.delivery_mechanisms,
            MediaTypes.EPUB_MEDIA_TYPE,
            DeliveryMechanism.LCP_DRM,
        )
        assert moby_dick_epub_lcp_drm_delivery_mechanism is not None

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
        assert (
            datetime.datetime(2016, 4, 25, 10, 25, 21, tzinfo=datetime.timezone.utc)
            == moby_dick_license.expires
        )
        assert 30 == moby_dick_license.checkouts_left
        assert 10 == moby_dick_license.checkouts_available

        # 3. Make sure that work objects contain all the required metadata
        assert isinstance(works, list)
        assert 1 == len(works)

        [moby_dick_work] = works
        assert isinstance(moby_dick_work, Work)
        assert moby_dick_edition == moby_dick_work.presentation_edition
        assert 1 == len(moby_dick_work.license_pools)
        assert moby_dick_license_pool == moby_dick_work.license_pools[0]

        # 4. Make sure that the failure is covered
        assert (
            "Error validating publication (identifier: urn:isbn:9781234567897, "
            "title: None, feed: http://example.com/feed): 2 validation errors"
            in caplog.text
        )

        # 5. Make sure that expected work id are queued for recalculation
        policy = PresentationCalculationPolicy.recalculate_everything()
        for w in works:
            assert opds2_with_odl_importer_fixture.work_policy_recalc_fixture.is_queued(
                w.id, policy
            )

    @freeze_time("2016-01-01T00:00:00+00:00")
    def test_import_audiobook_with_streaming(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_importer_fixture: OPDS2WithODLImporterFixture,
    ) -> None:
        """Ensure that OPDSWithODLImporter correctly processes and imports a feed with an audiobook."""

        opds2_with_odl_importer_fixture.queue_fixture_file("license-audiobook.json")
        (imported_editions, pools, works) = (
            opds2_with_odl_importer_fixture.import_fixture_file(
                "feed-audiobook-streaming.json"
            )
        )

        # Make sure we imported one edition and it is an audiobook
        assert isinstance(imported_editions, list)
        assert 1 == len(imported_editions)

        [edition] = imported_editions
        assert isinstance(edition, Edition)
        assert edition.primary_identifier.identifier == "9780792766919"
        assert edition.primary_identifier.type == "ISBN"
        assert EditionConstants.AUDIO_MEDIUM == edition.medium

        # Make sure that license pools have correct configuration
        assert isinstance(pools, list)
        assert 1 == len(pools)

        [license_pool] = pools
        assert not license_pool.open_access
        assert 1 == license_pool.licenses_owned
        assert 1 == license_pool.licenses_available

        assert 2 == len(license_pool.delivery_mechanisms)

        lcp_delivery_mechanism = opds2_with_odl_importer_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
            license_pool.delivery_mechanisms,
            MediaTypes.AUDIOBOOK_PACKAGE_LCP_MEDIA_TYPE,
            DeliveryMechanism.LCP_DRM,
        )
        assert lcp_delivery_mechanism is not None

        feedbooks_delivery_mechanism = opds2_with_odl_importer_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
            license_pool.delivery_mechanisms,
            MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
            DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM,
        )
        assert feedbooks_delivery_mechanism is not None

    @freeze_time("2016-01-01T00:00:00+00:00")
    def test_import_audiobook_no_streaming(
        self,
        opds2_with_odl_importer_fixture: OPDS2WithODLImporterFixture,
    ) -> None:
        """
        Ensure that OPDSWithODLImporter correctly processes and imports a feed with an audiobook
        that is not available for streaming.
        """
        opds2_with_odl_importer_fixture.queue_fixture_file("license-audiobook.json")

        (
            imported_editions,
            pools,
            works,
        ) = opds2_with_odl_importer_fixture.import_fixture_file(
            "feed-audiobook-no-streaming.json"
        )

        # Make sure we imported one edition and it is an audiobook
        assert isinstance(imported_editions, list)
        assert 1 == len(imported_editions)

        [edition] = imported_editions
        assert isinstance(edition, Edition)
        assert edition.primary_identifier.identifier == "9781603937221"
        assert edition.primary_identifier.type == "ISBN"
        assert EditionConstants.AUDIO_MEDIUM == edition.medium

        # Make sure that license pools have correct configuration
        assert isinstance(pools, list)
        assert 1 == len(pools)

        [license_pool] = pools
        assert not license_pool.open_access
        assert 1 == license_pool.licenses_owned
        assert 1 == license_pool.licenses_available

        assert 1 == len(license_pool.delivery_mechanisms)

        lcp_delivery_mechanism = opds2_with_odl_importer_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
            license_pool.delivery_mechanisms,
            MediaTypes.AUDIOBOOK_PACKAGE_LCP_MEDIA_TYPE,
            DeliveryMechanism.LCP_DRM,
        )
        assert lcp_delivery_mechanism is not None

    @pytest.mark.parametrize(
        "auth_type",
        [
            OPDS2AuthType.BASIC,
            OPDS2AuthType.OAUTH,
        ],
    )
    def test_import_open_access(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_importer_fixture: OPDS2WithODLImporterFixture,
        auth_type: OPDS2AuthType,
    ) -> None:
        """
        Ensure that OPDSWithODLImporter correctly processes and imports a feed with an
        open access book.
        """
        importer = opds2_with_odl_importer_fixture.importer
        importer._extractor._bearer_token_drm = auth_type == OPDS2AuthType.OAUTH
        (
            imported_editions,
            pools,
            works,
        ) = opds2_with_odl_importer_fixture.import_fixture_file(
            "open-access-title.json"
        )

        assert isinstance(imported_editions, list)
        assert 1 == len(imported_editions)

        [edition] = imported_editions
        assert isinstance(edition, Edition)
        assert (
            edition.primary_identifier.identifier
            == "https://www.feedbooks.com/book/7256"
        )
        assert edition.primary_identifier.type == "URI"
        assert edition.medium == EditionConstants.BOOK_MEDIUM

        # Make sure that license pools have correct configuration
        assert isinstance(pools, list)
        assert 1 == len(pools)

        [license_pool] = pools
        assert license_pool.open_access is True
        assert license_pool.unlimited_access is True

        assert 1 == len(license_pool.delivery_mechanisms)

        oa_ebook_delivery_mechanism = opds2_with_odl_importer_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
            license_pool.delivery_mechanisms,
            MediaTypes.EPUB_MEDIA_TYPE,
            None,
        )
        assert oa_ebook_delivery_mechanism is not None

    @pytest.mark.parametrize(
        "auth_type",
        [
            OPDS2AuthType.BASIC,
            OPDS2AuthType.OAUTH,
        ],
    )
    def test_import_unlimited_access(
        self,
        db: DatabaseTransactionFixture,
        opds2_with_odl_importer_fixture: OPDS2WithODLImporterFixture,
        auth_type: OPDS2AuthType,
    ) -> None:
        """
        Ensure that OPDSWithODLImporter correctly processes and imports a feed with an
        unlimited access book.
        """
        importer = opds2_with_odl_importer_fixture.importer
        importer._extractor._bearer_token_drm = auth_type == OPDS2AuthType.OAUTH

        (
            imported_editions,
            pools,
            works,
        ) = opds2_with_odl_importer_fixture.import_fixture_file(
            "unlimited-access-title.json"
        )

        assert isinstance(imported_editions, list)
        assert 1 == len(imported_editions)

        [edition] = imported_editions
        assert isinstance(edition, Edition)
        assert (
            edition.primary_identifier.identifier
            == "urn:uuid:a0f77af3-a2a6-4a29-8e1f-18e06e4e573e"
        )
        assert edition.primary_identifier.type == "URI"
        assert edition.medium == EditionConstants.AUDIO_MEDIUM

        # Make sure that license pools have correct configuration
        assert isinstance(pools, list)
        assert 1 == len(pools)

        [license_pool] = pools
        assert license_pool.open_access is False
        assert license_pool.unlimited_access is True

        assert 1 == len(license_pool.delivery_mechanisms)

        ebook_delivery_mechanism = opds2_with_odl_importer_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
            license_pool.delivery_mechanisms,
            MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
            (
                DeliveryMechanism.BEARER_TOKEN
                if auth_type == OPDS2AuthType.OAUTH
                else None
            ),
        )
        assert ebook_delivery_mechanism is not None

    @freeze_time("2016-01-01T00:00:00+00:00")
    def test_import_availability(
        self,
        opds2_with_odl_importer_fixture: OPDS2WithODLImporterFixture,
    ) -> None:
        feed_json = json.loads(
            opds2_with_odl_importer_fixture.files.sample_text("feed.json")
        )

        moby_dick_license_dict = feed_json["publications"][0]["licenses"][0]
        test_book_license_dict = feed_json["publications"][2]["licenses"][0]

        huck_finn_publication_dict = feed_json["publications"][1]
        huck_finn_publication_dict["licenses"] = copy.deepcopy(
            feed_json["publications"][0]["licenses"]
        )
        huck_finn_publication_dict["images"] = copy.deepcopy(
            feed_json["publications"][0]["images"]
        )
        huck_finn_license_dict = huck_finn_publication_dict["licenses"][0]

        MOBY_DICK_LICENSE_ID = "urn:uuid:f7847120-fc6f-11e3-8158-56847afe9799"
        TEST_BOOK_LICENSE_ID = "urn:uuid:f7847120-fc6f-11e3-8158-56847afe9798"
        HUCK_FINN_LICENSE_ID = f"urn:uuid:{uuid.uuid4()}"

        test_book_license_dict["metadata"]["availability"] = {
            "state": "unavailable",
            "reason": "https://registry.opds.io/reason#preordered",
            "until": "2016-01-20T00:00:00Z",
        }
        huck_finn_license_dict["metadata"]["identifier"] = HUCK_FINN_LICENSE_ID
        huck_finn_publication_dict["metadata"][
            "title"
        ] = "Adventures of Huckleberry Finn"

        # Mock responses from license status server
        def license_status_reply(
            license_id: str,
            concurrency: int = 10,
            checkouts: int | None = 30,
            expires: str | None = "2016-04-25T12:25:21+02:00",
        ) -> LicenseInfo:
            return LicenseInfo(
                identifier=license_id,
                status=LicenseStatus.available,
                checkouts=Checkouts(
                    available=concurrency,
                    left=checkouts,
                ),
                terms=Terms(
                    expires=expires,
                    concurrency=concurrency,
                ),
            )

        opds2_with_odl_importer_fixture.queue_response(
            license_status_reply(MOBY_DICK_LICENSE_ID)
        )
        opds2_with_odl_importer_fixture.queue_response(
            license_status_reply(HUCK_FINN_LICENSE_ID)
        )

        (
            imported_editions,
            pools,
            works,
        ) = opds2_with_odl_importer_fixture.import_feed(json.dumps(feed_json))

        assert isinstance(pools, list)
        assert 3 == len(pools)

        [moby_dick_pool, huck_finn_pool, test_book_pool] = pools

        def assert_pool(
            pool: LicensePool,
            identifier: str,
            identifier_type: str,
            licenses_owned: int,
            licenses_available: int,
            license_id: str,
            available_for_borrowing: bool,
            license_status: LicenseStatus,
        ) -> None:
            assert pool.identifier.identifier == identifier
            assert pool.identifier.type == identifier_type
            assert pool.licenses_owned == licenses_owned
            assert pool.licenses_available == licenses_available
            assert len(pool.licenses) == 1
            [license_info] = pool.licenses
            assert license_info.identifier == license_id
            assert license_info.is_available_for_borrowing is available_for_borrowing
            assert license_info.status == license_status

        assert_moby_dick_pool = functools.partial(
            assert_pool,
            identifier="978-3-16-148410-0",
            identifier_type="ISBN",
            license_id=MOBY_DICK_LICENSE_ID,
        )
        assert_test_book_pool = functools.partial(
            assert_pool,
            identifier="http://example.org/test-book",
            identifier_type="URI",
            license_id=TEST_BOOK_LICENSE_ID,
        )
        assert_huck_finn_pool = functools.partial(
            assert_pool,
            identifier="9781234567897",
            identifier_type="ISBN",
            license_id=HUCK_FINN_LICENSE_ID,
        )

        assert_moby_dick_pool(
            moby_dick_pool,
            licenses_owned=10,
            licenses_available=10,
            available_for_borrowing=True,
            license_status=LicenseStatus.available,
        )

        assert_test_book_pool(
            test_book_pool,
            licenses_owned=0,
            licenses_available=0,
            available_for_borrowing=False,
            license_status=LicenseStatus.unavailable,
        )

        assert_huck_finn_pool(
            huck_finn_pool,
            licenses_owned=10,
            licenses_available=10,
            available_for_borrowing=True,
            license_status=LicenseStatus.available,
        )

        # Harvest the feed again, but this time the status has changed
        moby_dick_license_dict["metadata"]["availability"] = {
            "state": "unavailable",
        }
        del test_book_license_dict["metadata"]["availability"]
        huck_finn_publication_dict["metadata"]["availability"] = {
            "state": "unavailable",
        }

        # Mock responses from license status server
        opds2_with_odl_importer_fixture.queue_response(
            license_status_reply(TEST_BOOK_LICENSE_ID, checkouts=None, expires=None)
        )

        # Harvest the feed again
        (
            imported_editions,
            pools,
            works,
        ) = opds2_with_odl_importer_fixture.import_feed(json.dumps(feed_json))

        assert isinstance(pools, list)
        assert 3 == len(pools)

        [moby_dick_pool, huck_finn_pool, test_book_pool] = pools

        assert_moby_dick_pool(
            moby_dick_pool,
            licenses_owned=0,
            licenses_available=0,
            available_for_borrowing=False,
            license_status=LicenseStatus.unavailable,
        )

        assert_test_book_pool(
            test_book_pool,
            licenses_owned=10,
            licenses_available=10,
            available_for_borrowing=True,
            license_status=LicenseStatus.available,
        )

        assert_huck_finn_pool(
            huck_finn_pool,
            licenses_owned=0,
            licenses_available=0,
            available_for_borrowing=False,
            license_status=LicenseStatus.unavailable,
        )

    @pytest.mark.parametrize(
        "license",
        [
            pytest.param(
                LicenseInfo(
                    identifier="urn:uuid:expired-license-1",
                    status=LicenseStatus.available,
                    checkouts=Checkouts(
                        left=52,
                        available=1,
                    ),
                    terms=Terms(
                        concurrency=1,
                        expires="2021-01-01T00:01:00+01:00",
                    ),
                ),
                id="expiration_date_in_the_past",
            ),
            pytest.param(
                LicenseInfo(
                    identifier="urn:uuid:left-is-zero",
                    status=LicenseStatus.available,
                    checkouts=Checkouts(
                        left=0,
                        available=1,
                    ),
                    terms=Terms(
                        concurrency=1,
                    ),
                ),
                id="left_is_zero",
            ),
            pytest.param(
                LicenseInfo(
                    identifier="urn:uuid:status-unavailable",
                    status=LicenseStatus.unavailable,
                    checkouts=Checkouts(
                        available=1,
                    ),
                ),
                id="status_unavailable",
            ),
        ],
    )
    @freeze_time("2021-01-01T00:00:00+00:00")
    def test_odl_importer_expired_licenses(
        self,
        opds2_with_odl_importer_fixture: OPDS2WithODLImporterFixture,
        license: LicenseInfo,
    ):
        """Ensure OPDSWithODLImporter imports expired licenses, but does not count them."""
        # Import the test feed with an expired ODL license.
        (
            imported_editions,
            imported_pools,
            imported_works,
        ) = opds2_with_odl_importer_fixture.import_fixture_file(licenses=[license])

        # The importer created 1 edition and 1 work.
        assert len(imported_editions) == 1
        assert len(imported_works) == 1

        # Ensure that the license pool was successfully created, with no available copies.
        assert len(imported_pools) == 1

        [imported_pool] = imported_pools
        assert imported_pool.licenses_owned == 0
        assert imported_pool.licenses_available == 0
        assert len(imported_pool.licenses) == 1

        # Ensure the license was imported and is expired.
        [imported_license] = imported_pool.licenses
        assert imported_license.is_inactive is True

    def test_odl_importer_reimport_expired_licenses(
        self,
        opds2_with_odl_importer_fixture: OPDS2WithODLImporterFixture,
    ):
        license_expiry = datetime.datetime.fromisoformat("2021-01-01T00:01:00+00:00")
        licenses = [
            LicenseInfo(
                identifier="test",
                status=LicenseStatus.available,
                checkouts=Checkouts(available=1),
                terms=Terms(
                    concurrency=1,
                    expires=license_expiry,
                ),
            )
        ]

        # First import the license when it is not expired
        with freeze_time(license_expiry - datetime.timedelta(days=1)):
            # Import the test feed.
            (
                imported_editions,
                imported_pools,
                imported_works,
            ) = opds2_with_odl_importer_fixture.import_fixture_file(licenses=licenses)

            # The importer created 1 edition and 1 work with no failures.
            assert len(imported_editions) == 1
            assert len(imported_works) == 1
            assert len(imported_pools) == 1

            # Ensure that the license pool was successfully created, with available copies.
            [imported_pool] = imported_pools
            assert imported_pool.licenses_owned == 1
            assert imported_pool.licenses_available == 1
            assert len(imported_pool.licenses) == 1

            # Ensure the license was imported and is not expired.
            [imported_license] = imported_pool.licenses
            assert imported_license.is_inactive is False

        # Reimport the license when it is expired
        with freeze_time(license_expiry + datetime.timedelta(days=1)):
            # Import the test feed.
            (
                imported_editions,
                imported_pools,
                imported_works,
            ) = opds2_with_odl_importer_fixture.import_fixture_file(licenses=licenses)

            # The importer created 1 edition and 1 work with no failures.
            assert len(imported_editions) == 1
            assert len(imported_works) == 1
            assert len(imported_pools) == 1

            # Ensure that the license pool was successfully created, with no available copies.
            [imported_pool] = imported_pools
            assert imported_pool.licenses_owned == 0
            assert imported_pool.licenses_available == 0
            assert len(imported_pool.licenses) == 1

            # Ensure the license was imported and is expired.
            [imported_license] = imported_pool.licenses
            assert imported_license.is_inactive is True

    @freeze_time("2021-01-01T00:00:00+00:00")
    def test_odl_importer_multiple_expired_licenses(
        self,
        opds2_with_odl_importer_fixture: OPDS2WithODLImporterFixture,
    ):
        """Ensure OPDSWithODLImporter imports expired licenses
        and does not count them in the total number of available licenses."""

        # 1.1. Import the test feed with three inactive ODL licenses and two active licenses.
        inactive = [
            # Expired
            # (expiry date in the past)
            LicenseInfo(
                identifier="urn:uuid:expired-license-1",
                status=LicenseStatus.available,
                checkouts=Checkouts(
                    available=1,
                ),
                terms=Terms(
                    concurrency=1,
                    expires=datetime_helpers.utc_now() - datetime.timedelta(days=1),
                ),
            ),
            # Expired
            # (left is 0)
            LicenseInfo(
                identifier="urn:uuid:expired-license-2",
                status=LicenseStatus.available,
                checkouts=Checkouts(
                    available=1,
                    left=0,
                ),
            ),
            # Expired
            # (status is unavailable)
            LicenseInfo(
                identifier="urn:uuid:expired-license-3",
                status=LicenseStatus.unavailable,
                checkouts=Checkouts(
                    available=1,
                ),
            ),
        ]
        active = [
            LicenseInfo(
                identifier="urn:uuid:active-license-1",
                status=LicenseStatus.available,
                checkouts=Checkouts(
                    available=1,
                ),
                terms=Terms(
                    concurrency=1,
                ),
            ),
            LicenseInfo(
                identifier="urn:uuid:active-license-2",
                status=LicenseStatus.available,
                checkouts=Checkouts(
                    available=5,
                    left=40,
                ),
                terms=Terms(
                    concurrency=5,
                ),
            ),
        ]

        (
            imported_editions,
            imported_pools,
            imported_works,
        ) = opds2_with_odl_importer_fixture.import_fixture_file(
            licenses=active + inactive
        )

        # License pool was successfully created
        assert len(imported_pools) == 1
        [imported_pool] = imported_pools

        # All licenses were imported
        assert len(imported_pool.licenses) == 5

        # Make sure that the license statistics are correct and include only active licenses.
        assert imported_pool.licenses_owned == 6
        assert imported_pool.licenses_available == 6

        # Correct number of active and inactive licenses
        assert sum(not l.is_inactive for l in imported_pool.licenses) == len(active)
        assert sum(l.is_inactive for l in imported_pool.licenses) == len(inactive)

    def test_odl_importer_reimport_multiple_licenses(
        self,
        opds2_with_odl_importer_fixture: OPDS2WithODLImporterFixture,
    ):
        """Ensure OPDSWithODLImporter correctly imports licenses that have already been imported."""

        # 1.1. Import the test feed with ODL licenses that are not expired.
        license_expiry = datetime.datetime.fromisoformat("2021-01-01T00:01:00+00:00")

        date = LicenseInfo(
            identifier="urn:uuid:date-license",
            status=LicenseStatus.available,
            checkouts=Checkouts(
                available=1,
            ),
            terms=Terms(
                concurrency=1,
                expires=license_expiry,
            ),
        )
        left = LicenseInfo(
            identifier="urn:uuid:left-license",
            status=LicenseStatus.available,
            checkouts=Checkouts(
                available=1,
                left=1,
            ),
            terms=Terms(
                concurrency=2,
            ),
        )
        perpetual = LicenseInfo(
            identifier="urn:uuid:perpetual-license",
            status=LicenseStatus.available,
            checkouts=Checkouts(
                available=0,
            ),
            terms=Terms(
                concurrency=1,
            ),
        )

        # Import with all licenses valid
        with freeze_time(license_expiry - datetime.timedelta(days=1)):
            (
                imported_editions,
                imported_pools,
                imported_works,
            ) = opds2_with_odl_importer_fixture.import_fixture_file(
                licenses=[date, left, perpetual]
            )

            assert len(imported_pools) == 1

            [imported_pool] = imported_pools
            assert len(imported_pool.licenses) == 3
            assert imported_pool.licenses_available == 2
            assert imported_pool.licenses_owned == 3

            # No licenses are expired
            assert sum(not l.is_inactive for l in imported_pool.licenses) == 3

        # Expire the first two licenses

        # The first one is expired by changing the time
        with freeze_time(license_expiry + datetime.timedelta(days=1)):
            # The second one is expired by setting left to 0
            left = LicenseInfo(
                identifier="urn:uuid:left-license",
                status=LicenseStatus.available,
                checkouts=Checkouts(
                    available=1,
                    left=0,
                ),
                terms=Terms(
                    concurrency=2,
                ),
            )

            # The perpetual license has a copy available
            perpetual = LicenseInfo(
                identifier="urn:uuid:perpetual-license",
                status=LicenseStatus.available,
                checkouts=Checkouts(
                    available=1,
                ),
                terms=Terms(
                    concurrency=1,
                ),
            )

            # Reimport
            (
                imported_editions,
                imported_pools,
                imported_works,
            ) = opds2_with_odl_importer_fixture.import_fixture_file(
                licenses=[date, left, perpetual]
            )

            assert len(imported_pools) == 1

            [imported_pool] = imported_pools
            assert len(imported_pool.licenses) == 3
            assert imported_pool.licenses_available == 1
            assert imported_pool.licenses_owned == 1

            # One license not expired
            assert sum(not l.is_inactive for l in imported_pool.licenses) == 1

            # Two licenses expired
            assert sum(l.is_inactive for l in imported_pool.licenses) == 2

    def test_fetch_license_info(
        self, opds2_with_odl_importer_fixture: OPDS2WithODLImporterFixture
    ):
        """Ensure that OPDS2WithODLImporter correctly retrieves license data from an OPDS2 feed."""

        def license_info_dict() -> dict[str, Any]:
            return LicenseInfo(
                identifier=str(uuid.uuid4()),
                status=LicenseStatus.available,
                checkouts=Checkouts(
                    available=10,
                ),
            ).model_dump(mode="json", exclude_none=True)

        importer = opds2_with_odl_importer_fixture.importer
        http_client = opds2_with_odl_importer_fixture.client

        fetch = partial(
            importer._fetch_license_document,
            "http://example.org/feed",
        )

        # Bad status code
        http_client.queue_response(400, content=b"Bad Request")
        assert fetch() is None
        assert len(http_client.requests) == 1
        assert http_client.requests.pop() == "http://example.org/feed"

        # 200 status - parses response body
        expiry = utc_now() + datetime.timedelta(days=1)
        license_helper = LicenseInfo(
            identifier=str(uuid.uuid4()),
            status=LicenseStatus.available,
            checkouts=Checkouts(
                available=10,
                left=4,
            ),
            terms=Terms(
                concurrency=11,
                expires=expiry,
            ),
        )
        http_client.queue_response(200, content=license_helper.model_dump_json())
        parsed = fetch()
        assert parsed.checkouts.available == 10
        assert parsed.checkouts.left == 4
        assert parsed.terms.concurrency == 11
        assert parsed.terms.expires == expiry
        assert parsed.status == LicenseStatus.available
        assert parsed.identifier == license_helper.identifier

        # 201 status - parses response body
        http_client.queue_response(201, content=license_helper.model_dump_json())
        parsed = fetch()
        assert parsed.checkouts.available == 10
        assert parsed.checkouts.left == 4
        assert parsed.terms.concurrency == 11
        assert parsed.terms.expires == expiry
        assert parsed.status == LicenseStatus.available
        assert parsed.identifier == license_helper.identifier

        # Bad data
        http_client.queue_response(201, content="{}")
        assert fetch() is None

        # No identifier
        license_dict = license_info_dict()
        license_dict.pop("identifier")
        http_client.queue_response(201, content=json.dumps(license_dict))
        assert fetch() is None

        # No status
        license_dict = license_info_dict()
        license_dict.pop("status")
        http_client.queue_response(201, content=json.dumps(license_dict))
        assert fetch() is None

        # Bad status
        license_dict = license_info_dict()
        license_dict["status"] = "bad"
        http_client.queue_response(201, content=json.dumps(license_dict))
        assert fetch() is None

        # No available
        license_dict = license_info_dict()
        license_dict["checkouts"].pop("available")
        http_client.queue_response(201, content=json.dumps(license_dict))
        assert fetch() is None

        # Format str
        license_dict = license_info_dict()
        license_dict["format"] = "single format"
        http_client.queue_response(201, content=json.dumps(license_dict))
        parsed = fetch()
        assert parsed is not None
        assert parsed.formats == ("single format",)

        # Format list
        license_dict = license_info_dict()
        license_dict["format"] = ["format1", "format2"]
        http_client.queue_response(201, content=json.dumps(license_dict))
        parsed = fetch()
        assert parsed is not None
        assert parsed.formats == ("format1", "format2")

    def test_next_page(self, opds2_files_fixture: OPDS2FilesFixture) -> None:
        # No next links
        feed = PublicationFeedNoValidation.model_validate_json(
            opds2_files_fixture.sample_data("feed.json")
        )
        assert OPDS2WithODLImporter.next_page(feed) is None

        # Feed has next link
        feed = PublicationFeedNoValidation.model_validate_json(
            opds2_files_fixture.sample_data("feed2.json")
        )
        assert (
            OPDS2WithODLImporter.next_page(feed)
            == "http://bookshelf-feed-demo.us-east-1.elasticbeanstalk.com/v1/publications?page=2&limit=100"
        )

    def test__filtered_publications(
        self,
        opds2_with_odl_importer_fixture: OPDS2WithODLImporterFixture,
        opds2_files_fixture: OPDS2FilesFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        importer = opds2_with_odl_importer_fixture.importer

        opds2_feed = json.loads(opds2_files_fixture.sample_text("feed.json"))
        publications = [opds2_feed["publications"][0], {}]
        filtered = list(importer._filtered_publications(publications))

        # Only the first publication is valid, so it is the one returned
        assert len(filtered) == 1
        identifier, publication = filtered[0]

        assert identifier.type == Identifier.ISBN
        assert identifier.identifier == "978-3-16-148410-0"
        assert publication.metadata.identifier == "urn:isbn:978-3-16-148410-0"

        # We also logged a warning about the invalid publication
        assert (
            "Error validating publication (identifier: None, title: None, feed: http://example.com/feed)"
            in caplog.text
        )

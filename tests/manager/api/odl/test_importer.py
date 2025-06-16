from __future__ import annotations

import copy
import datetime
import functools
import json
import uuid
from typing import Any
from unittest.mock import patch

import dateutil
import pytest
from freezegun import freeze_time
from requests import Response

from palace.manager.api.odl.api import OPDS2WithODLApi
from palace.manager.api.odl.importer import (
    OPDS2WithODLImporter,
    OPDS2WithODLImportMonitor,
)
from palace.manager.api.odl.settings import OPDS2AuthType, OPDS2WithODLSettings
from palace.manager.core.coverage import CoverageFailure
from palace.manager.data_layer.license import LicenseData
from palace.manager.data_layer.policy.presentation import PresentationCalculationPolicy
from palace.manager.opds.odl.info import LicenseStatus
from palace.manager.sqlalchemy.constants import (
    EditionConstants,
    IdentifierConstants,
    MediaTypes,
)
from palace.manager.sqlalchemy.model.contributor import Contribution, Contributor
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism, LicensePool
from palace.manager.sqlalchemy.model.resource import Hyperlink
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util import datetime_helpers
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http import HTTP
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.http import MockHttpClientFixture
from tests.fixtures.odl import (
    LicenseHelper,
    LicenseInfoHelper,
    OPDS2WithODLImporterFixture,
)


class TestOPDS2WithODLImporter:
    @freeze_time("2016-01-01T00:00:00+00:00")
    def test_import(
        self,
        opds2_with_odl_importer_fixture: OPDS2WithODLImporterFixture,
    ) -> None:
        """Ensure that OPDSWithODLImporter correctly processes and imports the ODL feed encoded using OPDS 2.x.

        NOTE: `freeze_time` decorator is required to treat the licenses in the ODL feed as non-expired.
        """
        # Arrange
        moby_dick_license = LicenseInfoHelper(
            license=LicenseHelper(
                identifier="urn:uuid:f7847120-fc6f-11e3-8158-56847afe9799",
                concurrency=10,
                checkouts=30,
                expires="2016-04-25T12:25:21+02:00",
            ),
            left=30,
            available=10,
        )

        opds2_with_odl_importer_fixture.queue_response(moby_dick_license)
        opds2_with_odl_importer_fixture.importer.ignored_identifier_types = [
            IdentifierConstants.URI
        ]

        # Act
        (
            imported_editions,
            pools,
            works,
            failures,
        ) = opds2_with_odl_importer_fixture.import_fixture_file("feed.json")

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

        assert "Feedbooks" == moby_dick_edition.data_source.name

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
        [moby_dick_license] = moby_dick_license_pool.licenses  # type: ignore
        assert (
            "urn:uuid:f7847120-fc6f-11e3-8158-56847afe9799"
            == moby_dick_license.identifier  # type: ignore
        )
        assert (
            "http://www.example.com/get{?id,checkout_id,expires,patron_id,passphrase,hint,hint_url,notification_url}"
            == moby_dick_license.checkout_url  # type: ignore
        )
        assert "http://www.example.com/status/294024" == moby_dick_license.status_url  # type: ignore
        assert (
            datetime.datetime(2016, 4, 25, 10, 25, 21, tzinfo=datetime.timezone.utc)
            == moby_dick_license.expires  # type: ignore
        )
        assert 30 == moby_dick_license.checkouts_left  # type: ignore
        assert 10 == moby_dick_license.checkouts_available  # type: ignore

        # 3. Make sure that work objects contain all the required metadata
        assert isinstance(works, list)
        assert 1 == len(works)

        [moby_dick_work] = works
        assert isinstance(moby_dick_work, Work)
        assert moby_dick_edition == moby_dick_work.presentation_edition
        assert 1 == len(moby_dick_work.license_pools)
        assert moby_dick_license_pool == moby_dick_work.license_pools[0]

        # 4. Make sure that the failure is covered
        assert 1 == len(failures)
        huck_finn_failures = failures["9781234567897"]

        assert isinstance(huck_finn_failures, list)
        assert 1 == len(huck_finn_failures)
        [huck_finn_failure] = huck_finn_failures
        assert isinstance(huck_finn_failure, CoverageFailure)
        assert "9781234567897" == huck_finn_failure.obj.identifier

        assert "2 validation errors" in huck_finn_failure.exception

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
        (
            imported_editions,
            pools,
            works,
            failures,
        ) = opds2_with_odl_importer_fixture.import_fixture_file(
            "feed-audiobook-streaming.json"
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
            failures,
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
        importer.settings = db.opds2_odl_settings(
            auth_type=auth_type,
        )
        (
            imported_editions,
            pools,
            works,
            failures,
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
        importer.settings = db.opds2_odl_settings(
            auth_type=auth_type,
        )

        (
            imported_editions,
            pools,
            works,
            failures,
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
        ) -> LicenseInfoHelper:
            return LicenseInfoHelper(
                license=LicenseHelper(
                    identifier=license_id,
                    concurrency=concurrency,
                    checkouts=checkouts,
                    expires=expires,
                ),
                left=checkouts,
                available=concurrency,
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
            failures,
        ) = opds2_with_odl_importer_fixture.importer.import_from_feed(
            json.dumps(feed_json)
        )

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
            failures,
        ) = opds2_with_odl_importer_fixture.importer.import_from_feed(
            json.dumps(feed_json)
        )

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
                LicenseInfoHelper(
                    license=LicenseHelper(
                        concurrency=1, expires="2021-01-01T00:01:00+01:00"
                    ),
                    left=52,
                    available=1,
                ),
                id="expiration_date_in_the_past",
            ),
            pytest.param(
                LicenseInfoHelper(
                    license=LicenseHelper(
                        concurrency=1,
                    ),
                    left=0,
                    available=1,
                ),
                id="left_is_zero",
            ),
            pytest.param(
                LicenseInfoHelper(
                    license=LicenseHelper(
                        concurrency=1,
                    ),
                    available=1,
                    status="unavailable",
                ),
                id="status_unavailable",
            ),
        ],
    )
    @freeze_time("2021-01-01T00:00:00+00:00")
    def test_odl_importer_expired_licenses(
        self,
        opds2_with_odl_importer_fixture: OPDS2WithODLImporterFixture,
        license: LicenseInfoHelper,
    ):
        """Ensure OPDSWithODLImporter imports expired licenses, but does not count them."""
        # Import the test feed with an expired ODL license.
        (
            imported_editions,
            imported_pools,
            imported_works,
            failures,
        ) = opds2_with_odl_importer_fixture.import_fixture_file(licenses=[license])

        # The importer created 1 edition and 1 work with no failures.
        assert failures == {}
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
        license_expiry = dateutil.parser.parse("2021-01-01T00:01:00+00:00")
        licenses = [
            LicenseInfoHelper(
                license=LicenseHelper(concurrency=1, expires=license_expiry),
                available=1,
            )
        ]

        # First import the license when it is not expired
        with freeze_time(license_expiry - datetime.timedelta(days=1)):
            # Import the test feed.
            (
                imported_editions,
                imported_pools,
                imported_works,
                failures,
            ) = opds2_with_odl_importer_fixture.import_fixture_file(licenses=licenses)

            # The importer created 1 edition and 1 work with no failures.
            assert failures == {}
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
                failures,
            ) = opds2_with_odl_importer_fixture.import_fixture_file(licenses=licenses)

            # The importer created 1 edition and 1 work with no failures.
            assert failures == {}
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
            LicenseInfoHelper(
                # Expired
                # (expiry date in the past)
                license=LicenseHelper(
                    concurrency=1,
                    expires=datetime_helpers.utc_now() - datetime.timedelta(days=1),
                ),
                available=1,
            ),
            LicenseInfoHelper(
                # Expired
                # (left is 0)
                license=LicenseHelper(concurrency=1),
                available=1,
                left=0,
            ),
            LicenseInfoHelper(
                # Expired
                # (status is unavailable)
                license=LicenseHelper(concurrency=1),
                available=1,
                status="unavailable",
            ),
        ]
        active = [
            LicenseInfoHelper(
                # Valid
                license=LicenseHelper(concurrency=1),
                available=1,
            ),
            LicenseInfoHelper(
                # Valid
                license=LicenseHelper(concurrency=5),
                available=5,
                left=40,
            ),
        ]

        (
            imported_editions,
            imported_pools,
            imported_works,
            failures,
        ) = opds2_with_odl_importer_fixture.import_fixture_file(
            licenses=active + inactive
        )

        assert failures == {}

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
        license_expiry = dateutil.parser.parse("2021-01-01T00:01:00+00:00")

        date = LicenseInfoHelper(
            license=LicenseHelper(
                concurrency=1,
                expires=license_expiry,
            ),
            available=1,
        )
        left = LicenseInfoHelper(
            license=LicenseHelper(concurrency=2), available=1, left=1
        )
        perpetual = LicenseInfoHelper(license=LicenseHelper(concurrency=1), available=0)
        licenses = [date, left, perpetual]

        # Import with all licenses valid
        with freeze_time(license_expiry - datetime.timedelta(days=1)):
            (
                imported_editions,
                imported_pools,
                imported_works,
                failures,
            ) = opds2_with_odl_importer_fixture.import_fixture_file(licenses=licenses)

            # No failures in the import
            assert failures == {}

            assert len(imported_pools) == 1

            [imported_pool] = imported_pools
            assert len(imported_pool.licenses) == 3
            assert imported_pool.licenses_available == 2
            assert imported_pool.licenses_owned == 3

            # No licenses are expired
            assert sum(not l.is_inactive for l in imported_pool.licenses) == len(
                licenses
            )

        # Expire the first two licenses

        # The first one is expired by changing the time
        with freeze_time(license_expiry + datetime.timedelta(days=1)):
            # The second one is expired by setting left to 0
            left.left = 0

            # The perpetual license has a copy available
            perpetual.available = 1

            # Reimport
            (
                imported_editions,
                imported_pools,
                imported_works,
                failures,
            ) = opds2_with_odl_importer_fixture.import_fixture_file(licenses=licenses)

            # No failures in the import
            assert failures == {}

            assert len(imported_pools) == 1

            [imported_pool] = imported_pools
            assert len(imported_pool.licenses) == 3
            assert imported_pool.licenses_available == 1
            assert imported_pool.licenses_owned == 1

            # One license not expired
            assert sum(not l.is_inactive for l in imported_pool.licenses) == 1

            # Two licenses expired
            assert sum(l.is_inactive for l in imported_pool.licenses) == 2

    def test_parse_license_info(self) -> None:
        """Ensure that OPDS2WithODLImporter correctly parses license information."""

        def license_info_dict() -> dict[str, Any]:
            return LicenseInfoHelper(available=10, license=LicenseHelper()).dict

        info_link = "http://example.org/info"
        checkout_link = "http://example.org/checkout"

        # All fields present
        expiry = utc_now() + datetime.timedelta(days=1)
        license_helper = LicenseInfoHelper(
            available=10, left=4, license=LicenseHelper(concurrency=11, expires=expiry)
        )
        license_dict = license_helper.dict
        parsed = OPDS2WithODLImporter.parse_license_info(
            json.dumps(license_dict), info_link, checkout_link
        )
        assert parsed.checkouts_available == 10
        assert parsed.checkouts_left == 4
        assert parsed.terms_concurrency == 11
        assert parsed.expires == expiry
        assert parsed.status == LicenseStatus.available
        assert parsed.identifier == license_helper.license.identifier

        # No identifier
        license_dict = license_info_dict()
        license_dict.pop("identifier")
        assert (
            OPDS2WithODLImporter.parse_license_info(
                json.dumps(license_dict), info_link, checkout_link
            )
            is None
        )

        # No status
        license_dict = license_info_dict()
        license_dict.pop("status")
        assert (
            OPDS2WithODLImporter.parse_license_info(
                json.dumps(license_dict), info_link, checkout_link
            )
            is None
        )

        # Bad status
        license_dict = license_info_dict()
        license_dict["status"] = "bad"
        assert (
            OPDS2WithODLImporter.parse_license_info(
                json.dumps(license_dict), info_link, checkout_link
            )
            is None
        )

        # No available
        license_dict = license_info_dict()
        license_dict["checkouts"].pop("available")
        assert (
            OPDS2WithODLImporter.parse_license_info(
                json.dumps(license_dict), info_link, checkout_link
            )
            is None
        )

        # No concurrency
        license_dict = license_info_dict()
        license_dict["terms"].pop("concurrency")
        parsed = OPDS2WithODLImporter.parse_license_info(
            json.dumps(license_dict), info_link, checkout_link
        )
        assert parsed.terms_concurrency is None

        # Format str
        license_dict = license_info_dict()
        license_dict["format"] = "single format"
        parsed = OPDS2WithODLImporter.parse_license_info(
            json.dumps(license_dict), info_link, checkout_link
        )
        assert parsed.content_types == ("single format",)

        # Format list
        license_dict = license_info_dict()
        license_dict["format"] = ["format1", "format2"]
        parsed = OPDS2WithODLImporter.parse_license_info(
            json.dumps(license_dict), info_link, checkout_link
        )
        assert parsed.content_types == ("format1", "format2")

    def test_fetch_license_info(self, http_client: MockHttpClientFixture):
        """Ensure that OPDS2WithODLImporter correctly retrieves license data from an OPDS2 feed."""

        # Bad status code
        http_client.queue_response(400, content=b"Bad Request")

        assert (
            OPDS2WithODLImporter.fetch_license_info(
                "http://example.org/feed", http_client.do_get
            )
            is None
        )
        assert len(http_client.requests) == 1
        assert http_client.requests.pop() == "http://example.org/feed"

        # 200 status - directly returns response body
        content = b"data"
        http_client.queue_response(200, content=content)
        assert (
            OPDS2WithODLImporter.fetch_license_info(
                "http://example.org/feed", http_client.do_get
            )
            == content
        )
        assert len(http_client.requests) == 1
        assert http_client.requests.pop() == "http://example.org/feed"

        # 201 status - directly returns response body
        http_client.queue_response(201, content=content)
        assert (
            OPDS2WithODLImporter.fetch_license_info(
                "http://example.org/feed", http_client.do_get
            )
            == content
        )
        assert len(http_client.requests) == 1
        assert http_client.requests.pop() == "http://example.org/feed"

    def test_get_license_data(self, monkeypatch: pytest.MonkeyPatch):
        expires = utc_now() + datetime.timedelta(days=1)

        responses: list[tuple[int, str]] = []

        def get(url: str, *args: Any, **kwargs: Any) -> Response:
            status_code, body = responses.pop(0)
            resp = Response()
            resp.status_code = status_code
            resp._content = body.encode("utf-8")
            return resp

        def get_license_data() -> LicenseData | None:
            return OPDS2WithODLImporter.get_license_data(
                "license_info_link",
                "checkout_link",
                "identifier",
                expires,
                12,
                get,
            )

        # Bad status code returns None
        responses.append((400, "Bad Request"))
        assert get_license_data() is None

        # Bad data returns None
        responses.append((200, "{}"))
        assert get_license_data() is None

        # Identifier mismatch returns None
        responses.append(
            (
                200,
                LicenseInfoHelper(
                    available=10, license=LicenseHelper(identifier="other")
                ).json,
            )
        )
        assert get_license_data() is None

        # Expiry mismatch makes license unavailable
        responses.append(
            (
                200,
                LicenseInfoHelper(
                    available=10,
                    license=LicenseHelper(
                        identifier="identifier",
                        concurrency=12,
                        expires=expires + datetime.timedelta(minutes=1),
                    ),
                ).json,
            )
        )
        license_data = get_license_data()
        assert license_data is not None
        assert license_data.status == LicenseStatus.unavailable

        # Concurrency mismatch makes license unavailable
        responses.append(
            (
                200,
                LicenseInfoHelper(
                    available=10,
                    license=LicenseHelper(
                        identifier="identifier", concurrency=11, expires=expires
                    ),
                ).json,
            )
        )
        license_data = get_license_data()
        assert license_data is not None
        assert license_data.status == LicenseStatus.unavailable

        # Good data returns LicenseData
        responses.append(
            (
                200,
                LicenseInfoHelper(
                    available=10,
                    license=LicenseHelper(
                        identifier="identifier", concurrency=12, expires=expires
                    ),
                ).json,
            )
        )
        license_data = get_license_data()
        assert license_data is not None
        assert license_data.status == LicenseStatus.available
        assert license_data.checkouts_available == 10
        assert license_data.expires == expires
        assert license_data.identifier == "identifier"
        assert license_data.terms_concurrency == 12


class OPDS2WithODLImportMonitorFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.feed_url = "https://opds.import.com:9999/feed"
        self.username = "username"
        self.password = "password"
        self.collection = db.collection(
            protocol=OPDS2WithODLApi,
            settings=OPDS2WithODLSettings(
                external_account_id=self.feed_url,
                username=self.username,
                password=self.password,
                data_source="OPDS",
            ),
        )
        self.monitor = OPDS2WithODLImportMonitor(
            db.session, self.collection, OPDS2WithODLImporter
        )


@pytest.fixture
def opds2_with_odl_import_monitor_fixture(
    db: DatabaseTransactionFixture,
) -> OPDS2WithODLImportMonitorFixture:
    return OPDS2WithODLImportMonitorFixture(db)


class TestOPDS2WithODLImportMonitor:
    def test_get(
        self,
        opds2_with_odl_import_monitor_fixture: OPDS2WithODLImportMonitorFixture,
    ):
        monitor = opds2_with_odl_import_monitor_fixture.monitor

        with patch.object(HTTP, "request_with_timeout") as mock_get:
            monitor._get("/absolute/path", {})
            assert mock_get.call_args.args == (
                "GET",
                "https://opds.import.com:9999/absolute/path",
            )

        with patch.object(HTTP, "request_with_timeout") as mock_get:
            monitor._get("relative/path", {})
            assert mock_get.call_args.args == (
                "GET",
                "https://opds.import.com:9999/relative/path",
            )

        with patch.object(HTTP, "request_with_timeout") as mock_get:
            monitor._get("http://example.com/full/url")
            assert mock_get.call_args.args == ("GET", "http://example.com/full/url")
            # assert that we set the expected extra args to the HTTP request
            kwargs = mock_get.call_args.kwargs
            assert kwargs.get("timeout") == 120
            assert kwargs.get("max_retry_count") == monitor._max_retry_count
            assert kwargs.get("allowed_response_codes") == ["2xx", "3xx"]

    def test_properties(
        self,
        opds2_with_odl_import_monitor_fixture: OPDS2WithODLImportMonitorFixture,
    ):
        monitor = opds2_with_odl_import_monitor_fixture.monitor

        assert monitor._username == opds2_with_odl_import_monitor_fixture.username
        assert monitor._password == opds2_with_odl_import_monitor_fixture.password
        assert monitor._auth_type == OPDS2AuthType.BASIC
        assert monitor._feed_url == opds2_with_odl_import_monitor_fixture.feed_url

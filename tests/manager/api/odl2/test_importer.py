from __future__ import annotations

import copy
import datetime
import functools
import json
import uuid

import dateutil
import pytest
from freezegun import freeze_time
from webpub_manifest_parser.odl.ast import ODLPublication
from webpub_manifest_parser.odl.semantic import (
    ODL_PUBLICATION_MUST_CONTAIN_EITHER_LICENSES_OR_OA_ACQUISITION_LINK_ERROR,
)
from webpub_manifest_parser.opds2.ast import OPDS2PublicationMetadata

from palace.manager.api.odl2.reaper import ODL2HoldReaper
from palace.manager.core.coverage import CoverageFailure
from palace.manager.sqlalchemy.constants import (
    EditionConstants,
    IdentifierConstants,
    MediaTypes,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.contributor import Contribution, Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicenseStatus,
)
from palace.manager.sqlalchemy.model.patron import Hold
from palace.manager.sqlalchemy.model.resource import Hyperlink
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util import datetime_helpers
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.odl2 import (
    LicenseHelper,
    LicenseInfoHelper,
    ODL2APIFixture,
    ODL2ImporterFixture,
)


class TestODL2Importer:
    @freeze_time("2016-01-01T00:00:00+00:00")
    def test_import(
        self,
        odl2_importer_fixture: ODL2ImporterFixture,
    ) -> None:
        """Ensure that ODL2Importer2 correctly processes and imports the ODL feed encoded using OPDS 2.x.

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

        odl2_importer_fixture.queue_response(moby_dick_license)

        config = odl2_importer_fixture.collection.integration_configuration
        odl2_importer_fixture.importer.ignored_identifier_types = [
            IdentifierConstants.URI
        ]
        DatabaseTransactionFixture.set_settings(
            config, odl2_skipped_license_formats=["text/html"]
        )

        # Act
        (
            imported_editions,
            pools,
            works,
            failures,
        ) = odl2_importer_fixture.import_fixture_file("feed.json")

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
        assert Contributor.AUTHOR_ROLE == moby_dick_author_author_contribution.role

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
        assert 30 == moby_dick_license_pool.licenses_owned
        assert 10 == moby_dick_license_pool.licenses_available

        assert 2 == len(moby_dick_license_pool.delivery_mechanisms)

        moby_dick_epub_adobe_drm_delivery_mechanism = (
            odl2_importer_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
                moby_dick_license_pool.delivery_mechanisms,
                MediaTypes.EPUB_MEDIA_TYPE,
                DeliveryMechanism.ADOBE_DRM,
            )
        )
        assert moby_dick_epub_adobe_drm_delivery_mechanism is not None

        moby_dick_epub_lcp_drm_delivery_mechanism = (
            odl2_importer_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
                moby_dick_license_pool.delivery_mechanisms,
                MediaTypes.EPUB_MEDIA_TYPE,
                DeliveryMechanism.LCP_DRM,
            )
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

        huck_finn_semantic_error = (
            ODL_PUBLICATION_MUST_CONTAIN_EITHER_LICENSES_OR_OA_ACQUISITION_LINK_ERROR(
                node=ODLPublication(
                    metadata=OPDS2PublicationMetadata(
                        identifier="urn:isbn:9781234567897"
                    )
                ),
                node_property=None,
            )
        )
        assert str(huck_finn_semantic_error) == huck_finn_failure.exception

    @freeze_time("2016-01-01T00:00:00+00:00")
    def test_import_audiobook_with_streaming(
        self,
        db: DatabaseTransactionFixture,
        odl2_importer_fixture: ODL2ImporterFixture,
    ) -> None:
        """Ensure that ODL2Importer2 correctly processes and imports a feed with an audiobook."""
        odl2_importer_fixture.queue_fixture_file("license-audiobook.json")

        db.set_settings(
            odl2_importer_fixture.collection.integration_configuration,
            odl2_skipped_license_formats=["text/html"],
        )

        (
            imported_editions,
            pools,
            works,
            failures,
        ) = odl2_importer_fixture.import_fixture_file("feed-audiobook-streaming.json")

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

        lcp_delivery_mechanism = (
            odl2_importer_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
                license_pool.delivery_mechanisms,
                MediaTypes.AUDIOBOOK_PACKAGE_LCP_MEDIA_TYPE,
                DeliveryMechanism.LCP_DRM,
            )
        )
        assert lcp_delivery_mechanism is not None

        feedbooks_delivery_mechanism = (
            odl2_importer_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
                license_pool.delivery_mechanisms,
                MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
                DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM,
            )
        )
        assert feedbooks_delivery_mechanism is not None

    @freeze_time("2016-01-01T00:00:00+00:00")
    def test_import_audiobook_no_streaming(
        self,
        odl2_importer_fixture: ODL2ImporterFixture,
    ) -> None:
        """
        Ensure that ODL2Importer2 correctly processes and imports a feed with an audiobook
        that is not available for streaming.
        """
        odl2_importer_fixture.queue_fixture_file("license-audiobook.json")

        (
            imported_editions,
            pools,
            works,
            failures,
        ) = odl2_importer_fixture.import_fixture_file(
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

        lcp_delivery_mechanism = (
            odl2_importer_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
                license_pool.delivery_mechanisms,
                MediaTypes.AUDIOBOOK_PACKAGE_LCP_MEDIA_TYPE,
                DeliveryMechanism.LCP_DRM,
            )
        )
        assert lcp_delivery_mechanism is not None

    @freeze_time("2016-01-01T00:00:00+00:00")
    def test_import_open_access(
        self,
        odl2_importer_fixture: ODL2ImporterFixture,
    ) -> None:
        """
        Ensure that ODL2Importer2 correctly processes and imports a feed with an
        open access book.
        """
        (
            imported_editions,
            pools,
            works,
            failures,
        ) = odl2_importer_fixture.import_fixture_file("oa-title.json")

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

        assert 1 == len(license_pool.delivery_mechanisms)

        oa_ebook_delivery_mechanism = (
            odl2_importer_fixture.get_delivery_mechanism_by_drm_scheme_and_content_type(
                license_pool.delivery_mechanisms,
                MediaTypes.EPUB_MEDIA_TYPE,
                None,
            )
        )
        assert oa_ebook_delivery_mechanism is not None

    @freeze_time("2016-01-01T00:00:00+00:00")
    def test_import_availability(
        self,
        odl2_importer_fixture: ODL2ImporterFixture,
    ) -> None:
        feed_json = json.loads(odl2_importer_fixture.files.sample_text("feed.json"))

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

        odl2_importer_fixture.queue_response(license_status_reply(MOBY_DICK_LICENSE_ID))
        odl2_importer_fixture.queue_response(license_status_reply(HUCK_FINN_LICENSE_ID))

        (
            imported_editions,
            pools,
            works,
            failures,
        ) = odl2_importer_fixture.importer.import_from_feed(json.dumps(feed_json))

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
            licenses_owned=30,
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
            licenses_owned=30,
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
        odl2_importer_fixture.queue_response(
            license_status_reply(TEST_BOOK_LICENSE_ID, checkouts=None, expires=None)
        )

        # Harvest the feed again
        (
            imported_editions,
            pools,
            works,
            failures,
        ) = odl2_importer_fixture.importer.import_from_feed(json.dumps(feed_json))

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
        odl2_importer_fixture: ODL2ImporterFixture,
        license: LicenseInfoHelper,
    ):
        """Ensure ODLImporter imports expired licenses, but does not count them."""
        # Import the test feed with an expired ODL license.
        (
            imported_editions,
            imported_pools,
            imported_works,
            failures,
        ) = odl2_importer_fixture.import_fixture_file(licenses=[license])

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
        odl2_importer_fixture: ODL2ImporterFixture,
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
            ) = odl2_importer_fixture.import_fixture_file(licenses=licenses)

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
            ) = odl2_importer_fixture.import_fixture_file(licenses=licenses)

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
        odl2_importer_fixture: ODL2ImporterFixture,
    ):
        """Ensure ODLImporter imports expired licenses
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
        ) = odl2_importer_fixture.import_fixture_file(licenses=active + inactive)

        assert failures == {}

        # License pool was successfully created
        assert len(imported_pools) == 1
        [imported_pool] = imported_pools

        # All licenses were imported
        assert len(imported_pool.licenses) == 5

        # Make sure that the license statistics are correct and include only active licenses.
        assert imported_pool.licenses_owned == 41
        assert imported_pool.licenses_available == 6

        # Correct number of active and inactive licenses
        assert sum(not l.is_inactive for l in imported_pool.licenses) == len(active)
        assert sum(l.is_inactive for l in imported_pool.licenses) == len(inactive)

    def test_odl_importer_reimport_multiple_licenses(
        self,
        odl2_importer_fixture: ODL2ImporterFixture,
    ):
        """Ensure ODLImporter correctly imports licenses that have already been imported."""

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
            license=LicenseHelper(concurrency=2), available=1, left=5
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
            ) = odl2_importer_fixture.import_fixture_file(licenses=licenses)

            # No failures in the import
            assert failures == {}

            assert len(imported_pools) == 1

            [imported_pool] = imported_pools
            assert len(imported_pool.licenses) == 3
            assert imported_pool.licenses_available == 2
            assert imported_pool.licenses_owned == 7

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
            ) = odl2_importer_fixture.import_fixture_file(licenses=licenses)

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


class TestODL2HoldReaper:
    def test_run_once(
        self, odl2_api_fixture: ODL2APIFixture, db: DatabaseTransactionFixture
    ):
        collection = odl2_api_fixture.collection
        work = odl2_api_fixture.work
        license = odl2_api_fixture.setup_license(work, concurrency=3, available=3)
        api = odl2_api_fixture.api
        pool = license.license_pool

        data_source = DataSource.lookup(db.session, "Feedbooks", autocreate=True)
        DatabaseTransactionFixture.set_settings(
            collection.integration_configuration,
            **{Collection.DATA_SOURCE_NAME_SETTING: data_source.name},
        )
        reaper = ODL2HoldReaper(db.session, collection, api=api)

        now = utc_now()
        yesterday = now - datetime.timedelta(days=1)

        expired_hold1, ignore = pool.on_hold_to(db.patron(), end=yesterday, position=0)
        expired_hold2, ignore = pool.on_hold_to(db.patron(), end=yesterday, position=0)
        expired_hold3, ignore = pool.on_hold_to(db.patron(), end=yesterday, position=0)
        current_hold, ignore = pool.on_hold_to(db.patron(), position=3)
        # This hold has an end date in the past, but its position is greater than 0
        # so the end date is not reliable.
        bad_end_date, ignore = pool.on_hold_to(db.patron(), end=yesterday, position=4)

        progress = reaper.run_once(reaper.timestamp().to_data())

        # The expired holds have been deleted and the other holds have been updated.
        assert 2 == db.session.query(Hold).count()
        assert [current_hold, bad_end_date] == db.session.query(Hold).order_by(
            Hold.start
        ).all()
        assert 0 == current_hold.position
        assert 0 == bad_end_date.position
        assert current_hold.end > now
        assert bad_end_date.end > now
        assert 1 == pool.licenses_available
        assert 2 == pool.licenses_reserved

        # The TimestampData returned reflects what work was done.
        assert "Holds deleted: 3. License pools updated: 1" == progress.achievements

        # The TimestampData does not include any timing information --
        # that will be applied by run().
        assert None == progress.start
        assert None == progress.finish

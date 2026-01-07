from __future__ import annotations

import datetime
from functools import partial

import pytest

from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.license.opds.odl.constants import FEEDBOOKS_AUDIO
from palace.manager.integration.license.opds.odl.extractor import OPDS2WithODLExtractor
from palace.manager.opds import opds2, rwpm
from palace.manager.opds.lcp.status import LoanStatus
from palace.manager.opds.odl.info import Checkouts, LicenseInfo, LicenseStatus
from palace.manager.opds.odl.odl import License, LicenseMetadata, Publication
from palace.manager.opds.odl.protection import Protection
from palace.manager.opds.odl.terms import Terms
from palace.manager.opds.opds2 import PublicationFeedNoValidation, StrictLink
from palace.manager.sqlalchemy.constants import EditionConstants, MediaTypes
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.files import OPDS2FilesFixture


class TestOPDS2WithODLExtractor:
    def test__extract_odl_license_data(self) -> None:
        create_metadata = partial(
            LicenseMetadata,
            identifier="identifier",
            created=utc_now(),
        )

        links = [
            StrictLink(
                rel=rwpm.LinkRelations.self,
                type=LicenseInfo.content_type(),
                href="self link",
            ),
            StrictLink(
                rel=opds2.AcquisitionLinkRelations.borrow,
                type=LoanStatus.content_type(),
                href="checkout link",
            ),
        ]

        create_license = partial(
            License,
            metadata=create_metadata(),
            links=links,
        )

        create_license_info = partial(
            LicenseInfo,
            identifier="identifier",
            status=LicenseStatus.available,
            checkouts=Checkouts(
                available=10,
            ),
        )

        # Expiry mismatch makes license unavailable
        license_info = create_license_info(
            terms=Terms(
                expires=utc_now() + datetime.timedelta(days=1),
            )
        )
        license = create_license(
            metadata=create_metadata(
                terms=Terms(
                    expires=utc_now() + datetime.timedelta(days=2),
                )
            ),
        )
        license_data = OPDS2WithODLExtractor._extract_odl_license_data(
            license_info, license
        )
        assert license_data is not None
        assert license_data.status == LicenseStatus.unavailable

        # Concurrency mismatch makes license unavailable
        license_info = create_license_info(terms=Terms(concurrency=12))
        license = create_license(
            metadata=create_metadata(terms=Terms(concurrency=11)),
        )
        license_data = OPDS2WithODLExtractor._extract_odl_license_data(
            license_info, license
        )
        assert license_data is not None
        assert license_data.status == LicenseStatus.unavailable

        # Good data returns LicenseData
        license_info = create_license_info()
        license = create_license()
        license_data = OPDS2WithODLExtractor._extract_odl_license_data(
            license_info, license
        )
        assert license_data is not None
        assert license_data.status == LicenseStatus.available

    def test__extract_contributor_roles(self) -> None:
        _extract_contributor_roles = OPDS2WithODLExtractor._extract_contributor_roles

        # If there are no roles, the function returns the default
        assert _extract_contributor_roles([], Contributor.Role.AUTHOR) == [
            Contributor.Role.AUTHOR
        ]

        # If the role is unknown, the default is used
        assert _extract_contributor_roles(["invalid"], Contributor.Role.AUTHOR) == [
            Contributor.Role.AUTHOR
        ]

        # Roles are not duplicated
        assert _extract_contributor_roles(
            [Contributor.Role.AUTHOR, Contributor.Role.AUTHOR], Contributor.Role.AUTHOR
        ) == [Contributor.Role.AUTHOR]
        assert _extract_contributor_roles(
            ["invalid", "invalid"], Contributor.Role.AUTHOR
        ) == [Contributor.Role.AUTHOR]

        # Role lookup is not case-sensitive
        assert _extract_contributor_roles(["aUtHoR"], Contributor.Role.ILLUSTRATOR) == [
            Contributor.Role.AUTHOR
        ]

        # Roles can be looked up via marc codes
        assert _extract_contributor_roles(["AUT"], Contributor.Role.ILLUSTRATOR) == [
            Contributor.Role.AUTHOR
        ]

    @pytest.mark.parametrize(
        "published,expected",
        [
            pytest.param(
                datetime.datetime(2015, 9, 29, 17, 0, tzinfo=datetime.UTC),
                datetime.date(2015, 9, 29),
                id="datetime with time info",
            ),
            pytest.param(
                datetime.datetime(2015, 9, 29, 0, 0),
                datetime.date(2015, 9, 29),
                id="datetime with no time info",
            ),
            pytest.param(
                datetime.date(2015, 9, 29),
                datetime.date(2015, 9, 29),
                id="date",
            ),
            pytest.param(
                None,
                None,
                id="none",
            ),
        ],
    )
    def test__extract_published_date(
        self,
        published: datetime.datetime | datetime.date | None,
        expected: datetime.date | None,
    ) -> None:
        assert OPDS2WithODLExtractor._extract_published_date(published) == expected

    def test_feed_next_url(
        self,
        opds2_files_fixture: OPDS2FilesFixture,
    ) -> None:
        # No next links
        feed = PublicationFeedNoValidation.model_validate_json(
            opds2_files_fixture.sample_data("feed.json")
        )
        assert OPDS2WithODLExtractor.feed_next_url(feed) is None

        # Feed has next link
        feed = PublicationFeedNoValidation.model_validate_json(
            opds2_files_fixture.sample_data("feed2.json")
        )
        assert (
            OPDS2WithODLExtractor.feed_next_url(feed)
            == "http://bookshelf-feed-demo.us-east-1.elasticbeanstalk.com/v1/publications?page=2&limit=100"
        )

    @pytest.mark.parametrize(
        "medium, license_format, expected_drm, expected_content_type",
        [
            pytest.param(
                EditionConstants.AUDIO_MEDIUM,
                FEEDBOOKS_AUDIO,
                DeliveryMechanism.FEEDBOOKS_AUDIOBOOK_DRM,
                MediaTypes.AUDIOBOOK_MANIFEST_MEDIA_TYPE,
                id="feedbooks-audio",
            ),
            pytest.param(
                EditionConstants.AUDIO_MEDIUM,
                MediaTypes.TEXT_HTML_MEDIA_TYPE,
                DeliveryMechanism.STREAMING_DRM,
                DeliveryMechanism.STREAMING_AUDIO_CONTENT_TYPE,
                id="audio-html",
            ),
            pytest.param(
                EditionConstants.BOOK_MEDIUM,
                MediaTypes.TEXT_HTML_MEDIA_TYPE,
                DeliveryMechanism.STREAMING_DRM,
                DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                id="text-html",
            ),
            pytest.param(
                None,
                MediaTypes.TEXT_HTML_MEDIA_TYPE,
                DeliveryMechanism.STREAMING_DRM,
                DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                id="none-medium-defaults-to-text",
            ),
        ],
    )
    def test__extract_odl_circulation_data_license_formats_mapping(
        self,
        medium: str | None,
        license_format: str,
        expected_drm: str | None,
        expected_content_type: str,
    ) -> None:
        """
        Test that special license formats (FEEDBOOKS_AUDIO, text/html) are correctly
        mapped to their appropriate content types and DRM schemes based on the
        publication medium, ignoring any protection formats specified in the license.
        """
        # Create a minimal ODL publication with a license that has:
        # - A special format (e.g., FEEDBOOKS_AUDIO or "text/html").
        # - A protection field with multiple DRM schemes to verify they're ignored.
        license_identifier = "test-license-123"
        publication_identifier = "urn:isbn:9780306406157"

        license_links = [
            StrictLink(
                rel=rwpm.LinkRelations.self,
                type=LicenseInfo.content_type(),
                href="http://example.org/license",
            ),
            StrictLink(
                rel=opds2.AcquisitionLinkRelations.borrow,
                type=LoanStatus.content_type(),
                href="http://example.org/borrow",
            ),
        ]

        # Create a license with the format we're testing
        # and a protection field with multiple DRM schemes
        license = License(
            metadata=LicenseMetadata(
                identifier=license_identifier,
                created=utc_now(),
                format=license_format,
                terms=Terms(concurrency=1),
                protection=Protection(
                    # These should be IGNORED for mapped formats
                    format=[
                        DeliveryMechanism.LCP_DRM,
                        DeliveryMechanism.ADOBE_DRM,
                    ]
                ),
            ),
            links=license_links,
        )

        publication = Publication(
            metadata=opds2.PublicationMetadata(
                type="http://schema.org/Book",
                identifier=publication_identifier,
                title="Test Book",
            ),
            images=[
                opds2.Link(
                    href="http://example.org/cover.jpg",
                    type="image/jpeg",
                )
            ],
            links=[],
            licenses=[license],
        )

        extractor: OPDS2WithODLExtractor[Publication] = OPDS2WithODLExtractor(
            parse_publication=lambda x: x,  # type: ignore[arg-type, return-value]
            base_url="http://example.org",
            data_source="Test Source",
        )
        license_info = LicenseInfo(
            identifier=license_identifier,
            status=LicenseStatus.available,
            checkouts=Checkouts(available=1),
            terms=Terms(concurrency=1),
            format=license_format,
        )
        identifier_data = IdentifierData.parse_urn(publication_identifier)
        circulation_data = extractor._extract_odl_circulation_data(
            publication=publication,
            license_info_documents={license_identifier: license_info},
            identifier=identifier_data,
            medium=medium,
        )

        # Ensure that we got one correct format.
        [format_data] = circulation_data.formats
        assert format_data.content_type == expected_content_type
        assert format_data.drm_scheme == expected_drm

    @pytest.mark.parametrize(
        "medium, license_format, skipped_formats",
        [
            pytest.param(
                EditionConstants.AUDIO_MEDIUM,
                FEEDBOOKS_AUDIO,
                {FEEDBOOKS_AUDIO},
                id="skip-feedbooks-audio",
            ),
            pytest.param(
                EditionConstants.AUDIO_MEDIUM,
                MediaTypes.TEXT_HTML_MEDIA_TYPE,
                {DeliveryMechanism.STREAMING_AUDIO_CONTENT_TYPE},
                id="skip-streaming-audio",
            ),
            pytest.param(
                EditionConstants.BOOK_MEDIUM,
                MediaTypes.TEXT_HTML_MEDIA_TYPE,
                {DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE},
                id="skip-streaming-text",
            ),
            pytest.param(
                EditionConstants.AUDIO_MEDIUM,
                MediaTypes.TEXT_HTML_MEDIA_TYPE,
                {MediaTypes.TEXT_HTML_MEDIA_TYPE},
                id="skip-text-html",
            ),
        ],
    )
    def test__extract_odl_circulation_data_skipped_license_formats(
        self,
        medium: str,
        license_format: str,
        skipped_formats: set[str],
    ) -> None:
        """
        Test that skipped license formats are omitted from circulation data formats.
        """
        license_identifier = "test-license-123"
        publication_identifier = "urn:isbn:9780306406157"

        license_links = [
            StrictLink(
                rel=rwpm.LinkRelations.self,
                type=LicenseInfo.content_type(),
                href="http://example.org/license",
            ),
            StrictLink(
                rel=opds2.AcquisitionLinkRelations.borrow,
                type=LoanStatus.content_type(),
                href="http://example.org/borrow",
            ),
        ]

        license = License(
            metadata=LicenseMetadata(
                identifier=license_identifier,
                created=utc_now(),
                format=license_format,
                terms=Terms(concurrency=1),
                protection=Protection(
                    format=[
                        DeliveryMechanism.LCP_DRM,
                        DeliveryMechanism.ADOBE_DRM,
                    ]
                ),
            ),
            links=license_links,
        )

        publication = Publication(
            metadata=opds2.PublicationMetadata(
                type="http://schema.org/Book",
                identifier=publication_identifier,
                title="Test Book",
            ),
            images=[
                opds2.Link(
                    href="http://example.org/cover.jpg",
                    type="image/jpeg",
                )
            ],
            links=[],
            licenses=[license],
        )

        extractor: OPDS2WithODLExtractor[Publication] = OPDS2WithODLExtractor(
            parse_publication=lambda x: x,  # type: ignore[arg-type, return-value]
            base_url="http://example.org",
            data_source="Test Source",
            skipped_license_formats=skipped_formats,
        )
        license_info = LicenseInfo(
            identifier=license_identifier,
            status=LicenseStatus.available,
            checkouts=Checkouts(available=1),
            terms=Terms(concurrency=1),
            format=license_format,
        )
        identifier_data = IdentifierData.parse_urn(publication_identifier)
        circulation_data = extractor._extract_odl_circulation_data(
            publication=publication,
            license_info_documents={license_identifier: license_info},
            identifier=identifier_data,
            medium=medium,
        )

        assert circulation_data.formats == []
        assert circulation_data.licenses is not None
        assert len(circulation_data.licenses) == 1

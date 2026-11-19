from __future__ import annotations

import datetime
import logging
from unittest.mock import MagicMock

import pytest

from palace.opds import opds2, rwpm
from palace.opds.lcp.status import LoanStatus
from palace.opds.odl.info import Checkouts, LicenseInfo, LicenseStatus
from palace.opds.odl.odl import License, LicenseMetadata, Publication
from palace.opds.odl.protection import Protection
from palace.opds.odl.terms import Terms
from palace.opds.opds2 import Link, PublicationFeedNoValidation
from palace.opds.schema_org import Audience
from palace.util.datetime_helpers import utc_now

from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.license.opds.odl.constants import FEEDBOOKS_AUDIO
from palace.manager.integration.license.opds.odl.extractor import OPDS2WithODLExtractor
from palace.manager.sqlalchemy.constants import EditionConstants, MediaTypes
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism, RightsStatus
from palace.manager.sqlalchemy.model.periodical import SerialType
from tests.fixtures.files import OPDS2FilesFixture, PeriodicalFilesFixture


class ODLExtractorTestFixture:
    """Factory functions for creating ODL extractor test objects."""

    def __init__(self) -> None:
        self.license_identifier: str = "test-license-123"
        self.publication_identifier: str = "urn:isbn:9780306406157"

    def license_links(self) -> list[Link]:
        """Create standard license links for testing."""
        return [
            Link(
                rel=rwpm.LinkRelations.self,
                type=LicenseInfo.content_type(),
                href="http://example.org/license",
            ),
            Link(
                rel=opds2.AcquisitionLinkRelations.borrow,
                type=LoanStatus.content_type(),
                href="http://example.org/borrow",
            ),
        ]

    def license(
        self,
        license_format: str = "",
        protection: Protection | None = None,
        terms: Terms | None = None,
    ) -> License:
        """Create a License with the given format, protection, and terms."""
        if protection is None:
            protection = Protection(
                format=[DeliveryMechanism.LCP_DRM, DeliveryMechanism.ADOBE_DRM]
            )
        if terms is None:
            terms = Terms(concurrency=1)
        return License(
            metadata=LicenseMetadata(
                identifier=self.license_identifier,
                created=utc_now(),
                format=license_format,
                terms=terms,
                protection=protection,
            ),
            links=self.license_links(),
        )

    def publication(self, license: License) -> Publication:
        """Create a Publication with the given license."""
        return Publication(
            metadata=opds2.PublicationMetadata(
                type="http://schema.org/Book",
                identifier=self.publication_identifier,
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

    def opds2_publication(
        self,
        type: str = "http://schema.org/Book",
        title: str = "Test Book",
        belongs_to: rwpm.BelongsTo | None = None,
    ) -> opds2.Publication:
        """Create a plain OPDS 2.0 Publication, with no ODL licenses."""
        return opds2.Publication(
            metadata=opds2.PublicationMetadata(
                type=type,
                identifier=self.publication_identifier,
                title=title,
                belongs_to=belongs_to or rwpm.BelongsTo(),
            ),
            images=[
                opds2.Link(
                    href="http://example.org/cover.jpg",
                    type="image/jpeg",
                )
            ],
            links=[
                opds2.Link(
                    rel=opds2.AcquisitionLinkRelations.borrow,
                    href="http://example.org/borrow",
                    type="application/webpub+json",
                )
            ],
        )

    def extractor(
        self,
        skipped_license_formats: set[str] | None = None,
    ) -> OPDS2WithODLExtractor[Publication]:
        """Create an OPDS2WithODLExtractor for testing."""
        return OPDS2WithODLExtractor(
            parse_publication=MagicMock(),
            base_url="http://example.org",
            data_source="Test Source",
            skipped_license_formats=skipped_license_formats,
        )

    def license_info(
        self,
        license_format: str = "",
        terms: Terms | None = None,
        checkouts: Checkouts | None = None,
    ) -> LicenseInfo:
        """Create a LicenseInfo document for testing."""
        if terms is None:
            terms = Terms(concurrency=1)
        if checkouts is None:
            checkouts = Checkouts(available=1)
        return LicenseInfo(
            identifier=self.license_identifier,
            status=LicenseStatus.available,
            checkouts=checkouts,
            terms=terms,
            format=license_format,
        )

    def identifier_data(self) -> IdentifierData:
        """Create IdentifierData from the publication identifier."""
        return IdentifierData.parse_urn(self.publication_identifier)


@pytest.fixture(scope="class")
def odl_extractor_fixture() -> ODLExtractorTestFixture:
    """Fixture providing factory functions for ODL extractor tests."""
    return ODLExtractorTestFixture()


class TestOPDS2WithODLExtractor:
    def test__extract_odl_license_data(
        self,
        odl_extractor_fixture: ODLExtractorTestFixture,
    ) -> None:
        # Expiry mismatch makes license unavailable
        info_terms = Terms(expires=utc_now() + datetime.timedelta(days=1))
        license_terms = Terms(expires=utc_now() + datetime.timedelta(days=2))
        license_info = odl_extractor_fixture.license_info(terms=info_terms)
        odl_license = odl_extractor_fixture.license(terms=license_terms)

        license_data = OPDS2WithODLExtractor._extract_odl_license_data(
            license_info, odl_license
        )
        assert license_data is not None
        assert license_data.status == LicenseStatus.unavailable

        # Concurrency mismatch makes license unavailable
        license_info = odl_extractor_fixture.license_info(terms=Terms(concurrency=12))
        odl_license = odl_extractor_fixture.license(terms=Terms(concurrency=11))

        license_data = OPDS2WithODLExtractor._extract_odl_license_data(
            license_info, odl_license
        )
        assert license_data is not None
        assert license_data.status == LicenseStatus.unavailable

        # Good data returns LicenseData
        license_info = odl_extractor_fixture.license_info()
        odl_license = odl_extractor_fixture.license()

        license_data = OPDS2WithODLExtractor._extract_odl_license_data(
            license_info, odl_license
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
        odl_extractor_fixture: ODLExtractorTestFixture,
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
        # The fixture's default protection includes multiple DRM schemes that should
        # be IGNORED for these special mapped formats.
        odl_license = odl_extractor_fixture.license(license_format)
        publication = odl_extractor_fixture.publication(odl_license)
        extractor = odl_extractor_fixture.extractor()

        circulation_data = extractor._extract_odl_circulation_data(
            publication=publication,
            license_info_documents={
                odl_extractor_fixture.license_identifier: odl_extractor_fixture.license_info(
                    license_format
                )
            },
            identifier=odl_extractor_fixture.identifier_data(),
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
        odl_extractor_fixture: ODLExtractorTestFixture,
        medium: str,
        license_format: str,
        skipped_formats: set[str],
    ) -> None:
        """
        Test that skipped license formats are omitted from circulation data formats.
        """
        odl_license = odl_extractor_fixture.license(license_format)
        publication = odl_extractor_fixture.publication(odl_license)
        extractor = odl_extractor_fixture.extractor(
            skipped_license_formats=skipped_formats
        )

        circulation_data = extractor._extract_odl_circulation_data(
            publication=publication,
            license_info_documents={
                odl_extractor_fixture.license_identifier: odl_extractor_fixture.license_info(
                    license_format
                )
            },
            identifier=odl_extractor_fixture.identifier_data(),
            medium=medium,
        )

        assert circulation_data.formats == []
        assert circulation_data.licenses is not None
        assert len(circulation_data.licenses) == 1

    def test__extract_odl_circulation_data_empty_protection_formats(
        self,
        odl_extractor_fixture: ODLExtractorTestFixture,
    ) -> None:
        """
        Test that when protection.formats is empty, the format is added with drm_scheme=None.
        """
        license_format = MediaTypes.EPUB_MEDIA_TYPE

        # Explicitly pass empty protection to test the no-DRM case
        odl_license = odl_extractor_fixture.license(
            license_format, protection=Protection(format=[])
        )
        publication = odl_extractor_fixture.publication(odl_license)
        extractor = odl_extractor_fixture.extractor()

        circulation_data = extractor._extract_odl_circulation_data(
            publication=publication,
            license_info_documents={
                odl_extractor_fixture.license_identifier: odl_extractor_fixture.license_info(
                    license_format
                )
            },
            identifier=odl_extractor_fixture.identifier_data(),
            medium=EditionConstants.BOOK_MEDIUM,
        )

        # Ensure we got exactly one format with no DRM
        assert len(circulation_data.formats) == 1
        [format_data] = circulation_data.formats
        assert format_data.content_type == MediaTypes.EPUB_MEDIA_TYPE
        assert format_data.drm_scheme is None
        assert format_data.rights_uri == RightsStatus.IN_COPYRIGHT

    def test__extract_schema_org_subjects_typical_age_range(self) -> None:
        """Test extraction of schema:typicalAgeRange into AGE_RANGE subject."""
        metadata = opds2.PublicationMetadata(
            type="http://schema.org/Book",
            identifier="urn:isbn:9780306406157",
            title="Test Book",
            typical_age_range="8-12",
        )

        subjects = OPDS2WithODLExtractor._extract_schema_org_subjects(metadata)

        assert len(subjects) == 1
        assert subjects[0].type == "schema:typicalAgeRange"
        assert subjects[0].identifier == "8-12"
        assert subjects[0].name == "8-12"

    def test__extract_schema_org_subjects_audience_type(self) -> None:
        """Test extraction of schema:audience.audienceType into FREEFORM_AUDIENCE subject."""
        metadata = opds2.PublicationMetadata(
            type="http://schema.org/Book",
            identifier="urn:isbn:9780306406157",
            title="Test Book",
            audience=Audience(
                type="schema:PeopleAudience",
                audience_type="Children",
            ),
        )

        subjects = OPDS2WithODLExtractor._extract_schema_org_subjects(metadata)

        assert len(subjects) == 1
        assert subjects[0].type == "schema:audience"
        assert subjects[0].identifier == "Children"
        assert subjects[0].name == "Children"

    def test__extract_schema_org_subjects_audience_age_range(self) -> None:
        """Test extraction of suggested min/max age from schema:audience."""
        # Both min and max age
        metadata = opds2.PublicationMetadata(
            type="http://schema.org/Book",
            identifier="urn:isbn:9780306406157",
            title="Test Book",
            audience=Audience(
                type="schema:PeopleAudience",
                suggested_min_age=5,
                suggested_max_age=10,
            ),
        )

        subjects = OPDS2WithODLExtractor._extract_schema_org_subjects(metadata)

        assert len(subjects) == 1
        assert subjects[0].type == "schema:typicalAgeRange"
        assert subjects[0].identifier == "5-10"

        # Only min age
        metadata = opds2.PublicationMetadata(
            type="http://schema.org/Book",
            identifier="urn:isbn:9780306406157",
            title="Test Book",
            audience=Audience(
                type="schema:PeopleAudience",
                suggested_min_age=8,
            ),
        )

        subjects = OPDS2WithODLExtractor._extract_schema_org_subjects(metadata)

        assert len(subjects) == 1
        assert subjects[0].identifier == "8-"

        # Only max age
        metadata = opds2.PublicationMetadata(
            type="http://schema.org/Book",
            identifier="urn:isbn:9780306406157",
            title="Test Book",
            audience=Audience(
                type="schema:PeopleAudience",
                suggested_max_age=12,
            ),
        )

        subjects = OPDS2WithODLExtractor._extract_schema_org_subjects(metadata)

        assert len(subjects) == 1
        assert subjects[0].identifier == "-12"

    def test__extract_schema_org_subjects_combined(self) -> None:
        """Test extraction when multiple schema.org fields are present."""
        metadata = opds2.PublicationMetadata(
            type="http://schema.org/Book",
            identifier="urn:isbn:9780306406157",
            title="Test Book",
            typical_age_range="5-12",
            audience=Audience(
                type="schema:PeopleAudience",
                audience_type="Children",
                suggested_min_age=6,
                suggested_max_age=11,
            ),
        )

        subjects = OPDS2WithODLExtractor._extract_schema_org_subjects(metadata)

        # Should have 3 subjects: typical_age_range, audience_type, and suggested age range
        assert len(subjects) == 3

        # Check types are present
        types = {s.type for s in subjects}
        assert "schema:typicalAgeRange" in types
        assert "schema:audience" in types

        # Check identifiers
        identifiers = {s.identifier for s in subjects}
        assert "5-12" in identifiers  # from typical_age_range
        assert "Children" in identifiers  # from audience_type
        assert "6-11" in identifiers  # from suggested ages

    @pytest.mark.parametrize(
        "provider_position,published,expected",
        [
            pytest.param(11, datetime.date(2026, 11, 1), 11, id="numbered-issue"),
            pytest.param(None, datetime.date(2026, 11, 1), 20261101, id="dated-issue"),
            pytest.param(20261124, None, 20261124, id="position-is-itself-a-date"),
            pytest.param(None, None, None, id="neither"),
            pytest.param(14.1, None, 14.1, id="half-issue"),
        ],
    )
    def test__issue_position(
        self,
        provider_position: int | float | None,
        published: datetime.date | None,
        expected: int | float | None,
    ) -> None:
        assert (
            OPDS2WithODLExtractor._issue_position(provider_position, published)
            == expected
        )

    @pytest.mark.parametrize(
        "volume_label,expected",
        [
            pytest.param("2026", 2026, id="year"),
            pytest.param("12", 12, id="number"),
            pytest.param(" 12 ", 12, id="padded"),
            pytest.param("Winter", None, id="not-a-number"),
            pytest.param("Series II", None, id="roman-numeral"),
            # These are digits as far as str.isdigit() is concerned, but they
            # are not a volume anyone would sort on.
            pytest.param("١٢", None, id="digits-from-another-script"),
            pytest.param("", None, id="empty"),
            pytest.param(None, None, id="no-volume"),
        ],
    )
    def test__volume_position(
        self, volume_label: str | None, expected: int | None
    ) -> None:
        assert OPDS2WithODLExtractor._volume_position(volume_label) == expected

    @pytest.mark.parametrize(
        "filename,serial_type_guessed",
        [
            pytest.param("magazine_with_volume.json", False, id="magazine-key"),
            pytest.param(
                "schema_periodical_flattened_volume.json",
                True,
                id="schema-periodical-key",
            ),
        ],
    )
    def test__extract_periodical_data_from_feed(
        self,
        odl_extractor_fixture: ODLExtractorTestFixture,
        periodical_files_fixture: PeriodicalFilesFixture,
        filename: str,
        serial_type_guessed: bool,
    ) -> None:
        """Both of the ways a vendor writes a magazine issue reach the same place."""
        feed = PublicationFeedNoValidation.model_validate_json(
            periodical_files_fixture.sample_data(filename)
        )
        publication = opds2.Publication.model_validate(feed.publications[0])
        extractor = odl_extractor_fixture.extractor()

        bibliographic = extractor.publication_bibliographic(
            identifier=IdentifierData.parse_urn(publication.metadata.identifier),
            publication=publication,
        )

        periodical = bibliographic.periodical
        assert periodical is not None
        assert periodical.serial_type == SerialType.MAGAZINE
        assert periodical.serial_type_guessed is serial_type_guessed
        assert periodical.title == "Example Science Monthly"
        assert periodical.sort_title == "Example Science Monthly"
        assert periodical.identifier == IdentifierData(
            type=Identifier.ISSN, identifier="1234-5679"
        )
        # A serial reference carries neither of these, so they come from the
        # issue, which is published by whoever publishes the magazine.
        assert periodical.publisher == "Example Publishing Co."
        assert periodical.language == "eng"
        assert periodical.volume_label == "2026"
        assert periodical.volume_position == 2026
        # The feed numbers this issue, so its date is not used.
        assert periodical.issue_position == 11
        assert periodical.issue_position_from_date is False

    def test__extract_periodical_data_no_belongs_to(
        self, odl_extractor_fixture: ODLExtractorTestFixture
    ) -> None:
        """A book says nothing about a serial, and gets no PeriodicalData."""
        publication = odl_extractor_fixture.opds2_publication()
        extractor = odl_extractor_fixture.extractor()

        assert (
            extractor._extract_periodical_data(
                publication, publisher=None, language=None, published=None
            )
            is None
        )

    def test__extract_periodical_data_dated_issue(
        self, odl_extractor_fixture: ODLExtractorTestFixture
    ) -> None:
        """An issue the feed does not number is placed by the date it came out."""
        publication = odl_extractor_fixture.opds2_publication(
            type="https://schema.org/PublicationIssue",
            title="November 2026",
            belongs_to=rwpm.BelongsTo(
                journal=rwpm.SerialReference(name="Example Journal"),
            ),
        )
        extractor = odl_extractor_fixture.extractor()

        periodical = extractor._extract_periodical_data(
            publication,
            publisher=None,
            language=None,
            published=datetime.date(2026, 11, 1),
        )

        assert periodical is not None
        assert periodical.serial_type == SerialType.JOURNAL
        assert periodical.serial_type_guessed is False
        assert periodical.identifier is None
        assert periodical.volume_label is None
        assert periodical.volume_position is None
        assert periodical.issue_position == 20261101
        assert periodical.issue_position_from_date is True

    def test__extract_periodical_data_unreadable_identifier(
        self,
        odl_extractor_fixture: ODLExtractorTestFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A serial whose ISSN we cannot read is still imported, without it."""
        publication = odl_extractor_fixture.opds2_publication(
            type="https://schema.org/PublicationIssue",
            title="November 2026",
            belongs_to=rwpm.BelongsTo(
                magazine=rwpm.SerialReference(
                    name="Example Science Monthly",
                    identifier="urn:issn:1234-5678",
                ),
            ),
        )
        extractor = odl_extractor_fixture.extractor()

        with caplog.at_level(logging.WARNING):
            periodical = extractor._extract_periodical_data(
                publication, publisher=None, language=None, published=None
            )

        assert periodical is not None
        assert periodical.title == "Example Science Monthly"
        assert periodical.identifier is None
        assert "urn:issn:1234-5678" in caplog.text

    def test__serial_type_guessed_more_specific_key_wins(self) -> None:
        """A feed sending both keys has not made us guess anything."""
        belongs_to = rwpm.BelongsTo.model_validate(
            {
                "magazine": {"name": "Example Science Monthly"},
                "schema:Periodical": {"name": "Example Science Monthly"},
            }
        )

        assert belongs_to.serial_type == rwpm.SerialType.MAGAZINE
        assert OPDS2WithODLExtractor._serial_type_guessed(belongs_to) is False

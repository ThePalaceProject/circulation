from __future__ import annotations

import re
from collections.abc import Generator

from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.link import LinkData
from palace.manager.data_layer.subject import SubjectData
from palace.manager.integration.license.boundless.constants import (
    INTERNAL_FORMAT_TO_DELIVERY_MECHANISM,
    BoundlessFormat,
)
from palace.manager.integration.license.boundless.model.xml import (
    AvailabilityResponse,
    Title,
)
from palace.manager.sqlalchemy.constants import LinkRelations, MediaTypes
from palace.manager.sqlalchemy.model.classification import Classification, Subject
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePoolStatus,
)
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.util.log import LoggerMixin


class BibliographicParser(LoggerMixin):
    # Authors with a special role have an abbreviation after their names,
    # e.g. "San Ruby (FRW)"
    ROLE_ABBREVIATION_REGEX = re.compile(r"\(([A-Z][A-Z][A-Z])\)$")
    ROLE_ABBREVIATION_TO_ROLE = dict(
        INT=Contributor.Role.INTRODUCTION,
        EDT=Contributor.Role.EDITOR,
        PHT=Contributor.Role.PHOTOGRAPHER,
        ILT=Contributor.Role.ILLUSTRATOR,
        TRN=Contributor.Role.TRANSLATOR,
        FRW=Contributor.Role.FOREWORD,
        ADP=Contributor.Role.AUTHOR,  # Author of adaptation
        COR=Contributor.Role.AUTHOR,  # Corporate author
    )

    @classmethod
    def _parse_contributor(
        cls,
        author: str,
        primary_author_found: bool = False,
        force_role: Contributor.Role | None = None,
    ) -> ContributorData:
        """Parse a contributor string.

        The contributor string looks like "Butler, Octavia" or "Walt
        Disney Pictures (COR)" or "Rex, Adam (ILT)". The optional
        three-letter code describes the contributor's role in the
        book.

        :param author: The string to parse.

        :param primary_author_found: If this is false, then a
            contributor with no three-letter code will be treated as
            the primary author. If this is true, then a contributor
            with no three-letter code will be treated as just a
            regular author.

        :param force_role: If this is set, the contributor will be
            assigned this role, no matter what. This takes precedence
            over the value implied by primary_author_found.
        """
        if primary_author_found:
            default_author_role = Contributor.Role.AUTHOR
        else:
            default_author_role = Contributor.Role.PRIMARY_AUTHOR
        role = default_author_role
        match = cls.ROLE_ABBREVIATION_REGEX.search(author)
        if match:
            role_type = match.groups()[0]
            role = cls.ROLE_ABBREVIATION_TO_ROLE.get(
                role_type, Contributor.Role.UNKNOWN
            )
            if role is Contributor.Role.AUTHOR and not primary_author_found:
                role = Contributor.Role.PRIMARY_AUTHOR
            author = author[:-5].strip()
        if force_role:
            role = force_role
        return ContributorData(sort_name=author, roles=[role])

    @classmethod
    def _extract_contributors(cls, title: Title) -> list[ContributorData]:
        """Extract contributors from a Title object."""
        contributors = []
        found_primary_author = False
        for contributor in title.contributors:
            contributor_data = cls._parse_contributor(contributor, found_primary_author)
            if Contributor.Role.PRIMARY_AUTHOR in contributor_data.roles:
                found_primary_author = True
            contributors.append(contributor_data)

        for narrator in title.narrators:
            contributor_data = cls._parse_contributor(
                narrator, force_role=Contributor.Role.NARRATOR
            )
            contributors.append(contributor_data)

        return contributors

    @staticmethod
    def _extract_subjects(title: Title) -> list[SubjectData]:
        subjects = [
            SubjectData(
                type=Subject.BISAC,
                identifier=None,
                name=subject_identifier,
                weight=Classification.TRUSTED_DISTRIBUTOR_WEIGHT,
            )
            for subject_identifier in title.subjects
        ]
        if title.audience:
            subjects.append(
                SubjectData(
                    type=Subject.AXIS_360_AUDIENCE,
                    identifier=title.audience,
                    weight=Classification.TRUSTED_DISTRIBUTOR_WEIGHT,
                )
            )
        return subjects

    @staticmethod
    def _extract_links(title: Title) -> list[LinkData]:
        links = []
        if title.annotation:
            links.append(
                LinkData(
                    rel=Hyperlink.DESCRIPTION,
                    content=title.annotation,
                    media_type=Representation.TEXT_PLAIN,
                )
            )
        thumbnail_url = title.image_url
        if thumbnail_url:
            # We presume all images from this service are JPEGs.
            media_type = MediaTypes.JPEG_MEDIA_TYPE
            if "/Medium/" in thumbnail_url:
                # We know about a URL hack for this service that lets us
                # get a larger image.
                full_size_url = thumbnail_url.replace("/Medium/", "/Large/")
            else:
                # If the URL hack won't work, treat the image we got
                # as both the full-sized image and its thumbnail.
                # This won't happen unless B&T changes the service.
                full_size_url = thumbnail_url

            thumbnail = LinkData(
                rel=LinkRelations.THUMBNAIL_IMAGE,
                href=thumbnail_url,
                media_type=media_type,
            )
            image = LinkData(
                rel=LinkRelations.IMAGE,
                href=full_size_url,
                media_type=media_type,
                thumbnail=thumbnail,
            )
            links.append(image)
        return links

    @staticmethod
    def _extract_identifiers(
        title: Title,
    ) -> tuple[IdentifierData, list[IdentifierData]]:
        identifiers = []
        primary_identifier = IdentifierData(
            type=Identifier.AXIS_360_ID, identifier=title.title_id
        )
        identifiers.append(primary_identifier)
        if title.isbn:
            identifiers.append(
                IdentifierData(type=Identifier.ISBN, identifier=title.isbn)
            )
        return primary_identifier, identifiers

    @classmethod
    def _extract_formats(cls, title: Title, medium: str) -> list[FormatData]:
        format_data = []
        available_formats = title.availability.available_formats_normalized

        for internal_format in available_formats:
            if internal_format == BoundlessFormat.axis_now:
                if medium == Edition.BOOK_MEDIUM:
                    format_data.append(
                        FormatData(
                            content_type=Representation.EPUB_MEDIA_TYPE,
                            drm_scheme=DeliveryMechanism.BAKER_TAYLOR_KDRM_DRM,
                        )
                    )

            elif delivery_data := INTERNAL_FORMAT_TO_DELIVERY_MECHANISM.get(
                internal_format
            ):
                format_data.append(
                    FormatData(
                        content_type=delivery_data.content_type,
                        drm_scheme=delivery_data.drm_scheme,
                    )
                )

            else:
                cls.logger().warning(
                    "Unrecognized Boundless format for %s: %s"
                    % (title.title_id, internal_format)
                )

        if not format_data:
            cls.logger().error(
                f"No supported format for {title.title_id} ({title!r})! Saw: {', '.join(title.availability.available_formats)}"
            )

        return format_data

    @classmethod
    def _extract_medium(cls, title: Title) -> str:
        """Extract the medium from the title."""
        return (
            Edition.AUDIO_MEDIUM if title.format_type == "ABT" else Edition.BOOK_MEDIUM
        )

    @classmethod
    def _extract(cls, title: Title) -> tuple[BibliographicData, CirculationData]:
        """Turn bibliographic metadata into a BibliographicData and a CirculationData objects,
        and return them as a tuple."""

        primary_identifier, identifiers = cls._extract_identifiers(title)
        medium = cls._extract_medium(title)
        formats = cls._extract_formats(title, medium)

        licenses_owned = title.availability.total_copies
        license_status = (
            LicensePoolStatus.ACTIVE
            if licenses_owned > 0
            else LicensePoolStatus.EXHAUSTED
        )

        circulationdata = CirculationData(
            data_source_name=DataSource.BOUNDLESS,
            primary_identifier_data=primary_identifier,
            formats=formats,
            licenses_owned=licenses_owned,
            licenses_available=title.availability.available_copies,
            licenses_reserved=0,
            patrons_in_hold_queue=title.availability.holds_queue_size,
            last_checked=title.availability.update_date,
            status=license_status,
        )

        bibliographic = BibliographicData(
            data_source_name=DataSource.BOUNDLESS,
            title=title.product_title,
            language=title.language,
            medium=medium,
            series=title.series,
            publisher=title.publisher,
            imprint=title.imprint,
            published=title.publication_date,
            primary_identifier_data=primary_identifier,
            identifiers=identifiers,
            subjects=cls._extract_subjects(title),
            contributors=cls._extract_contributors(title),
            links=cls._extract_links(title),
            circulation=circulationdata,
            duration=title.runtime,
            # This isn't 100% accurate, we don't get a last updated
            # date for the bibliographic metadata, just the availability
            # information, but it should suffice to keep track of
            # whether something has changed.
            data_source_last_updated=title.availability.update_date,
        )
        return bibliographic, circulationdata

    @classmethod
    def parse(
        cls, availability: AvailabilityResponse
    ) -> Generator[tuple[BibliographicData, CirculationData]]:
        """Parse an AvailabilityResponse object into a list of
        BibliographicData and CirculationData objects.
        """
        for title in availability.titles:
            yield cls._extract(title)

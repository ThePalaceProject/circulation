from __future__ import annotations

import re
from collections.abc import Generator

from palace.manager.api.axis.constants import Axis360Formats
from palace.manager.api.axis.models.xml import AvailabilityResponse, Title
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.link import LinkData
from palace.manager.data_layer.subject import SubjectData
from palace.manager.sqlalchemy.constants import LinkRelations, MediaTypes
from palace.manager.sqlalchemy.model.classification import Classification, Subject
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import DeliveryMechanism
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.util.log import LoggerMixin


class BibliographicParser(LoggerMixin):
    DELIVERY_DATA_FOR_AXIS_FORMAT: dict[str, tuple[str | None, str] | None] = {
        Axis360Formats.blio: None,  # Legacy format, handled the same way as AxisNow
        Axis360Formats.acoustik: (None, DeliveryMechanism.FINDAWAY_DRM),  # Audiobooks
        Axis360Formats.axis_now: None,  # Handled specially, for ebooks only.
        Axis360Formats.epub: (
            Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
        ),
        Axis360Formats.pdf: (
            Representation.PDF_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
        ),
    }

    # Axis authors with a special role have an abbreviation after their names,
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
        """Parse an Axis 360 contributor string.

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
    def _extract_formats(cls, title: Title) -> tuple[str, list[FormatData]]:
        formats = []
        seen_formats = []

        # All of the formats we don't support, like Blio, are ebook
        # formats. If this is an audiobook format (Acoustik), we'll
        # hear about it below.
        medium = Edition.BOOK_MEDIUM

        # If AxisNow is mentioned as a format, and this turns out to be a book,
        # we'll be adding an extra delivery mechanism.
        axisnow_seen = False

        # Blio is an older ebook format now used as an alias for AxisNow.
        blio_seen = False

        for axis_format in title.availability.available_formats:
            seen_formats.append(axis_format)

            if axis_format == Axis360Formats.blio:
                # We will be adding an AxisNow FormatData.
                blio_seen = True
                continue
            elif axis_format == Axis360Formats.axis_now:
                # We will only be adding an AxisNow FormatData if this
                # turns out to be an ebook.
                axisnow_seen = True
                continue

            if axis_format not in cls.DELIVERY_DATA_FOR_AXIS_FORMAT:
                cls.logger().warning(
                    "Unrecognized Axis format name for %s: %s"
                    % (title.title_id, axis_format)
                )
            elif delivery_data := cls.DELIVERY_DATA_FOR_AXIS_FORMAT.get(axis_format):
                content_type, drm_scheme = delivery_data
                formats.append(
                    FormatData(content_type=content_type, drm_scheme=drm_scheme)
                )

                if drm_scheme == DeliveryMechanism.FINDAWAY_DRM:
                    medium = Edition.AUDIO_MEDIUM
                else:
                    medium = Edition.BOOK_MEDIUM
        if blio_seen or (axisnow_seen and medium == Edition.BOOK_MEDIUM):
            # This ebook is available through AxisNow. Add an
            # appropriate FormatData.
            #
            # Audiobooks may also be available through AxisNow, but we
            # currently ignore that fact.
            formats.append(
                FormatData(content_type=None, drm_scheme=DeliveryMechanism.AXISNOW_DRM)
            )

        if not formats:
            cls.logger().error(
                f"No supported format for {title.title_id} ({title!r})! Saw: {', '.join(seen_formats)}"
            )

        return medium, formats

    @classmethod
    def _extract(cls, title: Title) -> tuple[BibliographicData, CirculationData]:
        """Turn bibliographic metadata into a BibliographicData and a CirculationData objects,
        and return them as a tuple."""

        primary_identifier, identifiers = cls._extract_identifiers(title)
        medium, formats = cls._extract_formats(title)

        circulationdata = CirculationData(
            data_source_name=DataSource.AXIS_360,
            primary_identifier_data=primary_identifier,
            formats=formats,
            licenses_owned=title.availability.total_copies,
            licenses_available=title.availability.available_copies,
            licenses_reserved=0,
            patrons_in_hold_queue=title.availability.holds_queue_size,
        )

        bibliographic = BibliographicData(
            data_source_name=DataSource.AXIS_360,
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
        )
        return bibliographic, circulationdata

    @classmethod
    def parse(
        cls, availability: AvailabilityResponse
    ) -> Generator[tuple[BibliographicData, CirculationData]]:
        """Parse an Axis 360 availability response into a list of
        BibliographicData and CirculationData objects.
        """
        for title in availability.titles:
            yield cls._extract(title)
